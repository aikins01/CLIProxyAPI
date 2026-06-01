import type { IncomingMessage, ServerResponse } from 'node:http';
import http from 'node:http';
import https from 'node:https';
import type { Socket } from 'node:net';

type AppHandler = (req: IncomingMessage, res: ServerResponse) => void | Promise<void>;

const host = process.env.HOST || '0.0.0.0';
const port = Number(process.env.PORT || 3000);
const runtimeUpstream = process.env.CLIPROXY_UPSTREAM || '';
const publicScheme = process.env.PUBLIC_SCHEME || 'https';
const runtimePrefixes = ['/api', '/threads', '/gateway', '/actors', '/metadata'];
const { handler } = (await import(new URL('./build/handler.js', import.meta.url).href)) as { handler: AppHandler };

function shouldProxyRuntime(url = '') {
  const [pathname = ''] = url.split(/[?#]/, 1);
  return runtimePrefixes.some((prefix) => pathname === prefix || pathname.startsWith(`${prefix}/`));
}

function proxyHeaders(headers: IncomingMessage['headers'], targetHost: string) {
  return {
    ...headers,
    host: targetHost,
    'x-forwarded-host': headers.host || '',
    'x-forwarded-proto': publicScheme
  };
}

function proxyResponseHeaders(headers: IncomingMessage['headers']) {
  const next = { ...headers };
  for (const name of [
    'connection',
    'keep-alive',
    'proxy-authenticate',
    'proxy-authorization',
    'te',
    'trailer',
    'transfer-encoding',
    'upgrade'
  ]) {
    delete next[name];
  }
  return next;
}

function runtimeTarget(requestUrl = '/') {
  return new URL(requestUrl, runtimeUpstream);
}

function proxyHttp(req: IncomingMessage, res: ServerResponse) {
  if (!runtimeUpstream) {
    res.writeHead(502, { 'content-type': 'application/json' });
    res.end(JSON.stringify({ error: 'CLIPROXY_UPSTREAM is not configured' }));
    return;
  }

  const target = runtimeTarget(req.url || '/');
  const transport = target.protocol === 'https:' ? https : http;
  const proxyReq = transport.request(
    target,
    {
      method: req.method,
      headers: proxyHeaders(req.headers, target.host)
    },
    (proxyRes) => {
      res.writeHead(proxyRes.statusCode || 502, proxyResponseHeaders(proxyRes.headers));
      proxyRes.pipe(res);
    }
  );

  proxyReq.on('error', (error) => {
    if (!res.headersSent) {
      res.writeHead(502, { 'content-type': 'application/json' });
    }
    res.end(JSON.stringify({ error: 'Runtime proxy failed', detail: error.message }));
  });

  req.pipe(proxyReq);
}

function writeRawResponse(socket: Socket, message: IncomingMessage) {
  socket.write(`HTTP/${message.httpVersion} ${message.statusCode || 502} ${message.statusMessage || 'Bad Gateway'}\r\n`);
  for (let index = 0; index < message.rawHeaders.length; index += 2) {
    socket.write(`${message.rawHeaders[index]}: ${message.rawHeaders[index + 1]}\r\n`);
  }
  socket.write('\r\n');
}

function proxyUpgrade(req: IncomingMessage, socket: Socket, head: Buffer) {
  if (!runtimeUpstream || !shouldProxyRuntime(req.url || '')) {
    socket.destroy();
    return;
  }

  const target = runtimeTarget(req.url || '/');
  const transport = target.protocol === 'https:' ? https : http;
  const proxyReq = transport.request(target, {
    method: req.method,
    headers: proxyHeaders(req.headers, target.host)
  });

  proxyReq.on('upgrade', (proxyRes, proxySocket, proxyHead) => {
    writeRawResponse(socket, proxyRes);
    if (proxyHead.length > 0) {
      socket.write(proxyHead);
    }
    if (head.length > 0) {
      proxySocket.write(head);
    }
    socket.pipe(proxySocket).pipe(socket);
  });

  proxyReq.on('response', (proxyRes) => {
    writeRawResponse(socket, proxyRes);
    proxyRes.pipe(socket);
  });

  proxyReq.on('error', () => socket.destroy());
  socket.on('error', () => proxyReq.destroy());
  proxyReq.end();
}

const server = http.createServer((req, res) => {
  if (shouldProxyRuntime(req.url || '')) {
    proxyHttp(req, res);
    return;
  }
  handler(req, res);
});

server.on('upgrade', proxyUpgrade);

server.listen(port, host, () => {
  console.log(`Listening on http://${host}:${port}`);
});
