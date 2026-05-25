import type { IncomingMessage, ServerResponse } from 'node:http';
import http from 'node:http';
import https from 'node:https';
import net, { type Socket } from 'node:net';
import tls from 'node:tls';

type AppHandler = (req: IncomingMessage, res: ServerResponse) => void | Promise<void>;
type HeaderValue = string | number | readonly string[] | undefined;

const host = process.env.HOST || '0.0.0.0';
const port = Number(process.env.PORT || 3000);
const runtimeUpstream = process.env.CLIPROXY_UPSTREAM || '';
const publicScheme = process.env.PUBLIC_SCHEME || 'https';
const runtimePrefixes = ['/api', '/threads', '/gateway', '/actors'];
const { handler } = (await import(new URL('./build/handler.js', import.meta.url).href)) as { handler: AppHandler };

function shouldProxyRuntime(url = '') {
  return runtimePrefixes.some((prefix) => url === prefix || url.startsWith(`${prefix}/`));
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

function headerLines(headers: Record<string, HeaderValue>) {
  return Object.entries(headers)
    .flatMap(([name, value]) => (Array.isArray(value) ? value.map((entry) => [name, entry]) : [[name, value]]))
    .filter((entry): entry is [string, string | number] => entry[1] !== undefined)
    .map(([name, value]) => `${name}: ${value}`);
}

function proxyUpgrade(req: IncomingMessage, socket: Socket, head: Buffer) {
  if (!runtimeUpstream || !shouldProxyRuntime(req.url || '')) {
    socket.destroy();
    return;
  }

  const target = runtimeTarget(req.url || '/');
  const targetPort = Number(target.port || (target.protocol === 'https:' ? 443 : 80));
  const targetSocket =
    target.protocol === 'https:'
      ? tls.connect({ host: target.hostname, port: targetPort, servername: target.hostname })
      : net.connect({ host: target.hostname, port: targetPort });
  const readyEvent = target.protocol === 'https:' ? 'secureConnect' : 'connect';

  targetSocket.once(readyEvent, () => {
    const path = `${target.pathname}${target.search}`;
    const headers = proxyHeaders(req.headers, target.host);
    targetSocket.write(`${req.method} ${path} HTTP/${req.httpVersion}\r\n${headerLines(headers).join('\r\n')}\r\n\r\n`);
    if (head.length > 0) {
      targetSocket.write(head);
    }
    socket.pipe(targetSocket).pipe(socket);
  });

  targetSocket.on('error', () => socket.destroy());
  socket.on('error', () => targetSocket.destroy());
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
