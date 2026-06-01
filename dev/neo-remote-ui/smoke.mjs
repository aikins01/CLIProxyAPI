import { spawn } from 'node:child_process';
import http from 'node:http';
import net from 'node:net';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const root = path.dirname(fileURLToPath(import.meta.url));

function assert(condition, message) {
  if (!condition) throw new Error(message);
}

function listen(server, port = 0) {
  return new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(port, '127.0.0.1', () => {
      server.off('error', reject);
      resolve(server.address().port);
    });
  });
}

function closeServer(server) {
  return new Promise((resolve, reject) => {
    server.close((error) => (error ? reject(error) : resolve()));
  });
}

async function freePort() {
  const server = http.createServer();
  const port = await listen(server);
  await closeServer(server);
  return port;
}

async function waitForHTTP(url, timeoutMs = 5000) {
  const deadline = Date.now() + timeoutMs;
  let lastError;
  while (Date.now() < deadline) {
    try {
      const response = await fetch(url);
      await response.arrayBuffer();
      return;
    } catch (error) {
      lastError = error;
      await new Promise((resolve) => setTimeout(resolve, 100));
    }
  }
  throw lastError ?? new Error(`timed out waiting for ${url}`);
}

function startRemoteServer(port, upstreamURL) {
  const child = spawn('node', ['server.ts'], {
    cwd: root,
    env: {
      ...process.env,
      HOST: '127.0.0.1',
      PORT: String(port),
      CLIPROXY_UPSTREAM: upstreamURL,
      PUBLIC_SCHEME: 'https'
    },
    stdio: ['ignore', 'pipe', 'pipe']
  });

  let output = '';
  child.stdout.on('data', (chunk) => {
    output += chunk.toString();
  });
  child.stderr.on('data', (chunk) => {
    output += chunk.toString();
  });
  child.output = () => output.trim();
  return child;
}

function watchChildExit(child) {
  let exitError;
  let resolveExit;
  const exited = new Promise((resolve) => {
    resolveExit = resolve;
  });
  child.once('exit', (code, signal) => {
    exitError = new Error(`remote UI server exited early with ${signal ?? code}: ${child.output()}`);
    resolveExit(exitError);
  });
  return {
    exited,
    assertRunning() {
      if (exitError) throw exitError;
    },
    stop() {
      child.removeAllListeners('exit');
      if (child.exitCode === null && child.signalCode === null) child.kill();
    }
  };
}

async function requestUpgrade(port, pathName, hostHeader = `127.0.0.1:${port}`) {
  return new Promise((resolve, reject) => {
    const socket = net.connect(port, '127.0.0.1');
    let response = '';
    socket.setTimeout(3000);
    socket.once('connect', () => {
      socket.write([
        `GET ${pathName} HTTP/1.1`,
        `Host: ${hostHeader}`,
        'Connection: Upgrade',
        'Upgrade: websocket',
        'Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==',
        'Sec-WebSocket-Version: 13',
        '',
        ''
      ].join('\r\n'));
    });
    socket.on('data', (chunk) => {
      response += chunk.toString();
      if (response.includes('\r\n\r\n')) {
        socket.destroy();
        resolve(response);
      }
    });
    socket.once('timeout', () => {
      socket.destroy();
      reject(new Error('timed out waiting for websocket upgrade response'));
    });
    socket.once('error', reject);
  });
}

async function readRequestBody(request) {
  const chunks = [];
  for await (const chunk of request) {
    chunks.push(Buffer.from(chunk));
  }
  return Buffer.concat(chunks).toString('utf8');
}

async function readJSON(response, label) {
  const text = await response.text();
  try {
    return JSON.parse(text);
  } catch (error) {
    throw new Error(`${label} response was not JSON: status=${response.status} body=${text.slice(0, 120)}`, { cause: error });
  }
}

let lastRuntimeRequest;
let lastUpgradeRequest;
function runtimeStatus(request) {
  if (request.url?.startsWith('/gateway/thread-actor/request/import')) return 409;
  if (request.method === 'GET' && request.headers.authorization && (request.url?.startsWith('/metadata') || request.url?.startsWith('/actors') || request.url?.startsWith('/threads'))) {
    return 200;
  }
  return request.method === 'POST' ? 200 : 401;
}

const upstream = http.createServer(async (request, response) => {
  const body = await readRequestBody(request);
  lastRuntimeRequest = {
    method: request.method,
    url: request.url,
    authorization: request.headers.authorization,
    contentType: request.headers['content-type'],
    ampClientApplication: request.headers['x-amp-client-application'],
    ampClientType: request.headers['x-amp-client-type'],
    ampClientVersion: request.headers['x-amp-client-version'],
    forwardedProto: request.headers['x-forwarded-proto'],
    forwardedHost: request.headers['x-forwarded-host'],
    body
  };
  const status = runtimeStatus(request);
  response.writeHead(status, {
    'content-type': 'application/json',
    'x-smoke-upstream': 'runtime'
  });
  response.end(JSON.stringify(lastRuntimeRequest));
});

upstream.on('upgrade', (request, socket) => {
  lastUpgradeRequest = {
    url: request.url,
    forwardedProto: request.headers['x-forwarded-proto'],
    forwardedHost: request.headers['x-forwarded-host']
  };
  socket.write([
    'HTTP/1.1 101 Switching Protocols',
    'Connection: Upgrade',
    'Upgrade: websocket',
    'X-Smoke-Upstream: runtime',
    '',
    ''
  ].join('\r\n'));
  socket.end();
});

const upstreamPort = await listen(upstream);
const appPort = await freePort();
const appHost = `127.0.0.1:${appPort}`;
const app = startRemoteServer(appPort, `http://127.0.0.1:${upstreamPort}`);
const appExit = watchChildExit(app);

try {
  const startupResult = await Promise.race([waitForHTTP(`http://127.0.0.1:${appPort}/`), appExit.exited]);
  if (startupResult instanceof Error) throw startupResult;
  appExit.assertRunning();

  const htmlResponse = await fetch(`http://127.0.0.1:${appPort}/`);
  appExit.assertRunning();
  const html = await htmlResponse.text();
  assert(htmlResponse.status === 200, `root status = ${htmlResponse.status}, want 200`);
  assert(html.includes('<title>Neo Remote</title>'), 'root HTML missing Neo Remote title');

  const apiResponse = await fetch(`http://127.0.0.1:${appPort}/api/internal?listThreads`);
  appExit.assertRunning();
  const apiBody = await readJSON(apiResponse, 'api');
  assert(apiResponse.status === 401, `runtime status = ${apiResponse.status}, want proxied 401`);
  assert(apiResponse.headers.get('x-smoke-upstream') === 'runtime', 'runtime proxy response header was not preserved');
  assert(apiBody.url === '/api/internal?listThreads', `runtime path = ${apiBody.url}`);
  assert(apiBody.forwardedProto === 'https', `x-forwarded-proto = ${apiBody.forwardedProto}`);
  assert(apiBody.forwardedHost === appHost, `x-forwarded-host = ${apiBody.forwardedHost}`);
  assert(lastRuntimeRequest?.url === '/api/internal?listThreads', 'runtime upstream did not receive /api/internal request');

  const metadataResponse = await fetch(`http://127.0.0.1:${appPort}/metadata`, {
    headers: { Authorization: 'Bearer amp-local-key' }
  });
  appExit.assertRunning();
  const metadataBody = await readJSON(metadataResponse, 'metadata');
  assert(metadataResponse.status === 200, `metadata status = ${metadataResponse.status}, want proxied 200`);
  assert(metadataBody.url === '/metadata', `metadata path = ${metadataBody.url}`);
  assert(metadataBody.authorization === 'Bearer amp-local-key', 'metadata authorization header was not preserved');
  assert(metadataBody.forwardedProto === 'https', `metadata x-forwarded-proto = ${metadataBody.forwardedProto}`);
  assert(metadataBody.forwardedHost === appHost, `metadata x-forwarded-host = ${metadataBody.forwardedHost}`);

  const actorsResponse = await fetch(`http://127.0.0.1:${appPort}/actors?name=threadActor&key=T-smoke`, {
    headers: { Authorization: 'Bearer amp-local-key' }
  });
  appExit.assertRunning();
  const actorsBody = await readJSON(actorsResponse, 'actors');
  assert(actorsResponse.status === 200, `actors status = ${actorsResponse.status}, want proxied 200`);
  assert(actorsBody.url === '/actors?name=threadActor&key=T-smoke', `actors path = ${actorsBody.url}`);
  assert(actorsBody.authorization === 'Bearer amp-local-key', 'actors authorization header was not preserved');
  assert(actorsBody.forwardedProto === 'https', `actors x-forwarded-proto = ${actorsBody.forwardedProto}`);
  assert(actorsBody.forwardedHost === appHost, `actors x-forwarded-host = ${actorsBody.forwardedHost}`);

  const threadsResponse = await fetch(`http://127.0.0.1:${appPort}/threads`, {
    headers: { Authorization: 'Bearer amp-local-key' }
  });
  appExit.assertRunning();
  const threadsBody = await readJSON(threadsResponse, 'threads');
  assert(threadsResponse.status === 200, `threads status = ${threadsResponse.status}, want proxied 200`);
  assert(threadsBody.url === '/threads', `threads path = ${threadsBody.url}`);
  assert(threadsBody.authorization === 'Bearer amp-local-key', 'threads authorization header was not preserved');
  assert(threadsBody.forwardedProto === 'https', `threads x-forwarded-proto = ${threadsBody.forwardedProto}`);
  assert(threadsBody.forwardedHost === appHost, `threads x-forwarded-host = ${threadsBody.forwardedHost}`);

  const rpcResponse = await fetch(`http://127.0.0.1:${appPort}/api/internal?listThreads`, {
    method: 'POST',
    headers: {
      Authorization: 'Bearer amp-local-key',
      'Content-Type': 'application/json',
      'X-Amp-Client-Application': 'CLI',
      'X-Amp-Client-Type': 'cli',
      'X-Amp-Client-Version': 'neo-remote-ui'
    },
    body: JSON.stringify({ method: 'listThreads', params: { limit: 2 } })
  });
  appExit.assertRunning();
  const rpcBody = await readJSON(rpcResponse, 'rpc');
  assert(rpcResponse.status === 200, `rpc status = ${rpcResponse.status}, want proxied 200`);
  assert(rpcBody.method === 'POST', `rpc method = ${rpcBody.method}`);
  assert(rpcBody.url === '/api/internal?listThreads', `rpc path = ${rpcBody.url}`);
  assert(rpcBody.authorization === 'Bearer amp-local-key', 'rpc authorization header was not preserved');
  assert(rpcBody.ampClientApplication === 'CLI', `rpc X-Amp-Client-Application = ${rpcBody.ampClientApplication}`);
  assert(rpcBody.ampClientType === 'cli', `rpc X-Amp-Client-Type = ${rpcBody.ampClientType}`);
  assert(rpcBody.ampClientVersion === 'neo-remote-ui', `rpc X-Amp-Client-Version = ${rpcBody.ampClientVersion}`);
  assert(rpcBody.forwardedProto === 'https', `rpc x-forwarded-proto = ${rpcBody.forwardedProto}`);
  assert(rpcBody.forwardedHost === appHost, `rpc x-forwarded-host = ${rpcBody.forwardedHost}`);
  assert(JSON.parse(rpcBody.body).params.limit === 2, `rpc body = ${rpcBody.body}`);

  const tailResponse = await fetch(`http://127.0.0.1:${appPort}/api/internal?getThreadTail`, {
    method: 'POST',
    headers: {
      Authorization: 'Bearer amp-local-key',
      'Content-Type': 'application/json',
      'X-Amp-Client-Application': 'CLI',
      'X-Amp-Client-Type': 'cli',
      'X-Amp-Client-Version': 'neo-remote-ui'
    },
    body: JSON.stringify({ method: 'getThreadTail', params: { thread: 'T-smoke', limit: 5 } })
  });
  appExit.assertRunning();
  const tailBody = await readJSON(tailResponse, 'thread tail rpc');
  assert(tailResponse.status === 200, `thread tail rpc status = ${tailResponse.status}, want proxied 200`);
  assert(tailBody.method === 'POST', `thread tail rpc method = ${tailBody.method}`);
  assert(tailBody.url === '/api/internal?getThreadTail', `thread tail rpc path = ${tailBody.url}`);
  assert(tailBody.authorization === 'Bearer amp-local-key', 'thread tail authorization header was not preserved');
  assert(tailBody.ampClientApplication === 'CLI', `thread tail X-Amp-Client-Application = ${tailBody.ampClientApplication}`);
  assert(tailBody.ampClientType === 'cli', `thread tail X-Amp-Client-Type = ${tailBody.ampClientType}`);
  assert(tailBody.ampClientVersion === 'neo-remote-ui', `thread tail X-Amp-Client-Version = ${tailBody.ampClientVersion}`);
  assert(tailBody.forwardedProto === 'https', `thread tail x-forwarded-proto = ${tailBody.forwardedProto}`);
  assert(tailBody.forwardedHost === appHost, `thread tail x-forwarded-host = ${tailBody.forwardedHost}`);
  assert(JSON.parse(tailBody.body).params.limit === 5, `thread tail body = ${tailBody.body}`);

  const attachmentResponse = await fetch(`http://127.0.0.1:${appPort}/api/attachments`, {
    method: 'POST',
    headers: {
      Authorization: 'Bearer amp-local-key',
      'Content-Type': 'application/json'
    },
    body: JSON.stringify({ data: 'aW1hZ2U=', mediaType: 'image/png', name: 'smoke.png' })
  });
  appExit.assertRunning();
  const attachmentBody = await readJSON(attachmentResponse, 'attachment');
  assert(attachmentResponse.status === 200, `attachment status = ${attachmentResponse.status}, want proxied 200`);
  assert(attachmentBody.url === '/api/attachments', `attachment path = ${attachmentBody.url}`);
  assert(attachmentBody.authorization === 'Bearer amp-local-key', 'attachment authorization header was not preserved');
  assert(attachmentBody.forwardedProto === 'https', `attachment x-forwarded-proto = ${attachmentBody.forwardedProto}`);
  assert(attachmentBody.forwardedHost === appHost, `attachment x-forwarded-host = ${attachmentBody.forwardedHost}`);
  assert(JSON.parse(attachmentBody.body).mediaType === 'image/png', `attachment body = ${attachmentBody.body}`);

  const importPath = '/gateway/thread-actor/request/import?rvt-method=getOrCreate&rvt-key=T-smoke&rvt-skip-ready-wait=true&cliproxy-client=neo-remote-ui&auth_token=amp-local-key';
  const importResponse = await fetch(`http://127.0.0.1:${appPort}${importPath}`, {
    method: 'POST',
    headers: {
      Authorization: 'Bearer amp-local-key',
      'Content-Type': 'application/json'
    },
    body: JSON.stringify({ thread: { id: 'T-smoke', agentMode: 'deep', messages: [] } })
  });
  appExit.assertRunning();
  const importBody = await readJSON(importResponse, 'import');
  assert(importResponse.status === 409, `import status = ${importResponse.status}, want proxied 409`);
  assert(importBody.url === importPath, `import path = ${importBody.url}`);
  assert(importBody.authorization === 'Bearer amp-local-key', 'import authorization header was not preserved');
  assert(importBody.forwardedProto === 'https', `import x-forwarded-proto = ${importBody.forwardedProto}`);
  assert(importBody.forwardedHost === appHost, `import x-forwarded-host = ${importBody.forwardedHost}`);
  assert(JSON.parse(importBody.body).thread.agentMode === 'deep', `import body = ${importBody.body}`);

  const upgradeResponse = await requestUpgrade(appPort, '/gateway/thread-actor/?rvt-key=T-smoke&auth_token=amp-local-key', appHost);
  appExit.assertRunning();
  assert(upgradeResponse.startsWith('HTTP/1.1 101'), `upgrade response was not 101: ${upgradeResponse}`);
  assert(lastUpgradeRequest?.url === '/gateway/thread-actor/?rvt-key=T-smoke&auth_token=amp-local-key', 'gateway upgrade did not reach runtime upstream');
  assert(lastUpgradeRequest?.forwardedProto === 'https', `upgrade x-forwarded-proto = ${lastUpgradeRequest?.forwardedProto}`);
  assert(lastUpgradeRequest?.forwardedHost === appHost, `upgrade x-forwarded-host = ${lastUpgradeRequest?.forwardedHost}`);

  console.log('neo remote smoke passed');
} finally {
  appExit.stop();
  await closeServer(upstream);
}
