import http from 'node:http';
import { timingSafeEqual } from 'node:crypto';
import decoder from './worker.js';

const { RUNNER_KEY, SGL_COOKIE, SGL_TOKEN } = process.env;
if (!RUNNER_KEY || RUNNER_KEY.length < 32 || !SGL_COOKIE || !SGL_TOKEN) {
  throw Error('Missing runner credentials');
}
const expected = Buffer.from('Bearer ' + RUNNER_KEY);
const server = http.createServer(async (req, res) => {
  const supplied = Buffer.from(req.headers.authorization || '');
  if (supplied.length !== expected.length || !timingSafeEqual(supplied, expected)) {
    res.writeHead(403).end();
    return;
  }
  if (req.method !== 'GET' || !req.url.startsWith('/') || req.url.startsWith('//')) {
    res.writeHead(400).end();
    return;
  }
  try {
    const request = new Request('http://localhost' + req.url);
    const result = await decoder.fetch(request, { SGL_TOKEN, SGL_COOKIE });
    const body = Buffer.from(await result.arrayBuffer());
    res.writeHead(result.status, Object.fromEntries(result.headers));
    res.end(body);
  } catch {
    res.writeHead(502).end();
  }
});
server.listen(18467, '127.0.0.1');
