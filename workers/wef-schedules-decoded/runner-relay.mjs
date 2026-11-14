import fs from 'node:fs';
import path from 'node:path';
import {recordRenewal} from './health-records.mjs';
import { publicRoute, fetchPublic } from './public-live.mjs';
import http from 'node:http';
import { timingSafeEqual } from 'node:crypto';
import decoder from './worker.js';

const { RUNNER_KEY, SGL_COOKIE, SGL_TOKEN } = process.env;
if (!RUNNER_KEY || RUNNER_KEY.length < 32 || !SGL_COOKIE || !SGL_TOKEN) {
  throw Error('Missing runner credentials');
}
let logToken=process.env.WEF_SHOWS_AIRTABLE_TOKEN;
if(!logToken){
 try{const text=fs.readFileSync(path.resolve(process.cwd(),'../wef-shows/runtime/collector.env'),'utf8');const match=text.match(/^WEF_SHOWS_AIRTABLE_TOKEN=(.*)$/m);if(match)logToken=match[1].trim().replace(/^['"]|['"]$/g,'');}catch{}
}
const logEnv={WEF_SHOWS_AIRTABLE_TOKEN:logToken};
let cachedToken;
let renewal;
async function renewSourceToken() {
  const started=new Date().toISOString(),id=crypto.randomUUID();let status=null;
  try{
  const response = await fetch('https://sglapi.wellingtoninternational.com/utilities/get-config', {
    redirect: 'error', signal: AbortSignal.timeout(15000),
    headers: {
      Accept: 'application/json, text/plain, */*', Cookie: SGL_COOKIE,
      'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 18_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.5 Mobile/15E148 Safari/604.1',
      Origin: 'https://www.wellingtoninternational.com', Referer: 'https://www.wellingtoninternational.com/',
      'sgl-request-origin': 'SGL-API', 'Cache-Control': 'no-cache', 'Accept-Language': 'en-US,en;q=0.9',
      'sec-fetch-mode': 'cors', 'sec-fetch-site': 'same-site', 'sec-fetch-dest': 'empty'
    }
  });
  status=response.status;
  if (!response.ok) throw Error('Token renewal failed');
  const { _t: token } = await response.json();
  const claims = JSON.parse(Buffer.from(token.split('.')[1], 'base64url'));
  if (claims.anon !== true || !Number.isFinite(claims.exp) || claims.exp * 1000 <= Date.now()) {
    throw Error('Invalid anonymous token');
  }
  cachedToken = { token, expires: claims.exp * 1000 };
  await recordRenewal(logEnv,{'refresh-id':id,'started-at':started,'completed-at':new Date().toISOString(),lane:'runner',endpoint:'https://sglapi.wellingtoninternational.com/utilities/get-config',reason:'missing-or-near-expiry',result:'Succeeded','http-status':status,'token-expires-at':new Date(cachedToken.expires).toISOString()});
  return token;
  }catch(error){await recordRenewal(logEnv,{'refresh-id':id,'started-at':started,'completed-at':new Date().toISOString(),lane:'runner',endpoint:'https://sglapi.wellingtoninternational.com/utilities/get-config',reason:'missing-or-near-expiry',result:'Failed','http-status':status,error:'Token renewal failed or invalid response'});throw error;}
}
function sourceToken() {
  if (cachedToken && cachedToken.expires > Date.now() + 86400000) return cachedToken.token;
  renewal ??= renewSourceToken().finally(() => { renewal = undefined; });
  return renewal;
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
    const url = new URL(request.url);
    const feed = publicRoute(url);
    if (feed && !feed.valid) { res.writeHead(400).end(); return; }
    let result;
    if (feed) {
      const upstream = await fetchPublic(url, feed.origin);
      result = new Response(upstream.body, {status:upstream.status, headers:{'Content-Type':'application/json','Cache-Control':'no-store'}});
    } else {
      const token = /^\/(?:schedule|classes(?:\/\d+)?|people\/\d+|ring\/[1-9]\d*)$/.test(new URL(request.url).pathname) ? await sourceToken() : SGL_TOKEN;
      result = await decoder.fetch(request, { SGL_TOKEN: token, SGL_COOKIE });
    }
    const body = Buffer.from(await result.arrayBuffer());
    res.writeHead(result.status, Object.fromEntries(result.headers));
    res.end(body);
  } catch (error) {
    const reason=decoder.diagnostic(error,{SGL_TOKEN,SGL_COOKIE,RUNNER_KEY,SGL_RETURNED_TOKEN:cachedToken?.token});
    res.writeHead(502,{'Content-Type':'application/json','Cache-Control':'no-store'}).end(JSON.stringify({message:'Runner request failed',reason,statusCode:502}));
  }
});
server.listen(18467, '127.0.0.1');
