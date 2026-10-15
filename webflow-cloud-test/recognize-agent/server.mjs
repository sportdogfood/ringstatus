import http from 'node:http';
import { randomBytes, timingSafeEqual } from 'node:crypto';
import { readFile, writeFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import { safeState, redactMessage, validateDecision, instructions, schema } from './policy.mjs';

const dir = dirname(fileURLToPath(import.meta.url));
const port = 43187;
const key = process.env.OPENAI_API_KEY;
if (!key) throw new Error('OPENAI_API_KEY is missing; no secret file is required.');
let pairing;
try { pairing = JSON.parse(await readFile(join(dir,'extension/local-config.json'),'utf8')); }
catch { pairing = {url:`http://127.0.0.1:${port}`, token:randomBytes(32).toString('hex')};
  await writeFile(join(dir,'extension/local-config.json'), JSON.stringify(pairing,null,2)); }
const metrics = {started_at:new Date().toISOString(), model:'gpt-5.4-mini', calls:0, input_tokens:0, output_tokens:0, estimated_usd:0};
let active = false;
const send = (res,status,body) => { res.writeHead(status,{'Content-Type':'application/json','Cache-Control':'no-store'}); res.end(JSON.stringify(body)); };
function authorized(req) {
  if (req.headers.host !== `127.0.0.1:${port}`) return false;
  const origin = req.headers.origin;
  if (origin && !/^chrome-extension:\/\/[a-p]{32}$/.test(origin)) return false;
  const actual = Buffer.from(String(req.headers['x-recognize-test-token'] || ''));
  const expected = Buffer.from(pairing.token);
  return actual.length === expected.length && timingSafeEqual(actual,expected);
}
const server = http.createServer(async (req,res) => {
  if (req.url === '/health' && req.method === 'GET') {
    if (req.headers.host !== `127.0.0.1:${port}`) return send(res,403,{ok:false});
    return send(res,200,{ok:true,...metrics, busy:active});
  }
  if (!authorized(req)) return send(res,403,{ok:false,error:'test_pairing_denied'});
  if (req.method !== 'POST' || req.url !== '/agent') return send(res,404,{ok:false,error:'not_found'});
  if (active) return send(res,409,{ok:false,error:'agent_busy'});
  if (metrics.calls >= 50 || metrics.estimated_usd >= 1) return send(res,429,{ok:false,error:'test_usage_limit'});
  active = true;
  try {
    let raw = '';
    for await (const chunk of req) { raw += chunk; if (Buffer.byteLength(raw) > 12000) throw new Error('request_too_large'); }
    const input = JSON.parse(raw);
    const state = safeState(input.state);
    const history = (Array.isArray(input.history) ? input.history : []).slice(-6).map(item => ({role:item.role === 'assistant' ? 'assistant':'user',content:redactMessage(item.content)}));
    const response = await fetch('https://api.openai.com/v1/chat/completions', {
      method:'POST', headers:{Authorization:`Bearer ${key}`,'Content-Type':'application/json'}, signal:AbortSignal.timeout(45000),
      body:JSON.stringify({model:metrics.model,reasoning_effort:'none',max_completion_tokens:450,store:false,
        messages:[{role:'system',content:instructions},...history,{role:'user',content:JSON.stringify({message:redactMessage(input.message),page:state})}],
        response_format:{type:'json_schema',json_schema:{name:'recognize_decision',strict:true,schema}}})
    });
    metrics.calls++;
    if (!response.ok) { await response.body?.cancel(); return send(res,502,{ok:false,error:`model_http_${response.status}`}); }
    const data = await response.json();
    metrics.input_tokens += data.usage?.prompt_tokens || 0;
    metrics.output_tokens += data.usage?.completion_tokens || 0;
    metrics.estimated_usd = +(metrics.input_tokens * .75 / 1e6 + metrics.output_tokens * 4.5 / 1e6).toFixed(6);
    const decision = validateDecision(JSON.parse(data.choices?.[0]?.message?.content || '{}'), state);
    await writeFile(join(dir,'usage.json'),JSON.stringify(metrics,null,2));
    return send(res,200,{ok:true,decision,usage:{...metrics}});
  } catch { return send(res,502,{ok:false,error:'agent_request_failed'}); }
  finally { active = false; }
});
server.listen(port,'127.0.0.1',() => console.log(`Recognize test agent listening at http://127.0.0.1:${port}; model ${metrics.model}.`));
