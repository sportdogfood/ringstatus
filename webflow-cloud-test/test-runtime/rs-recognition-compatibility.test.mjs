import test from 'node:test';
import assert from 'node:assert/strict';
import { fileURLToPath } from 'node:url';
import { readFile } from 'node:fs/promises';
import { build } from 'esbuild';
import { Miniflare } from 'miniflare';

// Real local Workers runtime, intercepted synthetic provider only. This is
// neither deployed HTTP/browser proof nor durable cross-isolate coordination.
const root = fileURLToPath(new URL('../', import.meta.url));
const runtimeConfig = JSON.parse(await readFile(new URL('../wrangler.json', import.meta.url), 'utf8'));
const { outputFiles } = await build({ stdin: { resolveDir: root, contents: `
import {POST,OPTIONS} from './src/pages/rs-recognition/action.js';
import {ALL as nativeEntry} from './src/pages/rs-inputs/native-recognition.js';
export default {fetch(request){return new URL(request.url).pathname.includes('/rs-inputs/native-recognition')?nativeEntry({request}):request.method==='OPTIONS'?OPTIONS():POST({request});}};
` }, bundle: true, format: 'esm', platform: 'browser', external: ['cloudflare:workers'], write: false });

test('Workers silent recognition renews cookie and records canonical Geo-IP once without an access cookie', async () => {
  const person = { id:'recSynthetic00001', fields:{person_uid:'synthetic',first_name:'Synthetic',person_name:'Synthetic Person',status:'Active',input_access:'approved'} };
  const token = '81a81b40-f954-4ad5-8911-8fc3bf91c4a1', events = [];
  const mf = new Miniflare({ modules:true, script:outputFiles[0].text,
    d1Databases:{RS_RECOGNITION_CONTROL_DB:'recognize-silent'},
    compatibilityDate:runtimeConfig.compatibility_date,compatibilityFlags:runtimeConfig.compatibility_flags,
    bindings:{AIRTABLE_TOKEN:'fixture',RS_INPUTS_RECOGNITION_BASE_ID:'app9kOZdIaGyKk5uG',RS_INPUTS_BASE_ID:'app9kOZdIaGyKk5uG',RS_INPUTS_WRITE_MODE:'isolated-trial',RS_RECOGNITION_SIGNAL_SECRET:'fixture-signal-secret'},
    outboundService:async request=>{
      const url=new URL(request.url);
      if(url.origin==='https://get.geojs.io')return Response.json({country_code:'US',region:'Florida',city:'Ocala'});
      assert.equal(url.origin,'https://api.airtable.com');
      assert.ok(url.pathname.startsWith('/v0/app9kOZdIaGyKk5uG/'));
      if(url.pathname.endsWith('/rs_devices_test'))return Response.json({records:[{id:'recSynthetic00002',fields:{device_token:token,status:'Active',person:[person.id]}}]});
      if(url.pathname.endsWith('/'+person.id))return Response.json(person);
      assert.ok(url.pathname.endsWith('/tblWjbASVMIjFLyW8'));
      if(request.method==='POST'){const body=await request.json();events.push(...body.records);return Response.json({records:[{id:'recSynthetic00003',fields:body.records[0].fields}]});}
      return Response.json({records:[]});
    }
  });
  try {
    await (await mf.getD1Database('RS_RECOGNITION_CONTROL_DB')).exec((await readFile(new URL('../migrations/recognize-control/0001_claims.sql',import.meta.url),'utf8')).replace(/--[^\n]*/g,'').replace(/\s+/g,' '));
    const call=(cookie)=>mf.dispatchFetch('https://example.invalid/test/rs-inputs/native-recognition?operation=recognize',{
      method:'POST',headers:{Origin:'https://example.invalid','Content-Type':'application/json','CF-Connecting-IP':'192.0.2.10',...(cookie?{Cookie:cookie}:{})},
      body:JSON.stringify({device_token:token,session_uid:'workers-silent-session',page_path:'/rs-recognize'})});
    const first=await call();assert.equal(first.status,200);assert.equal((await first.json()).recognized,true);
    const cookie=first.headers.get('Set-Cookie');assert.match(cookie,/Max-Age=31536000/);
    assert.equal((await call(cookie.split(';')[0])).status,200);assert.equal(events.length,1);
    assert.deepEqual(events[0].fields.person,[person.id]);assert.deepEqual(events[0].fields.device,['recSynthetic00002']);
    assert.equal(typeof events[0].fields.ip_hash,'string');assert.notEqual(events[0].fields.ip_hash,'192.0.2.10');
    assert.equal(events[0].fields.page_path,'/rs-recognize');
    person.fields.input_access='revoked';assert.equal((await (await call(cookie.split(';')[0])).json()).recognized,false);
  } finally {await mf.dispose();}
});

for (const native of [false, true]) {
  test(`Workers audit contract: ${native ? 'explicit native pending and safe retry' : 'existing caller error and safe retry'}`, async () => {
    let deviceWrites = 0, auditWrites = 0, failAudit = true;
    const mf = new Miniflare({ modules: true, script: outputFiles[0].text,
      d1Databases: { RS_RECOGNITION_CONTROL_DB: 'recognize-compatibility' },
      compatibilityDate: runtimeConfig.compatibility_date, compatibilityFlags: runtimeConfig.compatibility_flags,
      bindings: { AIRTABLE_TOKEN: 'fixture-token', RS_INPUTS_RECOGNITION_BASE_ID: 'app9kOZdIaGyKk5uG', RS_INPUTS_BASE_ID: 'app9kOZdIaGyKk5uG', RS_INPUTS_WRITE_MODE: 'isolated-trial', RS_INPUTS_SESSION_SECRET: 'ab'.repeat(32) },
      outboundService: async request => {
        const url = new URL(request.url), parts = url.pathname.split('/');
        assert.equal(url.origin, 'https://api.airtable.com');
        assert.equal(parts[2], 'app9kOZdIaGyKk5uG');
        assert.ok(['tblfkRSJAEMzuzApR', 'tblWjbASVMIjFLyW8'].includes(parts[3]));
        if (request.method === 'PATCH') { deviceWrites++; return Response.json({ records: [{ id: 'recSynthetic00001' }] }); }
        if (request.method === 'POST') {
          auditWrites++;
          return failAudit ? Response.json({ error: 'fixture-audit-failure' }, { status: 502 }) : Response.json({ records: [{ id: 'recSynthetic00002' }] });
        }
        return Response.json({ records: parts[3] === 'tblfkRSJAEMzuzApR' ? [{ id: 'recSynthetic00001' }] : auditWrites && !failAudit ? [{ id: 'recSynthetic00002' }] : [] });
      }
    });
    try {
      await (await mf.getD1Database('RS_RECOGNITION_CONTROL_DB')).exec((await readFile(new URL('../migrations/recognize-control/0001_claims.sql', import.meta.url), 'utf8')).replace(/--[^\n]*/g, '').replace(/\s+/g, ' '));
      const denied = await mf.dispatchFetch('https://example.invalid/test/rs-inputs/native-recognition?operation=device&device_token=synthetic-device');
      assert.equal(denied.status, 401);
      assert.equal((await denied.json()).error, 'authentication_required');
      const preflight = await mf.dispatchFetch('https://example.invalid/action', { method: 'OPTIONS' });
      assert.match(preflight.headers.get('Access-Control-Allow-Headers'), /X-RS-Audit-Outcome/);
      const options = { method: 'POST', headers: { 'Content-Type': 'application/json', ...(native ? { 'X-RS-Audit-Outcome': 'report' } : {}) }, body: JSON.stringify({ action: 'retire_device', device_token: 'synthetic-device', session_uid: 'synthetic-session', session_event_uid: 'synthetic-event' }) };
      const first = await mf.dispatchFetch('https://example.invalid/action', options);
      const firstBody = await first.json();
      assert.equal(first.status, native ? 200 : 502);
      if (native) assert.equal(firstBody.audit_status, 'pending');
      else assert.equal(firstBody.error, 'action_failed');
      failAudit = false;
      const retry = await mf.dispatchFetch('https://example.invalid/action', options);
      assert.equal(retry.status, 200);
      assert.equal((await retry.json()).audit_status, 'recorded');
      assert.equal(deviceWrites, 1);
      assert.equal(auditWrites, 1);
    } finally { await mf.dispose(); }
  });
}
