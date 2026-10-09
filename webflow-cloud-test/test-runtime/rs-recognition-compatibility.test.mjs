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
