import test from 'node:test';
import assert from 'node:assert/strict';
import { build } from 'esbuild';
import { Miniflare } from 'miniflare';
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';

test('Workers/D1 random OTP: shared atomic consumption, five guesses, expiry, send limit and no stored plaintext', async () => {
  const root = fileURLToPath(new URL('../', import.meta.url));
  const { outputFiles } = await build({ stdin: { resolveDir: root, contents: `
import {env} from 'cloudflare:workers';
import {createOtpStore} from './src/lib/rs-recognition-otp-store.js';
export default {async fetch(request){try{const p=await request.json();const s=createOtpStore(env,()=>p.now);return Response.json({ok:true,result:await s[p.op](...p.args)});}catch(e){return Response.json({ok:false,error:e.code},{status:e.status||500});}}};
` }, bundle: true, format: 'esm', platform: 'browser', external: ['cloudflare:workers'], write: false });
  const worker = name => ({ name, modules: true, script: outputFiles[0].text, compatibilityDate: '2026-06-01', compatibilityFlags: ['nodejs_compat'],
    d1Databases: { RS_RECOGNITION_CONTROL_DB: 'otp-shared-proof' }, bindings: { RS_INPUTS_SESSION_SECRET: 'ab'.repeat(32) } });
  const mf = new Miniflare({ workers: [worker('one'), worker('two')] });
  try {
    const db = await mf.getD1Database('RS_RECOGNITION_CONTROL_DB', 'one');
    await db.exec((await readFile(new URL('../migrations/recognize-control/0001_claims.sql', import.meta.url), 'utf8')).replace(/--[^\n]*/g, '').replace(/\s+/g, ' '));
    const one = await mf.getWorker('one'), two = await mf.getWorker('two');
    const call = async (w, op, args, now = 1000000) => {
      const response = await w.fetch('https://example.invalid/', { method: 'POST', body: JSON.stringify({ op, args, now }) });
      return { status: response.status, ...await response.json() };
    };
    const phone = '+12025550199';
    await call(one, 'start', ['one', phone]);
    const code = (await call(one, 'prepare', ['one', phone])).result;
    assert.match(code, /^\d{6}$/);
    assert.equal((await call(two, 'consume', ['one', '+12025550198', code])).result.verified, false);
    const parallel = await Promise.all([call(one, 'consume', ['one', phone, code]), call(two, 'consume', ['one', phone, code])]);
    assert.equal(parallel.filter(r => r.result.verified).length, 1);
    assert.equal((await call(two, 'consume', ['one', phone, code])).result.expired, true);
    await call(one, 'start', ['two', phone]);
    const next = (await call(one, 'prepare', ['two', phone])).result;
    const wrong = next === '000000' ? '000001' : '000000';
    const guesses = await Promise.all(Array.from({ length: 8 }, (_, i) => call(i % 2 ? one : two, 'consume', ['two', phone, wrong])));
    assert.equal(guesses.filter(r => !r.result.expired).length, 5);
    assert.equal((await call(one, 'consume', ['two', phone, next])).result.verified, false);
    await call(one, 'start', ['three', phone]);
    const last = (await call(one, 'prepare', ['three', phone])).result;
    assert.equal((await call(two, 'consume', ['three', phone, last], 1600001)).result.expired, true);
    assert.equal((await call(two, 'start', ['four', phone])).status, 429);
    const rows = await db.prepare('SELECT result_json FROM recognize_claims').all();
    for (const row of rows.results) {
      const value = JSON.parse(row.result_json);
      assert.equal(Object.hasOwn(value, 'code'), false); assert.equal(Object.hasOwn(value, 'phone'), false);
      if (value.hash) assert.match(value.hash, /^[a-f0-9]{64}$/);
    }
  } finally { await mf.dispose(); }
});
