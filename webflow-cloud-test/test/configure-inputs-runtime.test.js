import test from 'node:test';
import assert from 'node:assert/strict';
import { configure, TARGET } from '../scripts/configure-inputs-runtime.mjs';

function fixture({ app = {}, environment = {}, variables = [], nextCursor = null } = {}) {
  const calls = [];
  const state = [{ key: 'AIRTABLE_TOKEN', isSecret: true }, ...variables];
  const run = async (args, input) => {
    calls.push({ args, input });
    if (args[1] === 'get') return { app: { id: TARGET.appId, siteId: TARGET.siteId, appPath: 'webflow-cloud-test', ...app } };
    if (args[1] === 'environments') return { environments: [{ id: TARGET.environmentId, branch: TARGET.branch, mount: TARGET.mount, ...environment }] };
    if (args[2] === 'list') return { variables: state, nextCursor };
    assert.equal(args[2], 'set');
    const key = args[3];
    const row = { key, isSecret: args.includes('--secret'), ...(!args.includes('--secret') ? { value: input } : {}) };
    const index = state.findIndex(item => item.key === key);
    if (index === -1) state.push(row); else state[index] = row;
    return { success: true };
  };
  return { run, calls, state, writes: () => calls.filter(call => call.args[2] === 'set') };
}

test('offline preview never calls CLI or generates a secret', async () => {
  const result = await configure({ run: () => assert.fail(), generateSecret: () => assert.fail(), env: {} });
  assert.equal(result.mode, 'dry-run');
  assert.equal(result.target.mount, '/test');
});

for (const override of [{ app: { siteId: 'different-site' } }, { environment: { mount: '/' } }, { environment: { branch: 'different-branch' } }]) {
  test(`wrong target refuses all writes ${JSON.stringify(override)}`, async () => {
    const f = fixture(override);
    await assert.rejects(configure({ apply: true, env: {}, run: f.run }), /target/);
    assert.equal(f.writes().length, 0);
  });
}

test('CRM missing credential preflight makes no writes', async () => {
  const f = fixture();
  await assert.rejects(configure({ apply: true, env: { RS_INPUTS_BARN_STORAGE: 'zoho-crm' }, run: f.run }), /ZOHO_CLIENT_ID/);
  assert.equal(f.writes().length, 0);
});

test('session generated once, then preserved; no value enters argv or result', async () => {
  const f = fixture();
  const secret = 'ab'.repeat(32);
  let generated = 0;
  const first = await configure({ apply: true, env: {}, run: f.run, generateSecret: () => { generated++; return secret; } });
  const second = await configure({ apply: true, env: {}, run: f.run, generateSecret: () => { generated++; return secret; } });
  assert.equal(generated, 1);
  assert.equal(f.writes().filter(call => call.args[3] === 'RS_INPUTS_SESSION_SECRET').length, 1);
  assert.equal(f.writes().filter(call => call.args[3] === 'AIRTABLE_TOKEN').length, 0);
  assert.equal(JSON.stringify(f.calls.map(call => call.args)).includes(secret), false);
  assert.equal(JSON.stringify([first, second]).includes(secret), false);
  assert.equal(f.writes().find(call => call.args[3] === 'RS_INPUTS_SESSION_SECRET').input, secret);
});

test('CRM secrets go through stdin with secret flag and only approved keys change', async () => {
  const f = fixture({ variables: [{ key: 'UNRELATED', isSecret: true }] });
  const env = { RS_INPUTS_BARN_STORAGE: 'zoho-crm', ZOHO_CLIENT_ID: 'id-private', ZOHO_CLIENT_SECRET: 'secret-private', ZOHO_REFRESH_TOKEN: 'refresh-private' };
  await configure({ apply: true, env, run: f.run });
  for (const key of ['ZOHO_CLIENT_ID', 'ZOHO_CLIENT_SECRET', 'ZOHO_REFRESH_TOKEN']) {
    const call = f.writes().find(call => call.args[3] === key);
    assert.equal(call.input, env[key]);
    assert.ok(call.args.includes('--secret'));
    assert.equal(call.args.includes(env[key]), false);
  }
  assert.equal(f.writes().some(call => call.args[3] === 'UNRELATED'), false);
  assert.equal(f.calls.some(call => call.args.includes('deploy') || call.args.includes('login')), false);
});

test('unprotected existing credential and paginated metadata block writes', async () => {
  for (const config of [{ variables: [{ key: 'RS_INPUTS_SESSION_SECRET', isSecret: false }] }, { nextCursor: 'next' }]) {
    const f = fixture(config);
    await assert.rejects(configure({ apply: true, env: {}, run: f.run }), /secret protection|Incomplete/);
    assert.equal(f.writes().length, 0);
  }
});

test('failed readback cannot report configuration complete', async () => {
  const f = fixture();
  let reads = 0;
  const run = async (args, input) => {
    if (args[1] === 'env-vars' && args[2] === 'list' && ++reads === 2) return { variables: [] };
    return f.run(args, input);
  };
  await assert.rejects(configure({ apply: true, env: {}, run }), /readback failed/);
});

test('generated signing key satisfies exact access signing contract', async () => {
  const f = fixture();
  await configure({ apply: true, env: {}, run: f.run });
  assert.match(f.writes().find(call => call.args[3] === 'RS_INPUTS_SESSION_SECRET').input, /^[a-fA-F0-9]{64}$/);
});

test('invalid supplied signing key fails before any writes', async () => {
  const f = fixture();
  await assert.rejects(configure({ apply: true, env: { RS_INPUTS_SESSION_SECRET: 'z'.repeat(64) }, run: f.run }), /64 hexadecimal/);
  assert.equal(f.writes().length, 0);
});

test('wrong public value readback cannot report configuration complete', async () => {
  const f = fixture();
  let reads = 0;
  const run = async (args, input) => {
    const result = await f.run(args, input);
    if (args[1] === 'env-vars' && args[2] === 'list' && ++reads === 2) {
      result.variables.find(row => row.key === 'RS_INPUTS_BASE_ID').value = 'wrong';
    }
    return result;
  };
  await assert.rejects(configure({ apply: true, env: {}, run }), /Public configuration readback failed/);
});
