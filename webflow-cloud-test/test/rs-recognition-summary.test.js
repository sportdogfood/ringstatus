import test from 'node:test';
import assert from 'node:assert/strict';
import { controlDatabase } from '../test-support/recognize-control-db.mjs';
import { recordRecognitionSummary } from '../src/lib/rs-recognition-session.js';

function fixture(t) {
  const db = controlDatabase(); t.after(() => db.close());
  const env = { AIRTABLE_TOKEN: 'test', AIRTABLE_RS_RECOGNITION_BASE_ID: 'app9kOZdIaGyKk5uG', RS_RECOGNITION_CONTROL_DB: db };
  let stored = null, writes = 0, hook = null;
  const fetchImpl = async (url, options = {}) => {
    assert.match(String(url), /tblBgvP7pYSv8o20j/);
    if (!options.method) return Response.json({ records: stored ? [stored] : [] });
    writes++;
    const record = JSON.parse(options.body).records[0];
    if (hook) return hook(record, options.method);
    stored = { id: 'recSummary00000001', fields: record.fields };
    return Response.json({ records: [stored] });
  };
  const args = { env, fetchImpl, sessionUid: 'tab_session', tokenFingerprint: 'a'.repeat(64),
    personRecordId: 'recxMolAW8UhI3Hph', deviceRecordId: 'rec0OtWNkYWs7iGgk', recognized: true, now: () => 1000 };
  return { args, db, fetchImpl, get stored() { return stored; }, set stored(v) { stored = v; }, get writes() { return writes; }, set hook(v) { hook = v; } };
}

for (const existing of [false, true]) {
  for (const networkError of [false, true]) {
    test(`failed pre-write ${networkError ? 'network' : 'HTTP'} lookup permits same-tab retry (${existing ? 'existing' : 'first'} summary)`, async t => {
      const f = fixture(t);
      const first = existing ? await recordRecognitionSummary(f.args) : null;
      const writesBefore = f.writes;
      const failedLookup = async (url, options = {}) => {
        assert.equal(options.method, undefined);
        if (networkError) throw new Error('temporary lookup failure');
        return Response.json({ error: 'unavailable' }, { status: 503 });
      };
      await assert.rejects(recordRecognitionSummary({ ...f.args, fetchImpl: failedLookup }));
      assert.equal(f.writes, writesBefore);
      const claim = await f.db.prepare('SELECT state, result_json FROM recognize_claims').bind().first();
      if (existing) {
        assert.equal(claim.state, 'complete');
        assert.equal(JSON.parse(claim.result_json).fields.started_at, first.started_at);
      } else assert.equal(claim, null);
      const retried = await recordRecognitionSummary(f.args);
      assert.equal(retried.session_uid, f.args.sessionUid);
      assert.equal(retried.status, 'active');
      assert.equal(f.writes, writesBefore + 1);
      if (existing) assert.equal(retried.started_at, first.started_at);
    });
  }
}

test('one summary reuses binding/start and monotonically advances activity despite an earlier clock', async t => {
  const f = fixture(t);
  const first = await recordRecognitionSummary(f.args);
  const next = await recordRecognitionSummary({ ...f.args, now: () => 1 });
  assert.equal(first.started_at, next.started_at);
  assert.equal(Date.parse(next.last_activity_at), Date.parse(first.last_activity_at) + 1);
  assert.equal(f.writes, 2); assert.equal(next.status, 'active');
  assert.deepEqual(f.stored.fields.person, [f.args.personRecordId]);
});

test('a concurrent contender never performs a second write while winner is pending', async t => {
  const f = fixture(t); let release, reached;
  const writing = new Promise(resolve => { reached = resolve; });
  f.hook = async record => { reached(); await new Promise(resolve => { release = resolve; }); f.stored = { id: 'recSummary00000001', fields: record.fields }; return Response.json({ records: [f.stored] }); };
  const winner = recordRecognitionSummary(f.args);
  await writing;
  await assert.rejects(recordRecognitionSummary(f.args), { code: 'session_summary_write_uncertain' });
  assert.equal(f.writes, 1);
  release(); await winner;
});

test('uncertain create remains claimed; retries reconcile by reading without replaying the write', async t => {
  const f = fixture(t);
  f.hook = record => { f.stored = { id: 'recSummary00000001', fields: record.fields }; throw new Error('response lost'); };
  await assert.rejects(recordRecognitionSummary(f.args), { code: 'session_summary_write_uncertain' });
  const recovered = await recordRecognitionSummary(f.args);
  assert.equal(recovered.status, 'active'); assert.equal(f.writes, 1);
});

test('an uncertain write with no matching provider row never retries POST', async t => {
  const f = fixture(t); f.hook = () => { throw new Error('timeout'); };
  await assert.rejects(recordRecognitionSummary(f.args), { code: 'session_summary_write_uncertain' });
  await assert.rejects(recordRecognitionSummary(f.args), { code: 'session_summary_write_uncertain' });
  assert.equal(f.writes, 1);
});

test('different token or person cannot close or rebind a guessed UID', async t => {
  const f = fixture(t); await recordRecognitionSummary(f.args);
  for (const args of [{ tokenFingerprint: 'b'.repeat(64), recognized: false }, { personRecordId: 'recAAAAAAAAAAAAAA' }]) {
    await assert.rejects(recordRecognitionSummary({ ...f.args, ...args }), { code: 'session_identity_conflict' });
  }
  assert.equal(f.writes, 1); assert.equal(f.stored.fields.status, 'active');
});

for (const [retired, status] of [[false, 'ended'], [true, 'revoked']]) {
  test(`server denial with prior matching token closes as ${status} and never reopens`, async t => {
    const f = fixture(t); await recordRecognitionSummary(f.args);
    const closed = await recordRecognitionSummary({ ...f.args, recognized: false, personRecordId: '', deviceRecordId: '', retired });
    assert.equal(closed.status, status);
    await assert.rejects(recordRecognitionSummary(f.args), { code: 'session_closed' });
    assert.equal(f.writes, 2);
  });
}

test('unbound unknown visitor creates no summary or claim', async t => {
  const f = fixture(t);
  assert.equal(await recordRecognitionSummary({ ...f.args, recognized: false }), null);
  assert.equal(f.writes, 0);
  const row = await f.db.prepare('SELECT COUNT(*) AS count FROM recognize_claims').bind().first();
  assert.equal(row.count, 0);
});

test('provider identity drift is rejected without PATCH', async t => {
  const f = fixture(t); await recordRecognitionSummary(f.args);
  f.stored.fields.device = ['recAAAAAAAAAAAAAA'];
  await assert.rejects(recordRecognitionSummary(f.args), { code: 'session_identity_conflict' });
  assert.equal(f.writes, 1);
});

test('uncertain closure reconciliation returns terminal status and cannot be mistaken for active recognition', async t => {
  const f = fixture(t); await recordRecognitionSummary(f.args);
  f.hook = record => { f.stored = { id: 'recSummary00000001', fields: record.fields }; throw new Error('response lost'); };
  await assert.rejects(recordRecognitionSummary({ ...f.args, recognized: false }), { code: 'session_summary_write_uncertain' });
  await assert.rejects(recordRecognitionSummary(f.args), { code: 'session_closed' });
  assert.equal(f.writes, 2);
});

test('terminal provider start drift is rejected without adopting it or writing again', async t => {
  const f = fixture(t); const first = await recordRecognitionSummary(f.args);
  f.stored.fields.status = 'ended';
  f.stored.fields.started_at = new Date(5000).toISOString();
  await assert.rejects(recordRecognitionSummary(f.args), { code: 'session_summary_invalid' });
  assert.equal(f.writes, 1);
  const row = await f.db.prepare('SELECT state, result_json FROM recognize_claims').bind().first();
  assert.equal(row.state, 'complete');
  assert.equal(JSON.parse(row.result_json).fields.started_at, first.started_at);
});

test('a denial after uncertain active write reconciles then closes, with no create replay', async t => {
  const f = fixture(t);
  f.hook = record => { f.stored = { id: 'recSummary00000001', fields: record.fields }; throw new Error('response lost'); };
  await assert.rejects(recordRecognitionSummary(f.args), { code: 'session_summary_write_uncertain' });
  f.hook = null;
  assert.equal((await recordRecognitionSummary({ ...f.args, recognized: false })).status, 'ended');
  assert.equal(f.writes, 2);
});
