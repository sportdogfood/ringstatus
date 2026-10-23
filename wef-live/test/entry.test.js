const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { scheduled } = require('../entry');

test('failed scheduled invocation logs errors and does not block the next invocation', async () => {
  const runtime = fs.mkdtempSync(path.join(os.tmpdir(), 'wef-live-test-'));
  const rows = [];
  let calls = 0, releases = 0;
  const options = { runtime, lock: () => () => releases++, api: { upsert: async (table, key, values) => rows.push(...values) },
    run: async () => ({ run_id: 'run-' + ++calls, completed: '2026-10-23T18:00:00Z', ok: calls > 1, errors: calls === 1 ? ['Source HTTP 502'] : [] }) };
  try {
    fs.writeFileSync(path.join(runtime, 'halt.json'), 'historical failure');
    assert.equal((await scheduled(options)).ok, false);
    assert.equal((await scheduled(options)).ok, true);
    assert.equal(calls, 2); assert.equal(releases, 2);
    assert.equal(rows[0].message, 'Source HTTP 502');
    assert.equal(fs.readFileSync(path.join(runtime, 'halt.json'), 'utf8'), 'historical failure');
  } finally { process.exitCode = 0; fs.rmSync(runtime, { recursive: true }); }
});

test('unexpected collection and Airtable logging failures retain local evidence and release lock', async () => {
  const runtime = fs.mkdtempSync(path.join(os.tmpdir(), 'wef-live-test-'));
  let released = false;
  try {
    const report = await scheduled({ runtime, lock: () => () => { released = true; },
      run: async () => { throw Error('collection unavailable'); },
      api: { upsert: async () => { throw Error('Airtable unavailable'); } } });
    assert.equal(report.ok, false); assert.equal(released, true);
    assert.match(fs.readFileSync(path.join(runtime, 'errs-log.jsonl'), 'utf8'), /collection unavailable/);
    assert.match(fs.readFileSync(path.join(runtime, 'last-run.json'), 'utf8'), /Airtable unavailable/);
    assert.equal(fs.existsSync(path.join(runtime, 'halt.json')), false);
  } finally { process.exitCode = 0; fs.rmSync(runtime, { recursive: true }); }
});
