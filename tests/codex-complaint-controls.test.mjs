import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { execFileSync, spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { test } from 'node:test';
import { paths, save, hash, evaluate, evaluateContinuation, validateContract } from '../.codex/hooks/reliability.mjs';

// Replays bad records from complaint patterns. These are not live agent runs.
const source = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const root = fs.mkdtempSync(path.join(os.tmpdir(), 'rs-complaints-'));
process.on('exit', () => fs.rmSync(root, { recursive: true, force: true }));
execFileSync('git', ['init', '--quiet', root]);
fs.cpSync(path.join(source, '.codex'), path.join(root, '.codex'), { recursive: true });
save(path.join(root, '.codex/control/task-contract.json'), { status: 'read_only' });
const session = 'complaint-fixture';
const p = paths(root, session);
const artifact = path.join(root, 'observed-output.txt');
fs.writeFileSync(artifact, 'Fixture output; never production evidence.');
const ref = file => ({ path: file, sha256: hash(fs.readFileSync(file)) });
let contract, receipt, observation;
function setup(kind = 'rendered_visual') {
  contract = { version: 2, task_id: 'complaints', session_id: session,
    source_request: 'Verify the specified source and target, all required checks.',
    objective: 'Fixture only', target: 'draft-page', continuation_guard: true,
    requirements: [{ id: 'deliver', text: 'Verify source v24 on target', proof_level: 'live',
      proof: { kind, target: 'draft-page', source_version: 'v24', checks: ['desktop', 'mobile', 'intermediate-width'] } }] };
  observation = { kind, target: 'draft-page', source_version: 'v24',
    checks: contract.requirements[0].proof.checks.map(id => ({ id, status: 'PASS' })), artifacts: [ref(artifact)] };
  receipt = { task_id: contract.task_id, session_id: session, prompt_revision: 1,
    results: [{ id: 'deliver', status: 'PASS', proof_level: 'live', evidence: [] }] };
}
function write() {
  save(p.contract, contract);
  const baseline = hash(JSON.stringify(contract));
  save(p.state, { session_id: session, prompt_revision: 1, baseline, events: [] });
  receipt.contract_hash = baseline;
  const file = path.join(root, 'observations.json');
  save(file, observation);
  receipt.results[0].evidence = [ref(file)];
  save(p.receipt, receipt);
}
const verify = () => evaluate(root, { session_id: session });
const continuation = () => evaluateContinuation(root, { session_id: session });
function stop(message, active = false) {
  const result = spawnSync(process.execPath, [path.join(root, '.codex/hooks/stop_guard.mjs')], {
    cwd: root, encoding: 'utf8', input: JSON.stringify({ cwd: root, session_id: session,
      hook_event_name: 'Stop', last_assistant_message: message, stop_hook_active: active }) });
  assert.equal(result.status, 0, result.stderr);
  return JSON.parse(result.stdout);
}

test('C047: every fetched complaint is retained and has a control or explicit gap', () => {
  const data = JSON.parse(fs.readFileSync(path.join(source, '.codex/control/complaint-source-20261007.json')));
  const coverage = JSON.parse(fs.readFileSync(path.join(source, '.codex/control/complaint-coverage-20261007.json')));
  assert.equal(data.records.length, data.total);
  assert.equal(coverage.records.length, data.total);
  assert.equal(new Set(coverage.records.map(r => r.record_id)).size, data.total);
  for (const original of data.records) {
    const mapped = coverage.records.find(r => r.record_id === original.record_id);
    for (const key of ['record_id', 'name', 'complaint', 'recorded_solution']) assert.equal(mapped[key], original[key]);
    assert.ok(mapped.control && mapped.limitation && mapped.test);
    assert.ok(['partial', 'unresolved'].includes(mapped.coverage));
  }
});
test('C019/C020: matching typed records accepted at record-validation scope only', () => {
  setup(); write(); assert.equal(verify().ok, true);
});
test('Visual complaint: saved CSS is not a rendered visual observation', () => {
  setup(); observation.kind = 'stored_styles'; write();
  assert.equal(verify().ok, false); assert.match(verify().reasons.join(), /wrong evidence kind/);
});
test('C010: evidence from another target is rejected', () => {
  setup(); observation.target = 'source-page'; write(); assert.equal(verify().ok, false);
});
test('C062: v23 is not the requested saved v24', () => {
  setup(); observation.source_version = 'v23'; write(); assert.equal(verify().ok, false);
});
test('Viewport complaint: desktop alone cannot establish every required width', () => {
  setup(); observation.checks = observation.checks.slice(0, 1); write(); assert.equal(verify().ok, false);
});
test('Viewport complaint: one failed viewport prevents completion', () => {
  setup(); observation.checks[1].status = 'FAIL'; write(); assert.equal(verify().ok, false);
});
test('C019: duplicate observations do not replace a missing check', () => {
  setup(); observation.checks[1] = observation.checks[0]; write(); assert.equal(verify().ok, false);
});
test('C006: an observation without underlying artifacts is rejected', () => {
  setup(); observation.artifacts = []; write(); assert.equal(verify().ok, false);
});
test('C034: changed underlying artifact invalidates unchanged receipt', () => {
  setup(); write(); fs.appendFileSync(artifact, ' changed'); assert.equal(verify().ok, false);
});
test('C017: mocked runtime evidence is not native runtime evidence', () => {
  setup('native_runtime'); observation.kind = 'mock_test'; write(); assert.equal(verify().ok, false);
});
test('C018/C043: a manual run is not scheduled-run evidence', () => {
  setup('scheduled_run'); observation.trigger = 'manual'; write(); assert.equal(verify().ok, false);
});
test('C043: one run cannot satisfy a two-run cadence criterion', () => {
  setup('scheduled_run'); contract.requirements[0].proof.minimum_runs = 2;
  observation.trigger = 'schedule'; observation.run_ids = ['run-1', 'run-1']; write(); assert.equal(verify().ok, false);
});
test('C043: distinct scheduled runs meet the declared record criterion', () => {
  setup('scheduled_run'); contract.requirements[0].proof.minimum_runs = 2;
  observation.trigger = 'schedule'; observation.run_ids = ['run-1', 'run-2']; write(); assert.equal(verify().ok, true);
});
test('Tabs/media complaint: saved attributes are not interaction proof', () => {
  setup('interaction'); observation.kind = 'saved_attributes'; write(); assert.equal(verify().ok, false);
});
test('C054/C059: prepared handoff without delivery and acceptance is rejected', () => {
  setup('handoff'); contract.requirements[0].proof.checks = ['delivered', 'accepted'];
  observation.checks = [{ id: 'drafted', status: 'PASS' }]; write(); assert.equal(verify().ok, false);
});
test('C051: source-file availability does not satisfy discovery and invocation', () => {
  setup('capability'); contract.requirements[0].proof.checks = ['discovered', 'invoked'];
  observation.checks = [{ id: 'file-exists', status: 'PASS' }]; write(); assert.equal(verify().ok, false);
});
test('C046/C060/C061: incomplete report cannot stop with pending executable work', () => {
  setup(); receipt.results[0].status = 'PENDING'; receipt.results[0].next_action = 'Perform the already authorized source comparison'; write();
  assert.equal(continuation().allowed, false);
  assert.equal(stop('Result: PARTIAL').decision, 'block');
});
test('C062: a scoped browser blocker cannot hide an independent pending requirement', () => {
  setup(); receipt.results[0].status = 'BLOCKED';
  receipt.results[0].blocker = { kind: 'access', detail: 'Browser unavailable', evidence: [ref(artifact)] };
  contract.requirements.push({ id: 'inspect', text: 'Independent source inspection', proof_level: 'local' });
  receipt.results.push({ id: 'inspect', status: 'PENDING', next_action: 'Inspect accessible source' }); write();
  assert.equal(continuation().allowed, false);
});
test('C045: a specific evidenced blocker permits an accurate stop', () => {
  setup(); receipt.results[0].status = 'BLOCKED';
  receipt.results[0].blocker = { kind: 'access', detail: 'Only remaining check requires inaccessible browser', evidence: [ref(artifact)] }; write();
  assert.equal(continuation().allowed, true); assert.deepEqual(stop('Result: BLOCKED'), {});
});
test('C046: unsupported broad blocker is rejected', () => {
  setup(); receipt.results[0].status = 'BLOCKED'; receipt.results[0].blocker = { kind: 'access', detail: 'Cannot continue' }; write();
  assert.equal(continuation().allowed, false);
});
test('C019: incomplete report cannot hide invalid PASS evidence for other requirements', () => {
  setup(); observation.kind = 'saved_styles'; write();
  assert.equal(continuation().allowed, false);
});
test('Runner boundary: failed approved runner path stops; no alternate route requested', () => {
  setup(); receipt.results[0].status = 'PENDING'; receipt.results[0].next_action = 'Try direct endpoint';
  receipt.stop_reason = { kind: 'runner_failure', detail: 'Approved scheduled workflow failed', evidence: [ref(artifact)] }; write();
  assert.equal(continuation().allowed, true); assert.deepEqual(stop('Result: FAIL'), {});
});
test('User stop and missing authorization take precedence over unfinished work', () => {
  for (const kind of ['user_stop', 'authorization']) {
    setup(); receipt.results[0].status = 'PENDING';
    receipt.stop_reason = { kind, detail: 'Explicit stop boundary', evidence: [ref(artifact)] }; write();
    assert.equal(continuation().allowed, true);
  }
});
test('C009/C024: stale blocker record cannot authorize a new turn ending', () => {
  setup(); receipt.results[0].status = 'BLOCKED';
  receipt.results[0].blocker = { kind: 'capability', detail: 'Unavailable', evidence: [ref(artifact)] };
  receipt.prompt_revision = 0; write(); assert.equal(continuation().allowed, false);
});
test('No universal PARTIAL: ordinary answers never require the retained task receipt', () => {
  setup(); receipt.results[0].status = 'PENDING'; write();
  assert.deepEqual(stop('The count represents isolated test cases.'), {});
});
test('Bounded continuation cannot loop indefinitely', () => {
  setup(); receipt.results[0].status = 'PENDING'; write();
  assert.equal(stop('Result: PARTIAL').decision, 'block');
  assert.equal(stop('Result: PARTIAL', true).continue, false);
});
test('Malformed typed acceptance is rejected before binding', () => {
  setup(); contract.requirements[0].proof.checks = [];
  assert.throws(() => validateContract(contract), /Typed proof/);
});
test('Legacy contracts retain existing behavior until explicitly enrolled', () => {
  setup(); delete contract.continuation_guard; delete contract.requirements[0].proof;
  receipt.results[0].status = 'BLOCKED'; write();
  assert.deepEqual(stop('Result: BLOCKED'), {});
});
