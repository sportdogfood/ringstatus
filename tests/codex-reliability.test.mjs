import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { execFileSync, spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { test } from 'node:test';
import { paths, read, save, hash } from '../.codex/hooks/reliability.mjs';

const source = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const scratch = fs.mkdtempSync(path.join(os.tmpdir(), 'rs-reliability-test-'));
process.on('exit', () => fs.rmSync(scratch, { recursive: true, force: true }));
function fixture(label) {
  const root = path.join(scratch, label);
  fs.mkdirSync(root, { recursive: true });
  execFileSync('git', ['init', '--quiet', root]);
  fs.cpSync(path.join(source, '.codex'), path.join(root, '.codex'), { recursive: true });
  return root;
}
function hook(root, script, input) {
  const p = spawnSync(process.execPath, [path.join(root, '.codex/hooks', script)], {
    cwd: root, encoding: 'utf8', input: JSON.stringify({ cwd: root, ...input })
  });
  assert.equal(p.status, 0, p.stderr);
  return JSON.parse(p.stdout);
}
function cli(root, ...args) {
  return spawnSync(process.execPath, [path.join(root, '.codex/hooks/task-control.mjs'), ...args], { cwd: root, encoding: 'utf8' });
}
for (let round = 1; round <= 3; round++) {
  test(`independent lifecycle ${round}`, async t => {
    const root = fixture(`round-${round}`);
    const session = `session-${round}`;
    const p = paths(root, session);
    const input = { session_id: session, hook_event_name: 'Stop', last_assistant_message: 'Result: PASS', stop_hook_active: false };
    const stop = extra => hook(root, 'stop_guard.mjs', { ...input, ...extra });
    const prompt = () => hook(root, 'user_prompt_submit.mjs', { session_id: session, hook_event_name: 'UserPromptSubmit', prompt: 'Preserve the full outcome; also verify cancellation.' });
    const contract = { version: 2, task_id: `task-${round}`, session_id: session, objective: 'Verify full fixture journey', target: 'isolated fixture',
      source_request: 'Test create and cancellation; no production changes.',
      requirements: [{ id: 'create', text: 'Create is persisted', proof_level: 'live' }, { id: 'cancel', text: 'Cancel preserves saved state', proof_level: 'live' }] };
    const contractFile = path.join(root, 'input-contract.json');
    save(contractFile, contract);
    await t.test('startup records only this session, without claiming all controls active', () => {
      const out = hook(root, 'session_start.mjs', { session_id: session, hook_event_name: 'SessionStart', source: 'startup' });
      assert.match(out.hookSpecificOutput.additionalContext, /No task is bound/);
      assert.equal(read(p.state).session_id, session);
      assert.doesNotMatch(out.hookSpecificOutput.additionalContext, /controls are active/);
    });
    await t.test('binding uses a distinct contract instead of shared UNSET state', () => {
      prompt();
      assert.equal(cli(root, 'bind', session, contractFile).status, 0);
      assert.equal(read(path.join(root, '.codex/control/task-contract.json')).task_id, 'UNSET');
    });
    await t.test('missing receipt blocks completion', () => assert.equal(stop().decision, 'block'));
    await t.test('scope cannot be amended without a newer instruction', () => {
      save(contractFile, { ...contract, source_prompt_revision: read(p.state).prompt_revision, change_reason: 'Unsupported same-turn change' });
      assert.equal(cli(root, 'amend', session, contractFile).status, 1);
      assert.deepEqual(read(p.contract), contract);
    });
    await t.test('truthy PASS object cannot certify anything', () => {
      save(p.receipt, { result: 'PASS' });
      assert.equal(stop().decision, 'block');
    });
    const evidenceFile = path.join(root, 'evidence.json');
    fs.writeFileSync(evidenceFile, '{"create":"observed","cancel":"observed"}');
    const receipt = () => ({ task_id: contract.task_id, session_id: session, contract_hash: read(p.state).baseline,
      prompt_revision: read(p.state).prompt_revision,
      results: contract.requirements.map(r => ({ id: r.id, status: 'PASS', proof_level: r.proof_level,
        evidence: [{ path: evidenceFile, sha256: hash(fs.readFileSync(evidenceFile)) }] })) });
    await t.test('partial coverage cannot become overall PASS', () => {
      const r = receipt(); r.results.pop(); save(p.receipt, r); assert.equal(stop().decision, 'block');
    });
    await t.test('failed requirement blocks overall PASS', () => {
      const r = receipt(); r.results[1].status = 'FAIL'; save(p.receipt, r); assert.equal(stop().decision, 'block');
    });
    await t.test('local evidence cannot satisfy required live proof', () => {
      const r = receipt(); r.results[1].proof_level = 'local'; save(p.receipt, r); assert.equal(stop().decision, 'block');
    });
    await t.test('wrong task or session receipt fails', () => {
      for (const key of ['task_id', 'session_id', 'contract_hash']) {
        const r = receipt(); r[key] = 'other'; save(p.receipt, r); assert.equal(stop().decision, 'block');
      }
    });
    await t.test('duplicate results cannot replace omitted requirement', () => {
      const r = receipt(); r.results[1] = r.results[0]; save(p.receipt, r); assert.equal(stop().decision, 'block');
    });
    await t.test('matching complete evidence passes', () => { save(p.receipt, receipt()); assert.deepEqual(stop(), {}); });
    await t.test('resume preserves requirements and evidence', () => {
      hook(root, 'session_start.mjs', { session_id: session, hook_event_name: 'SessionStart', source: 'resume' });
      assert.deepEqual(read(p.contract), contract); assert.deepEqual(stop(), {});
    });
    await t.test('new user correction invalidates earlier sign-off', () => {
      prompt(); assert.equal(stop().decision, 'block'); assert.deepEqual(read(p.contract), contract);
    });
    await t.test('updated current receipt may pass', () => { save(p.receipt, receipt()); assert.deepEqual(stop(), {}); });
    await t.test('changed evidence invalidates sign-off', () => {
      fs.appendFileSync(evidenceFile, ' changed'); assert.equal(stop().decision, 'block'); save(p.receipt, receipt());
    });
    await t.test('deleted evidence invalidates sign-off', () => {
      const r = receipt(); r.results[0].evidence[0].path += '.missing'; save(p.receipt, r); assert.equal(stop().decision, 'block');
    });
    await t.test('silent narrowing of scope is detected', () => {
      const altered = structuredClone(contract); altered.requirements.pop(); save(p.contract, altered);
      save(p.receipt, receipt()); assert.equal(stop().decision, 'block'); save(p.contract, contract);
    });
    await t.test('another session cannot consume this contract or state', () => {
      hook(root, 'session_start.mjs', { session_id: 'other', hook_event_name: 'SessionStart', source: 'startup' });
      assert.equal(fs.existsSync(paths(root, 'other').contract), false);
      assert.notEqual(paths(root, 'other').state, p.state);
    });
    await t.test('blocked reporting is allowed without false completion', () => {
      fs.rmSync(p.receipt); assert.deepEqual(stop({ last_assistant_message: 'Result: BLOCKED. Live evidence unavailable.' }), {});
      const last = read(p.state).events.at(-1); assert.equal(last.verified, false); assert.equal(last.disposition, 'BLOCKED');
    });
    await t.test('one failed correction halts without silently certifying', () => {
      const out = stop({ stop_hook_active: true }); assert.equal(out.continue, false); assert.match(out.systemMessage, /NOT VERIFIED/);
    });
    await t.test('corrupt receipt yields an explicit control error', () => {
      fs.writeFileSync(p.receipt, 'invalid json'); const out = stop(); assert.equal(out.continue, false); assert.match(out.systemMessage, /NOT VERIFIED/);
    });
    await t.test('amend requires a new instruction and preserves original', () => {
      fs.rmSync(p.receipt); const amended = { ...contract, source_prompt_revision: read(p.state).prompt_revision, change_reason: 'New user instruction adds a documented condition' };
      save(contractFile, amended); assert.equal(cli(root, 'amend', session, contractFile).status, 0);
      assert.ok(fs.readdirSync(p.dir).some(n => n.startsWith('contract.json.revision-')));
      assert.equal(cli(root, 'amend', session, contractFile).status, 1);
    });
    await t.test('explicit new task archives prior scope and rejects prior evidence', () => {
      prompt();
      save(contractFile, { ...contract, task_id: `replacement-${round}`, source_prompt_revision: read(p.state).prompt_revision, change_reason: 'Explicit new test goal' });
      assert.equal(cli(root, 'new-task', session, contractFile).status, 0);
      assert.ok(fs.readdirSync(p.dir).some(n => n.startsWith('contract.json.previous-')));
      assert.equal(stop().decision, 'block');
    });
  });
}
