import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {spawnSync} from 'node:child_process';

const source = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const tempRoot = path.resolve(os.tmpdir());
const root = fs.mkdtempSync(path.join(tempRoot, 'rs-prompt-scope-'));
const control = path.join(root, '.codex/control');
const contract = {status: 'write_approved', owner_session_id: 'owner',
  objective: 'ONLY_OWNER_FEED_SMS_BARN_TASK', verification_required: true};
try {
  const init = spawnSync('git', ['init', '--quiet', root]);
  assert.equal(init.status, 0);
  fs.mkdirSync(control, {recursive: true});
  fs.writeFileSync(path.join(control, 'policy.txt'), 'GENERAL_POLICY_MUST_REMAIN');
  function run(session, value = contract) {
    fs.writeFileSync(path.join(control, 'task-contract.json'), JSON.stringify(value));
    const before = fs.readFileSync(path.join(control, 'task-contract.json'), 'utf8');
    const input = {cwd: root, hook_event_name: 'UserPromptSubmit', prompt: 'Unrelated work'};
    if (session !== undefined) input.session_id = session;
    const result = spawnSync(process.execPath, [path.join(source, '.codex/hooks/user_prompt_submit.mjs')],
      {cwd: root, encoding: 'utf8', input: JSON.stringify(input)});
    assert.equal(result.status, 0, result.stderr);
    assert.equal(fs.readFileSync(path.join(control, 'task-contract.json'), 'utf8'), before);
    const output = JSON.parse(result.stdout);
    if (session === undefined) {
      assert.equal(output.decision, 'block');
      assert.match(output.reason, /Missing session identity/);
      return result.stdout;
    }
    assert.ok(output.hookSpecificOutput, result.stdout);
    const text = output.hookSpecificOutput.additionalContext;
    assert.ok(text.includes('GENERAL_POLICY_MUST_REMAIN'));
    assert.ok(text.includes('RingStatus evidence control'));
    return text;
  }
  assert.ok(!run('unrelated-runner').includes(contract.objective), 'unrelated runner received another task');
  assert.ok(run('owner').includes(contract.objective), 'owner lost its intact write contract');
  assert.ok(!run('owner-extra').includes(contract.objective), 'partial owner match must not qualify');
  assert.ok(!run(undefined).includes(contract.objective), 'missing session must not receive task details');
  const unowned = {...contract}; delete unowned.owner_session_id;
  assert.ok(!run('unrelated-runner', unowned).includes(contract.objective), 'unowned contract must not broadcast');
  assert.ok(!run(undefined, unowned).includes(contract.objective), 'two missing owners must not match');
  console.log('PASS: six prompt ownership cases; policy/evidence context and contract file preserved');
} finally {
  const relative = path.relative(tempRoot, path.resolve(root));
  if (!relative.startsWith('rs-prompt-scope-') || relative.includes(path.sep) || path.isAbsolute(relative)) {
    throw new Error('Refusing cleanup outside the exact temporary test directory');
  }
  fs.rmSync(root, {recursive: true, force: true});
}
