// RingStatus-specific evidence checks, not a semantic judge or security boundary.
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { execFileSync } from 'node:child_process';

export const hash = value => crypto.createHash('sha256').update(value).digest('hex');
export const read = file => JSON.parse(fs.readFileSync(file, 'utf8'));
export function save(file, value) {
  fs.mkdirSync(path.dirname(file), { recursive: true });
  const temp = `${file}.${crypto.randomUUID()}.tmp`;
  fs.writeFileSync(temp, JSON.stringify(value, null, 2) + '\n');
  fs.renameSync(temp, file);
}
export function paths(root, session) {
  if (!session || typeof session !== 'string') throw new Error('Missing session identity');
  const gitDir = execFileSync('git', ['rev-parse', '--absolute-git-dir'], { cwd: root, encoding: 'utf8' }).trim();
  const dir = path.join(gitDir, 'ringstatus-control', hash(session));
  return { dir, state: path.join(dir, 'state.json'), contract: path.join(dir, 'contract.json'), receipt: path.join(dir, 'receipt.json') };
}
export function stateFor(p, session) {
  return fs.existsSync(p.state) ? read(p.state) : { session_id: session, prompt_revision: 0, prompt_hashes: [], baseline: null, events: [] };
}
export function event(p, state, name, details = {}) {
  state.events.push({ event: name, at: new Date().toISOString(), ...details });
  save(p.state, state);
}
export function observe(root, input) {
  const p = paths(root, input.session_id);
  const state = stateFor(p, input.session_id);
  if (input.hook_event_name === 'UserPromptSubmit') {
    state.prompt_revision++;
    state.prompt_hashes.push(hash(String(input.prompt || '')));
  }
  event(p, state, input.hook_event_name, { turn_id: input.turn_id || null, source: input.source || null });
  return { p, state };
}
export function validateContract(c) {
  if (c.version !== 2 || !c.task_id || c.task_id === 'UNSET' || !c.session_id || !c.objective || !c.target || !c.source_request)
    throw new Error('Contract needs version 2, task/session identity, objective, target and source request');
  if (!Array.isArray(c.requirements) || !c.requirements.length) throw new Error('No acceptance requirements');
  if (c.continuation_guard !== undefined && typeof c.continuation_guard !== 'boolean') throw new Error('continuation_guard must be boolean');
  const ids = new Set();
  for (const r of c.requirements) {
    if (!r.id || ids.has(r.id) || !r.text || !['local', 'live', 'document'].includes(r.proof_level)) throw new Error('Invalid or duplicate requirement');
    ids.add(r.id);
    if (r.proof) {
      if (!['source_inspection', 'capability', 'rendered_visual', 'interaction', 'native_runtime', 'scheduled_run', 'workflow', 'handoff', 'behavioral_run'].includes(r.proof.kind) ||
          !r.proof.target || !Array.isArray(r.proof.checks) || !r.proof.checks.length ||
          r.proof.checks.some(check => typeof check !== 'string' || !check.trim()) || new Set(r.proof.checks).size !== r.proof.checks.length)
        throw new Error('Typed proof needs a supported kind, exact target and unique acceptance checks');
      if (r.proof.minimum_runs !== undefined && (!Number.isInteger(r.proof.minimum_runs) || r.proof.minimum_runs < 1))
        throw new Error('minimum_runs must be a positive integer');
    }
  }
}
function evidenceIntact(root, evidence) {
  try {
    if (!evidence?.path || !/^[a-f0-9]{64}$/.test(evidence.sha256 || '')) return false;
    const file = path.resolve(root, evidence.path);
    return fs.statSync(file).isFile() && hash(fs.readFileSync(file)) === evidence.sha256;
  } catch { return false; }
}
// Checks the submitted observation record, not the truth of its claims.
export function checkTypedProof(root, requirement, evidence) {
  if (!requirement.proof) return [];
  const expected = requirement.proof;
  try {
    if (!evidenceIntact(root, evidence)) return ['missing or changed observation record'];
    const observed = read(path.resolve(root, evidence.path));
    const reasons = [];
    if (observed.kind !== expected.kind) reasons.push('wrong evidence kind');
    if (observed.target !== expected.target) reasons.push('wrong target');
    if (expected.source_version !== undefined && observed.source_version !== expected.source_version) reasons.push('wrong source version');
    const checks = Array.isArray(observed.checks) ? observed.checks : [];
    if (new Set(checks.map(check => check.id)).size !== checks.length) reasons.push('duplicate observation checks');
    for (const check of expected.checks) {
      if (checks.find(item => item.id === check)?.status !== 'PASS') reasons.push(`unverified check: ${check}`);
    }
    if (checks.some(check => check.status !== 'PASS')) reasons.push('observation contains an unresolved check');
    if (!Array.isArray(observed.artifacts) || !observed.artifacts.length || !observed.artifacts.every(item => evidenceIntact(root, item)))
      reasons.push('missing or changed underlying artifacts');
    if (expected.kind === 'scheduled_run' && observed.trigger !== 'schedule') reasons.push('manual trigger is not scheduled-run proof');
    if (expected.minimum_runs !== undefined) {
      const runs = Array.isArray(observed.run_ids) ? observed.run_ids.filter(id => typeof id === 'string' && id.trim()) : [];
      if (new Set(runs).size < expected.minimum_runs) reasons.push('insufficient distinct runs');
    }
    return reasons;
  } catch { return ['typed proof needs a readable observation JSON record']; }
}
export function evaluate(root, input) {
  const p = paths(root, input.session_id);
  const state = stateFor(p, input.session_id);
  if (!fs.existsSync(p.contract)) return { enrolled: false, ok: false, reasons: ['No session-bound task contract'] };
  const c = read(p.contract);
  validateContract(c);
  const reasons = [];
  if (c.session_id !== input.session_id) reasons.push('Contract belongs to another session');
  if (!state.baseline || hash(JSON.stringify(c)) !== state.baseline) reasons.push('Recorded scope changed after binding');
  const receipt = fs.existsSync(p.receipt) ? read(p.receipt) : {};
  if (receipt.task_id !== c.task_id || receipt.session_id !== input.session_id || receipt.contract_hash !== state.baseline)
    reasons.push('Receipt is missing or belongs to another task, session or scope');
  if (receipt.prompt_revision !== state.prompt_revision) reasons.push('Receipt predates the latest user instruction');
  const results = Array.isArray(receipt.results) ? receipt.results : [];
  if (results.length !== c.requirements.length || new Set(results.map(r => r.id)).size !== results.length)
    reasons.push('Every requirement needs exactly one result');
  for (const requirement of c.requirements) {
    const result = results.find(r => r.id === requirement.id);
    if (result?.status !== 'PASS') { reasons.push(`${requirement.id}: not PASS`); continue; }
    if (result.proof_level !== requirement.proof_level) reasons.push(`${requirement.id}: wrong proof level`);
    if (!Array.isArray(result.evidence) || !result.evidence.length) reasons.push(`${requirement.id}: no evidence`);
    for (const evidence of result.evidence || []) {
      try {
        if (!evidence.path || !/^[a-f0-9]{64}$/.test(evidence.sha256 || '')) throw new Error();
        const file = path.resolve(root, evidence.path);
        if (!fs.statSync(file).isFile() || hash(fs.readFileSync(file)) !== evidence.sha256) throw new Error();
      } catch { reasons.push(`${requirement.id}: missing or changed evidence`); }
    }
    if (requirement.proof && !(result.evidence || []).some(evidence => checkTypedProof(root, requirement, evidence).length === 0))
      reasons.push(`${requirement.id}: no matching typed proof (${(result.evidence || []).flatMap(evidence => checkTypedProof(root, requirement, evidence)).join('; ') || 'missing observation'})`);
  }
  return { enrolled: true, ok: !reasons.length, reasons, task_id: c.task_id, prompt_revision: state.prompt_revision };
}
export function evaluateContinuation(root, input) {
  const p = paths(root, input.session_id);
  if (!fs.existsSync(p.contract)) return { guarded: false, allowed: true, reasons: [] };
  const c = read(p.contract);
  if (c.continuation_guard !== true) return { guarded: false, allowed: true, reasons: [] };
  validateContract(c);
  const state = stateFor(p, input.session_id);
  const receipt = fs.existsSync(p.receipt) ? read(p.receipt) : {};
  const reasons = [];
  if (c.session_id !== input.session_id || hash(JSON.stringify(c)) !== state.baseline ||
      receipt.session_id !== input.session_id || receipt.task_id !== c.task_id || receipt.contract_hash !== state.baseline || receipt.prompt_revision !== state.prompt_revision)
    reasons.push('incomplete report needs the current task record');
  const stop = receipt.stop_reason;
  // User stop, missing authorization and a failed approved runner route always
  // take precedence over continuation. No hook executes or retries work.
  if (!reasons.length && ['user_stop', 'authorization', 'runner_failure'].includes(stop?.kind) && stop.detail?.trim() &&
      Array.isArray(stop.evidence) && stop.evidence.length && stop.evidence.every(e => evidenceIntact(root, e)))
    return { guarded: true, allowed: true, reasons: [], stop_reason: stop.kind };
  const results = Array.isArray(receipt.results) ? receipt.results : [];
  if (results.length !== c.requirements.length || new Set(results.map(r => r.id)).size !== results.length)
    reasons.push('account for each remaining requirement');
  const completion = evaluate(root, input);
  for (const requirement of c.requirements) {
    const result = results.find(r => r.id === requirement.id);
    if (result?.status === 'PASS') {
      reasons.push(...completion.reasons.filter(reason => reason.startsWith(`${requirement.id}:`)));
      continue;
    }
    if (result?.status === 'PENDING' || result?.next_action?.trim()) {
      reasons.push(`${requirement.id}: executable work remains`); continue;
    }
    const blocker = result?.blocker;
    if (!['BLOCKED', 'FAIL'].includes(result?.status) || !blocker?.detail?.trim() ||
        !['access', 'dependency', 'owner_decision', 'authorization', 'runner_failure', 'capability'].includes(blocker.kind) ||
        !Array.isArray(blocker.evidence) || !blocker.evidence.length || !blocker.evidence.every(e => evidenceIntact(root, e)))
      reasons.push(`${requirement.id}: missing specific blocker evidence`);
  }
  return { guarded: true, allowed: !reasons.length, reasons };
}
export function context(root, input) {
  const p = paths(root, input.session_id);
  const state = stateFor(p, input.session_id);
  const bound = fs.existsSync(p.contract);
  return 'RingStatus evidence control v2. ' +
    (bound ? `A task record is retained at ${p.contract}. Preserve its full scope when working on or reporting completion of that task. Current instruction revision: ${state.prompt_revision}. ` :
      `No task is bound for session ${input.session_id}. For implementation/verification work, bind the literal request, target and full acceptance using node .codex/hooks/task-control.mjs bind ${input.session_id} CONTRACT.json before work. Discussion alone needs no contract. `) +
    'Ordinary answers, acknowledgments, clarification, research and handoffs do not require a Result label or a recap of an older unresolved task. Report the outcome of the current request clearly. ' +
    'Read .codex/control/RELIABILITY-CONTROLS.md when preparing task evidence. Only an explicit completion report for the retained task uses Result: PASS and requires current per-requirement evidence. ' +
    'In that report, if requirements remain, use Result: PARTIAL, FAIL or BLOCKED and identify them; do not replace missing work with a narrower success claim. ' +
    'After corrections to that task, reconcile it and refresh its receipt before claiming completion. Never add authorization or change acceptance to get a pass. ' +
    'Hook execution and evidence integrity do not establish semantic correctness or truth of evidence.';
}
