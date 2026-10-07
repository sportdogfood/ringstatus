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
  const ids = new Set();
  for (const r of c.requirements) {
    if (!r.id || ids.has(r.id) || !r.text || !['local', 'live', 'document'].includes(r.proof_level)) throw new Error('Invalid or duplicate requirement');
    ids.add(r.id);
  }
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
  }
  return { enrolled: true, ok: !reasons.length, reasons, task_id: c.task_id, prompt_revision: state.prompt_revision };
}
export function context(root, input) {
  const p = paths(root, input.session_id);
  const state = stateFor(p, input.session_id);
  const bound = fs.existsSync(p.contract);
  return 'RingStatus evidence control v2. ' +
    (bound ? `Read the bound task at ${p.contract}. Preserve its full scope. Current instruction revision: ${state.prompt_revision}. ` :
      `No task is bound for session ${input.session_id}. For implementation/verification work, bind the literal request, target and full acceptance using node .codex/hooks/task-control.mjs bind ${input.session_id} CONTRACT.json before work. Discussion alone needs no contract. `) +
    'Read .codex/control/RELIABILITY-CONTROLS.md for the record schema. For bound tasks, a successful final answer starts with Result: PASS and requires current per-requirement evidence. ' +
    'If anything remains, report Result: PARTIAL, FAIL or BLOCKED and identify it; do not replace missing work with a narrower success claim. ' +
    'After user corrections, reconcile the existing task and refresh its receipt. Never add authorization or change acceptance to get a pass. ' +
    'Hook execution and evidence integrity do not establish semantic correctness or truth of evidence.';
}
