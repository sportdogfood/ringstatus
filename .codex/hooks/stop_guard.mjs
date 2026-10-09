import { readStdin, repoRoot, loadJson, output } from './common.mjs';
import { evaluate, evaluateContinuation, paths, stateFor, event } from './reliability.mjs';
try {
  const input = await readStdin();
  const root = repoRoot(input.cwd);
  const disposition = String(input.last_assistant_message || '').trim().match(/^\*{0,2}Result:\s*(PASS|PARTIAL|FAIL|BLOCKED)\b/i)?.[1]?.toUpperCase();
  // A retained task is not a completion claim. Ordinary replies must not
  // consume its receipt or acquire its stale or malformed evidence status.
  const result = disposition ? evaluate(root, input) : { enrolled: false, ok: false, reasons: [] };
  const incomplete = ['PARTIAL', 'FAIL', 'BLOCKED'].includes(disposition);
  const continuation = incomplete ? evaluateContinuation(root, input) : { allowed: true, reasons: [] };
  const contract = loadJson(root, '.codex/control/task-contract.json');
  const writeContractApplies = contract.status === 'write_approved' && contract.verification_required
    && (!contract.owner_session_id || contract.owner_session_id === input.session_id);
  const governed = Boolean(disposition) && (result.enrolled || writeContractApplies);
  const p = paths(root, input.session_id);
  const state = stateFor(p, input.session_id);
  // Incomplete reporting ends a turn; it never certifies task completion.
  const allowed = !governed || (disposition === 'PASS' && result.ok) || (incomplete && continuation.allowed);
  const reasons = incomplete ? continuation.reasons : result.reasons;
  event(p, state, 'Stop', { turn_id: input.turn_id || null, disposition: disposition || null, verified: disposition === 'PASS' && result.ok, allowed, reasons });
  if (allowed) output({});
  else if (input.stop_hook_active) output({ continue: false,
    stopReason: 'RingStatus completion remains UNVERIFIED after one correction.',
    systemMessage: 'RingStatus: completion NOT VERIFIED. Required evidence or accurate result status is still missing.' });
  else output({ decision: 'block', reason: 'RingStatus completion check: ' + reasons.join('; ') +
    (incomplete ? '. Continue the recorded executable work within authorization; keep access restrictions and failed-runner stop boundaries.' : '') +
    '. Retain the full task. For an explicit completion report, use Result: PASS only after every requirement has current evidence; otherwise use Result: PARTIAL, FAIL or BLOCKED and state the remaining work. Ordinary conversation does not require a result label. Do not broaden scope or rerun a failed production workflow.' });
} catch (error) {
  output({ continue: false, stopReason: 'RingStatus control error', systemMessage: `RingStatus completion NOT VERIFIED: ${error.message}` });
}
