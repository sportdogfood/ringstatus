import { readStdin, repoRoot, loadJson, output } from './common.mjs';
import { evaluate, paths, stateFor, event } from './reliability.mjs';
try {
  const input = await readStdin();
  const root = repoRoot(input.cwd);
  const result = evaluate(root, input);
  const contract = loadJson(root, '.codex/control/task-contract.json');
  const governed = result.enrolled || (contract.status === 'write_approved' && contract.verification_required);
  const p = paths(root, input.session_id);
  const state = stateFor(p, input.session_id);
  const disposition = String(input.last_assistant_message || '').trim().match(/^\*{0,2}Result:\s*(PASS|PARTIAL|FAIL|BLOCKED)\b/i)?.[1]?.toUpperCase();
  // Incomplete reporting ends a turn; it never certifies task completion.
  const allowed = !governed || (disposition === 'PASS' && result.ok) || ['PARTIAL', 'FAIL', 'BLOCKED'].includes(disposition);
  event(p, state, 'Stop', { turn_id: input.turn_id || null, disposition: disposition || null, verified: result.ok, allowed, reasons: result.reasons });
  if (allowed) output({});
  else if (input.stop_hook_active) output({ continue: false,
    stopReason: 'RingStatus completion remains UNVERIFIED after one correction.',
    systemMessage: 'RingStatus: completion NOT VERIFIED. Required evidence or accurate result status is still missing.' });
  else output({ decision: 'block', reason: 'RingStatus completion check: ' + result.reasons.join('; ') +
    '. Retain the full task. Finalize with Result: PASS only after every requirement has current evidence; otherwise use Result: PARTIAL, FAIL or BLOCKED and state the remaining work. Do not broaden scope or rerun a failed production workflow.' });
} catch (error) {
  output({ continue: false, stopReason: 'RingStatus control error', systemMessage: `RingStatus completion NOT VERIFIED: ${error.message}` });
}
