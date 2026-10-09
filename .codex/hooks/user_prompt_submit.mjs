import { readStdin, repoRoot, loadJson, loadText, output } from './common.mjs';
import { observe, context } from './reliability.mjs';
try {
  const input = await readStdin();
  const root = repoRoot(input.cwd);
  observe(root, input);
  const policy = loadText(root, '.codex/control/policy.txt');
  const contract = loadJson(root, '.codex/control/task-contract.json');
  // Task-specific instructions belong only to their explicitly identified owner.
  // Missing ownership must not broadcast a legacy task to new runners.
  const ownsContract = typeof input.session_id === 'string' && input.session_id.length > 0
    && contract.owner_session_id === input.session_id;
  const writeContext = ownsContract
    ? '\nExisting write contract (separate from evidence checks):\n' + JSON.stringify(contract)
    : '';
  output({ hookSpecificOutput: { hookEventName: 'UserPromptSubmit', additionalContext:
    context(root, input) + '\n' + policy + writeContext } });
} catch (error) {
  output({ decision: 'block', reason: `RingStatus instruction control unavailable: ${error.message}. Restore the control files before dependent work.` });
}
