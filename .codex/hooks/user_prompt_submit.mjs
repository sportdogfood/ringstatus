import { readStdin, repoRoot, loadJson, loadText, output } from './common.mjs';
import { observe, context } from './reliability.mjs';
try {
  const input = await readStdin();
  const root = repoRoot(input.cwd);
  observe(root, input);
  const policy = loadText(root, '.codex/control/policy.txt');
  const contract = loadJson(root, '.codex/control/task-contract.json');
  output({ hookSpecificOutput: { hookEventName: 'UserPromptSubmit', additionalContext:
    context(root, input) + '\n' + policy + '\nExisting write contract (separate from evidence checks):\n' + JSON.stringify(contract) } });
} catch (error) {
  output({ decision: 'block', reason: `RingStatus instruction control unavailable: ${error.message}. Restore the control files before dependent work.` });
}
