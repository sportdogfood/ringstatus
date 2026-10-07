import { readStdin, repoRoot, output } from './common.mjs';
import { observe, context } from './reliability.mjs';
try {
  const input = await readStdin();
  const root = repoRoot(input.cwd);
  observe(root, input);
  output({ hookSpecificOutput: { hookEventName: 'SessionStart', additionalContext: context(root, input) } });
} catch (error) {
  output({ systemMessage: `RingStatus session control UNVERIFIED: ${error.message}` });
}
