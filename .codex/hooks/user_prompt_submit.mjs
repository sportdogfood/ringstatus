import { readStdin, repoRoot, loadJson, loadText, output } from "./common.mjs";

const input = await readStdin();
const root = repoRoot(input.cwd);
const contract = loadJson(root, ".codex/control/task-contract.json");
const policy = loadText(root, ".codex/control/policy.txt");

output({
  hookSpecificOutput: {
    hookEventName: "UserPromptSubmit",
    additionalContext:
      policy +
      "\nCurrent mechanical task contract:\n" +
      JSON.stringify(contract, null, 2) +
      "\nIf the current user request changes the task, do not infer a broader contract. Stay read-only until the contract is explicitly updated."
  }
});
