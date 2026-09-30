import { readStdin, repoRoot, loadJson, output } from "./common.mjs";

const input = await readStdin();
const root = repoRoot(input.cwd);
const contract = loadJson(root, ".codex/control/task-contract.json");

if (input.stop_hook_active) {
  output({});
  process.exit(0);
}

if (contract.status === "write_approved" && contract.verification_required && !contract.verification_receipt) {
  output({
    decision: "block",
    reason:
      "RingStatus control: do not finalize this write task yet. The task contract requires verification evidence and no verification receipt is recorded. Run the approved verification only; do not broaden scope."
  });
  process.exit(0);
}

output({});
