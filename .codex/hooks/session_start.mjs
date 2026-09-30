import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { readStdin, output } from "./common.mjs";

const input = await readStdin();
const receiptPath = path.join(os.tmpdir(), "ringstatus-codex-hook-receipt.json");
const receipt = {
  hook_event_name: input.hook_event_name || "SessionStart",
  session_id: input.session_id || null,
  source: input.source || null,
  cwd: input.cwd || process.cwd(),
  created_at: new Date().toISOString()
};

fs.writeFileSync(receiptPath, JSON.stringify(receipt, null, 2) + "\n", "utf8");

output({
  hookSpecificOutput: {
    hookEventName: "SessionStart",
    additionalContext: "RingStatus project controls are active for this Codex session."
  }
});
