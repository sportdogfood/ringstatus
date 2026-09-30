import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";

const repo = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const receipt = path.join(os.tmpdir(), "ringstatus-codex-hook-receipt.json");
try { fs.unlinkSync(receipt); } catch {}

const p = spawnSync(process.execPath, [path.join(repo, ".codex/hooks/session_start.mjs")], {
  cwd: repo,
  input: JSON.stringify({
    hook_event_name: "SessionStart",
    session_id: "test-session",
    source: "startup",
    cwd: repo
  }),
  encoding: "utf8"
});

assert.equal(p.status, 0, p.stderr);
assert.equal(fs.existsSync(receipt), true, "session-start receipt must be written");
const data = JSON.parse(fs.readFileSync(receipt, "utf8"));
assert.equal(data.hook_event_name, "SessionStart");
assert.equal(data.session_id, "test-session");
assert.equal(data.source, "startup");
assert.equal(path.resolve(data.cwd), repo);
const out = JSON.parse(p.stdout);
assert.match(out.hookSpecificOutput.additionalContext, /controls are active/);
console.log("PASS session-start");
try { fs.unlinkSync(receipt); } catch {}
