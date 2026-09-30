import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const repo = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const contractPath = path.join(repo, ".codex/control/task-contract.json");
const original = fs.readFileSync(contractPath, "utf8");

function run(script, payload) {
  const p = spawnSync(process.execPath, [path.join(repo, ".codex/hooks", script)], {
    cwd: repo,
    input: JSON.stringify(payload),
    encoding: "utf8"
  });
  assert.equal(p.status, 0, p.stderr);
  return p.stdout.trim() ? JSON.parse(p.stdout) : {};
}
function denied(out) {
  return out?.hookSpecificOutput?.permissionDecision === "deny";
}
function writeContract(obj) {
  fs.writeFileSync(contractPath, JSON.stringify(obj, null, 2) + "\n");
}

try {
  let out = run("pre_tool_use.mjs", {
    cwd: repo, hook_event_name:"PreToolUse", tool_name:"apply_patch",
    tool_input:{command:"*** Begin Patch\n*** Update File: src/a.js\n*** End Patch"}
  });
  assert.equal(denied(out), true, "read-only contract must block apply_patch");

  out = run("pre_tool_use.mjs", {
    cwd: repo, hook_event_name:"PreToolUse", tool_name:"Bash",
    tool_input:{command:"git status --short"}
  });
  assert.equal(denied(out), false, "read-only shell command should pass");

  out = run("pre_tool_use.mjs", {
    cwd: repo, hook_event_name:"PreToolUse", tool_name:"Bash",
    tool_input:{command:"rm -rf tmp"}
  });
  assert.equal(denied(out), true, "mutating shell must block in read-only mode");

  const c = JSON.parse(original);
  c.status = "write_approved";
  c.allowed_write_paths = ["src/allowed"];
  c.allowed_shell_write_patterns = ["^npm test$"];
  c.allowed_mcp_write_tools = ["mcp__Example__update_record"];
  writeContract(c);

  out = run("pre_tool_use.mjs", {
    cwd: repo, hook_event_name:"PreToolUse", tool_name:"apply_patch",
    tool_input:{command:"*** Begin Patch\n*** Update File: src/allowed/a.js\n*** End Patch"}
  });
  assert.equal(denied(out), false, "approved path should pass");

  out = run("pre_tool_use.mjs", {
    cwd: repo, hook_event_name:"PreToolUse", tool_name:"apply_patch",
    tool_input:{command:"*** Begin Patch\n*** Update File: src/not-allowed.js\n*** End Patch"}
  });
  assert.equal(denied(out), true, "unapproved path must block");

  out = run("pre_tool_use.mjs", {
    cwd: repo, hook_event_name:"PreToolUse", tool_name:"mcp__Example__update_record",
    tool_input:{id:"1"}
  });
  assert.equal(denied(out), false, "approved MCP write tool should pass");

  out = run("pre_tool_use.mjs", {
    cwd: repo, hook_event_name:"PreToolUse", tool_name:"mcp__Example__delete_record",
    tool_input:{id:"1"}
  });
  assert.equal(denied(out), true, "unapproved MCP write tool must block");

  out = run("stop_guard.mjs", {
    cwd: repo, hook_event_name:"Stop", stop_hook_active:false, last_assistant_message:"done"
  });
  assert.equal(out.decision, "block", "write task without verification receipt must continue");

  c.verification_receipt = {kind:"test", result:"PASS"};
  writeContract(c);
  out = run("stop_guard.mjs", {
    cwd: repo, hook_event_name:"Stop", stop_hook_active:false, last_assistant_message:"done"
  });
  assert.equal(out.decision, undefined, "verified write task may stop");

  out = run("user_prompt_submit.mjs", {
    cwd: repo, hook_event_name:"UserPromptSubmit", prompt:"fix this"
  });
  assert.match(out.hookSpecificOutput.additionalContext, /Do only the task requested/);

  console.log("PASS 9");
} finally {
  fs.writeFileSync(contractPath, original);
}
