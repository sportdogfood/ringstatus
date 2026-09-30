import {
  readStdin, repoRoot, loadJson, deny, output,
  extractPatchPaths, isPathAllowed, looksMutatingShell, matchesAny
} from "./common.mjs";

const input = await readStdin();
const root = repoRoot(input.cwd);
const contract = loadJson(root, ".codex/control/task-contract.json");
const tool = String(input.tool_name || "");
const ti = input.tool_input || {};

const writeAuthorized = contract.status === "write_approved";
const shellLike = /^(?:Bash|shell|shell_command|exec_command)$/i.test(tool);
const patchLike = /^(?:apply_patch|Edit|Write)$/i.test(tool);
const mutatingToolName = /(?:^|_)(?:create|write|edit|update|delete|remove|rename|move|copy|mkdir|rmdir|touch|patch|replace|append|save|publish|deploy|send|reply|comment|merge|close|approve|reject|upload|set)(?:_|$)/i.test(tool);
const readOnlyToolName = /(?:^|_)(?:read|get|list|find|search|grep|view|inspect|status|diff|fetch|show|query)(?:_|$)/i.test(tool);

if (patchLike || tool === "apply_patch") {
  const paths = extractPatchPaths(ti.command);
  if (!writeAuthorized) {
    deny("RingStatus control: repository writes are not authorized for the current task contract.");
    process.exit(0);
  }
  if (!paths.length) {
    deny("RingStatus control: could not verify patch target paths; refusing an unscoped write.");
    process.exit(0);
  }
  const bad = paths.filter(p => !isPathAllowed(p, contract.allowed_write_paths || []));
  if (bad.length) {
    deny(`RingStatus control: write target outside approved scope: ${bad.join(", ")}`);
    process.exit(0);
  }
  output({});
  process.exit(0);
}

if (shellLike) {
  const cmd = String(ti.command || ti.cmd || ti.script || "");
  if (!looksMutatingShell(cmd)) {
    output({});
    process.exit(0);
  }
  if (!writeAuthorized) {
    deny("RingStatus control: mutating shell command blocked because the task contract is read-only.");
    process.exit(0);
  }
  if (!matchesAny(cmd, contract.allowed_shell_write_patterns || [])) {
    deny("RingStatus control: mutating shell command is not explicitly allowed by the task contract.");
    process.exit(0);
  }
  output({});
  process.exit(0);
}

if (tool.startsWith("mcp__")) {
  const mcpMutating = /(?:create|update|delete|write|publish|deploy|send|reply|comment|merge|close|approve|reject|upload|move|rename|set_)/i.test(tool);
  if (!mcpMutating) {
    output({});
    process.exit(0);
  }
  if (!writeAuthorized) {
    deny(`RingStatus control: write-capable MCP tool blocked in read-only task: ${tool}`);
    process.exit(0);
  }
  if (!(contract.allowed_mcp_write_tools || []).includes(tool)) {
    deny(`RingStatus control: MCP write tool is not explicitly approved: ${tool}`);
    process.exit(0);
  }
  output({});
  process.exit(0);
}

if (!writeAuthorized) {
  if (mutatingToolName) {
    deny(`RingStatus control: write-capable local tool blocked in read-only task: ${tool || "(unnamed)"}`);
    process.exit(0);
  }
  if (!readOnlyToolName) {
    deny(`RingStatus control: unclassified local tool blocked fail-closed in read-only task: ${tool || "(unnamed)"}`);
    process.exit(0);
  }
  output({});
  process.exit(0);
}

if (mutatingToolName) {
  deny(`RingStatus control: local write tool is not explicitly allowlisted by this contract: ${tool}`);
  process.exit(0);
}

output({});
