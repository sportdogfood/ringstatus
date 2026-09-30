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

if (tool === "apply_patch") {
  const paths = extractPatchPaths(ti.command);
  if (!writeAuthorized) {
    deny("RingStatus control: repository writes are not authorized for the current task contract.");
    process.exit(0);
  }
  if (!paths.length) {
    deny("RingStatus control: could not verify apply_patch target paths; refusing an unscoped write.");
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

if (tool === "Bash") {
  const cmd = String(ti.command || "");
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
  const mutatingName = /(?:create|update|delete|write|publish|deploy|send|reply|comment|merge|close|approve|reject|upload|move|rename|set_)/i.test(tool);
  if (!mutatingName) {
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

output({});
