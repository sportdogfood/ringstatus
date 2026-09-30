import fs from "node:fs";
import path from "node:path";
import { execFileSync } from "node:child_process";

export function readStdin() {
  return new Promise((resolve, reject) => {
    let s = "";
    process.stdin.setEncoding("utf8");
    process.stdin.on("data", c => s += c);
    process.stdin.on("end", () => {
      try { resolve(s.trim() ? JSON.parse(s) : {}); }
      catch (e) { reject(e); }
    });
    process.stdin.on("error", reject);
  });
}

export function repoRoot(cwd = process.cwd()) {
  try {
    return execFileSync("git", ["rev-parse", "--show-toplevel"], { cwd, encoding: "utf8" }).trim();
  } catch {
    return cwd;
  }
}

export function loadJson(root, rel) {
  return JSON.parse(fs.readFileSync(path.join(root, rel), "utf8"));
}

export function loadText(root, rel) {
  return fs.readFileSync(path.join(root, rel), "utf8");
}

export function output(obj) {
  process.stdout.write(JSON.stringify(obj));
}

export function deny(reason) {
  output({
    hookSpecificOutput: {
      hookEventName: "PreToolUse",
      permissionDecision: "deny",
      permissionDecisionReason: reason
    }
  });
}

export function norm(p) {
  return String(p || "").replaceAll("\\", "/").replace(/^\.\/+/, "");
}

export function isPathAllowed(candidate, allowed) {
  const c = norm(candidate);
  return allowed.some(raw => {
    const a = norm(raw);
    return c === a || c.startsWith(a.endsWith("/") ? a : a + "/");
  });
}

export function extractPatchPaths(command) {
  const out = [];
  const re = /^\*\*\* (?:Update|Add|Delete) File:\s+(.+)$/gm;
  let m;
  while ((m = re.exec(String(command || "")))) out.push(norm(m[1].trim()));
  return [...new Set(out)];
}

export function looksMutatingShell(command) {
  const c = String(command || "").trim();
  const patterns = [
    /\bgit\s+(?:add|commit|push|merge|rebase|reset|checkout|switch|restore|clean|rm|mv)\b/i,
    /\b(?:rm|del|erase|move|mv|cp|copy|mkdir|rmdir|rd|touch)\b/i,
    /(?:^|[;&|]\s*)(?:echo|printf|cat|type)\b[^;&|]*(?:>|>>)/i,
    /\b(?:npm|pnpm|yarn)\s+(?:install|add|remove|update)\b/i,
    /\b(?:python|py|node|powershell|pwsh)\b.*\b(?:write|delete|remove|rename|replace)\b/i
  ];
  return patterns.some(r => r.test(c));
}

export function matchesAny(value, patterns) {
  return patterns.some(p => {
    try { return new RegExp(p).test(value); }
    catch { return value === p; }
  });
}
