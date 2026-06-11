const fs = require("fs");
const path = require("path");
const { spawnSync } = require("child_process");

const showNo = process.argv[2] || "14906";
const focusDay = process.argv[3] || "2026-06-10";
const showTitle = process.argv.slice(4).join(" ") || "WEC Ocala Summer Series 1 CSI2*";

const repoRoot = path.resolve(__dirname, "..", "..");
const dataRepoRoot = path.resolve(repoRoot, "..", "ringstatus-data");
const builder = path.join(__dirname, "build-crt-mobile-data.js");

function run(command, args, options = {}) {
  const result = spawnSync(command, args, {
    cwd: repoRoot,
    encoding: "utf8",
    stdio: ["ignore", "pipe", "pipe"],
    ...options
  });
  if (result.status !== 0) {
    throw new Error(`${command} ${args.join(" ")} failed\n${result.stdout}\n${result.stderr}`);
  }
  return result.stdout.trim();
}

if (!fs.existsSync(dataRepoRoot)) {
  throw new Error(`Missing ringstatus-data repo at ${dataRepoRoot}`);
}

const buildOutput = run(process.execPath, [builder, showNo, focusDay, showTitle]);
const parsed = JSON.parse(buildOutput);
const sourceJson = parsed.dataOut;

const targetDir = path.join(
  dataRepoRoot,
  "docs",
  "horseshowing",
  "wec",
  showNo,
  focusDay
);
const targetJson = path.join(targetDir, "schedule.json");

fs.mkdirSync(targetDir, { recursive: true });
fs.copyFileSync(sourceJson, targetJson);

console.log(JSON.stringify({
  rows: parsed.rows,
  active_entries: parsed.active_entries,
  sourceJson,
  targetJson,
  publicPath: `/docs/horseshowing/wec/${showNo}/${focusDay}/schedule.json`,
  publicUrl: `https://ringstatus-proxy.gombcg.workers.dev/docs/horseshowing/wec/${showNo}/${focusDay}/schedule.json`
}, null, 2));
