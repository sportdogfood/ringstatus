const fs = require("fs");
const path = require("path");
const { spawnSync } = require("child_process");

const showNo = process.argv[2] || process.env.WEC_SHOW_NO || "14906";
const focusDay = process.argv[3] || process.env.WEC_FOCUS_DAY || "2026-06-10";
const showTitle = process.argv.slice(4).join(" ") || process.env.WEC_SHOW_TITLE || "WEC Ocala Summer Series 1 CSI2*";
const nowArg = process.env.WEC_NOW || "";

const repoRoot = path.resolve(__dirname, "..", "..");
const dataRepoRoot = path.resolve(repoRoot, "..", "ringstatus-data");
const importDir = path.join(dataRepoRoot, "docs", "horseshowing", "catalyst-import", `${showNo}-${focusDay}`);
const publicDir = path.join(dataRepoRoot, "docs", "horseshowing", "wec", showNo, focusDay);
const triggerCsv = path.join(publicDir, "time_triggers.csv");
const triggerJson = path.join(publicDir, "time_triggers.json");

function parseCsv(text) {
  const rows = [];
  let row = [];
  let cell = "";
  let quoted = false;
  for (let i = 0; i < text.length; i += 1) {
    const ch = text[i];
    const next = text[i + 1];
    if (quoted) {
      if (ch === '"' && next === '"') {
        cell += '"';
        i += 1;
      } else if (ch === '"') {
        quoted = false;
      } else {
        cell += ch;
      }
    } else if (ch === '"') {
      quoted = true;
    } else if (ch === ",") {
      row.push(cell);
      cell = "";
    } else if (ch === "\n") {
      row.push(cell.replace(/\r$/, ""));
      rows.push(row);
      row = [];
      cell = "";
    } else {
      cell += ch;
    }
  }
  if (cell || row.length) {
    row.push(cell.replace(/\r$/, ""));
    rows.push(row);
  }
  const [headers, ...body] = rows;
  if (!headers) return [];
  return body
    .filter((values) => values.some(Boolean))
    .map((values) => Object.fromEntries(headers.map((header, index) => [header, values[index] || ""])));
}

function csvEscape(value) {
  const s = String(value ?? "");
  return /[",\n\r]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
}

function writeCsv(filePath, rows, headers) {
  const lines = [headers.join(",")];
  for (const row of rows) {
    lines.push(headers.map((header) => csvEscape(row[header])).join(","));
  }
  fs.writeFileSync(filePath, `${lines.join("\n")}\n`);
}

function readRows(filePath) {
  return fs.existsSync(filePath) ? parseCsv(fs.readFileSync(filePath, "utf8")) : [];
}

function runNode(script, args) {
  const result = spawnSync(process.execPath, [script, ...args], {
    cwd: repoRoot,
    encoding: "utf8",
    stdio: ["ignore", "pipe", "pipe"]
  });
  if (result.status !== 0) {
    throw new Error(`${path.basename(script)} failed\n${result.stdout}\n${result.stderr}`);
  }
  return JSON.parse(result.stdout);
}

function parseDateTime(dateText, timeText) {
  if (!dateText || !timeText) return null;
  const [year, month, day] = String(dateText).split("-").map(Number);
  const [hour, minute = 0, second = 0] = String(timeText).split(":").map(Number);
  if (![year, month, day, hour].every(Number.isFinite)) return null;
  return new Date(year, month - 1, day, hour, Number(minute) || 0, Number(second) || 0);
}

function minutesUntil(target, now) {
  return Math.floor((target.getTime() - now.getTime()) / 60000);
}

function shouldCreateTrigger(target, now, threshold) {
  const minutes = minutesUntil(target, now);
  return minutes <= threshold && minutes >= 0;
}

function triggerRow({ sourceType, threshold, sourceKey, targetTime, now, row }) {
  const minutes = minutesUntil(targetTime, now);
  return {
    trigger_id: `${sourceType}|${sourceKey}|${threshold}`,
    created_at: now.toISOString(),
    show_no: showNo,
    focus_day: focusDay,
    source_type: sourceType,
    threshold_minutes: threshold,
    minutes_until: minutes,
    target_time: targetTime.toISOString(),
    source_key: sourceKey,
    ring_no: row.ring_no || "",
    ring_day_no: row.ring_day_no || "",
    class_no: row.class_no || "",
    entry_no: row.entry_no || "",
    entry_order: row.entry_order || "",
    class_name: row.class_name || "",
    horse: row.horse || "",
    rider: row.rider || "",
    trainer: row.trainer || ""
  };
}

function updateTriggers(now = new Date()) {
  fs.mkdirSync(publicDir, { recursive: true });

  const existing = readRows(triggerCsv);
  const seen = new Set(existing.map((row) => row.trigger_id).filter(Boolean));
  const created = [];

  const classRows = readRows(path.join(importDir, "hs_class_start_times.csv"))
    .filter((row) => row.show_no === showNo && row.focus_day === focusDay && row.class_start_time);
  for (const row of classRows) {
    const target = parseDateTime(row.focus_day, row.class_start_time);
    if (!target) continue;
    for (const threshold of [60, 30]) {
      const sourceKey = row.class_start_key || `${showNo}|${focusDay}|${row.ring_day_no}|${row.class_no}|${row.class_start_time}`;
      const id = `class_start_time|${sourceKey}|${threshold}`;
      if (seen.has(id) || !shouldCreateTrigger(target, now, threshold)) continue;
      const next = triggerRow({ sourceType: "class_start_time", threshold, sourceKey, targetTime: target, now, row });
      seen.add(next.trigger_id);
      created.push(next);
    }
  }

  const entryRows = readRows(path.join(importDir, "hs_entry_go_times.csv"))
    .filter((row) => row.show_no === showNo && row.focus_day === focusDay && row.go_time);
  for (const row of entryRows) {
    const target = parseDateTime(row.focus_day, row.go_time);
    if (!target) continue;
    for (const threshold of [40, 20]) {
      const sourceKey = row.entry_go_key || `${row.class_no}|${row.entry_no}|${row.entry_order}|${row.go_time}`;
      const id = `entry_go_time|${sourceKey}|${threshold}`;
      if (seen.has(id) || !shouldCreateTrigger(target, now, threshold)) continue;
      const next = triggerRow({ sourceType: "entry_go_time", threshold, sourceKey, targetTime: target, now, row });
      seen.add(next.trigger_id);
      created.push(next);
    }
  }

  const headers = [
    "trigger_id",
    "created_at",
    "show_no",
    "focus_day",
    "source_type",
    "threshold_minutes",
    "minutes_until",
    "target_time",
    "source_key",
    "ring_no",
    "ring_day_no",
    "class_no",
    "entry_no",
    "entry_order",
    "class_name",
    "horse",
    "rider",
    "trainer"
  ];
  const rows = [...existing, ...created];
  writeCsv(triggerCsv, rows, headers);
  fs.writeFileSync(triggerJson, `${JSON.stringify(rows, null, 2)}\n`);

  return {
    class_start_rows: classRows.length,
    entry_go_rows_with_go_time: entryRows.length,
    existing_triggers: existing.length,
    created_triggers: created.length,
    triggerCsv,
    triggerJson
  };
}

function main() {
  if (!fs.existsSync(importDir)) throw new Error(`Missing import dir ${importDir}`);

  const publish = runNode(path.join(__dirname, "publish-wec-schedule-json.js"), [showNo, focusDay, showTitle]);
  const now = nowArg ? new Date(nowArg) : new Date();
  if (Number.isNaN(now.getTime())) throw new Error(`Invalid WEC_NOW ${nowArg}`);
  const triggers = updateTriggers(now);

  console.log(JSON.stringify({
    ok: true,
    show_no: showNo,
    focus_day: focusDay,
    now: now.toISOString(),
    schedule_json: publish.targetJson,
    schedule_rows: publish.rows,
    active_entries: publish.active_entries,
    ...triggers
  }, null, 2));
}

main();
