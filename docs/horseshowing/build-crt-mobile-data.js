const fs = require("fs");
const path = require("path");

const showNo = process.argv[2] || "14906";
const focusDay = process.argv[3] || "2026-06-10";
const showTitleArg = process.argv.slice(4).join(" ");
const repoRoot = path.resolve(__dirname, "..", "..");
const dataRoot = path.resolve(repoRoot, "..", "ringstatus-data", "docs", "horseshowing", "catalyst-import", `${showNo}-${focusDay}`);
const helperRoot = path.join(__dirname, "helpers", showNo);
const outDir = path.join(__dirname, "reports");

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
  return body
    .filter((values) => values.some(Boolean))
    .map((values) => Object.fromEntries(headers.map((header, index) => [header, values[index] || ""])));
}

function readRows(filePath) {
  return fs.existsSync(filePath) ? parseCsv(fs.readFileSync(filePath, "utf8")) : [];
}

function isActiveHelper(row) {
  const active = String(row.active ?? "1").trim().toLowerCase();
  return !["0", "false", "no", "inactive"].includes(active);
}

function classStartTime(value) {
  if (!value) return "";
  const raw = String(value).trim();
  const compact = raw.replace(/\s+/g, "");
  const meridian = compact.match(/([ap])m?$/i)?.[1]?.toUpperCase() || "";
  const timePart = compact.replace(/([ap])m?$/i, "");
  const [hh, mm = "00"] = timePart.split(":");
  let hour = Number(hh);
  if (!Number.isFinite(hour)) return "";
  if (meridian === "P" && hour < 12) hour += 12;
  if (meridian === "A" && hour === 12) hour = 0;
  const seconds = timePart.split(":")[2] || "00";
  return `${String(hour).padStart(2, "0")}:${mm.padStart(2, "0")}:${seconds.padStart(2, "0")}`;
}

function displayTime(value) {
  const start = classStartTime(value);
  if (!start) return "check time";
  const [hh, mm] = start.split(":");
  let hour = Number(hh);
  const suffix = hour >= 12 ? "P" : "A";
  hour = hour % 12 || 12;
  return `${hour}${mm}${suffix}`;
}

function timeSortValue(value) {
  const start = classStartTime(value);
  if (!start) return 999999;
  const [hh, mm, ss] = start.split(":").map(Number);
  return (hh * 3600) + (mm * 60) + (ss || 0);
}

function normalizeDisplayClass(value) {
  return String(value || "")
    .toLowerCase()
    .replace(/[']/g, "")
    .replace(/\s+/g, " ")
    .trim();
}

function shouldHideClass(row, hideList) {
  const classText = String(row.class_name || "").toLowerCase();
  return hideList.some((item) => classText.includes(String(item).toLowerCase()));
}

function applyDisplayRules(rows, hideList) {
  const seen = new Set();
  return rows.filter((row) => {
    if (shouldHideClass(row, hideList)) return false;
    const key = [
      row.ring_name,
      row.start_display,
      normalizeDisplayClass(row.class_name)
    ].join("|").toLowerCase();
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

function main() {
  const schedulePath = path.join(dataRoot, "hs_class_start_times.csv");
  const updateSchedulePath = path.join(dataRoot, "hs_update_schedule.csv");
  const entryGoPath = path.join(dataRoot, "hs_entry_go_times.csv");
  const focusShowPath = path.join(helperRoot, "focus_show.csv");
  const ringsPath = path.join(helperRoot, "rings.csv");
  const classesPath = path.join(helperRoot, "classes.csv");
  const trainersPath = path.join(helperRoot, "trainers.csv");
  const horsesPath = path.join(helperRoot, "horses.csv");
  const classHidePath = path.join(helperRoot, "class_hide.csv");
  const templatePath = path.join(__dirname, "wef-print-template", "crt-mobile-v4-source.html");

  for (const required of [schedulePath, updateSchedulePath, entryGoPath, templatePath]) {
    if (!fs.existsSync(required)) throw new Error(`Missing ${required}`);
  }
  fs.mkdirSync(outDir, { recursive: true });

  const focusShow = readRows(focusShowPath).find((row) => row.show_no === showNo && row.focus_day === focusDay) || {};
  const showTitle = showTitleArg || focusShow.show_title || `Show ${showNo}`;
  const ringsByNo = new Map(readRows(ringsPath).filter(isActiveHelper).map((row) => [String(row.ring_no), row]));
  const classesByNo = new Map(readRows(classesPath).filter(isActiveHelper).map((row) => [String(row.class_no), row]));
  const trainersByName = new Map(readRows(trainersPath).filter(isActiveHelper).map((row) => [row.trainer, row]));
  const horsesByName = new Map(readRows(horsesPath).filter(isActiveHelper).map((row) => [row.horse, row]));
  const classHideList = readRows(classHidePath).filter(isActiveHelper).map((row) => row.hide_text || row.class_text || row.class_name).filter(Boolean);
  const updateByClassNo = new Map(readRows(updateSchedulePath)
    .filter((row) => row.show_no === showNo && row.focus_day === focusDay && row.class_no)
    .map((row) => [String(row.class_no), row]));

  const activeEntries = readRows(entryGoPath)
    .filter((row) => row.show_no === showNo && row.focus_day === focusDay && trainersByName.has(row.trainer))
    .map((row) => {
      const horse = horsesByName.get(row.horse) || {};
      return {
        ...row,
        horse_display: horse.horse_display || row.horse
      };
    });

  const scheduleRows = readRows(schedulePath)
    .filter((row) => row.show_no === showNo && row.focus_day === focusDay)
    .map((row) => {
      const update = updateByClassNo.get(String(row.class_no)) || {};
      const classHelper = classesByNo.get(String(row.class_no)) || {};
      const ring = ringsByNo.get(String(row.ring_no || update.ring_no)) || {};
      const sourceTime = update.time_text || update.time || update.class_start_time || row.class_start_time;
      const className = classHelper.class_display || [update.class_payout, update.class_name || row.class_name].filter(Boolean).join(" ");
      const classEntries = activeEntries.filter((entry) => String(entry.class_no) === String(row.class_no));
      const schedDisplay = classEntries.map((entry) => `${entry.horse_display} (${entry.entry_order})`).join(", ");
      const horses = classEntries.map((entry) => entry.horse_display).join("|");
      const classNumber = Number(classHelper.class_number || update.class_number || 0);
      const sortValue = (timeSortValue(sourceTime) * 10000) + (Number.isFinite(classNumber) ? classNumber : 0);
      return {
        show_id: showNo,
        show_days_report_title: showTitle,
        show_days_display_date: focusDay,
        show_day_key: focusDay,
        ring_number: Number(row.ring_no || update.ring_no || 9999),
        ring_name: ring.ring_display || ring.ring_name || update.ring_name || row.ring_name,
        class_group_id: String(row.class_no),
        class_group_sequence: sortValue,
        group_group_name: className,
        class_number: classHelper.class_number || update.class_number || "",
        class_name: className,
        start_display: displayTime(sourceTime),
        group_display: schedDisplay,
        sched_display: schedDisplay,
        "8778_sched_display": schedDisplay,
        sched_horses: horses
      };
    })
    .sort((a, b) => (
      Number(a.ring_number || 9999) - Number(b.ring_number || 9999) ||
      Number(a.class_group_sequence || 9999) - Number(b.class_group_sequence || 9999)
    ));

  const rows = applyDisplayRules(scheduleRows, classHideList);
  const dataOut = path.join(outDir, `${showNo}-${focusDay}-crt-mobile-data.json`);
  fs.writeFileSync(dataOut, `${JSON.stringify(rows, null, 2)}\n`);

  const htmlOut = path.join(outDir, `${showNo}-${focusDay}-crt-mobile.html`);
  const localDataName = path.basename(dataOut);
  const html = fs.readFileSync(templatePath, "utf8")
    .replace(
      'return "https://ringstatus-proxy.gombcg.workers.dev/docs/9999/schedules/schedule.json";',
      `return "${localDataName}";`
    );
  fs.writeFileSync(htmlOut, html);

  console.log(JSON.stringify({
    rows: rows.length,
    active_entries: activeEntries.length,
    dataOut,
    htmlOut
  }, null, 2));
}

main();
