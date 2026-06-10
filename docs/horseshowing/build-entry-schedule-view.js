const fs = require("fs");
const path = require("path");

const showNo = process.argv[2] || "14906";
const focusDay = process.argv[3] || "2026-06-10";
const showTitleArg = process.argv.slice(4).join(" ");
const generatedAt = new Date();
const PRINT_COLUMN_TARGET_ROWS = 30;
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

function csvEscape(value) {
  const text = String(value ?? "");
  return /[",\n\r]/.test(text) ? `"${text.replace(/"/g, '""')}"` : text;
}

function htmlEscape(value) {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
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

function isActiveHelper(row) {
  const active = String(row.active ?? "1").trim().toLowerCase();
  return !["0", "false", "no", "inactive"].includes(active);
}

function readRows(filePath) {
  return fs.existsSync(filePath) ? parseCsv(fs.readFileSync(filePath, "utf8")) : [];
}

function formatGeneratedAt(date) {
  const pad = (value) => String(value).padStart(2, "0");
  return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())} ${pad(date.getHours())}:${pad(date.getMinutes())}`;
}

function entryClassPrintRows(group) {
  return group.classes.reduce((sum, classGroup) => sum + (classGroup.entries.length ? 2 : 1), 0) + 2;
}

function splitEntryClassesForPrint(groups, targetRows = PRINT_COLUMN_TARGET_ROWS) {
  const left = [];
  const right = [];
  let leftRows = 0;
  let rightRows = 0;

  for (const group of groups) {
    const rowCount = entryClassPrintRows(group);
    if (left.length === 0 || leftRows + rowCount <= targetRows) {
      left.push(group);
      leftRows += rowCount;
    } else {
      right.push(group);
      rightRows += rowCount;
    }
  }

  return { left, right, leftRows, rightRows, targetRows };
}

function renderEntryClassGroup(group) {
  const rollup = group.entries.map((entry) => `${entry.horse_display} (${entry.entry_order})`).join(", ");
  return `
              <tr class="class-line${rollup ? " has-rollup" : ""}" data-ring-day-no="${htmlEscape(group.row.ring_day_no)}" data-class-no="${htmlEscape(group.row.class_no)}">
                <td class="time-col">${htmlEscape(group.row.display_time || "TBD")}</td>
                <td class="number-col">${htmlEscape(group.row.class_number)}</td>
                <td class="class-col">${htmlEscape(group.row.class)}</td>
              </tr>${rollup ? `
              <tr class="entry-rollup-row" data-ring-day-no="${htmlEscape(group.row.ring_day_no)}" data-class-no="${htmlEscape(group.row.class_no)}">
                <td class="entry-rollup" colspan="3">${htmlEscape(rollup)}</td>
              </tr>` : ""}`;
}

function shouldHideClass(row, hideList) {
  const classText = String(row.class || "").toLowerCase();
  return hideList.some((item) => classText.includes(String(item).toLowerCase()));
}

function normalizeDisplayClass(value) {
  return String(value || "")
    .toLowerCase()
    .replace(/[']/g, "")
    .replace(/\s+/g, " ")
    .trim();
}

function applyDisplayRules(rows, hideList) {
  const seen = new Set();
  return rows.filter((row) => {
    if (shouldHideClass(row, hideList)) return false;
    const key = [
      row.ring_display,
      row.display_time,
      normalizeDisplayClass(row.class)
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
  const horsesPath = path.join(helperRoot, "horses.csv");
  const ridersPath = path.join(helperRoot, "riders.csv");
  const trainersPath = path.join(helperRoot, "trainers.csv");
  const classesPath = path.join(helperRoot, "classes.csv");
  const classHidePath = path.join(helperRoot, "class_hide.csv");

  for (const required of [schedulePath, updateSchedulePath, entryGoPath]) {
    if (!fs.existsSync(required)) throw new Error(`Missing ${required}`);
  }
  fs.mkdirSync(outDir, { recursive: true });

  const focusShow = readRows(focusShowPath).find((row) => row.show_no === showNo && row.focus_day === focusDay) || {};
  const ringsByNo = new Map(readRows(ringsPath).filter(isActiveHelper).map((row) => [String(row.ring_no), row]));
  const horsesByName = new Map(readRows(horsesPath).filter(isActiveHelper).map((row) => [row.horse, row]));
  const ridersByName = new Map(readRows(ridersPath).filter(isActiveHelper).map((row) => [row.rider, row]));
  const trainersByName = new Map(readRows(trainersPath).filter(isActiveHelper).map((row) => [row.trainer, row]));
  const classesByNo = new Map(readRows(classesPath).filter(isActiveHelper).map((row) => [String(row.class_no), row]));
  const classHideList = readRows(classHidePath)
    .filter(isActiveHelper)
    .map((row) => row.hide_text || row.class_text || row.class_name)
    .filter(Boolean);
  const updateByClassNo = new Map(readRows(updateSchedulePath)
    .filter((row) => row.show_no === showNo && row.focus_day === focusDay && row.class_no)
    .map((row) => [String(row.class_no), row]));
  const scheduleByClass = new Map(readRows(schedulePath)
    .filter((row) => row.show_no === showNo && row.focus_day === focusDay)
    .map((row) => [String(row.class_no), row]));

  const isActiveTrainer = (trainer) => String(trainer?.active || "").trim() === "1";
  const includeEntry = (entry) => isActiveTrainer(trainersByName.get(entry.trainer));

  const activeEntryRows = readRows(entryGoPath)
    .filter((row) => row.show_no === showNo && row.focus_day === focusDay && includeEntry(row))
    .map((entry) => {
      const schedule = scheduleByClass.get(String(entry.class_no)) || {};
      const update = updateByClassNo.get(String(entry.class_no)) || {};
      const ring = ringsByNo.get(String(schedule.ring_no || update.ring_no)) || {};
      const classHelper = classesByNo.get(String(entry.class_no)) || {};
      const horse = horsesByName.get(entry.horse) || {};
      const rider = ridersByName.get(entry.rider) || {};
      const trainer = trainersByName.get(entry.trainer) || {};
      const classText = classHelper.class_display || [update.class_payout, update.class_name || schedule.class_name].filter(Boolean).join(" ");
      const sourceTime = entry.display_time || entry.class_start_time || update.time_text || update.time || update.class_start_time || schedule.class_start_time;
      return {
        show_no: showNo,
        focus_day: focusDay,
        ring_display: ring.ring_display || ring.ring_name || update.ring_name || schedule.ring_name,
        ring_no: entry.ring_no || schedule.ring_no || update.ring_no,
        ring_day_no: entry.ring_day_no || schedule.ring_day_no || update.ring_day_no,
        class_start_time: classStartTime(sourceTime),
        display_time: displayTime(sourceTime),
        class_no: entry.class_no,
        class_number: classHelper.class_number || update.class_number,
        class: classText,
        entry_no: entry.entry_no,
        entry_order: entry.entry_order,
        go_time: entry.go_time || "",
        horse: entry.horse,
        horse_display: horse.horse_display || entry.horse,
        rider: entry.rider,
        rider_display: rider.rider_display || entry.rider,
        trainer: entry.trainer,
        trainer_display: trainer.trainer_display || entry.trainer
      };
    })
    .sort((a, b) => (
      a.ring_display.localeCompare(b.ring_display) ||
      String(a.class_start_time || "99:99:99").localeCompare(String(b.class_start_time || "99:99:99")) ||
      Number(a.entry_order || 0) - Number(b.entry_order || 0)
    ));

  const scheduleRows = applyDisplayRules(readRows(schedulePath)
    .filter((row) => row.show_no === showNo && row.focus_day === focusDay)
    .map((row) => {
      const update = updateByClassNo.get(String(row.class_no)) || {};
      const classHelper = classesByNo.get(String(row.class_no)) || {};
      const ring = ringsByNo.get(String(row.ring_no || update.ring_no)) || {};
      const sourceTime = update.time_text || update.time || update.class_start_time || row.class_start_time;
      return {
        show_no: showNo,
        focus_day: focusDay,
        ring_display: ring.ring_display || ring.ring_name || update.ring_name || row.ring_name,
        ring_no: row.ring_no || update.ring_no,
        ring_day_no: row.ring_day_no || update.ring_day_no,
        class_start_time: classStartTime(sourceTime),
        display_time: displayTime(sourceTime),
        class_no: row.class_no,
        class_number: classHelper.class_number || update.class_number,
        class: classHelper.class_display || [update.class_payout, update.class_name || row.class_name].filter(Boolean).join(" ")
      };
    })
    .sort((a, b) => (
      a.ring_display.localeCompare(b.ring_display) ||
      String(a.class_start_time || "99:99:99").localeCompare(String(b.class_start_time || "99:99:99")) ||
      Number(a.class_number || 0) - Number(b.class_number || 0) ||
      Number(a.class_no || 0) - Number(b.class_no || 0)
    )), classHideList);

  const headers = [
    "show_no",
    "focus_day",
    "ring_display",
    "display_time",
    "class_number",
    "class",
    "entry_no",
    "entry_order",
    "go_time",
    "horse_display",
    "rider_display",
    "trainer_display",
    "ring_no",
    "ring_day_no",
    "class_no"
  ];
  const csvOut = path.join(outDir, `${showNo}-${focusDay}-entry-schedule.csv`);
  fs.writeFileSync(csvOut, [
    headers.join(","),
    ...activeEntryRows.map((row) => headers.map((header) => csvEscape(row[header])).join(","))
  ].join("\n") + "\n");

  const showTitle = showTitleArg || focusShow.show_title || `Show ${showNo}`;
  const showHead = showTitle.startsWith("WEC Ocala ") ? "WEC Ocala" : showTitle;
  const showSubtitle = showTitle.startsWith("WEC Ocala ") ? showTitle.replace(/^WEC Ocala\s+/, "") : "";
  const classGroups = [];
  for (const row of scheduleRows) {
    const classKey = `${row.ring_display}|${row.class_no}`;
    classGroups.push({
      key: classKey,
      row,
      entries: activeEntryRows.filter((entry) => String(entry.class_no) === String(row.class_no))
    });
  }

  const ringGroups = [];
  for (const group of classGroups) {
    let ringGroup = ringGroups.find((item) => item.ring_display === group.row.ring_display);
    if (!ringGroup) {
      ringGroup = { ring_display: group.row.ring_display, classes: [] };
      ringGroups.push(ringGroup);
    }
    ringGroup.classes.push(group);
  }

  const renderRingSection = (ringGroup) => `
    <section class="ring-group" data-print-rows="${ringGroup.classes.length + 2}">
      <h2>${htmlEscape(ringGroup.ring_display)}</h2>
      <table class="schedule-table">
        <colgroup>
          <col class="time-col-def">
          <col class="number-col-def">
          <col class="class-col-def">
        </colgroup>
        <thead>
          <tr>
            <th class="time-col">Time</th>
            <th class="number-col">#</th>
            <th class="class-head">Class</th>
          </tr>
        </thead>
        <tbody>${ringGroup.classes.map(renderEntryClassGroup).join("")}
        </tbody>
      </table>
    </section>`;

  const sections = ringGroups.map(renderRingSection).join("");

  const printColumns = splitEntryClassesForPrint(ringGroups);
  const printSections = `
    <section class="print-columns" data-target-rows="${printColumns.targetRows}" data-left-rows="${printColumns.leftRows}" data-right-rows="${printColumns.rightRows}">
      <div class="print-column">${printColumns.left.map(renderRingSection).join("")}</div>
      <div class="print-column">${printColumns.right.map(renderRingSection).join("")}</div>
    </section>`;

  const htmlOut = path.join(outDir, `${showNo}-${focusDay}-entry-schedule.html`);
  fs.writeFileSync(htmlOut, `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Entry Schedule ${showNo} ${focusDay}</title>
  <style>
    body { margin: 0; font-family: Arial, Helvetica, sans-serif; color: #172026; background: #f6f7f8; }
    main { width: 100%; box-sizing: border-box; margin: 0; padding: 24px; }
    h1 { margin: 0; font-size: 24px; font-weight: 700; letter-spacing: 0; }
    .show-subtitle { margin: 3px 0 0; color: #263238; font-size: 18px; font-weight: 600; }
    .focus-date { margin: 4px 0 18px; color: #4c5961; font-size: 15px; }
    .updated-at { margin: 24px 0 0; color: #5c6870; font-size: 12px; }
    .ring-group { margin: 0 0 18px; }
    h2 { margin: 0; padding: 8px 10px; font-size: 16px; line-height: 1.25; letter-spacing: 0; background: #263238; color: #fff; }
    table { width: 100%; border-collapse: collapse; background: #fff; border: 1px solid #d9dee2; table-layout: fixed; }
    th, td { padding: 8px 10px; border-bottom: 1px solid #e4e8eb; text-align: left; vertical-align: top; font-size: 13px; line-height: 1.35; }
    th { background: #eef2f4; font-weight: 700; }
    .time-col-def,
    .number-col-def { width: calc(5ch + 8px); }
    .class-col-def { width: auto; }
    .schedule-table { border-bottom: 0; }
    .schedule-table th.time-col,
    .schedule-table th.number-col { text-align: center; }
    .schedule-table th.class-head { text-align: left; }
    .schedule-table th.time-col,
    .schedule-table th.number-col,
    .schedule-table td.time-col,
    .schedule-table td.number-col { padding-left: 4px; padding-right: 4px; }
    .schedule-table td.time-col { text-align: right; white-space: nowrap; }
    .schedule-table td.number-col { text-align: right; white-space: nowrap; }
    .schedule-table td.class-col { width: auto; text-align: left; white-space: normal; overflow-wrap: normal; word-break: normal; }
    .class-line.has-rollup td { background: #f8fafb; border-top: 1px solid #d5dce1; border-bottom: 0; padding-bottom: 3px; }
    .entry-rollup-row td { background: #f8fafb; border-top: 0; border-bottom: 1px solid #d5dce1; padding: 0 10px 10px; }
    .entry-rollup { color: #263238; font-size: 12px; font-weight: 700; line-height: 1.3; white-space: normal; overflow: visible; text-overflow: clip; }
    @media print { body { background: #fff; } main { max-width: none; padding: 12px; } th { position: static; } }
  </style>
</head>
<body>
  <main>
    <h1>${htmlEscape(showHead)}</h1>
    ${showSubtitle ? `<div class="show-subtitle">${htmlEscape(showSubtitle)}</div>` : ""}
    <div class="focus-date">Entry schedule: ${htmlEscape(focusDay)}</div>
    ${sections || "<p>No active team entries matched this schedule.</p>"}
    <footer class="updated-at">Last updated: ${htmlEscape(formatGeneratedAt(generatedAt))}</footer>
  </main>
</body>
</html>`);

  console.log(JSON.stringify({
    filter: "trainers.active",
    matched_entries: activeEntryRows.length,
    trainers: new Set(activeEntryRows.map((row) => row.trainer_display || row.trainer)).size,
    visible_classes: scheduleRows.length,
    classes: classGroups.length,
    csvOut,
    htmlOut
  }, null, 2));

  const printHtmlOut = path.join(outDir, `${showNo}-${focusDay}-entry-schedule-print.html`);
  fs.writeFileSync(printHtmlOut, `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Entry Schedule Print ${showNo} ${focusDay}</title>
  <style>
    @page { size: letter; margin: 0.15in; }
    * { -webkit-print-color-adjust: exact; print-color-adjust: exact; }
    body { margin: 0; font-family: Arial, Helvetica, sans-serif; color: #172026; background: #fff; }
    main { width: 8.2in; box-sizing: border-box; margin: 0; padding: 0; }
    h1 { margin: 0; font-size: 23px; font-weight: 700; letter-spacing: 0; }
    .show-subtitle { margin: 2px 0 0; color: #263238; font-size: 15px; font-weight: 600; }
    .focus-date { margin: 3px 0 10px; color: #4c5961; font-size: 12px; }
    .updated-at { margin: 10px 0 0; color: #5c6870; font-size: 10px; }
    .print-columns { display: grid; grid-template-columns: minmax(0, 1fr) minmax(0, 1fr); gap: 10px; align-items: start; }
    .print-column { min-width: 0; }
    .ring-group { margin: 0 0 10px; break-inside: avoid; page-break-inside: avoid; }
    h2 { margin: 0; padding: 6px 8px; font-size: 14px; line-height: 1.2; letter-spacing: 0; background: #263238; color: #fff; }
    table { width: 100%; border-collapse: collapse; background: #fff; border: 1px solid #d9dee2; table-layout: fixed; }
    th, td { padding: 5px 6px; border-bottom: 1px solid #e4e8eb; text-align: left; vertical-align: top; font-size: 11.5px; line-height: 1.2; }
    th { background: #eef2f4; font-weight: 700; }
    tbody tr:nth-child(even) { background: #f8fafb; }
    .time-col-def,
    .number-col-def { width: calc(5ch + 4px); }
    .class-col-def { width: auto; }
    .schedule-table { border-bottom: 0; }
    .schedule-table th.time-col,
    .schedule-table th.number-col { text-align: center; }
    .schedule-table th.class-head { text-align: left; }
    .schedule-table th.time-col,
    .schedule-table th.number-col,
    .schedule-table td.time-col,
    .schedule-table td.number-col { padding-left: 2px; padding-right: 2px; }
    .schedule-table td.time-col { text-align: right; white-space: nowrap; }
    .schedule-table td.number-col { text-align: right; white-space: nowrap; }
    .schedule-table td.class-col { width: auto; text-align: left; white-space: normal; overflow-wrap: normal; word-break: normal; }
    .class-line.has-rollup td { background: #f8fafb; border-top: 1px solid #d5dce1; border-bottom: 0; padding-bottom: 3px; }
    .entry-rollup-row td { background: #f8fafb; border-top: 0; border-bottom: 1px solid #d5dce1; padding: 0 10px 10px; }
    .entry-rollup { color: #263238; font-size: 10.5px; font-weight: 700; line-height: 1.3; white-space: normal; overflow: visible; text-overflow: clip; }
    @media screen { body { background: #f6f7f8; } main { width: 8.2in; min-height: 10.7in; margin: 0; padding: 0.15in; background: #fff; } }
    @media print { th { position: static; } }
  </style>
</head>
<body>
  <main>
    <h1>${htmlEscape(showHead)}</h1>
    ${showSubtitle ? `<div class="show-subtitle">${htmlEscape(showSubtitle)}</div>` : ""}
    <div class="focus-date">Entry schedule: ${htmlEscape(focusDay)}</div>
    ${printSections || "<p>No active team entries matched this schedule.</p>"}
    <footer class="updated-at">Last updated: ${htmlEscape(formatGeneratedAt(generatedAt))}</footer>
  </main>
</body>
</html>`);

  console.log(JSON.stringify({
    printHtmlOut
  }, null, 2));
}

main();
