const fs = require("fs");
const path = require("path");

const showNo = process.argv[2] || "14906";
const focusDay = process.argv[3] || "2026-06-10";
const showTitleArg = process.argv.slice(4).join(" ");
const generatedAt = new Date();
const PRINT_COLUMN_TARGET_ROWS = 30;
const repoRoot = path.resolve(__dirname, "..", "..");
const dataRoot = path.resolve(repoRoot, "..", "ringstatus-data", "docs", "horseshowing", "catalyst-import", `${showNo}-${focusDay}`);
const normalizedRoot = path.resolve(repoRoot, "..", "ringstatus-data", "docs", "horseshowing", "normalized", `${showNo}-${focusDay}`);
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

function timeSort(value) {
  if (!value) return "99:99:99";
  return value;
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

function htmlEscape(value) {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

function shouldHideClass(row, hideList) {
  const classText = String(row.class_context || "").toLowerCase();
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
      normalizeDisplayClass(row.class_context)
    ].join("|").toLowerCase();
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

function formatGeneratedAt(date) {
  const pad = (value) => String(value).padStart(2, "0");
  const yyyy = date.getFullYear();
  const mm = pad(date.getMonth() + 1);
  const dd = pad(date.getDate());
  const hh = pad(date.getHours());
  const min = pad(date.getMinutes());
  return `${yyyy}-${mm}-${dd} ${hh}:${min}`;
}

function isActiveHelper(row) {
  const active = String(row.active ?? "1").trim().toLowerCase();
  return !["0", "false", "no", "inactive"].includes(active);
}

function loadHelperRows(helperPath, fallbackPath) {
  if (fs.existsSync(helperPath)) {
    return parseCsv(fs.readFileSync(helperPath, "utf8")).filter(isActiveHelper);
  }
  if (fallbackPath && fs.existsSync(fallbackPath)) {
    return parseCsv(fs.readFileSync(fallbackPath, "utf8"));
  }
  return [];
}

function ringGroupPrintRows(group) {
  return group.rows.length + 2;
}

function splitRingGroupsForPrint(groups, targetRows = PRINT_COLUMN_TARGET_ROWS) {
  const left = [];
  const right = [];
  let leftRows = 0;
  let rightRows = 0;

  for (const group of groups) {
    const rowCount = ringGroupPrintRows(group);
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

function renderRingSection(group) {
  return `
    <section class="ring-group" data-print-rows="${ringGroupPrintRows(group)}">
      <h2>${htmlEscape(group.ring_display)}</h2>
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
        <tbody>${group.rows.map((row) => `
          <tr data-ring-day-no="${htmlEscape(row.ring_day_no)}" data-class-no="${htmlEscape(row.class_no)}" data-entry-count="${htmlEscape(row.entry_count)}">
            <td class="time-col">${htmlEscape(row.display_time || "TBD")}</td>
            <td class="number-col">${htmlEscape(row.class_number)}</td>
            <td class="class-col">${htmlEscape(row.class_context)}</td>
          </tr>`).join("")}
        </tbody>
      </table>
    </section>`;
}

function main() {
  const schedulePath = path.join(dataRoot, "hs_class_start_times.csv");
  const updateSchedulePath = path.join(dataRoot, "hs_update_schedule.csv");
  const focusShowPath = path.join(dataRoot, "hs_focus_show.csv");
  const ringHelpersPath = path.join(helperRoot, "rings.csv");
  const classHelpersPath = path.join(helperRoot, "classes.csv");
  const classHideHelpersPath = path.join(helperRoot, "class_hide.csv");
  const ringsFallbackPath = path.join(normalizedRoot, "rings.csv");
  if (!fs.existsSync(schedulePath)) {
    throw new Error(`Missing ${schedulePath}`);
  }
  if (!fs.existsSync(updateSchedulePath)) {
    throw new Error(`Missing ${updateSchedulePath}`);
  }

  fs.mkdirSync(outDir, { recursive: true });

  const focusShow = fs.existsSync(focusShowPath)
    ? parseCsv(fs.readFileSync(focusShowPath, "utf8"))
      .find((row) => row.show_no === showNo && row.focus_day === focusDay)
    : null;
  const updateRows = parseCsv(fs.readFileSync(updateSchedulePath, "utf8"));
  const updateByClassNo = new Map(updateRows
    .filter((row) => row.show_no === showNo && row.focus_day === focusDay && row.class_no)
    .map((row) => [row.class_no, row]));
  const ringsByNo = new Map(loadHelperRows(ringHelpersPath, ringsFallbackPath).map((row) => [row.ring_no, row]));
  const classesByNo = new Map(loadHelperRows(classHelpersPath).map((row) => [row.class_no, row]));
  const classHideList = loadHelperRows(classHideHelpersPath)
    .map((row) => row.hide_text || row.class_text || row.class_name)
    .filter(Boolean);

  const rawRows = parseCsv(fs.readFileSync(schedulePath, "utf8"))
    .filter((row) => row.show_no === showNo && row.focus_day === focusDay)
    .map((row) => {
      const update = updateByClassNo.get(row.class_no) || {};
      const classHelper = classesByNo.get(row.class_no) || {};
      const sourceTime = update.time_text || update.time || update.class_start_time || row.class_start_time;
      return {
        ...row,
        class_start_time: classStartTime(sourceTime),
        display_time: displayTime(sourceTime),
        class_number: classHelper.class_number || update.class_number || "",
        class_context: classHelper.class_display || [update.class_payout, update.class_name || row.class_name].filter(Boolean).join(" "),
        ring_display: ringsByNo.get(row.ring_no)?.ring_display || ringsByNo.get(row.ring_no)?.ring_name || update.ring_display || row.ring_display || update.ring_name || row.ring_name
      };
    })
    .sort((a, b) => (
      a.ring_display.localeCompare(b.ring_display) ||
      timeSort(a.class_start_time).localeCompare(timeSort(b.class_start_time)) ||
      Number(a.class_number || 0) - Number(b.class_number || 0) ||
      Number(a.class_no || 0) - Number(b.class_no || 0)
    ));
  const rows = applyDisplayRules(rawRows, classHideList);

  const csvHeaders = [
    "show_no",
    "focus_day",
    "show_title",
    "show_focus_date",
    "ring_display",
    "ring_no",
    "ring_day_no",
    "class_start_time",
    "display_time",
    "class_number",
    "class_no",
    "class",
    "entry_count"
  ];
  const showTitle = showTitleArg || focusShow?.show_title || `Show ${showNo}`;
  const showHead = focusShow?.show_head || (showTitle.startsWith("WEC Ocala ") ? "WEC Ocala" : showTitle);
  const showSubtitle = focusShow?.show_subtitle || (showTitle.startsWith("WEC Ocala ") ? showTitle.replace(/^WEC Ocala\s+/, "") : "");
  const showFocusDate = focusDay;
  const csv = [
    csvHeaders.join(","),
    ...rows.map((row) => csvHeaders.map((header) => {
      if (header === "show_title") return csvEscape(showTitle);
      if (header === "show_focus_date") return csvEscape(showFocusDate);
      if (header === "class") return csvEscape(row.class_context);
      return csvEscape(row[header]);
    }).join(","))
  ].join("\n");

  const csvOut = path.join(outDir, `${showNo}-${focusDay}-focus-schedule.csv`);
  fs.writeFileSync(csvOut, `${csv}\n`);

  const hiddenRows = rawRows.length - rows.length;
  const missingTimes = rows.filter((row) => !row.class_start_time).length;
  const ringCount = new Set(rows.map((row) => row.ring_no)).size;
  const totalEntries = rows.reduce((sum, row) => sum + Number(row.entry_count || 0), 0);

  const groupedRows = [];
  for (const row of rows) {
    const group = groupedRows.find((item) => item.ring_display === row.ring_display);
    if (group) group.rows.push(row);
    else groupedRows.push({ ring_display: row.ring_display, rows: [row] });
  }

  const ringSections = groupedRows.map(renderRingSection).join("");
  const printColumns = splitRingGroupsForPrint(groupedRows);
  const printLeftSections = printColumns.left.map(renderRingSection).join("");
  const printRightSections = printColumns.right.map(renderRingSection).join("");

  const html = `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Horseshowing Focus Schedule ${showNo} ${focusDay}</title>
  <style>
    body { margin: 0; font-family: Arial, Helvetica, sans-serif; color: #172026; background: #f6f7f8; }
    main { width: 100%; box-sizing: border-box; margin: 0; padding: 24px; }
    h1 { margin: 0; font-size: 24px; font-weight: 700; letter-spacing: 0; }
    .show-subtitle { margin: 3px 0 0; color: #263238; font-size: 18px; font-weight: 600; }
    .focus-date { margin: 4px 0 18px; color: #4c5961; font-size: 15px; }
    .updated-at { margin: 24px 0 0; color: #5c6870; font-size: 12px; }
    .ring-group { margin: 0 0 22px; }
    h2 { margin: 0; padding: 8px 10px; font-size: 16px; line-height: 1.25; letter-spacing: 0; background: #263238; color: #fff; }
    table { width: 100%; border-collapse: collapse; background: #fff; border: 1px solid #d9dee2; table-layout: fixed; }
    th, td { padding: 8px 10px; border-bottom: 1px solid #e4e8eb; text-align: left; vertical-align: top; font-size: 13px; line-height: 1.35; }
    th { background: #eef2f4; font-weight: 700; }
    tbody tr:nth-child(even) { background: #f8fafb; }
    .time-col-def,
    .number-col-def { width: calc(5ch + 8px); }
    .class-col-def { width: auto; }
    .schedule-table th.time-col,
    .schedule-table th.number-col { text-align: center; }
    .schedule-table th.class-head { text-align: left; }
    .schedule-table th.time-col,
    .schedule-table th.number-col,
    .schedule-table td.time-col,
    .schedule-table td.number-col { padding-left: 4px; padding-right: 4px; }
    .schedule-table td.time-col { text-align: right; white-space: nowrap; }
    .schedule-table td.number-col { text-align: right; white-space: nowrap; }
    .schedule-table td.class-col { width: auto; text-align: left; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; overflow-wrap: normal; word-break: normal; }
    @media print { body { background: #fff; } main { max-width: none; padding: 12px; } th { position: static; } }
  </style>
</head>
<body>
  <main>
    <h1>${htmlEscape(showHead)}</h1>
    ${showSubtitle ? `<div class="show-subtitle">${htmlEscape(showSubtitle)}</div>` : ""}
    <div class="focus-date">Day: ${htmlEscape(showFocusDate)}</div>
    ${ringSections}
    <footer class="updated-at">Last updated: ${htmlEscape(formatGeneratedAt(generatedAt))}</footer>
  </main>
</body>
</html>`;

  const htmlOut = path.join(outDir, `${showNo}-${focusDay}-focus-schedule.html`);
  fs.writeFileSync(htmlOut, html);

  const printHtml = `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Horseshowing Focus Schedule Print ${showNo} ${focusDay}</title>
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
    .schedule-table th.time-col,
    .schedule-table th.number-col { text-align: center; }
    .schedule-table th.class-head { text-align: left; }
    .schedule-table th.time-col,
    .schedule-table th.number-col,
    .schedule-table td.time-col,
    .schedule-table td.number-col { padding-left: 2px; padding-right: 2px; }
    .schedule-table td.time-col { text-align: right; white-space: nowrap; }
    .schedule-table td.number-col { text-align: right; white-space: nowrap; }
    .schedule-table td.class-col { width: auto; text-align: left; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; overflow-wrap: normal; word-break: normal; }
    @media screen { body { background: #f6f7f8; } main { width: 8.2in; min-height: 10.7in; margin: 0; padding: 0.15in; background: #fff; } }
    @media print { th { position: static; } }
  </style>
</head>
<body>
  <main>
    <h1>${htmlEscape(showHead)}</h1>
    ${showSubtitle ? `<div class="show-subtitle">${htmlEscape(showSubtitle)}</div>` : ""}
    <div class="focus-date">Day: ${htmlEscape(showFocusDate)}</div>
    <section class="print-columns" data-target-rows="${printColumns.targetRows}" data-left-rows="${printColumns.leftRows}" data-right-rows="${printColumns.rightRows}">
      <div class="print-column">${printLeftSections}</div>
      <div class="print-column">${printRightSections}</div>
    </section>
    <footer class="updated-at">Last updated: ${htmlEscape(formatGeneratedAt(generatedAt))}</footer>
  </main>
</body>
</html>`;

  const printHtmlOut = path.join(outDir, `${showNo}-${focusDay}-focus-schedule-print.html`);
  fs.writeFileSync(printHtmlOut, printHtml);
  console.log(JSON.stringify({
    source_rows: rawRows.length,
    visible_rows: rows.length,
    hidden_rows: hiddenRows,
    rings: ringCount,
    entries: totalEntries,
    missing_times: missingTimes,
    print_target_rows_per_column: printColumns.targetRows,
    print_left_rows: printColumns.leftRows,
    print_right_rows: printColumns.rightRows,
    csvOut,
    htmlOut,
    printHtmlOut
  }, null, 2));
}

main();
