const fs = require("fs");
const path = require("path");

const BASE_ID = "app6XS1RvsPNRT6os";
const REPORT_DIR = path.join(__dirname, "reports");

function argValue(name, fallback = "") {
  const index = process.argv.indexOf(name);
  if (index >= 0 && process.argv[index + 1]) return process.argv[index + 1];
  return fallback;
}

function requiredToken() {
  const token = process.env.AIRTABLE_TOKEN || process.env.AIRTABLE_WEC_TOKEN;
  if (!token) throw new Error("AIRTABLE_TOKEN or AIRTABLE_WEC_TOKEN is required");
  return token;
}

const token = requiredToken();
const headers = { Authorization: `Bearer ${token}` };
const showNo = Number(argValue("--show-no", process.env.WEC_SHOW_NO || "14907"));
let focusDay = argValue("--focus-day", process.env.WEC_FOCUS_DAY || "");
const stamp = new Date().toISOString().replace(/[:.]/g, "-");

function airtableUrl(tableName) {
  return `https://api.airtable.com/v0/${BASE_ID}/${encodeURIComponent(tableName)}`;
}

async function airtableList(tableName, { formula = "", view = "", fields = [] } = {}) {
  const records = [];
  let offset = "";
  do {
    const url = new URL(airtableUrl(tableName));
    url.searchParams.set("pageSize", "100");
    if (formula) url.searchParams.set("filterByFormula", formula);
    if (view) url.searchParams.set("view", view);
    for (const field of fields) url.searchParams.append("fields[]", field);
    if (offset) url.searchParams.set("offset", offset);
    const response = await fetch(url, { headers });
    const body = await response.text();
    if (!response.ok) throw new Error(`${tableName} ${response.status}: ${body}`);
    const data = JSON.parse(body);
    records.push(...(data.records || []));
    offset = data.offset || "";
  } while (offset);
  return records;
}

function cell(fields, name) {
  const value = fields?.[name];
  if (Array.isArray(value)) {
    return value
      .map((item) => {
        if (item && typeof item === "object") return item.name || item.id || JSON.stringify(item);
        return String(item ?? "");
      })
      .filter(Boolean)
      .join("; ");
  }
  if (value && typeof value === "object") return value.name || value.id || JSON.stringify(value);
  return value == null ? "" : String(value);
}

function dateOnly(value) {
  return String(value || "").slice(0, 10);
}

function csvEscape(value) {
  const text = String(value ?? "");
  return /[",\n\r]/.test(text) ? `"${text.replace(/"/g, '""')}"` : text;
}

function toCsv(rows, columns) {
  return [
    columns.join(","),
    ...rows.map((row) => columns.map((column) => csvEscape(row[column])).join(","))
  ].join("\n");
}

function writeCsv(name, rows, columns) {
  const file = path.join(REPORT_DIR, `${name}-${showNo}-${focusDay}-${stamp}.csv`);
  fs.writeFileSync(file, toCsv(rows, columns));
  return file;
}

function escapeHtml(value) {
  return String(value ?? "").replace(/[&<>"']/g, (char) => ({
    "&": "&amp;",
    "<": "&lt;",
    ">": "&gt;",
    '"': "&quot;",
    "'": "&#39;"
  }[char]));
}

function renderTable(title, rows, columns) {
  return [
    `<section>`,
    `<h2>${escapeHtml(title)} <span>${rows.length}</span></h2>`,
    `<table><thead><tr>${columns.map((column) => `<th>${escapeHtml(column)}</th>`).join("")}</tr></thead>`,
    `<tbody>${rows.map((row) => `<tr>${columns.map((column) => `<td>${escapeHtml(row[column])}</td>`).join("")}</tr>`).join("")}</tbody></table>`,
    `</section>`
  ].join("");
}

async function resolveFocusDay() {
  const records = await airtableList("focus_show", { view: "active" });
  const matches = records.filter((record) => String(cell(record.fields, "show_no")) === String(showNo));
  if (matches.length !== 1) {
    throw new Error(`focus_show.active must have exactly one record for show_no=${showNo}; found ${matches.length}`);
  }
  const activeFocusDay = dateOnly(cell(matches[0].fields, "focus_day"));
  if (!activeFocusDay) throw new Error(`focus_show.active focus_day is blank for show_no=${showNo}`);
  if (focusDay && focusDay !== activeFocusDay) {
    throw new Error(`requested focus_day ${focusDay} does not match focus_show.active ${activeFocusDay}`);
  }
  focusDay = activeFocusDay;
  return focusDay;
}

async function fetchGetRingsSource() {
  const base = "https://www.horseshowing.com";
  const requestHeaders = {
    "user-agent": "Mozilla/5.0 (Linux; Android 15; Pixel 9) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/149.0.0.0 Mobile Safari/537.36",
    "accept-encoding": "identity",
    cookie: `HscomShowNo=${showNo}`
  };
  await fetch(`${base}/show.php?show=${showNo}`, { headers: requestHeaders });
  await fetch(`${base}/rings.php?show=${showNo}`, { headers: requestHeaders });
  const response = await fetch(`${base}/get_rings.php`, {
    method: "POST",
    headers: {
      ...requestHeaders,
      accept: "application/json, text/javascript, */*; q=0.01",
      origin: base,
      referer: `${base}/rings.php?show=${showNo}`,
      "x-requested-with": "XMLHttpRequest",
      "content-type": "application/x-www-form-urlencoded; charset=UTF-8"
    },
    body: `show_no=${showNo}`
  });
  const body = await response.text();
  let parsed = [];
  let parseError = "";
  try {
    parsed = JSON.parse(body);
  } catch (error) {
    parseError = error.message;
  }
  return {
    status: response.status,
    ok: response.ok && Array.isArray(parsed),
    rows: Array.isArray(parsed) ? parsed.length : 0,
    parse_error: parseError,
    sample: Array.isArray(parsed) && parsed[0] ? parsed[0] : null
  };
}

function countMissingLinks(rows, fields) {
  const missing = {};
  for (const field of fields) missing[field] = 0;
  for (const row of rows) {
    for (const field of fields) {
      if (!String(row[field] || "").trim()) missing[field] += 1;
    }
  }
  return missing;
}

function hasAnyMissing(missing) {
  return Object.values(missing).some((count) => count > 0);
}

async function main() {
  await resolveFocusDay();
  fs.mkdirSync(REPORT_DIR, { recursive: true });
  const getRingsSource = await fetchGetRingsSource();

  const lockedRecords = await airtableList("update_schedule_staging", { view: "lock_schedule" });
  const scopedIsoFormula = `AND({show_no}=${showNo}, IS_SAME({iso_date}, DATETIME_PARSE('${focusDay}','YYYY-MM-DD'), 'day'))`;
  const scopedFormula = `AND({show_no}=${showNo}, IS_SAME({focus_day}, DATETIME_PARSE('${focusDay}','YYYY-MM-DD'), 'day'))`;
  const updateScheduleRecords = await airtableList("update_schedule", { formula: scopedIsoFormula });
  const updateScheduleRows = updateScheduleRecords.map((record) => ({
    record_id: record.id,
    class_no: Number(cell(record.fields, "class_no")),
    ring_no: Number(cell(record.fields, "ring_no")),
    ring_day_no: Number(cell(record.fields, "ring_day_no")),
    ring_name: cell(record.fields, "ring_name"),
    time_text: cell(record.fields, "time_text"),
    event_name: cell(record.fields, "event_name"),
    entry_count: cell(record.fields, "entry_count")
  }));
  const stagingRecords = await airtableList("update_schedule_staging", { formula: scopedIsoFormula });
  const stagingRows = stagingRecords.map((record) => ({
    record_id: record.id,
    class_no: Number(cell(record.fields, "class_no")),
    ring_no: Number(cell(record.fields, "ring_no")),
    ring_day_no: Number(cell(record.fields, "ring_day_no")),
    ring_name: cell(record.fields, "ring_name"),
    time_text: cell(record.fields, "time_text"),
    event_name: cell(record.fields, "event_name"),
    entry_count: cell(record.fields, "entry_count"),
    lock: cell(record.fields, "lock"),
    shows: cell(record.fields, "shows"),
    classes: cell(record.fields, "classes"),
    ring_days: cell(record.fields, "ring_days"),
    rings: cell(record.fields, "rings"),
    show_days: cell(record.fields, "show_days"),
    events: cell(record.fields, "events"),
    focus_show: cell(record.fields, "focus_show")
  }));
  const lockedRows = lockedRecords
    .filter((record) => String(cell(record.fields, "show_no")) === String(showNo))
    .filter((record) => dateOnly(cell(record.fields, "iso_date")) === focusDay)
    .map((record) => ({
      record_id: record.id,
      class_no: Number(cell(record.fields, "class_no")),
      ring_no: Number(cell(record.fields, "ring_no")),
      ring_day_no: Number(cell(record.fields, "ring_day_no")),
      ring_name: cell(record.fields, "ring_name"),
      time_text: cell(record.fields, "time_text"),
      event_name: cell(record.fields, "event_name"),
      entry_count: cell(record.fields, "entry_count"),
      lock: cell(record.fields, "lock"),
      shows: cell(record.fields, "shows"),
      classes: cell(record.fields, "classes"),
      ring_days: cell(record.fields, "ring_days"),
      rings: cell(record.fields, "rings"),
      show_days: cell(record.fields, "show_days"),
      events: cell(record.fields, "events"),
      focus_show: cell(record.fields, "focus_show")
    }))
    .filter((row) => row.class_no > 0)
    .sort((a, b) => (a.ring_no - b.ring_no) || String(a.time_text).localeCompare(String(b.time_text)) || (a.class_no - b.class_no));

  const classStartRecords = await airtableList("class_start_times", { formula: scopedFormula });
  const classStartRows = classStartRecords
    .map((record) => ({
      record_id: record.id,
      class_start_key: cell(record.fields, "class_start_key"),
      show_no: cell(record.fields, "show_no"),
      focus_day: cell(record.fields, "focus_day"),
      ring_no: cell(record.fields, "ring_no"),
      ring_day_no: cell(record.fields, "ring_day_no"),
      class_no: Number(cell(record.fields, "class_no")),
      display_time: cell(record.fields, "display_time"),
      class_start_time: cell(record.fields, "class_start_time"),
      class_number: cell(record.fields, "class_number"),
      class_name: cell(record.fields, "class_name"),
      entry_count: cell(record.fields, "entry_count"),
      status: cell(record.fields, "status"),
      source: cell(record.fields, "source"),
      update_schedule_staging: cell(record.fields, "update_schedule_staging"),
      class_oog: cell(record.fields, "class_oog")
    }))
    .sort((a, b) => (Number(a.ring_no) - Number(b.ring_no)) || String(a.display_time).localeCompare(String(b.display_time)) || (a.class_no - b.class_no));

  const classOogRecords = await airtableList("class_oog", { formula: scopedFormula });
  const classOogRows = classOogRecords
    .map((record) => ({
      record_id: record.id,
      class_oog_key: cell(record.fields, "class_oog_key"),
      mirror_class_oog_key: cell(record.fields, "mirror_class_oog_key"),
      show_no: cell(record.fields, "show_no"),
      focus_day: cell(record.fields, "focus_day"),
      ring_no: cell(record.fields, "ring_no"),
      ring: cell(record.fields, "ring"),
      ring_day_no: cell(record.fields, "days") || cell(record.fields, "ring_day_no"),
      class_no: Number(cell(record.fields, "class_no")),
      class_label: cell(record.fields, "class_label"),
      entry_order: Number(cell(record.fields, "entry_order")),
      entry_no: cell(record.fields, "entry_no"),
      horse: cell(record.fields, "horse"),
      horse_display: cell(record.fields, "horse_display (from horses)"),
      rider: cell(record.fields, "rider"),
      trainer: cell(record.fields, "trainer"),
      source: cell(record.fields, "source"),
      update_schedule_staging: cell(record.fields, "update_schedule_staging"),
      class_start_times: cell(record.fields, "class_start_times"),
      lock_from_update_schedule_staging: cell(record.fields, "lock (from update_schedule_staging)")
    }))
    .sort((a, b) => (Number(a.ring_no) - Number(b.ring_no)) || (a.class_no - b.class_no) || (a.entry_order - b.entry_order));
  const getRingsRecords = await airtableList("get_rings", { formula: scopedFormula });
  const getRingsRows = getRingsRecords.map((record) => ({
    record_id: record.id,
    get_rings_key_mirror: cell(record.fields, "get_rings_key_mirror"),
    show_no: cell(record.fields, "show_no"),
    focus_day: cell(record.fields, "focus_day"),
    ring_no: cell(record.fields, "ring_no"),
    ring_day_no: cell(record.fields, "ring_day_no"),
    class_no: Number(cell(record.fields, "class_no")),
    class_text: cell(record.fields, "class_text"),
    entry_text: cell(record.fields, "entry_text"),
    total: cell(record.fields, "total"),
    n_gone: cell(record.fields, "n_gone"),
    n_to_go: cell(record.fields, "n_to_go"),
    elapsed: cell(record.fields, "elapsed"),
    timestamp: cell(record.fields, "timestamp"),
    shows: cell(record.fields, "shows"),
    classes: cell(record.fields, "classes"),
    rings: cell(record.fields, "rings"),
    ring_days: cell(record.fields, "ring_days"),
    entries: cell(record.fields, "entries"),
    focus_show: cell(record.fields, "focus_show")
  }));

  const entryGoRecords = await airtableList("entry_go_times", { formula: scopedFormula });
  const entryGoRows = entryGoRecords
    .map((record) => ({
      record_id: record.id,
      entry_go_key: cell(record.fields, "entry_go_key"),
      entry_go_key_mirror: cell(record.fields, "entry_go_key_mirror"),
      show_no: cell(record.fields, "show_no"),
      focus_day: cell(record.fields, "focus_day"),
      status: cell(record.fields, "status"),
      ring_no: cell(record.fields, "ring_no"),
      ring_day_no: cell(record.fields, "ring_day_no"),
      class_no: Number(cell(record.fields, "class_no")),
      entry_no: cell(record.fields, "entry_no"),
      entry_order: Number(cell(record.fields, "entry_order")),
      horse: cell(record.fields, "horse"),
      horse_display: cell(record.fields, "horse_display"),
      rider: cell(record.fields, "rider"),
      trainer: cell(record.fields, "trainer"),
      trainer_display: cell(record.fields, "trainer_display"),
      class_start_time: cell(record.fields, "class_start_time"),
      entry_go_time: cell(record.fields, "entry_go_time"),
      pace_seconds: cell(record.fields, "pace_seconds"),
      n_gone: cell(record.fields, "n_gone"),
      elapsed_seconds: cell(record.fields, "elapsed_seconds"),
      source: cell(record.fields, "source"),
      shows: cell(record.fields, "shows"),
      focus_show: cell(record.fields, "focus_show"),
      classes: cell(record.fields, "classes"),
      rings: cell(record.fields, "rings"),
      ring_days: cell(record.fields, "ring_days"),
      entries: cell(record.fields, "entries"),
      horses: cell(record.fields, "horses"),
      riders: cell(record.fields, "riders"),
      trainers: cell(record.fields, "trainers"),
      class_oog: cell(record.fields, "class_oog"),
      class_start_times: cell(record.fields, "class_start_times")
    }))
    .sort((a, b) => (Number(a.ring_no) - Number(b.ring_no)) || (a.class_no - b.class_no) || (a.entry_order - b.entry_order));

  const lockedClassNos = new Set(lockedRows.map((row) => String(row.class_no)));
  const classStartClassNos = new Set(classStartRows.map((row) => String(row.class_no)));
  const oogClassNos = new Set(classOogRows.map((row) => String(row.class_no)));
  const activeEntryGoRows = entryGoRows.filter((row) => row.status === "active");
  const classOogEntryKeys = new Set(classOogRows.map((row) => `${row.class_no}|${row.entry_no}`));
  const activeEntryGoKeys = new Set(activeEntryGoRows.map((row) => `${row.class_no}|${row.entry_no}`));
  const stagingMissingLinks = countMissingLinks(lockedRows, ["shows", "classes", "ring_days", "rings", "show_days", "events", "focus_show"]);
  const getRingsMissingLinks = countMissingLinks(getRingsRows, ["shows", "classes", "rings", "ring_days", "entries", "focus_show"]);
  const entryGoMissingLinks = countMissingLinks(activeEntryGoRows, ["shows", "focus_show", "classes", "rings", "ring_days", "entries", "horses", "riders", "trainers", "class_oog", "class_start_times"]);

  const summary = {
    show_no: showNo,
    focus_day: focusDay,
    get_rings_source_status: getRingsSource.status,
    get_rings_source_rows: getRingsSource.rows,
    get_rings_mirror_rows: getRingsRows.length,
    get_rings_rows_missing_links: getRingsMissingLinks,
    update_schedule_rows: updateScheduleRows.length,
    update_schedule_staging_rows: stagingRows.length,
    update_schedule_staging_lock_schedule_rows: lockedRows.length,
    update_schedule_staging_locked_missing_links: stagingMissingLinks,
    class_start_times_rows: classStartRows.length,
    class_start_times_missing_from_locked: lockedRows.filter((row) => !classStartClassNos.has(String(row.class_no))).map((row) => row.class_no),
    class_start_times_extra_vs_locked: classStartRows.filter((row) => !lockedClassNos.has(String(row.class_no))).map((row) => row.class_no),
    class_oog_rows: classOogRows.length,
    class_oog_unique_classes: [...oogClassNos].map(Number).sort((a, b) => a - b),
    class_oog_rows_missing_update_schedule_staging_link: classOogRows.filter((row) => !row.update_schedule_staging).length,
    class_oog_rows_missing_class_start_times_link: classOogRows.filter((row) => !row.class_start_times).length,
    class_oog_rows_missing_lock_lookup: classOogRows.filter((row) => !row.lock_from_update_schedule_staging).length,
    entry_go_times_rows: entryGoRows.length,
    entry_go_times_active_rows: activeEntryGoRows.length,
    entry_go_times_inactive_rows: entryGoRows.length - activeEntryGoRows.length,
    entry_go_times_missing_links: entryGoMissingLinks,
    entry_go_times_missing_from_class_oog: [...classOogEntryKeys].filter((key) => !activeEntryGoKeys.has(key)),
    entry_go_times_extra_vs_class_oog: [...activeEntryGoKeys].filter((key) => !classOogEntryKeys.has(key)),
    entry_go_times_missing_go_time: activeEntryGoRows.filter((row) => !row.entry_go_time).length
  };

  const classStartColumns = ["display_time", "class_no", "class_number", "class_name", "ring_no", "ring_day_no", "entry_count", "status", "update_schedule_staging", "class_oog", "record_id"];
  const classOogColumns = ["class_no", "entry_order", "entry_no", "horse", "horse_display", "rider", "trainer", "ring_no", "ring", "class_label", "update_schedule_staging", "class_start_times", "lock_from_update_schedule_staging", "record_id"];
  const entryGoColumns = ["status", "entry_go_time", "class_no", "entry_order", "entry_no", "horse_display", "rider", "trainer_display", "ring_no", "ring_day_no", "pace_seconds", "n_gone", "elapsed_seconds", "source", "class_oog", "class_start_times", "record_id"];
  const lockedColumns = ["class_no", "ring_no", "ring_day_no", "ring_name", "time_text", "event_name", "entry_count", "lock", "shows", "classes", "ring_days", "rings", "show_days", "events", "focus_show", "record_id"];
  const getRingsColumns = ["class_no", "ring_no", "ring_day_no", "class_text", "entry_text", "total", "n_gone", "n_to_go", "elapsed", "timestamp", "shows", "classes", "rings", "ring_days", "entries", "focus_show", "record_id"];
  const updateScheduleColumns = ["class_no", "ring_no", "ring_day_no", "ring_name", "time_text", "event_name", "entry_count", "record_id"];

  const classStartCsv = writeCsv("class-start-times-live", classStartRows, classStartColumns);
  const oogCsv = writeCsv("class-oog-live", classOogRows, classOogColumns);
  const lockedCsv = writeCsv("update-schedule-staging-lock-schedule-live", lockedRows, lockedColumns);
  const getRingsCsv = writeCsv("get-rings-live", getRingsRows, getRingsColumns);
  const entryGoCsv = writeCsv("entry-go-times-live", entryGoRows, entryGoColumns);
  const updateScheduleCsv = writeCsv("update-schedule-live", updateScheduleRows, updateScheduleColumns);
  const summaryFile = path.join(REPORT_DIR, `class-stage-audit-${showNo}-${focusDay}-${stamp}.json`);

  const htmlFile = path.join(REPORT_DIR, `class-stage-audit-${showNo}-${focusDay}-${stamp}.html`);
  const html = `<!doctype html><html><head><meta charset="utf-8"><title>WEC class stage audit</title><style>
body{font-family:Arial,sans-serif;margin:24px;color:#172027}h1{font-size:28px;margin:0 0 6px}p{margin:0 0 18px;color:#52606b}.summary{display:grid;grid-template-columns:repeat(4,minmax(140px,1fr));gap:10px;margin:18px 0}.card{border:1px solid #d5dde3;padding:10px;background:#f8fafb}.card b{display:block;font-size:20px}section{margin-top:24px}h2{font-size:18px;margin:0 0 8px}h2 span{font-weight:400;color:#52606b}table{border-collapse:collapse;width:100%;font-size:12px}th,td{border:1px solid #d7dee4;padding:5px 7px;text-align:left;vertical-align:top}th{background:#eef2f5;position:sticky;top:0}tbody tr:nth-child(even){background:#fafafa}.fail{color:#b42318;font-weight:700}.pass{color:#067647;font-weight:700}
</style></head><body><h1>WEC ${showNo} / ${focusDay}</h1><p>Repeatable Airtable export for focus_day, get_rings, update_schedule, update_schedule_staging, class_start_times, class_oog, and entry_go_times.</p><div class="summary"><div class="card"><b>${summary.get_rings_source_rows}</b>get_rings source rows</div><div class="card"><b>${summary.get_rings_mirror_rows}</b>get_rings mirror rows</div><div class="card"><b>${summary.update_schedule_rows}</b>update_schedule rows</div><div class="card"><b>${summary.update_schedule_staging_rows}</b>staging rows</div><div class="card"><b>${summary.update_schedule_staging_lock_schedule_rows}</b>locked staging rows</div><div class="card"><b>${summary.class_start_times_rows}</b>class_start_times rows</div><div class="card"><b>${summary.class_oog_rows}</b>class_oog rows</div><div class="card"><b>${summary.class_oog_unique_classes.length}</b>class_oog classes</div><div class="card"><b>${summary.entry_go_times_active_rows}</b>active entry_go_times</div></div>${renderTable("get_rings", getRingsRows, getRingsColumns)}${renderTable("update_schedule", updateScheduleRows, updateScheduleColumns)}${renderTable("update_schedule_staging lock_schedule", lockedRows, lockedColumns)}${renderTable("class_start_times", classStartRows, classStartColumns)}${renderTable("class_oog", classOogRows, classOogColumns)}${renderTable("entry_go_times", entryGoRows, entryGoColumns)}</body></html>`;

  const failures = [];
  if (!getRingsSource.ok) failures.push("get_rings source probe failed");
  if (getRingsSource.rows > 0 && getRingsRows.length < 1) failures.push("get_rings source has rows but Airtable mirror is empty");
  if (getRingsRows.length && hasAnyMissing(getRingsMissingLinks)) failures.push("get_rings rows missing helper links");
  if (updateScheduleRows.length < 1) failures.push("update_schedule has no focus-day rows");
  if (stagingRows.length < 1) failures.push("update_schedule_staging has no focus-day rows");
  if (hasAnyMissing(stagingMissingLinks)) failures.push("update_schedule_staging locked rows missing helper links");
  if (summary.class_start_times_missing_from_locked.length) failures.push("class_start_times missing locked class_no");
  if (summary.class_start_times_extra_vs_locked.length) failures.push("class_start_times has extra class_no outside locked staging");
  if (summary.class_oog_rows_missing_update_schedule_staging_link) failures.push("class_oog rows missing update_schedule_staging link");
  if (summary.class_oog_rows_missing_class_start_times_link) failures.push("class_oog rows missing class_start_times link");
  if (summary.class_oog_rows_missing_lock_lookup) failures.push("class_oog rows missing lock lookup");
  if (summary.entry_go_times_missing_from_class_oog.length) failures.push("entry_go_times missing class_oog class+entry rows");
  if (summary.entry_go_times_extra_vs_class_oog.length) failures.push("entry_go_times has extra active class+entry rows");
  if (hasAnyMissing(entryGoMissingLinks)) failures.push("active entry_go_times rows missing helper links");
  if (summary.entry_go_times_missing_go_time) failures.push("active entry_go_times rows missing entry_go_time");

  const payload = {
    ok: failures.length === 0,
    failures,
    summary,
    files: { htmlFile, classStartCsv, oogCsv, entryGoCsv, lockedCsv, getRingsCsv, updateScheduleCsv, summaryFile }
  };

  fs.writeFileSync(summaryFile, JSON.stringify(payload, null, 2));
  fs.writeFileSync(htmlFile, html);
  console.log(JSON.stringify(payload, null, 2));
  if (failures.length) process.exit(1);
}

main().catch((error) => {
  console.error(error.stack || error.message);
  process.exit(1);
});
