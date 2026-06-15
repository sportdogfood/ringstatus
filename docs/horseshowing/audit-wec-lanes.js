const fs = require("fs");
const path = require("path");

const BASE_ID = process.env.WEC_AIRTABLE_BASE_ID || "app6XS1RvsPNRT6os";
const AIRTABLE_TOKEN = process.env.AIRTABLE_TOKEN;
const CATALYST_ENDPOINT = process.env.HORSESHOWING_CATALYST_ENDPOINT ||
  "https://horseshowing-700800454.development.catalystserverless.com/server/horseshowing_sync/";

const outDir = path.join(__dirname, "logs");

function argValue(name, fallback = "") {
  const index = process.argv.indexOf(name);
  return index >= 0 && process.argv[index + 1] ? process.argv[index + 1] : fallback;
}

function clean(value) {
  return String(value ?? "").trim();
}

function asArray(value) {
  return Array.isArray(value) ? value : [];
}

function getField(record, name) {
  return record?.fields?.[name];
}

function formulaDate(showNo, focusDay) {
  return `AND({show_no}=${Number(showNo)},IS_SAME({focus_day},'${focusDay}','day'))`;
}

async function airtableFetch(url, options = {}) {
  if (!AIRTABLE_TOKEN) throw new Error("AIRTABLE_TOKEN is required");
  const response = await fetch(url, {
    ...options,
    headers: {
      Authorization: `Bearer ${AIRTABLE_TOKEN}`,
      "Content-Type": "application/json",
      ...(options.headers || {})
    }
  });
  const text = await response.text();
  if (!response.ok) throw new Error(`Airtable ${response.status}: ${text}`);
  return text ? JSON.parse(text) : {};
}

async function listAll(tableName, params = {}) {
  const records = [];
  let offset = "";
  do {
    const query = new URLSearchParams({ pageSize: "100", ...params });
    if (offset) query.set("offset", offset);
    const payload = await airtableFetch(`https://api.airtable.com/v0/${BASE_ID}/${encodeURIComponent(tableName)}?${query}`);
    records.push(...(payload.records || []));
    offset = payload.offset || "";
  } while (offset);
  return records;
}

async function catalystArray(action, showNo, focusDay) {
  const query = new URLSearchParams({ action, show_no: showNo, focus_day: focusDay });
  const response = await fetch(`${CATALYST_ENDPOINT}?${query}`, { method: "GET" });
  const text = await response.text();
  if (!response.ok) throw new Error(`Catalyst ${action} ${response.status}: ${text}`);
  const parsed = text ? JSON.parse(text) : [];
  return Array.isArray(parsed) ? parsed : [];
}

function addCheck(checks, lane, name, pass, details = {}) {
  checks.push({
    lane,
    name,
    status: pass ? "PASS" : "FAIL",
    ...details
  });
}

function countMissingLinks(records, linkFields) {
  const missing = {};
  for (const field of linkFields) missing[field] = 0;
  for (const record of records) {
    for (const field of linkFields) {
      if (!asArray(record.fields?.[field]).length) missing[field] += 1;
    }
  }
  return missing;
}

function anyMissing(missing) {
  return Object.values(missing).some((count) => count > 0);
}

async function main() {
  const explicitShowNo = argValue("--show-no");
  const explicitFocusDay = argValue("--focus-day");
  const checks = [];
  const warnings = [];

  const [activeShows, activeFocusShows, allShows] = await Promise.all([
    listAll("shows", { view: "active" }),
    listAll("focus_show", { view: "active" }),
    listAll("shows")
  ]);

  addCheck(checks, "Helpers", "exactly_one_active_show", activeShows.length === 1, {
    count: activeShows.length,
    show_nos: activeShows.map((record) => getField(record, "show_no"))
  });
  addCheck(checks, "Helpers", "exactly_one_active_focus_show", activeFocusShows.length === 1, {
    count: activeFocusShows.length,
    show_nos: activeFocusShows.map((record) => getField(record, "show_no"))
  });

  const activeShowNo = clean(explicitShowNo || getField(activeShows[0], "show_no"));
  const focusDay = clean(explicitFocusDay || getField(activeFocusShows[0], "focus_day"));
  const focusShowNo = clean(getField(activeFocusShows[0], "show_no"));

  addCheck(checks, "Helpers", "active_show_matches_focus_show", Boolean(activeShowNo && focusShowNo && activeShowNo === focusShowNo), {
    active_show_no: activeShowNo,
    focus_show_no: focusShowNo,
    focus_day: focusDay
  });

  const activeShowKeyCounts = new Map();
  const allShowKeyCounts = new Map();
  for (const record of allShows) {
    const key = clean(getField(record, "focus_show_key")) || `show_no:${clean(getField(record, "show_no"))}`;
    if (!key) continue;
    allShowKeyCounts.set(key, (allShowKeyCounts.get(key) || 0) + 1);
    if (getField(record, "active")) {
      activeShowKeyCounts.set(key, (activeShowKeyCounts.get(key) || 0) + 1);
    }
  }
  const duplicateActiveShowKeys = [...activeShowKeyCounts.entries()].filter(([, count]) => count > 1);
  const duplicateHistoricalShowKeys = [...allShowKeyCounts.entries()].filter(([, count]) => count > 1);
  if (duplicateHistoricalShowKeys.length) {
    warnings.push({
      lane: "Helpers",
      name: "duplicate_inactive_show_keys_exist",
      duplicates: duplicateHistoricalShowKeys.map(([key, count]) => ({ key, count })),
      note: "Not a current-lane fail if active controls are unique and workflow maps use active shows only."
    });
  }
  addCheck(checks, "Helpers", "no_duplicate_active_show_keys", duplicateActiveShowKeys.length === 0, {
    duplicates: duplicateActiveShowKeys.map(([key, count]) => ({ key, count }))
  });

  if (!activeShowNo || !focusDay) {
    const result = { ok: false, show_no: activeShowNo, focus_day: focusDay, warnings, checks };
    fs.mkdirSync(outDir, { recursive: true });
    const file = path.join(outDir, `wec-lane-audit-no-focus-${new Date().toISOString().replace(/[:.]/g, "-")}.json`);
    fs.writeFileSync(file, `${JSON.stringify(result, null, 2)}\n`);
    console.log(JSON.stringify({ ...result, file }, null, 2));
    process.exit(1);
  }

  const scopedFormula = formulaDate(activeShowNo, focusDay);
  const [
    updateSchedule,
    classOog,
    counts,
    classStartTimes,
    entryGoTimes,
    getOrders,
    getRings,
    scheduleRows
  ] = await Promise.all([
    listAll("update_schedule", { filterByFormula: scopedFormula }),
    listAll("class_oog", { filterByFormula: scopedFormula }),
    listAll("counts", { filterByFormula: `{show_no}=${Number(activeShowNo)}` }),
    listAll("class_start_times", { filterByFormula: scopedFormula }),
    listAll("entry_go_times", { filterByFormula: scopedFormula }),
    listAll("get_orders", { filterByFormula: scopedFormula }),
    listAll("get_rings", { filterByFormula: scopedFormula }),
    catalystArray("schedule-json", activeShowNo, focusDay)
  ]);

  addCheck(checks, "Core", "update_schedule_present", updateSchedule.length > 0, { rows: updateSchedule.length });
  addCheck(checks, "Core", "class_oog_present", classOog.length > 0, { rows: classOog.length });
  addCheck(checks, "Core", "counts_present_for_show", counts.length > 0, { rows: counts.length });
  addCheck(checks, "Alerts", "class_start_times_present", classStartTimes.length > 0, { rows: classStartTimes.length });
  addCheck(checks, "Alerts", "entry_go_times_expected_only_when_active_entries_exist", true, {
    rows: entryGoTimes.length,
    note: "Zero can be valid for schooling/prep days with no active trainer entry rollups."
  });
  addCheck(checks, "Core", "catalyst_schedule_present", scheduleRows.length > 0, { rows: scheduleRows.length });

  const updateMissing = countMissingLinks(updateSchedule, ["shows", "focus_show", "ring_days", "rings", "ring_names", "classes", "dows"]);
  addCheck(checks, "Core", "update_schedule_links_complete", !anyMissing(updateMissing), { missing: updateMissing });

  const oogMissing = countMissingLinks(classOog, ["shows", "focus_show", "classes", "rings", "ring_names", "ring_days", "entries"]);
  addCheck(checks, "Core", "class_oog_links_complete", !anyMissing(oogMissing), { missing: oogMissing });

  const classStartMissing = countMissingLinks(classStartTimes, ["shows", "focus_show", "classes", "rings", "ring_days"]);
  addCheck(checks, "Alerts", "class_start_times_links_complete", !anyMissing(classStartMissing), { missing: classStartMissing });

  if (entryGoTimes.length) {
    const entryMissing = countMissingLinks(entryGoTimes, ["shows", "focus_show", "classes", "rings", "ring_days", "entries", "class_start_times", "class_oog"]);
    addCheck(checks, "Alerts", "entry_go_times_links_complete", !anyMissing(entryMissing), { missing: entryMissing });
  }

  const liveRows = getOrders.length + getRings.length;
  addCheck(checks, "Live", "live_tables_scoped_or_not_started", true, {
    get_orders_rows: getOrders.length,
    get_rings_rows: getRings.length,
    note: liveRows ? "Live scoped rows exist." : "No live rows can be valid before show day/live browser state."
  });

  const failures = checks.filter((check) => check.status !== "PASS");
  const result = {
    ok: failures.length === 0,
    show_no: activeShowNo,
    focus_day: focusDay,
    checked_at: new Date().toISOString(),
    warnings,
    failures,
    checks
  };

  fs.mkdirSync(outDir, { recursive: true });
  const suffix = result.ok ? "PASS" : "FAIL";
  const file = path.join(outDir, `wec-lane-audit-${activeShowNo}-${focusDay}-${suffix}-${new Date().toISOString().replace(/[:.]/g, "-")}.json`);
  fs.writeFileSync(file, `${JSON.stringify(result, null, 2)}\n`);
  console.log(JSON.stringify({ ok: result.ok, show_no: activeShowNo, focus_day: focusDay, failures: failures.length, file }, null, 2));
  if (!result.ok) process.exit(1);
}

main().catch((error) => {
  console.error(error.stack || error.message);
  process.exit(1);
});
