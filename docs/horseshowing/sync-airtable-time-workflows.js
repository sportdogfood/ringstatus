const BASE_ID = process.env.WEC_AIRTABLE_BASE_ID || "app6XS1RvsPNRT6os";
const AIRTABLE_TOKEN = process.env.AIRTABLE_TOKEN;
const CATALYST_ENDPOINT = process.env.HORSESHOWING_CATALYST_ENDPOINT ||
  "https://horseshowing-700800454.development.catalystserverless.com/server/horseshowing_sync/";

const TABLES = {
  classStartTimes: "class_start_times",
  entryGoTimes: "entry_go_times",
  wecLogs: "wec-logs"
};

const TEXT_FIELDS = new Set([
  "class_start_key",
  "entry_go_key",
  "show_no",
  "focus_day",
  "ring_day_no",
  "class_number",
  "class_name",
  "class_start_time",
  "display_time",
  "source",
  "entry_no",
  "entry_order",
  "horse",
  "horse_display",
  "rider",
  "trainer",
  "trainer_display",
  "entry_go_time",
  "last_synced_at"
]);

const NUMBER_FIELDS = new Set([
  "ring_no",
  "class_no",
  "entry_count",
  "n_gone",
  "n_to_go",
  "elapsed_seconds",
  "pace_seconds",
  "time_till"
]);

function argValue(name, fallback = "") {
  const index = process.argv.indexOf(name);
  if (index >= 0 && process.argv[index + 1]) return process.argv[index + 1];
  return fallback;
}

function requireToken() {
  if (!AIRTABLE_TOKEN) throw new Error("AIRTABLE_TOKEN is required");
}

function clean(value) {
  return String(value ?? "").trim();
}

function intOrNull(value) {
  const parsed = Number.parseInt(clean(value), 10);
  return Number.isFinite(parsed) ? parsed : null;
}

function numberOrNull(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

async function airtableFetch(url, options = {}) {
  requireToken();
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

async function catalystGet(params) {
  const response = await fetch(`${CATALYST_ENDPOINT}?${params.toString()}`);
  const text = await response.text();
  if (!response.ok) throw new Error(`Catalyst ${response.status}: ${text}`);
  return JSON.parse(text);
}

async function getBaseTables() {
  return airtableFetch(`https://api.airtable.com/v0/meta/bases/${BASE_ID}/tables`);
}

async function ensureField(table, fieldsByName, fieldName) {
  if (fieldsByName.has(fieldName)) return false;
  const body = {
    name: fieldName,
    type: NUMBER_FIELDS.has(fieldName) ? "number" : "singleLineText"
  };
  if (body.type === "number") body.options = { precision: 0 };
  await airtableFetch(`https://api.airtable.com/v0/meta/bases/${BASE_ID}/tables/${table.id}/fields`, {
    method: "POST",
    body: JSON.stringify(body)
  });
  fieldsByName.set(fieldName, body);
  return true;
}

async function ensureFields(tableName, requiredFields) {
  const meta = await getBaseTables();
  const table = meta.tables.find((item) => item.name === tableName);
  if (!table) throw new Error(`Missing Airtable table ${tableName}`);
  const fieldsByName = new Map(table.fields.map((field) => [field.name, field]));
  let created = 0;
  for (const fieldName of requiredFields) {
    if (TEXT_FIELDS.has(fieldName) || NUMBER_FIELDS.has(fieldName)) {
      if (await ensureField(table, fieldsByName, fieldName)) created += 1;
    }
  }
  return { table, created };
}

async function upsertRecords(tableName, keyField, rows) {
  if (!rows.length) return { seen: 0, changed: 0 };
  let changed = 0;
  for (let index = 0; index < rows.length; index += 10) {
    const batch = rows.slice(index, index + 10);
    const payload = {
      performUpsert: { fieldsToMergeOn: [keyField] },
      records: batch.map((fields) => ({ fields })),
      typecast: true
    };
    await airtableFetch(`https://api.airtable.com/v0/${BASE_ID}/${encodeURIComponent(tableName)}`, {
      method: "PATCH",
      body: JSON.stringify(payload)
    });
    changed += batch.length;
  }
  return { seen: rows.length, changed };
}

async function writeLog({ checkName, showNo, focusDay, recordsSeen, recordsChanged, summary, payload }) {
  const createdAt = new Date().toISOString();
  const fields = {
    log_key: `${createdAt}|alerts|${checkName}`,
    created_at: createdAt,
    log_type: "alerts",
    check_name: checkName,
    workflow_lanes: "Alerts",
    show_no: showNo,
    focus_day: focusDay,
    status: "ok",
    records_seen: recordsSeen,
    records_changed: recordsChanged,
    summary,
    payload_json: JSON.stringify(payload).slice(0, 90000)
  };
  await airtableFetch(`https://api.airtable.com/v0/${BASE_ID}/${encodeURIComponent(TABLES.wecLogs)}`, {
    method: "POST",
    body: JSON.stringify({ fields, typecast: true })
  });
}

function buildClassStartRows(scheduleRows, nowIso) {
  return scheduleRows
    .filter((row) => clean(row.class_no) && clean(row.class_start_time))
    .map((row) => {
      const classNo = intOrNull(row.class_no);
      const focusDay = clean(row.show_day_key || row.show_days_display_date);
      const classStartKey = `${clean(row.show_id)}|${focusDay}|${clean(row.ring_day_no)}|${classNo}`;
      return {
        class_start_key: classStartKey,
        show_no: clean(row.show_id),
        focus_day: focusDay,
        ring_no: intOrNull(row.ring_number),
        ring_day_no: clean(row.ring_day_no),
        class_no: classNo,
        class_number: clean(row.class_number),
        class_name: clean(row.class_name),
        class_start_time: clean(row.class_start_time),
        display_time: clean(row.start_display),
        entry_count: intOrNull(row.entry_count),
        n_gone: intOrNull(row.n_gone),
        n_to_go: intOrNull(row.n_to_go),
        elapsed_seconds: intOrNull(row.elapsed_seconds),
        source: clean(row.live_source || "update_schedule.php"),
        last_synced_at: nowIso
      };
    });
}

function parseTime(focusDay, timeText) {
  if (!focusDay || !timeText) return null;
  const date = new Date(`${focusDay}T${timeText.length === 5 ? `${timeText}:00` : timeText}-04:00`);
  return Number.isNaN(date.getTime()) ? null : date;
}

function addSeconds(date, seconds) {
  return new Date(date.getTime() + seconds * 1000);
}

function buildEntryGoRows({ scheduleRows, classOogRows, activeTrainers, nowIso }) {
  const active = new Set(activeTrainers.map((item) => clean(item).toLowerCase()).filter(Boolean));
  const classByNo = new Map(scheduleRows.map((row) => [clean(row.class_no), row]));
  const rows = [];
  const now = new Date();
  for (const entry of classOogRows) {
    const trainer = clean(entry.trainer);
    if (!trainer || !active.has(trainer.toLowerCase())) continue;
    const classNo = clean(entry.class_no);
    const classRow = classByNo.get(classNo);
    if (!classRow || !clean(classRow.class_start_time)) continue;
    const focusDay = clean(classRow.show_day_key || classRow.show_days_display_date);
    const start = parseTime(focusDay, clean(classRow.class_start_time));
    if (!start) continue;
    const entryOrder = intOrNull(entry.entry_order);
    if (!entryOrder || entryOrder < 1) continue;
    const nGone = intOrNull(classRow.n_gone);
    const elapsedSeconds = intOrNull(classRow.elapsed_seconds);
    const paceSeconds = nGone && nGone > 6 && elapsedSeconds && elapsedSeconds > 0
      ? Math.max(30, Math.round(elapsedSeconds / nGone))
      : 120;
    const goTime = addSeconds(start, (entryOrder - 1) * paceSeconds);
    const entryNo = clean(entry.entry_no);
    rows.push({
      entry_go_key: `${clean(classRow.show_id)}|${focusDay}|${classNo}|${entryNo}`,
      show_no: clean(classRow.show_id),
      focus_day: focusDay,
      ring_no: intOrNull(classRow.ring_number || entry.ring_no),
      ring_day_no: clean(classRow.ring_day_no || entry.ring_day_no),
      class_no: intOrNull(classNo),
      class_number: clean(classRow.class_number),
      class_name: clean(classRow.class_name),
      entry_no: entryNo,
      entry_order: clean(entry.entry_order),
      horse: clean(entry.horse),
      horse_display: clean(entry.horse),
      rider: clean(entry.rider),
      trainer,
      trainer_display: trainer,
      class_start_time: clean(classRow.class_start_time),
      display_time: clean(classRow.start_display),
      entry_go_time: goTime.toTimeString().slice(0, 8),
      entry_count: intOrNull(classRow.entry_count),
      n_gone: nGone,
      elapsed_seconds: elapsedSeconds,
      pace_seconds: paceSeconds,
      time_till: Math.round(((goTime.getTime() - now.getTime()) / 60000) * 10) / 10,
      source: "class_oog.php|update_schedule.php",
      last_synced_at: nowIso
    });
  }
  return rows;
}

async function main() {
  const showNo = argValue("--show-no", process.env.WEC_SHOW_NO || "14906");
  const focusDay = argValue("--focus-day", process.env.WEC_FOCUS_DAY || "");
  if (!showNo) throw new Error("--show-no is required");

  const params = new URLSearchParams({ action: "schedule-json", show_no: showNo });
  if (focusDay) params.set("focus_day", focusDay);
  const scheduleRows = await catalystGet(params);
  const actualFocusDay = focusDay || clean(scheduleRows[0]?.show_day_key || scheduleRows[0]?.show_days_display_date);
  if (!actualFocusDay) throw new Error("focus_day could not be resolved");

  const snapshot = await catalystGet(new URLSearchParams({
    action: "focus-day-snapshot",
    show_no: showNo,
    focus_day: actualFocusDay
  }));
  const debug = await catalystGet(new URLSearchParams({
    action: "debug-show-config",
    show_no: showNo,
    focus_day: actualFocusDay
  }));
  const nowIso = new Date().toISOString();
  const activeTrainers = debug.focus_source?.active_trainers || [];

  const classStartRows = buildClassStartRows(scheduleRows, nowIso);
  const entryGoRows = buildEntryGoRows({
    scheduleRows,
    classOogRows: snapshot.class_oog || [],
    activeTrainers,
    nowIso
  });

  const classFields = Object.keys(classStartRows[0] || {
    class_start_key: "",
    show_no: "",
    focus_day: "",
    class_no: ""
  });
  const entryFields = Object.keys(entryGoRows[0] || {
    entry_go_key: "",
    show_no: "",
    focus_day: "",
    class_no: "",
    entry_no: ""
  });
  const classSchema = await ensureFields(TABLES.classStartTimes, classFields);
  const entrySchema = await ensureFields(TABLES.entryGoTimes, entryFields);
  const classResult = await upsertRecords(TABLES.classStartTimes, "class_start_key", classStartRows);
  const entryResult = await upsertRecords(TABLES.entryGoTimes, "entry_go_key", entryGoRows);

  await writeLog({
    checkName: "class_start_times",
    showNo,
    focusDay: actualFocusDay,
    recordsSeen: classResult.seen,
    recordsChanged: classResult.changed,
    summary: `class_start_times upserted=${classResult.changed} focus=${actualFocusDay}`,
    payload: { fields_created: classSchema.created, rows: classResult.seen }
  });
  await writeLog({
    checkName: "entry_go_times",
    showNo,
    focusDay: actualFocusDay,
    recordsSeen: entryResult.seen,
    recordsChanged: entryResult.changed,
    summary: `entry_go_times upserted=${entryResult.changed} active_trainers=${activeTrainers.length} focus=${actualFocusDay}`,
    payload: { fields_created: entrySchema.created, rows: entryResult.seen, active_trainers: activeTrainers }
  });

  console.log(JSON.stringify({
    ok: true,
    show_no: showNo,
    focus_day: actualFocusDay,
    class_start_times: classResult,
    entry_go_times: entryResult,
    fields_created: {
      class_start_times: classSchema.created,
      entry_go_times: entrySchema.created
    }
  }, null, 2));
}

main().catch((error) => {
  console.error(error.stack || error.message);
  process.exit(1);
});
