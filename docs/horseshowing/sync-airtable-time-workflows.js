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
  "class_start_key_mirror",
  "entry_go_key",
  "entry_go_key_mirror",
  "show_no",
  "focus_day",
  "class_number",
  "class_name",
  "class_start_time",
  "display_time",
  "source",
  "horse",
  "horse_display",
  "rider",
  "trainer",
  "trainer_display",
  "entry_go_time",
  "current_entry_no",
  "current_horse",
  "live_source",
  "status",
  "inactive_reason",
  "inactive_at",
  "last_synced_at"
]);

const NUMBER_FIELDS = new Set([
  "ring_no",
  "ring_day_no",
  "class_no",
  "entry_no",
  "entry_order",
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

function classStartTimeFromText(value) {
  const raw = clean(value).toUpperCase().replace(/\s+/g, "");
  if (!raw || raw === "CHECKTIME") return "";
  const match = raw.match(/^(\d{1,2})(?::?(\d{2}))?(AM|PM|A|P)?$/);
  if (!match) return "";
  let hour = Number(match[1]);
  const minute = Number(match[2] || "00");
  const suffix = match[3] || "";
  if (!Number.isFinite(hour) || !Number.isFinite(minute)) return "";
  if ((suffix === "PM" || suffix === "P") && hour < 12) hour += 12;
  if ((suffix === "AM" || suffix === "A") && hour === 12) hour = 0;
  return `${String(hour).padStart(2, "0")}:${String(minute).padStart(2, "0")}:00`;
}

function parseClassLabel(value, fallbackClassNo = "") {
  const label = clean(value);
  const match = label.match(/^(\d+)\)\s*(.*)$/);
  return {
    class_number: match?.[1] || clean(fallbackClassNo),
    class_name: match?.[2] ? clean(match[2]) : label
  };
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

async function listRecords(tableName, formula) {
  const records = [];
  let offset = "";
  do {
    const params = new URLSearchParams({ pageSize: "100" });
    if (formula) params.set("filterByFormula", formula);
    if (offset) params.set("offset", offset);
    const result = await airtableFetch(`https://api.airtable.com/v0/${BASE_ID}/${encodeURIComponent(tableName)}?${params.toString()}`);
    records.push(...(result.records || []));
    offset = result.offset || "";
  } while (offset);
  return records;
}

function inactiveRecordUpdates({ existingRows, keyField, activeKeys, reason, nowIso }) {
  const updates = [];
  for (const row of existingRows || []) {
    const key = clean(row.fields?.[keyField]);
    if (!key || activeKeys.has(key)) continue;
    updates.push({
      id: row.id,
      fields: {
        status: "inactive",
        inactive_reason: reason,
        inactive_at: nowIso
      }
    });
  }
  return updates;
}

async function patchRecords(tableName, records) {
  let changed = 0;
  for (let index = 0; index < records.length; index += 10) {
    const batch = records.slice(index, index + 10);
    await airtableFetch(`https://api.airtable.com/v0/${BASE_ID}/${encodeURIComponent(tableName)}`, {
      method: "PATCH",
      body: JSON.stringify({ records: batch, typecast: true })
    });
    changed += batch.length;
  }
  return changed;
}

async function markInactiveMissingRows({ tableName, keyField, showNo, focusDay, activeKeys, reason, nowIso }) {
  const showValue = Number.isFinite(Number(showNo)) ? Number(showNo) : `'${String(showNo).replace(/'/g, "\\'")}'`;
  const formula = `AND({focus_day}='${String(focusDay).replace(/'/g, "\\'")}',{show_no}=${showValue})`;
  const existingRows = await listRecords(tableName, formula);
  const updates = inactiveRecordUpdates({ existingRows, keyField, activeKeys, reason, nowIso });
  return {
    seen: existingRows.length,
    inactive: await patchRecords(tableName, updates)
  };
}

async function writeLog({ checkName, showNo, focusDay, recordsSeen, recordsChanged, summary, payload }) {
  const createdAt = new Date().toISOString();
  const fields = {
    log_key_run: `${createdAt}|alerts|${checkName}`,
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
        class_start_key_mirror: classStartKey,
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
        status: "active",
        inactive_reason: null,
        inactive_at: null,
        last_synced_at: nowIso
      };
    });
}

function buildClassStartRowsFromCoreSnapshot(updateScheduleRows, countRows, nowIso) {
  const countsByClass = new Map(
    (countRows || [])
      .filter((row) => clean(row.class_no))
      .map((row) => [clean(row.class_no), intOrNull(row.entry_count)])
  );
  return (updateScheduleRows || [])
    .map((row) => {
      const classNo = intOrNull(row.class_no);
      const focusDay = clean(row.focus_day || row.date_text || row.iso_date);
      const classStartTime = clean(row.time) || classStartTimeFromText(row.time_text);
      if (!classNo || !focusDay || !classStartTime) return null;
      const classStartKey = `${clean(row.show_no)}|${focusDay}|${clean(row.ring_day_no)}|${classNo}`;
      const classParts = parseClassLabel(row.event_name || row.class_name, classNo);
      const countsEntryCount = countsByClass.get(String(classNo));
      const entryCount = countsEntryCount ?? intOrNull(row.entry_count) ?? null;
      return {
        class_start_key: classStartKey,
        class_start_key_mirror: classStartKey,
        show_no: clean(row.show_no),
        focus_day: focusDay,
        ring_no: intOrNull(row.ring_no),
        ring_day_no: clean(row.ring_day_no),
        class_no: classNo,
        class_number: clean(row.class_number || classParts.class_number),
        class_name: clean(row.class_name || classParts.class_name),
        class_start_time: classStartTime,
        display_time: clean(row.time_text),
        entry_count: entryCount,
        source: countsEntryCount === null || countsEntryCount === undefined
          ? clean(row.source || "update_schedule.php")
          : "update_schedule.php|counts.php",
        status: "active",
        inactive_reason: null,
        inactive_at: null,
        last_synced_at: nowIso
      };
    })
    .filter(Boolean);
}

function parseTime(focusDay, timeText) {
  if (!focusDay || !timeText) return null;
  const date = new Date(`${focusDay}T${timeText.length === 5 ? `${timeText}:00` : timeText}-04:00`);
  return Number.isNaN(date.getTime()) ? null : date;
}

function addSeconds(date, seconds) {
  return new Date(date.getTime() + seconds * 1000);
}

function liveClassKey(row) {
  return `${clean(row.show_no || row.show_id)}|${clean(row.focus_day || row.show_day_key || row.show_days_display_date)}|${clean(row.class_no)}`;
}

function paceFromLive(row) {
  const nGone = intOrNull(row.n_gone);
  const elapsedSeconds = intOrNull(row.elapsed_seconds);
  return nGone && nGone > 6 && elapsedSeconds && elapsedSeconds > 0
    ? Math.max(30, Math.round(elapsedSeconds / nGone))
    : null;
}

function applyLiveTimingToClassRows(classRows, liveRows) {
  const liveByClass = new Map();
  for (const liveRow of liveRows || []) {
    const key = liveClassKey(liveRow);
    if (!key.endsWith("|")) liveByClass.set(key, liveRow);
  }
  return (classRows || []).map((row) => {
    const live = liveByClass.get(liveClassKey(row));
    if (!live) return row;
    const paceSeconds = paceFromLive(live);
    const sources = new Set(
      clean(row.source)
        .split("|")
        .map((item) => clean(item))
        .filter(Boolean)
    );
    if (clean(live.live_source)) sources.add(clean(live.live_source));
    return {
      ...row,
      n_gone: intOrNull(live.n_gone) ?? intOrNull(row.n_gone),
      n_to_go: intOrNull(live.n_to_go) ?? intOrNull(row.n_to_go),
      elapsed_seconds: intOrNull(live.elapsed_seconds) ?? intOrNull(row.elapsed_seconds),
      pace_seconds: paceSeconds ?? intOrNull(row.pace_seconds),
      current_entry_no: clean(live.current_entry_no) || clean(row.current_entry_no),
      current_horse: clean(live.current_horse) || clean(row.current_horse),
      live_source: clean(live.live_source) || clean(row.live_source),
      source: Array.from(sources).join("|") || clean(row.source),
      last_synced_at: clean(live.last_synced_at) || clean(row.last_synced_at)
    };
  });
}

function scheduleHorseDisplay(classRow, entryOrder) {
  const order = clean(entryOrder);
  if (!order || !Array.isArray(classRow?.trainer_rollups)) return "";
  const suffix = `(${order})`;
  for (const trainerRollup of classRow.trainer_rollups) {
    for (const horse of trainerRollup.horses || []) {
      const text = clean(horse);
      if (text.endsWith(suffix)) return clean(text.slice(0, -suffix.length));
    }
  }
  return "";
}

function buildEntryGoRows({ showNo, focusDay: fallbackFocusDay, scheduleRows, classOogRows, activeTrainers, horseDisplays, trainerDisplays, nowIso }) {
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
    const focusDay = clean(classRow.show_day_key || classRow.show_days_display_date || fallbackFocusDay);
    const start = parseTime(focusDay, clean(classRow.class_start_time));
    if (!start) continue;
    const entryOrder = intOrNull(entry.entry_order);
    if (!entryOrder || entryOrder < 1) continue;
    const nGone = intOrNull(classRow.n_gone);
    const elapsedSeconds = intOrNull(classRow.elapsed_seconds);
    const paceSeconds = paceFromLive(classRow) || 120;
    const goTime = addSeconds(start, (entryOrder - 1) * paceSeconds);
    const entryNo = clean(entry.entry_no);
    const horseDisplay = clean(horseDisplays?.[clean(entry.horse)] || scheduleHorseDisplay(classRow, entry.entry_order) || entry.horse);
    const showNoValue = clean(classRow.show_id || classRow.show_no || showNo);
    const entryGoKey = `${showNoValue}|${focusDay}|${classNo}|${entryNo}`;
    rows.push({
      entry_go_key: entryGoKey,
      entry_go_key_mirror: entryGoKey,
      show_no: intOrNull(showNoValue),
      focus_day: focusDay,
      ring_no: intOrNull(classRow.ring_number || entry.ring_no),
      ring_day_no: intOrNull(classRow.ring_day_no || entry.ring_day_no),
      class_no: intOrNull(classNo),
      class_number: clean(classRow.class_number),
      class_name: clean(classRow.class_name),
      entry_no: intOrNull(entryNo),
      entry_order: entryOrder,
      horse: clean(entry.horse),
      horse_display: horseDisplay,
      rider: clean(entry.rider),
      trainer,
      trainer_display: clean(trainerDisplays?.[trainer] || trainer),
      class_start_time: clean(classRow.class_start_time),
      display_time: clean(classRow.start_display),
      entry_go_time: goTime.toTimeString().slice(0, 8),
      entry_count: intOrNull(classRow.entry_count),
      n_gone: nGone,
      elapsed_seconds: elapsedSeconds,
      pace_seconds: paceSeconds,
      time_till: Math.round(((goTime.getTime() - now.getTime()) / 60000) * 10) / 10,
      source: "class_oog.php|update_schedule.php",
      status: "active",
      inactive_reason: null,
      inactive_at: null,
      last_synced_at: nowIso
    });
  }
  return rows;
}

async function main() {
  const showNo = argValue("--show-no", process.env.WEC_SHOW_NO || "14906");
  const focusDay = argValue("--focus-day", process.env.WEC_FOCUS_DAY || "");
  const stage = argValue("--stage", process.env.WEC_TIME_STAGE || "all");
  if (!showNo) throw new Error("--show-no is required");
  if (!["all", "class-start", "entry-go"].includes(stage)) throw new Error("--stage must be all, class-start, or entry-go");

  const snapshotParams = new URLSearchParams({
    action: "focus-day-snapshot",
    show_no: showNo
  });
  if (focusDay) snapshotParams.set("focus_day", focusDay);
  const snapshot = await catalystGet(snapshotParams);
  const actualFocusDay = focusDay || clean(snapshot.focus_day);
  if (!actualFocusDay) throw new Error("focus_day could not be resolved");
  const nowIso = new Date().toISOString();

  const runClassStart = stage === "all" || stage === "class-start";
  const runEntryGo = stage === "all" || stage === "entry-go";
  let classStartRows = runClassStart
    ? buildClassStartRowsFromCoreSnapshot(snapshot.update_schedule || [], snapshot.counts || [], nowIso)
    : [];

  let entryGoRows = [];
  let activeTrainers = [];
  let scheduleRows = [];
  let entrySchema = { created: 0 };
  let entryResult = { seen: 0, changed: 0 };
  if (runEntryGo) {
    const params = new URLSearchParams({ action: "schedule-json", show_no: showNo, focus_day: actualFocusDay });
    scheduleRows = await catalystGet(params);
    if (runClassStart) {
      classStartRows = applyLiveTimingToClassRows(classStartRows, scheduleRows);
    }
    const debug = await catalystGet(new URLSearchParams({
      action: "debug-show-config",
      show_no: showNo,
      focus_day: actualFocusDay
    }));
    activeTrainers = debug.focus_source?.active_trainers || [];
    const horseDisplays = debug.focus_source?.horse_displays || {};
    const trainerDisplays = debug.focus_source?.trainer_displays || {};
    entryGoRows = buildEntryGoRows({
      showNo,
      focusDay: actualFocusDay,
      scheduleRows,
      classOogRows: snapshot.class_oog || [],
      activeTrainers,
      horseDisplays,
      trainerDisplays,
      nowIso
    });
  }

  const classFields = Object.keys(classStartRows[0] || {
    class_start_key_mirror: "",
    show_no: "",
    focus_day: "",
    class_no: ""
  });
  const entryFields = Object.keys(entryGoRows[0] || {
    entry_go_key_mirror: "",
    show_no: "",
    focus_day: "",
    class_no: "",
    entry_no: ""
  });
  const classSchema = runClassStart ? await ensureFields(TABLES.classStartTimes, classFields) : { created: 0 };
  if (runEntryGo) entrySchema = await ensureFields(TABLES.entryGoTimes, entryFields);
  const classResult = runClassStart
    ? await upsertRecords(TABLES.classStartTimes, "class_start_key_mirror", classStartRows)
    : { seen: 0, changed: 0 };
  if (runEntryGo) entryResult = await upsertRecords(TABLES.entryGoTimes, "entry_go_key_mirror", entryGoRows);
  const classInactive = runClassStart
    ? await markInactiveMissingRows({
      tableName: TABLES.classStartTimes,
      keyField: "class_start_key_mirror",
      showNo,
      focusDay: actualFocusDay,
      activeKeys: new Set(classStartRows.map((row) => clean(row.class_start_key_mirror)).filter(Boolean)),
      reason: "missing_from_update_schedule",
      nowIso
    })
    : { seen: 0, inactive: 0 };
  const entryInactive = runEntryGo
    ? await markInactiveMissingRows({
      tableName: TABLES.entryGoTimes,
      keyField: "entry_go_key_mirror",
      showNo,
      focusDay: actualFocusDay,
      activeKeys: new Set(entryGoRows.map((row) => clean(row.entry_go_key_mirror)).filter(Boolean)),
      reason: "missing_from_class_oog",
      nowIso
    })
    : { seen: 0, inactive: 0 };

  if (runClassStart) {
    await writeLog({
      checkName: "class_start_times",
      showNo,
      focusDay: actualFocusDay,
      recordsSeen: classResult.seen,
      recordsChanged: classResult.changed + classInactive.inactive,
      summary: `class_start_times upserted=${classResult.changed} focus=${actualFocusDay}`,
      payload: {
        fields_created: classSchema.created,
        rows: classResult.seen,
        source: "focus-day-snapshot.update_schedule+counts",
        counts_source_rows: Number(snapshot.counts?.length || 0),
        counts_applied: classStartRows.filter((row) => clean(row.source).includes("counts.php")).length,
        inactive_existing_seen: classInactive.seen,
        inactive_marked: classInactive.inactive
      }
    });
  }
  if (runEntryGo) {
    await writeLog({
      checkName: "entry_go_times",
      showNo,
      focusDay: actualFocusDay,
      recordsSeen: entryResult.seen,
      recordsChanged: entryResult.changed + entryInactive.inactive,
      summary: `entry_go_times upserted=${entryResult.changed} active_trainers=${activeTrainers.length} focus=${actualFocusDay}`,
      payload: {
        fields_created: entrySchema.created,
        rows: entryResult.seen,
        active_trainers: activeTrainers,
        inactive_existing_seen: entryInactive.seen,
        inactive_marked: entryInactive.inactive
      }
    });
  }

  console.log(JSON.stringify({
    ok: true,
    stage,
    show_no: showNo,
    focus_day: actualFocusDay,
    class_start_times: classResult,
    entry_go_times: entryResult,
    inactive: {
      class_start_times: classInactive,
      entry_go_times: entryInactive
    },
    fields_created: {
      class_start_times: classSchema.created,
      entry_go_times: entrySchema.created
    }
  }, null, 2));
}

if (require.main === module) {
  main().catch((error) => {
    console.error(error.stack || error.message);
    process.exit(1);
  });
}

module.exports = {
  applyLiveTimingToClassRows,
  buildEntryGoRows,
  classStartTimeFromText,
  inactiveRecordUpdates,
  paceFromLive
};
