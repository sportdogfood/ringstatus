const BASE_ID = process.env.WEC_AIRTABLE_BASE_ID || "app6XS1RvsPNRT6os";
const AIRTABLE_TOKEN = process.env.AIRTABLE_TOKEN;
const CATALYST_ENDPOINT = process.env.HORSESHOWING_CATALYST_ENDPOINT ||
  "https://horseshowing-700800454.development.catalystserverless.com/server/horseshowing_sync/";

const TABLES = {
  classStartTimes: "class_start_times",
  entryGoTimes: "entry_go_times",
  classOog: "class_oog",
  shows: "shows",
  focusShow: "focus_show",
  classes: "classes",
  rings: "rings",
  ringDays: "ring_days",
  horses: "horses",
  riders: "riders",
  trainers: "trainers",
  entries: "entries",
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

function activeShowFormula(showNo) {
  const value = clean(showNo);
  if (!value) return "{active}=1";
  const showFormula = /^\d+$/.test(value) ? `{show_no}=${Number(value)}` : `{show_no}='${value.replace(/'/g, "\\'")}'`;
  return `AND(${showFormula},{active}=1)`;
}

function recordFields(record) {
  return record?.fields || {};
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

async function ensureLinkedFields(tableName, linkedFields) {
  const meta = await getBaseTables();
  const table = meta.tables.find((item) => item.name === tableName);
  if (!table) throw new Error(`Missing Airtable table ${tableName}`);
  const tablesByName = new Map(meta.tables.map((item) => [item.name, item]));
  const fieldsByName = new Map(table.fields.map((field) => [field.name, field]));
  let created = 0;
  for (const [fieldName, linkedTableName] of Object.entries(linkedFields)) {
    if (fieldsByName.has(fieldName)) continue;
    const linkedTable = tablesByName.get(linkedTableName);
    if (!linkedTable) throw new Error(`Missing Airtable linked table ${linkedTableName}`);
    await airtableFetch(`https://api.airtable.com/v0/meta/bases/${BASE_ID}/tables/${table.id}/fields`, {
      method: "POST",
      body: JSON.stringify({
        name: fieldName,
        type: "multipleRecordLinks",
        options: { linkedTableId: linkedTable.id }
      })
    });
    created += 1;
  }
  return { created };
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

async function updateRecords(tableName, rows) {
  if (!rows.length) return { seen: 0, changed: 0 };
  let changed = 0;
  for (let index = 0; index < rows.length; index += 10) {
    const batch = rows.slice(index, index + 10);
    await airtableFetch(`https://api.airtable.com/v0/${BASE_ID}/${encodeURIComponent(tableName)}`, {
      method: "PATCH",
      body: JSON.stringify({ records: batch, typecast: true })
    });
    changed += batch.length;
  }
  return { seen: rows.length, changed };
}

async function listRecords(tableName, formula, viewName = "") {
  const records = [];
  let offset = "";
  do {
    const params = new URLSearchParams({ pageSize: "100" });
    if (formula) params.set("filterByFormula", formula);
    if (viewName) params.set("view", viewName);
    if (offset) params.set("offset", offset);
    const result = await airtableFetch(`https://api.airtable.com/v0/${BASE_ID}/${encodeURIComponent(tableName)}?${params.toString()}`);
    records.push(...(result.records || []));
    offset = result.offset || "";
  } while (offset);
  return records;
}

async function getTableViews() {
  const meta = await getBaseTables();
  const views = new Map();
  for (const table of meta.tables || []) {
    views.set(table.name, new Set((table.views || []).map((view) => view.name)));
  }
  return views;
}

async function listViewRecordsIfExists(tableName, viewName, tableViews) {
  if (!tableViews.get(tableName)?.has(viewName)) return [];
  return listRecords(tableName, "", viewName);
}

function showFocusFormula(showNo, focusDay) {
  const showValue = /^\d+$/.test(String(showNo)) ? String(showNo) : `'${String(showNo).replace(/'/g, "\\'")}'`;
  return `AND(IS_SAME({focus_day}, '${String(focusDay).replace(/'/g, "\\'")}', 'day'), {show_no}=${showValue})`;
}

function normalizeText(value) {
  return clean(value).toLowerCase();
}

function fieldsToCoreSnapshot({ updateRecords, countRecords, oogRecords }) {
  const updateSchedule = updateRecords.map(recordFields).map((fields) => ({
    show_no: fields.show_no,
    focus_day: clean(fields.focus_day).slice(0, 10),
    ring_no: fields.ring_no,
    ring_day_no: fields.days || fields.ring_day_no,
    class_no: fields.class_no,
    event_name: fields.event_name,
    class_name: fields.class_name,
    class_number: fields.class_number,
    time_text: fields.time_text || fields.display_time,
    time: fields.time,
    entry_count: fields.entry_count,
    source: fields.source || "airtable.update_schedule"
  }));
  const counts = countRecords.map(recordFields).map((fields) => ({
    show_no: fields.show_no,
    class_no: fields.class_no,
    class_number: fields.class_number,
    class_name: fields.class_name,
    entry_count: fields.entry_count
  }));
  const classNos = new Set(updateSchedule.map((row) => clean(row.class_no)).filter(Boolean));
  const classOog = oogRecords
    .map(recordFields)
    .filter((fields) => classNos.has(clean(fields.class_no)))
    .map((fields) => ({
      show_no: fields.show_no,
      focus_day: clean(fields.focus_day).slice(0, 10),
      class_no: fields.class_no,
      class_order: fields.class_order,
      class_start_time: Array.isArray(fields["class_start_time (from class_start_times)"])
        ? clean(fields["class_start_time (from class_start_times)"][0])
        : clean(fields["class_start_time (from class_start_times)"]),
      left_15: fields.left_15,
      entry_no: fields.entry_no,
      entry_order: fields.entry_order,
      horse: fields.horse,
      rider: fields.rider,
      trainer: fields.trainer,
      ring_no: fields.ring_no,
      ring_day_no: fields.days || fields.ring_day_no,
      ignore: fields.ignore === true,
      auto_ignore_candidate: fields.auto_ignore_candidate === true,
      source: fields.source || "airtable.class_oog"
    }));
  return { update_schedule: updateSchedule, counts, class_oog: classOog };
}

async function readAirtableCoreSnapshot(showNo, focusDay) {
  const updateRecordsAll = await listRecords("update_schedule");
  const updateRecords = updateRecordsAll.filter((record) => {
    const fields = recordFields(record);
    return clean(fields.show_no) === clean(showNo)
      && (
        clean(fields.focus_day).slice(0, 10) === clean(focusDay)
        || clean(fields.iso_date).slice(0, 10) === clean(focusDay)
        || clean(fields.date_text).includes(clean(focusDay))
      );
  });
  const classNos = new Set(updateRecords.map((record) => clean(record.fields?.class_no)).filter(Boolean));
  const countRecords = (await listRecords("counts"))
    .filter((record) => clean(record.fields?.show_no) === clean(showNo));
  const oogRecords = await listRecords("class_oog");
  const snapshot = fieldsToCoreSnapshot({ updateRecords, countRecords, oogRecords });
  snapshot.counts = snapshot.counts.filter((row) => classNos.has(clean(row.class_no)));
  return snapshot;
}

async function readAirtableHelpers() {
  const trainerRecords = await listRecords("trainers");
  const activeTrainers = [];
  const trainerDisplays = {};
  for (const record of trainerRecords) {
    const fields = recordFields(record);
    const trainer = clean(fields.trainer);
    if (!trainer) continue;
    const display = clean(fields.trainer_display || trainer);
    trainerDisplays[trainer] = display;
    if (fields.active === true) activeTrainers.push(trainer);
  }

  const horseRecords = await listRecords("horses");
  const horseDisplays = {};
  for (const record of horseRecords) {
    const fields = recordFields(record);
    const horse = clean(fields.horse);
    const display = clean(fields.barn_name || fields.horse_display);
    if (!horse || !display) continue;
    horseDisplays[horse] = display;
    horseDisplays[normalizeText(horse)] = display;
    const akaRaw = clean(fields.aka);
    if (akaRaw) {
      for (const alias of akaRaw.split(/[,\n;|]/).map(clean).filter(Boolean)) {
        horseDisplays[alias] = display;
        horseDisplays[normalizeText(alias)] = display;
      }
    }
  }
  return { activeTrainers, trainerDisplays, horseDisplays };
}

function currentHorseFromEntryText(value) {
  const raw = clean(value).replace(/<br\s*\/?>/gi, " ");
  const match = raw.match(/^#\d+\s*,\s*(.*?)\s+In ring at/i);
  return match ? clean(match[1]) : "";
}

async function readAirtableLiveRows(showNo, focusDay) {
  const formula = showFocusFormula(showNo, focusDay);
  const [orders, rings] = await Promise.all([
    listRecords("get_orders", formula),
    listRecords("get_rings", formula)
  ]);
  const rows = [];
  for (const record of [...orders, ...rings]) {
    const fields = recordFields(record);
    if (!clean(fields.class_no)) continue;
    rows.push({
      show_no: fields.show_no,
      focus_day: clean(fields.focus_day).slice(0, 10),
      ring_no: fields.ring_no,
      ring_day_no: fields.ring_day_no,
      class_no: fields.class_no,
      n_gone: fields.n_gone,
      n_to_go: fields.n_to_go,
      elapsed_seconds: fields.elapsed,
      timestamp: fields.timestamp,
      current_entry_no: fields.entry_no || fields.entry_number,
      current_horse: currentHorseFromEntryText(fields.entry_text),
      live_source: record.id && record.fields?.get_orders_key ? "get_orders.php" : "get_rings.php",
      last_synced_at: new Date().toISOString()
    });
  }
  return rows;
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
  const formula = showFocusFormula(showNo, focusDay);
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
      if (!classNo || !focusDay) return null;
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

function scheduleClassKey(row, fallbackFocusDay = "", fallbackShowNo = "") {
  const showNo = clean(row.show_no || row.show_id || fallbackShowNo);
  const focusDay = clean(row.focus_day || row.show_day_key || row.show_days_display_date || fallbackFocusDay).slice(0, 10);
  const ringDayNo = clean(row.ring_day_no || row.days);
  const classNo = clean(row.class_no);
  return `${showNo}|${focusDay}|${ringDayNo}|${classNo}`;
}

async function linkEntryGoTimesToClassStartTimes(showNo, focusDay) {
  const formula = showFocusFormula(showNo, focusDay);
  const classRecords = await listRecords(TABLES.classStartTimes, formula);
  const entryRecords = await listRecords(TABLES.entryGoTimes, formula);
  const classByKey = new Map();

  for (const record of classRecords) {
    const fields = recordFields(record);
    const key = clean(fields.class_start_key_mirror)
      || `${clean(fields.show_no)}|${clean(fields.focus_day).slice(0, 10)}|${clean(fields.ring_day_no)}|${clean(fields.class_no)}`;
    if (key) classByKey.set(key, record.id);
  }

  const updates = [];
  for (const record of entryRecords) {
    const fields = recordFields(record);
    const key = `${clean(fields.show_no)}|${clean(fields.focus_day).slice(0, 10)}|${clean(fields.ring_day_no)}|${clean(fields.class_no)}`;
    const classRecordId = classByKey.get(key);
    if (!classRecordId) continue;
    const currentLinks = Array.isArray(fields.class_start_times) ? fields.class_start_times : [];
    if (currentLinks.length === 1 && currentLinks[0] === classRecordId) continue;
    updates.push({
      id: record.id,
      fields: {
        class_start_times: [classRecordId]
      }
    });
  }

  const result = await updateRecords(TABLES.entryGoTimes, updates);
  return {
    class_records: classRecords.length,
    entry_records: entryRecords.length,
    linked: result.changed
  };
}

async function linkEntryGoTimesToClassOog(showNo, focusDay) {
  const formula = showFocusFormula(showNo, focusDay);
  const [entryRecords, classOogRecords] = await Promise.all([
    listRecords(TABLES.entryGoTimes, formula),
    listRecords(TABLES.classOog, formula)
  ]);
  const classOogByKey = new Map();
  for (const record of classOogRecords) {
    const fields = recordFields(record);
    const key = classOogEntryGoKey(fields);
    if (key) classOogByKey.set(key, record.id);
  }
  const updates = [];
  let matches = 0;
  for (const record of entryRecords) {
    const current = recordFields(record);
    const classOogId = classOogByKey.get(entryGoKeyFromFields(current));
    if (classOogId) matches += 1;
    const fields = {};
    addLinkedFieldUpdate(fields, current, "class_oog", classOogId);
    if (Object.keys(fields).length) updates.push({ id: record.id, fields });
  }
  const result = await updateRecords(TABLES.entryGoTimes, updates);
  return {
    entry_records: entryRecords.length,
    class_oog_records: classOogRecords.length,
    matches,
    linked: result.changed
  };
}

function arraySame(left, right) {
  const a = Array.isArray(left) ? left : [];
  const b = Array.isArray(right) ? right : [];
  if (a.length !== b.length) return false;
  return a.every((item, index) => item === b[index]);
}

function addLinkedFieldUpdate(fields, currentFields, fieldName, recordId) {
  if (!recordId) return;
  const next = [recordId];
  if (arraySame(currentFields[fieldName], next)) return;
  fields[fieldName] = next;
}

function addIndex(map, key, recordId) {
  const normalized = normalizeText(key);
  if (normalized && !map.has(normalized)) map.set(normalized, recordId);
}

function helperIndexes({ horseRecords, riderRecords, trainerRecords, entryRecords }) {
  const horses = new Map();
  for (const record of horseRecords) {
    const fields = recordFields(record);
    addIndex(horses, fields.horse, record.id);
    const akaRaw = clean(fields.aka);
    if (akaRaw) {
      for (const alias of akaRaw.split(/[,\n;|]/).map(clean).filter(Boolean)) {
        addIndex(horses, alias, record.id);
      }
    }
  }

  const riders = new Map();
  for (const record of riderRecords) addIndex(riders, recordFields(record).rider, record.id);

  const trainers = new Map();
  for (const record of trainerRecords) addIndex(trainers, recordFields(record).trainer, record.id);

  const entries = new Map();
  for (const record of entryRecords) {
    const fields = recordFields(record);
    addIndex(entries, fields.entry_no, record.id);
  }

  return { horses, riders, trainers, entries };
}

function recordIndex(records, fieldName) {
  const map = new Map();
  for (const record of records || []) {
    addIndex(map, recordFields(record)[fieldName], record.id);
  }
  return map;
}

function classStartKey(fields) {
  return clean(fields.class_start_key_mirror)
    || `${clean(fields.show_no)}|${clean(fields.focus_day).slice(0, 10)}|${clean(fields.ring_day_no)}|${clean(fields.class_no)}`;
}

function classOogClassStartKey(fields) {
  return clean(fields.class_start_times_uuid)
    || `${clean(fields.show_no)}|${clean(fields.focus_day).slice(0, 10)}|${clean(fields.days || fields.ring_day_no)}|${clean(fields.class_no)}`;
}

function entryGoKeyFromFields(fields) {
  return clean(fields.entry_go_key_mirror)
    || clean(fields.entry_go_key)
    || `${clean(fields.show_no)}|${clean(fields.focus_day).slice(0, 10)}|${clean(fields.ring_day_no || fields.days)}|${clean(fields.class_no)}|${clean(fields.entry_no)}`;
}

function classOogEntryGoKey(fields) {
  return `${clean(fields.show_no)}|${clean(fields.focus_day).slice(0, 10)}|${clean(fields.days || fields.ring_day_no)}|${clean(fields.class_no)}|${clean(fields.entry_no)}`;
}

async function linkClassOogToGeneratedTablesAndHelpers(showNo, focusDay) {
  const formula = showFocusFormula(showNo, focusDay);
  const [
    classOogRecords,
    classStartRecords,
    entryGoRecords,
    showRecords,
    focusShowRecords,
    classRecords,
    ringRecords,
    ringDayRecords,
    horseRecords,
    riderRecords,
    trainerRecords,
    entryRecords
  ] = await Promise.all([
    listRecords(TABLES.classOog, formula),
    listRecords(TABLES.classStartTimes, formula),
    listRecords(TABLES.entryGoTimes, formula),
    listRecords(TABLES.shows, activeShowFormula(showNo)),
    listRecords(TABLES.focusShow, formula),
    listRecords(TABLES.classes),
    listRecords(TABLES.rings),
    listRecords(TABLES.ringDays),
    listRecords(TABLES.horses),
    listRecords(TABLES.riders),
    listRecords(TABLES.trainers),
    listRecords(TABLES.entries)
  ]);

  const classStartByKey = new Map();
  for (const record of classStartRecords) {
    const key = classStartKey(recordFields(record));
    if (key) classStartByKey.set(key, record.id);
  }

  const entryGoByKey = new Map();
  for (const record of entryGoRecords) {
    const key = entryGoKeyFromFields(recordFields(record));
    if (key) entryGoByKey.set(key, record.id);
  }

  const helpers = helperIndexes({ horseRecords, riderRecords, trainerRecords, entryRecords });
  const support = {
    shows: recordIndex(showRecords, "show_no"),
    focusShow: recordIndex(focusShowRecords, "show_no"),
    classes: recordIndex(classRecords, "class_no"),
    rings: recordIndex(ringRecords, "ring_no"),
    ringDays: recordIndex(ringDayRecords, "ring_day_no")
  };
  const updates = [];
  let classStartMatches = 0;
  let entryGoMatches = 0;
  let helperMatches = 0;
  let supportMatches = 0;

  for (const record of classOogRecords) {
    const current = recordFields(record);
    const fields = {};

    const classStartId = classStartByKey.get(classOogClassStartKey(current));
    if (classStartId) classStartMatches += 1;
    addLinkedFieldUpdate(fields, current, "class_start_times", classStartId);

    const entryGoId = entryGoByKey.get(classOogEntryGoKey(current));
    if (entryGoId) entryGoMatches += 1;
    addLinkedFieldUpdate(fields, current, "entry_go_times", entryGoId);

    const entryId = helpers.entries.get(normalizeText(current.entry_no));
    const horseId = helpers.horses.get(normalizeText(current.horse));
    const riderId = helpers.riders.get(normalizeText(current.rider));
    const trainerId = helpers.trainers.get(normalizeText(current.trainer));
    for (const id of [entryId, horseId, riderId, trainerId]) {
      if (id) helperMatches += 1;
    }
    addLinkedFieldUpdate(fields, current, "entries", entryId);
    addLinkedFieldUpdate(fields, current, "horses", horseId);
    addLinkedFieldUpdate(fields, current, "riders", riderId);
    addLinkedFieldUpdate(fields, current, "trainers", trainerId);

    const showId = support.shows.get(normalizeText(current.show_no));
    const focusShowId = support.focusShow.get(normalizeText(current.show_no));
    const classId = support.classes.get(normalizeText(current.class_no));
    const ringId = support.rings.get(normalizeText(current.ring_no));
    const ringDayId = support.ringDays.get(normalizeText(current.days || current.ring_day_no));
    for (const id of [showId, focusShowId, classId, ringId, ringDayId]) {
      if (id) supportMatches += 1;
    }
    addLinkedFieldUpdate(fields, current, "shows", showId);
    addLinkedFieldUpdate(fields, current, "focus_show", focusShowId);
    addLinkedFieldUpdate(fields, current, "classes", classId);
    addLinkedFieldUpdate(fields, current, "rings", ringId);
    addLinkedFieldUpdate(fields, current, "ring_days", ringDayId);

    if (Object.keys(fields).length) updates.push({ id: record.id, fields });
  }

  const result = await updateRecords(TABLES.classOog, updates);
  return {
    class_oog_records: classOogRecords.length,
    class_start_records: classStartRecords.length,
    entry_go_records: entryGoRecords.length,
    class_start_matches: classStartMatches,
    entry_go_matches: entryGoMatches,
    helper_matches: helperMatches,
    support_matches: supportMatches,
    linked: result.changed
  };
}

async function linkEntryGoTimesToHelpers(showNo, focusDay) {
  const formula = showFocusFormula(showNo, focusDay);
  const [
    entryGoRecords,
    showRecords,
    focusShowRecords,
    classRecords,
    ringRecords,
    ringDayRecords,
    horseRecords,
    riderRecords,
    trainerRecords,
    entryRecords
  ] = await Promise.all([
    listRecords(TABLES.entryGoTimes, formula),
    listRecords(TABLES.shows, activeShowFormula(showNo)),
    listRecords(TABLES.focusShow, formula),
    listRecords(TABLES.classes),
    listRecords(TABLES.rings),
    listRecords(TABLES.ringDays),
    listRecords(TABLES.horses),
    listRecords(TABLES.riders),
    listRecords(TABLES.trainers),
    listRecords(TABLES.entries)
  ]);
  const helpers = helperIndexes({ horseRecords, riderRecords, trainerRecords, entryRecords });
  const support = {
    shows: recordIndex(showRecords, "show_no"),
    focusShow: recordIndex(focusShowRecords, "show_no"),
    classes: recordIndex(classRecords, "class_no"),
    rings: recordIndex(ringRecords, "ring_no"),
    ringDays: recordIndex(ringDayRecords, "ring_day_no")
  };
  const updates = [];
  let helperMatches = 0;
  let supportMatches = 0;

  for (const record of entryGoRecords) {
    const current = recordFields(record);
    const fields = {};
    const entryId = helpers.entries.get(normalizeText(current.entry_no));
    const horseId = helpers.horses.get(normalizeText(current.horse));
    const riderId = helpers.riders.get(normalizeText(current.rider));
    const trainerId = helpers.trainers.get(normalizeText(current.trainer));
    for (const id of [entryId, horseId, riderId, trainerId]) {
      if (id) helperMatches += 1;
    }
    addLinkedFieldUpdate(fields, current, "entries", entryId);
    addLinkedFieldUpdate(fields, current, "horses", horseId);
    addLinkedFieldUpdate(fields, current, "riders", riderId);
    addLinkedFieldUpdate(fields, current, "trainers", trainerId);

    const showId = support.shows.get(normalizeText(current.show_no));
    const focusShowId = support.focusShow.get(normalizeText(current.show_no));
    const classId = support.classes.get(normalizeText(current.class_no));
    const ringId = support.rings.get(normalizeText(current.ring_no));
    const ringDayId = support.ringDays.get(normalizeText(current.ring_day_no));
    for (const id of [showId, focusShowId, classId, ringId, ringDayId]) {
      if (id) supportMatches += 1;
    }
    addLinkedFieldUpdate(fields, current, "shows", showId);
    addLinkedFieldUpdate(fields, current, "focus_show", focusShowId);
    addLinkedFieldUpdate(fields, current, "classes", classId);
    addLinkedFieldUpdate(fields, current, "rings", ringId);
    addLinkedFieldUpdate(fields, current, "ring_days", ringDayId);
    if (Object.keys(fields).length) updates.push({ id: record.id, fields });
  }

  const result = await updateRecords(TABLES.entryGoTimes, updates);
  return {
    entry_records: entryGoRecords.length,
    helper_matches: helperMatches,
    support_matches: supportMatches,
    linked: result.changed
  };
}

async function linkClassStartTimesToSupportTables(showNo, focusDay) {
  const formula = showFocusFormula(showNo, focusDay);
  const [
    classStartRecords,
    showRecords,
    focusShowRecords,
    classRecords,
    ringRecords,
    ringDayRecords
  ] = await Promise.all([
    listRecords(TABLES.classStartTimes, formula),
    listRecords(TABLES.shows, activeShowFormula(showNo)),
    listRecords(TABLES.focusShow, formula),
    listRecords(TABLES.classes),
    listRecords(TABLES.rings),
    listRecords(TABLES.ringDays)
  ]);
  const support = {
    shows: recordIndex(showRecords, "show_no"),
    focusShow: recordIndex(focusShowRecords, "show_no"),
    classes: recordIndex(classRecords, "class_no"),
    rings: recordIndex(ringRecords, "ring_no"),
    ringDays: recordIndex(ringDayRecords, "ring_day_no")
  };
  const updates = [];
  let supportMatches = 0;
  for (const record of classStartRecords) {
    const current = recordFields(record);
    const fields = {};
    const showId = support.shows.get(normalizeText(current.show_no));
    const focusShowId = support.focusShow.get(normalizeText(current.show_no));
    const classId = support.classes.get(normalizeText(current.class_no));
    const ringId = support.rings.get(normalizeText(current.ring_no));
    const ringDayId = support.ringDays.get(normalizeText(current.ring_day_no));
    for (const id of [showId, focusShowId, classId, ringId, ringDayId]) {
      if (id) supportMatches += 1;
    }
    addLinkedFieldUpdate(fields, current, "shows", showId);
    addLinkedFieldUpdate(fields, current, "focus_show", focusShowId);
    addLinkedFieldUpdate(fields, current, "classes", classId);
    addLinkedFieldUpdate(fields, current, "rings", ringId);
    addLinkedFieldUpdate(fields, current, "ring_days", ringDayId);
    if (Object.keys(fields).length) updates.push({ id: record.id, fields });
  }
  const result = await updateRecords(TABLES.classStartTimes, updates);
  return {
    class_records: classStartRecords.length,
    support_matches: supportMatches,
    linked: result.changed
  };
}

async function cleanupTodayTomorrowLinks() {
  const tableViews = await getTableViews();
  const viewNames = ["TODAY", "TOMORROW"];
  const [
    showRecords,
    focusShowRecords,
    classRecords,
    ringRecords,
    ringDayRecords,
    horseRecords,
    riderRecords,
    trainerRecords,
    entryRecords
  ] = await Promise.all([
    listRecords(TABLES.shows, "{active}=1"),
    listRecords(TABLES.focusShow, "{active}=1"),
    listRecords(TABLES.classes),
    listRecords(TABLES.rings),
    listRecords(TABLES.ringDays),
    listRecords(TABLES.horses),
    listRecords(TABLES.riders),
    listRecords(TABLES.trainers),
    listRecords(TABLES.entries)
  ]);

  const support = {
    shows: recordIndex(showRecords, "show_no"),
    focusShow: recordIndex(focusShowRecords, "show_no"),
    classes: recordIndex(classRecords, "class_no"),
    rings: recordIndex(ringRecords, "ring_no"),
    ringDays: recordIndex(ringDayRecords, "ring_day_no")
  };
  const helpers = helperIndexes({ horseRecords, riderRecords, trainerRecords, entryRecords });

  const scoped = {
    classOog: [],
    classStart: [],
    entryGo: []
  };
  const viewsUsed = [];
  for (const viewName of viewNames) {
    const classOog = await listViewRecordsIfExists(TABLES.classOog, viewName, tableViews);
    const classStart = await listViewRecordsIfExists(TABLES.classStartTimes, viewName, tableViews);
    const entryGo = await listViewRecordsIfExists(TABLES.entryGoTimes, viewName, tableViews);
    if (classOog.length) viewsUsed.push(`${TABLES.classOog}/${viewName}`);
    if (classStart.length) viewsUsed.push(`${TABLES.classStartTimes}/${viewName}`);
    if (entryGo.length) viewsUsed.push(`${TABLES.entryGoTimes}/${viewName}`);
    scoped.classOog.push(...classOog);
    scoped.classStart.push(...classStart);
    scoped.entryGo.push(...entryGo);
  }

  const classStartByKey = new Map();
  for (const record of scoped.classStart) {
    const key = classStartKey(recordFields(record));
    if (key) classStartByKey.set(key, record.id);
  }
  const entryGoByKey = new Map();
  for (const record of scoped.entryGo) {
    const key = entryGoKeyFromFields(recordFields(record));
    if (key) entryGoByKey.set(key, record.id);
  }
  const classOogByKey = new Map();
  for (const record of scoped.classOog) {
    const key = classOogEntryGoKey(recordFields(record));
    if (key) classOogByKey.set(key, record.id);
  }

  const classStartUpdates = [];
  for (const record of scoped.classStart) {
    const current = recordFields(record);
    const fields = {};
    addLinkedFieldUpdate(fields, current, "shows", support.shows.get(normalizeText(current.show_no)));
    addLinkedFieldUpdate(fields, current, "focus_show", support.focusShow.get(normalizeText(current.show_no)));
    addLinkedFieldUpdate(fields, current, "classes", support.classes.get(normalizeText(current.class_no)));
    addLinkedFieldUpdate(fields, current, "rings", support.rings.get(normalizeText(current.ring_no)));
    addLinkedFieldUpdate(fields, current, "ring_days", support.ringDays.get(normalizeText(current.ring_day_no)));
    if (Object.keys(fields).length) classStartUpdates.push({ id: record.id, fields });
  }

  const entryGoUpdates = [];
  for (const record of scoped.entryGo) {
    const current = recordFields(record);
    const fields = {};
    const classKey = `${clean(current.show_no)}|${clean(current.focus_day).slice(0, 10)}|${clean(current.ring_day_no)}|${clean(current.class_no)}`;
    addLinkedFieldUpdate(fields, current, "class_start_times", classStartByKey.get(classKey));
    addLinkedFieldUpdate(fields, current, "class_oog", classOogByKey.get(entryGoKeyFromFields(current)));
    addLinkedFieldUpdate(fields, current, "shows", support.shows.get(normalizeText(current.show_no)));
    addLinkedFieldUpdate(fields, current, "focus_show", support.focusShow.get(normalizeText(current.show_no)));
    addLinkedFieldUpdate(fields, current, "classes", support.classes.get(normalizeText(current.class_no)));
    addLinkedFieldUpdate(fields, current, "rings", support.rings.get(normalizeText(current.ring_no)));
    addLinkedFieldUpdate(fields, current, "ring_days", support.ringDays.get(normalizeText(current.ring_day_no)));
    addLinkedFieldUpdate(fields, current, "entries", helpers.entries.get(normalizeText(current.entry_no)));
    addLinkedFieldUpdate(fields, current, "horses", helpers.horses.get(normalizeText(current.horse)));
    addLinkedFieldUpdate(fields, current, "riders", helpers.riders.get(normalizeText(current.rider)));
    addLinkedFieldUpdate(fields, current, "trainers", helpers.trainers.get(normalizeText(current.trainer)));
    if (Object.keys(fields).length) entryGoUpdates.push({ id: record.id, fields });
  }

  const classOogUpdates = [];
  for (const record of scoped.classOog) {
    const current = recordFields(record);
    const fields = {};
    addLinkedFieldUpdate(fields, current, "class_start_times", classStartByKey.get(classOogClassStartKey(current)));
    addLinkedFieldUpdate(fields, current, "entry_go_times", entryGoByKey.get(classOogEntryGoKey(current)));
    addLinkedFieldUpdate(fields, current, "shows", support.shows.get(normalizeText(current.show_no)));
    addLinkedFieldUpdate(fields, current, "focus_show", support.focusShow.get(normalizeText(current.show_no)));
    addLinkedFieldUpdate(fields, current, "classes", support.classes.get(normalizeText(current.class_no)));
    addLinkedFieldUpdate(fields, current, "rings", support.rings.get(normalizeText(current.ring_no)));
    addLinkedFieldUpdate(fields, current, "ring_days", support.ringDays.get(normalizeText(current.days || current.ring_day_no)));
    addLinkedFieldUpdate(fields, current, "entries", helpers.entries.get(normalizeText(current.entry_no)));
    addLinkedFieldUpdate(fields, current, "horses", helpers.horses.get(normalizeText(current.horse)));
    addLinkedFieldUpdate(fields, current, "riders", helpers.riders.get(normalizeText(current.rider)));
    addLinkedFieldUpdate(fields, current, "trainers", helpers.trainers.get(normalizeText(current.trainer)));
    if (Object.keys(fields).length) classOogUpdates.push({ id: record.id, fields });
  }

  const classStartResult = await updateRecords(TABLES.classStartTimes, classStartUpdates);
  const entryGoResult = await updateRecords(TABLES.entryGoTimes, entryGoUpdates);
  const classOogResult = await updateRecords(TABLES.classOog, classOogUpdates);
  return {
    views_used: viewsUsed,
    class_start_times: { seen: scoped.classStart.length, changed: classStartResult.changed },
    entry_go_times: { seen: scoped.entryGo.length, changed: entryGoResult.changed },
    class_oog: { seen: scoped.classOog.length, changed: classOogResult.changed }
  };
}

function paceFromLive(row) {
  if (!row) return null;
  const nGone = intOrNull(row.n_gone);
  const elapsedSeconds = intOrNull(row.elapsed_seconds);
  return nGone && nGone > 6 && elapsedSeconds && elapsedSeconds > 0
    ? Math.max(30, Math.round(elapsedSeconds / nGone))
    : null;
}

function liveRingKey(row) {
  return `${clean(row.show_no || row.show_id)}|${clean(row.focus_day || row.show_day_key || row.show_days_display_date).slice(0, 10)}|${clean(row.ring_day_no || row.days)}|${clean(row.ring_no || row.ring_number)}`;
}

function livePaceByRing(liveRows) {
  const byRing = new Map();
  for (const liveRow of liveRows || []) {
    const paceSeconds = paceFromLive(liveRow);
    const key = liveRingKey(liveRow);
    if (!paceSeconds || key.includes("||") || key.endsWith("|")) continue;
    const timestamp = intOrNull(liveRow.timestamp) || 0;
    const current = byRing.get(key);
    if (!current || timestamp >= current.timestamp) {
      byRing.set(key, {
        pace_seconds: paceSeconds,
        timestamp,
        live_source: clean(liveRow.live_source)
      });
    }
  }
  return byRing;
}

function applyLiveTimingToClassRows(classRows, liveRows) {
  const liveByClass = new Map();
  for (const liveRow of liveRows || []) {
    const key = scheduleClassKey(liveRow);
    if (!key.includes("||") && !key.endsWith("|")) liveByClass.set(key, liveRow);
  }
  const paceByRing = livePaceByRing(liveRows);
  return (classRows || []).map((row) => {
    const live = liveByClass.get(scheduleClassKey(row));
    const ringPace = paceByRing.get(liveRingKey(row));
    if (!live && !ringPace) return row;
    const paceSeconds = paceFromLive(live) ?? ringPace?.pace_seconds;
    const sources = new Set(
      clean(row.source)
        .split("|")
        .map((item) => clean(item))
        .filter(Boolean)
    );
    if (clean(live?.live_source)) sources.add(clean(live.live_source));
    if (!paceFromLive(live) && ringPace?.live_source) sources.add(`${ringPace.live_source}:ring_pace`);
    return {
      ...row,
      n_gone: intOrNull(live?.n_gone) ?? intOrNull(row.n_gone),
      n_to_go: intOrNull(live?.n_to_go) ?? intOrNull(row.n_to_go),
      elapsed_seconds: intOrNull(live?.elapsed_seconds) ?? intOrNull(row.elapsed_seconds),
      pace_seconds: paceSeconds ?? intOrNull(row.pace_seconds),
      current_entry_no: clean(live?.current_entry_no) || clean(row.current_entry_no),
      current_horse: clean(live?.current_horse) || clean(row.current_horse),
      live_source: clean(live?.live_source) || clean(row.live_source),
      source: Array.from(sources).join("|") || clean(row.source),
      last_synced_at: clean(live?.last_synced_at) || clean(row.last_synced_at)
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

function entryClassifierKey(entry, classRow, fallbackFocusDay) {
  const focusDay = clean(
    entry.focus_day
    || classRow?.show_day_key
    || classRow?.show_days_display_date
    || fallbackFocusDay
  ).slice(0, 10);
  const ringNo = clean(entry.ring_no || classRow?.ring_number);
  const entryNo = clean(entry.entry_no);
  const classStartTime = clean(entry.class_start_time || classRow?.class_start_time);
  if (!focusDay || !ringNo || !entryNo || !classStartTime) return "";
  return `${focusDay}|${ringNo}|${entryNo}|${classStartTime}`;
}

function entryIdentity(entry, classRow, fallbackFocusDay, showNo) {
  const focusDay = clean(
    entry.focus_day
    || classRow?.show_day_key
    || classRow?.show_days_display_date
    || fallbackFocusDay
  ).slice(0, 10);
  const showNoValue = clean(entry.show_no || classRow?.show_id || classRow?.show_no || showNo);
  const ringDayNo = clean(entry.ring_day_no || entry.days || classRow?.ring_day_no);
  return `${showNoValue}|${focusDay}|${ringDayNo}|${clean(entry.class_no)}|${clean(entry.entry_no)}`;
}

function classOogIgnoreSummary({ classOogRows, classByKey, fallbackFocusDay, showNo, activeTrainers }) {
  const active = new Set((activeTrainers || []).map((item) => clean(item).toLowerCase()).filter(Boolean));
  const ignored = new Set();
  const manual = new Set();
  const automatic = new Set();
  const grouped = new Map();

  for (const entry of classOogRows || []) {
    const trainer = clean(entry.trainer).toLowerCase();
    if (!trainer || !active.has(trainer)) continue;
    const classRow = classByKey.get(scheduleClassKey(entry, fallbackFocusDay, showNo));
    const identity = entryIdentity(entry, classRow, fallbackFocusDay, showNo);
    if (!identity || identity.includes("||")) continue;
    if (entry.ignore === true || entry.auto_ignore_candidate === true) {
      ignored.add(identity);
      manual.add(identity);
      continue;
    }
    const groupKey = entryClassifierKey(entry, classRow, fallbackFocusDay);
    if (!groupKey) continue;
    if (!grouped.has(groupKey)) grouped.set(groupKey, []);
    grouped.get(groupKey).push({ entry, classRow, identity });
  }

  for (const group of grouped.values()) {
    if (group.length < 2) continue;
    const ordered = [...group].sort((left, right) => {
      const leftOrder = intOrNull(left.entry.class_order) ?? 0;
      const rightOrder = intOrNull(right.entry.class_order) ?? 0;
      if (leftOrder !== rightOrder) return leftOrder - rightOrder;
      return (intOrNull(left.entry.class_no) ?? 0) - (intOrNull(right.entry.class_no) ?? 0);
    });
    for (let index = 1; index < ordered.length; index += 1) {
      ignored.add(ordered[index].identity);
      automatic.add(ordered[index].identity);
    }
  }

  return {
    ignored,
    manual_count: manual.size,
    automatic_count: automatic.size,
    ignored_count: ignored.size,
    grouped_count: [...grouped.values()].filter((group) => group.length > 1).length
  };
}

function buildEntryGoRows({ showNo, focusDay: fallbackFocusDay, scheduleRows, classOogRows, activeTrainers, horseDisplays, trainerDisplays, nowIso }) {
  const active = new Set(activeTrainers.map((item) => clean(item).toLowerCase()).filter(Boolean));
  const classByKey = new Map(scheduleRows.map((row) => [scheduleClassKey(row, fallbackFocusDay, showNo), row]));
  const ignoreSummary = classOogIgnoreSummary({ classOogRows, classByKey, fallbackFocusDay, showNo, activeTrainers });
  buildEntryGoRows.lastIgnoreSummary = ignoreSummary;
  const rows = [];
  const now = new Date();
  for (const entry of classOogRows) {
    const trainer = clean(entry.trainer);
    if (!trainer || !active.has(trainer.toLowerCase())) continue;
    const classNo = clean(entry.class_no);
    const classRow = classByKey.get(scheduleClassKey(entry, fallbackFocusDay, showNo));
    if (!classRow) continue;
    const focusDay = clean(classRow.show_day_key || classRow.show_days_display_date || fallbackFocusDay);
    const entryOrder = intOrNull(entry.entry_order);
    if (!entryOrder || entryOrder < 1) continue;
    const nGone = intOrNull(classRow.n_gone);
    const elapsedSeconds = intOrNull(classRow.elapsed_seconds);
    const paceSeconds = paceFromLive(classRow) || 120;
    const start = parseTime(focusDay, clean(classRow.class_start_time));
    if (!start) continue;
    const goTime = addSeconds(start, (entryOrder - 1) * paceSeconds);
    const entryNo = clean(entry.entry_no);
    const ringDayNo = clean(classRow.ring_day_no || entry.ring_day_no || entry.days);
    const identity = `${clean(classRow.show_id || classRow.show_no || showNo)}|${focusDay}|${ringDayNo}|${classNo}|${entryNo}`;
    if (entry.ignore === true || entry.auto_ignore_candidate === true) continue;
    const horseName = clean(entry.horse);
    const horseDisplay = clean(horseDisplays?.[horseName] || horseDisplays?.[normalizeText(horseName)] || scheduleHorseDisplay(classRow, entry.entry_order) || entry.horse);
    const showNoValue = clean(classRow.show_id || classRow.show_no || showNo);
    const entryGoKey = `${showNoValue}|${focusDay}|${ringDayNo}|${classNo}|${entryNo}`;
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
      entry_go_time: goTime ? goTime.toTimeString().slice(0, 8) : "",
      entry_count: intOrNull(classRow.entry_count),
      n_gone: nGone,
      elapsed_seconds: elapsedSeconds,
      pace_seconds: paceSeconds,
      time_till: goTime ? Math.round(((goTime.getTime() - now.getTime()) / 60000) * 10) / 10 : null,
      source: clean(`class_oog.php|${clean(classRow.live_source || classRow.source || "update_schedule.php")}`),
      status: "active",
      inactive_reason: null,
      inactive_at: null,
      last_synced_at: nowIso
    });
  }
  return rows;
}

function classStartRowsToScheduleRows(classStartRows) {
  return (classStartRows || []).map((row) => ({
    show_id: clean(row.show_no),
    show_no: clean(row.show_no),
    show_day_key: clean(row.focus_day),
    show_days_display_date: clean(row.focus_day),
    ring_number: row.ring_no,
    ring_day_no: row.ring_day_no,
    class_no: row.class_no,
    class_number: row.class_number,
    class_name: row.class_name,
    class_start_time: row.class_start_time,
    start_display: row.display_time,
    entry_count: row.entry_count,
    n_gone: row.n_gone,
    n_to_go: row.n_to_go,
    elapsed_seconds: row.elapsed_seconds,
    pace_seconds: row.pace_seconds,
    live_source: row.source
  }));
}

function coreUpdateRowsToScheduleRows(updateScheduleRows, countRows) {
  const countsByClass = new Map(
    (countRows || [])
      .filter((row) => clean(row.class_no))
      .map((row) => [clean(row.class_no), intOrNull(row.entry_count)])
  );
  return (updateScheduleRows || [])
    .filter((row) => clean(row.class_no))
    .map((row) => {
      const classParts = parseClassLabel(row.event_name || row.class_name, row.class_no);
      const classStartTime = clean(row.time) || classStartTimeFromText(row.time_text);
      return {
        show_id: clean(row.show_no),
        show_no: clean(row.show_no),
        show_day_key: clean(row.focus_day || row.iso_date),
        show_days_display_date: clean(row.focus_day || row.iso_date),
        ring_number: row.ring_no,
        ring_day_no: row.ring_day_no,
        class_no: row.class_no,
        class_number: clean(row.class_number || classParts.class_number),
        class_name: clean(row.class_name || classParts.class_name),
        class_start_time: classStartTime,
        start_display: clean(row.time_text),
        entry_count: countsByClass.get(clean(row.class_no)) ?? intOrNull(row.entry_count),
        n_gone: row.n_gone,
        n_to_go: row.n_to_go,
        elapsed_seconds: row.elapsed_seconds,
        live_source: row.source
      };
    });
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
  const catalystHasCore = Array.isArray(snapshot.update_schedule) && snapshot.update_schedule.length > 0;
  const airtableCoreSnapshot = await readAirtableCoreSnapshot(showNo, actualFocusDay);
  const coreSnapshot = Array.isArray(airtableCoreSnapshot.update_schedule) && airtableCoreSnapshot.update_schedule.length > 0
    ? airtableCoreSnapshot
    : snapshot;

  const runClassStart = stage === "all" || stage === "class-start";
  const runEntryGo = stage === "all" || stage === "entry-go";
  let classStartRows = runClassStart
    ? buildClassStartRowsFromCoreSnapshot(coreSnapshot.update_schedule || [], coreSnapshot.counts || [], nowIso)
    : [];

  let entryGoRows = [];
  let activeTrainers = [];
  let scheduleRows = [];
  let liveScheduleRows = [];
  let entrySchema = { created: 0 };
  let entryResult = { seen: 0, changed: 0 };
  if (runEntryGo) {
    const params = new URLSearchParams({ action: "schedule-json", show_no: showNo, focus_day: actualFocusDay });
    liveScheduleRows = await catalystGet(params);
    const airtableLiveRows = await readAirtableLiveRows(showNo, actualFocusDay);
    const coreScheduleRows = coreUpdateRowsToScheduleRows(coreSnapshot.update_schedule || [], coreSnapshot.counts || []);
    if (Array.isArray(liveScheduleRows) && liveScheduleRows.length > 0) {
      scheduleRows = applyLiveTimingToClassRows(coreScheduleRows, liveScheduleRows);
    } else {
      scheduleRows = coreScheduleRows;
    }
    scheduleRows = applyLiveTimingToClassRows(scheduleRows, airtableLiveRows);
    if (runClassStart) {
      classStartRows = applyLiveTimingToClassRows(classStartRows, scheduleRows);
      classStartRows = applyLiveTimingToClassRows(classStartRows, airtableLiveRows);
    }
    const debug = await catalystGet(new URLSearchParams({
      action: "debug-show-config",
      show_no: showNo,
      focus_day: actualFocusDay
    }));
    const airtableHelpers = await readAirtableHelpers();
    activeTrainers = airtableHelpers.activeTrainers.length
      ? airtableHelpers.activeTrainers
      : (debug.focus_source?.active_trainers || []);
    const horseDisplays = {
      ...(debug.focus_source?.horse_displays || {}),
      ...airtableHelpers.horseDisplays
    };
    const trainerDisplays = {
      ...(debug.focus_source?.trainer_displays || {}),
      ...airtableHelpers.trainerDisplays
    };
    const entryScheduleRows = runClassStart ? classStartRowsToScheduleRows(classStartRows) : scheduleRows;
    entryGoRows = buildEntryGoRows({
      showNo,
      focusDay: actualFocusDay,
      scheduleRows: entryScheduleRows,
      classOogRows: coreSnapshot.class_oog || [],
      activeTrainers,
      horseDisplays,
      trainerDisplays,
      nowIso
    });
  }
  const entryIgnoreSummary = buildEntryGoRows.lastIgnoreSummary || {
    manual_count: 0,
    automatic_count: 0,
    ignored_count: 0,
    grouped_count: 0
  };

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
  const classLinkSchema = runClassStart
    ? await ensureLinkedFields(TABLES.classStartTimes, {
      shows: TABLES.shows,
      focus_show: TABLES.focusShow,
      classes: TABLES.classes,
      rings: TABLES.rings,
      ring_days: TABLES.ringDays
    })
    : { created: 0 };
  const entryLinkSchema = runEntryGo
    ? await ensureLinkedFields(TABLES.entryGoTimes, {
      shows: TABLES.shows,
      focus_show: TABLES.focusShow,
      classes: TABLES.classes,
      rings: TABLES.rings,
      ring_days: TABLES.ringDays,
      entries: TABLES.entries,
      horses: TABLES.horses,
      riders: TABLES.riders,
      trainers: TABLES.trainers,
      class_oog: TABLES.classOog
    })
    : { created: 0 };
  const classOogLinkSchema = runEntryGo
    ? await ensureLinkedFields(TABLES.classOog, {
      shows: TABLES.shows,
      focus_show: TABLES.focusShow,
      classes: TABLES.classes,
      rings: TABLES.rings,
      ring_days: TABLES.ringDays,
      entries: TABLES.entries,
      horses: TABLES.horses,
      riders: TABLES.riders,
      trainers: TABLES.trainers
    })
    : { created: 0 };
  const classResult = runClassStart
    ? await upsertRecords(TABLES.classStartTimes, "class_start_key_mirror", classStartRows)
    : { seen: 0, changed: 0 };
  if (runEntryGo) entryResult = await upsertRecords(TABLES.entryGoTimes, "entry_go_key_mirror", entryGoRows);
  const classSupportLinkResult = runClassStart
    ? await linkClassStartTimesToSupportTables(showNo, actualFocusDay)
    : { class_records: 0, support_matches: 0, linked: 0 };
  const linkResult = runClassStart && runEntryGo
    ? await linkEntryGoTimesToClassStartTimes(showNo, actualFocusDay)
    : { class_records: 0, entry_records: 0, linked: 0 };
  const entryClassOogLinkResult = runEntryGo
    ? await linkEntryGoTimesToClassOog(showNo, actualFocusDay)
    : { entry_records: 0, class_oog_records: 0, matches: 0, linked: 0 };
  const entryHelperLinkResult = runEntryGo
    ? await linkEntryGoTimesToHelpers(showNo, actualFocusDay)
    : { entry_records: 0, helper_matches: 0, linked: 0 };
  const classOogLinkResult = runEntryGo
    ? await linkClassOogToGeneratedTablesAndHelpers(showNo, actualFocusDay)
    : {
      class_oog_records: 0,
      class_start_records: 0,
      entry_go_records: 0,
      class_start_matches: 0,
      entry_go_matches: 0,
      helper_matches: 0,
      linked: 0
    };
  const scopedCleanupResult = await cleanupTodayTomorrowLinks();
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
        source: catalystHasCore ? "focus-day-snapshot.update_schedule+counts" : "airtable.update_schedule+counts",
        counts_source_rows: Number(coreSnapshot.counts?.length || 0),
        counts_applied: classStartRows.filter((row) => clean(row.source).includes("counts.php")).length,
        support_link_fields_created: classLinkSchema.created,
        support_links_checked: classSupportLinkResult.class_records,
        support_links_changed: classSupportLinkResult.linked,
        entry_go_links_checked: linkResult.entry_records,
        entry_go_links_changed: linkResult.linked,
        class_oog_class_start_matches: classOogLinkResult.class_start_matches,
        class_oog_links_changed: classOogLinkResult.linked,
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
        support_link_fields_created: entryLinkSchema.created,
        class_oog_link_fields_created: classOogLinkSchema.created,
        rows: entryResult.seen,
        active_trainers: activeTrainers,
        class_start_links_checked: linkResult.class_records,
        class_start_links_changed: linkResult.linked,
        class_oog_links_checked: entryClassOogLinkResult.entry_records,
        class_oog_links_changed: entryClassOogLinkResult.linked,
        helper_links_changed: entryHelperLinkResult.linked,
        support_links_changed: entryHelperLinkResult.linked,
        class_oog_entry_go_matches: classOogLinkResult.entry_go_matches,
        class_oog_helper_matches: classOogLinkResult.helper_matches,
        class_oog_support_matches: classOogLinkResult.support_matches,
        class_oog_links_changed: classOogLinkResult.linked,
        scoped_cleanup: scopedCleanupResult,
        class_oog_ignore_manual: entryIgnoreSummary.manual_count,
        class_oog_ignore_automatic: entryIgnoreSummary.automatic_count,
        class_oog_ignore_total: entryIgnoreSummary.ignored_count,
        class_oog_ignore_groups: entryIgnoreSummary.grouped_count,
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
    linked_class_start_times_to_support_tables: classSupportLinkResult,
    linked_entry_go_times_to_class_start_times: linkResult,
    linked_entry_go_times_to_class_oog: entryClassOogLinkResult,
    linked_entry_go_times_to_helpers: entryHelperLinkResult,
    linked_class_oog_to_generated_tables_and_helpers: classOogLinkResult,
    scoped_today_tomorrow_cleanup: scopedCleanupResult,
    class_oog_ignore: entryIgnoreSummary,
    inactive: {
      class_start_times: classInactive,
      entry_go_times: entryInactive
    },
    fields_created: {
      class_start_times: classSchema.created,
      class_start_time_links: classLinkSchema.created,
      entry_go_times: entrySchema.created,
      entry_go_time_links: entryLinkSchema.created,
      class_oog_links: classOogLinkSchema.created
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
  linkClassStartTimesToSupportTables,
  linkClassOogToGeneratedTablesAndHelpers,
  linkEntryGoTimesToHelpers,
  cleanupTodayTomorrowLinks,
  paceFromLive
};
