/**
 * Airtable Automation Script (CORRECTED FULL DROP)
 *
 * PURPOSE
 * - Read records from: watch_trips / view: classsignup
 * - For each source record:
 *    1) fetch classsignup_url
 *    2) find ONLY the matching entry_x_classes[] row where payload.entry_id == watch_trips.entry_id
 *    3) update that watch_trips record:
 *         - classsignup_oog
 *         - classsignup_at
 *         - entry_oog
 *    4) create / upsert ONE row into entry_oog for that matched entry only
 *
 * SOURCE
 * - table: watch_trips
 * - view : classsignup
 *
 * REQUIRED SOURCE FIELDS
 * - classsignup_url
 * - entry_id
 *
 * OPTIONAL SOURCE UPDATE FIELDS
 * - classsignup_oog   (number)
 * - classsignup_at    (date/time)
 * - entry_oog         (single line text)
 *
 * TARGET
 * - table: entry_oog
 * - upsert key field: entry_oog
 *
 * TARGET FIELDS
 * - entry_oog   (text)   => class_number-entry_number
 * - sid         (number)
 * - class_id    (number)
 * - class_number(number)
 * - entry_id    (number)
 * - entry_number(number)
 * - trainer_id  (number)
 * - rider_id    (number)
 * - order_of_go (number)
 * - dt          (text YYYY-MM-DD)
 * - class_name  (text)
 * - group_name  (text)
 * - class_type  (text)
 * - horse       (text)
 * - last_run    (date/time)
 */

const cfg = input.config ? input.config() : {};

const SOURCE_TABLE_NAME = cfg.source_table ?? "watch_trips";
const SOURCE_VIEW_NAME = cfg.source_view ?? "classsignup";
const SOURCE_URL_FIELD = cfg.source_url_field ?? "classsignup_url";
const SOURCE_ENTRY_ID_FIELD = cfg.source_entry_id_field ?? "entry_id";

const SOURCE_UPDATE = {
  classsignup_oog: cfg.source_classsignup_oog_field ?? "classsignup_oog",
  classsignup_at: cfg.source_classsignup_at_field ?? "classsignup_at",
  entry_oog: cfg.source_entry_oog_field ?? "entry_oog",
};

const TARGET_TABLE_NAME = cfg.target_table ?? "entry_oog";
const TARGET_KEY_FIELD = cfg.target_key_field ?? "entry_oog";

const TARGET = {
  entry_oog: "entry_oog",
  sid: "sid",
  class_id: "class_id",
  class_number: "class_number",
  entry_id: "entry_id",
  entry_number: "entry_number",
  trainer_id: "trainer_id",
  rider_id: "rider_id",
  order_of_go: "order_of_go",
  dt: "dt",
  class_name: "class_name",
  group_name: "group_name",
  class_type: "class_type",
  horse: "horse",
  last_run: "last_run",
};

const NOW_ISO = new Date().toISOString();
const FETCH_FN = typeof remoteFetchAsync === "function" ? remoteFetchAsync : fetch;

function getField(table, name) {
  return table.fields.find(f => f.name === name) ?? null;
}

function hasField(table, name) {
  return !!getField(table, name);
}

function toNum(value) {
  if (value === null || value === undefined || value === "") return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function toText(value) {
  if (value === null || value === undefined) return null;
  const s = String(value).trim();
  return s === "" ? null : s;
}

function toDateText(value) {
  if (!value) return null;
  const s = String(value);
  return s.length >= 10 ? s.slice(0, 10) : s;
}

function buildEntryOog(classNumber, entryNumber) {
  if (classNumber === null || classNumber === undefined || classNumber === "") return null;
  if (entryNumber === null || entryNumber === undefined || entryNumber === "") return null;
  return `${classNumber}-${entryNumber}`;
}

function setIfPresent(table, obj, fieldName, value) {
  if (!hasField(table, fieldName)) return;
  if (value === undefined) return;
  obj[fieldName] = value;
}

async function fetchJson(url) {
  const res = await FETCH_FN(url, {
    method: "GET",
    headers: { Accept: "application/json" },
  });

  if (!res.ok) {
    throw new Error(`Fetch failed ${res.status} for ${url}`);
  }

  return await res.json();
}

async function batchCreate(table, rows) {
  for (let i = 0; i < rows.length; i += 50) {
    await table.createRecordsAsync(rows.slice(i, i + 50));
  }
}

async function batchUpdate(table, rows) {
  for (let i = 0; i < rows.length; i += 50) {
    await table.updateRecordsAsync(rows.slice(i, i + 50));
  }
}

try {
  const sourceTable = base.getTable(SOURCE_TABLE_NAME);
  const sourceView = sourceTable.getView(SOURCE_VIEW_NAME);
  const targetTable = base.getTable(TARGET_TABLE_NAME);

  if (!hasField(sourceTable, SOURCE_URL_FIELD)) {
    throw new Error(`Missing source field: ${SOURCE_URL_FIELD}`);
  }
  if (!hasField(sourceTable, SOURCE_ENTRY_ID_FIELD)) {
    throw new Error(`Missing source field: ${SOURCE_ENTRY_ID_FIELD}`);
  }
  if (!hasField(targetTable, TARGET_KEY_FIELD)) {
    throw new Error(`Missing target key field: ${TARGET_KEY_FIELD}`);
  }

  const sourceFieldsToSelect = [SOURCE_URL_FIELD, SOURCE_ENTRY_ID_FIELD];
  if (hasField(sourceTable, SOURCE_UPDATE.classsignup_oog)) sourceFieldsToSelect.push(SOURCE_UPDATE.classsignup_oog);
  if (hasField(sourceTable, SOURCE_UPDATE.classsignup_at)) sourceFieldsToSelect.push(SOURCE_UPDATE.classsignup_at);
  if (hasField(sourceTable, SOURCE_UPDATE.entry_oog)) sourceFieldsToSelect.push(SOURCE_UPDATE.entry_oog);

  const sourceQuery = await sourceView.selectRecordsAsync({ fields: sourceFieldsToSelect });
  const targetQuery = await targetTable.selectRecordsAsync({ fields: [TARGET_KEY_FIELD] });

  const existingTargetByKey = new Map();
  let duplicateExistingKeys = 0;

  for (const rec of targetQuery.records) {
    const key = rec.getCellValueAsString(TARGET_KEY_FIELD).trim();
    if (!key) continue;
    if (existingTargetByKey.has(key)) {
      duplicateExistingKeys++;
      continue;
    }
    existingTargetByKey.set(key, rec.id);
  }

  const payloadCache = new Map();
  const sourceUpdates = [];
  const stagedTargetByKey = new Map();

  let sourceSkippedMissingUrl = 0;
  let sourceSkippedMissingEntryId = 0;
  let urlsFetched = 0;
  let payloadErrors = 0;
  let sourceRowsMatched = 0;
  let sourceRowsUnmatched = 0;
  let targetRowsBuilt = 0;
  let targetRowsSkipped = 0;

  for (const rec of sourceQuery.records) {
    const url = rec.getCellValueAsString(SOURCE_URL_FIELD).trim();
    const sourceEntryId = toNum(rec.getCellValueAsString(SOURCE_ENTRY_ID_FIELD).trim());

    if (!url) {
      sourceSkippedMissingUrl++;
      continue;
    }
    if (sourceEntryId === null) {
      sourceSkippedMissingEntryId++;
      continue;
    }

    let data;
    try {
      if (payloadCache.has(url)) {
        data = payloadCache.get(url);
      } else {
        data = await fetchJson(url);
        payloadCache.set(url, data);
        urlsFetched++;
      }
    } catch (err) {
      payloadErrors++;
      console.log(`ERROR url=${url} msg=${err.message}`);
      continue;
    }

    const sid = toNum(data?.show?.show_id ?? data?.show_id);
    const dt = toDateText(data?.class_group?.day);
    const classId = toNum(data?.class_data?.class_id);
    const classNumber = toNum(data?.class_data?.class_number);
    const className = toText(data?.class_data?.name);
    const groupName = toText(data?.class_group?.group_name);
    const classType = toText(data?.class_data?.class_type);

    const entries = Array.isArray(data?.entry_x_classes) ? data.entry_x_classes : [];
    const matched = entries.find(item => toNum(item?.entry_id) === sourceEntryId);

    if (!matched) {
      sourceRowsUnmatched++;
      continue;
    }

    sourceRowsMatched++;

    const entryId = toNum(matched?.entry_id);
    const entryNumber = toNum(matched?.entry_number);
    const trainerId = toNum(matched?.trainer_id);
    const riderId = toNum(matched?.rider_id);
    const orderOfGo = toNum(matched?.order_of_go);
    const horse = toText(matched?.horse);
    const entryOog = buildEntryOog(classNumber, entryNumber);

    // Update source watch_trips record
    const sourceFields = {};
    setIfPresent(sourceTable, sourceFields, SOURCE_UPDATE.classsignup_oog, orderOfGo);
    setIfPresent(sourceTable, sourceFields, SOURCE_UPDATE.classsignup_at, NOW_ISO);
    setIfPresent(sourceTable, sourceFields, SOURCE_UPDATE.entry_oog, entryOog);

    if (Object.keys(sourceFields).length > 0) {
      sourceUpdates.push({ id: rec.id, fields: sourceFields });
    }

    // Upsert single target record for this matched entry only
    if (!entryOog) {
      targetRowsSkipped++;
      continue;
    }

    const targetFields = {};
    setIfPresent(targetTable, targetFields, TARGET.entry_oog, entryOog);
    setIfPresent(targetTable, targetFields, TARGET.sid, sid);
    setIfPresent(targetTable, targetFields, TARGET.class_id, classId);
    setIfPresent(targetTable, targetFields, TARGET.class_number, classNumber);
    setIfPresent(targetTable, targetFields, TARGET.entry_id, entryId);
    setIfPresent(targetTable, targetFields, TARGET.entry_number, entryNumber);
    setIfPresent(targetTable, targetFields, TARGET.trainer_id, trainerId);
    setIfPresent(targetTable, targetFields, TARGET.rider_id, riderId);
    setIfPresent(targetTable, targetFields, TARGET.order_of_go, orderOfGo);
    setIfPresent(targetTable, targetFields, TARGET.dt, dt);
    setIfPresent(targetTable, targetFields, TARGET.class_name, className);
    setIfPresent(targetTable, targetFields, TARGET.group_name, groupName);
    setIfPresent(targetTable, targetFields, TARGET.class_type, classType);
    setIfPresent(targetTable, targetFields, TARGET.horse, horse);
    setIfPresent(targetTable, targetFields, TARGET.last_run, NOW_ISO);

    stagedTargetByKey.set(entryOog, targetFields);
    targetRowsBuilt++;
  }

  const targetCreates = [];
  const targetUpdates = [];

  for (const [key, fields] of stagedTargetByKey.entries()) {
    const existingId = existingTargetByKey.get(key);
    if (existingId) {
      targetUpdates.push({ id: existingId, fields });
    } else {
      targetCreates.push({ fields });
    }
  }

  await batchUpdate(sourceTable, sourceUpdates);
  await batchCreate(targetTable, targetCreates);
  await batchUpdate(targetTable, targetUpdates);

  output.set("ok", true);
  output.set("done", true);
  output.set("source_records_in_view", sourceQuery.records.length);
  output.set("source_skipped_missing_url", sourceSkippedMissingUrl);
  output.set("source_skipped_missing_entry_id", sourceSkippedMissingEntryId);
  output.set("unique_urls_fetched", urlsFetched);
  output.set("payload_errors", payloadErrors);
  output.set("source_rows_matched", sourceRowsMatched);
  output.set("source_rows_unmatched", sourceRowsUnmatched);
  output.set("source_updates", sourceUpdates.length);
  output.set("target_rows_built", targetRowsBuilt);
  output.set("target_rows_skipped", targetRowsSkipped);
  output.set("target_creates", targetCreates.length);
  output.set("target_updates", targetUpdates.length);
  output.set("duplicate_existing_keys", duplicateExistingKeys);
} catch (err) {
  output.set("ok", false);
  output.set("done", true);
  output.set("error", err.message);
  console.log(err);
}
