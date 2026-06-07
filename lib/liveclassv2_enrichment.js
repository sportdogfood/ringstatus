const DEFAULT_LIVECLASS_BASE_URL =
  "https://sgl.wellingtoninternational.com/iphonev2/index.php/esp/liveclassv2";

function isBlank(value) {
  return value === null ||
    value === undefined ||
    (typeof value === "string" && value.trim() === "") ||
    String(value).trim().toLowerCase() === "null" ||
    String(value).trim().toLowerCase() === "nan";
}

function strOrNull(value) {
  if (isBlank(value)) return null;
  return String(value).trim();
}

function numOrNull(value) {
  if (isBlank(value)) return null;
  const num = Number(value);
  return Number.isFinite(num) ? num : null;
}

function boolValue(value) {
  if (typeof value === "boolean") return value;
  if (typeof value === "number") return value !== 0;
  if (typeof value === "string") {
    const text = value.trim().toLowerCase();
    if (["true", "yes", "1", "checked"].includes(text)) return true;
    if (["false", "no", "0", "unchecked"].includes(text)) return false;
  }
  return false;
}

function toIsoDateOnly(value) {
  if (isBlank(value)) return null;
  if (value instanceof Date) return value.toISOString().slice(0, 10);
  const text = String(value).trim();
  if (/^\d{4}-\d{2}-\d{2}$/.test(text)) return text;
  if (/^\d{4}-\d{2}-\d{2}T/.test(text)) return text.slice(0, 10);
  const parsed = Date.parse(text);
  return Number.isFinite(parsed) ? new Date(parsed).toISOString().slice(0, 10) : null;
}

function splitNumericStrings(value) {
  let raw = Array.isArray(value) ? value : null;
  if (!raw) {
    const text = String(value || "").trim();
    if (text.startsWith("[") && text.endsWith("]")) {
      try {
        const parsed = JSON.parse(text);
        if (Array.isArray(parsed)) raw = parsed;
      } catch {
        raw = null;
      }
    }
    if (!raw) raw = text.split(",");
  }
  return raw
    .map((item) => String(item).trim())
    .filter(Boolean)
    .filter((item) => numOrNull(item) !== null);
}

function normalizeLiveGroupsRecord(record) {
  const fields = record?.fields || record || {};
  const classGroupId = numOrNull(fields.class_group_id);
  const showId = numOrNull(fields.show_id);
  const day = toIsoDateOnly(fields.live_focus_day) || toIsoDateOnly(fields.day);
  const classIds = splitNumericStrings(fields.class_ids || fields.classes);
  const classNumbers = splitNumericStrings(
    fields.class_numbers || fields.classNumbers || fields.class_numbers_list
  );

  if (classGroupId === null || showId === null || !classIds.length) return null;

  const class_pairs = classIds.map((classId, index) => ({
    class_id: classId,
    class_number: classNumbers[index] || null,
  }));

  return {
    recordId: record.id,
    class_group_id: classGroupId,
    show_id: showId,
    day,
    class_ids: classIds,
    class_numbers: classNumbers,
    class_pairs,
    has_JSON: boolValue(fields.has_JSON),
    status: strOrNull(fields.status),
    gone: numOrNull(fields.gone),
    total: numOrNull(fields.total),
    estimated_start_time: strOrNull(fields.estimated_start_time),
    ring_number: numOrNull(fields.ring_number),
    curr_updated_at: strOrNull(fields.curr_updated_at),
    ingested_at: strOrNull(fields.ingested_at),
  };
}

function buildLiveGroupsMap(records, appCtx) {
  const showId = numOrNull(appCtx?.app_show_id);
  const sqlDate = toIsoDateOnly(appCtx?.app_sql_date);
  const out = new Map();

  for (const record of records || []) {
    const row = normalizeLiveGroupsRecord(record);
    if (!row) continue;
    if (showId !== null && row.show_id !== showId) continue;
    if (sqlDate && row.day && row.day !== sqlDate) continue;
    if (!row.has_JSON) continue;
    out.set(String(row.class_group_id), row);
  }

  return out;
}

function resolveLiveClassIdsForTrip(liveGroup, { classId, classNumber } = {}) {
  const resolved = [];
  const seen = new Set();

  function push(value) {
    const normalized = strOrNull(value);
    if (!normalized || seen.has(normalized)) return;
    seen.add(normalized);
    resolved.push(normalized);
  }

  const explicitClassId = numOrNull(classId);
  if (explicitClassId !== null) push(explicitClassId);

  const wantedClassNumber = numOrNull(classNumber);
  if (wantedClassNumber !== null) {
    for (const pair of liveGroup?.class_pairs || []) {
      if (numOrNull(pair.class_number) === wantedClassNumber) {
        push(pair.class_id);
      }
    }
  }

  if (!resolved.length) {
    for (const fallbackClassId of liveGroup?.class_ids || []) push(fallbackClassId);
  }

  return resolved;
}

function buildLiveClassDataEndpoint({
  baseUrl = DEFAULT_LIVECLASS_BASE_URL,
  showId,
  classId,
  classGroupId,
  cacheBuster = Date.now(),
}) {
  if (isBlank(showId) || isBlank(classId)) return null;
  const url = new URL("getLiveClassData", String(baseUrl).replace(/\/+$/, "") + "/");
  url.searchParams.set("show_id", String(showId));
  url.searchParams.set("cid", String(classId));
  if (!isBlank(classGroupId)) url.searchParams.set("cgid", String(classGroupId));
  if (!isBlank(cacheBuster)) url.searchParams.set("t", String(cacheBuster));
  return url.toString();
}

function normalizeLiveClassDataPayload(payload) {
  const rows = Array.isArray(payload?.rows) ? payload.rows : [];
  return {
    class_id: numOrNull(payload?.ID),
    total_records: numOrNull(payload?.recs),
    ring_number: numOrNull(payload?.ring_number),
    curr_updated_at: strOrNull(payload?.curr_updated_at),
    rows: rows.map((row) => ({
      live_trip_id: strOrNull(row?.id),
      entry_id: numOrNull(row?.id),
      entry_number: numOrNull(row?.ENo),
      horse: strOrNull(row?.Hor),
      rider_name: strOrNull(row?.Rid),
      order_of_go: numOrNull(row?.OOG),
      actual_order: numOrNull(row?.Actual_OOG),
      gone_in: numOrNull(row?.Gone),
      position: numOrNull(row?.Pos),
    })),
  };
}

function findLiveClassTrip(normalizedPayload, { entryNumber } = {}) {
  const wantedEntryNumber = numOrNull(entryNumber);
  if (wantedEntryNumber === null) return null;
  return (normalizedPayload?.rows || []).find((row) => row.entry_number === wantedEntryNumber) || null;
}

module.exports = {
  DEFAULT_LIVECLASS_BASE_URL,
  buildLiveGroupsMap,
  buildLiveClassDataEndpoint,
  findLiveClassTrip,
  normalizeLiveGroupsRecord,
  normalizeLiveClassDataPayload,
  resolveLiveClassIdsForTrip,
};
