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

function pickFirst(...values) {
  for (const value of values) {
    if (!isBlank(value)) return value;
  }
  return undefined;
}

function normalizeEntryNumber(value) {
  if (value === undefined || value === null) return undefined;
  if (typeof value === "number" && Number.isFinite(value)) return value;
  const text = String(value).trim();
  if (!text) return undefined;
  if (/^\d+$/.test(text)) return Number(text);
  return text;
}

function normalizeKey(value) {
  if (isBlank(value)) return "";
  return String(value).trim();
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

function fallbackKeyPart(value) {
  return normalizeKey(value)
    .toLowerCase()
    .replace(/\s+/g, "_")
    .replace(/[^a-z0-9_.:-]/g, "");
}

function buildPeopleTripKey({ classNumber, entryNumber }) {
  if (isBlank(classNumber) || isBlank(entryNumber)) return "";
  return [
    "people",
    fallbackKeyPart(classNumber),
    fallbackKeyPart(entryNumber),
  ].join(":");
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

function buildScheduleMap(rows) {
  const byClassId = new Map();
  const byClassNumber = new Map();

  for (const record of rows || []) {
    const fields = record?.fields || {};
    const classId = numOrNull(fields.class_id);
    const classNumber = normalizeEntryNumber(fields.class_number);
    if (classId === null && classNumber === undefined) continue;

    const schedule = {
      recordId: record.id,
      record_id: strOrNull(fields.record_id) || record.id,
      schedule_key: strOrNull(fields.schedule_key),
      schedule_short: strOrNull(fields.schedule_short),
      class_sequence: strOrNull(fields.class_sequence),
      class_groupxclasses_id: numOrNull(fields.class_groupxclasses_id),
      class_group_id: numOrNull(fields.class_group_id),
      class_id: classId,
      class_number: classNumber,
      class_name: strOrNull(fields.class_name) || "",
      schedule_sequencetype: strOrNull(fields.schedule_sequencetype) || "",
      class_type: strOrNull(fields.class_type) || "",
      group_name: strOrNull(fields.group_name) || "",
      ring_number: numOrNull(fields.ring_number),
      estimated_start_time: strOrNull(fields.estimated_start_time) || "",
      estimated_end_time: strOrNull(fields.estimated_end_time),
      total_trips: numOrNull(fields.total_trips),
      completed_trips: numOrNull(fields.completed_trips),
      status: strOrNull(fields.status),
      class_group_sequence: numOrNull(fields.class_group_sequence),
      is_target: boolValue(fields.is_target),
      schedule_show_datev2: toIsoDateOnly(pickFirst(fields.schedule_show_datev2, fields[" scheduled_date"], fields.show_date)),
    };

    if (classId !== null) byClassId.set(String(classId), schedule);
    if (classNumber !== undefined) {
      const classNumberKey = String(classNumber);
      const existing = byClassNumber.get(classNumberKey);
      if (!existing || (!existing.is_target && schedule.is_target)) {
        byClassNumber.set(classNumberKey, schedule);
      }
    }
  }

  byClassId.byClassNumber = byClassNumber;
  return byClassId;
}

function findScheduleForTrip(scheduleByClassId, trip) {
  if (trip?.class_id !== null && trip?.class_id !== undefined) {
    const schedule = scheduleByClassId.get(String(trip.class_id));
    if (schedule) return schedule;
  }

  if (trip?.class_number !== undefined && scheduleByClassId?.byClassNumber instanceof Map) {
    const schedule = scheduleByClassId.byClassNumber.get(String(trip.class_number));
    if (schedule) return schedule;
  }

  return null;
}

function collectTripCandidates(obj, depth = 0, out = []) {
  if (depth > 6) return out;
  if (Array.isArray(obj)) {
    for (const item of obj) collectTripCandidates(item, depth + 1, out);
    return out;
  }
  if (!obj || typeof obj !== "object") return out;

  const hasClass = ("class_id" in obj) || ("classId" in obj) ||
    ("class_number" in obj) || ("classNumber" in obj);
  const hasHorse = ("horse" in obj) || ("Horse" in obj);
  const hasEntry = ("entry_id" in obj) || ("entryId" in obj) ||
    ("entry_number" in obj) || ("entryNumber" in obj) ||
    ("entry_no" in obj) || ("entryNo" in obj) ||
    ("number" in obj) || ("entryxclasses_uuid" in obj);
  if (hasClass && hasEntry && hasHorse) out.push(obj);

  for (const value of Object.values(obj)) collectTripCandidates(value, depth + 1, out);
  return out;
}

function normalizePeopleTripRow(raw, ownerPid) {
  const classId = numOrNull(raw?.class_id ?? raw?.classId);
  const entryId = numOrNull(raw?.entry_id ?? raw?.entryId);
  const horse = raw?.horse ?? raw?.Horse ?? "";
  const classNumber = normalizeEntryNumber(raw?.class_number ?? raw?.classNumber);
  const entryNumber = raw?.entry_number ?? raw?.entryNumber ?? raw?.entry_no ?? raw?.entryNo ?? raw?.number;
  const normalizedEntryNumber = normalizeEntryNumber(entryNumber);
  const rawEntryxclassesUuid = strOrNull(raw?.entryxclasses_uuid ?? raw?.entryxclassesUUID ?? raw?.uuid);
  const tripKey = buildPeopleTripKey({
    classNumber,
    entryNumber: normalizedEntryNumber,
  }) || rawEntryxclassesUuid;

  if (classId === null && classNumber === undefined) return null;
  if (entryId === null && normalizedEntryNumber === undefined && !rawEntryxclassesUuid) return null;
  if (!String(horse || "").trim() || !tripKey) return null;

  return {
    trip_key: tripKey,
    pid: Number(ownerPid),
    class_id: classId,
    entry_id: entryId,
    entryxclasses_uuid: rawEntryxclassesUuid,
    horse: String(horse).trim(),
    entry_number: normalizedEntryNumber,
    class_name: String(raw?.class_name ?? raw?.className ?? "").trim(),
    class_number: classNumber,
    class_group_id: numOrNull(raw?.class_group_id ?? raw?.classGroupId) ?? undefined,
    rider_name: String(raw?.rider_name ?? raw?.riderName ?? "").trim(),
    rider_id: numOrNull(raw?.rider_id ?? raw?.riderId) ?? undefined,
    placing: numOrNull(raw?.placing) ?? undefined,
    order_of_go: numOrNull(raw?.order_of_go ?? raw?.orderOfGo) ?? undefined,
    status: strOrNull(raw?.status ?? raw?.class_status ?? raw?.classStatus) ?? undefined,
  };
}

function normalizeTripsForScope({ sourceIds = [], trainerPids = [], peoplePayloads = new Map(), scheduleByClassId = new Map() }) {
  const effectiveSourceIds = Array.isArray(sourceIds) && sourceIds.length ? sourceIds : trainerPids;
  const normalizedRows = [];
  const outsideSchedule = [];
  const uniqueRows = new Map();
  const sourceRowCounts = {};
  const emptySourceIds = [];

  for (const sourceId of effectiveSourceIds) {
    const payload = peoplePayloads.get(sourceId);
    const candidates = collectTripCandidates(payload);
    let keptCount = 0;
    for (const raw of candidates) {
      const trip = normalizePeopleTripRow(raw, sourceId);
      if (!trip) continue;
      const schedule = findScheduleForTrip(scheduleByClassId, trip);
      if (!schedule) {
        outsideSchedule.push(`${trip.class_id ?? trip.class_number}|${trip.trip_key || trip.entryxclasses_uuid}`);
        continue;
      }

      const row = {
        record_id: null,
        trip_key: trip.trip_key,
        entryxclasses_uuid: trip.entryxclasses_uuid,
        pid: trip.pid,
        entry_id: trip.entry_id,
        entry_number: trip.entry_number,
        horse: trip.horse,
        class_id: schedule.class_id ?? trip.class_id,
        class_number: schedule.class_number ?? trip.class_number,
        class_name: schedule.class_name || trip.class_name,
        schedule_sequencetype: schedule.schedule_sequencetype,
        class_type: schedule.class_type,
        class_group_id: schedule.class_group_id ?? trip.class_group_id,
        group_name: schedule.group_name,
        class_groupxclasses_id: schedule.class_groupxclasses_id,
        ring_number: schedule.ring_number,
        estimated_start_time: schedule.estimated_start_time,
        estimated_end_time: schedule.estimated_end_time,
        total_trips: schedule.total_trips,
        completed_trips: schedule.completed_trips,
        class_group_sequence: schedule.class_group_sequence,
        schedule_key: schedule.schedule_key,
        schedule_short: schedule.schedule_short,
        class_sequence: schedule.class_sequence,
        schedule_show_datev2: schedule.schedule_show_datev2,
        rider_name: trip.rider_name,
        rider_id: trip.rider_id,
        placing: trip.placing,
        order_of_go: trip.order_of_go,
        status: trip.status || schedule.status,
        watch_schedule_record_id: schedule.recordId,
      };

      normalizedRows.push(row);
      keptCount += 1;
      const key = normalizeKey(row.trip_key || row.entryxclasses_uuid);
      if (key && !uniqueRows.has(key)) uniqueRows.set(key, row);
    }
    sourceRowCounts[String(sourceId)] = keptCount;
    if (candidates.length === 0) emptySourceIds.push(String(sourceId));
  }

  return {
    normalized_rows: normalizedRows,
    outside_schedule: outsideSchedule,
    unique_rows_by_key: uniqueRows,
    row_count: normalizedRows.length,
    unique_row_count: uniqueRows.size,
    source_row_counts: sourceRowCounts,
    empty_source_ids: emptySourceIds,
  };
}

module.exports = {
  buildPeopleTripKey,
  buildScheduleMap,
  collectTripCandidates,
  normalizePeopleTripRow,
  normalizeTripsForScope,
};
