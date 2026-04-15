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

  for (const record of rows || []) {
    const fields = record?.fields || {};
    const classId = numOrNull(fields.class_id);
    if (classId === null) continue;
    byClassId.set(String(classId), {
      recordId: record.id,
      record_id: strOrNull(fields.record_id) || record.id,
      class_groupxclasses_id: numOrNull(fields.class_groupxclasses_id),
      class_group_id: numOrNull(fields.class_group_id),
      class_id: classId,
      class_number: normalizeEntryNumber(fields.class_number),
      class_name: strOrNull(fields.class_name) || "",
      schedule_sequencetype: strOrNull(fields.schedule_sequencetype) || "",
      class_type: strOrNull(fields.class_type) || "",
      group_name: strOrNull(fields.group_name) || "",
      ring_number: numOrNull(fields.ring_number),
      estimated_start_time: strOrNull(fields.estimated_start_time) || "",
      estimated_end_time: strOrNull(fields.estimated_end_time),
      class_group_sequence: numOrNull(fields.class_group_sequence),
      schedule_show_datev2: toIsoDateOnly(pickFirst(fields.schedule_show_datev2, fields.show_date)),
    });
  }

  return byClassId;
}

function collectTripCandidates(obj, depth = 0, out = []) {
  if (depth > 6) return out;
  if (Array.isArray(obj)) {
    for (const item of obj) collectTripCandidates(item, depth + 1, out);
    return out;
  }
  if (!obj || typeof obj !== "object") return out;

  const hasClass = ("class_id" in obj) || ("classId" in obj);
  const hasHorse = ("horse" in obj) || ("Horse" in obj);
  const hasEntry = ("entry_id" in obj) || ("entryId" in obj) || ("entryxclasses_uuid" in obj);
  if (hasClass && hasEntry && hasHorse) out.push(obj);

  for (const value of Object.values(obj)) collectTripCandidates(value, depth + 1, out);
  return out;
}

function normalizePeopleTripRow(raw, ownerPid) {
  const classId = raw?.class_id ?? raw?.classId ?? null;
  const entryId = raw?.entry_id ?? raw?.entryId ?? null;
  const horse = raw?.horse ?? raw?.Horse ?? "";
  const entryxclassesUuid = raw?.entryxclasses_uuid ?? raw?.entryxclassesUUID ?? raw?.uuid ?? "";
  if (!classId || !entryId || !String(horse || "").trim() || !String(entryxclassesUuid || "").trim()) return null;

  const entryNumber = raw?.entry_number ?? raw?.entryNumber ?? raw?.entry_no ?? raw?.entryNo ?? raw?.number;

  return {
    pid: Number(ownerPid),
    class_id: Number(classId),
    entry_id: Number(entryId),
    entryxclasses_uuid: String(entryxclassesUuid).trim(),
    horse: String(horse).trim(),
    entry_number: normalizeEntryNumber(entryNumber),
    class_name: String(raw?.class_name ?? raw?.className ?? "").trim(),
    class_number: normalizeEntryNumber(raw?.class_number ?? raw?.classNumber),
    rider_name: String(raw?.rider_name ?? raw?.riderName ?? "").trim(),
    rider_id: numOrNull(raw?.rider_id ?? raw?.riderId) ?? undefined,
    placing: numOrNull(raw?.placing) ?? undefined,
  };
}

function normalizeTripsForScope({ trainerPids = [], peoplePayloads = new Map(), scheduleByClassId = new Map() }) {
  const normalizedRows = [];
  const outsideSchedule = [];
  const uniqueRows = new Map();

  for (const pid of trainerPids) {
    const payload = peoplePayloads.get(pid);
    const candidates = collectTripCandidates(payload);
    for (const raw of candidates) {
      const trip = normalizePeopleTripRow(raw, pid);
      if (!trip) continue;
      const schedule = scheduleByClassId.get(String(trip.class_id));
      if (!schedule) {
        outsideSchedule.push(`${trip.class_id}|${trip.entryxclasses_uuid}`);
        continue;
      }

      const row = {
        record_id: null,
        entryxclasses_uuid: trip.entryxclasses_uuid,
        pid: trip.pid,
        entry_id: trip.entry_id,
        entry_number: trip.entry_number,
        horse: trip.horse,
        class_id: schedule.class_id,
        class_number: schedule.class_number ?? trip.class_number,
        class_name: schedule.class_name || trip.class_name,
        schedule_sequencetype: schedule.schedule_sequencetype,
        class_type: schedule.class_type,
        class_group_id: schedule.class_group_id,
        group_name: schedule.group_name,
        class_groupxclasses_id: schedule.class_groupxclasses_id,
        ring_number: schedule.ring_number,
        estimated_start_time: schedule.estimated_start_time,
        estimated_end_time: schedule.estimated_end_time,
        class_group_sequence: schedule.class_group_sequence,
        schedule_show_datev2: schedule.schedule_show_datev2,
        rider_name: trip.rider_name,
        rider_id: trip.rider_id,
        placing: trip.placing,
        watch_schedule_record_id: schedule.recordId,
      };

      normalizedRows.push(row);
      const key = normalizeKey(row.entryxclasses_uuid);
      if (key && !uniqueRows.has(key)) uniqueRows.set(key, row);
    }
  }

  return {
    normalized_rows: normalizedRows,
    outside_schedule: outsideSchedule,
    unique_rows_by_key: uniqueRows,
    row_count: normalizedRows.length,
    unique_row_count: uniqueRows.size,
  };
}

module.exports = {
  buildScheduleMap,
  collectTripCandidates,
  normalizePeopleTripRow,
  normalizeTripsForScope,
};
