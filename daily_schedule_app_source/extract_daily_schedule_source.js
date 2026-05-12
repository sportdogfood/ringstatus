#!/usr/bin/env node
"use strict";

const fs = require("node:fs");
const path = require("node:path");

const ROOT = __dirname;
const DEFAULT_CONTRACT_PATH = path.join(ROOT, "field_contract.json");

function isBlank(value) {
  return value === undefined || value === null || String(value).trim() === "";
}

function asArray(value) {
  if (Array.isArray(value)) return value;
  if (isBlank(value)) return [];
  return [value];
}

function firstValue(...values) {
  for (const value of values) {
    if (Array.isArray(value)) {
      const first = value.find((item) => !isBlank(item));
      if (!isBlank(first)) return first;
    } else if (!isBlank(value)) {
      return value;
    }
  }
  return null;
}

function cleanValue(value) {
  if (Array.isArray(value)) return value.map(cleanValue).filter((item) => !isBlank(item));
  if (value && typeof value === "object") return value;
  if (typeof value === "string") return value.trim();
  return value ?? null;
}

function compactObject(input) {
  const out = {};
  for (const [key, value] of Object.entries(input || {})) {
    const cleaned = cleanValue(value);
    if (Array.isArray(cleaned) && cleaned.length === 0) continue;
    if (isBlank(cleaned)) continue;
    out[key] = cleaned;
  }
  return out;
}

function joinKey(parts) {
  return parts.map((part) => String(part ?? "").trim()).join("|");
}

function makeScheduleKey(parts) {
  return joinKey([
    parts.sid,
    parts.sql_date,
    parts.ring_number,
    parts.class_number,
    parts.class_sequence,
  ]);
}

function makeScheduleShort(parts) {
  return joinKey([parts.ring_number, parts.class_number, parts.class_sequence]);
}

function makeScheduleInstanceKey(parts) {
  const base = makeScheduleKey(parts);
  if (!isBlank(parts.cgid)) return `${base}|cgid:${String(parts.cgid).trim()}`;
  if (!isBlank(parts.class_groupxclasses_id)) return `${base}|cgxc:${String(parts.class_groupxclasses_id).trim()}`;
  return `${base}|record:${String(parts.record_id || "").trim()}`;
}

function makeTripsKey(parts) {
  return joinKey([
    parts.sid,
    parts.sql_date,
    parts.ring_number,
    parts.class_number,
    parts.class_sequence,
    parts.pid,
    parts.entry_number,
  ]);
}

function makeTripsShortKey(parts) {
  return joinKey([parts.class_number, parts.class_sequence, parts.pid, parts.entry_number]);
}

function makeTripInstanceKey(parts) {
  const base = makeTripsKey(parts);
  if (!isBlank(parts.trip_tie_breaker)) return `${base}|${String(parts.trip_tie_breaker).trim()}`;
  return `${base}|record:${String(parts.record_id || "").trim()}`;
}

function makeFullNestingKey(parts) {
  return joinKey([
    parts.sid,
    parts.sql_date,
    parts.ring_number,
    parts.time,
    parts.cgid,
    parts.class_number,
    parts.class_sequence,
    parts.pid,
    parts.entry_number,
  ]);
}

function rowId(row) {
  return row?.id || row?.recordId || null;
}

function fieldsOf(row) {
  return row?.fields || {};
}

function scheduleIdentity(row) {
  const f = fieldsOf(row);
  const sid = firstValue(f.sid, f.show_id, f.app_show_idv2);
  const sqlDate = firstValue(f.schedule_show_datev2, f.app_sql_datev2, f.show_date);
  const classSequence = firstValue(f.class_group_sequence, f.class_sequence, f.entry_sequence);
  const explicitTieBreaker = firstValue(f.entry_sequence);
  const classGroupTieBreaker = firstValue(f.class_group_id);
  const classGroupXClassesTieBreaker = firstValue(f.class_groupxclasses_id);
  const fallbackTieBreaker = `fallback:${firstValue(f.estimated_start_time, f.time_sort, f.class_id, f.class_groupxclasses_id) || "no-time"}:${rowId(row) || "no-record-id"}`;
  const parts = {
    sid,
    sql_date: sqlDate,
    ring_number: firstValue(f.ring_number),
    time: firstValue(f.estimated_start_time),
    cgid: firstValue(f.class_group_id),
    class_groupxclasses_id: firstValue(f.class_groupxclasses_id),
    class_number: firstValue(f.class_number),
    class_sequence: classSequence,
    record_id: rowId(row),
    schedule_tie_breaker: firstValue(
      explicitTieBreaker,
      isBlank(classGroupTieBreaker) ? null : `class_group_id:${classGroupTieBreaker}`,
      isBlank(classGroupXClassesTieBreaker) ? null : `class_groupxclasses_id:${classGroupXClassesTieBreaker}`,
      fallbackTieBreaker
    ),
    schedule_tie_breaker_source: !isBlank(explicitTieBreaker)
      ? "entry_sequence"
      : !isBlank(classGroupTieBreaker)
        ? "class_group_id"
        : !isBlank(classGroupXClassesTieBreaker)
          ? "class_groupxclasses_id"
          : "fallback:estimated_start_time_record_id",
  };
  return {
    ...parts,
    schedule_key: makeScheduleKey(parts),
    schedule_instance_key: makeScheduleInstanceKey(parts),
    schedule_short: makeScheduleShort(parts),
  };
}

function tripIdentity(row) {
  const f = fieldsOf(row);
  const sid = firstValue(f.sid, f.show_id, f.app_show_idv2);
  const sqlDate = firstValue(f.schedule_show_datev2, f.scheduled_date, f.app_sql_datev2, f.show_date);
  const classSequence = firstValue(f.class_group_sequence, f.class_sequence, f.entry_sequence);
  const explicitTieBreaker = firstValue(f.entry_sequence);
  const hEidTieBreaker = firstValue(f.h_eid);
  const entryIdTieBreaker = firstValue(f.entry_id);
  const recordTieBreaker = rowId(row);
  const tripTieBreaker = firstValue(
    isBlank(explicitTieBreaker) ? null : `entry_sequence:${explicitTieBreaker}`,
    isBlank(hEidTieBreaker) ? null : `h_eid:${hEidTieBreaker}`,
    isBlank(entryIdTieBreaker) ? null : `entry_id:${entryIdTieBreaker}`,
    isBlank(recordTieBreaker) ? null : `record:${recordTieBreaker}`
  );
  const tripTieBreakerSource = !isBlank(explicitTieBreaker)
    ? "entry_sequence"
    : !isBlank(hEidTieBreaker)
      ? "h_eid"
      : !isBlank(entryIdTieBreaker)
        ? "entry_id"
        : "record_id";
  const parts = {
    sid,
    sql_date: sqlDate,
    ring_number: firstValue(f.ring_number),
    time: firstValue(f.estimated_go_time, f.estimated_start_time),
    cgid: firstValue(f.class_group_id),
    class_number: firstValue(f.class_number),
    class_sequence: classSequence,
    pid: firstValue(f.pid),
    entry_number: firstValue(f.entry_number),
    schedule_tie_breaker: firstValue(f.entry_sequence),
    trip_tie_breaker: tripTieBreaker,
    trip_tie_breaker_source: tripTieBreakerSource,
    record_id: rowId(row),
  };
  return {
    ...parts,
    schedule_key: makeScheduleKey(parts),
    schedule_short: makeScheduleShort(parts),
    trips_key: makeTripsKey(parts),
    trip_instance_key: makeTripInstanceKey(parts),
    trips_short_key: makeTripsShortKey(parts),
    full_nesting_key: makeFullNestingKey(parts),
  };
}

function latestByLinkedRecord(rows, linkFields) {
  const map = new Map();
  const sorted = [...(rows || [])].sort((a, b) => {
    const at = Date.parse(firstValue(fieldsOf(a).created_at, fieldsOf(a).Created) || "") || 0;
    const bt = Date.parse(firstValue(fieldsOf(b).created_at, fieldsOf(b).Created) || "") || 0;
    return bt - at;
  });

  for (const row of sorted) {
    const f = fieldsOf(row);
    const ids = [];
    for (const fieldName of linkFields) {
      ids.push(...asArray(f[fieldName]));
    }
    for (const id of ids.map((value) => String(value || "").trim()).filter(Boolean)) {
      if (!map.has(id)) map.set(id, row);
    }
  }
  return map;
}

function pickPrefixed(fields, prefix) {
  const out = {};
  for (const [key, value] of Object.entries(fields || {})) {
    if (key.startsWith(prefix) && !isBlank(value)) out[key] = cleanValue(value);
  }
  return out;
}

function pick(fields, names) {
  const out = {};
  for (const name of names) {
    if (!isBlank(fields?.[name])) out[name] = cleanValue(fields[name]);
  }
  return out;
}

function addUnique(map, key, value) {
  if (isBlank(key) || isBlank(value)) return;
  const normalizedKey = String(key);
  if (!map[normalizedKey]) map[normalizedKey] = [];
  if (!map[normalizedKey].includes(value)) map[normalizedKey].push(value);
}

function buildSourcePayload(input) {
  const generatedAt = input.generatedAt || new Date().toISOString();
  const scheduleRows = input.scheduleRows || [];
  const tripRows = input.tripRows || [];
  const scheduleLogRows = input.scheduleLogRows || [];
  const tripLogRows = input.tripLogRows || [];
  const heartbeatRows = input.heartbeatRows || [];
  const contractVersion = input.contractVersion || "2026-05-12.daily_schedule_app_source.v1";

  const latestScheduleLogByScheduleId = latestByLinkedRecord(scheduleLogRows, ["watch_schedule"]);
  const latestTripLogByTripId = latestByLinkedRecord(tripLogRows, ["watch_trips", "watch_trip_record_id"]);

  const scheduleByRecordId = new Map();
  const scheduleByKey = new Map();
  const lanes = {
    heartbeat: [],
    show: [],
    rings: [],
    groups: [],
    class_start: [],
    classes: [],
    entries: [],
    trip_go: [],
    trips: [],
    horses: [],
    riders: [],
  };

  const seenLaneIds = Object.fromEntries(Object.keys(lanes).map((key) => [key, new Set()]));
  function pushLane(laneName, id, object) {
    if (isBlank(id) || seenLaneIds[laneName].has(String(id))) return;
    seenLaneIds[laneName].add(String(id));
    lanes[laneName].push(compactObject({ id, ...object }));
  }

  for (const row of heartbeatRows) {
    const f = fieldsOf(row);
    const id = rowId(row) || firstValue(f.record_id, f.heartbeat_id);
    pushLane("heartbeat", id, {
      record_id: id,
      heartbeat_id: firstValue(f.heartbeat_id),
      hb_at: firstValue(f.hb_at),
      show_id: firstValue(f.show_id),
      app_show_id: firstValue(f.app_show_id),
      app_sql_date: firstValue(f.app_sql_date),
      app_dow_raw: firstValue(f.app_dow_raw),
      shifted_to_next_day: firstValue(f.shifted_to_next_day),
      mode: firstValue(f.mode),
      time: firstValue(f.time),
    });
  }

  for (const row of scheduleRows) {
    const f = fieldsOf(row);
    const id = rowId(row);
    const identity = scheduleIdentity(row);
    const latestLog = latestScheduleLogByScheduleId.get(id);
    const latestLogFields = fieldsOf(latestLog);

    scheduleByRecordId.set(id, { row, identity });
    if (!isBlank(identity.schedule_key)) {
      if (!scheduleByKey.has(identity.schedule_key)) scheduleByKey.set(identity.schedule_key, []);
      scheduleByKey.get(identity.schedule_key).push({ row, identity });
    }

    pushLane("show", `show:${identity.sid}:${identity.sql_date}`, {
      sid: identity.sid,
      sql_date: identity.sql_date,
      show_id: firstValue(f.show_id),
      app_show_idv2: firstValue(f.app_show_idv2),
      app_sql_datev2: firstValue(f.app_sql_datev2),
      app_dow_rawv2: firstValue(f.app_dow_rawv2),
    });
    pushLane("rings", `ring:${identity.sid}:${identity.sql_date}:${identity.ring_number}`, {
      schedule_key: identity.schedule_key,
      sid: identity.sid,
      sql_date: identity.sql_date,
      ring_number: identity.ring_number,
      ringName: firstValue(f.ringName),
      ring_nickname: firstValue(f.ring_nickname),
      status: firstValue(f.status),
      scope_status: firstValue(f.scope_status),
    });
    pushLane("groups", `group:${id}`, {
      schedule_key: identity.schedule_key,
      schedule_instance_key: identity.schedule_instance_key,
      schedule_short: identity.schedule_short,
      schedule_tie_breaker: identity.schedule_tie_breaker,
      schedule_tie_breaker_source: identity.schedule_tie_breaker_source,
      schedule_record_id: id,
      class_group_id: firstValue(f.class_group_id),
      class_group_sequence: firstValue(f.class_group_sequence),
      class_groupxclasses_id: firstValue(f.class_groupxclasses_id),
      group_name: firstValue(f.group_name),
      group_name_tags: firstValue(f.group_name_tags),
      total_trips: firstValue(f.total_trips),
      completed_trips: firstValue(f.completed_trips),
      status: firstValue(f.status),
    });
    pushLane("class_start", `class_start:${id}`, {
      schedule_record_id: id,
      schedule_key: identity.schedule_key,
      schedule_instance_key: identity.schedule_instance_key,
      schedule_tie_breaker: identity.schedule_tie_breaker,
      schedule_tie_breaker_source: identity.schedule_tie_breaker_source,
      estimated_start_time: firstValue(f.estimated_start_time),
      estimated_end_time: firstValue(f.estimated_end_time),
      manual_time_override: firstValue(f.manual_time_override),
      latest_schedule_log_record_id: rowId(latestLog),
      latest_schedule_log: pick(latestLogFields, [
        "created_at",
        "rs_start_time",
        "calc_status",
        "changed_fields",
      ]),
    });
    pushLane("classes", `class:${id}`, {
      schedule_record_id: id,
      schedule_key: identity.schedule_key,
      schedule_instance_key: identity.schedule_instance_key,
      schedule_tie_breaker: identity.schedule_tie_breaker,
      schedule_tie_breaker_source: identity.schedule_tie_breaker_source,
      class_id: firstValue(f.class_id),
      class_number: firstValue(f.class_number),
      class_sequence: identity.class_sequence,
      class_name: firstValue(f.class_name),
      class_type: firstValue(f.class_type),
      schedule_sequencetype: firstValue(f.schedule_sequencetype),
    });
  }

  const validation = {
    unresolved_trip_parents: [],
    duplicate_schedule_keys: [],
    duplicate_trips_keys: [],
    rs_mismatches: [],
  };

  const scheduleKeyCounts = {};
  for (const { identity } of scheduleByRecordId.values()) {
    if (identity.schedule_key) scheduleKeyCounts[identity.schedule_key] = (scheduleKeyCounts[identity.schedule_key] || 0) + 1;
  }
  validation.duplicate_schedule_keys = Object.entries(scheduleKeyCounts)
    .filter(([, count]) => count > 1)
    .map(([key, count]) => {
      const rows = scheduleByKey.get(key) || [];
      const tieBreakers = rows
        .map((item) => item.identity.schedule_tie_breaker)
        .filter((value) => !isBlank(value));
      const missingTieBreakerRecordIds = rows
        .filter((item) => item.identity.schedule_tie_breaker_source !== "entry_sequence")
        .map((item) => rowId(item.row));
      const fallbackTieBreakerRecordIds = rows
        .filter((item) => item.identity.schedule_tie_breaker_source !== "entry_sequence")
        .map((item) => rowId(item.row));
      const tieBreakerUnique = tieBreakers.length === count && new Set(tieBreakers.map(String)).size === count;
      return {
        key,
        count,
        tie_breaker_field: "entry_sequence",
        fallback_tie_breaker: "class_group_id, class_groupxclasses_id, then estimated_start_time + record_id",
        tie_breakers: tieBreakers,
        missing_tie_breaker_record_ids: missingTieBreakerRecordIds,
        fallback_tie_breaker_record_ids: fallbackTieBreakerRecordIds,
        tie_breaker_unique: tieBreakerUnique,
        severity: tieBreakerUnique ? "warning" : "error",
        workflow_blocking: false,
        resolution_strategy: "schedule_key groups rows; schedule_instance_key separates class-group instances",
      };
    });

  const tripsKeyCounts = {};
  const relevantScheduleLogIds = new Set();
  const relevantTripLogIds = new Set();
  const indexes = {
    by_schedule_key: {},
    by_trips_key: {},
    children_by_parent_key: {},
    by_airtable_record_id: {},
    logs_by_trip_key: {},
  };

  for (const row of scheduleRows) {
    const id = rowId(row);
    const identity = scheduleIdentity(row);
    indexes.by_airtable_record_id[id] = { lane: "class_start", schedule_key: identity.schedule_key, schedule_instance_key: identity.schedule_instance_key };
    addUnique(indexes.by_schedule_key, identity.schedule_key, id);
  }

  for (const row of tripRows) {
    const f = fieldsOf(row);
    const id = rowId(row);
    const identity = tripIdentity(row);
    const latestLog = latestTripLogByTripId.get(id);
    const latestLogFields = fieldsOf(latestLog);
    const linkedScheduleId = firstValue(f.schedule_rid, f.watch_schedule);
    const linkedSchedule = linkedScheduleId ? scheduleByRecordId.get(String(linkedScheduleId)) : null;
    const keySchedules = scheduleByKey.get(identity.schedule_key) || [];
    const tieBreakerSchedule = !isBlank(identity.schedule_tie_breaker)
      ? keySchedules.find((item) => String(item.identity.schedule_tie_breaker) === String(identity.schedule_tie_breaker))
      : null;
    const keySchedule = keySchedules.length === 1 ? keySchedules[0] : null;
    const parent = linkedSchedule || tieBreakerSchedule || keySchedule || null;

    if (!parent) {
      validation.unresolved_trip_parents.push({
        record_id: id,
        trips_key: identity.trips_key,
        schedule_key: identity.schedule_key,
        reason: "missing_schedule_parent",
      });
    }

    tripsKeyCounts[identity.trips_key] = (tripsKeyCounts[identity.trips_key] || 0) + 1;
    indexes.by_airtable_record_id[id] = { lane: "trips", trips_key: identity.trips_key, trip_instance_key: identity.trip_instance_key, schedule_key: identity.schedule_key };
    if (!isBlank(identity.trips_key)) indexes.by_trips_key[identity.trips_key] = id;
    addUnique(indexes.children_by_parent_key, identity.schedule_key, identity.trips_key);
    if (latestLog) {
      relevantTripLogIds.add(rowId(latestLog));
      addUnique(indexes.logs_by_trip_key, identity.trips_key, rowId(latestLog));
    }

    const rsCurrent = pickPrefixed(f, "rs_");
    const rsLatestLog = pickPrefixed(latestLogFields, "rs_");
    const rsDiff = pick(latestLogFields, ["rs_start_time_diff", "rs_go_time_diff", "rs_order_of_go_diff"]);
    for (const [key, value] of Object.entries(rsCurrent)) {
      if (key.endsWith("_diff")) continue;
      if (!isBlank(rsLatestLog[key]) && String(rsLatestLog[key]) !== String(value)) {
        validation.rs_mismatches.push({
          record_id: id,
          trips_key: identity.trips_key,
          field: key,
          watch_trips: value,
          trip_logs: rsLatestLog[key],
          latest_trip_log_record_id: rowId(latestLog),
        });
      }
    }

    pushLane("entries", `entry:${identity.trips_key}`, {
      trips_key: identity.trips_key,
      trip_instance_key: identity.trip_instance_key,
      trips_short_key: identity.trips_short_key,
      schedule_key: identity.schedule_key,
      pid: identity.pid,
      entry_number: firstValue(f.entry_number),
      entry_sequence: firstValue(f.entry_sequence),
      trip_tie_breaker: identity.trip_tie_breaker,
      trip_tie_breaker_source: identity.trip_tie_breaker_source,
      entry_id: firstValue(f.entry_id),
      h_eid: firstValue(f.h_eid),
    });
    pushLane("trip_go", `trip_go:${identity.trips_key}`, {
      trip_record_id: id,
      schedule_record_id: parent ? rowId(parent.row) : null,
      trips_key: identity.trips_key,
      trip_instance_key: identity.trip_instance_key,
      schedule_key: identity.schedule_key,
      full_nesting_key: identity.full_nesting_key,
      pid: identity.pid,
      entry_number: identity.entry_number,
      entry_sequence: firstValue(f.entry_sequence),
      trip_tie_breaker: identity.trip_tie_breaker,
      trip_tie_breaker_source: identity.trip_tie_breaker_source,
      estimated_start_time: firstValue(f.estimated_start_time),
      estimated_go_time: firstValue(f.estimated_go_time),
      actual_order: firstValue(f.actual_order),
      actual_time: firstValue(f.actual_time),
      actual_go: firstValue(f.actual_go),
      gone_in: firstValue(f.gone_in),
      getLiveClassData: firstValue(f.getLiveClassData),
      rs_current: rsCurrent,
      latest_trip_log_record_id: rowId(latestLog),
      rs_latest_log: rsLatestLog,
      rs_diff: rsDiff,
    });
    pushLane("trips", `trip:${identity.trips_key}`, {
      trip_record_id: id,
      schedule_record_id: parent ? rowId(parent.row) : null,
      trips_key: identity.trips_key,
      trip_instance_key: identity.trip_instance_key,
      schedule_key: identity.schedule_key,
      pid: identity.pid,
      entry_number: identity.entry_number,
      entry_sequence: firstValue(f.entry_sequence),
      trip_tie_breaker: identity.trip_tie_breaker,
      trip_tie_breaker_source: identity.trip_tie_breaker_source,
      status: firstValue(f.status),
      completed_trips: firstValue(f.completed_trips),
      total_trips: firstValue(f.total_trips),
      last_score: firstValue(f.last_score),
      is_target: firstValue(f.is_target),
      manual_time_override: firstValue(f.manual_time_override),
      last_seen_at: firstValue(f.last_seen_at),
      latest_ingested_at: firstValue(f.latest_ingested_at),
    });
    pushLane("horses", `horse:${firstValue(f.horse, f.horse_name, f.h_eid) || identity.trips_key}`, {
      trips_key: identity.trips_key,
      horse: firstValue(f.horse),
      horse_name: firstValue(f.horse_name),
      h_eid: firstValue(f.h_eid),
    });
    pushLane("riders", `rider:${firstValue(f.pid, f.rider_name) || identity.trips_key}`, {
      trips_key: identity.trips_key,
      pid: firstValue(f.pid),
      rider_name: firstValue(f.rider_name),
    });
  }

  validation.duplicate_trips_keys = Object.entries(tripsKeyCounts)
    .filter(([key, count]) => key && count > 1)
    .map(([key, count]) => {
      const rows = tripRows
        .map((row) => ({ row, identity: tripIdentity(row) }))
        .filter((item) => item.identity.trips_key === key);
      const tieBreakers = rows
        .map((item) => item.identity.trip_tie_breaker)
        .filter((value) => !isBlank(value));
      const missingTieBreakerRecordIds = rows
        .filter((item) => isBlank(item.identity.trip_tie_breaker))
        .map((item) => rowId(item.row));
      return {
        key,
        count,
        tie_breaker_field: "entry_sequence",
        fallback_tie_breaker: "h_eid, entry_id, then record_id",
        tie_breakers: tieBreakers,
        missing_tie_breaker_record_ids: missingTieBreakerRecordIds,
        tie_breaker_unique: tieBreakers.length === count && new Set(tieBreakers.map(String)).size === count,
        severity: tieBreakers.length === count && new Set(tieBreakers.map(String)).size === count ? "warning" : "error",
        workflow_blocking: false,
        resolution_strategy: "trips_key identifies trip rows; trip_instance_key separates imperfect row instances",
      };
    });

  for (const row of scheduleRows) {
    const latestLog = latestScheduleLogByScheduleId.get(rowId(row));
    if (latestLog) relevantScheduleLogIds.add(rowId(latestLog));
  }

  const sideLanes = {
    results: [],
    alerts: [],
    logs: {
      schedule_logs: scheduleLogRows.filter((row) => relevantScheduleLogIds.has(rowId(row))).map((row) => compactObject({
        record_id: rowId(row),
        source_table: "schedule_logs",
        ...pick(fieldsOf(row), ["watch_schedule", "created_at", "rs_start_time", "calc_status", "changed_fields", "inputs_json", "computed_outputs_json", "anomalies_json"]),
      })),
      trip_logs: tripLogRows.filter((row) => relevantTripLogIds.has(rowId(row))).map((row) => compactObject({
        record_id: rowId(row),
        source_table: "trip_logs",
        ...pick(fieldsOf(row), [
          "watch_trips",
          "watch_trip_record_id",
          "watch_schedule",
          "watch_schedule_record_id",
          "created_at",
          "calc_mode",
          "calc_version",
          "calc_status",
          "skip_reason",
          "changed_fields",
          "inputs_json",
          "prior_outputs_json",
          "computed_outputs_json",
          "anomalies_json",
          "rs_start_time_diff",
          "rs_go_time_diff",
          "rs_order_of_go_diff",
        ]),
        rs_log: pickPrefixed(fieldsOf(row), "rs_"),
      })),
    },
  };

  return {
    meta: {
      generated_at: generatedAt,
      field_contract_version: contractVersion,
      row_counts: {
        heartbeat: heartbeatRows.length,
        watch_schedule: scheduleRows.length,
        watch_trips: tripRows.length,
        schedule_logs: scheduleLogRows.length,
        trip_logs: tripLogRows.length,
      },
      note: "Flat source payload only; primary app nesting is intentionally not built here.",
    },
    lane_order: [
      "heartbeat",
      "show",
      "rings",
      "groups",
      "class_start",
      "classes",
      "entries",
      "trip_go",
      "trips",
      "horses",
      "riders",
    ],
    lanes,
    side_lanes: sideLanes,
    indexes,
    reports: { validation },
  };
}

function loadJson(filePath) {
  return JSON.parse(fs.readFileSync(filePath, "utf8"));
}

function writeJson(filePath, value) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  fs.writeFileSync(filePath, JSON.stringify(value, null, 2) + "\n", "utf8");
}

function airtableHeaders() {
  if (!process.env.AIRTABLE_TOKEN) throw new Error("Missing env AIRTABLE_TOKEN");
  return { Authorization: `Bearer ${process.env.AIRTABLE_TOKEN}` };
}

function airtableBaseUrl(tableName) {
  if (!process.env.AIRTABLE_BASE_ID) throw new Error("Missing env AIRTABLE_BASE_ID");
  return `https://api.airtable.com/v0/${process.env.AIRTABLE_BASE_ID}/${encodeURIComponent(tableName)}`;
}

async function fetchJson(url, options = {}) {
  const response = await fetch(url, options);
  const json = await response.json().catch(() => ({}));
  if (!response.ok) {
    const err = new Error(json?.error?.message || `Airtable request failed ${response.status}`);
    err.status = response.status;
    err.type = json?.error?.type;
    throw err;
  }
  return json;
}

function appendQuery(url, key, value) {
  if (Array.isArray(value)) {
    for (const item of value) appendQuery(url, key, item);
  } else if (!isBlank(value)) {
    url.searchParams.append(key, value);
  }
}

async function airtableList(tableConfig, fallbackWithoutFields = true) {
  const table = process.env[`TABLE_${String(tableConfig.table).toUpperCase()}`] || tableConfig.table;
  const records = [];
  let offset = null;
  let useFields = Array.isArray(tableConfig.fields) && tableConfig.fields.length > 0;
  let useSort = Array.isArray(tableConfig.sort) && tableConfig.sort.length > 0;

  while (true) {
    const url = new URL(airtableBaseUrl(table));
    if (!isBlank(tableConfig.view)) url.searchParams.set("view", tableConfig.view);
    if (tableConfig.max_records) url.searchParams.set("maxRecords", String(tableConfig.max_records));
    if (useFields) appendQuery(url, "fields[]", tableConfig.fields);
    if (useSort) {
      for (const [index, sort] of (tableConfig.sort || []).entries()) {
        if (sort.field) url.searchParams.set(`sort[${index}][field]`, sort.field);
        if (sort.direction) url.searchParams.set(`sort[${index}][direction]`, sort.direction);
      }
    }
    if (offset) url.searchParams.set("offset", offset);

    try {
      const json = await fetchJson(url.toString(), { headers: airtableHeaders() });
      records.push(...(json.records || []));
      offset = json.offset || null;
      if (!offset) break;
    } catch (err) {
      if (fallbackWithoutFields && useFields && err.status === 422 && String(err.type || "").toUpperCase() === "UNKNOWN_FIELD_NAME") {
        console.log(`warn: ${table} unknown field in fields[]; retrying without fields[]`);
        records.length = 0;
        offset = null;
        useFields = false;
        continue;
      }
      if (useSort && err.status === 422 && String(err.type || "").toUpperCase() === "UNKNOWN_FIELD_NAME") {
        console.log(`warn: ${table} unknown sort field; retrying without sort`);
        records.length = 0;
        offset = null;
        useSort = false;
        continue;
      }
      throw err;
    }
  }

  return records;
}

async function fetchAllSources(contract) {
  const tables = contract.tables || {};
  const [heartbeatRows, scheduleRows, tripRows, scheduleLogRows, tripLogRows] = await Promise.all([
    airtableList(tables.heartbeat || { table: "heartbeat", max_records: 1 }),
    airtableList(tables.watch_schedule),
    airtableList(tables.watch_trips),
    airtableList(tables.schedule_logs),
    airtableList(tables.trip_logs),
  ]);

  return { heartbeatRows, scheduleRows, tripRows, scheduleLogRows, tripLogRows };
}

function parseArgs(argv) {
  const args = {
    contract: DEFAULT_CONTRACT_PATH,
    out: path.join(ROOT, "samples", "latest_daily_schedule_app_source.json"),
    report: path.join(ROOT, "reports", "latest_validation_report.json"),
    fixture: null,
  };
  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    if (arg === "--contract") args.contract = path.resolve(argv[++i]);
    else if (arg === "--out") args.out = path.resolve(argv[++i]);
    else if (arg === "--report") args.report = path.resolve(argv[++i]);
    else if (arg === "--fixture") args.fixture = path.resolve(argv[++i]);
    else if (arg === "--help") args.help = true;
    else throw new Error(`Unknown argument: ${arg}`);
  }
  return args;
}

function printHelp() {
  console.log(`Usage:
  node daily_schedule_app_source/extract_daily_schedule_source.js [options]

Options:
  --contract <path>  Field contract JSON. Default: daily_schedule_app_source/field_contract.json
  --out <path>       Output sample JSON path. Default: daily_schedule_app_source/samples/latest_daily_schedule_app_source.json
  --report <path>    Validation report path. Default: daily_schedule_app_source/reports/latest_validation_report.json
  --fixture <path>   Read local fixture JSON instead of Airtable.

Required for Airtable mode:
  AIRTABLE_TOKEN
  AIRTABLE_BASE_ID
`);
}

async function main(argv = process.argv.slice(2)) {
  const args = parseArgs(argv);
  if (args.help) {
    printHelp();
    return;
  }

  const contract = loadJson(args.contract);
  const sources = args.fixture ? loadJson(args.fixture) : await fetchAllSources(contract);
  const payload = buildSourcePayload({
    ...sources,
    contractVersion: contract.contract_version,
  });
  const report = {
    generated_at: payload.meta.generated_at,
    field_contract_version: payload.meta.field_contract_version,
    row_counts: payload.meta.row_counts,
    validation: payload.reports.validation,
  };

  writeJson(args.out, payload);
  writeJson(args.report, report);
  console.log(JSON.stringify({
    ok: true,
    out: args.out,
    report: args.report,
    row_counts: payload.meta.row_counts,
    unresolved_trip_parents: report.validation.unresolved_trip_parents.length,
    rs_mismatches: report.validation.rs_mismatches.length,
  }, null, 2));
}

if (require.main === module) {
  main().catch((err) => {
    console.error(err && err.stack ? err.stack : String(err));
    process.exitCode = 1;
  });
}

module.exports = {
  buildSourcePayload,
  makeScheduleKey,
  makeScheduleShort,
  makeTripsKey,
  makeTripsShortKey,
  makeFullNestingKey,
  scheduleIdentity,
  tripIdentity,
};
