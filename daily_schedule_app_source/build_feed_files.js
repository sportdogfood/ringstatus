#!/usr/bin/env node
"use strict";

const fs = require("node:fs");
const path = require("node:path");

const LOCAL_ROOT = __dirname;
const DEFAULT_SOURCE = path.join(LOCAL_ROOT, "samples", "latest_daily_schedule_app_source.json");
const DEFAULT_OUT_DIR = path.join(LOCAL_ROOT, "feed");

function isBlank(value) {
  if (value === undefined || value === null) return true;
  if (Array.isArray(value)) return value.length === 0;
  if (typeof value === "object") return false;
  return String(value).trim() === "";
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

function compactObject(input) {
  const out = {};
  for (const [key, value] of Object.entries(input || {})) {
    if (isBlank(value)) continue;
    if (Array.isArray(value) && value.length === 0) continue;
    if (value && typeof value === "object" && !Array.isArray(value) && Object.keys(value).length === 0) continue;
    out[key] = value;
  }
  return out;
}

function byFirst(...fieldNames) {
  return (row) => firstValue(...fieldNames.map((fieldName) => row?.[fieldName]));
}

function keyBy(rows, getter) {
  const map = new Map();
  for (const row of rows || []) {
    const key = getter(row);
    if (!isBlank(key) && !map.has(String(key))) map.set(String(key), row);
  }
  return map;
}

function groupBy(rows, getter) {
  const map = new Map();
  for (const row of rows || []) {
    const key = getter(row);
    if (isBlank(key)) continue;
    const text = String(key);
    if (!map.has(text)) map.set(text, []);
    map.get(text).push(row);
  }
  return map;
}

function addIndex(index, key, rowId) {
  if (isBlank(key) || isBlank(rowId)) return;
  const text = String(key).trim();
  if (!index[text]) index[text] = [];
  if (!index[text].includes(rowId)) index[text].push(rowId);
}

function splitTags(value) {
  if (Array.isArray(value)) return value.map((item) => String(item).trim()).filter(Boolean);
  if (isBlank(value)) return [];
  return String(value).split(",").map((item) => item.trim()).filter(Boolean);
}

function timeSort(value) {
  const text = String(value || "");
  const match = text.match(/^(\d{1,2}):(\d{2})(?::(\d{2}))?/);
  if (!match) return 999999;
  return Number(match[1]) * 3600 + Number(match[2]) * 60 + Number(match[3] || 0);
}

function sourcedValue(value, source) {
  return isBlank(value) ? null : { value, source };
}

function sourcedTime(value, source) {
  return isBlank(value) ? null : { time: value, source };
}

function calculatorNumber(scheduleLog, current, latestLog, name) {
  const scheduleValue = scheduleLog?.[name];
  if (!isBlank(scheduleValue)) return sourcedValue(scheduleValue, `schedule_logs.latest_schedule_log.${name}`);
  const currentValue = current?.[name];
  if (!isBlank(currentValue)) return sourcedValue(currentValue, `watch_trips.rs_current.${name}`);
  const latestValue = latestLog?.[name];
  if (!isBlank(latestValue)) return sourcedValue(latestValue, `trip_logs.rs_latest_log.${name}`);
  return null;
}

function calculatorTime(scheduleLog, current, latestLog, name, fallbackValue, fallbackSource) {
  const scheduleValue = scheduleLog?.[name];
  if (!isBlank(scheduleValue)) return sourcedTime(scheduleValue, `schedule_logs.latest_schedule_log.${name}`);
  const currentValue = current?.[name];
  if (!isBlank(currentValue)) return sourcedTime(currentValue, `watch_trips.rs_current.${name}`);
  const latestValue = latestLog?.[name];
  if (!isBlank(latestValue)) return sourcedTime(latestValue, `trip_logs.rs_latest_log.${name}`);
  if (!isBlank(fallbackValue)) return sourcedTime(fallbackValue, fallbackSource);
  return null;
}

function buildLookups(payload) {
  const lanes = payload?.lanes || {};
  const classes = lanes.classes || [];
  const starts = lanes.class_start || [];
  const groups = lanes.groups || [];
  const rings = lanes.rings || [];
  const entries = lanes.entries || [];
  const tripGos = lanes.trip_go || [];
  const trips = lanes.trips || [];
  const horses = lanes.horses || [];
  const riders = lanes.riders || [];

  const classByRecord = keyBy(classes, byFirst("schedule_record_id"));
  const startByRecord = keyBy(starts, byFirst("schedule_record_id"));
  const groupByRecord = keyBy(groups, byFirst("schedule_record_id"));
  const ringByNumber = keyBy(rings, byFirst("ring_number"));
  const entryByTrip = keyBy(entries, byFirst("trip_instance_key", "trips_key"));
  const horseByTrip = keyBy(horses, byFirst("trips_key"));
  const riderByTrip = keyBy(riders, byFirst("trips_key"));
  const riderByPid = keyBy(riders, byFirst("pid"));
  const tripGoByTrip = keyBy(tripGos, byFirst("trip_instance_key", "trips_key"));
  const tripsByScheduleRecord = groupBy(trips, byFirst("schedule_record_id"));
  const tripGosByScheduleRecord = groupBy(tripGos, byFirst("schedule_record_id"));
  const tripGosByScheduleKey = groupBy(tripGos, byFirst("schedule_key"));

  return {
    lanes,
    classByRecord,
    startByRecord,
    groupByRecord,
    ringByNumber,
    entryByTrip,
    horseByTrip,
    riderByTrip,
    riderByPid,
    tripGoByTrip,
    tripsByScheduleRecord,
    tripGosByScheduleRecord,
    tripGosByScheduleKey,
  };
}

function buildScheduleRows(payload) {
  const lookups = buildLookups(payload);
  const starts = lookups.lanes.class_start || [];

  return starts.map((start, sourceIndex) => {
    const scheduleRecordId = firstValue(start.schedule_record_id);
    const cls = lookups.classByRecord.get(String(scheduleRecordId)) || {};
    const group = lookups.groupByRecord.get(String(scheduleRecordId)) || {};
    const scheduleKey = firstValue(start.schedule_key, cls.schedule_key, group.schedule_key);
    const scheduleInstanceKey = firstValue(start.schedule_instance_key, cls.schedule_instance_key, group.schedule_instance_key, scheduleKey);
    const ringNumber = firstValue(group.ring_number, cls.ring_number, scheduleKey ? String(scheduleKey).split("|")[2] : null);
    const ring = lookups.ringByNumber.get(String(ringNumber)) || {};
    const rowTripGos = [
      ...(lookups.tripGosByScheduleRecord.get(String(scheduleRecordId)) || []),
      ...(!lookups.tripGosByScheduleRecord.has(String(scheduleRecordId)) ? (lookups.tripGosByScheduleKey.get(String(scheduleKey)) || []) : []),
    ];
    const trips = rowTripGos.map((go) => {
      const tripId = firstValue(go.trip_instance_key, go.trips_key);
      const entry = lookups.entryByTrip.get(String(tripId)) || {};
      const horse = lookups.horseByTrip.get(String(go.trips_key)) || {};
      const rider = lookups.riderByTrip.get(String(go.trips_key)) || lookups.riderByPid.get(String(firstValue(go.pid, entry.pid))) || {};
      return compactObject({
        trip_record_id: firstValue(go.trip_record_id),
        trips_key: firstValue(go.trips_key),
        trip_instance_key: firstValue(go.trip_instance_key),
        pid: firstValue(go.pid, entry.pid),
        entry_number: firstValue(entry.entry_number, go.entry_number),
        entry_sequence: firstValue(entry.entry_sequence, go.entry_sequence),
        rider_name: firstValue(rider.rider_name),
        horse: firstValue(horse.horse_name, horse.horse),
        estimated_go_time: firstValue(go.estimated_go_time),
        go: calculatorTime(null, go.rs_current, go.rs_latest_log, "rs_go_time", go.estimated_go_time, "watch_trips.estimated_go_time"),
        go_starts_in_mins: calculatorNumber(null, go.rs_current, go.rs_latest_log, "rs_min_till_go"),
        order_of_go: calculatorNumber(null, go.rs_current, go.rs_latest_log, "rs_order_of_go"),
        latest_trip_log_record_id: firstValue(go.latest_trip_log_record_id),
      });
    }).sort((a, b) => timeSort(a.estimated_go_time) - timeSort(b.estimated_go_time) || Number(a.entry_sequence || 0) - Number(b.entry_sequence || 0));

    const scheduleLog = start.latest_schedule_log || {};
    const completedTrips = firstValue(scheduleLog.rs_completed_trips, group.completed_trips);
    const totalTrips = firstValue(scheduleLog.rs_total_trips, group.total_trips);

    return compactObject({
      id: scheduleInstanceKey,
      source_index: sourceIndex,
      schedule_record_id: scheduleRecordId,
      schedule_key: scheduleKey,
      schedule_instance_key: scheduleInstanceKey,
      ring_number: ringNumber,
      ring_name: firstValue(ring.ringName, ring.ring_nickname, group.ringName, group.ring_nickname, `Ring ${ringNumber}`),
      group_name: firstValue(group.group_name),
      group_name_tags: firstValue(group.group_name_tags),
      class_number: firstValue(cls.class_number),
      class_sequence: firstValue(cls.class_sequence),
      class_name: firstValue(cls.class_name, group.group_name),
      class_type: firstValue(cls.class_type),
      sequence_type: firstValue(cls.schedule_sequencetype),
      status: firstValue(scheduleLog.rs_status, group.status),
      start: calculatorTime(scheduleLog, null, null, "rs_start_time", start.estimated_start_time, "watch_schedule.estimated_start_time"),
      end: calculatorTime(scheduleLog, null, null, "rs_end_time", start.estimated_end_time, "watch_schedule.estimated_end_time"),
      starts_in_mins: calculatorNumber(scheduleLog, null, null, "rs_mins_till_start"),
      ends_in_mins: null,
      progress: compactObject({
        completed_trips: completedTrips,
        total_trips: totalTrips,
        left_trips: !isBlank(completedTrips) && !isBlank(totalTrips) ? Math.max(Number(totalTrips) - Number(completedTrips), 0) : null,
        source: !isBlank(scheduleLog.rs_completed_trips) || !isBlank(scheduleLog.rs_total_trips) ? "schedule_logs.latest_schedule_log" : "watch_schedule/groups",
      }),
      latest_schedule_log_record_id: firstValue(start.latest_schedule_log_record_id),
      trips,
    });
  }).sort((a, b) => {
    const ringCompare = Number(a.ring_number || 0) - Number(b.ring_number || 0);
    if (ringCompare) return ringCompare;
    const timeCompare = timeSort(a.start?.time) - timeSort(b.start?.time);
    if (timeCompare) return timeCompare;
    return Number(a.source_index || 0) - Number(b.source_index || 0);
  });
}

function buildRawFeed(payload, options = {}) {
  const generatedAt = options.generatedAt || new Date().toISOString();
  return {
    ...payload,
    meta: {
      ...(payload?.meta || {}),
      feed_generated_at: generatedAt,
      feed_shape: "raw_flat_lanes",
    },
  };
}

function buildIndexedFeed(payload, options = {}) {
  const generatedAt = options.generatedAt || new Date().toISOString();
  const rows = buildScheduleRows(payload);
  const indexed = {
    rider: {},
    horse: {},
    status: {},
    ring: {},
    class_type: {},
    group_name_tags: {},
  };

  for (const row of rows) {
    addIndex(indexed.status, row.status, row.id);
    addIndex(indexed.ring, row.ring_number, row.id);
    addIndex(indexed.class_type, row.class_type, row.id);
    for (const tag of splitTags(row.group_name_tags)) addIndex(indexed.group_name_tags, tag, row.id);
    for (const trip of row.trips || []) {
      addIndex(indexed.rider, trip.rider_name, row.id);
      addIndex(indexed.horse, trip.horse, row.id);
    }
  }

  return {
    meta: {
      generated_at: generatedAt,
      source_generated_at: payload?.meta?.generated_at || null,
      shape: "indexed_schedule_rows",
      note: "Flat rows with indexes for lookup. This is not the final nested app dataset.",
    },
    rows,
    indexed,
  };
}

function pickNowNextFollowing(rows) {
  const activeIndex = rows.findIndex((row) => {
    const status = String(row.status || "").toLowerCase();
    if (status.includes("complete")) return false;
    const completed = row.progress?.completed_trips;
    const total = row.progress?.total_trips;
    if (status.includes("current") || status.includes("underway") || status.includes("running")) return true;
    if (!isBlank(total) && Number(total) > 0 && Number(completed || 0) < Number(total)) return true;
    if (!isBlank(status)) return true;
    return false;
  });
  const index = activeIndex >= 0 ? activeIndex : Math.max(rows.length - 1, 0);
  return {
    now: rows[index] || null,
    next: rows[index + 1] || null,
    following: rows[index + 2] || null,
    previous: rows[index - 1] || null,
  };
}

function uniqueRefsByRow(refs) {
  const seen = new Set();
  const out = [];
  for (const ref of refs || []) {
    const key = ref?.row?.id;
    if (isBlank(key) || seen.has(String(key))) continue;
    seen.add(String(key));
    out.push(ref);
  }
  return out;
}

function previousRowByRing(rows) {
  const byRing = groupBy(rows, byFirst("ring_number"));
  const previous = new Map();
  for (const ringRows of byRing.values()) {
    for (let i = 0; i < ringRows.length; i += 1) {
      previous.set(ringRows[i].id, ringRows[i - 1] || null);
    }
  }
  return previous;
}

function statusClass(row, previous) {
  if (!row) return null;
  const out = compactObject({
    schedule_instance_key: row.schedule_instance_key,
    schedule_key: row.schedule_key,
    ring_number: row.ring_number,
    ring_name: row.ring_name,
    group_name: row.group_name,
    class_number: row.class_number,
    class_name: row.class_name,
    class_type: row.class_type,
    status: row.status,
    start: row.start,
    end: row.end,
    starts_in_mins: row.starts_in_mins,
    ends_in_mins: row.ends_in_mins,
    progress: row.progress,
  });
  out.previous_class = previous ? compactObject({
    schedule_instance_key: previous.schedule_instance_key,
    class_number: previous.class_number,
    class_name: previous.class_name,
    status: previous.status,
    progress: previous.progress,
  }) : null;
  return out;
}

function statusTrip(row, trip, previous) {
  const base = statusClass(row, previous);
  if (!base) return null;
  return {
    ...base,
    trip: compactObject({
      trips_key: trip?.trips_key,
      trip_instance_key: trip?.trip_instance_key,
      pid: trip?.pid,
      entry_number: trip?.entry_number,
      rider_name: trip?.rider_name,
      horse: trip?.horse,
      go: trip?.go,
      go_starts_in_mins: trip?.go_starts_in_mins,
      order_of_go: trip?.order_of_go,
    }),
  };
}

function buildStatusFeed(payload, options = {}) {
  const generatedAt = options.generatedAt || new Date().toISOString();
  const rows = buildScheduleRows(payload);
  const previousByRowId = previousRowByRing(rows);
  const rowsByRing = groupBy(rows, byFirst("ring_number"));
  const tripRefs = [];
  for (const row of rows) {
    for (const trip of row.trips || []) tripRefs.push({ row, trip });
  }

  const byRing = {};
  for (const [ringNumber, ringRows] of rowsByRing.entries()) {
    const picked = pickNowNextFollowing(ringRows);
    byRing[ringNumber] = {
      as_of: firstValue(payload?.lanes?.heartbeat?.[0]?.time, payload?.meta?.generated_at),
      ring_number: ringNumber,
      ring_name: firstValue(picked.now?.ring_name, ringRows[0]?.ring_name),
      now: statusClass(picked.now, picked.previous),
      next: statusClass(picked.next, picked.now),
      following: statusClass(picked.following, picked.next),
    };
  }

  const byRider = {};
  const byHorse = {};
  const riderTrips = groupBy(tripRefs, (ref) => ref.trip.rider_name);
  const horseTrips = groupBy(tripRefs, (ref) => ref.trip.horse);

  for (const [riderName, refs] of riderTrips.entries()) {
    const uniqueRefs = uniqueRefsByRow(refs);
    const picked = pickNowNextFollowing(uniqueRefs.map((ref) => ref.row));
    const nowRef = refs.find((ref) => ref.row.id === picked.now?.id) || refs[0];
    byRider[riderName] = {
      as_of: firstValue(payload?.lanes?.heartbeat?.[0]?.time, payload?.meta?.generated_at),
      rider_name: riderName,
      now: statusTrip(nowRef?.row, nowRef?.trip, previousByRowId.get(nowRef?.row?.id) || null),
      next: statusClass(picked.next, picked.now),
      following: statusClass(picked.following, picked.next),
    };
  }

  for (const [horseName, refs] of horseTrips.entries()) {
    const uniqueRefs = uniqueRefsByRow(refs);
    const picked = pickNowNextFollowing(uniqueRefs.map((ref) => ref.row));
    const nowRef = refs.find((ref) => ref.row.id === picked.now?.id) || refs[0];
    byHorse[horseName] = {
      as_of: firstValue(payload?.lanes?.heartbeat?.[0]?.time, payload?.meta?.generated_at),
      horse: horseName,
      now: statusTrip(nowRef?.row, nowRef?.trip, previousByRowId.get(nowRef?.row?.id) || null),
      next: statusClass(picked.next, picked.now),
      following: statusClass(picked.following, picked.next),
    };
  }

  return {
    meta: {
      generated_at: generatedAt,
      source_generated_at: payload?.meta?.generated_at || null,
      shape: "derived_status_from_calculator_outputs",
    },
    time_policy: {
      show_timezone: process.env.SHOW_TZ || "America/New_York",
      show_time_zone_offset_mins: Number(process.env.SHOW_TIME_ZONE_OFFSET || "-240"),
      convert_show_clock_strings: false,
      note: "Clock strings from show/Airtable source rows are preserved. Relative minute fields come from calculator rs_* outputs when present.",
    },
    alert_defaults: {
      ring_walk_lead_mins: Number(process.env.RING_WALK_LEAD_MINS || "0"),
      ring_walk_offset_mins: Number(process.env.RING_WALK_OFFSET_MINS || "0"),
      ring_walk_total_mins: Number(process.env.RING_WALK_LEAD_MINS || "0") + Number(process.env.RING_WALK_OFFSET_MINS || "0"),
    },
    by_ring: byRing,
    by_rider: byRider,
    by_horse: byHorse,
  };
}

function parseArgs(argv) {
  const args = {
    source: DEFAULT_SOURCE,
    outDir: DEFAULT_OUT_DIR,
  };
  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    if (arg === "--source") args.source = path.resolve(argv[++i]);
    else if (arg === "--out-dir") args.outDir = path.resolve(argv[++i]);
    else if (arg === "--help") args.help = true;
    else throw new Error(`Unknown argument: ${arg}`);
  }
  return args;
}

function writeJson(filePath, value) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  fs.writeFileSync(filePath, JSON.stringify(value, null, 2) + "\n", "utf8");
}

function main(argv = process.argv.slice(2)) {
  const args = parseArgs(argv);
  if (args.help) {
    console.log("Usage: node daily_schedule_app_source/build_feed_files.js [--source <path>] [--out-dir <path>]");
    return;
  }

  const payload = JSON.parse(fs.readFileSync(args.source, "utf8"));
  const generatedAt = new Date().toISOString();
  const raw = buildRawFeed(payload, { generatedAt });
  const indexed = buildIndexedFeed(payload, { generatedAt });
  const status = buildStatusFeed(payload, { generatedAt });

  const out = {
    raw: path.join(args.outDir, "feed.raw.json"),
    indexed: path.join(args.outDir, "feed.indexed.json"),
    status: path.join(args.outDir, "feed.status.json"),
  };
  writeJson(out.raw, raw);
  writeJson(out.indexed, indexed);
  writeJson(out.status, status);

  console.log(JSON.stringify({
    ok: true,
    out,
    rows: indexed.rows.length,
    rings: Object.keys(status.by_ring).length,
    riders: Object.keys(status.by_rider).length,
    horses: Object.keys(status.by_horse).length,
  }, null, 2));
}

if (require.main === module) {
  try {
    main();
  } catch (err) {
    console.error(err && err.stack ? err.stack : String(err));
    process.exitCode = 1;
  }
}

module.exports = {
  buildRawFeed,
  buildIndexedFeed,
  buildStatusFeed,
  buildScheduleRows,
};
