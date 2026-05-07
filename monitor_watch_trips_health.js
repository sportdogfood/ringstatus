const AIRTABLE_TOKEN = process.env.AIRTABLE_TOKEN || "";
const AIRTABLE_BASE_ID = process.env.AIRTABLE_BASE_ID || "";

const TABLE_HEARTBEAT = process.env.TABLE_HEARTBEAT || "heartbeat";
const TABLE_WATCH_SCHEDULE = process.env.TABLE_WATCH_SCHEDULE || "watch_schedule";
const TABLE_GROUPS_LIVE = process.env.TABLE_GROUPS_LIVE || "groups_live";
const TABLE_WATCH_TRIPS = process.env.TABLE_WATCH_TRIPS || "watch_trips";
const VIEW_HEARTBEAT = process.env.WATCH_TRIPS_HEALTH_VIEW || process.env.WATCH_VIEW || "heartbeat";

const HEARTBEAT_CREATED_FIELD = process.env.HEARTBEAT_CREATED_FIELD || "created_time";
const HEARTBEAT_MODE_FIELD = process.env.HEARTBEAT_MODE_FIELD || process.env.FIELD_MODE || "mode";
const HEARTBEAT_CADENCE_FIELD = process.env.HEARTBEAT_CADENCE_FIELD || process.env.FIELD_CADENCE || "cadence";
const HEARTBEAT_SET_INTERVALS_FIELD = process.env.HEARTBEAT_SET_INTERVALS_FIELD || process.env.FIELD_SET_INTERVALS || "set_intervals";
const HEARTBEAT_INTERVAL_FIELD = process.env.HEARTBEAT_INTERVAL_FIELD || process.env.FIELD_INTERVAL || "interval";
const HTTP_TIMEOUT_MS = Number(process.env.HTTP_TIMEOUT_MS || "20000");
const DEFAULT_INTERVAL_SECONDS = Number(process.env.MONITOR_INTERVAL_SECONDS || "60");
const DEFAULT_CYCLES = Number(process.env.MONITOR_CYCLES || "20");

if (!AIRTABLE_TOKEN) throw new Error("Missing AIRTABLE_TOKEN");
if (!AIRTABLE_BASE_ID) throw new Error("Missing AIRTABLE_BASE_ID");

const args = new Set(process.argv.slice(2));
const once = args.has("--once") || String(process.env.MONITOR_ONCE || "0") === "1";
const intervalSeconds = once ? 0 : Math.max(1, DEFAULT_INTERVAL_SECONDS);
const cycles = once ? 1 : Math.max(1, DEFAULT_CYCLES);

const WATCH_SCHEDULE_FIELDS = [
  "estimated_start_time",
  "total_trips",
  "completed_trips",
  "status",
];

const GROUPS_LIVE_FIELDS = [
  "estimated_start_time",
  "total",
  "gone",
  "status",
];

const WATCH_TRIPS_RAW_FIELDS = [
  "entry_id",
  "order_of_go",
  "estimated_start_time",
  "total_trips",
  "completed_trips",
  "status",
  "gone_in",
  "h_eid",
  "entry_number",
  "estimated_go_time",
  "actual_time",
];

const WATCH_TRIPS_RS_FIELDS = [
  "rs_order_of_go",
  "rs_start_time",
  "rs_status",
  "rs_completed_trips",
  "rs_go_time",
  "rs_running_order_of_go",
  "rs_gone_in",
  "rs_end_time",
  "rs_trip_default",
  "rs_trip_time",
  "rs_trip_time2",
  "rs_go_mins_from_start",
  "rs_go_time_from_start",
  "rs_length",
  "rs_min_till_go",
  "rs_min_to_actual_go",
  "rs_mins_since_start",
  "rs_mins_till_start",
];

function isBlank(value) {
  return value === null ||
    value === undefined ||
    (typeof value === "string" && value.trim() === "") ||
    (Array.isArray(value) && value.length === 0);
}

function firstValue(value) {
  return Array.isArray(value) ? value[0] : value;
}

function strOrNull(value) {
  const first = firstValue(value);
  if (isBlank(first)) return null;
  return String(first).trim();
}

function numOrNull(value) {
  const text = strOrNull(value);
  if (text === null) return null;
  const num = Number(text);
  return Number.isFinite(num) ? num : null;
}

function toIsoDateOnly(value) {
  const text = strOrNull(value);
  if (!text) return null;
  const direct = text.match(/\d{4}-\d{2}-\d{2}/);
  if (direct) return direct[0];
  const date = new Date(text);
  if (Number.isFinite(date.getTime())) return date.toISOString().slice(0, 10);
  return null;
}

function populated(fields, fieldName) {
  return !isBlank(fields?.[fieldName]);
}

function countPopulated(rows, fields) {
  const out = {};
  for (const fieldName of fields) {
    out[fieldName] = rows.filter((row) => populated(row.fields || {}, fieldName)).length;
  }
  return out;
}

function airtableUrl(tableName, params = {}) {
  const url = new URL(`https://api.airtable.com/v0/${AIRTABLE_BASE_ID}/${encodeURIComponent(tableName)}`);
  for (const [key, value] of Object.entries(params)) {
    if (Array.isArray(value)) {
      for (const item of value) url.searchParams.append(key, item);
    } else if (value !== undefined && value !== null) {
      url.searchParams.set(key, String(value));
    }
  }
  return url;
}

async function fetchWithTimeout(url, options = {}) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), HTTP_TIMEOUT_MS);
  try {
    return await fetch(url, { ...options, signal: controller.signal });
  } finally {
    clearTimeout(timeout);
  }
}

async function airtableList(tableName, params = {}) {
  const rows = [];
  let offset = null;

  do {
    const response = await fetchWithTimeout(airtableUrl(tableName, { ...params, offset }), {
      headers: { Authorization: `Bearer ${AIRTABLE_TOKEN}` },
    });
    const text = await response.text();
    if (!response.ok) {
      throw new Error(`Airtable list failed (${response.status}) ${tableName}: ${text.slice(0, 500)}`);
    }
    const json = JSON.parse(text);
    rows.push(...(json.records || []));
    offset = json.offset || null;
  } while (offset);

  return rows;
}

async function latestHeartbeat() {
  const rows = await airtableList(TABLE_HEARTBEAT, {
    maxRecords: 1,
    "sort[0][field]": HEARTBEAT_CREATED_FIELD,
    "sort[0][direction]": "desc",
  });
  return rows[0] || null;
}

function heartbeatSlot(fields = {}) {
  const active = [
    fields.isA ? "A" : null,
    fields.isB ? "B" : null,
    fields.isC ? "C" : null,
    fields.isD ? "D" : null,
  ].filter(Boolean);
  return active.length === 1 ? active[0] : null;
}

function reasonCounts(rows) {
  const counts = new Map();
  for (const row of rows) {
    const reason = strOrNull(row.fields?.hb_second_pass_reason) || "(blank)";
    counts.set(reason, (counts.get(reason) || 0) + 1);
  }
  return Array.from(counts.entries())
    .sort((a, b) => b[1] - a[1])
    .slice(0, 10)
    .map(([reason, count]) => ({ reason, count }));
}

function missingSamples(rows) {
  const critical = ["order_of_go", "estimated_start_time", "total_trips", "completed_trips", "status"];
  const samples = [];
  for (const row of rows) {
    const fields = row.fields || {};
    const missing = critical.filter((fieldName) => !populated(fields, fieldName));
    if (!missing.length) continue;
    samples.push({
      record_id: row.id,
      ring_number: firstValue(fields.ring_number) ?? null,
      class_number: firstValue(fields.class_number) ?? null,
      entry_number: firstValue(fields.entry_number) ?? null,
      class_id: firstValue(fields.class_id) ?? null,
      missing,
      hb_second_pass_reason: strOrNull(fields.hb_second_pass_reason),
    });
    if (samples.length >= 8) break;
  }
  return samples;
}

async function snapshot() {
  const heartbeat = await latestHeartbeat();
  const heartbeatFields = heartbeat?.fields || {};
  const appShowId = numOrNull(firstValue(heartbeatFields.app_show_id)) ?? numOrNull(heartbeatFields.show_id);
  const appSqlDate = toIsoDateOnly(firstValue(heartbeatFields.app_sql_date)) ?? toIsoDateOnly(heartbeatFields.sql_date);

  const [watchScheduleRows, watchTripsRows, allGroupsLiveRows] = await Promise.all([
    airtableList(TABLE_WATCH_SCHEDULE, { view: VIEW_HEARTBEAT, pageSize: 100 }),
    airtableList(TABLE_WATCH_TRIPS, { view: VIEW_HEARTBEAT, pageSize: 100 }),
    airtableList(TABLE_GROUPS_LIVE, { pageSize: 100 }),
  ]);

  const groupsLiveRows = allGroupsLiveRows.filter((row) => {
    const fields = row.fields || {};
    const showId = numOrNull(fields.show_id);
    const day = toIsoDateOnly(fields.day);
    return (appShowId === null || showId === appShowId) &&
      (!appSqlDate || day === appSqlDate);
  });

  const effectiveHEid = watchTripsRows.filter((row) => {
    const fields = row.fields || {};
    return populated(fields, "h_eid") || populated(fields, "entry_number");
  }).length;

  return {
    ok: true,
    observed_at: new Date().toISOString(),
    heartbeat: {
      record_id: heartbeat?.id || null,
      created_time: strOrNull(heartbeatFields.created_time),
      slot: heartbeatSlot(heartbeatFields),
      mode: strOrNull(heartbeatFields[HEARTBEAT_MODE_FIELD]),
      cadence: firstValue(heartbeatFields[HEARTBEAT_CADENCE_FIELD]) ?? null,
      set_intervals: firstValue(heartbeatFields[HEARTBEAT_SET_INTERVALS_FIELD]) ?? null,
      interval: firstValue(heartbeatFields[HEARTBEAT_INTERVAL_FIELD]) ?? null,
      app_sid: appShowId,
      raw_sql_date: strOrNull(heartbeatFields.sql_date),
      app_sql_date: strOrNull(firstValue(heartbeatFields.app_sql_date)) || strOrNull(heartbeatFields.sql_date),
      shifted_to_next_day: firstValue(heartbeatFields.shifted_to_next_day) ?? null,
      app_time: strOrNull(heartbeatFields.time),
    },
    watch_schedule: {
      rows: watchScheduleRows.length,
      populated: countPopulated(watchScheduleRows, WATCH_SCHEDULE_FIELDS),
    },
    groups_live: {
      rows: groupsLiveRows.length,
      populated: countPopulated(groupsLiveRows, GROUPS_LIVE_FIELDS),
    },
    watch_trips: {
      rows: watchTripsRows.length,
      raw_populated: {
        ...countPopulated(watchTripsRows, WATCH_TRIPS_RAW_FIELDS),
        effective_h_eid: effectiveHEid,
      },
      rs_populated: countPopulated(watchTripsRows, WATCH_TRIPS_RS_FIELDS),
      second_pass_reasons: reasonCounts(watchTripsRows),
      missing_critical_samples: missingSamples(watchTripsRows),
    },
  };
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

(async function main() {
  for (let i = 0; i < cycles; i += 1) {
    try {
      console.log(JSON.stringify(await snapshot()));
    } catch (error) {
      console.log(JSON.stringify({
        ok: false,
        observed_at: new Date().toISOString(),
        error: String(error?.stack || error?.message || error).slice(0, 1000),
      }));
    }
    if (i < cycles - 1) await sleep(intervalSeconds * 1000);
  }
})().catch((error) => {
  console.error(error);
  process.exit(1);
});
