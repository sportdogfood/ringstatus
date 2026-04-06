/**
 * trips_calculator.js
 *
 * Downstream calculator for derived rs_* timing fields on watch_trips.
 * - reads normalized watch_trips rows
 * - computes 13 rs_* outputs
 * - patches changed outputs in promote mode
 * - writes audit rows to trip_logs
 */

const AIRTABLE_TOKEN = process.env.AIRTABLE_TOKEN || "";
const AIRTABLE_BASE_ID = process.env.AIRTABLE_BASE_ID || "";

const WATCH_TABLE = process.env.WATCH_TABLE || "watch_trips";
const WATCH_VIEW = process.env.WATCH_VIEW || "hb_targets";
const TRIP_LOGS_TABLE = process.env.TRIP_LOGS_TABLE || "trip_logs";
const MAX_RECORDS = Number(process.env.MAX_RECORDS || "500");

const HTTP_TIMEOUT_MS = Number(process.env.HTTP_TIMEOUT_MS || "20000");
const AT_RETRY_ATTEMPTS = Number(process.env.AT_RETRY_ATTEMPTS || "3");
const AT_RETRY_BASE_MS = Number(process.env.AT_RETRY_BASE_MS || "400");
const AT_RETRY_MAX_MS = Number(process.env.AT_RETRY_MAX_MS || "2000");

const DRY_RUN = String(process.env.DRY_RUN || "0") === "1";
const CALC_MODE = String(process.env.CALC_MODE || "shadow").trim().toLowerCase() === "promote"
  ? "promote"
  : "shadow";
const CALC_VERSION = String(process.env.CALC_VERSION || "trips_calculator_v1_2").trim();

const WATCH_FIELDS = {
  ENTRYXCLASSES_UUID: process.env.FIELD_ENTRYXCLASSES_UUID || "entryxclasses_uuid",
  APP_SHOW_ID: process.env.FIELD_APP_SHOW_ID || "app_show_id",
  APP_SQL_DATE: process.env.FIELD_APP_SQL_DATE || "app_sql_date",
  APP_TIME: process.env.FIELD_APP_TIME || "app_time",
  STATUS: process.env.FIELD_CLASS_STATUS || "status",
  CLASS_STATUS_FALLBACK: process.env.FIELD_CLASS_STATUS_FALLBACK || "class_status",
  ESTIMATED_START_TIME: process.env.FIELD_ESTIMATED_START_TIME || "estimated_start_time",
  ESTIMATED_END_TIME: process.env.FIELD_ESTIMATED_END_TIME || "estimated_end_time",
  REMAINING_TRIPS: process.env.FIELD_REMAINING_TRIPS || "remaining_trips",
  TOTAL_TRIPS: process.env.FIELD_TOTAL_TRIPS || "total_trips",
  COMPLETED_TRIPS: process.env.FIELD_COMPLETED_TRIPS || "completed_trips",
  ACTUAL_TIME: process.env.FIELD_ACTUAL_TIME || "actual_time",
  ESTIMATED_TIME: process.env.FIELD_ESTIMATED_TIME || "estimated_time",
  ESTIMATED_GO_TIME: process.env.FIELD_ESTIMATED_GO_TIME || "estimated_go_time",
  ORDER_OF_GO: process.env.FIELD_ORDER_OF_GO || "order_of_go",
  ACTUAL_ORDER: process.env.FIELD_ACTUAL_ORDER || "actual_order",
  ACTUAL_GO: process.env.FIELD_ACTUAL_GO || "actual_go",
  GONE_IN: process.env.FIELD_GONE_IN || "gone_in",
  H_EID: process.env.FIELD_H_EID || "h_eid",
  RS: {
    STATUS: process.env.FIELD_RS_STATUS || "rs_status",
    START_TIME: process.env.FIELD_RS_START_TIME || "rs_start_time",
    GO_TIME: process.env.FIELD_RS_GO_TIME || "rs_go_time",
    COMPLETED_TRIPS: process.env.FIELD_RS_COMPLETED_TRIPS || "rs_completed_trips",
    GONE_IN: process.env.FIELD_RS_GONE_IN || "rs_gone_in",
    TRIP_DEFAULT: process.env.FIELD_RS_TRIP_DEFAULT || "rs_trip_default",
    ORDER_OF_GO: process.env.FIELD_RS_ORDER_OF_GO || "rs_order_of_go",
    RUNNING_ORDER_OF_GO: process.env.FIELD_RS_RUNNING_ORDER_OF_GO || "rs_running_order_of_go",
    MINS_TILL_START: process.env.FIELD_RS_MINS_TILL_START || "rs_mins_till_start",
    MINS_SINCE_START: process.env.FIELD_RS_MINS_SINCE_START || "rs_mins_since_start",
    TRIP_TIME: process.env.FIELD_RS_TRIP_TIME || "rs_trip_time",
    TRIP_TIME2: process.env.FIELD_RS_TRIP_TIME2 || "rs_trip_time2",
    LENGTH: process.env.FIELD_RS_LENGTH || "rs_length",
    END_TIME: process.env.FIELD_RS_END_TIME || "rs_end_time",
    GO_MINS_FROM_START: process.env.FIELD_RS_GO_MINS_FROM_START || "rs_go_mins_from_start",
    GO_TIME_FROM_START: process.env.FIELD_RS_GO_TIME_FROM_START || "rs_go_time_from_start",
    MIN_TILL_GO: process.env.FIELD_RS_MIN_TILL_GO || "rs_min_till_go",
    MIN_TO_ACTUAL_GO: process.env.FIELD_RS_MIN_TO_ACTUAL_GO || "rs_min_to_actual_go",
  },
};

const LOG_KEY_FIELDS = {
  CALC_LOG_KEY: process.env.LOG_FIELD_CALC_LOG_KEY || "calc_log_key",
  WATCH_TRIPS_LINK: process.env.LOG_FIELD_WATCH_TRIPS_LINK || "watch_trips",
  WATCH_TRIP_RECORD_ID: process.env.LOG_FIELD_WATCH_TRIP_RECORD_ID || "watch_trip_record_id",
  ENTRYXCLASSES_UUID: process.env.LOG_FIELD_ENTRYXCLASSES_UUID || "entryxclasses_uuid",
  APP_SHOW_ID: process.env.LOG_FIELD_APP_SHOW_ID || "app_show_id",
  APP_SQL_DATE: process.env.LOG_FIELD_APP_SQL_DATE || "app_sql_date",
  CALC_MODE: process.env.LOG_FIELD_CALC_MODE || "calc_mode",
  CALC_VERSION: process.env.LOG_FIELD_CALC_VERSION || "calc_version",
  CALC_STATUS: process.env.LOG_FIELD_CALC_STATUS || "calc_status",
  SKIP_REASON: process.env.LOG_FIELD_SKIP_REASON || "skip_reason",
  CHANGED_FIELDS: process.env.LOG_FIELD_CHANGED_FIELDS || "changed_fields",
  CREATED_AT: process.env.LOG_FIELD_CREATED_AT || "created_at",
};

const LOG_RAW_FIELDS = {
  APP_TIME: process.env.LOG_RAW_APP_TIME || "raw_app_time",
  STATUS: process.env.LOG_RAW_STATUS || "raw_status",
  CLASS_STATUS: process.env.LOG_RAW_CLASS_STATUS || "raw_class_status",
  ESTIMATED_START_TIME: process.env.LOG_RAW_ESTIMATED_START_TIME || "raw_estimated_start_time",
  ESTIMATED_END_TIME: process.env.LOG_RAW_ESTIMATED_END_TIME || "raw_estimated_end_time",
  ACTUAL_TIME: process.env.LOG_RAW_ACTUAL_TIME || "raw_actual_time",
  ESTIMATED_TIME: process.env.LOG_RAW_ESTIMATED_TIME || "raw_estimated_time",
  ESTIMATED_GO_TIME: process.env.LOG_RAW_ESTIMATED_GO_TIME || "raw_estimated_go_time",
  ORDER_OF_GO: process.env.LOG_RAW_ORDER_OF_GO || "raw_order_of_go",
  ACTUAL_ORDER: process.env.LOG_RAW_ACTUAL_ORDER || "raw_actual_order",
  ACTUAL_GO: process.env.LOG_RAW_ACTUAL_GO || "raw_actual_go",
  GONE_IN: process.env.LOG_RAW_GONE_IN || "raw_gone_in",
  REMAINING_TRIPS: process.env.LOG_RAW_REMAINING_TRIPS || "raw_remaining_trips",
  TOTAL_TRIPS: process.env.LOG_RAW_TOTAL_TRIPS || "raw_total_trips",
  COMPLETED_TRIPS: process.env.LOG_RAW_COMPLETED_TRIPS || "raw_completed_trips",
  H_EID: process.env.LOG_RAW_H_EID || "raw_h_eid",
};

const LOG_NORM_FIELDS = {
  CLASS_STATUS: process.env.LOG_NORM_CLASS_STATUS || "norm_class_status",
  APP_TIME_TEXT: process.env.LOG_NORM_APP_TIME_TEXT || "norm_app_time_text",
  ESTIMATED_START_TIME_TEXT: process.env.LOG_NORM_ESTIMATED_START_TIME_TEXT || "norm_estimated_start_time_text",
  ESTIMATED_END_TIME_TEXT: process.env.LOG_NORM_ESTIMATED_END_TIME_TEXT || "norm_estimated_end_time_text",
  ACTUAL_TIME_TEXT: process.env.LOG_NORM_ACTUAL_TIME_TEXT || "norm_actual_time_text",
  ESTIMATED_TIME_TEXT: process.env.LOG_NORM_ESTIMATED_TIME_TEXT || "norm_estimated_time_text",
  ESTIMATED_GO_TIME_TEXT: process.env.LOG_NORM_ESTIMATED_GO_TIME_TEXT || "norm_estimated_go_time_text",
  H_EID: process.env.LOG_NORM_H_EID || "norm_h_eid",
  APP_TIME_MINS: process.env.LOG_NORM_APP_TIME_MINS || "norm_app_time_mins",
  ESTIMATED_START_TIME_MINS: process.env.LOG_NORM_ESTIMATED_START_TIME_MINS || "norm_estimated_start_time_mins",
  ESTIMATED_END_TIME_MINS: process.env.LOG_NORM_ESTIMATED_END_TIME_MINS || "norm_estimated_end_time_mins",
  ACTUAL_TIME_MINS: process.env.LOG_NORM_ACTUAL_TIME_MINS || "norm_actual_time_mins",
  ESTIMATED_TIME_MINS: process.env.LOG_NORM_ESTIMATED_TIME_MINS || "norm_estimated_time_mins",
  ESTIMATED_GO_TIME_MINS: process.env.LOG_NORM_ESTIMATED_GO_TIME_MINS || "norm_estimated_go_time_mins",
  ORDER_OF_GO: process.env.LOG_NORM_ORDER_OF_GO || "norm_order_of_go",
  ACTUAL_ORDER: process.env.LOG_NORM_ACTUAL_ORDER || "norm_actual_order",
  ACTUAL_GO: process.env.LOG_NORM_ACTUAL_GO || "norm_actual_go",
  GONE_IN: process.env.LOG_NORM_GONE_IN || "norm_gone_in",
  REMAINING_TRIPS: process.env.LOG_NORM_REMAINING_TRIPS || "norm_remaining_trips",
  TOTAL_TRIPS: process.env.LOG_NORM_TOTAL_TRIPS || "norm_total_trips",
  COMPLETED_TRIPS: process.env.LOG_NORM_COMPLETED_TRIPS || "norm_completed_trips",
};

const LOG_CALC_FIELDS = {
  EFFECTIVE_ORDER: process.env.LOG_CALC_EFFECTIVE_ORDER || "calc_effective_order",
  TRIP_MINUTES_RAW: process.env.LOG_CALC_TRIP_MINUTES_RAW || "calc_trip_minutes_raw",
  TRIP_MINUTES_FINAL: process.env.LOG_CALC_TRIP_MINUTES_FINAL || "calc_trip_minutes_final",
  PROJECTED_CLASS_MINUTES: process.env.LOG_CALC_PROJECTED_CLASS_MINUTES || "calc_projected_class_minutes",
  START_ANCHOR_MINS: process.env.LOG_CALC_START_ANCHOR_MINS || "calc_start_anchor_mins",
  START_ANCHOR_TEXT: process.env.LOG_CALC_START_ANCHOR_TEXT || "calc_start_anchor_text",
  TRIP_MINUTES_USED_DEFAULT: process.env.LOG_CALC_TRIP_MINUTES_USED_DEFAULT || "calc_trip_minutes_used_default",
  USED_ESTIMATED_GO_FALLBACK: process.env.LOG_CALC_USED_ESTIMATED_GO_FALLBACK || "calc_used_estimated_go_fallback",
};

const LOG_RS_FIELDS = {
  STATUS: process.env.LOG_RS_STATUS || "rs_status",
  START_TIME: process.env.LOG_RS_START_TIME || "rs_start_time",
  GO_TIME: process.env.LOG_RS_GO_TIME || "rs_go_time",
  COMPLETED_TRIPS: process.env.LOG_RS_COMPLETED_TRIPS || "rs_completed_trips",
  GONE_IN: process.env.LOG_RS_GONE_IN || "rs_gone_in",
  TRIP_DEFAULT: process.env.LOG_RS_TRIP_DEFAULT || "rs_trip_default",
  ORDER_OF_GO: process.env.LOG_RS_ORDER_OF_GO || "rs_order_of_go",
  RUNNING_ORDER_OF_GO: process.env.LOG_RS_RUNNING_ORDER_OF_GO || "rs_running_order_of_go",
  MINS_TILL_START: process.env.LOG_RS_MINS_TILL_START || "rs_mins_till_start",
  MINS_SINCE_START: process.env.LOG_RS_MINS_SINCE_START || "rs_mins_since_start",
  TRIP_TIME: process.env.LOG_RS_TRIP_TIME || "rs_trip_time",
  TRIP_TIME2: process.env.LOG_RS_TRIP_TIME2 || "rs_trip_time2",
  LENGTH: process.env.LOG_RS_LENGTH || "rs_length",
  END_TIME: process.env.LOG_RS_END_TIME || "rs_end_time",
  GO_MINS_FROM_START: process.env.LOG_RS_GO_MINS_FROM_START || "rs_go_mins_from_start",
  GO_TIME_FROM_START: process.env.LOG_RS_GO_TIME_FROM_START || "rs_go_time_from_start",
  MIN_TILL_GO: process.env.LOG_RS_MIN_TILL_GO || "rs_min_till_go",
  MIN_TO_ACTUAL_GO: process.env.LOG_RS_MIN_TO_ACTUAL_GO || "rs_min_to_actual_go",
};

const LOG_JSON_FIELDS = {
  INPUTS_JSON: process.env.LOG_FIELD_INPUTS_JSON || "inputs_json",
  PRIOR_OUTPUTS_JSON: process.env.LOG_FIELD_PRIOR_OUTPUTS_JSON || "prior_outputs_json",
  COMPUTED_OUTPUTS_JSON: process.env.LOG_FIELD_COMPUTED_OUTPUTS_JSON || "computed_outputs_json",
  ANOMALIES_JSON: process.env.LOG_FIELD_ANOMALIES_JSON || "anomalies_json",
};

const INVALID_ORDER_NUMS = new Set([0, 10000, 100000]);
const INVALID_TIME_TEXT = new Set(["00:00:00"]);
const TRIP_MINUTES_DEFAULT = 3;

const WATCH_SOURCE_FIELDS = [
  WATCH_FIELDS.ENTRYXCLASSES_UUID,
  WATCH_FIELDS.APP_SHOW_ID,
  WATCH_FIELDS.APP_SQL_DATE,
  WATCH_FIELDS.APP_TIME,
  WATCH_FIELDS.STATUS,
  WATCH_FIELDS.CLASS_STATUS_FALLBACK,
  WATCH_FIELDS.ESTIMATED_START_TIME,
  WATCH_FIELDS.ESTIMATED_END_TIME,
  WATCH_FIELDS.REMAINING_TRIPS,
  WATCH_FIELDS.TOTAL_TRIPS,
  WATCH_FIELDS.COMPLETED_TRIPS,
  WATCH_FIELDS.ACTUAL_TIME,
  WATCH_FIELDS.ESTIMATED_TIME,
  WATCH_FIELDS.ESTIMATED_GO_TIME,
  WATCH_FIELDS.ORDER_OF_GO,
  WATCH_FIELDS.ACTUAL_ORDER,
  WATCH_FIELDS.ACTUAL_GO,
  WATCH_FIELDS.GONE_IN,
  WATCH_FIELDS.H_EID,
];

const WATCH_OUTPUT_FIELDS = Object.values(WATCH_FIELDS.RS);

const OUTPUT_DURATION_FIELDS = new Set([
  WATCH_FIELDS.RS.TRIP_TIME,
  WATCH_FIELDS.RS.TRIP_TIME2,
]);

const OUTPUT_NUMBER_FIELDS = new Set([
  WATCH_FIELDS.RS.COMPLETED_TRIPS,
  WATCH_FIELDS.RS.GONE_IN,
  WATCH_FIELDS.RS.ORDER_OF_GO,
  WATCH_FIELDS.RS.RUNNING_ORDER_OF_GO,
  WATCH_FIELDS.RS.MINS_TILL_START,
  WATCH_FIELDS.RS.MINS_SINCE_START,
  WATCH_FIELDS.RS.GO_MINS_FROM_START,
  WATCH_FIELDS.RS.MIN_TILL_GO,
  WATCH_FIELDS.RS.MIN_TO_ACTUAL_GO,
]);

const OUTPUT_TEXT_FIELDS = new Set([
  WATCH_FIELDS.RS.STATUS,
  WATCH_FIELDS.RS.START_TIME,
  WATCH_FIELDS.RS.GO_TIME,
  WATCH_FIELDS.RS.TRIP_DEFAULT,
  WATCH_FIELDS.RS.LENGTH,
  WATCH_FIELDS.RS.END_TIME,
  WATCH_FIELDS.RS.GO_TIME_FROM_START,
]);

const WATCH_TO_LOG_RS_FIELD_MAP = {
  [WATCH_FIELDS.RS.STATUS]: LOG_RS_FIELDS.STATUS,
  [WATCH_FIELDS.RS.START_TIME]: LOG_RS_FIELDS.START_TIME,
  [WATCH_FIELDS.RS.GO_TIME]: LOG_RS_FIELDS.GO_TIME,
  [WATCH_FIELDS.RS.COMPLETED_TRIPS]: LOG_RS_FIELDS.COMPLETED_TRIPS,
  [WATCH_FIELDS.RS.GONE_IN]: LOG_RS_FIELDS.GONE_IN,
  [WATCH_FIELDS.RS.TRIP_DEFAULT]: LOG_RS_FIELDS.TRIP_DEFAULT,
  [WATCH_FIELDS.RS.ORDER_OF_GO]: LOG_RS_FIELDS.ORDER_OF_GO,
  [WATCH_FIELDS.RS.RUNNING_ORDER_OF_GO]: LOG_RS_FIELDS.RUNNING_ORDER_OF_GO,
  [WATCH_FIELDS.RS.MINS_TILL_START]: LOG_RS_FIELDS.MINS_TILL_START,
  [WATCH_FIELDS.RS.MINS_SINCE_START]: LOG_RS_FIELDS.MINS_SINCE_START,
  [WATCH_FIELDS.RS.TRIP_TIME]: LOG_RS_FIELDS.TRIP_TIME,
  [WATCH_FIELDS.RS.TRIP_TIME2]: LOG_RS_FIELDS.TRIP_TIME2,
  [WATCH_FIELDS.RS.LENGTH]: LOG_RS_FIELDS.LENGTH,
  [WATCH_FIELDS.RS.END_TIME]: LOG_RS_FIELDS.END_TIME,
  [WATCH_FIELDS.RS.GO_MINS_FROM_START]: LOG_RS_FIELDS.GO_MINS_FROM_START,
  [WATCH_FIELDS.RS.GO_TIME_FROM_START]: LOG_RS_FIELDS.GO_TIME_FROM_START,
  [WATCH_FIELDS.RS.MIN_TILL_GO]: LOG_RS_FIELDS.MIN_TILL_GO,
  [WATCH_FIELDS.RS.MIN_TO_ACTUAL_GO]: LOG_RS_FIELDS.MIN_TO_ACTUAL_GO,
};

function requireEnv(name, value) {
  if (!value) throw new Error(`Missing required env: ${name}`);
}

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

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

function firstNonBlank(...values) {
  for (const value of values) {
    if (!isBlank(value)) return value;
  }
  return null;
}

function chunk(items, size) {
  const out = [];
  for (let i = 0; i < items.length; i += size) out.push(items.slice(i, i + size));
  return out;
}

function stableStringify(value) {
  const seen = new WeakSet();

  function sortNode(node) {
    if (node === null || typeof node !== "object") return node;
    if (seen.has(node)) return null;
    seen.add(node);
    if (Array.isArray(node)) return node.map(sortNode);

    const out = {};
    for (const key of Object.keys(node).sort()) out[key] = sortNode(node[key]);
    return out;
  }

  return JSON.stringify(sortNode(value));
}

function roundNumber(value, digits = 6) {
  if (!Number.isFinite(value)) return null;
  const factor = 10 ** digits;
  return Math.round(value * factor) / factor;
}

function formatDecimalText(value, digits = 4) {
  if (!Number.isFinite(value)) return null;
  return value.toFixed(digits).replace(/\.?0+$/, "");
}

function pushSample(list, value, limit = 10) {
  if (!Array.isArray(list) || list.length >= limit) return;
  if (!value) return;
  if (list.includes(value)) return;
  list.push(value);
}

function normalizeTimeText(value) {
  const text = strOrNull(value);
  if (!text) return null;
  return INVALID_TIME_TEXT.has(text) ? null : text;
}

function normalizeOrderValue(value) {
  const num = numOrNull(value);
  if (num === null) return null;
  if (INVALID_ORDER_NUMS.has(num)) return null;
  return Math.trunc(num);
}

function normalizeCountValue(value) {
  const num = numOrNull(value);
  return num === null ? null : roundNumber(num, 6);
}

function normalizeClassStatusValue(...candidates) {
  for (const candidate of candidates) {
    if (Array.isArray(candidate)) continue;

    if (candidate && typeof candidate === "object") {
      const named = strOrNull(candidate.name ?? candidate.label ?? candidate.value);
      if (named) return named;
      continue;
    }

    const text = strOrNull(candidate);
    if (text) return text;
  }

  return null;
}

function parseClockToMinutes(value) {
  const text = normalizeTimeText(value);
  if (!text) return null;

  let match = text.match(/^(\d{1,2}):(\d{2})(?::(\d{2}))?\s*(AM|PM)$/i);
  if (match) {
    let hh = Number(match[1]);
    const mm = Number(match[2]);
    const ss = Number(match[3] || "0");
    const ap = match[4].toUpperCase();

    if (!Number.isFinite(hh) || !Number.isFinite(mm) || !Number.isFinite(ss)) return null;
    if (hh < 1 || hh > 12 || mm < 0 || mm > 59 || ss < 0 || ss > 59) return null;

    if (ap === "AM") {
      if (hh === 12) hh = 0;
    } else if (hh !== 12) {
      hh += 12;
    }

    return hh * 60 + mm + (ss / 60);
  }

  match = text.match(/^(\d{1,2}):(\d{2})(?::(\d{2}))?$/);
  if (!match) return null;

  const hh = Number(match[1]);
  const mm = Number(match[2]);
  const ss = Number(match[3] || "0");
  if (!Number.isFinite(hh) || !Number.isFinite(mm) || !Number.isFinite(ss)) return null;
  if (hh < 0 || hh > 23 || mm < 0 || mm > 59 || ss < 0 || ss > 59) return null;

  return hh * 60 + mm + (ss / 60);
}

function formatClockFromMinutes(value) {
  if (!Number.isFinite(value) || value < 0) return null;

  const totalSeconds = Math.round(value * 60);
  const hh = Math.floor(totalSeconds / 3600);
  const mm = Math.floor((totalSeconds % 3600) / 60);
  const ss = totalSeconds % 60;

  return `${hh}:${String(mm).padStart(2, "0")}:${String(ss).padStart(2, "0")}`;
}

function durationSecondsFromMinutes(value) {
  if (!Number.isFinite(value)) return null;
  return Math.round(value * 60);
}

function jsonForField(value, maxLen = 8000) {
  try {
    const text = stableStringify(value);
    if (text.length <= maxLen) return text;
    return `${text.slice(0, maxLen)}...(truncated)`;
  } catch (err) {
    return String(err?.message || err);
  }
}

function serializeRawValueForLog(value) {
  if (value === undefined || value === null) return undefined;
  if (typeof value === "string") {
    const text = value.trim();
    return text || undefined;
  }
  if (typeof value === "number" || typeof value === "boolean") {
    return String(value);
  }
  try {
    return stableStringify(value);
  } catch (_) {
    const text = String(value).trim();
    return text || undefined;
  }
}

async function fetchWithTimeout(url, opts = {}) {
  const ac = new AbortController();
  const timer = setTimeout(() => ac.abort(), HTTP_TIMEOUT_MS);
  try {
    return await fetch(url, { ...opts, signal: ac.signal });
  } finally {
    clearTimeout(timer);
  }
}

function isRetryableFetchError(err) {
  const name = String(err?.name || "");
  const code = String(err?.code || "");
  const msg = String(err?.message || "");
  return name === "AbortError" ||
    code === "UND_ERR_CONNECT_TIMEOUT" ||
    code === "UND_ERR_HEADERS_TIMEOUT" ||
    code === "UND_ERR_BODY_TIMEOUT" ||
    /timeout/i.test(msg) ||
    /fetch failed/i.test(msg);
}

async function fetchWithRetry(url, opts = {}, retry = {}) {
  const attempts = Math.max(1, Math.floor(Number(retry.attempts ?? AT_RETRY_ATTEMPTS)));
  const baseMs = Math.max(0, Math.floor(Number(retry.baseMs ?? AT_RETRY_BASE_MS)));
  const maxMs = Math.max(250, Math.floor(Number(retry.maxMs ?? AT_RETRY_MAX_MS)));

  let lastErr = null;

  for (let i = 1; i <= attempts; i += 1) {
    try {
      const res = await fetchWithTimeout(url, opts);
      if (res.status === 429 || (res.status >= 500 && res.status <= 599)) {
        if (i === attempts) return res;
        await sleep(Math.min(maxMs, baseMs * i + Math.floor(Math.random() * 200)));
        continue;
      }
      return res;
    } catch (err) {
      lastErr = err;
      if (!isRetryableFetchError(err) || i === attempts) throw err;
      await sleep(Math.min(maxMs, baseMs * i + Math.floor(Math.random() * 250)));
    }
  }

  throw lastErr || new Error("fetchWithRetry failed");
}

function airtableUrl(tableName) {
  return `https://api.airtable.com/v0/${AIRTABLE_BASE_ID}/${encodeURIComponent(tableName)}`;
}

async function airtableList({ table, view, fields = [], maxRecords = 0 }) {
  const rows = [];
  let offset = null;

  while (true) {
    const url = new URL(airtableUrl(table));
    if (view) url.searchParams.set("view", view);
    url.searchParams.set("pageSize", "100");
    if (maxRecords > 0) url.searchParams.set("maxRecords", String(maxRecords));
    if (offset) url.searchParams.set("offset", offset);
    for (const fieldName of fields) {
      if (fieldName) url.searchParams.append("fields[]", fieldName);
    }

    const res = await fetchWithRetry(url.toString(), {
      headers: { Authorization: `Bearer ${AIRTABLE_TOKEN}` }
    });

    if (!res.ok) {
      const body = await res.text().catch(() => "");
      throw new Error(`Airtable list failed (${res.status}) ${table}/${view || "-"}: ${body}`);
    }

    const json = await res.json().catch(() => ({}));
    rows.push(...(json.records || []));
    if (maxRecords > 0 && rows.length >= maxRecords) return rows.slice(0, maxRecords);
    offset = json.offset;
    if (!offset) break;
  }

  return rows;
}

async function airtablePatchRecords({ table, updates }) {
  if (!updates.length) return;

  const res = await fetchWithRetry(airtableUrl(table), {
    method: "PATCH",
    headers: {
      Authorization: `Bearer ${AIRTABLE_TOKEN}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ records: updates }),
  });

  if (!res.ok) {
    const body = await res.text().catch(() => "");
    throw new Error(`Airtable patch failed (${res.status}) ${table}: ${body}`);
  }
}

async function airtableCreateRecords({ table, records }) {
  if (!records.length) return;

  const res = await fetchWithRetry(airtableUrl(table), {
    method: "POST",
    headers: {
      Authorization: `Bearer ${AIRTABLE_TOKEN}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ records }),
  });

  if (!res.ok) {
    const body = await res.text().catch(() => "");
    throw new Error(`Airtable create failed (${res.status}) ${table}: ${body}`);
  }
}

async function airtablePatchWithFallback({ table, updates }) {
  if (!updates.length) return { okRows: 0, failedRows: [] };

  let okRows = 0;
  const failedRows = [];

  for (const batch of chunk(updates, 10)) {
    try {
      await airtablePatchRecords({ table, updates: batch });
      okRows += batch.length;
    } catch (batchErr) {
      console.log(`patch warn: batch failed, falling back :: ${String(batchErr?.message || batchErr).slice(0, 300)}`);
      for (const row of batch) {
        try {
          await airtablePatchRecords({ table, updates: [row] });
          okRows += 1;
        } catch (rowErr) {
          failedRows.push({
            record_id: row.id,
            reason: String(rowErr?.message || rowErr).slice(0, 300),
            attempted_fields: Object.keys(row.fields || {}),
          });
        }
      }
    }
  }

  return { okRows, failedRows };
}

async function airtableCreateWithFallback({ table, records }) {
  if (!records.length) return { okRows: 0, failedRows: [] };

  let okRows = 0;
  const failedRows = [];

  for (const batch of chunk(records, 10)) {
    try {
      await airtableCreateRecords({ table, records: batch });
      okRows += batch.length;
    } catch (batchErr) {
      console.log(`create warn: batch failed, falling back :: ${String(batchErr?.message || batchErr).slice(0, 300)}`);
      for (const row of batch) {
        try {
          await airtableCreateRecords({ table, records: [row] });
          okRows += 1;
        } catch (rowErr) {
          failedRows.push({
            entryxclasses_uuid: row?.fields?.[LOG_KEY_FIELDS.ENTRYXCLASSES_UUID] || "",
            reason: String(rowErr?.message || rowErr).slice(0, 300),
          });
        }
      }
    }
  }

  return { okRows, failedRows };
}

function buildRawInputs(fields) {
  return {
    entryxclasses_uuid: fields[WATCH_FIELDS.ENTRYXCLASSES_UUID],
    app_show_id: fields[WATCH_FIELDS.APP_SHOW_ID],
    app_sql_date: fields[WATCH_FIELDS.APP_SQL_DATE],
    app_time: fields[WATCH_FIELDS.APP_TIME],
    status: fields[WATCH_FIELDS.STATUS],
    class_status: fields[WATCH_FIELDS.CLASS_STATUS_FALLBACK],
    estimated_start_time: fields[WATCH_FIELDS.ESTIMATED_START_TIME],
    estimated_end_time: fields[WATCH_FIELDS.ESTIMATED_END_TIME],
    remaining_trips: fields[WATCH_FIELDS.REMAINING_TRIPS],
    total_trips: fields[WATCH_FIELDS.TOTAL_TRIPS],
    completed_trips: fields[WATCH_FIELDS.COMPLETED_TRIPS],
    actual_time: fields[WATCH_FIELDS.ACTUAL_TIME],
    estimated_time: fields[WATCH_FIELDS.ESTIMATED_TIME],
    estimated_go_time: fields[WATCH_FIELDS.ESTIMATED_GO_TIME],
    order_of_go: fields[WATCH_FIELDS.ORDER_OF_GO],
    actual_order: fields[WATCH_FIELDS.ACTUAL_ORDER],
    actual_go: fields[WATCH_FIELDS.ACTUAL_GO],
    gone_in: fields[WATCH_FIELDS.GONE_IN],
    h_eid: fields[WATCH_FIELDS.H_EID],
  };
}

function parseTimeInput(label, rawValue, anomalies) {
  const rawText = strOrNull(rawValue);
  const text = normalizeTimeText(rawValue);
  if (!text) return { text: null, minutes: null };

  const minutes = parseClockToMinutes(text);
  if (minutes === null && rawText) {
    anomalies.push(`invalid_${label}:${rawText}`);
  }

  return { text, minutes };
}

function buildNormalizedInputs(record) {
  const fields = record?.fields || {};
  const anomalies = [];
  const rawInputs = buildRawInputs(fields);

  const appTime = parseTimeInput("app_time", rawInputs.app_time, anomalies);
  const estimatedStartTime = parseTimeInput("estimated_start_time", rawInputs.estimated_start_time, anomalies);
  const estimatedEndTime = parseTimeInput("estimated_end_time", rawInputs.estimated_end_time, anomalies);
  const actualTime = parseTimeInput("actual_time", rawInputs.actual_time, anomalies);
  const estimatedTime = parseTimeInput("estimated_time", rawInputs.estimated_time, anomalies);
  const estimatedGoTime = parseTimeInput("estimated_go_time", rawInputs.estimated_go_time, anomalies);

  return {
    rawInputs,
    anomalies,
    values: {
      entryxclasses_uuid: strOrNull(rawInputs.entryxclasses_uuid),
      app_show_id: numOrNull(rawInputs.app_show_id),
      app_sql_date: strOrNull(rawInputs.app_sql_date),
      class_status: normalizeClassStatusValue(
        rawInputs.status,
        rawInputs.class_status
      ),
      remaining_trips: normalizeCountValue(rawInputs.remaining_trips),
      total_trips: normalizeCountValue(rawInputs.total_trips),
      completed_trips: normalizeCountValue(rawInputs.completed_trips),
      order_of_go: normalizeOrderValue(rawInputs.order_of_go),
      actual_order: normalizeOrderValue(rawInputs.actual_order),
      actual_go: normalizeOrderValue(rawInputs.actual_go),
      gone_in: normalizeCountValue(rawInputs.gone_in),
      h_eid: strOrNull(rawInputs.h_eid),
      app_time_text: appTime.text,
      app_time_minutes: appTime.minutes,
      estimated_start_time_text: estimatedStartTime.text,
      estimated_start_time_minutes: estimatedStartTime.minutes,
      estimated_end_time_text: estimatedEndTime.text,
      estimated_end_time_minutes: estimatedEndTime.minutes,
      actual_time_text: actualTime.text,
      actual_time_minutes: actualTime.minutes,
      estimated_time_text: estimatedTime.text,
      estimated_time_minutes: estimatedTime.minutes,
      estimated_go_time_text: estimatedGoTime.text,
      estimated_go_time_minutes: estimatedGoTime.minutes,
    },
  };
}

function determineEligibility(values) {
  const skipReasons = [];
  const classStatus = String(values.class_status || "").trim().toLowerCase();

  if (!values.entryxclasses_uuid) skipReasons.push("missing_entryxclasses_uuid");
  if (!values.h_eid) skipReasons.push("missing_h_eid");
  if (values.gone_in === 1) skipReasons.push("gone_in_1");
  if (classStatus && /complete(d)?/.test(classStatus)) skipReasons.push("class_complete");

  return {
    eligible: skipReasons.length === 0,
    skipReasons,
  };
}

function computeCanonicalOutputs(values, priorAnomalies = []) {
  const anomalies = [...priorAnomalies];

  const tripMinutesDefault = TRIP_MINUTES_DEFAULT;
  const effectiveOrder = firstNonBlank(values.actual_order, values.actual_go, values.order_of_go);
  const runningOrder = (
    effectiveOrder !== null && values.completed_trips !== null
      ? roundNumber(effectiveOrder - values.completed_trips, 6)
      : null
  );

  if (runningOrder !== null && runningOrder < 0) anomalies.push("negative_running_order");

  const minutesUntilStart = (
    values.estimated_start_time_minutes !== null && values.app_time_minutes !== null
      ? roundNumber(values.estimated_start_time_minutes - values.app_time_minutes, 6)
      : null
  );

  const minutesSinceStart = (
    values.estimated_start_time_minutes !== null && values.app_time_minutes !== null
      ? roundNumber(values.app_time_minutes - values.estimated_start_time_minutes, 6)
      : null
  );

  let rawTripMinutes = null;
  if (minutesSinceStart !== null && values.completed_trips !== null && values.completed_trips > 0) {
    rawTripMinutes = roundNumber(minutesSinceStart / values.completed_trips, 6);
  }

  const tripMinutesUsedDefault =
    rawTripMinutes === null ||
    rawTripMinutes < 2 ||
    rawTripMinutes > 4;

  let tripMinutes = rawTripMinutes;
  if (tripMinutesUsedDefault) {
    anomalies.push(rawTripMinutes === null ? "trip_minutes_default_missing_rate" : "trip_minutes_default_out_of_range");
    tripMinutes = tripMinutesDefault;
  }

  const tripDurationSeconds = durationSecondsFromMinutes(tripMinutes);
  const projectedClassMinutes = (
    values.total_trips !== null
      ? roundNumber(values.total_trips * tripMinutes, 6)
      : null
  );

  const startAnchorMinutes = firstNonBlank(values.actual_time_minutes, values.estimated_start_time_minutes);
  const startAnchorText = firstNonBlank(values.actual_time_text, values.estimated_start_time_text);
  if (startAnchorMinutes === null) anomalies.push("missing_start_anchor");
  if (effectiveOrder === null) anomalies.push("missing_effective_order");

  const projectedEndMinutes = (
    startAnchorMinutes !== null && projectedClassMinutes !== null
      ? roundNumber(startAnchorMinutes + projectedClassMinutes, 6)
      : null
  );
  const projectedEndClock = formatClockFromMinutes(projectedEndMinutes);

  let goMinutesFromStart = null;
  let usedEstimatedGoFallback = false;

  if (effectiveOrder !== null) {
    goMinutesFromStart = roundNumber(Math.max(effectiveOrder - 1, 0) * tripMinutes, 6);
  } else if (values.estimated_go_time_minutes !== null && startAnchorMinutes !== null) {
    goMinutesFromStart = roundNumber(values.estimated_go_time_minutes - startAnchorMinutes, 6);
    usedEstimatedGoFallback = true;
  }

  if (usedEstimatedGoFallback) anomalies.push("used_estimated_go_fallback");
  if (goMinutesFromStart !== null && goMinutesFromStart < 0) anomalies.push("negative_go_minutes_from_start");

  const goClockMinutes = (
    startAnchorMinutes !== null && goMinutesFromStart !== null
      ? roundNumber(startAnchorMinutes + goMinutesFromStart, 6)
      : values.estimated_go_time_minutes
  );

  const goClockFromStart = (
    startAnchorMinutes !== null && goMinutesFromStart !== null
      ? formatClockFromMinutes(goClockMinutes)
      : values.estimated_go_time_text
  );

  const minutesUntilGo = (
    goClockMinutes !== null && values.app_time_minutes !== null
      ? roundNumber(goClockMinutes - values.app_time_minutes, 6)
      : null
  );

  const minutesFromActualStartToGo = (
    goClockMinutes !== null && values.actual_time_minutes !== null
      ? roundNumber(goClockMinutes - values.actual_time_minutes, 6)
      : null
  );

  const canonical = {
    trip_minutes_default: tripMinutesDefault,
    effective_order: effectiveOrder,
    running_order: runningOrder,
    minutes_until_start: minutesUntilStart,
    minutes_since_start: minutesSinceStart,
    raw_trip_minutes: rawTripMinutes,
    trip_minutes: tripMinutes,
    trip_minutes_final: tripMinutes,
    trip_minutes_used_default: tripMinutesUsedDefault,
    trip_duration_seconds: tripDurationSeconds,
    projected_class_minutes: projectedClassMinutes,
    start_anchor_minutes: startAnchorMinutes,
    start_anchor_text: startAnchorText,
    projected_end_clock: projectedEndClock,
    go_minutes_from_start: goMinutesFromStart,
    go_clock_from_start: goClockFromStart,
    minutes_until_go: minutesUntilGo,
    minutes_from_actual_start_to_go: minutesFromActualStartToGo,
    used_estimated_go_fallback: usedEstimatedGoFallback,
  };

  const outputs = {
    [WATCH_FIELDS.RS.STATUS]: values.class_status,
    [WATCH_FIELDS.RS.START_TIME]: startAnchorText,
    [WATCH_FIELDS.RS.GO_TIME]: goClockFromStart,
    [WATCH_FIELDS.RS.COMPLETED_TRIPS]: values.completed_trips,
    [WATCH_FIELDS.RS.GONE_IN]: values.gone_in,
    [WATCH_FIELDS.RS.TRIP_DEFAULT]: String(tripMinutesDefault),
    [WATCH_FIELDS.RS.ORDER_OF_GO]: effectiveOrder,
    [WATCH_FIELDS.RS.RUNNING_ORDER_OF_GO]: runningOrder,
    [WATCH_FIELDS.RS.MINS_TILL_START]: minutesUntilStart,
    [WATCH_FIELDS.RS.MINS_SINCE_START]: minutesSinceStart,
    [WATCH_FIELDS.RS.TRIP_TIME]: tripDurationSeconds,
    [WATCH_FIELDS.RS.TRIP_TIME2]: tripDurationSeconds,
    [WATCH_FIELDS.RS.LENGTH]: projectedClassMinutes === null ? null : formatDecimalText(projectedClassMinutes, 4),
    [WATCH_FIELDS.RS.END_TIME]: projectedEndClock,
    [WATCH_FIELDS.RS.GO_MINS_FROM_START]: goMinutesFromStart,
    [WATCH_FIELDS.RS.GO_TIME_FROM_START]: goClockFromStart,
    [WATCH_FIELDS.RS.MIN_TILL_GO]: minutesUntilGo,
    [WATCH_FIELDS.RS.MIN_TO_ACTUAL_GO]: minutesFromActualStartToGo,
  };

  return { canonical, outputs, anomalies };
}

function normalizeOutputValue(fieldName, value) {
  if (OUTPUT_DURATION_FIELDS.has(fieldName)) {
    const num = numOrNull(value);
    return num === null ? null : Math.round(num);
  }

  if (OUTPUT_NUMBER_FIELDS.has(fieldName)) {
    const num = numOrNull(value);
    return num === null ? null : roundNumber(num, 6);
  }

  if (OUTPUT_TEXT_FIELDS.has(fieldName)) {
    return strOrNull(value);
  }

  return value ?? null;
}

function sameOutputValue(fieldName, left, right) {
  const a = normalizeOutputValue(fieldName, left);
  const b = normalizeOutputValue(fieldName, right);
  if (a === null && b === null) return true;
  if (typeof a === "number" && typeof b === "number") return Math.abs(a - b) < 0.000001;
  return a === b;
}

function buildPriorOutputs(fields) {
  const out = {};
  for (const fieldName of WATCH_OUTPUT_FIELDS) {
    out[fieldName] = normalizeOutputValue(fieldName, fields[fieldName]);
  }
  return out;
}

function buildChangedFields(priorOutputs, computedOutputs) {
  const changedNames = [];
  const patchFields = {};

  for (const fieldName of WATCH_OUTPUT_FIELDS) {
    const nextValue = normalizeOutputValue(fieldName, computedOutputs[fieldName]);
    const prevValue = normalizeOutputValue(fieldName, priorOutputs[fieldName]);
    if (sameOutputValue(fieldName, prevValue, nextValue)) continue;
    changedNames.push(fieldName);
    patchFields[fieldName] = nextValue;
  }

  return { changedNames, patchFields };
}

function setIfPresent(target, fieldName, value) {
  if (!fieldName) return;
  if (value === undefined || value === null) return;
  if (typeof value === "string" && value.trim() === "") return;
  target[fieldName] = value;
}

function linkOne(recordId) {
  return recordId ? [{ id: recordId }] : undefined;
}

function buildRawLogValues(rawInputs) {
  const raw = rawInputs || {};
  return {
    [LOG_RAW_FIELDS.APP_TIME]: serializeRawValueForLog(raw.app_time),
    [LOG_RAW_FIELDS.STATUS]: serializeRawValueForLog(raw.status),
    [LOG_RAW_FIELDS.CLASS_STATUS]: serializeRawValueForLog(raw.class_status),
    [LOG_RAW_FIELDS.ESTIMATED_START_TIME]: serializeRawValueForLog(raw.estimated_start_time),
    [LOG_RAW_FIELDS.ESTIMATED_END_TIME]: serializeRawValueForLog(raw.estimated_end_time),
    [LOG_RAW_FIELDS.ACTUAL_TIME]: serializeRawValueForLog(raw.actual_time),
    [LOG_RAW_FIELDS.ESTIMATED_TIME]: serializeRawValueForLog(raw.estimated_time),
    [LOG_RAW_FIELDS.ESTIMATED_GO_TIME]: serializeRawValueForLog(raw.estimated_go_time),
    [LOG_RAW_FIELDS.ORDER_OF_GO]: serializeRawValueForLog(raw.order_of_go),
    [LOG_RAW_FIELDS.ACTUAL_ORDER]: serializeRawValueForLog(raw.actual_order),
    [LOG_RAW_FIELDS.ACTUAL_GO]: serializeRawValueForLog(raw.actual_go),
    [LOG_RAW_FIELDS.GONE_IN]: serializeRawValueForLog(raw.gone_in),
    [LOG_RAW_FIELDS.REMAINING_TRIPS]: serializeRawValueForLog(raw.remaining_trips),
    [LOG_RAW_FIELDS.TOTAL_TRIPS]: serializeRawValueForLog(raw.total_trips),
    [LOG_RAW_FIELDS.COMPLETED_TRIPS]: serializeRawValueForLog(raw.completed_trips),
    [LOG_RAW_FIELDS.H_EID]: serializeRawValueForLog(raw.h_eid),
  };
}

function buildNormalizedLogValues(normalized) {
  const values = normalized || {};
  return {
    [LOG_NORM_FIELDS.CLASS_STATUS]: values.class_status,
    [LOG_NORM_FIELDS.APP_TIME_TEXT]: values.app_time_text,
    [LOG_NORM_FIELDS.ESTIMATED_START_TIME_TEXT]: values.estimated_start_time_text,
    [LOG_NORM_FIELDS.ESTIMATED_END_TIME_TEXT]: values.estimated_end_time_text,
    [LOG_NORM_FIELDS.ACTUAL_TIME_TEXT]: values.actual_time_text,
    [LOG_NORM_FIELDS.ESTIMATED_TIME_TEXT]: values.estimated_time_text,
    [LOG_NORM_FIELDS.ESTIMATED_GO_TIME_TEXT]: values.estimated_go_time_text,
    [LOG_NORM_FIELDS.H_EID]: values.h_eid,
    [LOG_NORM_FIELDS.APP_TIME_MINS]: values.app_time_minutes,
    [LOG_NORM_FIELDS.ESTIMATED_START_TIME_MINS]: values.estimated_start_time_minutes,
    [LOG_NORM_FIELDS.ESTIMATED_END_TIME_MINS]: values.estimated_end_time_minutes,
    [LOG_NORM_FIELDS.ACTUAL_TIME_MINS]: values.actual_time_minutes,
    [LOG_NORM_FIELDS.ESTIMATED_TIME_MINS]: values.estimated_time_minutes,
    [LOG_NORM_FIELDS.ESTIMATED_GO_TIME_MINS]: values.estimated_go_time_minutes,
    [LOG_NORM_FIELDS.ORDER_OF_GO]: values.order_of_go,
    [LOG_NORM_FIELDS.ACTUAL_ORDER]: values.actual_order,
    [LOG_NORM_FIELDS.ACTUAL_GO]: values.actual_go,
    [LOG_NORM_FIELDS.GONE_IN]: values.gone_in,
    [LOG_NORM_FIELDS.REMAINING_TRIPS]: values.remaining_trips,
    [LOG_NORM_FIELDS.TOTAL_TRIPS]: values.total_trips,
    [LOG_NORM_FIELDS.COMPLETED_TRIPS]: values.completed_trips,
  };
}

function buildCalcLogValues(canonical) {
  const calc = canonical || {};
  return {
    [LOG_CALC_FIELDS.EFFECTIVE_ORDER]: calc.effective_order,
    [LOG_CALC_FIELDS.TRIP_MINUTES_RAW]: calc.raw_trip_minutes,
    [LOG_CALC_FIELDS.TRIP_MINUTES_FINAL]: calc.trip_minutes_final,
    [LOG_CALC_FIELDS.PROJECTED_CLASS_MINUTES]: calc.projected_class_minutes,
    [LOG_CALC_FIELDS.START_ANCHOR_MINS]: calc.start_anchor_minutes,
    [LOG_CALC_FIELDS.START_ANCHOR_TEXT]: calc.start_anchor_text,
    [LOG_CALC_FIELDS.TRIP_MINUTES_USED_DEFAULT]: calc.trip_minutes_used_default,
    [LOG_CALC_FIELDS.USED_ESTIMATED_GO_FALLBACK]: calc.used_estimated_go_fallback,
  };
}

function buildRsLogValues(computedOutputs) {
  const out = {};
  for (const fieldName of WATCH_OUTPUT_FIELDS) {
    out[WATCH_TO_LOG_RS_FIELD_MAP[fieldName]] = computedOutputs?.[fieldName];
  }
  return out;
}

function finalCalcStatus(result, patchFailure) {
  if (!result.eligibility.eligible) return "skipped";
  if (patchFailure) return "patch_failed";
  if (result.changedFields.length > 0) return CALC_MODE === "shadow" ? "shadow_changed" : "changed";
  if (result.anomalies.length > 0) return "anomaly";
  return "unchanged";
}

function shouldCreateLogRow(result, patchFailure) {
  return !result.eligibility.eligible ||
    result.changedFields.length > 0 ||
    result.anomalies.length > 0 ||
    !!patchFailure;
}

function buildTripLogRecord(result, patchFailure) {
  const fields = {};
  const createdAt = new Date().toISOString();
  const status = finalCalcStatus(result, patchFailure);
  const skipReason = result.eligibility.skipReasons.join(",");
  const changedFields = result.changedFields.join(",");
  const anomalies = patchFailure
    ? [...result.anomalies, `patch_failed:${patchFailure.reason}`]
    : result.anomalies;
  const rawInputs = result.inputsForLog?.raw || {};
  const normalized = result.inputsForLog?.normalized || {};
  const canonical = result.canonicalOutputs || {};
  const computedOutputs = result.computedOutputs || {};
  const calcLogKey = [
    result.entryxclasses_uuid || result.recordId || "na",
    CALC_VERSION,
    CALC_MODE,
    createdAt,
  ].join("|");
  const rawLogValues = buildRawLogValues(rawInputs);
  const normalizedLogValues = buildNormalizedLogValues(normalized);
  const calcLogValues = buildCalcLogValues(canonical);
  const rsLogValues = buildRsLogValues(computedOutputs);

  setIfPresent(fields, LOG_KEY_FIELDS.CALC_LOG_KEY, calcLogKey);
  setIfPresent(fields, LOG_KEY_FIELDS.ENTRYXCLASSES_UUID, result.entryxclasses_uuid);
  setIfPresent(fields, LOG_KEY_FIELDS.WATCH_TRIP_RECORD_ID, result.recordId);
  if (LOG_KEY_FIELDS.WATCH_TRIPS_LINK && result.recordId) {
    fields[LOG_KEY_FIELDS.WATCH_TRIPS_LINK] = linkOne(result.recordId);
  }
  setIfPresent(fields, LOG_KEY_FIELDS.APP_SHOW_ID, result.app_show_id);
  setIfPresent(fields, LOG_KEY_FIELDS.APP_SQL_DATE, result.app_sql_date);
  for (const [fieldName, value] of Object.entries(rawLogValues)) {
    setIfPresent(fields, fieldName, value);
  }
  for (const [fieldName, value] of Object.entries(normalizedLogValues)) {
    setIfPresent(fields, fieldName, value);
  }
  for (const [fieldName, value] of Object.entries(calcLogValues)) {
    setIfPresent(fields, fieldName, value);
  }
  for (const [fieldName, value] of Object.entries(rsLogValues)) {
    setIfPresent(fields, fieldName, value);
  }
  setIfPresent(fields, LOG_KEY_FIELDS.CALC_MODE, CALC_MODE);
  setIfPresent(fields, LOG_KEY_FIELDS.CALC_VERSION, CALC_VERSION);
  setIfPresent(fields, LOG_KEY_FIELDS.CALC_STATUS, status);
  setIfPresent(fields, LOG_KEY_FIELDS.SKIP_REASON, skipReason || undefined);
  setIfPresent(fields, LOG_KEY_FIELDS.CHANGED_FIELDS, changedFields || undefined);
  setIfPresent(fields, LOG_JSON_FIELDS.INPUTS_JSON, jsonForField(result.inputsForLog));
  setIfPresent(fields, LOG_JSON_FIELDS.PRIOR_OUTPUTS_JSON, jsonForField(result.priorOutputs));
  setIfPresent(fields, LOG_JSON_FIELDS.COMPUTED_OUTPUTS_JSON, jsonForField(computedOutputs));
  setIfPresent(fields, LOG_JSON_FIELDS.ANOMALIES_JSON, anomalies.length ? jsonForField(anomalies) : undefined);
  setIfPresent(fields, LOG_KEY_FIELDS.CREATED_AT, createdAt);

  return { fields };
}

async function main() {
  requireEnv("AIRTABLE_TOKEN", AIRTABLE_TOKEN);
  requireEnv("AIRTABLE_BASE_ID", AIRTABLE_BASE_ID);

  const records = await airtableList({
    table: WATCH_TABLE,
    view: WATCH_VIEW,
    maxRecords: MAX_RECORDS,
  });

  const summary = {
    calc_mode: CALC_MODE,
    calc_version: CALC_VERSION,
    dry_run: DRY_RUN,
    processed_in_view: records.length,
    eligible_rows: 0,
    skipped_rows: 0,
    changed_rows: 0,
    unchanged_rows: 0,
    anomaly_rows: 0,
    watch_trips_updates_planned: 0,
    watch_trips_patched: 0,
    watch_trips_patch_failures: 0,
    trip_logs_planned: 0,
    trip_logs_created: 0,
    trip_logs_failures: 0,
    skip_reason_counts: {},
    anomaly_samples: [],
    failed_patch_samples: [],
    failed_trip_log_samples: [],
  };

  const results = [];
  const watchTripUpdates = [];

  for (const record of records) {
    const built = buildNormalizedInputs(record);
    const values = built.values;
    const eligibility = determineEligibility(values);
    const priorOutputs = buildPriorOutputs(record.fields || {});

    const result = {
      recordId: record.id,
      entryxclasses_uuid: values.entryxclasses_uuid,
      app_show_id: values.app_show_id,
      app_sql_date: values.app_sql_date,
      inputsForLog: { raw: built.rawInputs, normalized: values },
      eligibility,
      anomalies: [...built.anomalies],
      priorOutputs,
      canonicalOutputs: null,
      computedOutputs: null,
      changedFields: [],
      patch: null,
    };

    if (!eligibility.eligible) {
      summary.skipped_rows += 1;
      for (const reason of eligibility.skipReasons) {
        summary.skip_reason_counts[reason] = (summary.skip_reason_counts[reason] || 0) + 1;
      }
      results.push(result);
      continue;
    }

    summary.eligible_rows += 1;

    const computed = computeCanonicalOutputs(values, result.anomalies);
    result.anomalies = computed.anomalies;
    result.canonicalOutputs = computed.canonical;
    result.computedOutputs = computed.outputs;

    const changed = buildChangedFields(priorOutputs, computed.outputs);
    result.changedFields = changed.changedNames;

    if (result.changedFields.length > 0) {
      summary.changed_rows += 1;
      result.patch = { id: record.id, fields: changed.patchFields };
      watchTripUpdates.push(result.patch);
    } else {
      summary.unchanged_rows += 1;
    }

    if (result.anomalies.length > 0) {
      summary.anomaly_rows += 1;
      pushSample(summary.anomaly_samples, `${result.entryxclasses_uuid || record.id}:${result.anomalies[0]}`);
    }

    results.push(result);
  }

  summary.watch_trips_updates_planned = watchTripUpdates.length;

  const patchFailureById = new Map();
  if (CALC_MODE === "promote" && !DRY_RUN && watchTripUpdates.length > 0) {
    const patchResult = await airtablePatchWithFallback({
      table: WATCH_TABLE,
      updates: watchTripUpdates,
    });

    summary.watch_trips_patched = patchResult.okRows;
    summary.watch_trips_patch_failures = patchResult.failedRows.length;

    for (const failure of patchResult.failedRows) {
      patchFailureById.set(failure.record_id, failure);
      pushSample(
        summary.failed_patch_samples,
        `${failure.record_id}:${String(failure.reason || "").slice(0, 140)}`
      );
    }
  }

  const tripLogRecords = [];
  for (const result of results) {
    const patchFailure = patchFailureById.get(result.recordId) || null;
    if (!shouldCreateLogRow(result, patchFailure)) continue;
    tripLogRecords.push(buildTripLogRecord(result, patchFailure));
  }

  summary.trip_logs_planned = tripLogRecords.length;

  if (!DRY_RUN && tripLogRecords.length > 0) {
    const createResult = await airtableCreateWithFallback({
      table: TRIP_LOGS_TABLE,
      records: tripLogRecords,
    });

    summary.trip_logs_created = createResult.okRows;
    summary.trip_logs_failures = createResult.failedRows.length;

    for (const failure of createResult.failedRows) {
      pushSample(
        summary.failed_trip_log_samples,
        `${failure.entryxclasses_uuid || "unknown"}:${String(failure.reason || "").slice(0, 140)}`
      );
    }
  }

  console.log(JSON.stringify(summary, null, 2));
}

main().catch((err) => {
  console.error(String(err?.stack || err?.message || err));
  process.exit(1);
});
