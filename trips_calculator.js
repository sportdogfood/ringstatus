/**
 * trips_calculator.js
 *
 * Downstream calculator for derived rs_* timing fields on watch_trips.
 * - reads normalized watch_trips rows
 * - computes rs_* timing outputs plus optional class-level alert tags
 * - optionally tags one class-level alert parent row per duplicated class_id
 * - patches changed outputs in promote mode
 * - writes audit rows to trip_logs
 */

const AIRTABLE_TOKEN = process.env.AIRTABLE_TOKEN || "";
const AIRTABLE_BASE_ID = process.env.AIRTABLE_BASE_ID || "";

const WATCH_TABLE = process.env.WATCH_TABLE || "watch_trips";
const WATCH_VIEW = process.env.WATCH_VIEW || "hb_targets";
const TRIP_LOGS_TABLE = process.env.TRIP_LOGS_TABLE || "trip_logs";
const TABLE_TRIGGER_TAGS = process.env.TABLE_TRIGGER_TAGS || "trigger_tags";
const MAX_RECORDS = Number(process.env.MAX_RECORDS || "500");

const HTTP_TIMEOUT_MS = Number(process.env.HTTP_TIMEOUT_MS || "20000");
const AT_RETRY_ATTEMPTS = Number(process.env.AT_RETRY_ATTEMPTS || "3");
const AT_RETRY_BASE_MS = Number(process.env.AT_RETRY_BASE_MS || "400");
const AT_RETRY_MAX_MS = Number(process.env.AT_RETRY_MAX_MS || "2000");

const DRY_RUN = String(process.env.DRY_RUN || "0") === "1";
const CALC_MODE = String(process.env.CALC_MODE || "shadow").trim().toLowerCase() === "promote"
  ? "promote"
  : "shadow";
const CALC_VERSION = String(process.env.CALC_VERSION || "trips_calculator_v1_3").trim();
const WATCH_LAST_LOG_FIELD = String(process.env.WATCH_LAST_LOG_FIELD || "last_log").trim();

function optionalFieldEnv(name) {
  return String(process.env[name] || "").trim();
}

const WATCH_FIELDS = {
  ENTRYXCLASSES_UUID: process.env.FIELD_ENTRYXCLASSES_UUID || "entryxclasses_uuid",
  APP_SHOW_ID: process.env.FIELD_APP_SHOW_ID || "app_show_id",
  APP_SQL_DATE: process.env.FIELD_APP_SQL_DATE || "app_sql_date",
  CLASS_ID: process.env.FIELD_CLASS_ID || "class_id",
  ENTRY_NUMBER: process.env.FIELD_ENTRY_NUMBER || "entry_number",
  SCHEDULE_RID: process.env.FIELD_SCHEDULE_RID || "schedule_rid",
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
  HB_SECOND_PASS_REASON: process.env.FIELD_HB_SECOND_PASS_REASON || "hb_second_pass_reason",
  ORDER_OF_GO: process.env.FIELD_ORDER_OF_GO || "order_of_go",
  CLASSSIGNUP_OOG: process.env.FIELD_CLASSSIGNUP_OOG || "classsignup_oog",
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
    RUNNING_ORDER_OF_GO_MINS_TILL: process.env.FIELD_RS_RUNNING_ORDER_OF_GO_MINS_TILL || "rs_running_order_of_go_mins_till",
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
    CLASS_ALERT_PARENT: process.env.FIELD_RS_CLASS_ALERT_PARENT || "rs_class_alert_parent",
    CLASS_ALERT_PARENT_RECORD_ID: process.env.FIELD_RS_CLASS_ALERT_PARENT_RECORD_ID || "rs_class_alert_parent_record_id",
    CLASS_ALERT_GROUP_SIZE: process.env.FIELD_RS_CLASS_ALERT_GROUP_SIZE || "rs_class_alert_group_size",
  },
};

const LOG_KEY_FIELDS = {
  CALC_LOG_KEY: process.env.LOG_FIELD_CALC_LOG_KEY || "calc_log_key",
  RS_RUN_ID: process.env.LOG_FIELD_RS_RUN_ID || "rs_run_id",
  WATCH_TRIPS_LINK: process.env.LOG_FIELD_WATCH_TRIPS_LINK || "watch_trips",
  WATCH_TRIP_RECORD_ID: process.env.LOG_FIELD_WATCH_TRIP_RECORD_ID || "watch_trip_record_id",
  WATCH_SCHEDULE_LINK: process.env.LOG_FIELD_WATCH_SCHEDULE_LINK || "watch_schedule",
  WATCH_SCHEDULE_RECORD_ID: process.env.LOG_FIELD_WATCH_SCHEDULE_RECORD_ID || "watch_schedule_record_id",
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

// Compact source snapshot copied directly from watch_trips after trips_tagger updates it.
const LOG_SOURCE_FIELDS = {
  APP_TIME: process.env.LOG_SOURCE_APP_TIME || "app_time",
  STATUS: process.env.LOG_SOURCE_STATUS || "status",
  CLASS_STATUS: process.env.LOG_SOURCE_CLASS_STATUS || "class_status",
  ESTIMATED_START_TIME: process.env.LOG_SOURCE_ESTIMATED_START_TIME || "estimated_start_time",
  ESTIMATED_END_TIME: process.env.LOG_SOURCE_ESTIMATED_END_TIME || "estimated_end_time",
  ACTUAL_TIME: process.env.LOG_SOURCE_ACTUAL_TIME || "actual_time",
  ESTIMATED_TIME: process.env.LOG_SOURCE_ESTIMATED_TIME || "estimated_time",
  ESTIMATED_GO_TIME: process.env.LOG_SOURCE_ESTIMATED_GO_TIME || "estimated_go_time",
  ORDER_OF_GO: process.env.LOG_SOURCE_ORDER_OF_GO || "order_of_go",
  ACTUAL_ORDER: process.env.LOG_SOURCE_ACTUAL_ORDER || "actual_order",
  ACTUAL_GO: optionalFieldEnv("LOG_SOURCE_ACTUAL_GO"),
  GONE_IN: process.env.LOG_SOURCE_GONE_IN || "gone_in",
  REMAINING_TRIPS: process.env.LOG_SOURCE_REMAINING_TRIPS || "remaining_trips",
  TOTAL_TRIPS: process.env.LOG_SOURCE_TOTAL_TRIPS || "total_trips",
  COMPLETED_TRIPS: process.env.LOG_SOURCE_COMPLETED_TRIPS || "completed_trips",
  H_EID: process.env.LOG_SOURCE_H_EID || "h_eid",
};

// Legacy duplicate namespaces like raw.*, raw_*, normalized.*, and norm_* are opt-in only.
const LOG_RAW_FIELDS = {
  APP_TIME: optionalFieldEnv("LOG_RAW_APP_TIME"),
  STATUS: optionalFieldEnv("LOG_RAW_STATUS"),
  CLASS_STATUS: optionalFieldEnv("LOG_RAW_CLASS_STATUS"),
  ESTIMATED_START_TIME: optionalFieldEnv("LOG_RAW_ESTIMATED_START_TIME"),
  ESTIMATED_END_TIME: optionalFieldEnv("LOG_RAW_ESTIMATED_END_TIME"),
  ACTUAL_TIME: optionalFieldEnv("LOG_RAW_ACTUAL_TIME"),
  ESTIMATED_TIME: optionalFieldEnv("LOG_RAW_ESTIMATED_TIME"),
  ESTIMATED_GO_TIME: optionalFieldEnv("LOG_RAW_ESTIMATED_GO_TIME"),
  ORDER_OF_GO: optionalFieldEnv("LOG_RAW_ORDER_OF_GO"),
  ACTUAL_ORDER: optionalFieldEnv("LOG_RAW_ACTUAL_ORDER"),
  ACTUAL_GO: optionalFieldEnv("LOG_RAW_ACTUAL_GO"),
  GONE_IN: optionalFieldEnv("LOG_RAW_GONE_IN"),
  REMAINING_TRIPS: optionalFieldEnv("LOG_RAW_REMAINING_TRIPS"),
  TOTAL_TRIPS: optionalFieldEnv("LOG_RAW_TOTAL_TRIPS"),
  COMPLETED_TRIPS: optionalFieldEnv("LOG_RAW_COMPLETED_TRIPS"),
  H_EID: optionalFieldEnv("LOG_RAW_H_EID"),
};

const LOG_NORM_FIELDS = {
  CLASS_STATUS: optionalFieldEnv("LOG_NORM_CLASS_STATUS"),
  APP_TIME_TEXT: optionalFieldEnv("LOG_NORM_APP_TIME_TEXT"),
  ESTIMATED_START_TIME_TEXT: optionalFieldEnv("LOG_NORM_ESTIMATED_START_TIME_TEXT"),
  ESTIMATED_END_TIME_TEXT: optionalFieldEnv("LOG_NORM_ESTIMATED_END_TIME_TEXT"),
  ACTUAL_TIME_TEXT: optionalFieldEnv("LOG_NORM_ACTUAL_TIME_TEXT"),
  ESTIMATED_TIME_TEXT: optionalFieldEnv("LOG_NORM_ESTIMATED_TIME_TEXT"),
  ESTIMATED_GO_TIME_TEXT: optionalFieldEnv("LOG_NORM_ESTIMATED_GO_TIME_TEXT"),
  H_EID: optionalFieldEnv("LOG_NORM_H_EID"),
  APP_TIME_MINS: optionalFieldEnv("LOG_NORM_APP_TIME_MINS"),
  ESTIMATED_START_TIME_MINS: optionalFieldEnv("LOG_NORM_ESTIMATED_START_TIME_MINS"),
  ESTIMATED_END_TIME_MINS: optionalFieldEnv("LOG_NORM_ESTIMATED_END_TIME_MINS"),
  ACTUAL_TIME_MINS: optionalFieldEnv("LOG_NORM_ACTUAL_TIME_MINS"),
  ESTIMATED_TIME_MINS: optionalFieldEnv("LOG_NORM_ESTIMATED_TIME_MINS"),
  ESTIMATED_GO_TIME_MINS: optionalFieldEnv("LOG_NORM_ESTIMATED_GO_TIME_MINS"),
  ORDER_OF_GO: optionalFieldEnv("LOG_NORM_ORDER_OF_GO"),
  ACTUAL_ORDER: optionalFieldEnv("LOG_NORM_ACTUAL_ORDER"),
  ACTUAL_GO: optionalFieldEnv("LOG_NORM_ACTUAL_GO"),
  GONE_IN: optionalFieldEnv("LOG_NORM_GONE_IN"),
  REMAINING_TRIPS: optionalFieldEnv("LOG_NORM_REMAINING_TRIPS"),
  TOTAL_TRIPS: optionalFieldEnv("LOG_NORM_TOTAL_TRIPS"),
  COMPLETED_TRIPS: optionalFieldEnv("LOG_NORM_COMPLETED_TRIPS"),
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

const LOG_DECISION_FIELDS = {
  RUNNING_ORDER_IS_TEN_OUT: process.env.LOG_FIELD_RUNNING_ORDER_IS_TEN_OUT || "running_order_is_ten_out",
};

// trip_logs stores audit copies of computed rs_* outputs. These are not raw source
// fields, and some trip_logs column types intentionally differ from watch_trips.
const LOG_RS_FIELDS = {
  STATUS: process.env.LOG_RS_STATUS || "rs_status",
  START_TIME: process.env.LOG_RS_START_TIME || "rs_start_time",
  GO_TIME: process.env.LOG_RS_GO_TIME || "rs_go_time",
  COMPLETED_TRIPS: process.env.LOG_RS_COMPLETED_TRIPS || "rs_completed_trips",
  GONE_IN: process.env.LOG_RS_GONE_IN || "rs_gone_in",
  TRIP_DEFAULT: process.env.LOG_RS_TRIP_DEFAULT || "rs_trip_default",
  ORDER_OF_GO: process.env.LOG_RS_ORDER_OF_GO || "rs_order_of_go",
  RUNNING_ORDER_OF_GO: process.env.LOG_RS_RUNNING_ORDER_OF_GO || "rs_running_order_of_go",
  RUNNING_ORDER_OF_GO_MINS_TILL: process.env.LOG_RS_RUNNING_ORDER_OF_GO_MINS_TILL || "rs_running_order_of_go_mins_till",
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
// Reject pace candidates outside the operational band and fall back to default.
const TRIP_MINUTES_MIN = Number(process.env.TRIP_MINUTES_MIN || "1.8");
const TRIP_MINUTES_MAX = Number(process.env.TRIP_MINUTES_MAX || "3.8");

const WATCH_SOURCE_FIELDS = [
  WATCH_FIELDS.ENTRYXCLASSES_UUID,
  WATCH_FIELDS.APP_SHOW_ID,
  WATCH_FIELDS.APP_SQL_DATE,
  WATCH_FIELDS.CLASS_ID,
  WATCH_FIELDS.ENTRY_NUMBER,
  WATCH_FIELDS.SCHEDULE_RID,
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
  WATCH_FIELDS.HB_SECOND_PASS_REASON,
  WATCH_FIELDS.ORDER_OF_GO,
  WATCH_FIELDS.CLASSSIGNUP_OOG,
  WATCH_FIELDS.ACTUAL_ORDER,
  WATCH_FIELDS.ACTUAL_GO,
  WATCH_FIELDS.GONE_IN,
  WATCH_FIELDS.H_EID,
];

const WATCH_OUTPUT_FIELDS = Object.values(WATCH_FIELDS.RS);

const OUTPUT_DURATION_FIELDS = new Set([
  WATCH_FIELDS.RS.TRIP_DEFAULT,
  WATCH_FIELDS.RS.TRIP_TIME,
  WATCH_FIELDS.RS.TRIP_TIME2,
]);

const OUTPUT_NUMBER_FIELDS = new Set([
  WATCH_FIELDS.RS.COMPLETED_TRIPS,
  WATCH_FIELDS.RS.GONE_IN,
  WATCH_FIELDS.RS.ORDER_OF_GO,
  WATCH_FIELDS.RS.RUNNING_ORDER_OF_GO,
  WATCH_FIELDS.RS.RUNNING_ORDER_OF_GO_MINS_TILL,
  WATCH_FIELDS.RS.MINS_TILL_START,
  WATCH_FIELDS.RS.MINS_SINCE_START,
  WATCH_FIELDS.RS.GO_MINS_FROM_START,
  WATCH_FIELDS.RS.MIN_TILL_GO,
  WATCH_FIELDS.RS.MIN_TO_ACTUAL_GO,
  WATCH_FIELDS.RS.CLASS_ALERT_GROUP_SIZE,
]);

const OUTPUT_TEXT_FIELDS = new Set([
  WATCH_FIELDS.RS.STATUS,
  WATCH_FIELDS.RS.START_TIME,
  WATCH_FIELDS.RS.GO_TIME,
  WATCH_FIELDS.RS.LENGTH,
  WATCH_FIELDS.RS.END_TIME,
  WATCH_FIELDS.RS.GO_TIME_FROM_START,
  WATCH_FIELDS.RS.CLASS_ALERT_PARENT_RECORD_ID,
]);

const RS_CONDITIONAL_CLEAR_FIELDS = new Set([
  WATCH_FIELDS.RS.MIN_TILL_GO,
  WATCH_FIELDS.RS.MIN_TO_ACTUAL_GO,
  WATCH_FIELDS.RS.MINS_SINCE_START,
  WATCH_FIELDS.RS.MINS_TILL_START,
  WATCH_FIELDS.RS.ORDER_OF_GO,
  WATCH_FIELDS.RS.RUNNING_ORDER_OF_GO,
  WATCH_FIELDS.RS.GO_MINS_FROM_START,
  WATCH_FIELDS.RS.GO_TIME_FROM_START,
]);

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

function boolValue(value) {
  if (value === true || value === 1) return true;
  if (value === false || value === 0 || value === null || value === undefined) return false;
  const text = String(value).trim().toLowerCase();
  return text === "true" || text === "1" || text === "yes" || text === "checked";
}

function normalizeKey(value) {
  return strOrNull(value) || "";
}

function compareTriggerValue(left, right) {
  const leftNum = numOrNull(left);
  const rightNum = numOrNull(right);
  if (leftNum !== null && rightNum !== null) return leftNum === rightNum;
  return normalizeKey(left).toLowerCase() === normalizeKey(right).toLowerCase();
}

function parseTriggerNumber(value) {
  const num = numOrNull(value);
  return num !== null ? num : null;
}

function fieldsHasTruthy(fields, fieldName) {
  if (!fieldName) return false;
  return boolValue(fields?.[fieldName]);
}

function normalizeTriggerFieldAlias(fieldName) {
  const text = strOrNull(fieldName);
  if (!text) return null;
  if (text === "rs_mins_till_go") return "rs_min_till_go";
  return text;
}

function chunk(items, size) {
  const out = [];
  for (let i = 0; i < items.length; i += size) out.push(items.slice(i, i + size));
  return out;
}

function parseAirtableError(bodyText) {
  const body = String(bodyText || "");
  let json = null;
  try {
    json = JSON.parse(body);
  } catch (_) {}

  const type = String(json?.error?.type || "").trim();
  const message = String(json?.error?.message || body || "").trim();
  return { type, message, body };
}

function extractUnknownFieldName(err) {
  const status = Number(err?._airtable_status);
  const type = String(err?._airtable_type || "").trim().toUpperCase();
  const message = String(err?._airtable_message || err?.message || "");
  if (status !== 422 || type !== "UNKNOWN_FIELD_NAME") return null;
  const match = message.match(/Unknown field name:\s*"([^"]+)"/i);
  return match ? match[1] : null;
}

function stripFieldFromRecords(records, fieldName) {
  return records.map((record) => {
    const fields = { ...(record?.fields || {}) };
    delete fields[fieldName];
    return { ...record, fields };
  });
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

function normalizeClassIdValue(value) {
  if (value && typeof value === "object") {
    const nested = firstNonBlank(value.id, value.value, value.name, value.label);
    return strOrNull(nested);
  }
  return strOrNull(value);
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

function isCompleteStatus(value) {
  const text = String(value || "").trim().toLowerCase();
  return !!text && /complete(d)?/.test(text);
}

function compareNullableNumbers(left, right) {
  const hasLeft = Number.isFinite(left);
  const hasRight = Number.isFinite(right);
  if (hasLeft && hasRight) {
    if (left === right) return 0;
    return left < right ? -1 : 1;
  }
  if (hasLeft) return -1;
  if (hasRight) return 1;
  return 0;
}

function compareNullableText(left, right) {
  const a = strOrNull(left);
  const b = strOrNull(right);
  if (a && b) return a.localeCompare(b);
  if (a) return -1;
  if (b) return 1;
  return 0;
}

function positiveDurationMinutes(laterMinutes, earlierMinutes) {
  if (!Number.isFinite(laterMinutes) || !Number.isFinite(earlierMinutes)) return null;
  const diff = roundNumber(laterMinutes - earlierMinutes, 6);
  return diff !== null && diff > 0 ? diff : null;
}

function minutesPerUnit(totalMinutes, units) {
  if (!Number.isFinite(totalMinutes) || totalMinutes <= 0) return null;
  if (!Number.isFinite(units) || units <= 0) return null;
  return roundNumber(totalMinutes / units, 6);
}

function isUsableTripMinutes(value) {
  return Number.isFinite(value) && value >= TRIP_MINUTES_MIN && value <= TRIP_MINUTES_MAX;
}

function deriveTripMinutes(values, context) {
  const candidates = [];
  const startAnchorMinutes = context?.startAnchorMinutes ?? null;
  const effectiveOrder = context?.effectiveOrder ?? null;
  const classIsComplete = isCompleteStatus(values.class_status);
  const hasStarted =
    Number.isFinite(startAnchorMinutes) &&
    Number.isFinite(values.app_time_minutes) &&
    values.app_time_minutes >= startAnchorMinutes;

  const elapsedFromStart = minutesPerUnit(
    positiveDurationMinutes(values.app_time_minutes, startAnchorMinutes),
    values.completed_trips
  );
  if (elapsedFromStart !== null) {
    candidates.push({ source: "elapsed_from_start", minutes: elapsedFromStart });
  }

  const remainingToEstimatedEnd = hasStarted
    ? minutesPerUnit(
      positiveDurationMinutes(values.estimated_end_time_minutes, values.app_time_minutes),
      values.remaining_trips
    )
    : null;
  if (remainingToEstimatedEnd !== null) {
    candidates.push({ source: "remaining_to_estimated_end", minutes: remainingToEstimatedEnd });
  }

  const classWindow = minutesPerUnit(
    positiveDurationMinutes(values.estimated_end_time_minutes, startAnchorMinutes),
    values.total_trips
  );
  if (classWindow !== null) {
    candidates.push({ source: "class_window", minutes: classWindow });
  }

  const estimatedGoWindow = minutesPerUnit(
    positiveDurationMinutes(values.estimated_go_time_minutes, startAnchorMinutes),
    effectiveOrder !== null ? Math.max(effectiveOrder - 1, 0) : null
  );
  if (estimatedGoWindow !== null) {
    candidates.push({ source: "estimated_go_window", minutes: estimatedGoWindow });
  }

  const preferredSources = classIsComplete
    ? ["class_window", "elapsed_from_start", "remaining_to_estimated_end", "estimated_go_window"]
    : ["elapsed_from_start", "remaining_to_estimated_end", "class_window", "estimated_go_window"];

  for (const source of preferredSources) {
    const candidate = candidates.find((item) => item.source === source);
    if (candidate && isUsableTripMinutes(candidate.minutes)) {
      return {
        minutes: candidate.minutes,
        source: candidate.source,
        usedDefault: false,
        rejectedMinutes: null,
        rejectedSource: null,
      };
    }
  }

  const firstCandidate = candidates.find((item) => Number.isFinite(item.minutes) && item.minutes > 0) || null;
  return {
    minutes: TRIP_MINUTES_DEFAULT,
    source: "default",
    usedDefault: true,
    rejectedMinutes: firstCandidate?.minutes ?? null,
    rejectedSource: firstCandidate?.source ?? null,
  };
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

let airtableMetaTablesPromise = null;

async function airtableMetaTables() {
  if (!airtableMetaTablesPromise) {
    airtableMetaTablesPromise = (async () => {
      const res = await fetchWithRetry(`https://api.airtable.com/v0/meta/bases/${AIRTABLE_BASE_ID}/tables`, {
        headers: { Authorization: `Bearer ${AIRTABLE_TOKEN}` }
      });

      if (!res.ok) {
        const body = await res.text().catch(() => "");
        throw new Error(`Airtable meta failed (${res.status}): ${body}`);
      }

      const json = await res.json().catch(() => ({}));
      return Array.isArray(json?.tables) ? json.tables : [];
    })();
  }

  return airtableMetaTablesPromise;
}

async function airtableTableFieldMeta(tableName) {
  const tables = await airtableMetaTables();
  const table = tables.find((item) => String(item?.name || "").trim() === tableName) || null;
  const out = new Map();

  for (const field of Array.isArray(table?.fields) ? table.fields : []) {
    const fieldName = String(field?.name || "").trim();
    if (!fieldName) continue;
    out.set(fieldName, {
      name: fieldName,
      type: String(field?.type || "").trim(),
    });
  }

  return out;
}

async function airtableTableFieldSet(tableName) {
  const fieldMeta = await airtableTableFieldMeta(tableName);
  return new Set(fieldMeta.keys());
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

async function fetchActiveTriggerTags() {
  const fieldSet = await airtableTableFieldSet(TABLE_TRIGGER_TAGS);
  const requestedFields = [
    "trigger_name",
    "trigger_lane",
    "trigger_type",
    "source_table",
    "source_view",
    "is_active",
    "query",
    "this_field",
    "argument",
    "from",
    "to",
    "output_field",
    "priority",
  ].filter((fieldName) => fieldSet.has(fieldName));

  const rows = await airtableList({
    table: TABLE_TRIGGER_TAGS,
    fields: requestedFields,
    maxRecords: MAX_RECORDS,
  });

  return rows
    .filter((row) => fieldsHasTruthy(row?.fields || {}, "is_active"))
    .map((row) => {
      const fields = row?.fields || {};
      return {
        recordId: row.id,
        trigger_name: strOrNull(fields.trigger_name),
        trigger_lane: strOrNull(fields.trigger_lane),
        trigger_type: strOrNull(fields.trigger_type),
        source_table: strOrNull(fields.source_table),
        source_view: strOrNull(fields.source_view),
        query: strOrNull(fields.query),
        this_field: normalizeTriggerFieldAlias(fields.this_field),
        argument: strOrNull(fields.argument),
        from: firstNonBlank(fields.from, fields.From),
        to: firstNonBlank(fields.to, fields.To),
        output_field: strOrNull(fields.output_field),
        priority: numOrNull(fields.priority) ?? 999,
      };
    })
    .filter((row) => normalizeKey(row.source_table).toLowerCase() === normalizeKey(TRIP_LOGS_TABLE).toLowerCase())
    .sort((a, b) => a.priority - b.priority || String(a.trigger_name || "").localeCompare(String(b.trigger_name || "")));
}

async function fetchPriorTripLogMap(triggerTags) {
  const requestedFields = new Set([LOG_KEY_FIELDS.ENTRYXCLASSES_UUID, LOG_KEY_FIELDS.CREATED_AT]);
  requestedFields.add(LOG_RS_FIELDS.START_TIME);
  requestedFields.add(LOG_RS_FIELDS.GO_TIME);
  requestedFields.add(LOG_RS_FIELDS.ORDER_OF_GO);
  for (const trigger of triggerTags || []) {
    const fieldName = strOrNull(trigger?.this_field);
    if (fieldName) requestedFields.add(fieldName);
    const outputField = strOrNull(trigger?.output_field);
    if (outputField) requestedFields.add(outputField);
  }

  const rows = await airtableList({
    table: TRIP_LOGS_TABLE,
    fields: Array.from(requestedFields),
    maxRecords: Math.max(MAX_RECORDS * 10, 1000),
  });

  rows.sort((left, right) => {
    const leftCreated = Date.parse(String(left?.fields?.[LOG_KEY_FIELDS.CREATED_AT] || "")) || 0;
    const rightCreated = Date.parse(String(right?.fields?.[LOG_KEY_FIELDS.CREATED_AT] || "")) || 0;
    return rightCreated - leftCreated;
  });

  const byUuid = new Map();
  for (const row of rows) {
    const entryxclassesUuid = strOrNull(row?.fields?.[LOG_KEY_FIELDS.ENTRYXCLASSES_UUID]);
    if (!entryxclassesUuid || byUuid.has(entryxclassesUuid)) continue;
    byUuid.set(entryxclassesUuid, row.fields || {});
  }

  return byUuid;
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
  if (!records.length) return [];

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
    const parsed = parseAirtableError(body);
    const err = new Error(`Airtable create failed (${res.status}) ${table}: ${body}`);
    err._airtable_status = res.status;
    err._airtable_type = parsed.type;
    err._airtable_message = parsed.message;
    err._airtable_body = parsed.body;
    throw err;
  }

  const body = await res.text().catch(() => "");
  let json = {};
  try {
    json = body ? JSON.parse(body) : {};
  } catch (_) {
    json = {};
  }
  return Array.isArray(json?.records) ? json.records : [];
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
  if (!records.length) return { okRows: 0, failedRows: [], createdRows: [] };

  let okRows = 0;
  const failedRows = [];
  const createdRows = [];
  const ignoredUnknownFields = new Set();

  for (const originalBatch of chunk(records, 10)) {
    let batch = originalBatch;
    for (const fieldName of ignoredUnknownFields) {
      batch = stripFieldFromRecords(batch, fieldName);
    }

    try {
      while (true) {
        try {
          const created = await airtableCreateRecords({ table, records: batch });
          okRows += batch.length;
          if (Array.isArray(created) && created.length > 0) {
            createdRows.push(...created);
          }
          break;
        } catch (batchErr) {
          const unknownField = extractUnknownFieldName(batchErr);
          if (!unknownField) throw batchErr;
          if (ignoredUnknownFields.has(unknownField)) throw batchErr;
          ignoredUnknownFields.add(unknownField);
          console.log(`create warn: ignoring unknown trip_logs field "${unknownField}" and retrying batch`);
          batch = stripFieldFromRecords(batch, unknownField);
        }
      }
    } catch (batchErr) {
      console.log(`create warn: batch failed, falling back :: ${String(batchErr?.message || batchErr).slice(0, 300)}`);
      for (const originalRow of batch) {
        let row = originalRow;
        for (const fieldName of ignoredUnknownFields) {
          row = stripFieldFromRecords([row], fieldName)[0];
        }
        try {
          while (true) {
            try {
              const created = await airtableCreateRecords({ table, records: [row] });
              okRows += 1;
              if (Array.isArray(created) && created.length > 0) {
                createdRows.push(...created);
              }
              break;
            } catch (rowErr) {
              const unknownField = extractUnknownFieldName(rowErr);
              const rowFields = row?.fields || {};
              if (!unknownField || !Object.prototype.hasOwnProperty.call(rowFields, unknownField)) {
                throw rowErr;
              }
              ignoredUnknownFields.add(unknownField);
              console.log(`create warn: ignoring unknown trip_logs field "${unknownField}" and retrying row`);
              row = stripFieldFromRecords([row], unknownField)[0];
            }
          }
        } catch (rowErr) {
          failedRows.push({
            entryxclasses_uuid: row?.fields?.[LOG_KEY_FIELDS.ENTRYXCLASSES_UUID] || "",
            reason: String(rowErr?.message || rowErr).slice(0, 300),
          });
        }
      }
    }
  }

  return { okRows, failedRows, createdRows };
}

function buildRawInputs(fields) {
  return {
    entryxclasses_uuid: fields[WATCH_FIELDS.ENTRYXCLASSES_UUID],
    app_show_id: fields[WATCH_FIELDS.APP_SHOW_ID],
    app_sql_date: fields[WATCH_FIELDS.APP_SQL_DATE],
    class_id: fields[WATCH_FIELDS.CLASS_ID],
    entry_number: fields[WATCH_FIELDS.ENTRY_NUMBER],
    schedule_rid: fields[WATCH_FIELDS.SCHEDULE_RID],
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
    hb_second_pass_reason: fields[WATCH_FIELDS.HB_SECOND_PASS_REASON],
    order_of_go: fields[WATCH_FIELDS.ORDER_OF_GO],
    classsignup_oog: fields[WATCH_FIELDS.CLASSSIGNUP_OOG],
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
      class_id: normalizeClassIdValue(rawInputs.class_id),
      class_status: normalizeClassStatusValue(
        rawInputs.status,
        rawInputs.class_status
      ),
      remaining_trips: normalizeCountValue(rawInputs.remaining_trips),
      total_trips: normalizeCountValue(rawInputs.total_trips),
      completed_trips: normalizeCountValue(rawInputs.completed_trips),
      order_of_go: normalizeOrderValue(firstNonBlank(rawInputs.order_of_go, rawInputs.classsignup_oog)),
      actual_order: normalizeOrderValue(rawInputs.actual_order),
      actual_go: normalizeOrderValue(rawInputs.actual_go),
      gone_in: normalizeCountValue(rawInputs.gone_in),
      h_eid: strOrNull(firstNonBlank(rawInputs.h_eid, rawInputs.entry_number)),
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

function deriveEffectiveOrder(values) {
  return firstNonBlank(values.actual_order, values.actual_go, values.order_of_go);
}

function compareClassAlertCandidates(left, right) {
  return compareNullableNumbers(left.effectiveOrder, right.effectiveOrder) ||
    compareNullableNumbers(left.estimatedGoTimeMinutes, right.estimatedGoTimeMinutes) ||
    compareNullableNumbers(left.appTimeMinutes, right.appTimeMinutes) ||
    compareNullableText(left.entryxclasses_uuid, right.entryxclasses_uuid) ||
    compareNullableText(left.recordId, right.recordId);
}

function buildClassAlertAssignments(preparedRows) {
  const groups = new Map();

  for (const prepared of preparedRows) {
    const classId = prepared?.values?.class_id;
    if (!classId) continue;

    if (!groups.has(classId)) groups.set(classId, []);
    groups.get(classId).push({
      recordId: prepared.record.id,
      entryxclasses_uuid: prepared.values.entryxclasses_uuid,
      effectiveOrder: deriveEffectiveOrder(prepared.values),
      estimatedGoTimeMinutes: prepared.values.estimated_go_time_minutes,
      appTimeMinutes: prepared.values.app_time_minutes,
      eligible: !!prepared.eligibility?.eligible,
    });
  }

  const assignments = new Map();
  let duplicatedClassCount = 0;
  let parentRowCount = 0;

  for (const [classId, rows] of groups.entries()) {
    if (!Array.isArray(rows) || rows.length === 0) continue;

    const orderedRows = rows.slice().sort(compareClassAlertCandidates);
    const eligibleRows = orderedRows.filter((row) => row.eligible);
    // Prefer the earliest eligible trip in the class as the alert parent.
    const chosenParent = eligibleRows[0] || orderedRows[0];
    const groupSize = orderedRows.length;
    const hasPeers = groupSize > 1;

    if (hasPeers) duplicatedClassCount += 1;

    for (const row of orderedRows) {
      const isParent = hasPeers && row.recordId === chosenParent.recordId;
      if (isParent) parentRowCount += 1;

      assignments.set(row.recordId, {
        classId,
        groupSize,
        hasPeers,
        isParent,
        parentRecordId: hasPeers ? chosenParent.recordId : null,
      });
    }
  }

  return {
    assignments,
    classCount: groups.size,
    duplicatedClassCount,
    parentRowCount,
  };
}

function determineEligibility(values) {
  const skipReasons = [];

  if (!values.entryxclasses_uuid) skipReasons.push("missing_entryxclasses_uuid");
  if (!values.h_eid) skipReasons.push("missing_h_eid");

  return {
    eligible: skipReasons.length === 0,
    skipReasons,
  };
}

function computeCanonicalOutputs(values, priorAnomalies = [], context = {}) {
  const anomalies = [...priorAnomalies];

  const tripMinutesDefault = TRIP_MINUTES_DEFAULT;
  const startAnchorMinutes = firstNonBlank(values.actual_time_minutes, values.estimated_start_time_minutes);
  const startAnchorText = firstNonBlank(values.actual_time_text, values.estimated_start_time_text);
  const effectiveOrder = deriveEffectiveOrder(values);
  const classAlert = context.classAlert || {};
  const classAlertParent = classAlert.isParent === true;
  const classAlertParentRecordId = strOrNull(classAlert.parentRecordId);
  const classAlertGroupSize = Number.isFinite(classAlert.groupSize)
    ? Math.max(0, Math.trunc(classAlert.groupSize))
    : null;
  const rawRunningOrder = (
    effectiveOrder !== null && values.completed_trips !== null
      ? roundNumber(effectiveOrder - values.completed_trips, 6)
      : null
  );
  const shouldBlankRunningOrder = values.gone_in === 1;
  const runningOrder = (
    shouldBlankRunningOrder || rawRunningOrder === null || rawRunningOrder < 0
      ? null
      : rawRunningOrder
  );

  if (!shouldBlankRunningOrder && rawRunningOrder !== null && rawRunningOrder < 0) {
    anomalies.push("negative_running_order");
  }

  const minutesUntilStart = (
    startAnchorMinutes !== null && values.app_time_minutes !== null
      ? roundNumber(startAnchorMinutes - values.app_time_minutes, 6)
      : null
  );

  const minutesSinceStart = (
    startAnchorMinutes !== null && values.app_time_minutes !== null
      ? roundNumber(values.app_time_minutes - startAnchorMinutes, 6)
      : null
  );

  const derivedTripMinutes = deriveTripMinutes(values, {
    startAnchorMinutes,
    effectiveOrder,
  });
  const rawTripMinutes = derivedTripMinutes.usedDefault ? derivedTripMinutes.rejectedMinutes : derivedTripMinutes.minutes;
  const tripMinutesUsedDefault = derivedTripMinutes.usedDefault;
  const tripMinutesSource = derivedTripMinutes.source;

  let tripMinutes = derivedTripMinutes.minutes;
  if (tripMinutesUsedDefault) {
    anomalies.push(rawTripMinutes === null ? "trip_minutes_default_missing_rate" : "trip_minutes_default_out_of_range");
    tripMinutes = tripMinutesDefault;
  }

  const tripDefaultDurationSeconds = durationSecondsFromMinutes(tripMinutesDefault);
  const tripDurationSeconds = durationSecondsFromMinutes(tripMinutes);
  const projectedClassMinutes = (
    values.total_trips !== null
      ? roundNumber(values.total_trips * tripMinutes, 6)
      : null
  );

  if (startAnchorMinutes === null) anomalies.push("missing_start_anchor");
  if (effectiveOrder === null) anomalies.push("missing_effective_order");

  const projectedEndMinutes = (
    startAnchorMinutes !== null && projectedClassMinutes !== null
      ? roundNumber(startAnchorMinutes + projectedClassMinutes, 6)
      : null
  );
  const projectedEndClock = formatClockFromMinutes(projectedEndMinutes);
  const runningOrderMinsTill = (
    runningOrder !== null
      ? roundNumber(Math.max(runningOrder - 1, 0) * tripMinutes, 6)
      : null
  );
  const runningOrderIsTenOut = (
    effectiveOrder !== null &&
    effectiveOrder >= 14 &&
    runningOrder !== null &&
    runningOrder >= 9 &&
    runningOrder <= 13
  );

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
    running_order_mins_till: runningOrderMinsTill,
    running_order_is_ten_out: runningOrderIsTenOut,
    minutes_until_start: minutesUntilStart,
    minutes_since_start: minutesSinceStart,
    raw_trip_minutes: rawTripMinutes,
    trip_minutes: tripMinutes,
    trip_minutes_final: tripMinutes,
    trip_minutes_source: tripMinutesSource,
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
    class_alert_parent: classAlertParent,
    class_alert_parent_record_id: classAlertParentRecordId,
    class_alert_group_size: classAlertGroupSize,
  };

  const outputs = {
    [WATCH_FIELDS.RS.STATUS]: values.class_status,
    [WATCH_FIELDS.RS.START_TIME]: startAnchorText,
    [WATCH_FIELDS.RS.GO_TIME]: goClockFromStart,
    [WATCH_FIELDS.RS.COMPLETED_TRIPS]: values.completed_trips,
    [WATCH_FIELDS.RS.GONE_IN]: values.gone_in,
    [WATCH_FIELDS.RS.TRIP_DEFAULT]: tripDefaultDurationSeconds,
    [WATCH_FIELDS.RS.ORDER_OF_GO]: effectiveOrder,
    [WATCH_FIELDS.RS.RUNNING_ORDER_OF_GO]: runningOrder,
    [WATCH_FIELDS.RS.RUNNING_ORDER_OF_GO_MINS_TILL]: runningOrderMinsTill,
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
    [WATCH_FIELDS.RS.CLASS_ALERT_PARENT]: classAlertParent,
    [WATCH_FIELDS.RS.CLASS_ALERT_PARENT_RECORD_ID]: classAlertParentRecordId,
    [WATCH_FIELDS.RS.CLASS_ALERT_GROUP_SIZE]: classAlertGroupSize,
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

function statusAllowsConditionalClear(value) {
  const text = String(value || "").trim().toLowerCase();
  return text === "complete" || text === "completed";
}

function shouldPreserveConditionalRsValue(fieldName, prevValue, nextValue, computedOutputs) {
  if (!RS_CONDITIONAL_CLEAR_FIELDS.has(fieldName)) return false;
  if (prevValue === null || prevValue === undefined) return false;
  if (nextValue !== null) return false;

  const goneIn = numOrNull(computedOutputs?.[WATCH_FIELDS.RS.GONE_IN]);
  const rsStatus = computedOutputs?.[WATCH_FIELDS.RS.STATUS];
  const allowClear = goneIn === 1 || statusAllowsConditionalClear(rsStatus);

  return !allowClear;
}

function buildPriorOutputs(fields, outputFieldNames = WATCH_OUTPUT_FIELDS) {
  const out = {};
  for (const fieldName of outputFieldNames) {
    out[fieldName] = normalizeOutputValue(fieldName, fields[fieldName]);
  }
  return out;
}

function buildChangedFields(priorOutputs, computedOutputs, outputFieldNames = WATCH_OUTPUT_FIELDS) {
  const changedNames = [];
  const patchFields = {};

  for (const fieldName of outputFieldNames) {
    const normalizedNext = normalizeOutputValue(fieldName, computedOutputs[fieldName]);
    const prevValue = normalizeOutputValue(fieldName, priorOutputs[fieldName]);
    const nextValue = shouldPreserveConditionalRsValue(
      fieldName,
      prevValue,
      normalizedNext,
      computedOutputs
    )
      ? prevValue
      : normalizedNext;
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

function formatDiffDisplayValue(value) {
  if (value === undefined || value === null) return "blank";
  if (typeof value === "string") {
    const text = value.trim();
    return text === "" ? "blank" : text;
  }
  if (typeof value === "number") {
    return Number.isFinite(value) ? String(roundNumber(value, 6)) : "blank";
  }
  if (typeof value === "boolean") return value ? "true" : "false";
  return String(value);
}

function buildDiffText(priorValue, currentValue) {
  const left = priorValue ?? null;
  const right = currentValue ?? null;
  if (left === right) return null;
  return `${formatDiffDisplayValue(priorValue)} -> ${formatDiffDisplayValue(currentValue)}`;
}

function buildNumericDiffValue(priorValue, currentValue) {
  const leftTime = parseClockToMinutes(priorValue);
  const rightTime = parseClockToMinutes(currentValue);
  if (leftTime !== null && rightTime !== null) {
    const delta = roundNumber(rightTime - leftTime, 6);
    return Math.abs(delta) < 0.000001 ? null : delta;
  }

  const leftNum = numOrNull(priorValue);
  const rightNum = numOrNull(currentValue);
  if (leftNum === null || rightNum === null) return null;

  const delta = roundNumber(rightNum - leftNum, 6);
  return Math.abs(delta) < 0.000001 ? null : delta;
}

function buildDiffValueForField(priorValue, currentValue, fieldMeta) {
  const fieldType = String(fieldMeta?.type || "").trim().toLowerCase();
  if (fieldType === "number" || fieldType === "currency" || fieldType === "percent" || fieldType === "rating") {
    return buildNumericDiffValue(priorValue, currentValue);
  }
  return buildDiffText(priorValue, currentValue);
}

function linkOne(recordId) {
  return recordId ? [recordId] : undefined;
}

function isAirtableRecordId(value) {
  const text = strOrNull(value);
  return !!text && /^rec[a-zA-Z0-9]+$/.test(text);
}

function buildSourceLogValues(rawInputs, normalized) {
  const raw = rawInputs || {};
  const values = normalized || {};

  return {
    [LOG_SOURCE_FIELDS.APP_TIME]: strOrNull(raw.app_time),
    [LOG_SOURCE_FIELDS.STATUS]: strOrNull(raw.status),
    [LOG_SOURCE_FIELDS.CLASS_STATUS]: values.class_status,
    [LOG_SOURCE_FIELDS.ESTIMATED_START_TIME]: strOrNull(raw.estimated_start_time),
    [LOG_SOURCE_FIELDS.ESTIMATED_END_TIME]: strOrNull(raw.estimated_end_time),
    [LOG_SOURCE_FIELDS.ACTUAL_TIME]: strOrNull(raw.actual_time),
    [LOG_SOURCE_FIELDS.ESTIMATED_TIME]: strOrNull(raw.estimated_time),
    [LOG_SOURCE_FIELDS.ESTIMATED_GO_TIME]: strOrNull(raw.estimated_go_time),
    [LOG_SOURCE_FIELDS.ORDER_OF_GO]: numOrNull(firstNonBlank(raw.order_of_go, raw.classsignup_oog)),
    [LOG_SOURCE_FIELDS.ACTUAL_ORDER]: numOrNull(raw.actual_order),
    [LOG_SOURCE_FIELDS.ACTUAL_GO]: numOrNull(raw.actual_go),
    [LOG_SOURCE_FIELDS.GONE_IN]: numOrNull(raw.gone_in),
    [LOG_SOURCE_FIELDS.REMAINING_TRIPS]: numOrNull(raw.remaining_trips),
    [LOG_SOURCE_FIELDS.TOTAL_TRIPS]: numOrNull(raw.total_trips),
    [LOG_SOURCE_FIELDS.COMPLETED_TRIPS]: numOrNull(raw.completed_trips),
    [LOG_SOURCE_FIELDS.H_EID]: numOrNull(raw.h_eid),
  };
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

function buildDecisionLogValues(canonical) {
  const calc = canonical || {};
  return {
    [LOG_DECISION_FIELDS.RUNNING_ORDER_IS_TEN_OUT]: !!calc.running_order_is_ten_out,
  };
}

function buildRsLogValues(canonical, computedOutputs) {
  const calc = canonical || {};
  const outputs = computedOutputs || {};

  return {
    [LOG_RS_FIELDS.STATUS]: outputs[WATCH_FIELDS.RS.STATUS],
    [LOG_RS_FIELDS.START_TIME]: outputs[WATCH_FIELDS.RS.START_TIME],
    [LOG_RS_FIELDS.GO_TIME]: outputs[WATCH_FIELDS.RS.GO_TIME],
    [LOG_RS_FIELDS.COMPLETED_TRIPS]: outputs[WATCH_FIELDS.RS.COMPLETED_TRIPS],
    [LOG_RS_FIELDS.GONE_IN]: outputs[WATCH_FIELDS.RS.GONE_IN],
    [LOG_RS_FIELDS.TRIP_DEFAULT]: durationSecondsFromMinutes(calc.trip_minutes_default),
    [LOG_RS_FIELDS.ORDER_OF_GO]: outputs[WATCH_FIELDS.RS.ORDER_OF_GO],
    [LOG_RS_FIELDS.RUNNING_ORDER_OF_GO]: outputs[WATCH_FIELDS.RS.RUNNING_ORDER_OF_GO],
    [LOG_RS_FIELDS.RUNNING_ORDER_OF_GO_MINS_TILL]: outputs[WATCH_FIELDS.RS.RUNNING_ORDER_OF_GO_MINS_TILL],
    [LOG_RS_FIELDS.MINS_TILL_START]: outputs[WATCH_FIELDS.RS.MINS_TILL_START],
    [LOG_RS_FIELDS.MINS_SINCE_START]: outputs[WATCH_FIELDS.RS.MINS_SINCE_START],
    [LOG_RS_FIELDS.TRIP_TIME]: outputs[WATCH_FIELDS.RS.TRIP_TIME],
    [LOG_RS_FIELDS.TRIP_TIME2]: outputs[WATCH_FIELDS.RS.TRIP_TIME2],
    [LOG_RS_FIELDS.LENGTH]: calc.projected_class_minutes,
    [LOG_RS_FIELDS.END_TIME]: outputs[WATCH_FIELDS.RS.END_TIME],
    [LOG_RS_FIELDS.GO_MINS_FROM_START]: outputs[WATCH_FIELDS.RS.GO_MINS_FROM_START],
    [LOG_RS_FIELDS.GO_TIME_FROM_START]: outputs[WATCH_FIELDS.RS.GO_TIME_FROM_START],
    [LOG_RS_FIELDS.MIN_TILL_GO]: outputs[WATCH_FIELDS.RS.MIN_TILL_GO],
    [LOG_RS_FIELDS.MIN_TO_ACTUAL_GO]: outputs[WATCH_FIELDS.RS.MIN_TO_ACTUAL_GO],
  };
}

function finalCalcStatus(result, patchFailure) {
  if (!result.eligibility.eligible) return "skipped";
  if (patchFailure) return "patch_failed";
  if (result.changedFields.length > 0) return CALC_MODE === "shadow" ? "shadow_changed" : "changed";
  if (result.anomalies.length > 0) return "anomaly";
  return "unchanged";
}

function shouldSuppressTripLog(result) {
  const hbReason = String(result?.inputsForLog?.raw?.hb_second_pass_reason || "").trim().toLowerCase();

  // Once trips_tagger can no longer match a live trip, it clears trip-level fields.
  // Those transient rows are often deleted shortly after, so do not create calculator
  // audit rows for them.
  if (hbReason.startsWith("err:no_trip_match")) return true;
  return false;
}

function shouldCreateLogRow(result, patchFailure) {
  if (shouldSuppressTripLog(result)) return false;
  return !result.eligibility.eligible ||
    result.changedFields.length > 0 ||
    result.anomalies.length > 0 ||
    !!patchFailure;
}

function buildTripTriggerEvaluationContext(result, priorLogFields) {
  const raw = result.inputsForLog?.raw || {};
  const normalized = result.inputsForLog?.normalized || {};
  const canonical = result.canonicalOutputs || {};
  const currentOutputs = result.computedOutputs || {};
  const priorOutputs = result.priorOutputs || {};

  return {
    currentByField: {
      entryxclasses_uuid: result.entryxclasses_uuid,
      app_show_id: result.app_show_id,
      app_sql_date: result.app_sql_date,
      class_id: normalized.class_id,
      status: strOrNull(raw.status),
      class_status: normalized.class_status,
      app_time: normalized.app_time_text,
      estimated_start_time: normalized.estimated_start_time_text,
      estimated_end_time: normalized.estimated_end_time_text,
      estimated_time: normalized.estimated_time_text,
      estimated_go_time: normalized.estimated_go_time_text,
      order_of_go: normalized.order_of_go,
      actual_order: normalized.actual_order,
      actual_go: normalized.actual_go,
      gone_in: normalized.gone_in,
      remaining_trips: normalized.remaining_trips,
      total_trips: normalized.total_trips,
      completed_trips: normalized.completed_trips,
      h_eid: normalized.h_eid,
      running_order_is_ten_out: !!canonical.running_order_is_ten_out,
      ...currentOutputs,
    },
    priorByField: {
      ...priorOutputs,
      ...(priorLogFields || {}),
    },
  };
}

function evaluateTriggerTag(trigger, triggerContext) {
  const query = normalizeKey(trigger?.query).toLowerCase();
  const argument = normalizeKey(trigger?.argument).toLowerCase();
  const fieldName = strOrNull(trigger?.this_field);
  if (!query || !fieldName) return false;

  const currentValue = triggerContext.currentByField[fieldName];
  const priorValue = triggerContext.priorByField[fieldName];
  const outputField = strOrNull(trigger?.output_field);
  const priorTriggered = outputField ? boolValue(triggerContext.priorByField[outputField]) : false;

  if (query === "first_transition") {
    if (priorTriggered) return false;
    const targetValue = firstNonBlank(trigger?.to, trigger?.from);
    if (isBlank(targetValue)) return false;
    if (argument === "from_not") {
      return compareTriggerValue(currentValue, targetValue) && !compareTriggerValue(priorValue, targetValue);
    }
    return compareTriggerValue(currentValue, targetValue) && compareTriggerValue(priorValue, trigger?.from);
  }

  if (query === "in_range") {
    const currentNum = parseTriggerNumber(currentValue);
    const fromNum = parseTriggerNumber(trigger?.from);
    const toNum = parseTriggerNumber(trigger?.to);
    if (currentNum === null || fromNum === null || toNum === null) return false;
    const low = Math.min(fromNum, toNum);
    const high = Math.max(fromNum, toNum);
    return currentNum >= low && currentNum <= high;
  }

  if (query === "first_in_range") {
    if (priorTriggered) return false;
    const currentNum = parseTriggerNumber(currentValue);
    const priorNum = parseTriggerNumber(priorValue);
    const fromNum = parseTriggerNumber(trigger?.from);
    const toNum = parseTriggerNumber(trigger?.to);
    if (currentNum === null || fromNum === null || toNum === null) return false;
    const low = Math.min(fromNum, toNum);
    const high = Math.max(fromNum, toNum);
    const currentInRange = currentNum >= low && currentNum <= high;
    const priorInRange = priorNum !== null && priorNum >= low && priorNum <= high;
    return currentInRange && !priorInRange;
  }

  if (query === "equals") {
    return compareTriggerValue(currentValue, firstNonBlank(trigger?.to, trigger?.from));
  }

  if (query === "gt" || query === "gte" || query === "lt" || query === "lte") {
    const currentNum = parseTriggerNumber(currentValue);
    const targetNum = parseTriggerNumber(firstNonBlank(trigger?.to, trigger?.from));
    if (currentNum === null || targetNum === null) return false;
    if (query === "gt") return currentNum > targetNum;
    if (query === "gte") return currentNum >= targetNum;
    if (query === "lt") return currentNum < targetNum;
    return currentNum <= targetNum;
  }

  return false;
}

function applyTriggerTags(fields, triggerTags, triggerContext, tripLogFieldSet) {
  const fired = [];
  for (const trigger of triggerTags || []) {
    const outputField = strOrNull(trigger?.output_field);
    if (!outputField || !tripLogFieldSet.has(outputField)) continue;
    if (!evaluateTriggerTag(trigger, triggerContext)) continue;
    fields[outputField] = true;
    fired.push(outputField);
  }
  return fired;
}

function buildTripLogRecord(result, patchFailure, calcRunId, tripLogFieldSet, tripLogFieldMeta, triggerTags, priorLogFields) {
  const fields = {};
  const createdAt = calcRunId || new Date().toISOString();
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
    calcRunId || createdAt,
  ].join("|");
  const sourceLogValues = buildSourceLogValues(rawInputs, normalized);
  const rawLogValues = buildRawLogValues(rawInputs);
  const normalizedLogValues = buildNormalizedLogValues(normalized);
  const calcLogValues = buildCalcLogValues(canonical);
  const decisionLogValues = buildDecisionLogValues(canonical);
  const rsLogValues = buildRsLogValues(canonical, computedOutputs);
  const scheduleRecordId = strOrNull(rawInputs.schedule_rid);
  const rsStartTimeDiff = buildDiffValueForField(
    priorLogFields?.[LOG_RS_FIELDS.START_TIME],
    rsLogValues[LOG_RS_FIELDS.START_TIME],
    tripLogFieldMeta.get("rs_start_time_diff")
  );
  const rsGoTimeDiff = buildDiffValueForField(
    priorLogFields?.[LOG_RS_FIELDS.GO_TIME],
    rsLogValues[LOG_RS_FIELDS.GO_TIME],
    tripLogFieldMeta.get("rs_go_time_diff")
  );
  const rsOrderOfGoDiff = buildDiffValueForField(
    priorLogFields?.[LOG_RS_FIELDS.ORDER_OF_GO],
    rsLogValues[LOG_RS_FIELDS.ORDER_OF_GO],
    tripLogFieldMeta.get("rs_order_of_go_diff")
  );

  setIfPresent(fields, LOG_KEY_FIELDS.CALC_LOG_KEY, calcLogKey);
  setIfPresent(fields, LOG_KEY_FIELDS.RS_RUN_ID, calcRunId || createdAt);
  setIfPresent(fields, LOG_KEY_FIELDS.ENTRYXCLASSES_UUID, result.entryxclasses_uuid);
  setIfPresent(fields, LOG_KEY_FIELDS.WATCH_TRIP_RECORD_ID, result.recordId);
  if (LOG_KEY_FIELDS.WATCH_TRIPS_LINK && result.recordId) {
    fields[LOG_KEY_FIELDS.WATCH_TRIPS_LINK] = linkOne(result.recordId);
  }
  setIfPresent(fields, LOG_KEY_FIELDS.WATCH_SCHEDULE_RECORD_ID, scheduleRecordId);
  if (LOG_KEY_FIELDS.WATCH_SCHEDULE_LINK && isAirtableRecordId(scheduleRecordId)) {
    fields[LOG_KEY_FIELDS.WATCH_SCHEDULE_LINK] = linkOne(scheduleRecordId);
  }
  setIfPresent(fields, LOG_KEY_FIELDS.APP_SHOW_ID, result.app_show_id);
  setIfPresent(fields, LOG_KEY_FIELDS.APP_SQL_DATE, result.app_sql_date);
  for (const [fieldName, value] of Object.entries(sourceLogValues)) {
    setIfPresent(fields, fieldName, value);
  }
  for (const [fieldName, value] of Object.entries(rawLogValues)) {
    setIfPresent(fields, fieldName, value);
  }
  for (const [fieldName, value] of Object.entries(normalizedLogValues)) {
    setIfPresent(fields, fieldName, value);
  }
  for (const [fieldName, value] of Object.entries(calcLogValues)) {
    setIfPresent(fields, fieldName, value);
  }
  for (const [fieldName, value] of Object.entries(decisionLogValues)) {
    setIfPresent(fields, fieldName, value);
  }
  for (const [fieldName, value] of Object.entries(rsLogValues)) {
    setIfPresent(fields, fieldName, value);
  }
  setIfPresent(fields, tripLogFieldSet.has("rs_start_time_diff") ? "rs_start_time_diff" : "", rsStartTimeDiff);
  setIfPresent(fields, tripLogFieldSet.has("rs_go_time_diff") ? "rs_go_time_diff" : "", rsGoTimeDiff);
  setIfPresent(fields, tripLogFieldSet.has("rs_order_of_go_diff") ? "rs_order_of_go_diff" : "", rsOrderOfGoDiff);
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
  const triggerContext = buildTripTriggerEvaluationContext(result, priorLogFields);
  const firedTriggerFields = applyTriggerTags(fields, triggerTags, triggerContext, tripLogFieldSet);

  return { fields, firedTriggerFields };
}

async function main() {
  requireEnv("AIRTABLE_TOKEN", AIRTABLE_TOKEN);
  requireEnv("AIRTABLE_BASE_ID", AIRTABLE_BASE_ID);
  const calcRunId = new Date().toISOString();
  const watchTableFieldMeta = await airtableTableFieldMeta(WATCH_TABLE);
  const watchTableFieldSet = await airtableTableFieldSet(WATCH_TABLE);
  const tripLogFieldMeta = await airtableTableFieldMeta(TRIP_LOGS_TABLE);
  const tripLogFieldSet = new Set(tripLogFieldMeta.keys());
  const lastLogFieldMeta = watchTableFieldMeta.get(WATCH_LAST_LOG_FIELD) || null;
  const canWriteLastLogField = !!(
    WATCH_LAST_LOG_FIELD &&
    lastLogFieldMeta &&
    String(lastLogFieldMeta.type || "").trim() === "multipleRecordLinks"
  );
  const activeTriggerTags = await fetchActiveTriggerTags();
  const priorTripLogByUuid = await fetchPriorTripLogMap(activeTriggerTags);
  const activeWatchOutputFields = WATCH_OUTPUT_FIELDS.filter((fieldName) => watchTableFieldSet.has(fieldName));

  const records = await airtableList({
    table: WATCH_TABLE,
    view: WATCH_VIEW,
    maxRecords: MAX_RECORDS,
  });

  const summary = {
    calc_run_id: calcRunId,
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
    watch_trips_last_log_field: WATCH_LAST_LOG_FIELD || null,
    watch_trips_last_log_field_writable: canWriteLastLogField,
    watch_trips_last_log_planned: 0,
    watch_trips_last_log_patched: 0,
    watch_trips_last_log_patch_failures: 0,
    trip_logs_planned: 0,
    trip_logs_created: 0,
    trip_logs_failures: 0,
    trigger_tags_active: activeTriggerTags.length,
    trigger_hits: 0,
    class_alert_classes_in_view: 0,
    class_alert_duplicated_classes: 0,
    class_alert_parent_rows: 0,
    trip_minutes_default_rows: 0,
    trip_minutes_source_counts: {},
    skip_reason_counts: {},
    anomaly_samples: [],
    failed_patch_samples: [],
    failed_trip_log_samples: [],
  };

  const results = [];
  const watchTripUpdates = [];
  const preparedRows = [];

  for (const record of records) {
    const built = buildNormalizedInputs(record);
    const values = built.values;
    const eligibility = determineEligibility(values);
    const priorOutputs = buildPriorOutputs(record.fields || {}, activeWatchOutputFields);

    preparedRows.push({
      record,
      built,
      values,
      eligibility,
      priorOutputs,
    });
  }

  const classAlertAssignments = buildClassAlertAssignments(preparedRows);
  summary.class_alert_classes_in_view = classAlertAssignments.classCount;
  summary.class_alert_duplicated_classes = classAlertAssignments.duplicatedClassCount;
  summary.class_alert_parent_rows = classAlertAssignments.parentRowCount;

  for (const prepared of preparedRows) {
    const record = prepared.record;
    const built = prepared.built;
    const values = prepared.values;
    const eligibility = prepared.eligibility;
    const priorOutputs = prepared.priorOutputs;

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

    const computed = computeCanonicalOutputs(values, result.anomalies, {
      classAlert: classAlertAssignments.assignments.get(record.id) || null,
    });
    result.anomalies = computed.anomalies;
    result.canonicalOutputs = computed.canonical;
    result.computedOutputs = computed.outputs;

    const tripMinutesSource = String(computed.canonical?.trip_minutes_source || "unknown");
    summary.trip_minutes_source_counts[tripMinutesSource] =
      (summary.trip_minutes_source_counts[tripMinutesSource] || 0) + 1;
    if (computed.canonical?.trip_minutes_used_default) {
      summary.trip_minutes_default_rows += 1;
    }

    const changed = buildChangedFields(priorOutputs, computed.outputs, activeWatchOutputFields);
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
    const tripLogRecord = buildTripLogRecord(
      result,
      patchFailure,
      calcRunId,
      tripLogFieldSet,
      tripLogFieldMeta,
      activeTriggerTags,
      priorTripLogByUuid.get(result.entryxclasses_uuid) || null
    );
    summary.trigger_hits += tripLogRecord.firedTriggerFields.length;
    tripLogRecords.push({ fields: tripLogRecord.fields });
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

    if (canWriteLastLogField && createResult.createdRows.length > 0) {
      const latestLogByWatchTripId = new Map();
      for (const row of createResult.createdRows) {
        const watchTripRecordId = strOrNull(row?.fields?.[LOG_KEY_FIELDS.WATCH_TRIP_RECORD_ID]);
        const tripLogRecordId = strOrNull(row?.id);
        if (!watchTripRecordId || !isAirtableRecordId(watchTripRecordId) || !isAirtableRecordId(tripLogRecordId)) {
          continue;
        }
        latestLogByWatchTripId.set(watchTripRecordId, tripLogRecordId);
      }

      const lastLogUpdates = [];
      for (const [watchTripRecordId, tripLogRecordId] of latestLogByWatchTripId.entries()) {
        lastLogUpdates.push({
          id: watchTripRecordId,
          fields: { [WATCH_LAST_LOG_FIELD]: linkOne(tripLogRecordId) },
        });
      }

      summary.watch_trips_last_log_planned = lastLogUpdates.length;

      if (lastLogUpdates.length > 0) {
        const lastLogPatch = await airtablePatchWithFallback({
          table: WATCH_TABLE,
          updates: lastLogUpdates,
        });
        summary.watch_trips_last_log_patched = lastLogPatch.okRows;
        summary.watch_trips_last_log_patch_failures = lastLogPatch.failedRows.length;
        for (const failure of lastLogPatch.failedRows) {
          pushSample(
            summary.failed_patch_samples,
            `${failure.record_id}:${String(failure.reason || "").slice(0, 140)}`
          );
        }
      }
    }
  }

  console.log(JSON.stringify(summary, null, 2));
}

main().catch((err) => {
  console.error(String(err?.stack || err?.message || err));
  process.exit(1);
});
