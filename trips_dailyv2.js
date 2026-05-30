const AIRTABLE_TOKEN = process.env.AIRTABLE_TOKEN || "";
const AIRTABLE_BASE_ID = process.env.AIRTABLE_BASE_ID || "";
const CUSTOMER_ID = Number(process.env.CUSTOMER_ID || "15");
const fs = require("fs");
const path = require("path");

const {
  buildPeopleTripKey,
  buildScheduleMap,
  collectTripCandidates,
  normalizeTripsForScope,
} = require("./trips_normalizer_v2");
const {
  assertValidPayload,
  isSoftPayloadError,
  softPayloadLogFields,
} = require("./lib/soft_payload_guard");
const {
  fetchTextWithConfiguredTransport,
} = require("./lib/sgl_fetch_adapter");
const {
  buildAirtableFieldMeta,
  buildScopeFieldPatch,
} = require("./lib/scope_fields");

const BASE_URL = String(
  process.env.SGL_DATA_BASE_URL ||
  process.env.SGL_DIRECT_BASE_URL ||
  process.env.SGL_API_BASE_URL ||
  process.env.BASE_URL ||
  "https://sglapi.wellingtoninternational.com"
).trim().replace(/\/+$/, "");

const TABLE_HEARTBEAT = process.env.TABLE_HEARTBEAT || "heartbeat";
const TABLE_SHOW_TARGET = process.env.TABLE_SHOW_TARGET || process.env.TABLE_SHOW || "show";
const TABLE_SHOWS = process.env.TABLE_SHOWS || "shows";
const TABLE_WATCH_SCHEDULE = process.env.TABLE_WATCH_SCHEDULE || "watch_schedule";
const TABLE_WATCH_TRIPS = process.env.TABLE_WATCH_TRIPS || "watch_trips";
const TABLE_ACTIVE_TENANTS = process.env.TABLE_ACTIVE_TENANTS || "active_tenants";
const TABLE_ACTIVE_CLASSES = process.env.TABLE_ACTIVE_CLASSES || "active_classes";
const TABLE_ACTIVE_ENTRIES = process.env.TABLE_ACTIVE_ENTRIES || "active_entries";
const TABLE_WW_RIDERS = process.env.TABLE_WW_RIDERS || "ww_riders";
const TABLE_WW_HORSES = process.env.TABLE_WW_HORSES || "ww_horses";
const TABLE_WW_TRAINERS = process.env.TABLE_WW_TRAINERS || "ww_trainers";
const TABLE_AUTOMATION_ERRS = process.env.TABLE_AUTOMATION_ERRS || "automation_errs";

const VIEW_WATCH_SCHEDULE = process.env.VIEW_WATCH_SCHEDULE || "heartbeat";
const VIEW_WATCH_TRIPS = process.env.VIEW_WATCH_TRIPS || "heartbeat";
const VIEW_ACTIVE_TENANTS = process.env.VIEW_ACTIVE_TENANTS || "active_tenants";
const VIEW_SHOW_TARGET = process.env.VIEW_SHOW_TARGET || "heartbeat";
const HEARTBEAT_SORT_FIELD = process.env.HEARTBEAT_SORT_FIELD || "hb_at";

const WATCH_TRIPS_HEARTBEAT_FIELDS = [
  "trips_key",
  "entryxclasses_uuid",
  "show_id",
  "show_date",
  "app_show_id",
  "app_show_idv2",
  "app_sql_date",
  "app_sql_datev2",
  "schedule_show_datev2",
  "scheduled_date",
  "ring_number",
  "class_number",
  "pid",
  "entry_number",
  "heartbeat",
  "watch_schedule",
  "is_current_scope",
  "inactive",
  "archive",
  "dropped_at",
];

const HTTP_TIMEOUT_MS = Number(process.env.HTTP_TIMEOUT_MS || "20000");
const AT_RETRY_ATTEMPTS = Number(process.env.AT_RETRY_ATTEMPTS || "3");
const AT_RETRY_BASE_MS = Number(process.env.AT_RETRY_BASE_MS || "400");
const AT_RETRY_MAX_MS = Number(process.env.AT_RETRY_MAX_MS || "2000");
const DRY_RUN = String(process.env.DRY_RUN || "0") === "1";
const FETCH_CONCURRENCY = Math.max(1, Number(process.env.FETCH_CONCURRENCY || "4"));
const TRIPS_SCOPE_TIMEZONE = process.env.TRIPS_SCOPE_TIMEZONE || process.env.SCHEDULE_SCOPE_TIMEZONE || "America/New_York";
const SGL_PAYLOAD_ROOT = String(process.env.SGL_PAYLOAD_ROOT || "C:\\actions-runner\\ringstatus").trim();
const EARLY_SGL_PAYLOAD_ROOT = String(
  process.env.EARLY_SGL_PAYLOAD_ROOT ||
  path.join(SGL_PAYLOAD_ROOT, "early_sgl_payloads")
).trim();
const MANUAL_SGL_PAYLOAD_ROOT = String(
  process.env.MANUAL_SGL_PAYLOAD_ROOT ||
  path.join(SGL_PAYLOAD_ROOT, "manual_sgl_payloads")
).trim();
const EARLY_PEOPLE_PAYLOAD_DIR = String(
  process.env.EARLY_PEOPLE_PAYLOAD_DIR ||
  path.join(EARLY_SGL_PAYLOAD_ROOT, "people")
).trim();
const MANUAL_PEOPLE_PAYLOAD_DIR = String(
  process.env.MANUAL_PEOPLE_PAYLOAD_DIR ||
  path.join(MANUAL_SGL_PAYLOAD_ROOT, "people")
).trim();
const DEFAULT_PEOPLE_FALLBACK_DIRS = [
  EARLY_PEOPLE_PAYLOAD_DIR,
  MANUAL_PEOPLE_PAYLOAD_DIR,
];

const PROTECTED_WATCH_TRIPS_FIELDS = new Set([
  "status",
  "estimated_start_time",
  "estimated_end_time",
  "estimated_go_time",
  "order_of_go",
  "remaining_trips",
  "total_trips",
  "completed_trips",
  "actual_time",
  "estimated_time",
  "gone_in",
  "h_eid",
  "time_one",
  "time_two",
  "time_three",
  "score",
  "score1",
  "score2",
  "score3",
  "placing"
]);
const WATCH_TRIPS_MANUAL_TIME_FIELDS = [
  "estimated_start_time",
  "estimated_end_time",
  "estimated_go_time",
  "estimated_time",
  "actual_time",
  "scheduled_estimated_start_time",
  "latest_estimated_start_time",
];

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

function pickFirst(...values) {
  for (const value of values) {
    if (!isBlank(value)) return value;
  }
  return undefined;
}

function firstValue(value) {
  if (Array.isArray(value)) {
    for (const item of value) {
      if (!isBlank(item)) return item;
    }
    return null;
  }
  return value;
}

function firstLinkId(value) {
  const raw = firstValue(value);
  if (isBlank(raw)) return null;
  return String(raw).trim();
}

function boolValue(value) {
  const raw = firstValue(value);
  if (raw === true || raw === 1) return true;
  if (raw === false || raw === 0 || raw === null || raw === undefined) return false;
  const text = String(raw).trim().toLowerCase();
  return text === "true" || text === "1" || text === "yes" || text === "checked";
}

function addDaysSql(sqlDate, days) {
  const text = toIsoDateOnly(sqlDate);
  if (!text || !/^\d{4}-\d{2}-\d{2}$/.test(text)) throw new Error(`Invalid sql_date: ${sqlDate}`);
  const ms = Date.parse(`${text}T00:00:00.000Z`);
  if (!Number.isFinite(ms)) throw new Error(`Invalid sql_date: ${text}`);
  return new Date(ms + days * 86400000).toISOString().slice(0, 10);
}

function compareSqlDate(left, right) {
  const a = toIsoDateOnly(left);
  const b = toIsoDateOnly(right);
  if (!a || !b || !/^\d{4}-\d{2}-\d{2}$/.test(a) || !/^\d{4}-\d{2}-\d{2}$/.test(b)) {
    throw new Error(`Invalid sql_date comparison: ${left} ${right}`);
  }
  if (a < b) return -1;
  if (a > b) return 1;
  return 0;
}

function dayNameForSqlDate(sqlDate) {
  const text = toIsoDateOnly(sqlDate);
  const date = new Date(`${text}T00:00:00.000Z`);
  return ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"][date.getUTCDay()];
}

function localMinutesForTripsScope(now = new Date(), timeZone = TRIPS_SCOPE_TIMEZONE) {
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone,
    hour12: false,
    hour: "2-digit",
    minute: "2-digit",
  }).formatToParts(now);
  const hour = Number(parts.find((part) => part.type === "hour")?.value);
  const minute = Number(parts.find((part) => part.type === "minute")?.value);
  if (!Number.isFinite(hour) || !Number.isFinite(minute)) {
    throw new Error(`Unable to resolve local trips scope time for ${timeZone}`);
  }
  return hour * 60 + minute;
}

function showHeartbeatTargetDate(fields, now = new Date()) {
  const focusDay = toIsoDateOnly(fields?.focus_day);
  const startDate = toIsoDateOnly(fields?.start_date);
  const endDate = toIsoDateOnly(fields?.end_date);
  if (!focusDay || !startDate || !endDate) {
    return { skipped: true, reason: "missing_show_target_date", target_date: null };
  }
  const minutes = localMinutesForTripsScope(now);
  const isDayWindow = minutes >= 6 * 60 && minutes < 17 * 60;
  const targetDate = isDayWindow ? focusDay : addDaysSql(focusDay, 1);
  if (compareSqlDate(targetDate, startDate) < 0 || compareSqlDate(targetDate, endDate) > 0) {
    return {
      skipped: true,
      reason: "target_date_outside_show_window",
      focus_day: focusDay,
      start_date: startDate,
      end_date: endDate,
      target_date: null,
      proposed_target_date: targetDate,
      is_day_window: isDayWindow,
    };
  }
  return {
    skipped: false,
    reason: null,
    focus_day: focusDay,
    start_date: startDate,
    end_date: endDate,
    target_date: targetDate,
    proposed_target_date: targetDate,
    is_day_window: isDayWindow,
  };
}

function normalizeKey(value) {
  if (isBlank(value)) return "";
  return String(value).trim();
}

function keyPart(value) {
  if (isBlank(value)) return "";
  return String(value).trim();
}

function joinKeyParts(parts) {
  if (parts.some((part) => !keyPart(part))) return "";
  return parts.map(keyPart).join("|");
}

function joinKeyPartsWithOptional(requiredParts, optionalParts = []) {
  const base = joinKeyParts(requiredParts);
  if (!base) return "";
  const extras = optionalParts.map(keyPart).filter(Boolean);
  return extras.length ? [base, ...extras].join("|") : base;
}

function scheduleTieBreakerFromKey(key) {
  const parts = String(key || "").split("|");
  return parts.length >= 5 ? keyPart(parts[4]) : "";
}

function buildScheduleKeyParts({ sid, sqlDate, ringNumber, classNumber, tieBreaker }) {
  return {
    scheduleKey: joinKeyPartsWithOptional([sid, sqlDate, ringNumber, classNumber], [tieBreaker]),
    scheduleShort: joinKeyPartsWithOptional([ringNumber, classNumber], [tieBreaker]),
  };
}

function buildTripKeyParts({ sid, sqlDate, ringNumber, classNumber, tieBreaker, pid, entryNumber, time, cgid }) {
  const scheduleKeys = buildScheduleKeyParts({ sid, sqlDate, ringNumber, classNumber, tieBreaker });
  return {
    ...scheduleKeys,
    tripsKey: joinKeyPartsWithOptional([sid, sqlDate, ringNumber, classNumber, pid, entryNumber], [tieBreaker]),
    tripsShortKey: joinKeyPartsWithOptional([classNumber, pid, entryNumber], [tieBreaker]),
    fullNestingKey: joinKeyPartsWithOptional([sid, sqlDate, ringNumber, time, cgid, classNumber, pid, entryNumber], [tieBreaker]),
  };
}

function tripRowKeyFromFields(fields = {}) {
  const tripsKey = normalizeKey(firstValue(fields.trips_key));
  if (tripsKey) return tripsKey;

  return buildPeopleTripKey({
    classNumber: firstValue(fields.class_number),
    entryNumber: firstValue(fields.entry_number),
  }) || normalizeKey(firstValue(fields.entryxclasses_uuid));
}

function tripRowCandidateKeysFromFields(fields = {}) {
  const keys = new Set();
  const add = (value) => {
    const key = normalizeKey(value);
    if (key) keys.add(key);
  };

  const tripsKey = normalizeKey(firstValue(fields.trips_key));
  add(tripsKey);
  const parts = tripsKey ? tripsKey.split("|").map(keyPart) : [];
  if (parts.length === 7) {
    add([...parts.slice(0, 4), ...parts.slice(5)].join("|"));
    add([...parts.slice(0, 4), ...parts.slice(5), parts[4]].join("|"));
  }

  const common = {
    sid: pickFirst(fields.show_id, fields.app_show_id, fields.app_show_idv2, fields.app_sid),
    sqlDate: pickFirst(
      toIsoDateOnly(fields.schedule_show_datev2),
      toIsoDateOnly(fields.scheduled_date),
      fields.show_date,
      fields.app_sql_date,
      fields.app_sql_datev2
    ),
    ringNumber: fields.ring_number,
    classNumber: fields.class_number,
    pid: fields.pid,
    entryNumber: fields.entry_number,
  };
  add(buildTripKeyParts(common).tripsKey);
  const realTieBreaker = keyPart(fields.class_group_sequence);
  if (realTieBreaker) add(buildTripKeyParts({ ...common, tieBreaker: realTieBreaker }).tripsKey);

  add(buildPeopleTripKey({
    classNumber: firstValue(fields.class_number),
    entryNumber: firstValue(fields.entry_number),
  }));
  add(firstValue(fields.entryxclasses_uuid));
  return [...keys];
}

function normalizePidToken(value) {
  const num = numOrNull(value);
  return num === null || num <= 0 ? "" : String(num);
}

function chunk(items, size) {
  const out = [];
  for (let i = 0; i < items.length; i += size) out.push(items.slice(i, i + size));
  return out;
}

function sameValue(left, right) {
  if (Array.isArray(left) || Array.isArray(right)) {
    return JSON.stringify(left ?? null) === JSON.stringify(right ?? null);
  }
  return left === right;
}

function normalizeEntryNumber(value) {
  if (value === undefined || value === null) return undefined;
  if (typeof value === "number" && Number.isFinite(value)) return value;
  const text = String(value).trim();
  if (!text) return undefined;
  if (/^\d+$/.test(text)) return Number(text);
  return text;
}

function minTimeText(left, right) {
  if (!left) return right || null;
  if (!right) return left || null;
  return String(left) <= String(right) ? left : right;
}

function maxTimeText(left, right) {
  if (!left) return right || null;
  if (!right) return left || null;
  return String(left) >= String(right) ? left : right;
}

function toIsoDateOnly(value) {
  if (isBlank(value)) return null;
  if (value instanceof Date) return value.toISOString().slice(0, 10);
  const text = String(value).trim();
  const isoText = /^\d{4}-\d{2}-\d{2}$/.test(text)
    ? text
    : (/^\d{4}-\d{2}-\d{2}T/.test(text) ? text.slice(0, 10) : null);
  if (isoText) {
    const parsedIso = new Date(`${isoText}T00:00:00.000Z`);
    return Number.isFinite(parsedIso.getTime()) && parsedIso.toISOString().slice(0, 10) === isoText
      ? isoText
      : null;
  }
  const parsed = Date.parse(text);
  return Number.isFinite(parsed) ? new Date(parsed).toISOString().slice(0, 10) : null;
}

function resolveTripScheduleDate(source) {
  if (!source || typeof source !== "object") return null;
  return toIsoDateOnly(pickFirst(
    firstValue(source.app_sql_datev2),
    firstValue(source.app_sql_date),
    firstValue(source.schedule_show_datev2),
    firstValue(source.scheduled_date),
    firstValue(source[" scheduled_date"]),
    firstValue(source["schedule_show_datev2 (from watch_schedule)"]),
    firstValue(source.show_date),
    firstValue(source.date)
  ));
}

function setIfPresent(target, fieldName, value) {
  if (!fieldName) return;
  if (value === undefined || value === null) return;
  if (typeof value === "string" && value.trim() === "") return;
  target[fieldName] = value;
}

function isBlankPatchValue(value) {
  return value === null ||
    value === undefined ||
    (typeof value === "string" && value.trim() === "") ||
    (Array.isArray(value) && value.length === 0);
}

function sanitizeWatchTripsPatchUpdates(tableName, updates) {
  if (tableName !== TABLE_WATCH_TRIPS) return updates;

  return updates
    .map((row) => {
      const fields = { ...(row.fields || {}) };
      for (const fieldName of PROTECTED_WATCH_TRIPS_FIELDS) {
        if (Object.prototype.hasOwnProperty.call(fields, fieldName) && isBlankPatchValue(fields[fieldName])) {
          delete fields[fieldName];
        }
      }
      return { ...row, fields };
    })
    .filter((row) => Object.keys(row.fields || {}).length > 0);
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

function isRetryableFetchError(error) {
  const name = String(error?.name || "");
  const code = String(error?.code || "");
  const message = String(error?.message || "");
  if (name === "AbortError") return true;
  if (code === "UND_ERR_CONNECT_TIMEOUT") return true;
  if (code === "UND_ERR_HEADERS_TIMEOUT") return true;
  if (code === "UND_ERR_BODY_TIMEOUT") return true;
  if (/timeout/i.test(message)) return true;
  if (/fetch failed/i.test(message)) return true;
  return false;
}

async function fetchWithRetry(url, options = {}, retry = {}) {
  const attempts = Math.max(1, Math.floor(Number(retry.attempts ?? AT_RETRY_ATTEMPTS)));
  const baseMs = Math.max(0, Math.floor(Number(retry.baseMs ?? AT_RETRY_BASE_MS)));
  const maxMs = Math.max(250, Math.floor(Number(retry.maxMs ?? AT_RETRY_MAX_MS)));

  let lastError = null;
  for (let attempt = 1; attempt <= attempts; attempt += 1) {
    try {
      const response = await fetchWithTimeout(url, options);
      if (response.status === 429 || (response.status >= 500 && response.status <= 599)) {
        if (attempt === attempts) return response;
        await sleep(Math.min(maxMs, baseMs * attempt + Math.floor(Math.random() * 200)));
        continue;
      }
      return response;
    } catch (error) {
      lastError = error;
      if (!isRetryableFetchError(error) || attempt === attempts) throw error;
      await sleep(Math.min(maxMs, baseMs * attempt + Math.floor(Math.random() * 250)));
    }
  }
  throw lastError || new Error("fetchWithRetry failed");
}

function airtableUrl(tableName) {
  return `https://api.airtable.com/v0/${AIRTABLE_BASE_ID}/${encodeURIComponent(tableName)}`;
}

async function airtableList(tableName, queryParams = {}) {
  const records = [];
  let offset = null;

  while (true) {
    const url = new URL(airtableUrl(tableName));
    for (const [key, value] of Object.entries(queryParams)) {
      if (value === undefined || value === null || value === "") continue;
      if (Array.isArray(value)) {
        for (const item of value) {
          if (item === undefined || item === null || item === "") continue;
          url.searchParams.append(key, String(item));
        }
        continue;
      }
      url.searchParams.set(key, String(value));
    }
    if (offset) url.searchParams.set("offset", offset);

    const response = await fetchWithRetry(url.toString(), {
      headers: { Authorization: `Bearer ${AIRTABLE_TOKEN}` },
    });

    if (!response.ok) {
      const body = await response.text().catch(() => "");
      throw new Error(`Airtable list failed (${response.status}) ${tableName}: ${body}`);
    }

    const json = await response.json().catch(() => ({}));
    records.push(...(json.records || []));
    offset = json.offset;
    if (!offset) break;
  }

  return records;
}

async function airtableCreateRecords(tableName, records) {
  if (!records.length) return { okRows: 0, failedRows: [] };

  let okRows = 0;
  const failedRows = [];

  for (const batch of chunk(records, 10)) {
    try {
      const response = await fetchWithRetry(airtableUrl(tableName), {
        method: "POST",
        headers: {
          Authorization: `Bearer ${AIRTABLE_TOKEN}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ records: batch, typecast: true }),
      });

      if (!response.ok) {
        const body = await response.text().catch(() => "");
        throw new Error(`Airtable create failed (${response.status}) ${tableName}: ${body}`);
      }

      okRows += batch.length;
    } catch (error) {
      for (const row of batch) {
        failedRows.push({
          key: row?.fields?.entryxclasses_uuid ?? null,
          reason: String(error?.message || error).slice(0, 300),
        });
      }
    }
  }

  return { okRows, failedRows };
}

async function createAutomationErr(fields) {
  if (DRY_RUN) return { skipped: true, reason: "dry_run" };
  const safeFields = {};
  for (const [key, value] of Object.entries(fields || {})) {
    if (value === undefined || value === null) continue;
    if (typeof value === "string" && value.trim() === "") continue;
    safeFields[key] = value;
  }
  if (!Object.keys(safeFields).length) return { skipped: true, reason: "empty_fields" };

  try {
    const result = await airtableCreateRecords(TABLE_AUTOMATION_ERRS, [{ fields: safeFields }]);
    if (result.failedRows?.length) {
      console.log(`automation_errs write warn: ${JSON.stringify(result.failedRows).slice(0, 300)}`);
    }
    return result;
  } catch (error) {
    console.log(`automation_errs write warn: ${String(error?.message || error).slice(0, 300)}`);
    return { skipped: true, reason: String(error?.message || error).slice(0, 300) };
  }
}

function endpointParts(endpoint) {
  try {
    const url = new URL(String(endpoint || ""));
    const pathText = url.pathname || "";
    const peopleMatch = pathText.match(/\/people\/([^/]+)/i);
    const isPeopleEndpoint = !!peopleMatch;
    return {
      path: `${pathText}${url.search || ""}`,
      pid: peopleMatch ? numOrNull(peopleMatch[1]) : null,
      people_show_id: isPeopleEndpoint ? numOrNull(url.searchParams.get("show_id")) : null,
      app_show_id: numOrNull(url.searchParams.get("show_id")),
      app_sql_date: strOrNull(url.searchParams.get("date")),
    };
  } catch {
    return {
      path: String(endpoint || ""),
      pid: null,
      people_show_id: null,
      app_show_id: null,
      app_sql_date: null,
    };
  }
}

async function recordSoftPayloadAudit(error, endpoint, audit = {}) {
  if (!isSoftPayloadError(error)) return null;
  const info = softPayloadLogFields(error);
  const parts = endpointParts(info.endpoint || endpoint);
  const errorType = strOrNull(info.reason) || "soft_payload";
  const appShowId = numOrNull(audit.app_show_id) ?? parts.app_show_id;
  const appSqlDate = strOrNull(audit.app_sql_date) || parts.app_sql_date;
  const pathText = parts.path || String(info.endpoint || endpoint || "");
  const pid = numOrNull(audit.pid) ?? parts.pid;
  const automationKey = strOrNull(audit.automation_key) ||
    [
      "trips_dailyv2",
      errorType,
      appShowId || "show",
      appSqlDate || "date",
      pid || "pid",
      pathText,
    ].join("|").slice(0, 1000);

  return createAutomationErr({
    automation_key: automationKey,
    automation_name: strOrNull(audit.automation_name) || "trips_dailyv2",
    error_type: errorType,
    app_sql_date: appSqlDate,
    run_id: numOrNull(audit.run_id),
    last_run: strOrNull(audit.last_run),
    resolved: false,
    message: [
      `path=${pathText}`,
      `endpoint=${info.endpoint || endpoint || ""}`,
      `status=${info.http_status ?? ""}`,
      `body_length=${info.body_length ?? ""}`,
      `content_length=${info.content_length ?? ""}`,
      `transport=${error?.transport || error?.metadata?.transport || ""}`,
      `source=${strOrNull(audit.source) || ""}`,
      `message=${String(error?.message || errorType).slice(0, 500)}`,
    ].join(" "),
    pid,
    app_show_id: appShowId,
    people_show_id: numOrNull(audit.people_show_id) ?? parts.people_show_id,
  });
}

async function recordPayloadPingAudit(endpoint, response, text, audit = {}) {
  const parts = endpointParts(endpoint);
  const errorType = "payload_ok";
  const appShowId = numOrNull(audit.app_show_id) ?? parts.app_show_id;
  const appSqlDate = strOrNull(audit.app_sql_date) || parts.app_sql_date;
  const pathText = parts.path || String(endpoint || "");
  const pid = numOrNull(audit.pid) ?? parts.pid;
  const bodyLength = Buffer.byteLength(text || "", "utf8");
  const automationKey = strOrNull(audit.automation_key) ||
    [
      "trips_dailyv2",
      errorType,
      appShowId || "show",
      appSqlDate || "date",
      pid || "pid",
      pathText,
    ].join("|").slice(0, 1000);

  return createAutomationErr({
    automation_key: automationKey,
    automation_name: strOrNull(audit.automation_name) || "trips_dailyv2",
    error_type: errorType,
    app_sql_date: appSqlDate,
    run_id: numOrNull(audit.run_id),
    last_run: strOrNull(audit.last_run),
    resolved: true,
    message: [
      `path=${pathText}`,
      `endpoint=${endpoint || ""}`,
      `status=${response?.status ?? ""}`,
      `body_length=${bodyLength}`,
      `content_length=${response?.headers?.get?.("content-length") ?? ""}`,
      `transport=${audit.transport || ""}`,
      `source=${strOrNull(audit.source) || ""}`,
      "message=payload_ok",
    ].join(" "),
    pid,
    app_show_id: appShowId,
    people_show_id: numOrNull(audit.people_show_id) ?? parts.people_show_id,
  });
}

async function recordNoTripsAudit({ tenantId, endpoint, heartbeat, runId, lastRun, source }) {
  const appShowId = heartbeat?.app_show_id ?? null;
  const appSqlDate = heartbeat?.app_sql_date || null;
  return createAutomationErr({
    automation_key: [
      "trips_dailyv2",
      "no_trips",
      appShowId ?? "show_unknown",
      appSqlDate || "date_unknown",
      tenantId,
    ].join("|").slice(0, 1000),
    automation_name: "trips_dailyv2",
    error_type: "no_trips",
    app_sql_date: appSqlDate,
    run_id: runId,
    last_run: lastRun,
    resolved: true,
    message: [
      `endpoint=${endpoint || ""}`,
      `source=${source || ""}`,
      "message=no_trips_in_people_payload",
    ].join(" "),
    pid: numOrNull(tenantId),
    app_show_id: appShowId,
    people_show_id: appShowId,
  });
}

async function airtablePatchRecords(tableName, updates) {
  const safeUpdates = sanitizeWatchTripsPatchUpdates(tableName, updates);
  if (!safeUpdates.length) return { okRows: 0, failedRows: [] };

  let okRows = 0;
  const failedRows = [];

  for (const batch of chunk(safeUpdates, 10)) {
    try {
      const response = await fetchWithRetry(airtableUrl(tableName), {
        method: "PATCH",
        headers: {
          Authorization: `Bearer ${AIRTABLE_TOKEN}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ records: batch, typecast: true }),
      });

      if (!response.ok) {
        const body = await response.text().catch(() => "");
        throw new Error(`Airtable patch failed (${response.status}) ${tableName}: ${body}`);
      }

      okRows += batch.length;
    } catch (error) {
      for (const row of batch) {
        failedRows.push({
          record_id: row.id,
          key: row?.fields?.entryxclasses_uuid ?? null,
          reason: String(error?.message || error).slice(0, 300),
        });
      }
    }
  }

  return { okRows, failedRows };
}

async function fetchTableFieldSet(tableName) {
  const response = await fetchWithRetry(`https://api.airtable.com/v0/meta/bases/${AIRTABLE_BASE_ID}/tables`, {
    headers: { Authorization: `Bearer ${AIRTABLE_TOKEN}` },
  });

  if (!response.ok) {
    const body = await response.text().catch(() => "");
    throw new Error(`Airtable meta failed (${response.status}) ${tableName}: ${body}`);
  }

  const json = await response.json().catch(() => ({}));
  const table = Array.isArray(json?.tables)
    ? json.tables.find((item) => String(item?.name || "").trim() === tableName)
    : null;
  return new Set(Array.isArray(table?.fields) ? table.fields.map((field) => String(field?.name || "").trim()).filter(Boolean) : []);
}

async function fetchTableWritableFieldSet(tableName) {
  const response = await fetchWithRetry(`https://api.airtable.com/v0/meta/bases/${AIRTABLE_BASE_ID}/tables`, {
    headers: { Authorization: `Bearer ${AIRTABLE_TOKEN}` },
  });

  if (!response.ok) {
    const body = await response.text().catch(() => "");
    throw new Error(`Airtable meta failed (${response.status}) ${tableName}: ${body}`);
  }

  const json = await response.json().catch(() => ({}));
  const table = Array.isArray(json?.tables)
    ? json.tables.find((item) => String(item?.name || "").trim() === tableName)
    : null;
  return buildAirtableFieldMeta(Array.isArray(table?.fields) ? table.fields : []).writableNames;
}

async function fetchScopeStatusChoices(tableName) {
  const response = await fetchWithRetry(`https://api.airtable.com/v0/meta/bases/${AIRTABLE_BASE_ID}/tables`, {
    headers: { Authorization: `Bearer ${AIRTABLE_TOKEN}` },
  });

  if (!response.ok) {
    const body = await response.text().catch(() => "");
    throw new Error(`Airtable meta failed (${response.status}) ${tableName}: ${body}`);
  }

  const json = await response.json().catch(() => ({}));
  const table = Array.isArray(json?.tables)
    ? json.tables.find((item) => String(item?.name || "").trim() === tableName)
    : null;
  const field = Array.isArray(table?.fields)
    ? table.fields.find((item) => String(item?.name || "").trim() === "scope_status")
    : null;
  const choices = Array.isArray(field?.options?.choices) ? field.options.choices : [];
  return new Set(choices.map((choice) => String(choice?.name || "").trim()).filter(Boolean));
}

async function fetchLatestHeartbeat() {
  const rows = await airtableList(TABLE_HEARTBEAT, {
    maxRecords: 1,
    pageSize: 1,
    "sort[0][field]": HEARTBEAT_SORT_FIELD,
    "sort[0][direction]": "desc",
    "fields[]": [
      "record_id",
      "heartbeat_id",
      "show_id",
      "app_show_id",
      "app_sql_date",
      "app_dow_raw",
      "shifted_to_next_day",
      "mode",
      "time",
      "customer_id",
      "focus_day",
      "ring_collection",
      "show_scope_key",
      "show",
    ],
  });

  if (!rows.length) throw new Error(`No heartbeat rows found in ${TABLE_HEARTBEAT}`);

  const record = rows[0];
  const fields = record?.fields || {};
  const appShowId = numOrNull(fields.app_show_id) ?? numOrNull(fields.show_id);
  const appSqlDate = strOrNull(fields.app_sql_date);
  const appDowRaw = strOrNull(fields.app_dow_raw);
  const recordId = strOrNull(fields.record_id) || record.id;

  if (appShowId === null) throw new Error("Latest heartbeat missing app_show_id/show_id");
  if (!appSqlDate) throw new Error("Latest heartbeat missing app_sql_date");
  if (!appDowRaw) throw new Error("Latest heartbeat missing app_dow_raw");
  if (recordId !== record.id) throw new Error(`Heartbeat record_id mismatch: field=${recordId} actual=${record.id}`);

  return {
    recordId: record.id,
    scope_run_id: strOrNull(fields.heartbeat_id) || record.id,
    app_show_id: appShowId,
    app_sql_date: appSqlDate,
    app_dow_raw: appDowRaw,
    shifted_to_next_day: boolValue(fields.shifted_to_next_day),
    mode: strOrNull(fields.mode),
    app_time: strOrNull(fields.time),
    customer_id: numOrNull(fields.customer_id),
    focus_day: toIsoDateOnly(fields.focus_day),
    ring_collection: strOrNull(fields.ring_collection),
    show_scope_key: strOrNull(fields.show_scope_key),
    show_record_id: firstValue(fields.show) || null,
  };
}

async function fetchShowTargetRows() {
  return airtableList(TABLE_SHOW_TARGET, {
    view: VIEW_SHOW_TARGET,
    pageSize: 100,
    "fields[]": [
      "show_id",
      "customer_id",
      "focus_day",
      "start_date",
      "end_date",
      "heartbeat",
      "show_rid",
    ],
  });
}

async function resolveHeartbeatScopesFromShowTarget(latestHeartbeat, now = new Date()) {
  let rows = [];
  try {
    rows = await fetchShowTargetRows();
  } catch (error) {
    return [{
      heartbeat: latestHeartbeat,
      show_target_scope: {
        table: TABLE_SHOW_TARGET,
        view: VIEW_SHOW_TARGET,
        rows: 0,
        valid_rows: 0,
        fetch_error: String(error?.message || error),
        source: "latest_heartbeat_fallback",
      },
    }];
  }

  const skippedRows = [];
  const valid = [];
  for (const row of rows) {
    const fields = row?.fields || {};
    const targetInfo = showHeartbeatTargetDate(fields, now);
    const showId = numOrNull(fields.show_id);
    if (targetInfo.skipped || showId === null) {
      skippedRows.push({
        record_id: row.id,
        show_id: showId,
        ...targetInfo,
      });
      continue;
    }
    valid.push({ row, fields, targetInfo, showId });
  }

  if (!valid.length) {
    return [{
      heartbeat: latestHeartbeat,
      show_target_scope: {
        table: TABLE_SHOW_TARGET,
        view: VIEW_SHOW_TARGET,
        rows: rows.length,
        valid_rows: 0,
        skipped_rows: skippedRows,
        source: "latest_heartbeat_fallback",
      },
    }];
  }

  return valid.map((selected) => {
    const customerId = numOrNull(selected.fields.customer_id) ?? latestHeartbeat.customer_id ?? CUSTOMER_ID;
    const targetDate = selected.targetInfo.target_date;
    return {
      heartbeat: {
        ...latestHeartbeat,
        app_show_id: selected.showId,
        app_sql_date: targetDate,
        app_dow_raw: dayNameForSqlDate(targetDate),
        shifted_to_next_day: !selected.targetInfo.is_day_window,
        customer_id: customerId,
        focus_day: selected.targetInfo.focus_day,
        show_record_id: selected.row.id,
        show_scope_key: `${customerId}|${selected.showId}|${selected.targetInfo.focus_day}`,
        scope_run_id: `${latestHeartbeat.scope_run_id}|show:${selected.showId}|${targetDate}`,
      },
      show_target_scope: {
        table: TABLE_SHOW_TARGET,
        view: VIEW_SHOW_TARGET,
        rows: rows.length,
        valid_rows: valid.length,
        selected_record_id: selected.row.id,
        selected_show_id: selected.showId,
        selected_target_date: targetDate,
        skipped_rows: skippedRows,
        source: "show_heartbeat_target",
      },
    };
  });
}

async function resolveHeartbeatFromShowTarget(latestHeartbeat, now = new Date()) {
  const scopes = await resolveHeartbeatScopesFromShowTarget(latestHeartbeat, now);
  return scopes[0];
}

async function fetchShowRecordId(appShowId) {
  const rows = await airtableList(TABLE_SHOWS, {
    maxRecords: 1,
    pageSize: 1,
    filterByFormula: `{show_id}=${Number(appShowId)}`,
    "fields[]": ["show_id"],
  });

  return rows[0]?.id || null;
}

async function fetchWatchScheduleRows() {
  const fieldSet = await fetchTableFieldSet(TABLE_WATCH_SCHEDULE).catch(() => new Set());
  return airtableList(TABLE_WATCH_SCHEDULE, {
    view: VIEW_WATCH_SCHEDULE,
    pageSize: 100,
    "fields[]": [
      "record_id",
      "schedule_key",
      "schedule_short",
      "show_id",
      "app_show_idv2",
      "app_sql_datev2",
      "app_dow_rawv2",
      "customer_id",
      "focus_day",
      "ring_collection",
      "show_scope_key",
      "show",
      "class_groupxclasses_id",
      "class_group_id",
      "class_id",
      "class_number",
      "class_name",
      "schedule_sequencetype",
      "class_type",
      "group_name",
      "ring_number",
      "estimated_start_time",
      "estimated_end_time",
      "total_trips",
      "completed_trips",
      "status",
      "class_group_sequence",
      "is_target",
      "schedule_show_datev2",
      " scheduled_date",
      "show_date",
      "archive",
      "inactive",
      "dropped_at",
    ].filter((fieldName) => !fieldSet.size || fieldSet.has(fieldName)),
  });
}

async function fetchActiveTenantRows() {
  const rows = await airtableList(TABLE_ACTIVE_TENANTS, {
    view: VIEW_ACTIVE_TENANTS,
    pageSize: 100,
    "fields[]": ["tenant_id", "tenant_active"],
  });

  return rows
    .filter((row) => boolValue(row?.fields?.tenant_active))
    .map((row) => ({
      recordId: row.id,
      tenant_id: normalizePidToken(row?.fields?.tenant_id),
    }));
}

async function fetchWwTrainerRecordIdByPid() {
  const rows = await airtableList(TABLE_WW_TRAINERS, {
    pageSize: 100,
    "fields[]": ["pid"],
  });

  return new Map(
    rows
      .map((row) => [normalizePidToken(row?.fields?.pid), row.id])
      .filter(([pid]) => Boolean(pid))
  );
}

async function fetchExistingTripsForShow(appShowId) {
  const rows = await airtableList(TABLE_WATCH_TRIPS, {
    pageSize: 100,
  });

  return rows.filter((row) => {
    const fields = row?.fields || {};
    return numOrNull(fields.show_id) === appShowId ||
      numOrNull(fields.app_show_id) === appShowId ||
      numOrNull(fields.app_show_idv2) === appShowId;
  });
}

async function fetchHeartbeatViewTripRows() {
  return airtableList(TABLE_WATCH_TRIPS, {
    view: VIEW_WATCH_TRIPS,
    pageSize: 100,
    "fields[]": WATCH_TRIPS_HEARTBEAT_FIELDS,
  });
}

function hasManualTimeOverride(fields) {
  return boolValue(fields?.manual_time_overide) || boolValue(fields?.manual_time_override);
}

function applyManualTimeOverrideToTripFields(fields, existingRow) {
  if (!hasManualTimeOverride(existingRow?.fields || {})) return false;
  for (const fieldName of WATCH_TRIPS_MANUAL_TIME_FIELDS) {
    delete fields[fieldName];
  }
  return true;
}

async function fetchTripScheduleBackfillRows(appShowId) {
  const rows = await airtableList(TABLE_WATCH_TRIPS, {
    pageSize: 100,
    "fields[]": [
      "show_id",
      "app_show_id",
      "app_show_idv2",
      "schedule_show_datev2",
      "scheduled_date",
      "schedule_show_datev2 (from watch_schedule)",
      "show_date",
      "date",
    ],
  });

  return rows.filter((row) => {
    const fields = row?.fields || {};
    return numOrNull(fields.show_id) === appShowId ||
      numOrNull(fields.app_show_id) === appShowId ||
      numOrNull(fields.app_show_idv2) === appShowId;
  });
}

function buildTripScheduleBackfillUpdates(rows, watchTripsFieldSet) {
  const updates = [];

  for (const row of rows) {
    const fields = row?.fields || {};
    const resolvedScheduleDate = resolveTripScheduleDate(fields);
    if (!resolvedScheduleDate) continue;

    const directScheduleShowDate = toIsoDateOnly(firstValue(fields.schedule_show_datev2));
    const directScheduledDate = strOrNull(firstValue(fields.scheduled_date));
    const patchFields = {};

    if (watchTripsFieldSet.has("schedule_show_datev2") && directScheduleShowDate !== resolvedScheduleDate) {
      patchFields.schedule_show_datev2 = resolvedScheduleDate;
    }

    if (watchTripsFieldSet.has("scheduled_date") && directScheduledDate !== resolvedScheduleDate) {
      patchFields.scheduled_date = resolvedScheduleDate;
    }

    if (Object.keys(patchFields).length) {
      updates.push({
        id: row.id,
        fields: patchFields,
      });
    }
  }

  return updates;
}

function chooseExistingWinner(rows, heartbeatViewIdSet) {
  if (!rows.length) return null;
  const scored = rows.map((row, index) => {
    let score = 0;
    if (boolValue(row?.fields?.archive) || boolValue(row?.fields?.inactive) || !isBlank(row?.fields?.dropped_at)) {
      score -= 100;
    }
    if (heartbeatViewIdSet.has(row.id)) score += 10;
    if (boolValue(row?.fields?.is_current_scope)) score += 5;
    if (firstValue(row?.fields?.heartbeat)) score += 3;
    score -= index;
    return { row, score };
  });
  scored.sort((a, b) => b.score - a.score);
  return scored[0].row;
}

function buildDuplicateTripArchiveUpdates(existingRows, heartbeatViewIdSet, nowIso, dateOnly, watchTripsFieldSet) {
  const grouped = new Map();
  for (const row of existingRows || []) {
    const fields = row?.fields || {};
    const key = normalizeKey(firstValue(fields.trips_key)) || tripRowKeyFromFields(fields);
    if (!key) continue;
    if (!grouped.has(key)) grouped.set(key, []);
    grouped.get(key).push(row);
  }

  const updates = [];
  const maybeSet = (fields, name, value) => {
    if (!watchTripsFieldSet.has(name)) return;
    setIfPresent(fields, name, value);
  };

  for (const rows of grouped.values()) {
    const activeRows = rows.filter((row) => {
      const fields = row?.fields || {};
      return !boolValue(fields.archive) && !boolValue(fields.inactive) && isBlank(fields.dropped_at);
    });
    if (rows.length <= 1) continue;

    const winner = chooseExistingWinner(activeRows.length ? activeRows : rows, heartbeatViewIdSet);
    for (const row of rows) {
      if (!winner || row.id === winner.id) continue;
      const currentFields = row?.fields || {};
      if (boolValue(currentFields.archive) && boolValue(currentFields.inactive) && !isBlank(currentFields.dropped_at)) {
        continue;
      }
      const fields = {};
      maybeSet(fields, "is_current_scope", false);
      maybeSet(fields, "inactive", true);
      maybeSet(fields, "archive", true);
      maybeSet(fields, "dropped_at", dateOnly);
      maybeSet(fields, "run_time", nowIso);
      maybeSet(fields, "last_seen_at", dateOnly);
      updates.push({ id: row.id, fields });
    }
  }

  return updates;
}

async function fetchJson(url, audit = {}) {
  const fetched = await fetchTextWithConfiguredTransport(url, async (endpoint) => {
    const response = await fetchWithRetry(endpoint, {
      method: "GET",
      headers: { Accept: "application/json" },
    });
    const text = await response.text().catch(() => "");
    return { response, text, endpoint };
  });

  const { response, text } = fetched;
  const endpoint = fetched.endpoint || url;
  if (!response.ok) throw new Error(`Fetch failed (${response.status}): ${text.slice(0, 1200)}`);
  let json = null;
  try {
    json = JSON.parse(text);
  } catch {
    throw new Error(`Response was not valid JSON. First 1200 chars:\n${text.slice(0, 1200)}`);
  }

  try {
    assertValidPayload({
      payload: json,
      text,
      response,
      lane: "trips_dailyv2",
      endpoint,
      expectedPredicate: (payload) => Array.isArray(payload?.trips) ||
        collectTripCandidates(payload).length > 0 ||
        payload?.people !== undefined ||
        payload?.entries !== undefined ||
        payload?.classes !== undefined ||
        payload?.show_id !== undefined,
    });
  } catch (error) {
    throw error;
  }
  await recordPayloadPingAudit(endpoint, response, text, {
    ...audit,
    transport: fetched.transport,
  });
  return json;
}

function linkOne(recordId) {
  return recordId ? [recordId] : undefined;
}

function pickWritableFields(fieldSet, values) {
  const fields = {};
  for (const [name, value] of Object.entries(values || {})) {
    if (!fieldSet.has(name)) continue;
    if (value === undefined || value === null) continue;
    if (typeof value === "string" && value.trim() === "") continue;
    fields[name] = value;
  }
  return fields;
}

function buildPeopleEndpoint(sourceId, heartbeat) {
  const customerId = heartbeat.customer_id ?? CUSTOMER_ID;
  return `${BASE_URL}/people/${encodeURIComponent(sourceId)}?pid=${encodeURIComponent(sourceId)}&show_id=${encodeURIComponent(heartbeat.app_show_id)}&customer_id=${encodeURIComponent(customerId)}`;
}

function escapeRegExp(value) {
  return String(value).replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function peopleFallbackDirs() {
  const raw = strOrNull(process.env.SGL_PEOPLE_FALLBACK_DIRS);
  const values = raw
    ? raw.split(path.delimiter)
    : DEFAULT_PEOPLE_FALLBACK_DIRS;

  return values
    .map((value) => strOrNull(value))
    .filter(Boolean);
}

function collectFilesRecursive(dirPath, out = []) {
  if (!dirPath || !fs.existsSync(dirPath)) return out;

  for (const entry of fs.readdirSync(dirPath, { withFileTypes: true })) {
    const fullPath = path.join(dirPath, entry.name);
    if (entry.isDirectory()) collectFilesRecursive(fullPath, out);
    else if (entry.isFile()) out.push(fullPath);
  }

  return out;
}

function candidatePeopleFallbackFiles(pid, appShowId) {
  const pidText = escapeRegExp(pid);
  const showText = escapeRegExp(appShowId);
  const peopleJson = new RegExp(`^people_${pidText}_show_${showText}_[0-9]+\\.json$`, "i");

  const candidates = [];
  for (const dirPath of peopleFallbackDirs()) {
    for (const filePath of collectFilesRecursive(dirPath)) {
      const name = path.basename(filePath);
      if (name.endsWith(".pretty.json")) continue;
      if (!peopleJson.test(name)) continue;

      const stat = fs.statSync(filePath);
      if (!stat.size || stat.size <= 2) continue;
      candidates.push({ filePath, size: stat.size, mtimeMs: stat.mtimeMs });
    }
  }

  candidates.sort((left, right) => right.mtimeMs - left.mtimeMs);
  return candidates;
}

function loadPeopleFallbackPayload(pid, appShowId) {
  const candidates = candidatePeopleFallbackFiles(pid, appShowId);
  const failures = [];

  for (const candidate of candidates) {
    let text = "";
    try {
      text = fs.readFileSync(candidate.filePath, "utf8");
      const payload = JSON.parse(text);
      assertValidPayload({
        payload,
        text,
        response: {
          status: 200,
          headers: {
            get(name) {
              return String(name || "").toLowerCase() === "content-length"
                ? String(Buffer.byteLength(text || "", "utf8"))
                : null;
            },
          },
        },
        lane: "trips_dailyv2_fallback",
        endpoint: candidate.filePath,
        expectedPredicate: (data) => Array.isArray(data?.trips) ||
          collectTripCandidates(data).length > 0 ||
          data?.people !== undefined ||
          data?.entries !== undefined ||
          data?.classes !== undefined ||
          data?.show_id !== undefined,
      });
      return {
        ok: true,
        payload,
        file_path: candidate.filePath,
        body_length: Buffer.byteLength(text || "", "utf8"),
        mtime_ms: candidate.mtimeMs,
      };
    } catch (error) {
      failures.push({
        file_path: candidate.filePath,
        reason: String(error?.message || error).slice(0, 300),
      });
    }
  }

  return { ok: false, file_path: null, body_length: null, failures };
}

function peoplePayloadFileName(pid, appShowId, epochSeconds = Math.floor(Date.now() / 1000)) {
  return `people_${pid}_show_${appShowId}_${epochSeconds}.json`;
}

function writeEarlyPeoplePayload(pid, appShowId, payload, epochSeconds) {
  const dirPath = EARLY_PEOPLE_PAYLOAD_DIR;
  fs.mkdirSync(dirPath, { recursive: true });
  const filePath = path.join(dirPath, peoplePayloadFileName(pid, appShowId, epochSeconds));
  fs.writeFileSync(filePath, `${JSON.stringify(payload, null, 2)}\n`, "utf8");
  const stat = fs.statSync(filePath);
  return { ok: true, file_path: filePath, body_length: stat.size };
}

async function fetchPeoplePayloadWithFallback(tenantId, heartbeat, { runId = null, lastRun = null } = {}) {
  const endpoint = buildPeopleEndpoint(tenantId, heartbeat);
  try {
    const payload = await fetchJson(endpoint, {
      app_show_id: heartbeat.app_show_id,
      app_sql_date: heartbeat.app_sql_date,
      pid: tenantId,
      people_show_id: heartbeat.app_show_id,
      run_id: runId,
      last_run: lastRun,
      source: "people_endpoint",
    });
    let cache = null;
    if (!DRY_RUN) {
      try {
        cache = writeEarlyPeoplePayload(tenantId, heartbeat.app_show_id, payload, Math.floor(Date.now() / 1000));
      } catch (error) {
        cache = { ok: false, reason: String(error?.message || error).slice(0, 300) };
      }
    } else {
      cache = { skipped: true, reason: "dry_run" };
    }
    return { payload, endpoint, source: "live_people_endpoint", fallback: null, cache };
  } catch (error) {
    const softPayload = isSoftPayloadError(error);
    if (softPayload) {
      await recordSoftPayloadAudit(error, endpoint, {
        app_show_id: heartbeat.app_show_id,
        app_sql_date: heartbeat.app_sql_date,
        pid: tenantId,
        people_show_id: heartbeat.app_show_id,
        run_id: runId,
        last_run: lastRun,
        source: "people_endpoint",
      });
    }

    const fallback = loadPeopleFallbackPayload(tenantId, heartbeat.app_show_id);
    if (fallback.ok) {
      return {
        payload: fallback.payload,
        endpoint,
        source: "people_payload_fallback",
        fallback,
        cache: null,
        fetch_error: String(error?.message || error).slice(0, 300),
        soft_payload: softPayload,
      };
    }

    error.peopleFallback = fallback;
    throw error;
  }
}

function extractPeopleShowId(payload) {
  return numOrNull(pickFirst(payload?.show_id, payload?.people?.show_id));
}

function extractPayloadTrips(payload) {
  if (Array.isArray(payload?.trips)) return payload.trips;
  return collectTripCandidates(payload);
}

function buildNumericRunId(nowIso) {
  const ms = Date.parse(nowIso);
  return Number.isFinite(ms) ? ms : Date.now();
}

async function upsertByNumberId({
  tableName,
  fieldSet,
  fieldId,
  fieldPid,
  fieldInactive,
  fieldRunId,
  fieldLastRun,
  fieldNewFlag,
  fieldPidMismatch,
  rows,
}) {
  if (!rows.length) {
    return {
      table: tableName,
      created_planned: 0,
      updated_planned: 0,
      writes: { created: 0, updated: 0, create_failures: [], update_failures: [] },
    };
  }

  const existingRows = await airtableList(tableName, {
    pageSize: 100,
    "fields[]": [fieldId, fieldPid],
  });

  const existingById = new Map();
  for (const row of existingRows) {
    const idNum = numOrNull(row?.fields?.[fieldId]);
    if (idNum === null) continue;
    if (!existingById.has(idNum)) existingById.set(idNum, row);
  }

  const creates = [];
  const updates = [];

  for (const row of rows) {
    const idNum = numOrNull(row?.[fieldId]);
    const pidNum = numOrNull(row?.[fieldPid]);
    if (idNum === null) continue;
    const existing = existingById.get(idNum);
    if (!existing) {
      creates.push({ fields: pickWritableFields(fieldSet, row) });
      continue;
    }

    const oldPid = numOrNull(existing?.fields?.[fieldPid]);
    const pidMismatch = oldPid !== null && pidNum !== null && oldPid !== pidNum;
    updates.push({
      id: existing.id,
      fields: pickWritableFields(fieldSet, {
        ...row,
        [fieldInactive]: false,
        [fieldRunId]: row[fieldRunId],
        [fieldLastRun]: row[fieldLastRun],
        [fieldNewFlag]: false,
        [fieldPidMismatch]: pidMismatch,
      }),
    });
  }

  const summary = {
    table: tableName,
    created_planned: creates.length,
    updated_planned: updates.length,
    writes: { created: 0, updated: 0, create_failures: [], update_failures: [] },
  };

  if (!DRY_RUN) {
    const createResult = await airtableCreateRecords(tableName, creates);
    const updateResult = await airtablePatchRecords(tableName, updates);
    summary.writes.created = createResult.okRows;
    summary.writes.updated = updateResult.okRows;
    summary.writes.create_failures = createResult.failedRows;
    summary.writes.update_failures = updateResult.failedRows;
  }

  return summary;
}

async function upsertActiveByKey({
  tableName,
  fieldSet,
  fieldKey,
  fieldPid,
  fieldAppSid,
  fieldInactive,
  fieldRunId,
  fieldLastRun,
  fieldNewFlag,
  fieldPidMismatch,
  rows,
  scopePid,
  scopeAppSid,
  scopeByPid = true,
  runId,
  lastRun,
}) {
  const existingRows = await airtableList(tableName, {
    pageSize: 100,
    "fields[]": [fieldKey, fieldPid, fieldAppSid],
  });

  const relevantExisting = existingRows.filter((row) => {
    const appSidNum = numOrNull(row?.fields?.[fieldAppSid]);
    if (appSidNum !== scopeAppSid) return false;
    if (!scopeByPid) return true;
    const pidNum = numOrNull(row?.fields?.[fieldPid]);
    return pidNum === scopePid;
  });

  const existingByKey = new Map();
  for (const row of relevantExisting) {
    const key = normalizeKey(row?.fields?.[fieldKey]);
    if (!key || existingByKey.has(key)) continue;
    existingByKey.set(key, row);
  }

  const keepKeys = new Set();
  const creates = [];
  const updates = [];

  for (const row of rows) {
    const key = normalizeKey(row?.[fieldKey]);
    if (!key) continue;
    keepKeys.add(key);
    const existing = existingByKey.get(key);
    if (!existing) {
      creates.push({ fields: pickWritableFields(fieldSet, row) });
      continue;
    }

    const oldPid = fieldPid ? numOrNull(existing?.fields?.[fieldPid]) : null;
    const currentPid = fieldPid ? numOrNull(row?.[fieldPid]) : null;
    const pidMismatch = fieldPid && oldPid !== null && currentPid !== null && oldPid !== currentPid;
    updates.push({
      id: existing.id,
      fields: pickWritableFields(fieldSet, {
        ...row,
        [fieldInactive]: row[fieldInactive] ?? false,
        [fieldRunId]: row[fieldRunId],
        [fieldLastRun]: row[fieldLastRun],
        [fieldNewFlag]: false,
        [fieldPidMismatch]: pidMismatch,
      }),
    });
  }

  const inactivations = [];
  for (const row of relevantExisting) {
    const key = normalizeKey(row?.fields?.[fieldKey]);
    if (!key || keepKeys.has(key)) continue;
    inactivations.push({
      id: row.id,
      fields: pickWritableFields(fieldSet, {
        [fieldInactive]: true,
        [fieldRunId]: runId,
        [fieldLastRun]: lastRun,
      }),
    });
  }

  const summary = {
    table: tableName,
    created_planned: creates.length,
    updated_planned: updates.length,
    inactivated_planned: inactivations.length,
    writes: {
      created: 0,
      updated: 0,
      inactivated: 0,
      create_failures: [],
      update_failures: [],
      inactivate_failures: [],
    },
  };

  if (!DRY_RUN) {
    const createResult = await airtableCreateRecords(tableName, creates);
    const updateResult = await airtablePatchRecords(tableName, updates);
    const inactivateResult = await airtablePatchRecords(tableName, inactivations);
    summary.writes.created = createResult.okRows;
    summary.writes.updated = updateResult.okRows;
    summary.writes.inactivated = inactivateResult.okRows;
    summary.writes.create_failures = createResult.failedRows;
    summary.writes.update_failures = updateResult.failedRows;
    summary.writes.inactivate_failures = inactivateResult.failedRows;
  }

  return summary;
}

async function upsertActiveGroups({
  tableName,
  fieldSet,
  rows,
  scopeAppSid,
  runId,
  lastRun,
}) {
  if (!fieldSet.size) {
    return {
      table: tableName,
      created_planned: 0,
      updated_planned: 0,
      inactivated_planned: 0,
      writes: {
        created: 0,
        updated: 0,
        inactivated: 0,
        create_failures: [],
        update_failures: [],
        inactivate_failures: [],
      },
      skipped: true,
    };
  }

  const existingRows = await airtableList(tableName, {
    pageSize: 100,
    "fields[]": ["key", "app_sid"],
  });

  const relevantExisting = existingRows.filter((row) => numOrNull(row?.fields?.app_sid) === scopeAppSid);
  const existingByKey = new Map();
  for (const row of relevantExisting) {
    const key = normalizeKey(row?.fields?.key);
    if (!key || existingByKey.has(key)) continue;
    existingByKey.set(key, row);
  }

  const keepKeys = new Set();
  const creates = [];
  const updates = [];

  for (const row of rows) {
    const key = normalizeKey(row?.key);
    if (!key) continue;
    keepKeys.add(key);
    const existing = existingByKey.get(key);
    if (!existing) {
      creates.push({ fields: row });
      continue;
    }
    updates.push({
      id: existing.id,
      fields: row,
    });
  }

  const inactivations = [];
  for (const row of relevantExisting) {
    const key = normalizeKey(row?.fields?.key);
    if (!key || keepKeys.has(key)) continue;
    inactivations.push({
      id: row.id,
      fields: pickWritableFields(fieldSet, {
        inactive: true,
        run_id: runId,
        last_run: lastRun,
      }),
    });
  }

  const summary = {
    table: tableName,
    created_planned: creates.length,
    updated_planned: updates.length,
    inactivated_planned: inactivations.length,
    writes: {
      created: 0,
      updated: 0,
      inactivated: 0,
      create_failures: [],
      update_failures: [],
      inactivate_failures: [],
    },
    skipped: false,
  };

  if (DRY_RUN) return summary;

  const createResult = await airtableCreateRecords(tableName, creates);
  const updateResult = await airtablePatchRecords(tableName, updates);
  const inactivateResult = await airtablePatchRecords(tableName, inactivations);

  summary.writes.created = createResult.okRows;
  summary.writes.updated = updateResult.okRows;
  summary.writes.inactivated = inactivateResult.okRows;
  summary.writes.create_failures = createResult.failedRows;
  summary.writes.update_failures = updateResult.failedRows;
  summary.writes.inactivate_failures = inactivateResult.failedRows;

  return summary;
}

function normalizeLinkIds(value) {
  if (!Array.isArray(value)) return [];
  return [...new Set(value.map((item) => strOrNull(item)).filter(Boolean))].sort();
}

function sameLinkIds(left, right) {
  return JSON.stringify(normalizeLinkIds(left)) === JSON.stringify(normalizeLinkIds(right));
}

async function fetchScopedActiveRows(tableName, fieldNames, scopeAppSid) {
  const rows = await airtableList(tableName, {
    pageSize: 100,
    "fields[]": fieldNames,
  });

  return rows.filter((row) => numOrNull(row?.fields?.app_sid) === scopeAppSid);
}

async function syncActiveTableLinks({
  scopeAppSid,
  activeGroupsFieldSet,
  activeClassesFieldSet,
  activeEntriesFieldSet,
  entryClassKeysByEntryKey,
}) {
  const summary = {
    active_groups_to_classes: { planned: 0, updated: 0, failures: [] },
    active_classes_to_groups: { planned: 0, updated: 0, failures: [] },
    active_entries_to_classes: { planned: 0, updated: 0, failures: [] },
    active_classes_to_entries: { planned: 0, updated: 0, failures: [] },
    skipped: false,
  };

  const needsGroupClassLinks = activeGroupsFieldSet.has("active_classes");
  const needsClassGroupLinks = activeClassesFieldSet.has("active_groups");
  const needsEntryClassLinks = activeEntriesFieldSet.has("active_classes");
  const needsClassEntryLinks = activeClassesFieldSet.has("active_entries");

  if (!needsGroupClassLinks && !needsClassGroupLinks && !needsEntryClassLinks && !needsClassEntryLinks) {
    summary.skipped = true;
    return summary;
  }

  let groupRows = [];
  let classRows = [];
  let entryRows = [];
  try {
    [groupRows, classRows, entryRows] = await Promise.all([
      fetchScopedActiveRows(
        TABLE_ACTIVE_GROUPS,
        ["app_sid", "class_group_id", "inactive", "active_classes"],
        scopeAppSid
      ),
      fetchScopedActiveRows(
        TABLE_ACTIVE_CLASSES,
        ["app_sid", "class_id", "class_group_id", "inactive", "active_groups", "active_entries"],
        scopeAppSid
      ),
      fetchScopedActiveRows(
        TABLE_ACTIVE_ENTRIES,
        ["app_sid", "entry_id", "inactive", "active_classes"],
        scopeAppSid
      ),
    ]);
  } catch (e) {
    summary.skipped = true;
    summary.error = String(e?.message || e).slice(0, 500);
    return summary;
  }

  const activeGroups = groupRows.filter((row) => !boolValue(row?.fields?.inactive));
  const activeClasses = classRows.filter((row) => !boolValue(row?.fields?.inactive));
  const activeEntries = entryRows.filter((row) => !boolValue(row?.fields?.inactive));

  const groupRecordIdByGroupId = new Map();
  for (const row of activeGroups) {
    const classGroupId = normalizeKey(row?.fields?.class_group_id);
    if (!classGroupId || groupRecordIdByGroupId.has(classGroupId)) continue;
    groupRecordIdByGroupId.set(classGroupId, row.id);
  }

  const classRecordIdByClassId = new Map();
  const classRecordIdsByGroupId = new Map();
  for (const row of activeClasses) {
    const classId = normalizeKey(row?.fields?.class_id);
    const classGroupId = normalizeKey(row?.fields?.class_group_id);
    if (classId && !classRecordIdByClassId.has(classId)) classRecordIdByClassId.set(classId, row.id);
    if (classGroupId) {
      if (!classRecordIdsByGroupId.has(classGroupId)) classRecordIdsByGroupId.set(classGroupId, new Set());
      classRecordIdsByGroupId.get(classGroupId).add(row.id);
    }
  }

  const entryRecordIdByEntryId = new Map();
  const entryRecordIdsByClassId = new Map();
  for (const row of activeEntries) {
    const entryId = normalizeKey(row?.fields?.entry_id);
    if (entryId && !entryRecordIdByEntryId.has(entryId)) entryRecordIdByEntryId.set(entryId, row.id);
  }
  for (const [entryKey, classKeySet] of entryClassKeysByEntryKey.entries()) {
    const entryRecordId = entryRecordIdByEntryId.get(entryKey);
    if (!entryRecordId) continue;
    for (const classKey of classKeySet) {
      if (!classKey) continue;
      if (!entryRecordIdsByClassId.has(classKey)) entryRecordIdsByClassId.set(classKey, new Set());
      entryRecordIdsByClassId.get(classKey).add(entryRecordId);
    }
  }

  const groupUpdates = [];
  if (needsGroupClassLinks) {
    for (const row of activeGroups) {
      const classGroupId = normalizeKey(row?.fields?.class_group_id);
      const desired = [...(classRecordIdsByGroupId.get(classGroupId) || new Set())].sort();
      const current = normalizeLinkIds(row?.fields?.active_classes);
      if (sameLinkIds(current, desired)) continue;
      summary.active_groups_to_classes.planned += 1;
      groupUpdates.push({ id: row.id, fields: { active_classes: desired } });
    }
  }

  const classUpdates = [];
  if (needsClassGroupLinks || needsClassEntryLinks) {
    for (const row of activeClasses) {
      const fields = {};
      if (needsClassGroupLinks) {
        const classGroupId = normalizeKey(row?.fields?.class_group_id);
        const desiredGroupLinks = groupRecordIdByGroupId.get(classGroupId) ? [groupRecordIdByGroupId.get(classGroupId)] : [];
        const currentGroupLinks = normalizeLinkIds(row?.fields?.active_groups);
        if (!sameLinkIds(currentGroupLinks, desiredGroupLinks)) {
          fields.active_groups = desiredGroupLinks;
          summary.active_classes_to_groups.planned += 1;
        }
      }
      if (needsClassEntryLinks) {
        const classId = normalizeKey(row?.fields?.class_id);
        const desiredEntryLinks = [...(entryRecordIdsByClassId.get(classId) || new Set())].sort();
        const currentEntryLinks = normalizeLinkIds(row?.fields?.active_entries);
        if (!sameLinkIds(currentEntryLinks, desiredEntryLinks)) {
          fields.active_entries = desiredEntryLinks;
          summary.active_classes_to_entries.planned += 1;
        }
      }
      if (Object.keys(fields).length) classUpdates.push({ id: row.id, fields });
    }
  }

  const entryUpdates = [];
  if (needsEntryClassLinks) {
    for (const row of activeEntries) {
      const entryId = normalizeKey(row?.fields?.entry_id);
      const desiredClassLinks = [...(entryClassKeysByEntryKey.get(entryId) || new Set())]
        .map((classKey) => classRecordIdByClassId.get(classKey))
        .filter(Boolean)
        .sort();
      const currentClassLinks = normalizeLinkIds(row?.fields?.active_classes);
      if (sameLinkIds(currentClassLinks, desiredClassLinks)) continue;
      summary.active_entries_to_classes.planned += 1;
      entryUpdates.push({ id: row.id, fields: { active_classes: desiredClassLinks } });
    }
  }

  if (DRY_RUN) return summary;

  if (groupUpdates.length) {
    const result = await airtablePatchRecords(TABLE_ACTIVE_GROUPS, groupUpdates);
    summary.active_groups_to_classes.updated = result.okRows;
    summary.active_groups_to_classes.failures = result.failedRows;
  }
  if (classUpdates.length) {
    const result = await airtablePatchRecords(TABLE_ACTIVE_CLASSES, classUpdates);
    summary.active_classes_to_groups.updated = result.okRows;
    summary.active_classes_to_entries.updated = result.okRows;
    summary.active_classes_to_groups.failures = result.failedRows;
    summary.active_classes_to_entries.failures = result.failedRows;
  }
  if (entryUpdates.length) {
    const result = await airtablePatchRecords(TABLE_ACTIVE_ENTRIES, entryUpdates);
    summary.active_entries_to_classes.updated = result.okRows;
    summary.active_entries_to_classes.failures = result.failedRows;
  }

  return summary;
}

async function buildAuxiliaryRowsForTenant({
  payload,
  tenantId,
  activeTenantRecordId,
  wwTrainerRecordId,
  heartbeat,
  showRecordId,
  runId,
  nowIso,
  dateOnly,
  scheduleByClassId,
  activeClassesFieldSet,
  activeEntriesFieldSet,
  activeGroupsFieldSet,
  wwRidersFieldSet,
  wwHorsesFieldSet,
}) {
  const trips = extractPayloadTrips(payload);
  const riderById = new Map();
  const horseById = new Map();
  const classById = new Map();
  const entryById = new Map();

  for (const trip of trips) {
    const riderId = numOrNull(pickFirst(trip?.rider_id, trip?.riderId));
    const riderName = strOrNull(pickFirst(trip?.rider_name, trip?.riderName));
    if (riderId !== null) {
      const existing = riderById.get(riderId) || { rider_id: riderId, rider_name: null };
      if (!existing.rider_name && riderName) existing.rider_name = riderName;
      riderById.set(riderId, existing);
    }

    const horseId = numOrNull(pickFirst(trip?.horse_id, trip?.horseId));
    const horseName = strOrNull(pickFirst(trip?.horse, trip?.Horse));
    const entryId = numOrNull(pickFirst(trip?.entry_id, trip?.entryId));
    const entryNumber = normalizeEntryNumber(pickFirst(trip?.entry_number, trip?.entryNumber, trip?.entry_no, trip?.entryNo, trip?.number));
    const rawClassGroupId = numOrNull(pickFirst(trip?.class_group_id, trip?.classGroupId));
    const rawGroupName = strOrNull(pickFirst(trip?.group_name, trip?.groupName));
    const rawClassGroupSequence = numOrNull(pickFirst(
      trip?.class_group_sequence,
      trip?.classGroupSequence,
      trip?.group_sequence,
      trip?.groupSequence
    ));
    const rawRingNumber = numOrNull(pickFirst(trip?.ring_number, trip?.ringNumber, trip?.ring));
    if (horseId !== null) {
      const existing = horseById.get(horseId) || {
        horse_id: horseId,
        horse: null,
        entry_id: null,
        entry_number: undefined,
      };
      if (!existing.horse && horseName) existing.horse = horseName;
      if (existing.entry_id === null && entryId !== null) existing.entry_id = entryId;
      if (existing.entry_number === undefined && entryNumber !== undefined) existing.entry_number = entryNumber;
      horseById.set(horseId, existing);
    }

    const classId = numOrNull(pickFirst(trip?.class_id, trip?.classId));
    const classNumber = normalizeEntryNumber(pickFirst(trip?.class_number, trip?.classNumber));
    const className = strOrNull(pickFirst(trip?.class_name, trip?.className));
    if (classId !== null) {
      const existing = classById.get(classId) || {
        class_id: classId,
        class_number: undefined,
        class_name: null,
        class_group_id: null,
        group_name: null,
        class_group_sequence: null,
        ring_number: null,
      };
      if (existing.class_number === undefined && classNumber !== undefined) existing.class_number = classNumber;
      if (!existing.class_name && className) existing.class_name = className;
       if (existing.class_group_id === null && rawClassGroupId !== null) existing.class_group_id = rawClassGroupId;
       if (!existing.group_name && rawGroupName) existing.group_name = rawGroupName;
       if (existing.class_group_sequence === null && rawClassGroupSequence !== null) existing.class_group_sequence = rawClassGroupSequence;
       if (existing.ring_number === null && rawRingNumber !== null) existing.ring_number = rawRingNumber;
      classById.set(classId, existing);
    }

    if (entryId !== null) {
      const existing = entryById.get(entryId) || {
        entry_id: entryId,
        entry_number: undefined,
        horse_id: null,
        horse: null,
        rider_id: null,
        rider_name: null,
        class_ids: new Set(),
        class_group_ids: new Set(),
      };
      if (existing.entry_number === undefined && entryNumber !== undefined) existing.entry_number = entryNumber;
      if (existing.horse_id === null && horseId !== null) existing.horse_id = horseId;
      if (!existing.horse && horseName) existing.horse = horseName;
      if (existing.rider_id === null && riderId !== null) existing.rider_id = riderId;
      if (!existing.rider_name && riderName) existing.rider_name = riderName;
      if (classId !== null) existing.class_ids.add(String(classId));
      if (rawClassGroupId !== null) existing.class_group_ids.add(String(rawClassGroupId));
      entryById.set(entryId, existing);
    }
  }

  const commonLinks = {
    shows: linkOne(showRecordId),
    active_tenants: linkOne(activeTenantRecordId),
    ww_trainers: linkOne(wwTrainerRecordId),
  };

  const classEndpointEnrichment = {
    skipped: true,
    reason: "people_payload_plus_watch_schedule_only",
    failures: [],
  };

  const riderRows = [...riderById.values()].map((row) => pickWritableFields(wwRidersFieldSet, {
    rider_id: row.rider_id,
    rider_name: row.rider_name || undefined,
    name: row.rider_name || undefined,
    pid: Number(tenantId),
    app_sid: heartbeat.app_show_id,
    app_sql_date: heartbeat.app_sql_date,
    shows: commonLinks.shows,
    active_tenants: commonLinks.active_tenants,
    ww_trainers: commonLinks.ww_trainers,
    inactive: false,
    run_id: runId,
    last_run: dateOnly,
    new_rider: true,
    pid_mismatch: false,
  }));

  const horseRows = [...horseById.values()].map((row) => pickWritableFields(wwHorsesFieldSet, {
    horse_id: row.horse_id,
    horse: row.horse || undefined,
    pid: Number(tenantId),
    trainer_id: Number(tenantId),
    app_sid: heartbeat.app_show_id,
    app_sql_date: heartbeat.app_sql_date,
    shows: commonLinks.shows,
    active_tenants: commonLinks.active_tenants,
    ww_trainers: commonLinks.ww_trainers,
    entry_id: row.entry_id ?? undefined,
    entry_number: row.entry_number,
    inactive: false,
    run_id: runId,
    last_run: dateOnly,
    new_horse: true,
    pid_mismatch: false,
  }));

  const enrichedClassRows = [...classById.values()].map((row) => {
    const schedule = scheduleByClassId.get(String(row.class_id));
    const resolvedScheduleDate = schedule?.schedule_show_datev2;
    const resolvedEstimatedStart = schedule?.estimated_start_time;
    const resolvedClassGroupId = row.class_group_id ?? schedule?.class_group_id;
    const resolvedRingNumber = row.ring_number ?? schedule?.ring_number;
    const resolvedGroupName = row.group_name || schedule?.group_name || undefined;
    const resolvedClassGroupSequence = row.class_group_sequence ?? schedule?.class_group_sequence;
    const classMatchesAppDate = resolvedScheduleDate === heartbeat.app_sql_date;
    return {
      key: String(row.class_id),
      app_sid: heartbeat.app_show_id,
      app_sql_date: heartbeat.app_sql_date,
      app_show_idv2: heartbeat.app_show_id,
      app_sql_datev2: heartbeat.app_sql_date,
      app_dow_rawv2: heartbeat.app_dow_raw,
      is_today: classMatchesAppDate,
      " is_today": classMatchesAppDate,
      shows: commonLinks.shows,
      pid: Number(tenantId),
      class_id: row.class_id,
      class_number: row.class_number,
      class_name: row.class_name || undefined,
      watch_schedule: schedule?.recordId ? linkOne(schedule.recordId) : undefined,
      class_group_id: resolvedClassGroupId,
      group_name: resolvedGroupName,
      class_group_sequence: resolvedClassGroupSequence,
      ring_number: resolvedRingNumber,
      total_trips: schedule?.total_trips,
      status: undefined,
      class_type: schedule?.class_type,
      schedule_sequencetype: schedule?.schedule_sequencetype,
      show_id: heartbeat.app_show_id,
      schedule_date: resolvedScheduleDate || undefined,
      scheduled_date: resolvedScheduleDate || undefined,
      scheduled_estimated_start_time: resolvedEstimatedStart || undefined,
      inactive: false,
      run_id: runId,
      last_run: dateOnly,
      new_class_id: true,
      pid_mismatch: false,
    };
  });

  const classRows = enrichedClassRows.map((row) => pickWritableFields(activeClassesFieldSet, row));

  const groupById = new Map();
  for (const row of enrichedClassRows) {
    const classGroupId = numOrNull(row.class_group_id);
    if (classGroupId === null) continue;

    const key = String(classGroupId);
    const existing = groupById.get(key) || {
      key,
      app_sid: heartbeat.app_show_id,
      app_sql_date: heartbeat.app_sql_date,
      shows: commonLinks.shows,
      class_group_id: classGroupId,
      class_group_sequence: undefined,
      group_name: null,
      ring_number: undefined,
      estimated_start_time: null,
      estimated_end_time: null,
      total: 0,
      inactive: false,
      run_id: runId,
      last_run: dateOnly,
    };

    const totalTrips = numOrNull(row.total_trips);
    existing.class_group_sequence = existing.class_group_sequence ?? numOrNull(row.class_group_sequence);
    existing.group_name = existing.group_name || strOrNull(row.group_name);
    existing.ring_number = existing.ring_number ?? numOrNull(row.ring_number);
    existing.estimated_start_time = minTimeText(existing.estimated_start_time, strOrNull(row.scheduled_estimated_start_time ?? row.estimated_start_time));
    existing.estimated_end_time = maxTimeText(existing.estimated_end_time, strOrNull(row.estimated_end_time));
    existing.total += totalTrips ?? 0;
    groupById.set(key, existing);
  }

  const groupRows = [...groupById.values()].map((row) => pickWritableFields(activeGroupsFieldSet, row));

  const entryRows = [...entryById.values()].map((row) => {
    const singleClassId = row.class_ids.size === 1 ? numOrNull([...row.class_ids][0]) : null;
    const singleClassGroupId = row.class_group_ids.size === 1 ? numOrNull([...row.class_group_ids][0]) : null;

    return pickWritableFields(activeEntriesFieldSet, {
      key: String(row.entry_id),
      app_sid: heartbeat.app_show_id,
      app_sql_date: heartbeat.app_sql_date,
      shows: commonLinks.shows,
      pid: Number(tenantId),
      entry_id: row.entry_id,
      entry_number: row.entry_number,
      horse_id: row.horse_id ?? undefined,
      horse: row.horse || undefined,
      rider_id: row.rider_id ?? undefined,
      rider_name: row.rider_name || undefined,
      class_id: singleClassId ?? undefined,
      class_group_id: singleClassGroupId ?? undefined,
      ww_trainers: commonLinks.ww_trainers,
      inactive: false,
      run_id: runId,
      last_run: dateOnly,
      new_entry: true,
      pid_mismatch: false,
    });
  });

  return {
    tripCount: trips.length,
    riderRows,
    horseRows,
    classRows,
    groupRows,
    entryRows,
    entryClassLinks: [...entryById.values()].map((row) => ({
      entry_key: String(row.entry_id),
      class_keys: [...row.class_ids].map((value) => normalizeKey(value)).filter(Boolean),
    })),
    classEndpointEnrichment,
  };
}

function buildCurrentFields(row, heartbeat, showRecordId, nowIso, dateOnly, currentScopeStatus, watchTripsFieldSet) {
  const fields = {};
  const resolvedScheduleDate = resolveTripScheduleDate(row);
  const resolvedScheduledDate = resolvedScheduleDate;
  const isActiveForScope = resolvedScheduledDate === heartbeat.app_sql_date;
  const scheduleTieBreaker = scheduleTieBreakerFromKey(row.schedule_key);
  const tripKeys = buildTripKeyParts({
    sid: heartbeat.app_show_id,
    sqlDate: resolvedScheduleDate || heartbeat.app_sql_date,
    ringNumber: row.ring_number,
    classNumber: row.class_number,
    tieBreaker: scheduleTieBreaker,
    pid: row.pid,
    entryNumber: row.entry_number,
    time: row.estimated_start_time,
    cgid: row.class_group_id,
  });
  const maybeSet = (name, value) => {
    if (!watchTripsFieldSet.has(name)) return;
    setIfPresent(fields, name, value);
  };

  if (isActiveForScope) maybeSet("heartbeat", [heartbeat.recordId]);
  maybeSet("shows", showRecordId ? [showRecordId] : undefined);
  Object.assign(fields, buildScopeFieldPatch(watchTripsFieldSet, heartbeat));
  maybeSet("watch_schedule", row.watch_schedule_record_id ? [row.watch_schedule_record_id] : undefined);
  maybeSet("entryxclasses_uuid", row.entryxclasses_uuid);
  maybeSet("schedule_key", tripKeys.scheduleKey);
  maybeSet("schedule_short", tripKeys.scheduleShort);
  maybeSet("trips_key", tripKeys.tripsKey);
  maybeSet("trips_short_key", tripKeys.tripsShortKey);
  maybeSet("full_nesting_key", tripKeys.fullNestingKey);
  maybeSet("show_id", heartbeat.app_show_id);
  maybeSet("show_date", resolvedScheduleDate || heartbeat.app_sql_date);
  maybeSet("app_show_id", heartbeat.app_show_id);
  maybeSet("app_sql_date", heartbeat.app_sql_date);
  maybeSet("app_sid", heartbeat.app_show_id);
  maybeSet("app_dt", heartbeat.app_sql_date);
  maybeSet("app_time", heartbeat.app_time);
  maybeSet("mode", heartbeat.mode);
  maybeSet("date", heartbeat.app_sql_date);
  maybeSet("dow_raw", heartbeat.app_dow_raw);
  maybeSet("shifted_to_next_day", heartbeat.shifted_to_next_day);
  maybeSet("app_show_idv2", heartbeat.app_show_id);
  maybeSet("app_sql_datev2", heartbeat.app_sql_date);
  maybeSet("app_dow_rawv2", heartbeat.app_dow_raw);
  maybeSet("shifted_to_next_dayv2", heartbeat.shifted_to_next_day);
  maybeSet("scope_run_id", heartbeat.scope_run_id);
  maybeSet("is_current_scope", isActiveForScope);
  maybeSet("scope_status", currentScopeStatus);
  maybeSet("inactive", false);
  maybeSet("archive", false);
  maybeSet("last_seen_at", dateOnly);
  maybeSet("dropped_at", null);
  maybeSet("run_id", heartbeat.scope_run_id);
  maybeSet("run_time", nowIso);
  maybeSet("pid", row.pid);
  maybeSet("entry_id", row.entry_id);
  maybeSet("entry_number", row.entry_number);
  maybeSet("horse", row.horse);
  maybeSet("class_id", row.class_id);
  maybeSet("class_number", row.class_number);
  maybeSet("class_name", row.class_name);
  maybeSet("schedule_sequencetype", row.schedule_sequencetype);
  maybeSet("class_type", row.class_type);
  maybeSet("class_group_id", row.class_group_id);
  maybeSet("group_name", row.group_name);
  maybeSet("class_groupxclasses_id", row.class_groupxclasses_id);
  maybeSet("ring_number", row.ring_number);
  maybeSet("estimated_start_time", row.estimated_start_time);
  maybeSet("schedule_starttime", row.schedule_starttime);
  maybeSet("estimated_end_time", row.estimated_end_time);
  maybeSet("total_trips", row.total_trips);
  maybeSet("completed_trips", row.completed_trips);
  maybeSet("latest_estimated_start_time", row.estimated_start_time);
  maybeSet("latest_ingested_at", nowIso);
  maybeSet("class_group_sequence", row.class_group_sequence);
  maybeSet("status", row.status);
  maybeSet("order_of_go", row.order_of_go);
  maybeSet("rider_name", row.rider_name);
  maybeSet("rider_id", row.rider_id);
  maybeSet("placing", row.placing);
  maybeSet("sgl_token_raw", row.sgl_token_raw);
  maybeSet("sgl_token_prefix", row.sgl_token_prefix);
  maybeSet("sgl_token_length", row.sgl_token_length);
  maybeSet("sgl_token_hash", row.sgl_token_hash);
  maybeSet("schedule_show_datev2", resolvedScheduleDate);
  maybeSet("scheduled_date", resolvedScheduledDate);
  maybeSet("is_missing", false);

  return fields;
}

function buildDroppedFields(heartbeat, nowIso, dateOnly, droppedScopeStatus, watchTripsFieldSet) {
  const fields = {};
  const maybeSet = (name, value) => {
    if (!watchTripsFieldSet.has(name)) return;
    setIfPresent(fields, name, value);
  };

  Object.assign(fields, buildScopeFieldPatch(watchTripsFieldSet, heartbeat));
  maybeSet("show_id", heartbeat.app_show_id);
  maybeSet("show_date", heartbeat.app_sql_date);
  maybeSet("app_show_id", heartbeat.app_show_id);
  maybeSet("app_sql_date", heartbeat.app_sql_date);
  maybeSet("app_sid", heartbeat.app_show_id);
  maybeSet("app_dt", heartbeat.app_sql_date);
  maybeSet("app_time", heartbeat.app_time);
  maybeSet("mode", heartbeat.mode);
  maybeSet("date", heartbeat.app_sql_date);
  maybeSet("dow_raw", heartbeat.app_dow_raw);
  maybeSet("shifted_to_next_day", heartbeat.shifted_to_next_day);
  maybeSet("app_show_idv2", heartbeat.app_show_id);
  maybeSet("app_sql_datev2", heartbeat.app_sql_date);
  maybeSet("app_dow_rawv2", heartbeat.app_dow_raw);
  maybeSet("shifted_to_next_dayv2", heartbeat.shifted_to_next_day);
  maybeSet("scope_run_id", heartbeat.scope_run_id);
  maybeSet("is_current_scope", false);
  maybeSet("scope_status", droppedScopeStatus);
  maybeSet("inactive", true);
  maybeSet("archive", true);
  maybeSet("dropped_at", dateOnly);
  maybeSet("run_id", heartbeat.scope_run_id);
  maybeSet("run_time", nowIso);
  maybeSet("last_seen_at", dateOnly);

  return fields;
}

function rowScheduledDateMatchesScope(row, heartbeat) {
  const resolvedScheduledDate = resolveTripScheduleDate(row);
  return resolvedScheduledDate === heartbeat.app_sql_date;
}

function selectTripRowsForWriteScope(uniqueRows) {
  return [...uniqueRows.values()].filter((row) => resolveTripScheduleDate(row));
}

function scheduleRowMatchesHeartbeat(row, heartbeat) {
  const fields = row?.fields || {};
  if (boolValue(fields.archive) || boolValue(fields.inactive) || !isBlank(fields.dropped_at)) return false;
  const rowShowId = numOrNull(fields.show_id) ?? numOrNull(fields.app_show_idv2);
  const rowDate = resolveTripScheduleDate(fields);
  return rowShowId === heartbeat.app_show_id && rowDate === heartbeat.app_sql_date;
}

function tripRowMatchesHeartbeat(row, heartbeat) {
  const fields = row?.fields || {};
  const rowShowId = numOrNull(fields.show_id) ?? numOrNull(fields.app_show_idv2) ?? numOrNull(fields.app_show_id);
  const rowDate = resolveTripScheduleDate(fields);
  return rowShowId === heartbeat.app_show_id && rowDate === heartbeat.app_sql_date;
}

function scheduleJoinClassCount(scheduleByClassId) {
  const classIdCount = scheduleByClassId instanceof Map ? scheduleByClassId.size : 0;
  const classNumberCount = scheduleByClassId?.byClassNumber instanceof Map ? scheduleByClassId.byClassNumber.size : 0;
  return Math.max(classIdCount, classNumberCount);
}

async function runTripsForHeartbeatScope({ heartbeatScope, nowIso, dateOnly, runId }) {
  const heartbeat = heartbeatScope.heartbeat;
  const [
    watchTripsFieldSet,
    scopeStatusChoices,
    showRecordId,
    scheduleRows,
    activeTenantRows,
    wwTrainerRecordIdByPid,
    wwRidersFieldSet,
    wwHorsesFieldSet,
  ] = await Promise.all([
    fetchTableWritableFieldSet(TABLE_WATCH_TRIPS),
    fetchScopeStatusChoices(TABLE_WATCH_TRIPS).catch(() => new Set()),
    fetchShowRecordId(heartbeat.app_show_id).catch(() => null),
    fetchWatchScheduleRows(),
    fetchActiveTenantRows(),
    fetchWwTrainerRecordIdByPid().catch(() => new Map()),
    fetchTableFieldSet(TABLE_WW_RIDERS),
    fetchTableFieldSet(TABLE_WW_HORSES),
  ]);
  const activeGroupsFieldSet = new Set();
  const activeClassesFieldSet = new Set();
  const activeEntriesFieldSet = new Set();

  const currentScopeStatus = scopeStatusChoices.has("current") ? "current" : null;
  const droppedScopeStatus = scopeStatusChoices.has("dropped") ? "dropped" : null;
  const scopedScheduleRows = scheduleRows.filter((row) => scheduleRowMatchesHeartbeat(row, heartbeat));
  const targetScheduleRows = scopedScheduleRows.filter((row) => boolValue(row?.fields?.is_target));
  const scheduleRowsForTripJoin = targetScheduleRows.length ? targetScheduleRows : scopedScheduleRows;
  const scheduleByClassId = buildScheduleMap(scheduleRowsForTripJoin);
  const scheduleJoinClasses = scheduleJoinClassCount(scheduleByClassId);
  const activeTenantMap = new Map();
  for (const row of activeTenantRows) {
    if (!row?.tenant_id || activeTenantMap.has(row.tenant_id)) continue;
    activeTenantMap.set(row.tenant_id, row);
  }
  const activeTenantIds = [...activeTenantMap.keys()];

  if (!activeTenantIds.length) {
    return {
      ok: true,
      run_status: "NOOP",
      reason: "No active tenant_id values found from active_tenants view",
      app_show_id: heartbeat.app_show_id,
      app_sql_date: heartbeat.app_sql_date,
      show_target_scope: heartbeatScope.show_target_scope,
    };
  }

  const peopleFailures = [];
  const tenantSummaries = [];
  const normalizedRows = [];
  const outsideSchedule = [];
  const uniqueRows = new Map();
  const uniqueGroupRows = new Map();
  const uniqueClassRows = new Map();
  const uniqueEntryRows = new Map();
  const entryClassKeysByEntryKey = new Map();

  const preparedTenants = [];
  const softPayloadSamples = [];

  for (const tenantId of activeTenantIds) {
    const tenantRow = activeTenantMap.get(tenantId) || null;
    let endpoint = buildPeopleEndpoint(tenantId, heartbeat);
    let payload = null;
    let payloadSource = "live_people_endpoint";
    let peopleFallback = null;
    let peopleCache = null;
    try {
      const fetchResult = await fetchPeoplePayloadWithFallback(tenantId, heartbeat, {
        runId,
        lastRun: dateOnly,
      });
      payload = fetchResult.payload;
      endpoint = fetchResult.endpoint;
      payloadSource = fetchResult.source;
      peopleFallback = fetchResult.fallback;
      peopleCache = fetchResult.cache;
    } catch (error) {
      const softPayload = isSoftPayloadError(error);
      peopleFailures.push({
        tenant_id: tenantId,
        endpoint,
        reason: String(error?.message || error).slice(0, 300),
        soft_payload: softPayload,
        fallback: error?.peopleFallback || null,
      });
      tenantSummaries.push({
        tenant_id: tenantId,
        endpoint,
        status: softPayload ? "soft_payload_blocked" : "fetch_failed",
        fallback: error?.peopleFallback || null,
      });
      if (softPayload) {
        softPayloadSamples.push({
          tenant_id: tenantId,
          endpoint,
          reason: error?.reason || "soft_payload",
          body_length: error?.body_length ?? null,
          content_length: error?.content_length ?? null,
        });
      }
      continue;
    }

    const peopleShowId = extractPeopleShowId(payload);
    if (peopleShowId !== null && peopleShowId !== heartbeat.app_show_id) {
      peopleFailures.push({
        tenant_id: tenantId,
        endpoint,
        reason: `people_show_id_conflict:${peopleShowId}`,
      });
      tenantSummaries.push({
        tenant_id: tenantId,
        endpoint,
        status: "show_id_conflict",
        people_show_id: peopleShowId,
      });
      continue;
    }

    tenantSummaries.push({
      tenant_id: tenantId,
      endpoint,
      status: "payload_ready",
      source: payloadSource,
      fallback_file_path: peopleFallback?.file_path || null,
      cached_file_path: peopleCache?.file_path || null,
    });

    const wwTrainerRecordId = wwTrainerRecordIdByPid.get(String(tenantId)) || null;
    const auxiliaryRows = await buildAuxiliaryRowsForTenant({
      payload,
      tenantId,
      activeTenantRecordId: tenantRow?.recordId || null,
      wwTrainerRecordId,
      heartbeat,
      showRecordId,
      runId,
      nowIso,
      dateOnly,
      scheduleByClassId,
      activeClassesFieldSet,
      activeEntriesFieldSet,
      activeGroupsFieldSet,
      wwRidersFieldSet,
      wwHorsesFieldSet,
    });

    preparedTenants.push({
      tenantId,
      endpoint,
      payload,
      payloadSource,
      peopleFallback,
      peopleCache,
      auxiliaryRows,
    });
  }

  if (softPayloadSamples.length && preparedTenants.length === 0) {
    return {
      ok: false,
      dry_run: DRY_RUN,
      run_status: "SOFT_PAYLOAD_BLOCKED",
      reason: "soft_payload_empty",
      app_show_id: heartbeat.app_show_id,
      app_sql_date: heartbeat.app_sql_date,
      active_tenant_ids: activeTenantIds.length,
      show_target_scope: heartbeatScope.show_target_scope,
      people_failures: peopleFailures,
      tenant_summaries: tenantSummaries,
      soft_payload_samples: softPayloadSamples.slice(0, 10),
      writes_blocked: true,
    };
  }

  for (const prepared of preparedTenants) {
    const { tenantId, endpoint, payload, payloadSource, peopleFallback, peopleCache, auxiliaryRows } = prepared;

    const riderSync = await upsertByNumberId({
      tableName: TABLE_WW_RIDERS,
      fieldSet: wwRidersFieldSet,
      fieldId: "rider_id",
      fieldPid: "pid",
      fieldInactive: "inactive",
      fieldRunId: "run_id",
      fieldLastRun: "last_run",
      fieldNewFlag: "new_rider",
      fieldPidMismatch: "pid_mismatch",
      rows: auxiliaryRows.riderRows,
    });

    const horseSync = await upsertByNumberId({
      tableName: TABLE_WW_HORSES,
      fieldSet: wwHorsesFieldSet,
      fieldId: "horse_id",
      fieldPid: "pid",
      fieldInactive: "inactive",
      fieldRunId: "run_id",
      fieldLastRun: "last_run",
      fieldNewFlag: "new_horse",
      fieldPidMismatch: "pid_mismatch",
      rows: auxiliaryRows.horseRows,
    });

    for (const row of auxiliaryRows.classRows || []) {
      const key = normalizeKey(row?.key);
      if (!key) continue;
      const existing = uniqueClassRows.get(key);
      if (!existing) {
        uniqueClassRows.set(key, { ...row });
        continue;
      }
      existing.app_show_idv2 = existing.app_show_idv2 ?? row.app_show_idv2;
      existing.app_sql_datev2 = existing.app_sql_datev2 ?? row.app_sql_datev2;
      existing.app_dow_rawv2 = existing.app_dow_rawv2 ?? row.app_dow_rawv2;
      existing.is_today = Boolean(existing.is_today) || Boolean(row.is_today);
      existing[" is_today"] = Boolean(existing[" is_today"]) || Boolean(row[" is_today"]);
      existing.class_number = existing.class_number ?? row.class_number;
      existing.class_name = existing.class_name || row.class_name;
      existing.watch_schedule = existing.watch_schedule || row.watch_schedule;
      existing.class_group_id = existing.class_group_id ?? row.class_group_id;
      existing.group_name = existing.group_name || row.group_name;
      existing.class_group_sequence = existing.class_group_sequence ?? row.class_group_sequence;
      existing.ring_number = existing.ring_number ?? row.ring_number;
      existing.total_trips = existing.total_trips ?? row.total_trips;
      existing.status = existing.status || row.status;
      existing.class_type = existing.class_type || row.class_type;
      existing.schedule_sequencetype = existing.schedule_sequencetype || row.schedule_sequencetype;
      existing.show_id = existing.show_id ?? row.show_id;
      existing.schedule_date = existing.schedule_date || row.schedule_date;
      existing.scheduled_date = existing.scheduled_date || row.scheduled_date;
      existing.scheduled_estimated_start_time = existing.scheduled_estimated_start_time || row.scheduled_estimated_start_time;
      existing.inactive = Boolean(existing.inactive) && Boolean(row.inactive);
      if (!sameValue(existing.pid, row.pid)) existing.pid_mismatch = true;
    }

    for (const row of auxiliaryRows.entryRows || []) {
      const key = normalizeKey(row?.key);
      if (!key) continue;
      const existing = uniqueEntryRows.get(key);
      if (!existing) {
        uniqueEntryRows.set(key, { ...row });
        continue;
      }
      existing.entry_number = existing.entry_number ?? row.entry_number;
      existing.horse_id = existing.horse_id ?? row.horse_id;
      existing.horse = existing.horse || row.horse;
      existing.rider_id = existing.rider_id ?? row.rider_id;
      existing.rider_name = existing.rider_name || row.rider_name;
      if (!sameValue(existing.pid, row.pid)) existing.pid_mismatch = true;
    }

    for (const row of auxiliaryRows.entryClassLinks || []) {
      const entryKey = normalizeKey(row?.entry_key);
      if (!entryKey) continue;
      const classKeySet = entryClassKeysByEntryKey.get(entryKey) || new Set();
      for (const classKey of row?.class_keys || []) {
        const normalizedClassKey = normalizeKey(classKey);
        if (normalizedClassKey) classKeySet.add(normalizedClassKey);
      }
      entryClassKeysByEntryKey.set(entryKey, classKeySet);
    }

    for (const row of auxiliaryRows.groupRows || []) {
      const key = normalizeKey(row?.key);
      if (!key) continue;
      const existing = uniqueGroupRows.get(key);
      if (!existing) {
        uniqueGroupRows.set(key, { ...row });
        continue;
      }

      existing.class_group_sequence = existing.class_group_sequence ?? numOrNull(row.class_group_sequence);
      existing.group_name = existing.group_name || strOrNull(row.group_name);
      existing.ring_number = existing.ring_number ?? numOrNull(row.ring_number);
      existing.estimated_start_time = minTimeText(existing.estimated_start_time, strOrNull(row.estimated_start_time));
      existing.estimated_end_time = maxTimeText(existing.estimated_end_time, strOrNull(row.estimated_end_time));
      existing.total = (numOrNull(existing.total) ?? 0) + (numOrNull(row.total) ?? 0);
    }

    const perTenantNormalized = normalizeTripsForScope({
      sourceIds: [tenantId],
      peoplePayloads: new Map([[tenantId, payload]]),
      scheduleByClassId,
    });

    normalizedRows.push(...perTenantNormalized.normalized_rows);
    outsideSchedule.push(...perTenantNormalized.outside_schedule);
    for (const [key, row] of perTenantNormalized.unique_rows_by_key.entries()) {
      if (!uniqueRows.has(key)) uniqueRows.set(key, row);
    }

    tenantSummaries.push({
      tenant_id: tenantId,
      endpoint,
      trip_count: auxiliaryRows.tripCount,
      empty_payload: auxiliaryRows.tripCount === 0,
      watch_trip_rows: perTenantNormalized.unique_row_count,
      outside_schedule_count: perTenantNormalized.outside_schedule.length,
      rider_sync: riderSync,
      horse_sync: horseSync,
      active_class_rows: auxiliaryRows.classRows.length,
      active_entry_rows: auxiliaryRows.entryRows.length,
      people_payload_source: payloadSource,
      people_fallback_file_path: peopleFallback?.file_path || null,
      people_cache_file_path: peopleCache?.file_path || null,
      class_detail_enrichment: auxiliaryRows.classEndpointEnrichment,
    });
  }

  const classSync = {
    skipped: true,
    reason: "active_tables_deprecated",
    table: TABLE_ACTIVE_CLASSES,
    created_planned: 0,
    updated_planned: 0,
    inactivated_planned: 0,
    writes: { created: 0, updated: 0, inactivated: 0, create_failures: [], update_failures: [], inactivate_failures: [] },
  };

  const entrySync = {
    skipped: true,
    reason: "active_tables_deprecated",
    table: TABLE_ACTIVE_ENTRIES,
    created_planned: 0,
    updated_planned: 0,
    inactivated_planned: 0,
    writes: { created: 0, updated: 0, inactivated: 0, create_failures: [], update_failures: [], inactivate_failures: [] },
  };

  const activeGroupSync = {
    skipped: true,
    reason: "active_tables_deprecated",
    table: "active_groups",
    created_planned: 0,
    updated_planned: 0,
    inactivated_planned: 0,
    writes: {
      created: 0,
      updated: 0,
      inactivated: 0,
      create_failures: [],
      update_failures: [],
      inactivate_failures: [],
    },
  };

  const activeLinkSync = {
    skipped: true,
    reason: "active_tables_deprecated",
    active_groups_to_classes: { planned: 0, updated: 0, failures: [] },
    active_classes_to_groups: { planned: 0, updated: 0, failures: [] },
    active_entries_to_classes: { planned: 0, updated: 0, failures: [] },
    active_classes_to_entries: { planned: 0, updated: 0, failures: [] },
  };

  const emptyTenantIds = tenantSummaries
    .filter((item) => item.empty_payload)
    .map((item) => item.tenant_id);
  const noTripsAudits = [];
  for (const item of tenantSummaries.filter((summary) => summary.empty_payload)) {
    noTripsAudits.push(await recordNoTripsAudit({
      tenantId: item.tenant_id,
      endpoint: item.endpoint,
      heartbeat,
      runId,
      lastRun: dateOnly,
      source: item.source,
    }));
  }

  const existingRows = await fetchExistingTripsForShow(heartbeat.app_show_id);
  const heartbeatViewRows = await fetchHeartbeatViewTripRows().catch(() => []);
  const heartbeatViewIdSet = new Set(heartbeatViewRows.map((row) => row.id));

  const groupedExisting = new Map();
  for (const row of existingRows) {
    for (const key of tripRowCandidateKeysFromFields(row?.fields || {})) {
      if (!groupedExisting.has(key)) groupedExisting.set(key, []);
      groupedExisting.get(key).push(row);
    }
  }

  const existingByKey = new Map();
  for (const [key, rows] of groupedExisting.entries()) {
    const winner = chooseExistingWinner(rows, heartbeatViewIdSet);
    if (winner) existingByKey.set(key, winner);
  }
  const duplicateArchiveUpdates = buildDuplicateTripArchiveUpdates(
    existingRows,
    heartbeatViewIdSet,
    nowIso,
    dateOnly,
    watchTripsFieldSet
  );

  const createRecords = [];
  const updateRecords = [];
  const manualTimeOverrideGuard = {
    table: TABLE_WATCH_TRIPS,
    field: "manual_time_override",
    preserved: 0,
    samples: [],
  };
  const keepKeySet = new Set();
  const scopedRows = selectTripRowsForWriteScope(uniqueRows, heartbeat);
  const skippedInvalidScheduledDate = uniqueRows.size - scopedRows.length;
  let skippedMalformedScheduleKey = 0;
  const skippedMalformedScheduleKeySamples = [];

  for (const row of scopedRows) {
    const scheduleTieBreaker = scheduleTieBreakerFromKey(row.schedule_key);
    if (row.schedule_key && String(row.schedule_key).split("|").length > 5 && !scheduleTieBreaker) {
      skippedMalformedScheduleKey += 1;
      if (skippedMalformedScheduleKeySamples.length < 10) {
        skippedMalformedScheduleKeySamples.push({
          schedule_key: row.schedule_key,
          class_number: row.class_number ?? null,
          entry_number: row.entry_number ?? null,
        });
      }
      continue;
    }
    const tripKeys = buildTripKeyParts({
      sid: heartbeat.app_show_id,
      sqlDate: resolveTripScheduleDate(row) || heartbeat.app_sql_date,
      ringNumber: row.ring_number,
      classNumber: row.class_number,
      tieBreaker: scheduleTieBreaker,
      pid: row.pid,
      entryNumber: row.entry_number,
      time: row.estimated_start_time,
      cgid: row.class_group_id,
    });
    const legacyKey = normalizeKey(row.trip_key || row.entryxclasses_uuid);
    const key = normalizeKey(tripKeys.tripsKey || legacyKey);
    if (!key) continue;
    keepKeySet.add(key);
    if (legacyKey) keepKeySet.add(legacyKey);
    const existing = existingByKey.get(key) || existingByKey.get(legacyKey);
    const fields = buildCurrentFields(row, heartbeat, showRecordId, nowIso, dateOnly, currentScopeStatus, watchTripsFieldSet);
    if (existing && applyManualTimeOverrideToTripFields(fields, existing)) {
      manualTimeOverrideGuard.preserved += 1;
      if (manualTimeOverrideGuard.samples.length < 10) {
        manualTimeOverrideGuard.samples.push({
          key,
          record_id: existing.id,
          class_number: fields.class_number ?? row.class_number ?? null,
          entry_number: fields.entry_number ?? row.entry_number ?? null,
        });
      }
    }
    if (existing) updateRecords.push({ id: existing.id, fields });
    else createRecords.push({ fields });
  }

  if (!scopedRows.length) {
    const dropUpdates = [];
    for (const row of heartbeatViewRows) {
      if (!tripRowMatchesHeartbeat(row, heartbeat)) continue;
      dropUpdates.push({
        id: row.id,
        fields: buildDroppedFields(heartbeat, nowIso, dateOnly, droppedScopeStatus, watchTripsFieldSet),
      });
    }

    const emptySummary = {
      ok: true,
      dry_run: DRY_RUN,
      run_status: "NOOP",
      reason: "No writable people trips found",
      app_show_id: heartbeat.app_show_id,
      app_sql_date: heartbeat.app_sql_date,
      active_tenant_ids: activeTenantIds.length,
      show_target_scope: heartbeatScope.show_target_scope,
      watch_schedule_classes: scheduleJoinClasses,
      normalized_rows: normalizedRows.length,
      filtered_out_scheduled_date_mismatch: 0,
      skipped_invalid_scheduled_date: skippedInvalidScheduledDate,
      outside_schedule_count: outsideSchedule.length,
      active_classes: classSync,
      active_entries: entrySync,
      empty_tenant_ids: emptyTenantIds,
      no_trips_audits: noTripsAudits,
      tenant_summaries: tenantSummaries,
      people_failures: peopleFailures,
      drops_planned: dropUpdates.length,
      duplicate_archives_planned: duplicateArchiveUpdates.length,
      active_groups: activeGroupSync,
    active_links: activeLinkSync,
    manual_time_override_guard: manualTimeOverrideGuard,
    writes: {
        created: 0,
        updated: 0,
        dropped: 0,
        duplicate_archived: 0,
        create_failures: [],
        update_failures: [],
        drop_failures: [],
        duplicate_archive_failures: [],
      },
      schedule_date_backfill: {
        planned: 0,
        updated: 0,
        failures: [],
      },
    };

    if (!DRY_RUN) {
      const dropResult = await airtablePatchRecords(TABLE_WATCH_TRIPS, dropUpdates);
      const duplicateArchiveResult = await airtablePatchRecords(TABLE_WATCH_TRIPS, duplicateArchiveUpdates);
      const backfillRows = await fetchTripScheduleBackfillRows(heartbeat.app_show_id);
      const backfillUpdates = buildTripScheduleBackfillUpdates(backfillRows, watchTripsFieldSet);
      const backfillResult = await airtablePatchRecords(TABLE_WATCH_TRIPS, backfillUpdates);
      emptySummary.writes.dropped = dropResult.okRows;
      emptySummary.writes.duplicate_archived = duplicateArchiveResult.okRows;
      emptySummary.writes.drop_failures = dropResult.failedRows;
      emptySummary.writes.duplicate_archive_failures = duplicateArchiveResult.failedRows;
      emptySummary.schedule_date_backfill.planned = backfillUpdates.length;
      emptySummary.schedule_date_backfill.updated = backfillResult.okRows;
      emptySummary.schedule_date_backfill.failures = backfillResult.failedRows;
    } else {
      const backfillRows = await fetchTripScheduleBackfillRows(heartbeat.app_show_id);
      const backfillUpdates = buildTripScheduleBackfillUpdates(backfillRows, watchTripsFieldSet);
      emptySummary.schedule_date_backfill.planned = backfillUpdates.length;
    }

    return emptySummary;
  }

  const dropUpdates = [];
  for (const row of heartbeatViewRows) {
    if (!tripRowMatchesHeartbeat(row, heartbeat)) continue;
    const key = tripRowKeyFromFields(row?.fields || {});
    if (!key || keepKeySet.has(key)) continue;
    dropUpdates.push({
      id: row.id,
      fields: buildDroppedFields(heartbeat, nowIso, dateOnly, droppedScopeStatus, watchTripsFieldSet),
    });
  }

  const summary = {
    ok: true,
    dry_run: DRY_RUN,
    app_show_id: heartbeat.app_show_id,
    app_sql_date: heartbeat.app_sql_date,
    app_dow_raw: heartbeat.app_dow_raw,
    shifted_to_next_day: heartbeat.shifted_to_next_day,
    mode: heartbeat.mode,
    show_target_scope: heartbeatScope.show_target_scope,
    active_tenant_ids: activeTenantIds.length,
    watch_schedule_rows: scheduleRows.length,
    scoped_watch_schedule_rows: scopedScheduleRows.length,
    target_watch_schedule_rows: targetScheduleRows.length,
    watch_schedule_join_rows: scheduleRowsForTripJoin.length,
    watch_schedule_classes: scheduleJoinClasses,
    normalized_rows: normalizedRows.length,
    unique_rows: scopedRows.length,
    skipped_malformed_schedule_key: skippedMalformedScheduleKey,
    skipped_malformed_schedule_key_samples: skippedMalformedScheduleKeySamples,
    filtered_out_scheduled_date_mismatch: 0,
    skipped_invalid_scheduled_date: skippedInvalidScheduledDate,
    people_failures: peopleFailures,
    soft_payload_samples: softPayloadSamples.slice(0, 10),
    partial_people_payload_failures: softPayloadSamples.length,
    empty_tenant_ids: emptyTenantIds,
    no_trips_audits: noTripsAudits,
    tenant_summaries: tenantSummaries,
    outside_schedule_count: outsideSchedule.length,
    active_groups: activeGroupSync,
    active_classes: classSync,
    active_entries: entrySync,
    active_links: activeLinkSync,
    creates_planned: createRecords.length,
    updates_planned: updateRecords.length,
    drops_planned: dropUpdates.length,
    duplicate_archives_planned: duplicateArchiveUpdates.length,
    existing_show_rows: existingRows.length,
    heartbeat_view_rows: heartbeatViewRows.length,
    manual_time_override_guard: manualTimeOverrideGuard,
    writes: {
      created: 0,
      updated: 0,
      dropped: 0,
      duplicate_archived: 0,
      create_failures: [],
      update_failures: [],
      drop_failures: [],
      duplicate_archive_failures: [],
    },
    schedule_date_backfill: {
      planned: 0,
      updated: 0,
      failures: [],
    },
  };

  const backfillRows = await fetchTripScheduleBackfillRows(heartbeat.app_show_id);
  const backfillUpdates = buildTripScheduleBackfillUpdates(backfillRows, watchTripsFieldSet);
  summary.schedule_date_backfill.planned = backfillUpdates.length;

  if (!DRY_RUN) {
    const createResult = await airtableCreateRecords(TABLE_WATCH_TRIPS, createRecords);
    const updateResult = await airtablePatchRecords(TABLE_WATCH_TRIPS, updateRecords);
    const dropResult = await airtablePatchRecords(TABLE_WATCH_TRIPS, dropUpdates);
    const duplicateArchiveResult = await airtablePatchRecords(TABLE_WATCH_TRIPS, duplicateArchiveUpdates);
    const backfillResult = await airtablePatchRecords(TABLE_WATCH_TRIPS, backfillUpdates);
    summary.writes.created = createResult.okRows;
    summary.writes.updated = updateResult.okRows;
    summary.writes.dropped = dropResult.okRows;
    summary.writes.duplicate_archived = duplicateArchiveResult.okRows;
    summary.writes.create_failures = createResult.failedRows;
    summary.writes.update_failures = updateResult.failedRows;
    summary.writes.drop_failures = dropResult.failedRows;
    summary.writes.duplicate_archive_failures = duplicateArchiveResult.failedRows;
    summary.schedule_date_backfill.updated = backfillResult.okRows;
    summary.schedule_date_backfill.failures = backfillResult.failedRows;
  }

  return summary;
}

async function main() {
  requireEnv("AIRTABLE_TOKEN", AIRTABLE_TOKEN);
  requireEnv("AIRTABLE_BASE_ID", AIRTABLE_BASE_ID);

  const nowIso = new Date().toISOString();
  const dateOnly = nowIso.slice(0, 10);
  const runId = buildNumericRunId(nowIso);
  const latestHeartbeat = await fetchLatestHeartbeat();
  const heartbeatScopes = await resolveHeartbeatScopesFromShowTarget(latestHeartbeat, new Date(nowIso));
  const results = [];

  for (const heartbeatScope of heartbeatScopes) {
    results.push(await runTripsForHeartbeatScope({
      heartbeatScope,
      nowIso,
      dateOnly,
      runId,
    }));
  }

  const output = results.length === 1
    ? results[0]
    : {
        ok: results.every((result) => result.ok),
        dry_run: DRY_RUN,
        source: "show_heartbeat_multi",
        show_target_scope: {
          table: TABLE_SHOW_TARGET,
          view: VIEW_SHOW_TARGET,
          rows: results[0]?.show_target_scope?.rows ?? 0,
          valid_rows: results.length,
        },
        results,
      };

  if (!output.ok) process.exitCode = 1;
  console.log(JSON.stringify(output, null, 2));
}

if (require.main === module) {
  main()
    .then(() => {
      process.exit(0);
    })
    .catch((error) => {
      const message = String(error?.stack || error?.message || error);
      console.error(JSON.stringify({
        ok: false,
        error: message.slice(0, 4000),
      }));
      process.exit(1);
    });
}

module.exports = {
  applyManualTimeOverrideToTripFields,
  buildCurrentFields,
  buildDuplicateTripArchiveUpdates,
  buildTripKeyParts,
  buildDroppedFields,
  hasManualTimeOverride,
  selectTripRowsForWriteScope,
  tripRowKeyFromFields,
  WATCH_TRIPS_HEARTBEAT_FIELDS,
};
