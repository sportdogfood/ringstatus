const {
  normalizeSchedulePayload,
  chooseScheduleVariant,
} = require("./schedule_normalizer_v2");
const fs = require("fs");
const path = require("path");
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
  writableFieldName,
} = require("./lib/scope_fields");

const AIRTABLE_TOKEN = process.env.AIRTABLE_TOKEN || "";
const AIRTABLE_BASE_ID = process.env.AIRTABLE_BASE_ID || "";
const CUSTOMER_ID = Number(process.env.CUSTOMER_ID || "15");
const SGL_BASE_URL = String(
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
const TABLE_ACTIVE_GROUPS = process.env.TABLE_ACTIVE_GROUPS || "active_groups";
const TABLE_GROUPS_LIVE = process.env.TABLE_GROUPS_LIVE || "groups_live";
const TABLE_AUTOMATION_ERRS = process.env.TABLE_AUTOMATION_ERRS || "automation_errs";

const VIEW_HEARTBEAT = process.env.VIEW_HEARTBEAT || "heartbeat";
const VIEW_SHOW_TARGET = process.env.VIEW_SHOW_TARGET || "heartbeat";
const VIEW_WATCH_SCHEDULE_HEARTBEAT = process.env.VIEW_WATCH_SCHEDULE_HEARTBEAT || "heartbeat";
const HEARTBEAT_SORT_FIELD = process.env.HEARTBEAT_SORT_FIELD || "hb_at";
const SCHEDULE_SCOPE_TIMEZONE = process.env.SCHEDULE_SCOPE_TIMEZONE || "America/New_York";

const HTTP_TIMEOUT_MS = Number(process.env.HTTP_TIMEOUT_MS || "20000");
const AT_RETRY_ATTEMPTS = Number(process.env.AT_RETRY_ATTEMPTS || "3");
const AT_RETRY_BASE_MS = Number(process.env.AT_RETRY_BASE_MS || "400");
const AT_RETRY_MAX_MS = Number(process.env.AT_RETRY_MAX_MS || "2000");
const FETCH_CONCURRENCY = Math.max(1, Number(process.env.FETCH_CONCURRENCY || "4"));
const DRY_RUN = String(process.env.DRY_RUN || "0") === "1";
const HOTPATCH_APP_SHOW_ID = strOrNull(process.env.HOTPATCH_APP_SHOW_ID);
const HOTPATCH_APP_SQL_DATE = toIsoDateOnly(process.env.HOTPATCH_APP_SQL_DATE);
const SYNC_ACTIVE_GROUPS_FROM_SCHEDULE = String(process.env.SYNC_ACTIVE_GROUPS_FROM_SCHEDULE || "0") === "1";
const VALID_DOW_RAW = new Set(["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]);
const VALID_MODES = new Set(["DAY", "NIGHT", "OVERNIGHT"]);
const SGL_PAYLOAD_ROOT = String(process.env.SGL_PAYLOAD_ROOT || "C:\\actions-runner\\ringstatus").trim();
const EARLY_SGL_PAYLOAD_ROOT = String(
  process.env.EARLY_SGL_PAYLOAD_ROOT ||
  path.join(SGL_PAYLOAD_ROOT, "early_sgl_payloads")
).trim();
const MANUAL_SGL_PAYLOAD_ROOT = String(
  process.env.MANUAL_SGL_PAYLOAD_ROOT ||
  path.join(SGL_PAYLOAD_ROOT, "manual_sgl_payloads")
).trim();
const EARLY_SCHEDULE_PAYLOAD_DIR = String(
  process.env.EARLY_SCHEDULE_PAYLOAD_DIR ||
  path.join(EARLY_SGL_PAYLOAD_ROOT, "schedule")
).trim();
const EARLY_PEOPLE_PAYLOAD_DIR = String(
  process.env.EARLY_PEOPLE_PAYLOAD_DIR ||
  path.join(EARLY_SGL_PAYLOAD_ROOT, "people")
).trim();
const MANUAL_SCHEDULE_PAYLOAD_DIR = String(
  process.env.MANUAL_SCHEDULE_PAYLOAD_DIR ||
  path.join(MANUAL_SGL_PAYLOAD_ROOT, "schedule")
).trim();
const MANUAL_PEOPLE_PAYLOAD_DIR = String(
  process.env.MANUAL_PEOPLE_PAYLOAD_DIR ||
  path.join(MANUAL_SGL_PAYLOAD_ROOT, "people")
).trim();
const MANUAL_SCHEDULE_HTML_DIR = String(
  process.env.MANUAL_SCHEDULE_HTML_DIR ||
  path.join(MANUAL_SGL_PAYLOAD_ROOT, "schedule-html")
).trim();
const REPO_MANUAL_SGL_PAYLOAD_ROOT = path.join(__dirname, "manual_sgl_payloads");
const DEFAULT_SCHEDULE_HTML_FALLBACK_DIRS = [
  MANUAL_SGL_PAYLOAD_ROOT,
  MANUAL_SCHEDULE_HTML_DIR,
  REPO_MANUAL_SGL_PAYLOAD_ROOT,
  path.join(REPO_MANUAL_SGL_PAYLOAD_ROOT, "schedule-html"),
];
const DEFAULT_SCHEDULE_FALLBACK_DIRS = [
  EARLY_SCHEDULE_PAYLOAD_DIR,
  MANUAL_SGL_PAYLOAD_ROOT,
  MANUAL_SCHEDULE_PAYLOAD_DIR,
  REPO_MANUAL_SGL_PAYLOAD_ROOT,
];
const PREFETCH_FORWARD_SCHEDULES = String(process.env.PREFETCH_FORWARD_SCHEDULES || "1") !== "0";
const MAX_FORWARD_SCHEDULE_PREFETCH_DAYS = Math.max(
  0,
  Number(process.env.MAX_FORWARD_SCHEDULE_PREFETCH_DAYS || "14")
);
const PRELIVE_ESTIMATED_START_TIME_MIN = String(
  process.env.PRELIVE_ESTIMATED_START_TIME_MIN || "07:00:00"
).trim();
const PRELIVE_ESTIMATED_START_TIME_MAX = String(
  process.env.PRELIVE_ESTIMATED_START_TIME_MAX || "19:00:00"
).trim();

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

function toIsoDateOnly(value) {
  if (isBlank(value)) return null;
  if (value instanceof Date) return value.toISOString().slice(0, 10);
  const text = String(value).trim();
  if (/^\d{4}-\d{2}-\d{2}$/.test(text)) return text;
  if (/^\d{4}-\d{2}-\d{2}T/.test(text)) return text.slice(0, 10);
  const parsed = Date.parse(text);
  return Number.isFinite(parsed) ? new Date(parsed).toISOString().slice(0, 10) : null;
}

function dayOfWeekUtc(sqlDate) {
  const iso = toIsoDateOnly(sqlDate);
  if (!iso) return null;
  const ms = Date.parse(`${iso}T00:00:00.000Z`);
  if (!Number.isFinite(ms)) return null;
  return new Date(ms).getUTCDay();
}

function dowName(dow) {
  return ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"][dow] || null;
}

function buildScopeKey(appShowId, appSqlDate, appDowRaw, shiftedToNextDay) {
  return [
    isBlank(appShowId) ? "" : String(appShowId),
    strOrNull(appSqlDate) || "",
    strOrNull(appDowRaw) || "",
    shiftedToNextDay ? "1" : "0",
  ].join("|");
}

function buildScheduleEndpoint(appSqlDate, appShowId, customerId = CUSTOMER_ID) {
  if (isBlank(appSqlDate) || isBlank(appShowId)) return null;
  return `${SGL_BASE_URL}/schedule?date=${encodeURIComponent(appSqlDate)}&show_id=${encodeURIComponent(appShowId)}&customer_id=${encodeURIComponent(customerId)}`;
}

function buildScheduleEmptyEndpoint(appShowId, customerId = CUSTOMER_ID) {
  if (isBlank(appShowId)) return null;
  return `${SGL_BASE_URL}/schedule?date=00/00/00&show_id=${encodeURIComponent(appShowId)}&customer_id=${encodeURIComponent(customerId)}`;
}

function buildClassListEndpoint(appShowId, customerId = CUSTOMER_ID) {
  if (isBlank(appShowId)) return null;
  return `${SGL_BASE_URL}/classes?show_id=${encodeURIComponent(appShowId)}&customer_id=${encodeURIComponent(customerId)}`;
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

function buildScheduleKeyParts({ sid, sqlDate, ringNumber, classNumber, tieBreaker }) {
  return {
    scheduleKey: joinKeyPartsWithOptional([sid, sqlDate, ringNumber, classNumber], [tieBreaker]),
    scheduleShort: joinKeyPartsWithOptional([ringNumber, classNumber], [tieBreaker]),
  };
}

function chunk(items, size) {
  const out = [];
  for (let i = 0; i < items.length; i += size) out.push(items.slice(i, i + size));
  return out;
}

function buildNumericRunId(nowIso) {
  const ms = Date.parse(nowIso);
  return Number.isFinite(ms) ? ms : Date.now();
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

function boolValue(value) {
  const raw = firstValue(value);
  if (raw === true || raw === 1) return true;
  if (raw === false || raw === 0 || raw === null || raw === undefined) return false;
  const text = String(raw).trim().toLowerCase();
  return text === "true" || text === "1";
}

function strictSqlDate(value, fieldName) {
  const text = strOrNull(value);
  if (!text) throw new Error(`Missing required heartbeat field: ${fieldName}`);
  if (!/^\d{4}-\d{2}-\d{2}$/.test(text)) {
    throw new Error(`Invalid heartbeat ${fieldName}: ${text}`);
  }
  return text;
}

function strictDowRaw(value, fieldName) {
  const text = strOrNull(value);
  if (!text) throw new Error(`Missing required heartbeat field: ${fieldName}`);
  if (!VALID_DOW_RAW.has(text)) {
    throw new Error(`Invalid heartbeat ${fieldName}: ${text}`);
  }
  return text;
}

function strictMode(value, fieldName) {
  const text = strOrNull(value);
  if (!text) throw new Error(`Missing required heartbeat field: ${fieldName}`);
  if (!VALID_MODES.has(text)) {
    throw new Error(`Invalid heartbeat ${fieldName}: ${text}`);
  }
  return text;
}

function addDaysSql(sqlDate, days) {
  const base = strictSqlDate(sqlDate, "sql_date");
  const ms = Date.parse(`${base}T00:00:00.000Z`);
  if (!Number.isFinite(ms)) throw new Error(`Invalid heartbeat sql_date: ${base}`);
  const next = new Date(ms + days * 86400000);
  return next.toISOString().slice(0, 10);
}

function localMinutesForScheduleScope(now = new Date(), timeZone = SCHEDULE_SCOPE_TIMEZONE) {
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone,
    hour12: false,
    hour: "2-digit",
    minute: "2-digit",
  }).formatToParts(now);
  const hour = Number(parts.find((part) => part.type === "hour")?.value);
  const minute = Number(parts.find((part) => part.type === "minute")?.value);
  if (!Number.isFinite(hour) || !Number.isFinite(minute)) {
    throw new Error(`Unable to resolve local schedule scope time for ${timeZone}`);
  }
  return hour * 60 + minute;
}

function showHeartbeatTargetDate(fields, now = new Date()) {
  const focusDay = strictSqlDate(toIsoDateOnly(fields?.focus_day), "show.focus_day");
  const startDate = strictSqlDate(toIsoDateOnly(fields?.start_date), "show.start_date");
  const endDate = strictSqlDate(toIsoDateOnly(fields?.end_date), "show.end_date");
  const shiftedToNextDay = boolValue(fields?.shifted_to_next_day);
  const targetDate = focusDay;

  if (compareSqlDate(targetDate, startDate) < 0 || compareSqlDate(targetDate, endDate) > 0) {
    return {
      target_date: null,
      focus_day: focusDay,
      start_date: startDate,
      end_date: endDate,
      is_day_window: true,
      shifted_to_next_day: shiftedToNextDay,
      skipped: true,
      reason: "target_date_outside_show_window",
      proposed_target_date: targetDate,
    };
  }

  return {
    target_date: targetDate,
    focus_day: focusDay,
    start_date: startDate,
    end_date: endDate,
    is_day_window: true,
    shifted_to_next_day: shiftedToNextDay,
    skipped: false,
    reason: null,
    proposed_target_date: targetDate,
  };
}

function compareSqlDate(left, right) {
  const a = strictSqlDate(left, "left_sql_date");
  const b = strictSqlDate(right, "right_sql_date");
  if (a < b) return -1;
  if (a > b) return 1;
  return 0;
}

function sameValue(left, right) {
  if (Array.isArray(left) || Array.isArray(right)) {
    return JSON.stringify(left ?? null) === JSON.stringify(right ?? null);
  }
  return left === right;
}

async function runPool(items, concurrency, worker) {
  const queue = [...items];
  const threads = [];
  const width = Math.max(1, concurrency);

  for (let i = 0; i < width; i += 1) {
    threads.push((async () => {
      while (queue.length) {
        const item = queue.shift();
        if (item === undefined) break;
        await worker(item);
      }
    })());
  }

  await Promise.all(threads);
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
        const waitMs = Math.min(maxMs, baseMs * attempt + Math.floor(Math.random() * 200));
        await sleep(waitMs);
        continue;
      }

      return response;
    } catch (error) {
      lastError = error;
      if (!isRetryableFetchError(error) || attempt === attempts) throw error;
      const waitMs = Math.min(maxMs, baseMs * attempt + Math.floor(Math.random() * 250));
      await sleep(waitMs);
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
        body: JSON.stringify({ records: batch }),
      });

      if (!response.ok) {
        const body = await response.text().catch(() => "");
        throw new Error(`Airtable create failed (${response.status}) ${tableName}: ${body}`);
      }

      okRows += batch.length;
    } catch (error) {
      for (const row of batch) {
        failedRows.push({
          key: row?.fields?.class_groupxclasses_id ?? null,
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
  const automationKey = strOrNull(audit.automation_key) ||
    [
      "schedules_dailyv2",
      errorType,
      appShowId || "show",
      appSqlDate || "date",
      pathText,
    ].join("|").slice(0, 1000);

  return createAutomationErr({
    automation_key: automationKey,
    automation_name: strOrNull(audit.automation_name) || "schedules_dailyv2",
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
    pid: numOrNull(audit.pid) ?? parts.pid,
    app_show_id: appShowId,
    people_show_id: numOrNull(audit.people_show_id) ?? parts.people_show_id,
  });
}

async function recordPayloadPingAudit(endpoint, response, text, audit = {}) {
  const parts = endpointParts(endpoint);
  const appShowId = numOrNull(audit.app_show_id) ?? parts.app_show_id;
  const appSqlDate = strOrNull(audit.app_sql_date) || parts.app_sql_date;
  const pathText = parts.path || String(endpoint || "");
  const bodyLength = Buffer.byteLength(text || "", "utf8");
  const automationKey = strOrNull(audit.automation_key) ||
    [
      "schedules_dailyv2",
      "payload_ok",
      appShowId || "show",
      appSqlDate || "date",
      pathText,
    ].join("|").slice(0, 1000);

  return createAutomationErr({
    automation_key: automationKey,
    automation_name: strOrNull(audit.automation_name) || "schedules_dailyv2",
    error_type: "payload_ok",
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
    pid: numOrNull(audit.pid) ?? parts.pid,
    app_show_id: appShowId,
    people_show_id: numOrNull(audit.people_show_id) ?? parts.people_show_id,
  });
}

async function airtablePatchRecords(tableName, updates) {
  if (!updates.length) return { okRows: 0, failedRows: [] };

  let okRows = 0;
  const failedRows = [];

  for (const batch of chunk(updates, 10)) {
    try {
      const response = await fetchWithRetry(airtableUrl(tableName), {
        method: "PATCH",
        headers: {
          Authorization: `Bearer ${AIRTABLE_TOKEN}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ records: batch }),
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
          key: row?.fields?.class_groupxclasses_id ?? null,
          reason: String(error?.message || error).slice(0, 300),
        });
      }
    }
  }

  return { okRows, failedRows };
}

async function fetchJson(url, audit = {}) {
  let fetched = null;
  try {
    fetched = await fetchTextWithConfiguredTransport(url, async (endpoint) => {
      const response = await fetchWithRetry(endpoint, {
        method: "GET",
        headers: { Accept: "application/json" },
      });
      const text = await response.text().catch(() => "");
      return { response, text, endpoint };
    });
  } catch (error) {
    await recordSoftPayloadAudit(error, url, audit);
    throw error;
  }

  const { response, text } = fetched;
  const endpoint = fetched.endpoint || url;
  if (!response.ok) {
    throw new Error(`Fetch failed (${response.status}): ${text.slice(0, 1200)}`);
  }

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
      lane: "schedules_dailyv2",
      endpoint,
      expectedTopLevelKeys: ["show", "show_date", "showDate", "show_days_list", "rings", "schedule", "classes", "class_groups"],
    });
  } catch (error) {
    if (isSoftPayloadError(error)) {
      await recordSoftPayloadAudit(error, endpoint, audit);
      throw new Error(
        `${error?.reason || error?.message || "soft_payload"} ` +
        `endpoint=${endpoint} status=${error?.http_status ?? response.status} ` +
        `body_length=${error?.body_length ?? Buffer.byteLength(text || "", "utf8")} ` +
        `content_length=${error?.content_length ?? response.headers?.get?.("content-length") ?? "unknown"}`
      );
    }
    throw error;
  }
  await recordPayloadPingAudit(endpoint, response, text, {
    ...audit,
    transport: fetched.transport,
  });
  return json;
}

async function fetchClassListJson(url, audit = {}) {
  let fetched = null;
  try {
    fetched = await fetchTextWithConfiguredTransport(url, async (endpoint) => {
      const response = await fetchWithRetry(endpoint, {
        method: "GET",
        headers: { Accept: "application/json" },
      });
      const text = await response.text().catch(() => "");
      return { response, text, endpoint };
    });
  } catch (error) {
    await recordSoftPayloadAudit(error, url, audit);
    throw error;
  }

  const { response, text } = fetched;
  const endpoint = fetched.endpoint || url;
  if (!response.ok) {
    throw new Error(`Fetch failed (${response.status}): ${text.slice(0, 1200)}`);
  }

  let json = null;
  try {
    json = JSON.parse(text);
  } catch {
    throw new Error(`Class list response was not valid JSON. First 1200 chars:\n${text.slice(0, 1200)}`);
  }

  if (!classCatalogRowsFromPayload(json).length) {
    throw new Error(`Class list payload did not include classes[]: endpoint=${endpoint}`);
  }

  await recordPayloadPingAudit(endpoint, response, text, {
    ...audit,
    transport: fetched.transport,
  });
  return json;
}

function extractPayloadShowId(payload) {
  return numOrNull(pickFirst(payload?.show?.show_id, payload?.show_id));
}

function extractPayloadShowDate(payload) {
  return toIsoDateOnly(pickFirst(payload?.show_date, payload?.showDate, payload?.show?.show_date));
}

function assertSchedulePayloadScope(payload, scope, source) {
  const expectedShowId = numOrNull(scope?.app_show_idv2);
  const expectedDate = toIsoDateOnly(scope?.app_sql_datev2);
  const payloadShowId = extractPayloadShowId(payload);
  const payloadDate = extractPayloadShowDate(payload);

  if (expectedShowId !== null && payloadShowId !== null && payloadShowId !== expectedShowId) {
    throw new Error(`schedule_payload_show_id_conflict source=${source} expected=${expectedShowId} actual=${payloadShowId}`);
  }
  if (expectedDate && payloadDate && payloadDate !== expectedDate) {
    throw new Error(`schedule_payload_show_date_conflict source=${source} expected=${expectedDate} actual=${payloadDate}`);
  }
}

function escapeRegExp(value) {
  return String(value).replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function scheduleFallbackDirs() {
  const raw = strOrNull(process.env.SGL_SCHEDULE_FALLBACK_DIRS);
  const values = raw
    ? raw.split(path.delimiter)
    : DEFAULT_SCHEDULE_FALLBACK_DIRS;

  return [...new Set(values
    .map((value) => strOrNull(value))
    .filter(Boolean))];
}

function scheduleHtmlFallbackDirs() {
  const raw = strOrNull(process.env.SGL_SCHEDULE_HTML_FALLBACK_DIRS);
  const values = raw
    ? [...raw.split(path.delimiter), ...DEFAULT_SCHEDULE_HTML_FALLBACK_DIRS]
    : DEFAULT_SCHEDULE_HTML_FALLBACK_DIRS;

  return [...new Set(values
    .map((value) => strOrNull(value))
    .filter(Boolean))];
}

function scheduleHtmlLookupSummary(appShowId, appSqlDate) {
  const dateHyphen = String(appSqlDate || "");
  const dateUnderscore = dateHyphen.replace(/-/g, "_");
  const showText = String(appShowId || "");
  return {
    searched_dirs: scheduleHtmlFallbackDirs().map((dirPath) => ({
      dir_path: dirPath,
      exists: fs.existsSync(dirPath),
    })),
    expected_filename_shapes: [
      `schedule_html_${dateHyphen}_show_${showText}_EPOCH.html`,
      `schedule_html_${dateHyphen}_show_id_${showText}_EPOCH.html`,
      `schedule_html_${dateUnderscore}_show_${showText}_EPOCH.html`,
      `schedule_html_${dateUnderscore}_show_id_${showText}_EPOCH.html`,
    ],
  };
}

function collectFilesRecursive(dirPath, out = []) {
  if (!dirPath || !fs.existsSync(dirPath)) return out;

  for (const entry of fs.readdirSync(dirPath, { withFileTypes: true })) {
    const fullPath = path.join(dirPath, entry.name);
    if (entry.isDirectory()) {
      collectFilesRecursive(fullPath, out);
    } else if (entry.isFile()) {
      out.push(fullPath);
    }
  }

  return out;
}

function candidateScheduleFallbackFiles(appShowId, appSqlDate) {
  const showText = escapeRegExp(appShowId);
  const dateHyphen = escapeRegExp(appSqlDate);
  const dateUnderscore = escapeRegExp(String(appSqlDate || "").replace(/-/g, "_"));
  const scheduleJson = new RegExp(`^schedule_(?:${dateHyphen}|${dateUnderscore})_show(?:_id)?_${showText}_[0-9]+\\.json$`, "i");

  const candidates = [];
  for (const dirPath of scheduleFallbackDirs()) {
    for (const filePath of collectFilesRecursive(dirPath)) {
      const name = path.basename(filePath);
      if (name.endsWith(".pretty.json")) continue;
      if (!scheduleJson.test(name)) continue;

      const stat = fs.statSync(filePath);
      if (!stat.size || stat.size <= 2) continue;
      candidates.push({ filePath, size: stat.size, mtimeMs: stat.mtimeMs });
    }
  }

  candidates.sort((left, right) => right.mtimeMs - left.mtimeMs);
  return candidates;
}

function loadScheduleFallbackPayload(appShowId, appSqlDate) {
  const candidates = candidateScheduleFallbackFiles(appShowId, appSqlDate);
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
        lane: "schedules_dailyv2_fallback",
        endpoint: candidate.filePath,
        expectedTopLevelKeys: ["show", "show_date", "showDate", "show_days_list", "rings", "schedule", "classes", "class_groups"],
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

  return {
    ok: false,
    file_path: null,
    body_length: null,
    failures,
  };
}

function candidateScheduleHtmlFiles(appShowId, appSqlDate) {
  const showText = escapeRegExp(appShowId);
  const dateHyphen = escapeRegExp(appSqlDate);
  const dateUnderscore = escapeRegExp(String(appSqlDate || "").replace(/-/g, "_"));
  const scheduleHtml = new RegExp(`^schedule_html_(?:${dateHyphen}|${dateUnderscore})_show(?:_id)?_${showText}_[0-9]+\\.html$`, "i");

  const candidates = [];
  for (const dirPath of scheduleHtmlFallbackDirs()) {
    for (const filePath of collectFilesRecursive(dirPath)) {
      const name = path.basename(filePath);
      if (!scheduleHtml.test(name)) continue;

      const stat = fs.statSync(filePath);
      if (!stat.size) continue;
      candidates.push({ filePath, size: stat.size, mtimeMs: stat.mtimeMs });
    }
  }

  candidates.sort((left, right) => right.mtimeMs - left.mtimeMs);
  return candidates;
}

function decodeHtmlText(value) {
  return String(value || "")
    .replace(/&nbsp;/gi, " ")
    .replace(/&amp;/gi, "&")
    .replace(/&#39;/g, "'")
    .replace(/&quot;/gi, '"')
    .replace(/<[^>]+>/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function normalizeHtmlScheduleTimeText(value) {
  const match = String(value || "").match(/\b(\d{1,2})(?::(\d{2}))?\s*(AM|PM)\b/i);
  if (!match) return null;

  let hour = Number(match[1]);
  const minute = Number(match[2] || "0");
  const meridiem = match[3].toUpperCase();
  if (!Number.isInteger(hour) || !Number.isInteger(minute)) return null;
  if (hour < 1 || hour > 12 || minute < 0 || minute > 59) return null;
  if (meridiem === "PM" && hour !== 12) hour += 12;
  if (meridiem === "AM" && hour === 12) hour = 0;
  return `${String(hour).padStart(2, "0")}:${String(minute).padStart(2, "0")}:00`;
}

function timeTextToSeconds(value) {
  const match = String(value || "").trim().match(/^(\d{1,2}):(\d{2})(?::(\d{2}))?$/);
  if (!match) return null;
  const hour = Number(match[1]);
  const minute = Number(match[2]);
  const second = Number(match[3] || "0");
  if (!Number.isFinite(hour) || !Number.isFinite(minute) || !Number.isFinite(second)) return null;
  if (hour < 0 || hour > 23 || minute < 0 || minute > 59 || second < 0 || second > 59) return null;
  return hour * 3600 + minute * 60 + second;
}

function isSuspiciousPreliveEstimatedStartTime(value) {
  const valueSeconds = timeTextToSeconds(value);
  const minSeconds = timeTextToSeconds(PRELIVE_ESTIMATED_START_TIME_MIN);
  const maxSeconds = timeTextToSeconds(PRELIVE_ESTIMATED_START_TIME_MAX);
  if (valueSeconds === null || minSeconds === null || maxSeconds === null) return false;
  return valueSeconds < minSeconds || valueSeconds > maxSeconds;
}

function parseScheduleHtmlTimeOverlay(htmlText) {
  const rowsByGroupId = new Map();
  const rowsByClassNumber = new Map();
  const parsedRows = [];
  const rowMatches = String(htmlText || "").match(/<tr\b[\s\S]*?<\/tr>/gi) || [];

  for (const rowHtml of rowMatches) {
    if (!/\bclass_group_row\b/i.test(rowHtml)) continue;
    if (/Total\s+Trips/i.test(rowHtml)) continue;

    const estimatedStartTime = normalizeHtmlScheduleTimeText(decodeHtmlText(rowHtml));
    if (!estimatedStartTime) continue;

    const cgidMatch = rowHtml.match(/(?:[?&]|&amp;)cgid=(\d+)/i);
    const ringMatch = rowHtml.match(/(?:[?&]|&amp;)ring=(\d+)/i);
    const bracketMatch = decodeHtmlText(rowHtml).match(/\[([0-9,\s]+)\]/);
    const classNumbers = bracketMatch
      ? bracketMatch[1].split(",").map((value) => numOrNull(value)).filter((value) => value !== null)
      : [];
    const item = {
      class_group_id: cgidMatch ? numOrNull(cgidMatch[1]) : null,
      ring_number: ringMatch ? numOrNull(ringMatch[1]) : null,
      class_numbers: classNumbers,
      estimated_start_time: estimatedStartTime,
    };

    parsedRows.push(item);
    if (item.class_group_id !== null) rowsByGroupId.set(normalizeKey(item.class_group_id), item);
    for (const classNumber of classNumbers) {
      if (!rowsByClassNumber.has(normalizeKey(classNumber))) rowsByClassNumber.set(normalizeKey(classNumber), item);
    }
  }

  return { parsedRows, rowsByGroupId, rowsByClassNumber };
}

function applyScheduleHtmlTimeOverlay(rows, appShowId, appSqlDate) {
  const candidates = candidateScheduleHtmlFiles(appShowId, appSqlDate);
  const lookup = scheduleHtmlLookupSummary(appShowId, appSqlDate);
  const summary = {
    ok: false,
    skipped: candidates.length === 0,
    reason: candidates.length === 0 ? "no_matching_manual_schedule_html" : null,
    alert: candidates.length === 0 ? "manual_schedule_html_missing" : null,
    manual_schedule_html_dir: MANUAL_SCHEDULE_HTML_DIR,
    manual_schedule_html_lookup: lookup,
    file_path: candidates[0]?.filePath || null,
    body_length: candidates[0]?.size || null,
    parsed_rows: 0,
    matched_rows: 0,
    updated_rows: 0,
    failures: [],
  };

  if (!candidates.length) return { rows, summary };

  for (const candidate of candidates) {
    try {
      const htmlText = fs.readFileSync(candidate.filePath, "utf8");
      const parsed = parseScheduleHtmlTimeOverlay(htmlText);
      summary.ok = true;
      summary.skipped = false;
      summary.reason = null;
      summary.alert = null;
      summary.file_path = candidate.filePath;
      summary.body_length = candidate.size;
      summary.parsed_rows = parsed.parsedRows.length;

      const nextRows = (rows || []).map((row) => {
        const fields = row?.fields || {};
        const byGroup = parsed.rowsByGroupId.get(normalizeKey(fields.class_group_id));
        const byClass = parsed.rowsByClassNumber.get(normalizeKey(fields.class_number));
        const overlay = byGroup || byClass || null;
        if (!overlay?.estimated_start_time) return row;

        summary.matched_rows += 1;
        if (!isBlank(fields.estimated_start_time)) return row;

        summary.updated_rows += 1;
        return {
          ...row,
          fields: {
            ...fields,
            estimated_start_time: overlay.estimated_start_time,
          },
          refs: {
            ...(row?.refs || {}),
            schedule_html_time_overlay: candidate.filePath,
          },
        };
      });

      return { rows: nextRows, summary };
    } catch (error) {
      summary.failures.push({
        file_path: candidate.filePath,
        reason: String(error?.message || error).slice(0, 300),
      });
    }
  }

  summary.reason = "manual_schedule_html_parse_failed";
  summary.alert = "manual_schedule_html_parse_failed";
  return { rows, summary };
}

function schedulePayloadFileName(appShowId, appSqlDate, epochSeconds = Math.floor(Date.now() / 1000)) {
  return `schedule_${appSqlDate}_show_${appShowId}_${epochSeconds}.json`;
}

function writeEarlySchedulePayload(appShowId, appSqlDate, payload, epochSeconds) {
  const dirPath = EARLY_SCHEDULE_PAYLOAD_DIR;
  fs.mkdirSync(dirPath, { recursive: true });
  const filePath = path.join(dirPath, schedulePayloadFileName(appShowId, appSqlDate, epochSeconds));
  fs.writeFileSync(filePath, `${JSON.stringify(payload, null, 2)}\n`, "utf8");
  const stat = fs.statSync(filePath);
  return {
    ok: true,
    file_path: filePath,
    body_length: stat.size,
  };
}

function nextSqlDate(sqlDate) {
  return addDaysSql(sqlDate, 1);
}

function forwardScheduleDates(currentDate, endDate) {
  const dates = [];
  if (!currentDate || !endDate) return dates;
  let current = nextSqlDate(currentDate);
  let guard = 0;
  while (compareSqlDate(current, endDate) <= 0 && guard < MAX_FORWARD_SCHEDULE_PREFETCH_DAYS) {
    dates.push(current);
    current = nextSqlDate(current);
    guard += 1;
  }
  return dates;
}

async function cacheSuccessfulSchedulePayloads(scope, currentPayload, { runId = null, lastRun = null } = {}) {
  const summary = {
    enabled: PREFETCH_FORWARD_SCHEDULES,
    early_schedule_dir: EARLY_SCHEDULE_PAYLOAD_DIR,
    skipped: false,
    current: null,
    forward: [],
  };
  Object.defineProperty(summary, "forwardPayloads", {
    value: [],
    enumerable: false,
    writable: true,
  });

  if (!PREFETCH_FORWARD_SCHEDULES) return summary;
  if (DRY_RUN) {
    summary.skipped = true;
    summary.reason = "dry_run";
    return summary;
  }

  const appShowId = scope?.app_show_idv2;
  const customerId = scope?.customer_id || CUSTOMER_ID;
  const currentDate = scope?.app_sql_datev2;
  const endDate = scope?.show_app_sql_end_date;
  const epochSeconds = Math.floor(Date.now() / 1000);

  try {
    summary.current = writeEarlySchedulePayload(appShowId, currentDate, currentPayload, epochSeconds);
  } catch (error) {
    summary.current = {
      ok: false,
      reason: String(error?.message || error).slice(0, 300),
    };
  }

  for (const scheduleDate of forwardScheduleDates(currentDate, endDate)) {
    const url = buildScheduleEndpoint(scheduleDate, appShowId, customerId);
    const item = { date: scheduleDate, url, ok: false, file_path: null, reason: null };
    try {
      const payload = await fetchJson(url, {
        automation_key: `schedules_dailyv2|forward_schedule|${appShowId}|${scheduleDate}`,
        automation_name: "schedules_dailyv2",
        source: "forward_schedule_cache",
        app_show_id: appShowId,
        app_sql_date: scheduleDate,
        run_id: runId,
        last_run: lastRun,
      });
      const writeResult = writeEarlySchedulePayload(appShowId, scheduleDate, payload, epochSeconds);
      item.ok = true;
      item.file_path = writeResult.file_path;
      item.body_length = writeResult.body_length;
      summary.forwardPayloads.push({ date: scheduleDate, payload });
    } catch (error) {
      item.reason = String(error?.reason || error?.message || error).slice(0, 300);
    }
    summary.forward.push(item);
  }

  return summary;
}

function scopeForScheduleDate(scope, scheduleDate, source = "forward_schedule_cache") {
  return {
    ...scope,
    app_sql_datev2: scheduleDate,
    app_dow_rawv2: dowName(dayOfWeekUtc(scheduleDate)),
    shifted_to_next_dayv2: false,
    scope_key: buildScopeKey(scope.app_show_idv2, scheduleDate, dowName(dayOfWeekUtc(scheduleDate)), false),
    app_sql_date_source: source,
    scope_resolution_source: source,
  };
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
      "hb_at",
      "show_id",
      "sql_date",
      "app_show_id",
      "app_sql_date",
      "app_dow_raw",
      "shifted_to_next_day",
      "mode",
      "show_date",
      "time",
      "set_to_default_app_sql_date",
      "default_app_sql_date_is",
      "show_app_sql_end_date",
      "show_app_sql_start_date",
      "app_sql_date_source",
      "customer_id",
      "focus_day",
      "ring_collection",
      "show_scope_key",
      "show",
    ],
  });

  if (!rows.length) {
    throw new Error(`No heartbeat rows found in ${TABLE_HEARTBEAT}`);
  }

  return rows[0];
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
      "shifted_to_next_day",
      "heartbeat",
      "show_rid",
    ],
  });
}

function buildShowTargetBaseContext(heartbeatContext, showRow, targetInfo) {
  const fields = showRow?.fields || {};
  const appShowId = numOrNull(fields.show_id);
  if (appShowId === null) throw new Error(`show/${VIEW_SHOW_TARGET} row ${showRow?.id || "unknown"} missing show_id`);
  const customerId = numOrNull(fields.customer_id) ?? heartbeatContext.customer_id ?? CUSTOMER_ID;
  const appDowRaw = strictDowRaw(dowName(dayOfWeekUtc(targetInfo.target_date)), "show_target_app_dow_raw");

  return {
    ...heartbeatContext,
    app_show_idv2: appShowId,
    raw_sql_date: targetInfo.focus_day,
    current_app_sql_date: targetInfo.target_date,
    current_app_dow_raw: appDowRaw,
    current_shifted_to_next_day: boolValue(targetInfo.shifted_to_next_day),
    current_set_to_default_app_sql_date: false,
    current_default_app_sql_date_is: targetInfo.focus_day,
    current_show_app_sql_start_date: targetInfo.start_date,
    current_show_app_sql_end_date: targetInfo.end_date,
    current_app_sql_date_source: "show_heartbeat_target",
    customer_id: customerId,
    focus_day: targetInfo.focus_day,
    show_record_id: showRow.id,
    show_scope_key: `${customerId}|${appShowId}|${targetInfo.target_date}`,
    show_target_record_id: showRow.id,
    show_target: targetInfo,
  };
}

async function fetchWatchScheduleScopeStatusChoices() {
  const response = await fetchWithRetry(`https://api.airtable.com/v0/meta/bases/${AIRTABLE_BASE_ID}/tables`, {
    headers: { Authorization: `Bearer ${AIRTABLE_TOKEN}` },
  });

  if (!response.ok) {
    const body = await response.text().catch(() => "");
    throw new Error(`Airtable meta failed (${response.status}) watch_schedule: ${body}`);
  }

  const json = await response.json().catch(() => ({}));
  const table = Array.isArray(json?.tables)
    ? json.tables.find((item) => String(item?.name || "").trim() === TABLE_WATCH_SCHEDULE)
    : null;
  const field = Array.isArray(table?.fields)
    ? table.fields.find((item) => String(item?.name || "").trim() === "scope_status")
    : null;
  const choices = Array.isArray(field?.options?.choices) ? field.options.choices : [];
  return new Set(choices.map((choice) => String(choice?.name || "").trim()).filter(Boolean));
}

async function fetchHeartbeatFieldSet() {
  const response = await fetchWithRetry(`https://api.airtable.com/v0/meta/bases/${AIRTABLE_BASE_ID}/tables`, {
    headers: { Authorization: `Bearer ${AIRTABLE_TOKEN}` },
  });

  if (!response.ok) {
    const body = await response.text().catch(() => "");
    throw new Error(`Airtable meta failed (${response.status}) heartbeat: ${body}`);
  }

  const json = await response.json().catch(() => ({}));
  const table = Array.isArray(json?.tables)
    ? json.tables.find((item) => String(item?.name || "").trim() === TABLE_HEARTBEAT)
    : null;
  return new Set(Array.isArray(table?.fields) ? table.fields.map((field) => String(field?.name || "").trim()).filter(Boolean) : []);
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

async function fetchTableFieldMeta(tableName) {
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

  return buildAirtableFieldMeta(Array.isArray(table?.fields) ? table.fields : []);
}

function resolveFieldName(fieldMeta, logicalName) {
  if (!fieldMeta || !logicalName) return null;
  return writableFieldName(fieldMeta, logicalName);
}

function setResolvedField(fields, fieldMeta, logicalName, value) {
  const actualName = resolveFieldName(fieldMeta, logicalName);
  if (!actualName || value === undefined) return;
  fields[actualName] = value;
}

function clearResolvedField(fields, fieldMeta, logicalName) {
  const actualName = resolveFieldName(fieldMeta, logicalName);
  if (!actualName) return;
  fields[actualName] = null;
}

function scheduleRowKeyFromFields(fields = {}) {
  const scheduleKey = normalizeKey(firstValue(fields.schedule_key));
  if (scheduleKey) return scheduleKey;

  const classGroupId = numOrNull(fields.class_group_id);
  const classNumber = numOrNull(fields.class_number);
  if (classGroupId !== null && classNumber !== null) {
    return `${classGroupId}_${classNumber}`;
  }

  return normalizeKey(pickFirst(fields.class_groupxclasses_id, fields.class_id));
}

function scheduleRowCandidateKeysFromFields(fields = {}) {
  const keys = new Set();
  const add = (value) => {
    const key = normalizeKey(value);
    if (key) keys.add(key);
  };

  const scheduleKey = normalizeKey(firstValue(fields.schedule_key));
  add(scheduleKey);
  const scheduleKeyParts = scheduleKey ? scheduleKey.split("|").map(keyPart) : [];
  if (scheduleKeyParts.length >= 4) add(scheduleKeyParts.slice(0, 4).join("|"));

  const baseParts = buildScheduleKeyParts({
    sid: pickFirst(fields.show_id, fields.app_show_idv2, fields.app_sid),
    sqlDate: pickFirst(toIsoDateOnly(fields.schedule_show_datev2), fields.app_sql_datev2, fields.sql_date, fields.schedule_date),
    ringNumber: fields.ring_number,
    classNumber: fields.class_number,
  });
  add(baseParts.scheduleKey);
  const realTieBreaker = keyPart(fields.class_group_sequence);
  if (realTieBreaker) {
    add(buildScheduleKeyParts({
      sid: pickFirst(fields.show_id, fields.app_show_idv2, fields.app_sid),
      sqlDate: pickFirst(toIsoDateOnly(fields.schedule_show_datev2), fields.app_sql_datev2, fields.sql_date, fields.schedule_date),
      ringNumber: fields.ring_number,
      classNumber: fields.class_number,
      tieBreaker: realTieBreaker,
    }).scheduleKey);
  }

  const classGroupId = numOrNull(fields.class_group_id);
  const classNumber = numOrNull(fields.class_number);
  if (classGroupId !== null && classNumber !== null) add(`${classGroupId}_${classNumber}`);
  add(pickFirst(fields.class_groupxclasses_id, fields.class_id));
  return [...keys];
}

function splitNumericStrings(value) {
  const raw = Array.isArray(value) ? value : String(value || "").split(",");
  return raw
    .map((item) => String(item).trim())
    .filter(Boolean)
    .filter((item) => numOrNull(item) !== null);
}

function resolveGroupsLiveClassId(groupsLiveDetail, classNumber) {
  const wantedClassNumber = numOrNull(classNumber);
  if (wantedClassNumber === null || !groupsLiveDetail) return null;

  const classIds = groupsLiveDetail.class_ids || splitNumericStrings(groupsLiveDetail.classes);
  const classNumbers = groupsLiveDetail.class_numbers || splitNumericStrings(
    pickFirst(groupsLiveDetail.classNumbers, groupsLiveDetail.class_numbers_list)
  );

  for (let index = 0; index < classNumbers.length; index += 1) {
    if (numOrNull(classNumbers[index]) === wantedClassNumber) {
      return numOrNull(classIds[index]);
    }
  }

  return null;
}

function buildBaseHeartbeatContext(record) {
  const fields = record?.fields || {};
  const rawShowId = numOrNull(fields.show_id);
  const appShowId = numOrNull(fields.app_show_id);
  const rawSqlDate = strictSqlDate(fields.sql_date, "sql_date");
  const mode = strictMode(fields.mode, "mode");
  const recordId = strOrNull(fields.record_id);

  if (rawShowId === null) throw new Error("Latest heartbeat is missing show_id");
  if (appShowId === null) throw new Error("Latest heartbeat is missing app_show_id");
  if (rawShowId !== appShowId) throw new Error(`Heartbeat app_show_id mismatch: show_id=${rawShowId} app_show_id=${appShowId}`);
  if (!recordId) throw new Error("Latest heartbeat is missing record_id");
  if (recordId !== record.id) {
    throw new Error(`Heartbeat record_id mismatch: field=${recordId} actual=${record.id}`);
  }

  return {
    heartbeat_record_id: record.id,
    heartbeat_rid: recordId,
    hb_at: strOrNull(fields.hb_at),
    app_show_idv2: appShowId,
    scope_run_id: strOrNull(fields.heartbeat_id) || record.id,
    heartbeat_time: strOrNull(fields.time),
    heartbeat_show_date: toIsoDateOnly(fields.show_date),
    raw_sql_date: rawSqlDate,
    mode,
    current_app_sql_date: strOrNull(fields.app_sql_date),
    current_app_dow_raw: strOrNull(fields.app_dow_raw),
    current_shifted_to_next_day: boolValue(fields.shifted_to_next_day),
    current_set_to_default_app_sql_date: boolValue(fields.set_to_default_app_sql_date),
    current_default_app_sql_date_is: strOrNull(fields.default_app_sql_date_is),
    current_show_app_sql_start_date: strOrNull(fields.show_app_sql_start_date),
    current_show_app_sql_end_date: strOrNull(fields.show_app_sql_end_date),
    current_show_app_name: strOrNull(fields.show_app_name),
    current_app_sql_date_source: strOrNull(fields.app_sql_date_source),
    customer_id: numOrNull(fields.customer_id),
    focus_day: toIsoDateOnly(fields.focus_day),
    ring_collection: strOrNull(fields.ring_collection),
    show_scope_key: strOrNull(fields.show_scope_key),
    show_record_id: firstValue(fields.show) || null,
  };
}

function extractScheduleDefaultInfo(payload) {
  const show = payload?.show && typeof payload.show === "object" ? payload.show : {};
  const showAppName = strOrNull(pickFirst(show.show_name, payload?.show_name));
  const showAppSqlStartDate = toIsoDateOnly(pickFirst(show.start_date, payload?.start_date));
  const showAppSqlEndDate = toIsoDateOnly(pickFirst(show.end_date, payload?.end_date));
  const defaultAppSqlDateIs = strictSqlDate(pickFirst(payload?.show_date, payload?.showDate), "default_app_sql_date_is");
  const validDates = Array.isArray(payload?.show_days_list)
    ? payload.show_days_list.map((item) => toIsoDateOnly(item?.date)).filter(Boolean)
    : [];

  return {
    show_app_name: showAppName,
    show_app_sql_start_date: showAppSqlStartDate,
    show_app_sql_end_date: showAppSqlEndDate,
    default_app_sql_date_is: defaultAppSqlDateIs,
    valid_dates: validDates,
  };
}

function candidateDateFromMode(rawSqlDate, mode) {
  return strictSqlDate(rawSqlDate, "sql_date");
}

function hasHotpatchScopeOverride() {
  return HOTPATCH_APP_SHOW_ID || HOTPATCH_APP_SQL_DATE;
}

function assertHotpatchScopeMatches(baseContext) {
  if (!hasHotpatchScopeOverride()) return;
  if (!HOTPATCH_APP_SHOW_ID || !HOTPATCH_APP_SQL_DATE) {
    throw new Error("HOTPATCH_APP_SHOW_ID and HOTPATCH_APP_SQL_DATE must be set together");
  }
  const showId = Number(HOTPATCH_APP_SHOW_ID);
  if (!Number.isFinite(showId)) {
    throw new Error(`HOTPATCH_APP_SHOW_ID must be numeric: ${HOTPATCH_APP_SHOW_ID}`);
  }
  if (showId !== baseContext.app_show_idv2) {
    throw new Error(`HOTPATCH_APP_SHOW_ID mismatch: heartbeat=${baseContext.app_show_idv2} override=${showId}`);
  }
}

function isValidAppSqlDate(candidateDate, scheduleInfo) {
  if (scheduleInfo.valid_dates.length) {
    return scheduleInfo.valid_dates.includes(candidateDate);
  }
  if (scheduleInfo.show_app_sql_start_date && compareSqlDate(candidateDate, scheduleInfo.show_app_sql_start_date) < 0) {
    return false;
  }
  if (scheduleInfo.show_app_sql_end_date && compareSqlDate(candidateDate, scheduleInfo.show_app_sql_end_date) > 0) {
    return false;
  }
  return true;
}

function resolveHeartbeatScope(baseContext, emptyPayload) {
  const scheduleInfo = extractScheduleDefaultInfo(emptyPayload);
  const candidateAppSqlDate = strictSqlDate(
    pickFirst(baseContext.current_app_sql_date, candidateDateFromMode(baseContext.raw_sql_date, baseContext.mode)),
    "app_sql_date"
  );
  const showHeartbeatScoped = strOrNull(baseContext.current_app_sql_date_source) === "show_heartbeat_target";
  const validCandidate = isValidAppSqlDate(candidateAppSqlDate, scheduleInfo);
  assertHotpatchScopeMatches(baseContext);
  const setToDefault = hasHotpatchScopeOverride() || showHeartbeatScoped ? false : !validCandidate;
  const baseFinalAppSqlDate = hasHotpatchScopeOverride()
    ? HOTPATCH_APP_SQL_DATE
    : showHeartbeatScoped
    ? candidateAppSqlDate
    : setToDefault
    ? scheduleInfo.default_app_sql_date_is
    : candidateAppSqlDate;
  const finalAppSqlDate = baseFinalAppSqlDate;
  const finalAppDowRaw = strictDowRaw(dowName(dayOfWeekUtc(finalAppSqlDate)), "derived_app_dow_raw");
  const shiftedToNextDay = boolValue(baseContext.current_shifted_to_next_day);
  const appSqlDateSource = hasHotpatchScopeOverride()
    ? "hotpatch_env_override"
    : setToDefault
    ? "default_day"
    : strOrNull(baseContext.current_app_sql_date_source) || "heartbeat_app_sql_date";

  return {
    heartbeat_record_id: baseContext.heartbeat_record_id,
    heartbeat_rid: baseContext.heartbeat_rid,
    hb_at: baseContext.hb_at,
    app_show_idv2: baseContext.app_show_idv2,
    app_sql_datev2: finalAppSqlDate,
    app_dow_rawv2: finalAppDowRaw,
    shifted_to_next_dayv2: shiftedToNextDay,
    scope_key: buildScopeKey(baseContext.app_show_idv2, finalAppSqlDate, finalAppDowRaw, shiftedToNextDay),
    scope_run_id: baseContext.scope_run_id,
    heartbeat_time: baseContext.heartbeat_time,
    heartbeat_show_date: baseContext.heartbeat_show_date,
    raw_sql_date: baseContext.raw_sql_date,
    mode: baseContext.mode,
    set_to_default_app_sql_date: setToDefault,
    default_app_sql_date_is: scheduleInfo.default_app_sql_date_is,
    show_app_sql_start_date: scheduleInfo.show_app_sql_start_date,
    show_app_sql_end_date: scheduleInfo.show_app_sql_end_date,
    show_app_name: scheduleInfo.show_app_name,
    app_sql_date_source: appSqlDateSource,
    candidate_app_sql_date: candidateAppSqlDate,
    customer_id: baseContext.customer_id,
    focus_day: baseContext.focus_day,
    ring_collection: baseContext.ring_collection,
    show_scope_key: baseContext.show_scope_key,
    show_record_id: baseContext.show_record_id,
  };
}

function resolveHeartbeatScopeFromCurrentHeartbeat(baseContext, reason) {
  const candidateAppSqlDate = strictSqlDate(
    pickFirst(baseContext.current_app_sql_date, candidateDateFromMode(baseContext.raw_sql_date, baseContext.mode)),
    "app_sql_date"
  );
  assertHotpatchScopeMatches(baseContext);
  const finalAppSqlDate = strictSqlDate(
    pickFirst(HOTPATCH_APP_SQL_DATE, candidateAppSqlDate, baseContext.current_app_sql_date, candidateAppSqlDate),
    "app_sql_date"
  );
  const inferredAppDowRaw = dowName(dayOfWeekUtc(finalAppSqlDate));
  const finalAppDowRaw = strictDowRaw(
    pickFirst(baseContext.current_app_dow_raw, inferredAppDowRaw),
    "app_dow_raw"
  );
  const currentSource = strOrNull(baseContext.current_app_sql_date_source);
  const setToDefault = hasHotpatchScopeOverride()
    ? false
    : currentSource === "default_day"
    ? true
    : boolValue(baseContext.current_set_to_default_app_sql_date);
  const shiftedToNextDay = boolValue(baseContext.current_shifted_to_next_day);
  const defaultAppSqlDateIs = strictSqlDate(
    pickFirst(baseContext.current_default_app_sql_date_is, finalAppSqlDate),
    "default_app_sql_date_is"
  );
  const appSqlDateSource = hasHotpatchScopeOverride() ? "hotpatch_env_override" : currentSource || (
    setToDefault
      ? "default_day"
      : "heartbeat_app_sql_date"
  );

  return {
    heartbeat_record_id: baseContext.heartbeat_record_id,
    heartbeat_rid: baseContext.heartbeat_rid,
    hb_at: baseContext.hb_at,
    app_show_idv2: baseContext.app_show_idv2,
    app_sql_datev2: finalAppSqlDate,
    app_dow_rawv2: finalAppDowRaw,
    shifted_to_next_dayv2: shiftedToNextDay,
    scope_key: buildScopeKey(baseContext.app_show_idv2, finalAppSqlDate, finalAppDowRaw, shiftedToNextDay),
    scope_run_id: baseContext.scope_run_id,
    heartbeat_time: baseContext.heartbeat_time,
    heartbeat_show_date: baseContext.heartbeat_show_date,
    raw_sql_date: baseContext.raw_sql_date,
    mode: baseContext.mode,
    set_to_default_app_sql_date: setToDefault,
    default_app_sql_date_is: defaultAppSqlDateIs,
    show_app_sql_start_date: baseContext.current_show_app_sql_start_date,
    show_app_sql_end_date: baseContext.current_show_app_sql_end_date,
    show_app_name: baseContext.current_show_app_name,
    app_sql_date_source: appSqlDateSource,
    candidate_app_sql_date: candidateAppSqlDate,
    scope_resolution_source: "heartbeat_current_fields_fallback",
    scope_resolution_error: strOrNull(reason),
    customer_id: baseContext.customer_id,
    focus_day: baseContext.focus_day,
    ring_collection: baseContext.ring_collection,
    show_scope_key: baseContext.show_scope_key,
    show_record_id: baseContext.show_record_id,
  };
}

function buildHeartbeatPatchFields(resolvedScope, heartbeatFieldSet) {
  const fields = {};
  const maybeSet = (name, value) => {
    if (!heartbeatFieldSet.has(name)) return;
    fields[name] = value;
  };

  maybeSet("app_sql_date", resolvedScope.app_sql_datev2);
  maybeSet("app_dow_raw", resolvedScope.app_dow_rawv2);
  maybeSet("shifted_to_next_day", resolvedScope.shifted_to_next_dayv2);
  maybeSet("set_to_default_app_sql_date", resolvedScope.set_to_default_app_sql_date);
  maybeSet("default_app_sql_date_is", resolvedScope.default_app_sql_date_is);
  maybeSet("show_app_sql_start_date", resolvedScope.show_app_sql_start_date);
  maybeSet("show_app_sql_end_date", resolvedScope.show_app_sql_end_date);
  maybeSet("show_app_name", resolvedScope.show_app_name);
  maybeSet("app_sql_date_source", resolvedScope.app_sql_date_source);

  return fields;
}

function diffHeartbeatFields(currentFields, nextFields) {
  const diff = {};
  for (const [name, value] of Object.entries(nextFields)) {
    if (!sameValue(currentFields?.[name], value)) diff[name] = value;
  }
  return diff;
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

async function fetchExistingRowsForShow(appShowId) {
  const rows = await airtableList(TABLE_WATCH_SCHEDULE, {
    pageSize: 100,
  });

  return rows.filter((row) => {
    const fields = row?.fields || {};
    const rawShowId = numOrNull(fields.show_id);
    const appShowIdV2 = numOrNull(fields.app_show_idv2);
    return rawShowId === appShowId || appShowIdV2 === appShowId;
  });
}

async function fetchHeartbeatViewRows() {
  return airtableList(TABLE_WATCH_SCHEDULE, {
    view: VIEW_WATCH_SCHEDULE_HEARTBEAT,
    pageSize: 100,
    "fields[]": ["class_groupxclasses_id", "class_group_id", "class_number", "class_id", "heartbeat", "is_current_scope"],
  });
}

function chooseExistingWinner(rows, heartbeatViewIdSet) {
  if (!rows.length) return null;

  const scored = rows.map((row, index) => {
    let score = 0;
    if (heartbeatViewIdSet.has(row.id)) score += 10;
    if (boolValue(row?.fields?.is_current_scope)) score += 5;
    if (firstValue(row?.fields?.heartbeat)) score += 3;
    score -= index;
    return { row, score };
  });

  scored.sort((a, b) => b.score - a.score);
  return scored[0].row;
}

function hasManualTimeOverride(fields) {
  return boolValue(fields?.manual_time_overide) || boolValue(fields?.manual_time_override);
}

function buildCurrentFields(
  normalizedRow,
  scope,
  heartbeatRecordId,
  showRecordId,
  nowIso,
  dateOnly,
  recordState,
  scopeStatusValue,
  watchScheduleFieldMeta,
  options = {}
) {
  const fields = { ...normalizedRow.fields };
  const isCurrentScope = options.isCurrentScope !== false;
  const classDetail = normalizedRow?.class_detail || null;
  const groupsLiveDetail = normalizedRow?.groups_live_detail || null;
  const groupsLiveDay = toIsoDateOnly(groupsLiveDetail?.day);
  const resolvedScheduledDate = toIsoDateOnly(
    pickFirst(classDetail?.schedule_date, fields.scheduled_date, fields.schedule_show_datev2, fields.show_date, groupsLiveDay)
  );
  delete fields.scope_status;
  if (resolvedScheduledDate) {
    setResolvedField(fields, watchScheduleFieldMeta, "schedule_date", resolvedScheduledDate);
    setResolvedField(fields, watchScheduleFieldMeta, "scheduled_date", resolvedScheduledDate);
  }
  if (classDetail?.status) {
    setResolvedField(fields, watchScheduleFieldMeta, "status", classDetail.status);
  }
  if (classDetail?.total_trips !== null && classDetail?.total_trips !== undefined) {
    setResolvedField(fields, watchScheduleFieldMeta, "total_trips", classDetail.total_trips);
  } else if (fields.total_trips !== undefined) {
    setResolvedField(fields, watchScheduleFieldMeta, "total_trips", fields.total_trips);
  }
  const completedTrips = classDetail?.completed_trips ?? fields.completed_trips;
  if (completedTrips !== null && completedTrips !== undefined) {
    setResolvedField(fields, watchScheduleFieldMeta, "completed_trips", completedTrips);
  }
  if (groupsLiveDetail) {
    if (groupsLiveDetail.recordId) {
      setResolvedField(fields, watchScheduleFieldMeta, "groups_live", [groupsLiveDetail.recordId]);
    }
    const resolvedClassId = resolveGroupsLiveClassId(groupsLiveDetail, fields.class_number);
    if (resolvedClassId !== null && isBlank(fields.class_id)) {
      setResolvedField(fields, watchScheduleFieldMeta, "class_id", resolvedClassId);
    }
    if (groupsLiveDetail.ring_number !== null && groupsLiveDetail.ring_number !== undefined && isBlank(fields.ring_number)) {
      setResolvedField(fields, watchScheduleFieldMeta, "ring_number", groupsLiveDetail.ring_number);
    }
    if (groupsLiveDay) {
      if (isBlank(fields.show_date)) setResolvedField(fields, watchScheduleFieldMeta, "show_date", groupsLiveDay);
      if (isBlank(fields.sql_date)) setResolvedField(fields, watchScheduleFieldMeta, "sql_date", groupsLiveDay);
      if (isBlank(fields.schedule_show_datev2)) setResolvedField(fields, watchScheduleFieldMeta, "schedule_show_datev2", groupsLiveDay);
      if (isBlank(fields.scheduled_date)) setResolvedField(fields, watchScheduleFieldMeta, "scheduled_date", groupsLiveDay);
      if (isBlank(fields.schedule_date)) setResolvedField(fields, watchScheduleFieldMeta, "schedule_date", groupsLiveDay);
    }
    if (groupsLiveDetail.estimated_start_time) {
      setResolvedField(fields, watchScheduleFieldMeta, "estimated_start_time", groupsLiveDetail.estimated_start_time);
      setResolvedField(fields, watchScheduleFieldMeta, "latest_estimated_start_time", groupsLiveDetail.estimated_start_time);
      setResolvedField(fields, watchScheduleFieldMeta, "___latest_estimated_start_time", groupsLiveDetail.estimated_start_time);
    }
    if (groupsLiveDetail.status) {
      setResolvedField(fields, watchScheduleFieldMeta, "status", groupsLiveDetail.status);
      setResolvedField(fields, watchScheduleFieldMeta, "latest_status", groupsLiveDetail.status);
    }
    if (groupsLiveDetail.total !== null && groupsLiveDetail.total !== undefined) {
      setResolvedField(fields, watchScheduleFieldMeta, "total_trips", groupsLiveDetail.total);
    }
    if (groupsLiveDetail.gone !== null && groupsLiveDetail.gone !== undefined) {
      setResolvedField(fields, watchScheduleFieldMeta, "completed_trips", groupsLiveDetail.gone);
    }
    setResolvedField(fields, watchScheduleFieldMeta, "latest_ingested_at", pickFirst(groupsLiveDetail.ingested_at, groupsLiveDetail.curr_updated_at));
  }
  const scheduleTieBreaker = keyPart(normalizedRow.schedule_key_tie_breaker);
  const scheduleKeys = buildScheduleKeyParts({
    sid: pickFirst(fields.show_id, scope.app_show_idv2),
    sqlDate: pickFirst(resolvedScheduledDate, fields.app_sql_datev2, scope.app_sql_datev2),
    ringNumber: fields.ring_number,
    classNumber: fields.class_number,
    tieBreaker: scheduleTieBreaker,
  });
  if (scheduleKeys.scheduleKey) setResolvedField(fields, watchScheduleFieldMeta, "schedule_key", scheduleKeys.scheduleKey);
  if (scheduleKeys.scheduleShort) setResolvedField(fields, watchScheduleFieldMeta, "schedule_short", scheduleKeys.scheduleShort);
  const fullNestingParts = [
    pickFirst(fields.show_id, scope.app_show_idv2),
    pickFirst(resolvedScheduledDate, fields.app_sql_datev2, scope.app_sql_datev2),
    fields.ring_number,
    fields.estimated_start_time,
    fields.class_group_id,
    fields.class_number,
  ];
  if (scheduleTieBreaker) fullNestingParts.push(scheduleTieBreaker);
  fullNestingParts.push(fields.pid, fields.entry_number);
  const fullNestingKey = joinKeyParts(fullNestingParts);
  if (fullNestingKey) setResolvedField(fields, watchScheduleFieldMeta, "full_nesting_key", fullNestingKey);
  Object.assign(fields, buildScopeFieldPatch(watchScheduleFieldMeta, scope));
  fields.heartbeat = isCurrentScope && heartbeatRecordId ? [heartbeatRecordId] : [];
  fields.record_state = recordState;
  fields.run_tag = scope.app_sql_datev2;
  fields.last_updated_at = nowIso;
  fields.is_current_scope = isCurrentScope;
  fields.scope_run_id = scope.scope_run_id;
  setResolvedField(fields, watchScheduleFieldMeta, "inactive", false);
  setResolvedField(fields, watchScheduleFieldMeta, "archive", false);
  if (scopeStatusValue) fields.scope_status = scopeStatusValue;
  fields.last_seen_at = dateOnly;
  fields.dropped_at = null;
  fields.is_gotcha = false;
  if (showRecordId) fields.shows = [showRecordId];
  return fields;
}

function resolveActualScheduleShowDate(normalizedRow) {
  const fields = normalizedRow?.fields || {};
  const classDetail = normalizedRow?.class_detail || null;
  return toIsoDateOnly(
    pickFirst(fields.schedule_show_datev2, classDetail?.schedule_date, fields.show_date)
  );
}

function resolveExistingScheduleShowDate(row) {
  const fields = row?.fields || {};
  return toIsoDateOnly(
    pickFirst(fields.schedule_show_datev2, fields.show_date)
  );
}

function rowScheduledDateMatchesScope(normalizedRow, scope) {
  const fields = normalizedRow?.fields || {};
  const classDetail = normalizedRow?.class_detail || null;
  const resolvedScheduledDate = toIsoDateOnly(
    pickFirst(classDetail?.schedule_date, fields.scheduled_date, fields.schedule_show_datev2, fields.show_date)
  );
  return resolvedScheduledDate === scope.app_sql_datev2;
}

function classCatalogRowsFromPayload(payload) {
  if (Array.isArray(payload)) return payload;
  if (Array.isArray(payload?.classes)) return payload.classes;
  return [];
}

function buildClassIdByNumberFromClassesPayload(payload) {
  const byNumber = new Map();
  const conflicts = [];
  const conflictedNumbers = new Set();

  for (const row of classCatalogRowsFromPayload(payload)) {
    const classNumber = numOrNull(pickFirst(row?.number, row?.class_number, row?.classNumber));
    const classId = numOrNull(pickFirst(row?.class_id, row?.classId, row?.id));
    if (classNumber === null || classId === null) continue;

    const key = String(classNumber);
    const existing = byNumber.get(key);
    if (existing && existing.class_id !== classId) {
      conflicts.push({
        class_number: classNumber,
        class_ids: [existing.class_id, classId],
      });
      conflictedNumbers.add(key);
      byNumber.delete(key);
      continue;
    }

    if (!conflictedNumbers.has(key)) {
      byNumber.set(key, {
        class_id: classId,
        class_number: classNumber,
        class_name: strOrNull(pickFirst(row?.name, row?.class_name, row?.className)),
        entry_count: numOrNull(row?.entry_count),
      });
    }
  }

  return {
    byNumber,
    catalog_rows: classCatalogRowsFromPayload(payload).length,
    usable_rows: byNumber.size,
    conflicts,
  };
}

function applyClassListIdEnrichment(rows, classIdByNumber) {
  let enriched = 0;
  const nextRows = (rows || []).map((row) => {
    const fields = row?.fields || {};
    if (!isBlank(fields.class_id)) return row;

    const classNumber = numOrNull(fields.class_number);
    if (classNumber === null) return row;

    const match = classIdByNumber?.get?.(String(classNumber));
    if (!match?.class_id) return row;

    enriched += 1;
    return {
      ...row,
      fields: {
        ...fields,
        class_id: match.class_id,
      },
      class_id_enrichment: {
        source: "classes_list",
        class_number: classNumber,
        class_id: match.class_id,
      },
    };
  });

  return { rows: nextRows, enriched };
}

function existingScheduleRowMatchesScope(row, scope) {
  const fields = row?.fields || {};
  const rowShowId = numOrNull(pickFirst(fields.show_id, fields.app_show_idv2, fields.sid));
  if (rowShowId !== null && rowShowId !== numOrNull(scope?.app_show_idv2)) return false;

  const rowDate = toIsoDateOnly(
    pickFirst(fields.schedule_show_datev2, fields.app_sql_datev2, fields.show_date, fields.scheduled_date)
  );
  if (rowDate && rowDate !== scope?.app_sql_datev2) return false;

  return true;
}

function buildDroppedFields(scope, nowIso, dateOnly, scopeStatusValue, watchScheduleFieldMeta) {
  const fields = {
    heartbeat: [],
    is_current_scope: false,
    dropped_at: dateOnly,
    last_updated_at: nowIso,
    run_tag: scope.app_sql_datev2,
    record_state: "existing",
  };
  Object.assign(fields, buildScopeFieldPatch(watchScheduleFieldMeta, scope));
  setResolvedField(fields, watchScheduleFieldMeta, "inactive", true);
  if (scopeStatusValue) fields.scope_status = scopeStatusValue;
  return fields;
}

function minTimeText(left, right) {
  if (!left) return right || null;
  if (!right) return left || null;
  return String(left) <= String(right) ? left : right;
}

function applyPreliveEstimatedStartTimeGuard(fields, existingRow, scope) {
  const candidate = strOrNull(fields?.estimated_start_time);
  if (!candidate) return null;
  if (String(scope?.mode || "").toUpperCase() !== "NIGHT") return null;
  if (!boolValue(scope?.shifted_to_next_dayv2)) return null;
  if (!isSuspiciousPreliveEstimatedStartTime(candidate)) return null;

  const existingValue = strOrNull(existingRow?.fields?.estimated_start_time);
  if (existingValue && existingValue !== candidate) {
    fields.estimated_start_time = existingValue;
    return {
      action: "preserved_existing",
      candidate,
      existing: existingValue,
    };
  }

  delete fields.estimated_start_time;
  return {
    action: "omitted_suspicious",
    candidate,
    existing: existingValue || null,
  };
}

function maxTimeText(left, right) {
  if (!left) return right || null;
  if (!right) return left || null;
  return String(left) >= String(right) ? left : right;
}

function parseTimestampMs(value) {
  const text = strOrNull(value);
  if (!text) return null;
  const ms = Date.parse(text);
  return Number.isFinite(ms) ? ms : null;
}

function groupsLiveRecencyMs(row) {
  return parseTimestampMs(pickFirst(row?.curr_updated_at, row?.ingested_at, row?.created_time)) || 0;
}

function chooseGroupsLiveWinner(existing, candidate) {
  if (!existing) return candidate;
  if (boolValue(candidate?.is_live) && !boolValue(existing?.is_live)) return candidate;
  if (!boolValue(candidate?.is_live) && boolValue(existing?.is_live)) return existing;
  return groupsLiveRecencyMs(candidate) >= groupsLiveRecencyMs(existing) ? candidate : existing;
}

async function fetchGroupsLiveRows(appShowId, targetDays) {
  const fieldSet = await fetchTableFieldSet(TABLE_GROUPS_LIVE).catch(() => new Set());
  const baseFields = [
    "class_group_id",
    "show_id",
    "day",
    "ring_number",
    "estimated_start_time",
    "gone",
    "total",
    "status",
    "curr_updated_at",
    "ingested_at",
    "created_time",
    "is_live",
    "stop_updating",
    "classes",
    "classNumbers",
    "class_numbers",
    "class_numbers_list",
  ];
  const requestedFields = fieldSet.size
    ? baseFields.filter((fieldName) => fieldSet.has(fieldName))
    : baseFields;

  if (fieldSet.size && (!fieldSet.has("class_group_id") || !fieldSet.has("show_id"))) {
    return [];
  }

  const query = { pageSize: 100 };
  if (requestedFields.length) query["fields[]"] = requestedFields;

  const normalizedDays = new Set(
    Array.from(targetDays || [])
      .map((value) => toIsoDateOnly(value))
      .filter(Boolean)
  );

  const rows = await airtableList(TABLE_GROUPS_LIVE, query);
  return rows
    .map((row) => {
      const fields = row?.fields || {};
      return {
        recordId: row.id,
        class_group_id: numOrNull(fields.class_group_id),
        show_id: numOrNull(fields.show_id),
        day: toIsoDateOnly(fields.day),
        ring_number: numOrNull(fields.ring_number),
        estimated_start_time: strOrNull(fields.estimated_start_time),
        gone: numOrNull(fields.gone),
        total: numOrNull(fields.total),
        status: strOrNull(fields.status),
        curr_updated_at: strOrNull(fields.curr_updated_at),
        ingested_at: strOrNull(fields.ingested_at),
        created_time: strOrNull(fields.created_time),
        is_live: boolValue(fields.is_live),
        stop_updating: boolValue(fields.stop_updating),
        class_ids: splitNumericStrings(fields.classes),
        class_numbers: splitNumericStrings(pickFirst(fields.classNumbers, fields.class_numbers, fields.class_numbers_list)),
      };
    })
    .filter((row) => row.class_group_id !== null)
    .filter((row) => row.show_id === appShowId)
    .filter((row) => !row.stop_updating)
    .filter((row) => !normalizedDays.size || !row.day || normalizedDays.has(row.day));
}

function buildGroupsLiveMap(rows) {
  const byGroupId = new Map();
  for (const row of rows || []) {
    const key = normalizeKey(row?.class_group_id);
    if (!key) continue;
    byGroupId.set(key, chooseGroupsLiveWinner(byGroupId.get(key), row));
  }
  return byGroupId;
}

function applyGroupsLiveFallback(rows, groupsById) {
  let matched = 0;

  const nextRows = (rows || []).map((row) => {
    const classGroupId = numOrNull(row?.fields?.class_group_id);
    const groupsLiveDetail = groupsById?.get?.(normalizeKey(classGroupId)) || null;
    if (!groupsLiveDetail) return row;

    matched += 1;
    return {
      ...row,
      groups_live_detail: groupsLiveDetail,
    };
  });

  return { rows: nextRows, matched };
}

function buildActiveGroupRows(rows, scope, runId, dateOnly, activeGroupsFieldSet) {
  if (!activeGroupsFieldSet.size) return [];

  const byGroup = new Map();

  for (const row of rows || []) {
    const fields = row?.fields || {};
    const classGroupId = numOrNull(fields.class_group_id);
    if (classGroupId === null) continue;

    const key = `${scope.app_show_idv2}|${classGroupId}`;
    const existing = byGroup.get(key) || {
      key,
      app_sid: scope.app_show_idv2,
      app_sql_date: scope.app_sql_datev2,
      class_group_id: classGroupId,
      class_group_sequence: undefined,
      group_name: null,
      estimated_start_time: null,
      estimated_end_time: null,
      ring_number: undefined,
      run_id: runId,
      last_run: dateOnly,
      inactive: false,
      schedule_date: null,
      scheduled_date: null,
      scheduled_estimated_start_time: null,
    };

    existing.class_group_sequence = existing.class_group_sequence ?? numOrNull(fields.class_group_sequence);
    existing.group_name = existing.group_name || strOrNull(fields.group_name);
    existing.ring_number = existing.ring_number ?? numOrNull(fields.ring_number);
    existing.estimated_start_time = minTimeText(existing.estimated_start_time, strOrNull(fields.estimated_start_time));
    existing.estimated_end_time = maxTimeText(existing.estimated_end_time, strOrNull(fields.estimated_end_time));

    const resolvedScheduleDate = toIsoDateOnly(pickFirst(fields.schedule_show_datev2, fields.show_date));
    existing.schedule_date = existing.schedule_date || resolvedScheduleDate;
    existing.scheduled_date = existing.scheduled_date || resolvedScheduleDate;
    existing.scheduled_estimated_start_time = existing.scheduled_estimated_start_time || strOrNull(fields.estimated_start_time);

    byGroup.set(key, existing);
  }

  return [...byGroup.values()].map((row) => {
    const fields = {};
    for (const [name, value] of Object.entries(row)) {
      if (!activeGroupsFieldSet.has(name)) continue;
      if (value === undefined || value === null) continue;
      if (typeof value === "string" && value.trim() === "") continue;
      fields[name] = value;
    }
    return fields;
  });
}

async function upsertActiveGroups({
  fieldSet,
  rows,
  scopeAppSid,
  runId,
  lastRun,
}) {
  if (!fieldSet.size) {
    return {
      table: TABLE_ACTIVE_GROUPS,
      created_planned: 0,
      updated_planned: 0,
      inactivated_planned: 0,
      writes: { created: 0, updated: 0, inactivated: 0, create_failures: [], update_failures: [], inactivate_failures: [] },
      skipped: true,
    };
  }

  const existingRows = await airtableList(TABLE_ACTIVE_GROUPS, {
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
      fields: {
        inactive: true,
        run_id: runId,
        last_run: lastRun,
      },
    });
  }

  const summary = {
    table: TABLE_ACTIVE_GROUPS,
    created_planned: creates.length,
    updated_planned: updates.length,
    inactivated_planned: inactivations.length,
    writes: { created: 0, updated: 0, inactivated: 0, create_failures: [], update_failures: [], inactivate_failures: [] },
    skipped: false,
  };

  if (DRY_RUN) return summary;

  const createResult = await airtableCreateRecords(TABLE_ACTIVE_GROUPS, creates);
  const updateResult = await airtablePatchRecords(TABLE_ACTIVE_GROUPS, updates);
  const inactivateResult = await airtablePatchRecords(TABLE_ACTIVE_GROUPS, inactivations);

  summary.writes.created = createResult.okRows;
  summary.writes.updated = updateResult.okRows;
  summary.writes.inactivated = inactivateResult.okRows;
  summary.writes.create_failures = createResult.failedRows;
  summary.writes.update_failures = updateResult.failedRows;
  summary.writes.inactivate_failures = inactivateResult.failedRows;

  return summary;
}

async function runScheduleForBaseContext({
  heartbeatRecord,
  baseHeartbeatContext,
  nowIso,
  dateOnly,
  runId,
  currentScopeStatus,
  droppedScopeStatus,
  heartbeatFieldSet,
  watchScheduleFieldMeta,
  activeGroupsFieldSet,
  patchHeartbeat = true,
}) {
  const emptyUrl = buildScheduleEmptyEndpoint(baseHeartbeatContext.app_show_idv2, baseHeartbeatContext.customer_id || CUSTOMER_ID);
  let emptyPayload = null;
  let emptyPingError = null;
  let scope = null;

  try {
    emptyPayload = await fetchJson(emptyUrl, {
      automation_key: `schedules_dailyv2|empty_ping_schedule|${baseHeartbeatContext.app_show_idv2}|00/00/00`,
      automation_name: "schedules_dailyv2",
      source: "empty_ping_schedule",
      app_show_id: baseHeartbeatContext.app_show_idv2,
      app_sql_date: baseHeartbeatContext.raw_sql_date,
      run_id: runId,
      last_run: dateOnly,
    });
  } catch (error) {
    emptyPingError = String(error?.message || error);
  }

  if (emptyPayload) {
    try {
      scope = resolveHeartbeatScope(baseHeartbeatContext, emptyPayload);
    } catch (error) {
      emptyPingError = String(error?.message || error);
      scope = resolveHeartbeatScopeFromCurrentHeartbeat(baseHeartbeatContext, emptyPingError);
    }
  } else {
    scope = resolveHeartbeatScopeFromCurrentHeartbeat(baseHeartbeatContext, emptyPingError || "empty_ping_schedule_unavailable");
  }

  const heartbeatPatchFields = buildHeartbeatPatchFields(scope, heartbeatFieldSet);
  const heartbeatChangedFields = diffHeartbeatFields(heartbeatRecord?.fields || {}, heartbeatPatchFields);

  if (patchHeartbeat && !DRY_RUN && Object.keys(heartbeatChangedFields).length) {
    await airtablePatchRecords(TABLE_HEARTBEAT, [{
      id: heartbeatRecord.id,
      fields: heartbeatChangedFields,
    }]);
  }

  const datedUrl = buildScheduleEndpoint(scope.app_sql_datev2, scope.app_show_idv2, scope.customer_id || CUSTOMER_ID);
  let datedPayload = null;
  let datedFetchError = null;
  let datedFallback = null;

  try {
    datedPayload = await fetchJson(datedUrl, {
      automation_key: `schedules_dailyv2|dated_schedule|${scope.app_show_idv2}|${scope.app_sql_datev2}`,
      automation_name: "schedules_dailyv2",
      source: "dated_schedule",
      app_show_id: scope.app_show_idv2,
      app_sql_date: scope.app_sql_datev2,
      run_id: runId,
      last_run: dateOnly,
    });
  } catch (error) {
    datedFetchError = String(error?.message || error);
    if (!isSoftPayloadError(error) && !/^soft_payload_/i.test(datedFetchError)) throw error;

    datedFallback = loadScheduleFallbackPayload(scope.app_show_idv2, scope.app_sql_datev2);
    if (!datedFallback.ok) throw error;
    datedPayload = datedFallback.payload;
  }
  assertSchedulePayloadScope(
    datedPayload,
    scope,
    datedFallback?.ok ? "dated_schedule_fallback" : "dated_schedule"
  );

  const datedResult = {
    ok: true,
    source: datedFallback?.ok ? "dated_schedule_fallback" : "dated_schedule",
    url: datedUrl,
    fetch_error: datedFetchError,
    fallback_file: datedFallback?.file_path || null,
    fallback_body_length: datedFallback?.body_length || null,
    ...normalizeSchedulePayload(datedPayload, {
      scope,
      source: datedFallback?.ok ? "dated_schedule_fallback" : "dated_schedule",
      generatedAt: nowIso,
      generatedDate: dateOnly,
    }),
  };
  const schedulePayloadCache = datedFallback?.ok
    ? {
        enabled: PREFETCH_FORWARD_SCHEDULES,
        skipped: true,
        reason: "dated_schedule_from_fallback",
        early_schedule_dir: EARLY_SCHEDULE_PAYLOAD_DIR,
        current: null,
        forward: [],
      }
    : await cacheSuccessfulSchedulePayloads(scope, datedPayload, { runId, lastRun: dateOnly });

  const emptyResult = emptyPayload
    ? {
        ok: true,
        source: "empty_ping_schedule",
        url: emptyUrl,
        error: emptyPingError,
        ...normalizeSchedulePayload(emptyPayload, {
          scope,
          source: "empty_ping_schedule",
          generatedAt: nowIso,
          generatedDate: dateOnly,
        }),
      }
    : {
        ok: false,
        source: "empty_ping_schedule",
        url: emptyUrl,
        error: emptyPingError || "empty_ping_schedule_unavailable",
        rows: [],
        keep_keys: [],
        schedule_show_datev2: null,
      };

  const chosen = chooseScheduleVariant(datedResult, emptyResult);
  const classEndpointEnrichment = {
    skipped: true,
    reason: "classes_endpoint_unreliable_for_schedule_lane",
    enriched_rows: 0,
    failures: [],
  };
  let scopedRowsBase = chosen.rows.filter((row) => rowScheduledDateMatchesScope(row, scope));
  const classListEndpoint = buildClassListEndpoint(scope.app_show_idv2, scope.customer_id || CUSTOMER_ID);
  const classListEnrichment = {
    skipped: true,
    reason: "no_rows_needing_class_id",
    endpoint: classListEndpoint,
    catalog_rows: 0,
    usable_rows: 0,
    enriched_rows: 0,
    conflicts: [],
    error: null,
  };
  const needsClassIdEnrichment = scopedRowsBase.some((row) =>
    isBlank(row?.fields?.class_id) && numOrNull(row?.fields?.class_number) !== null
  );
  if (needsClassIdEnrichment && classListEndpoint) {
    try {
      const classListPayload = await fetchClassListJson(classListEndpoint, {
        automation_key: `schedules_dailyv2|classes_list|${scope.app_show_idv2}`,
        automation_name: "schedules_dailyv2_classes_list",
        source: "classes_list",
        app_show_id: scope.app_show_idv2,
        app_sql_date: scope.app_sql_datev2,
        run_id: runId,
        last_run: dateOnly,
      });
      const catalog = buildClassIdByNumberFromClassesPayload(classListPayload);
      const overlay = applyClassListIdEnrichment(scopedRowsBase, catalog.byNumber);
      scopedRowsBase = overlay.rows;
      classListEnrichment.skipped = false;
      classListEnrichment.reason = "exact_class_number_match";
      classListEnrichment.catalog_rows = catalog.catalog_rows;
      classListEnrichment.usable_rows = catalog.usable_rows;
      classListEnrichment.enriched_rows = overlay.enriched;
      classListEnrichment.conflicts = catalog.conflicts.slice(0, 10);
    } catch (error) {
      classListEnrichment.skipped = true;
      classListEnrichment.reason = "classes_list_fetch_failed";
      classListEnrichment.error = String(error?.message || error).slice(0, 500);
    }
  }
  const scheduleHtmlTimeOverlay = applyScheduleHtmlTimeOverlay(
    scopedRowsBase,
    scope.app_show_idv2,
    scope.app_sql_datev2
  );
  let scopedRows = scheduleHtmlTimeOverlay.rows;
  let groupsLiveRows = [];
  let groupsLiveMatchedRows = 0;
  let groupsLiveError = null;
  try {
    groupsLiveRows = await fetchGroupsLiveRows(scope.app_show_idv2, new Set([scope.app_sql_datev2]));
    const groupsLiveOverlay = applyGroupsLiveFallback(scopedRows, buildGroupsLiveMap(groupsLiveRows));
    scopedRows = groupsLiveOverlay.rows;
    groupsLiveMatchedRows = groupsLiveOverlay.matched;
  } catch (error) {
    groupsLiveError = String(error?.message || error);
  }
  const showRecordId = await fetchShowRecordId(scope.app_show_idv2).catch(() => null);
  const existingRows = await fetchExistingRowsForShow(scope.app_show_idv2);
  const heartbeatViewRows = await fetchHeartbeatViewRows().catch(() => []);
  const heartbeatViewIdSet = new Set(heartbeatViewRows.map((row) => row.id));

  const zeroRowScheduleFailure = chosen.rows.length === 0 &&
    existingRows.length > 0 &&
    (
      !datedResult.rows.length ||
      !emptyResult.ok ||
      !!emptyResult.error
    );

  if (zeroRowScheduleFailure) {
    throw new Error(
      `Refusing destructive zero-row schedule sync for show ${scope.app_show_idv2} on ${scope.app_sql_datev2}; ` +
      `dated_rows=${datedResult.rows.length} empty_rows=${emptyResult.rows.length} empty_ok=${emptyResult.ok} ` +
      `empty_error=${emptyResult.error || 'none'} existing_rows=${existingRows.length}`
    );
  }

  const groupedExisting = new Map();
  for (const row of existingRows) {
    for (const key of scheduleRowCandidateKeysFromFields(row?.fields || {})) {
      if (!groupedExisting.has(key)) groupedExisting.set(key, []);
      groupedExisting.get(key).push(row);
    }
  }

  const existingByKey = new Map();
  for (const [key, rows] of groupedExisting.entries()) {
    const winner = chooseExistingWinner(rows, heartbeatViewIdSet);
    if (winner) existingByKey.set(key, winner);
  }

  const createRecords = [];
  const updateRecords = [];
  const forwardScheduleWriteSummary = {
    enabled: PREFETCH_FORWARD_SCHEDULES,
    dates_seen: 0,
    rows_seen: 0,
    rows_written: 0,
    creates_planned: 0,
    updates_planned: 0,
    skipped_missing_schedule_tie_breaker: 0,
    skipped_missing_schedule_tie_breaker_samples: [],
  };
  const estimatedStartTimeGuard = {
    enabled: String(scope.mode || "").toUpperCase() === "NIGHT" && boolValue(scope.shifted_to_next_dayv2),
    valid_min: PRELIVE_ESTIMATED_START_TIME_MIN,
    valid_max: PRELIVE_ESTIMATED_START_TIME_MAX,
    manual_time_override_preserved: 0,
    preserved_existing: 0,
    omitted_suspicious: 0,
    samples: [],
  };
  const keepKeySet = new Set();
  const keepRecordIdSet = new Set();
  const actualScheduleShowDateByKey = new Map();
  const scheduleBaseKeyCounts = new Map();
  for (const row of scopedRows) {
    const seedFields = row?.fields || {};
    const rowBaseKey = buildScheduleKeyParts({
      sid: pickFirst(seedFields.show_id, scope.app_show_idv2),
      sqlDate: pickFirst(toIsoDateOnly(seedFields.schedule_show_datev2), seedFields.app_sql_datev2, scope.app_sql_datev2),
      ringNumber: seedFields.ring_number,
      classNumber: seedFields.class_number,
    }).scheduleKey || normalizeKey(row.key);
    if (!rowBaseKey) continue;
    scheduleBaseKeyCounts.set(rowBaseKey, (scheduleBaseKeyCounts.get(rowBaseKey) || 0) + 1);
  }
  const skippedMissingScheduleTieBreaker = [];

  for (const row of scopedRows) {
    const seedFields = row?.fields || {};
    const baseScheduleKey = buildScheduleKeyParts({
      sid: pickFirst(seedFields.show_id, scope.app_show_idv2),
      sqlDate: pickFirst(toIsoDateOnly(seedFields.schedule_show_datev2), seedFields.app_sql_datev2, scope.app_sql_datev2),
      ringNumber: seedFields.ring_number,
      classNumber: seedFields.class_number,
    }).scheduleKey || normalizeKey(row.key);
    const needsTieBreaker = baseScheduleKey && (scheduleBaseKeyCounts.get(baseScheduleKey) || 0) > 1;
    const scheduleTieBreaker = needsTieBreaker ? keyPart(seedFields.class_group_sequence) : "";
    if (needsTieBreaker && !scheduleTieBreaker) {
      skippedMissingScheduleTieBreaker.push({
        base_key: baseScheduleKey,
        class_number: seedFields.class_number ?? null,
        ring_number: seedFields.ring_number ?? null,
        class_group_id: seedFields.class_group_id ?? null,
      });
      continue;
    }
    row.schedule_key_tie_breaker = scheduleTieBreaker;
    const rowKeyParts = buildScheduleKeyParts({
      sid: pickFirst(seedFields.show_id, scope.app_show_idv2),
      sqlDate: pickFirst(toIsoDateOnly(seedFields.schedule_show_datev2), seedFields.app_sql_datev2, scope.app_sql_datev2),
      ringNumber: seedFields.ring_number,
      classNumber: seedFields.class_number,
      tieBreaker: scheduleTieBreaker,
    });
    const legacyKey = normalizeKey(row.key);
    const key = normalizeKey(rowKeyParts.scheduleKey || legacyKey);
    if (!key) continue;
    keepKeySet.add(key);
    if (legacyKey) keepKeySet.add(legacyKey);
    const actualScheduleShowDate = resolveActualScheduleShowDate(row);
    actualScheduleShowDateByKey.set(key, actualScheduleShowDate);
    if (legacyKey) actualScheduleShowDateByKey.set(legacyKey, actualScheduleShowDate);

    const existing = existingByKey.get(key) || existingByKey.get(legacyKey);
    if (existing) keepRecordIdSet.add(existing.id);
    const fields = buildCurrentFields(
      row,
      scope,
      heartbeatRecord.id,
      showRecordId,
      nowIso,
      dateOnly,
      existing ? "existing" : "new",
      currentScopeStatus,
      watchScheduleFieldMeta
    );
    let guardResult = null;
    if (hasManualTimeOverride(existing?.fields || {})) {
      delete fields.estimated_start_time;
      estimatedStartTimeGuard.manual_time_override_preserved += 1;
      guardResult = {
        action: "manual_time_override_preserved",
        candidate: strOrNull(row?.fields?.estimated_start_time) || null,
        existing: strOrNull(existing?.fields?.estimated_start_time) || null,
      };
    } else {
      guardResult = applyPreliveEstimatedStartTimeGuard(fields, existing, scope);
    }
    if (guardResult) {
      if (guardResult.action === "preserved_existing") estimatedStartTimeGuard.preserved_existing += 1;
      if (guardResult.action === "omitted_suspicious") estimatedStartTimeGuard.omitted_suspicious += 1;
      if (estimatedStartTimeGuard.samples.length < 10) {
        estimatedStartTimeGuard.samples.push({
          key,
          class_number: fields.class_number ?? row?.fields?.class_number ?? null,
          class_group_id: fields.class_group_id ?? row?.fields?.class_group_id ?? null,
          ...guardResult,
        });
      }
    }

    if (existing) {
      updateRecords.push({ id: existing.id, fields });
    } else {
      createRecords.push({ fields });
    }
  }

  for (const forwardItem of schedulePayloadCache.forwardPayloads || []) {
    const forwardDate = toIsoDateOnly(forwardItem?.date);
    if (!forwardDate || !forwardItem?.payload) continue;
    const forwardScope = scopeForScheduleDate(scope, forwardDate);
    const forwardResult = normalizeSchedulePayload(forwardItem.payload, {
      scope: forwardScope,
      source: "forward_schedule_cache",
      generatedAt: nowIso,
      generatedDate: dateOnly,
    });
    let forwardRows = forwardResult.rows.filter((row) => rowScheduledDateMatchesScope(row, forwardScope));
    forwardScheduleWriteSummary.dates_seen += 1;
    forwardScheduleWriteSummary.rows_seen += forwardRows.length;

    const forwardBaseKeyCounts = new Map();
    for (const row of forwardRows) {
      const seedFields = row?.fields || {};
      const rowBaseKey = buildScheduleKeyParts({
        sid: pickFirst(seedFields.show_id, forwardScope.app_show_idv2),
        sqlDate: pickFirst(toIsoDateOnly(seedFields.schedule_show_datev2), seedFields.app_sql_datev2, forwardScope.app_sql_datev2),
        ringNumber: seedFields.ring_number,
        classNumber: seedFields.class_number,
      }).scheduleKey || normalizeKey(row.key);
      if (!rowBaseKey) continue;
      forwardBaseKeyCounts.set(rowBaseKey, (forwardBaseKeyCounts.get(rowBaseKey) || 0) + 1);
    }

    for (const row of forwardRows) {
      const seedFields = row?.fields || {};
      const baseScheduleKey = buildScheduleKeyParts({
        sid: pickFirst(seedFields.show_id, forwardScope.app_show_idv2),
        sqlDate: pickFirst(toIsoDateOnly(seedFields.schedule_show_datev2), seedFields.app_sql_datev2, forwardScope.app_sql_datev2),
        ringNumber: seedFields.ring_number,
        classNumber: seedFields.class_number,
      }).scheduleKey || normalizeKey(row.key);
      const needsTieBreaker = baseScheduleKey && (forwardBaseKeyCounts.get(baseScheduleKey) || 0) > 1;
      const scheduleTieBreaker = needsTieBreaker ? keyPart(seedFields.class_group_sequence) : "";
      if (needsTieBreaker && !scheduleTieBreaker) {
        forwardScheduleWriteSummary.skipped_missing_schedule_tie_breaker += 1;
        if (forwardScheduleWriteSummary.skipped_missing_schedule_tie_breaker_samples.length < 10) {
          forwardScheduleWriteSummary.skipped_missing_schedule_tie_breaker_samples.push({
            date: forwardDate,
            base_key: baseScheduleKey,
            class_number: seedFields.class_number ?? null,
            ring_number: seedFields.ring_number ?? null,
            class_group_id: seedFields.class_group_id ?? null,
          });
        }
        continue;
      }

      row.schedule_key_tie_breaker = scheduleTieBreaker;
      const rowKeyParts = buildScheduleKeyParts({
        sid: pickFirst(seedFields.show_id, forwardScope.app_show_idv2),
        sqlDate: pickFirst(toIsoDateOnly(seedFields.schedule_show_datev2), seedFields.app_sql_datev2, forwardScope.app_sql_datev2),
        ringNumber: seedFields.ring_number,
        classNumber: seedFields.class_number,
        tieBreaker: scheduleTieBreaker,
      });
      const legacyKey = normalizeKey(row.key);
      const key = normalizeKey(rowKeyParts.scheduleKey || legacyKey);
      if (!key) continue;

      const existing = existingByKey.get(key) || existingByKey.get(legacyKey);
      const fields = buildCurrentFields(
        row,
        forwardScope,
        null,
        showRecordId,
        nowIso,
        dateOnly,
        existing ? "prefetch_existing" : "prefetch_new",
        null,
        watchScheduleFieldMeta,
        { isCurrentScope: false }
      );
      if (existing) {
        updateRecords.push({ id: existing.id, fields });
        forwardScheduleWriteSummary.updates_planned += 1;
      } else {
        createRecords.push({ fields });
        forwardScheduleWriteSummary.creates_planned += 1;
      }
      forwardScheduleWriteSummary.rows_written += 1;
    }
  }

  const dropUpdates = [];
  let droppedForScheduleShowDateMismatch = 0;
  for (const row of existingRows) {
    const key = scheduleRowKeyFromFields(row?.fields || {});
    if (!key) continue;
    if (!existingScheduleRowMatchesScope(row, scope)) continue;
    if (boolValue(row?.fields?.inactive) || firstValue(row?.fields?.dropped_at)) continue;

    const hasCurrentMarkers =
      heartbeatViewIdSet.has(row.id) ||
      boolValue(row?.fields?.is_current_scope) ||
      !!firstValue(row?.fields?.heartbeat);
    if (!hasCurrentMarkers) continue;

    if (keepRecordIdSet.has(row.id)) continue;

    if (keepKeySet.has(key)) {
      const actualScheduleShowDate = actualScheduleShowDateByKey.get(key);
      const existingScheduleShowDate = resolveExistingScheduleShowDate(row);
      if (!actualScheduleShowDate || existingScheduleShowDate === actualScheduleShowDate) continue;
      droppedForScheduleShowDateMismatch += 1;
    }

    dropUpdates.push({
      id: row.id,
      fields: buildDroppedFields(scope, nowIso, dateOnly, droppedScopeStatus, watchScheduleFieldMeta),
    });
  }

  const summary = {
    ok: true,
    dry_run: DRY_RUN,
    scope,
    chosen_source: chosen.source,
    heartbeat_patch_fields: heartbeatChangedFields,
    row_count: scopedRows.length,
    filtered_out_scheduled_date_mismatch: chosen.rows.length - scopedRows.length,
    skipped_missing_schedule_tie_breaker: skippedMissingScheduleTieBreaker.length,
    skipped_missing_schedule_tie_breaker_samples: skippedMissingScheduleTieBreaker.slice(0, 10),
    dropped_due_to_schedule_show_date_mismatch: droppedForScheduleShowDateMismatch,
    creates_planned: createRecords.length,
    updates_planned: updateRecords.length,
    drops_planned: dropUpdates.length,
    existing_show_rows: existingRows.length,
    heartbeat_view_rows: heartbeatViewRows.length,
    show_record_bound: !!showRecordId,
    fetches: {
      dated_schedule: {
        url: datedUrl,
        source: datedResult.source,
        fetch_error: datedResult.fetch_error || null,
        fallback_file: datedResult.fallback_file || null,
        fallback_body_length: datedResult.fallback_body_length || null,
        rows: datedResult.rows.length,
        schedule_show_datev2: datedResult.schedule_show_datev2 || null,
      },
      schedule_payload_cache: schedulePayloadCache,
      empty_ping_schedule: {
        url: emptyUrl,
        ok: emptyResult.ok,
        error: emptyResult.error || null,
        rows: emptyResult.rows.length,
        schedule_show_datev2: emptyResult.schedule_show_datev2 || null,
      },
      classes_endpoint: {
        skipped: classEndpointEnrichment.skipped,
        reason: classEndpointEnrichment.reason,
        enriched_rows: classEndpointEnrichment.enriched_rows,
        failures: classEndpointEnrichment.failures,
      },
      classes_list_enrichment: classListEnrichment,
      schedule_html_time_overlay: scheduleHtmlTimeOverlay.summary,
      groups_live_fallback: {
        table: TABLE_GROUPS_LIVE,
        rows: groupsLiveRows.length,
        matched_rows: groupsLiveMatchedRows,
        error: groupsLiveError,
      },
      forward_schedule_writes: forwardScheduleWriteSummary,
    },
    writes: {
      created: 0,
      updated: 0,
      dropped: 0,
      create_failures: [],
      update_failures: [],
      drop_failures: [],
    },
    estimated_start_time_guard: estimatedStartTimeGuard,
    active_groups: {
      table: TABLE_ACTIVE_GROUPS,
      created_planned: 0,
      updated_planned: 0,
      inactivated_planned: 0,
      writes: { created: 0, updated: 0, inactivated: 0, create_failures: [], update_failures: [], inactivate_failures: [] },
      skipped: true,
      reason: "active_tables_deprecated",
    },
  };

  const activeGroupRows = [];
  summary.active_groups.created_planned = 0;

  if (DRY_RUN) {
    return summary;
  }

  const createResult = await airtableCreateRecords(TABLE_WATCH_SCHEDULE, createRecords);
  const updateResult = await airtablePatchRecords(TABLE_WATCH_SCHEDULE, updateRecords);
  const dropResult = await airtablePatchRecords(TABLE_WATCH_SCHEDULE, dropUpdates);

  summary.writes.created = createResult.okRows;
  summary.writes.updated = updateResult.okRows;
  summary.writes.dropped = dropResult.okRows;
  summary.writes.create_failures = createResult.failedRows;
  summary.writes.update_failures = updateResult.failedRows;
  summary.writes.drop_failures = dropResult.failedRows;
  return summary;
}

async function runDaily() {
  requireEnv("AIRTABLE_TOKEN", AIRTABLE_TOKEN);
  requireEnv("AIRTABLE_BASE_ID", AIRTABLE_BASE_ID);

  const nowIso = new Date().toISOString();
  const dateOnly = nowIso.slice(0, 10);
  const runId = buildNumericRunId(nowIso);

  const heartbeatRecord = await fetchLatestHeartbeat();
  const fallbackHeartbeatContext = buildBaseHeartbeatContext(heartbeatRecord);
  const scopeStatusChoices = await fetchWatchScheduleScopeStatusChoices().catch(() => new Set());
  const currentScopeStatus = scopeStatusChoices.has("current") ? "current" : null;
  const droppedScopeStatus = scopeStatusChoices.has("dropped") ? "dropped" : null;
  const heartbeatFieldSet = await fetchHeartbeatFieldSet().catch(() => new Set());
  const watchScheduleFieldMeta = await fetchTableFieldMeta(TABLE_WATCH_SCHEDULE);
  const activeGroupsFieldSet = SYNC_ACTIVE_GROUPS_FROM_SCHEDULE
    ? await fetchTableFieldSet(TABLE_ACTIVE_GROUPS).catch(() => new Set())
    : new Set();

  let showTargetRows = [];
  let showTargetFetchError = null;
  try {
    showTargetRows = await fetchShowTargetRows();
  } catch (error) {
    showTargetFetchError = String(error?.message || error);
  }

  const skippedShowTargets = [];
  const showTargetContexts = [];
  for (const row of showTargetRows) {
    const targetInfo = showHeartbeatTargetDate(row.fields, new Date(nowIso));
    if (targetInfo.skipped) {
      skippedShowTargets.push({
        record_id: row.id,
        show_id: numOrNull(row.fields?.show_id),
        ...targetInfo,
      });
      continue;
    }
    showTargetContexts.push(buildShowTargetBaseContext(fallbackHeartbeatContext, row, targetInfo));
  }

  const contexts = showTargetContexts.length ? showTargetContexts : [fallbackHeartbeatContext];
  const patchHeartbeat = contexts.length === 1;
  const results = [];
  for (const baseHeartbeatContext of contexts) {
    results.push(await runScheduleForBaseContext({
      heartbeatRecord,
      baseHeartbeatContext,
      nowIso,
      dateOnly,
      runId,
      currentScopeStatus,
      droppedScopeStatus,
      heartbeatFieldSet,
      watchScheduleFieldMeta,
      activeGroupsFieldSet,
      patchHeartbeat,
    }));
  }

  if (results.length === 1) {
    return {
      ...results[0],
      show_target_scope: {
        table: TABLE_SHOW_TARGET,
        view: VIEW_SHOW_TARGET,
        rows: showTargetRows.length,
        valid_rows: showTargetContexts.length,
        skipped_rows: skippedShowTargets,
        fetch_error: showTargetFetchError,
        source: showTargetContexts.length ? "show_heartbeat" : "latest_heartbeat_fallback",
      },
    };
  }

  return {
    ok: results.every((result) => result.ok),
    dry_run: DRY_RUN,
    source: "show_heartbeat_multi",
    show_target_scope: {
      table: TABLE_SHOW_TARGET,
      view: VIEW_SHOW_TARGET,
      rows: showTargetRows.length,
      valid_rows: showTargetContexts.length,
      skipped_rows: skippedShowTargets,
      fetch_error: showTargetFetchError,
    },
    results,
  };
}

async function main() {
  const result = await runDaily();
  process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
}

if (require.main === module) {
  main()
    .then(() => {
      process.exit(0);
    })
    .catch((error) => {
      process.stdout.write(`${JSON.stringify({
        ok: false,
        error: String(error?.message || error),
        dry_run: DRY_RUN,
      }, null, 2)}\n`);
      process.exit(1);
    });
}

module.exports = {
  applyGroupsLiveFallback,
  applyClassListIdEnrichment,
  applyScheduleHtmlTimeOverlay,
  buildClassIdByNumberFromClassesPayload,
  buildCurrentFields,
  buildClassListEndpoint,
  applyPreliveEstimatedStartTimeGuard,
  hasManualTimeOverride,
  isSuspiciousPreliveEstimatedStartTime,
  showHeartbeatTargetDate,
  normalizeHtmlScheduleTimeText,
  parseScheduleHtmlTimeOverlay,
  forwardScheduleDates,
  resolveHeartbeatScope,
  resolveHeartbeatScopeFromCurrentHeartbeat,
  runDaily,
  scheduleHtmlFallbackDirs,
  scheduleRowKeyFromFields,
  scopeForScheduleDate,
};
