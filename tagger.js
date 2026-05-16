// tagger.js (FULL DROP)
// heartbeat modes: DAY / NIGHT / OVERNIGHT / IDLE / OFF
// - raw clock always comes from endpoint, or system time with a bounded last-known show fallback
// - mode logic is based only on app_time as provided by the endpoint/system fallback
// - mode is written for cadence/orchestration only; it does not choose app_sql_date
// - app_sql_date comes from focused show controls, explicit overrides, or raw sql_date/default metadata
// - heartbeat owns the final app_sql_date, app_dow_raw, shifted_to_next_day, and app-date provenance fields
// - shows table: match by show_id/app_show_id or create if missing
// - shows table heartbeat link is overwritten to latest heartbeat only
// - new_app_show_id is checked only when a new show row is created

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
  normalizeHeartbeatMode,
  isHeartbeatControlMode,
  resolveHeartbeatCadenceSeconds,
} = require("./lib/heartbeat_mode");
const {
  boolValue,
  computeDefaultShowDateGuard,
  decideEffectiveMode,
  normalizeControlMode,
} = require("./lib/default_show_date_guard");
const {
  classifyWatchScheduleHeartbeatRelink,
} = require("./lib/watch_schedule_scope_relink");
const {
  classifyWatchTripsHeartbeatRelink,
} = require("./lib/watch_trips_scope_relink");

const AIRTABLE_TOKEN   = process.env.AIRTABLE_TOKEN || "";
const AIRTABLE_BASE_ID = process.env.AIRTABLE_BASE_ID || "";
const CUSTOMER_ID      = Number(process.env.CUSTOMER_ID || "15");
const SGL_BASE_URL = String(
  process.env.SGL_DATA_BASE_URL ||
  process.env.SGL_DIRECT_BASE_URL ||
  process.env.SGL_API_BASE_URL ||
  process.env.BASE_URL ||
  "https://sglapi.wellingtoninternational.com"
).trim().replace(/\/+$/, "");

if (!AIRTABLE_TOKEN) throw new Error("Missing AIRTABLE_TOKEN");
if (!AIRTABLE_BASE_ID) throw new Error("Missing AIRTABLE_BASE_ID");

const TABLE_HEARTBEAT      = process.env.TABLE_HEARTBEAT || "heartbeat";
const TABLE_SHOW_TARGET    = process.env.TABLE_SHOW_TARGET || process.env.TABLE_SHOW || "show";
const TABLE_SHOWS          = process.env.TABLE_SHOWS || "shows";
const TABLE_WATCH_SCHEDULE = process.env.TABLE_WATCH_SCHEDULE || "watch_schedule";
const TABLE_WATCH_TRIPS    = process.env.TABLE_WATCH_TRIPS || "watch_trips";
const TABLE_SCHEDULER      = process.env.TABLE_SCHEDULER || "scheduler";
const TABLE_ACTIVE_TENANTS = process.env.TABLE_ACTIVE_TENANTS || "active_tenants";
const TABLE_ACTIVE_ALERTS  = process.env.TABLE_ACTIVE_ALERTS || "active_alerts";
const TABLE_WATCH_RINGS    = process.env.TABLE_WATCH_RINGS || "watch_rings";
const TABLE_PUBLISH_QUEUE  = process.env.TABLE_PUBLISH_QUEUE || process.env.PUBLISH_QUEUE_TABLE || "publish_queue";

const VIEW_HEARTBEAT = process.env.VIEW_HEARTBEAT || "heartbeat";
const VIEW_SHOW_TARGET = process.env.VIEW_SHOW_TARGET || "heartbeat";

const FIELD_LINK_HEARTBEAT     = process.env.FIELD_LINK_HEARTBEAT || "heartbeat";
const FIELD_SHOW_ID            = process.env.FIELD_SHOW_ID || "show_id";
const FIELD_NEW_APP_SHOW_ID    = process.env.FIELD_NEW_APP_SHOW_ID || "new_app_show_id";
const FIELD_NEW_APP_SHOW_ID_AT = process.env.FIELD_NEW_APP_SHOW_ID_AT || "new_app_show_id_at";
const FIELD_CUSTOMER_ID        = process.env.FIELD_CUSTOMER_ID || "customer_id";
const FIELD_CUSTOMER_ID_OVERRIDE = process.env.FIELD_CUSTOMER_ID_OVERRIDE || "customer_id_override";
const FIELD_SHOW_TARGET_HEARTBEAT = process.env.FIELD_SHOW_TARGET_HEARTBEAT || "heartbeat";

const FIELD_MODE             = process.env.FIELD_MODE || "mode";
const FIELD_EPOCH            = process.env.FIELD_EPOCH || "epoch";
const FIELD_HB_DURATION      = process.env.FIELD_HB_DURATION || "hb_duration";
const FIELD_INTERVAL         = process.env.FIELD_INTERVAL || "interval";
const FIELD_CADENCE          = process.env.FIELD_CADENCE || "cadence";
const FIELD_SET_INTERVALS    = process.env.FIELD_SET_INTERVALS || "set_intervals";
const FIELD_HB_AT            = process.env.FIELD_HB_AT || "hb_at";

const FIELD_APP_SHOW_ID      = process.env.FIELD_APP_SHOW_ID || "app_show_id";
const FIELD_APP_SQL_DATE     = process.env.FIELD_APP_SQL_DATE || "app_sql_date";
const FIELD_APP_DOW_RAW      = process.env.FIELD_APP_DOW_RAW || "app_dow_raw";
const FIELD_DOW_RAW          = process.env.FIELD_DOW_RAW || "dow_raw";
const FIELD_SHIFTED_NEXT_DAY = process.env.FIELD_SHIFTED_NEXT_DAY || "shifted_to_next_day";
const FIELD_SET_TO_DEFAULT_APP_SQL_DATE = process.env.FIELD_SET_TO_DEFAULT_APP_SQL_DATE || "set_to_default_app_sql_date";
const FIELD_DEFAULT_APP_SQL_DATE_IS = process.env.FIELD_DEFAULT_APP_SQL_DATE_IS || "default_app_sql_date_is";
const FIELD_SHOW_APP_SQL_START_DATE = process.env.FIELD_SHOW_APP_SQL_START_DATE || "show_app_sql_start_date";
const FIELD_SHOW_APP_SQL_END_DATE = process.env.FIELD_SHOW_APP_SQL_END_DATE || "show_app_sql_end_date";
const FIELD_SHOW_APP_NAME = process.env.FIELD_SHOW_APP_NAME || "show_app_name";
const FIELD_APP_SQL_DATE_SOURCE = process.env.FIELD_APP_SQL_DATE_SOURCE || "app_sql_date_source";
const FIELD_CLOCK_MODE = process.env.FIELD_CLOCK_MODE || "clock_mode";
const FIELD_MODE_SOURCE = process.env.FIELD_MODE_SOURCE || "mode_source";
const FIELD_MODE_REASON = process.env.FIELD_MODE_REASON || "mode_reason";
const FIELD_DEFAULT_SHOW_DATE_STATUS = process.env.FIELD_DEFAULT_SHOW_DATE_STATUS || "default_show_date_status";
const FIELD_DEFAULT_SHOW_DATE_REASON = process.env.FIELD_DEFAULT_SHOW_DATE_REASON || "default_show_date_reason";
const FIELD_ARCHIVE = process.env.FIELD_ARCHIVE || "archive";
const FIELD_INACTIVE = process.env.FIELD_INACTIVE || "inactive";
const FIELD_SCOPE_STATUS = process.env.FIELD_SCOPE_STATUS || "scope_status";
const FIELD_MODE_CONTROL = process.env.FIELD_MODE_CONTROL || "mode_control";
const FIELD_MODE_CONTROL_REASON = process.env.FIELD_MODE_CONTROL_REASON || "mode_control_reason";
const FIELD_IS_DEFAULT_SHOW_MANUAL_OVERRIDE = process.env.FIELD_IS_DEFAULT_SHOW_MANUAL_OVERRIDE || "is_default_show_manual_override";
const FIELD_SHOW_NAME_BASE = process.env.FIELD_SHOW_NAME_BASE || "show_name";
const FIELD_SHOW_START_DATE_BASE = process.env.FIELD_SHOW_START_DATE_BASE || "start_date";
const FIELD_SHOW_END_DATE_BASE = process.env.FIELD_SHOW_END_DATE_BASE || "end_date";
const FIELD_SHOW_FOCUS_DAY = process.env.FIELD_SHOW_FOCUS_DAY || "focus_day";

const HEARTBEAT_ID_FIELD   = process.env.HEARTBEAT_ID_FIELD || "heartbeat_id";
const HEARTBEAT_SHOW_ID    = process.env.HEARTBEAT_SHOW_ID || "show_id";
const HEARTBEAT_SHOW_DATE  = process.env.HEARTBEAT_SHOW_DATE || "show_date";
const HEARTBEAT_SQL_DATE   = process.env.HEARTBEAT_SQL_DATE || "sql_date";
const HEARTBEAT_TIME       = process.env.HEARTBEAT_TIME || "time";

const DAY_INTERVAL_MIN       = Number(process.env.DAY_INTERVAL_MIN || "6");
const NIGHT_INTERVAL_MIN     = Number(process.env.NIGHT_INTERVAL_MIN || "120");
const OVERNIGHT_INTERVAL_MIN = Number(process.env.OVERNIGHT_INTERVAL_MIN || "99999");
const IDLE_INTERVAL_MIN      = Number(process.env.IDLE_INTERVAL_MIN || "30");
const OFF_INTERVAL_MIN       = Number(process.env.OFF_INTERVAL_MIN || "60");

const HTTP_TIMEOUT_MS   = Number(process.env.HTTP_TIMEOUT_MS || "20000");
const AT_RETRY_ATTEMPTS = Number(process.env.AT_RETRY_ATTEMPTS || "3");
const AT_RETRY_BASE_MS  = Number(process.env.AT_RETRY_BASE_MS || "400");
const AT_RETRY_MAX_MS   = Number(process.env.AT_RETRY_MAX_MS || "2000");
const LAST_KNOWN_CLOCK_MAX_AGE_MIN = Math.max(1, Number(process.env.LAST_KNOWN_CLOCK_MAX_AGE_MIN || "360") || 360);
const LAST_KNOWN_HEARTBEAT_LOOKBACK = Math.max(1, Number(process.env.LAST_KNOWN_HEARTBEAT_LOOKBACK || "100") || 100);
const FORCE_MODE        = String(process.env.FORCE_MODE || "").trim().toUpperCase();
const HOTPATCH_APP_SHOW_ID = strOrNull(process.env.HOTPATCH_APP_SHOW_ID);
const HOTPATCH_APP_SQL_DATE = toIsoDateOnly(process.env.HOTPATCH_APP_SQL_DATE);
const HEARTBEAT_TARGET_APP_SHOW_ID = strOrNull(process.env.HEARTBEAT_TARGET_APP_SHOW_ID || process.env.HEARTBEAT_HOTPATCH_APP_SHOW_ID);
const HEARTBEAT_TARGET_SQL_DATES = parseSqlDateSet(process.env.HEARTBEAT_TARGET_SQL_DATES || process.env.HEARTBEAT_HOTPATCH_APP_SQL_DATES);
const HEARTBEAT_TARGET_CUSTOMER_ID = strOrNull(process.env.HEARTBEAT_TARGET_CUSTOMER_ID || process.env.HEARTBEAT_HOTPATCH_CUSTOMER_ID);
const DRY_RUN           = String(process.env.DRY_RUN || "0") === "1";
const HB_TZ             = process.env.HB_TIMEZONE || "America/New_York";
const DEFAULT_TAGGER_STATE_PATH = "C:\\actions-runner\\ringstatus\\tagger_runtime_state.json";
const TAGGER_STATE_FILE_ENV = String(process.env.TAGGER_STATE_FILE || "").trim();
const TAGGER_STATE_PATH = TAGGER_STATE_FILE_ENV
  ? (path.isAbsolute(TAGGER_STATE_FILE_ENV)
      ? TAGGER_STATE_FILE_ENV
      : path.resolve(__dirname, TAGGER_STATE_FILE_ENV))
  : DEFAULT_TAGGER_STATE_PATH;

const LOG_ACCEPTED_ENDPOINT = String(process.env.LOG_ACCEPTED_ENDPOINT || "1") === "1";
const LOG_TRANSITIONS       = String(process.env.LOG_TRANSITIONS || "1") === "1";
const LOG_SHOWS_SYNC        = String(process.env.LOG_SHOWS_SYNC || "1") === "1";
const LOG_RELINK_SUMMARY    = String(process.env.LOG_RELINK_SUMMARY || "0") === "1";

const sleep = (ms) => new Promise(r => setTimeout(r, ms));

function logEvent(level, event, data = {}) {
  const payload = {
    level,
    event,
    ts: new Date().toISOString(),
    dry_run: DRY_RUN,
    ...data
  };
  try {
    console.log(JSON.stringify(payload));
  } catch {
    console.log(`[${level}] ${event}`);
  }
}

const logInfo = (event, data) => logEvent("info", event, data);
const logWarn = (event, data) => logEvent("warn", event, data);
const logError = (event, data) => logEvent("error", event, data);

function hasValue(v) {
  return !(v === null || v === undefined || String(v).trim() === "");
}

function isBlank(v) {
  return !hasValue(v) ||
    String(v).trim().toLowerCase() === "null" ||
    String(v).trim().toLowerCase() === "nan";
}

function strOrNull(v) {
  return isBlank(v) ? null : String(v).trim();
}

function airtableValueName(v) {
  if (Array.isArray(v)) return v.length ? airtableValueName(v[0]) : null;
  if (v && typeof v === "object" && "name" in v) return v.name;
  return v;
}

function toFiniteNumber(v) {
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
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

function loadRuntimeState() {
  try {
    if (!fs.existsSync(TAGGER_STATE_PATH)) return {};
    const raw = fs.readFileSync(TAGGER_STATE_PATH, "utf8");
    const json = JSON.parse(raw);
    return json && typeof json === "object" ? json : {};
  } catch (e) {
    logWarn("runtime_state_read_failed", {
      state_file: TAGGER_STATE_PATH,
      error_message: String(e?.message || e).slice(0, 240)
    });
    return {};
  }
}

function saveRuntimeState(nextState) {
  try {
    fs.mkdirSync(path.dirname(TAGGER_STATE_PATH), { recursive: true });
    fs.writeFileSync(TAGGER_STATE_PATH, `${JSON.stringify(nextState, null, 2)}\n`, "utf8");
    return true;
  } catch (e) {
    logWarn("runtime_state_write_failed", {
      state_file: TAGGER_STATE_PATH,
      error_message: String(e?.message || e).slice(0, 240)
    });
    return false;
  }
}

function persistLastKnownClockRecord(record) {
  const showId = hasValue(record?.show_id) ? record.show_id : null;
  const originEpoch = toFiniteNumber(record?.origin_epoch);

  if (!hasValue(showId) || !Number.isFinite(originEpoch)) return false;

  const state = loadRuntimeState();
  const nextState = {
    ...state,
    lastKnownClock: {
      version: 1,
      customer_id: CUSTOMER_ID,
      saved_at: new Date().toISOString(),
      ...record,
      show_id: showId,
      origin_epoch: originEpoch,
    }
  };

  return saveRuntimeState(nextState);
}

function persistLastKnownClock(clock, source, extra = {}) {
  return persistLastKnownClockRecord({
    source,
    origin_epoch: toFiniteNumber(clock?.nowEpoch),
    show_id: hasValue(clock?.showId) ? clock.showId : null,
    show_date: hasValue(clock?.showDate) ? clock.showDate : null,
    sql_date: hasValue(clock?.sqlDate) ? clock.sqlDate : null,
    time: hasValue(clock?.time) ? clock.time : null,
    recorded_at: hasValue(clock?.iso) ? clock.iso : null,
    ...extra,
  });
}

function buildClockFromCachedShow(systemClock, cached, source) {
  const showId = hasValue(cached?.show_id) ? cached.show_id : null;
  const originEpoch = toFiniteNumber(cached?.origin_epoch);

  if (!hasValue(showId) || !Number.isFinite(originEpoch)) return null;

  const ageSec = systemClock.nowEpoch - originEpoch;
  const maxAgeSec = LAST_KNOWN_CLOCK_MAX_AGE_MIN * 60;
  if (!Number.isFinite(ageSec) || ageSec < 0 || ageSec > maxAgeSec) return null;

  return {
    clock: {
      ...systemClock,
      source,
      showId,
      showDate: hasValue(cached?.show_date) ? cached.show_date : null,
      last_known_source: cached?.source || source,
      last_known_age_sec: ageSec,
      last_known_origin_epoch: originEpoch,
      last_known_sql_date: hasValue(cached?.sql_date) ? cached.sql_date : null,
      last_known_recorded_at: hasValue(cached?.recorded_at) ? cached.recorded_at : null,
      last_known_heartbeat_record_id: hasValue(cached?.heartbeat_record_id) ? cached.heartbeat_record_id : null,
    },
    meta: {
      ageSec,
      cachedSource: cached?.source || source,
      sqlDate: hasValue(cached?.sql_date) ? cached.sql_date : null,
      heartbeatRecordId: hasValue(cached?.heartbeat_record_id) ? cached.heartbeat_record_id : null,
    }
  };
}

function getLastKnownClockFromState(systemClock) {
  const state = loadRuntimeState();
  const cached = state?.lastKnownClock;

  if (!cached || typeof cached !== "object") {
    return { reason: "missing_state_cache" };
  }

  if (toFiniteNumber(cached.customer_id) !== CUSTOMER_ID) {
    return {
      reason: "customer_mismatch",
      customer_id: cached.customer_id ?? null
    };
  }

  const hit = buildClockFromCachedShow(systemClock, cached, "system_last_known_clock");
  if (!hit) {
    return { reason: "stale_or_invalid_state_cache" };
  }

  return { ...hit, reason: null };
}

async function bootstrapLastKnownClockFromHeartbeat(systemClock) {
  const rows = await airtableListSome({
    table: TABLE_HEARTBEAT,
    fields: [
      FIELD_EPOCH,
      HEARTBEAT_SHOW_ID,
      HEARTBEAT_SHOW_DATE,
      HEARTBEAT_SQL_DATE,
      HEARTBEAT_TIME,
      FIELD_HB_AT,
    ],
    maxRecords: LAST_KNOWN_HEARTBEAT_LOOKBACK,
    sortField: FIELD_EPOCH,
    sortDirection: "desc"
  });

  for (const row of rows) {
    const fields = row?.fields || {};
    const cached = {
      customer_id: CUSTOMER_ID,
      source: "heartbeat_bootstrap",
      origin_epoch: toFiniteNumber(fields?.[FIELD_EPOCH]),
      show_id: hasValue(fields?.[HEARTBEAT_SHOW_ID]) ? fields[HEARTBEAT_SHOW_ID] : null,
      show_date: hasValue(fields?.[HEARTBEAT_SHOW_DATE]) ? fields[HEARTBEAT_SHOW_DATE] : null,
      sql_date: hasValue(fields?.[HEARTBEAT_SQL_DATE]) ? fields[HEARTBEAT_SQL_DATE] : null,
      time: hasValue(fields?.[HEARTBEAT_TIME]) ? fields[HEARTBEAT_TIME] : null,
      recorded_at: hasValue(fields?.[FIELD_HB_AT]) ? fields[FIELD_HB_AT] : null,
      heartbeat_record_id: row.id,
    };

    const hit = buildClockFromCachedShow(systemClock, cached, "system_last_known_heartbeat");
    if (!hit) continue;

    persistLastKnownClockRecord(cached);

    return {
      ...hit,
      reason: null
    };
  }

  return { reason: "no_recent_heartbeat_with_show_id" };
}

async function recoverClockFromLastKnown(systemClock, fallbackEvent) {
  const stateHit = getLastKnownClockFromState(systemClock);
  if (stateHit.clock) {
    logWarn("last_known_clock_reused", {
      fallback_event: fallbackEvent,
      fallback_source: "state_cache",
      show_id: stateHit.clock.showId,
      show_date: stateHit.clock.showDate,
      cache_source: stateHit.meta.cachedSource,
      cache_age_sec: stateHit.meta.ageSec,
      cache_sql_date: stateHit.meta.sqlDate,
      heartbeat_record_id: stateHit.meta.heartbeatRecordId,
    });
    return stateHit.clock;
  }

  try {
    const heartbeatHit = await bootstrapLastKnownClockFromHeartbeat(systemClock);
    if (heartbeatHit.clock) {
      logWarn("last_known_clock_reused", {
        fallback_event: fallbackEvent,
        fallback_source: "heartbeat_bootstrap",
        show_id: heartbeatHit.clock.showId,
        show_date: heartbeatHit.clock.showDate,
        cache_source: heartbeatHit.meta.cachedSource,
        cache_age_sec: heartbeatHit.meta.ageSec,
        cache_sql_date: heartbeatHit.meta.sqlDate,
        heartbeat_record_id: heartbeatHit.meta.heartbeatRecordId,
      });
      return heartbeatHit.clock;
    }

    logWarn("last_known_clock_unavailable", {
      fallback_event: fallbackEvent,
      state_reason: stateHit.reason,
      heartbeat_reason: heartbeatHit.reason
    });
  } catch (e) {
    logWarn("last_known_clock_lookup_failed", {
      fallback_event: fallbackEvent,
      state_reason: stateHit.reason,
      error_message: String(e?.message || e).slice(0, 240)
    });
  }

  return null;
}

async function fallbackToBestKnownClock(systemClock, event, data) {
  logWarn(event, data);
  return (await recoverClockFromLastKnown(systemClock, event)) || systemClock;
}

function airtableUrl(tableName) {
  return `https://api.airtable.com/v0/${AIRTABLE_BASE_ID}/${encodeURIComponent(tableName)}`;
}

function headers() {
  return {
    Authorization: `Bearer ${AIRTABLE_TOKEN}`,
    "Content-Type": "application/json",
  };
}

async function fetchWithTimeout(url, opts = {}, timeoutMs = HTTP_TIMEOUT_MS) {
  const ac = new AbortController();
  const t = setTimeout(() => ac.abort(), timeoutMs);
  try {
    return await fetch(url, { ...opts, signal: ac.signal });
  } finally {
    clearTimeout(t);
  }
}

function isRetryableFetchError(e) {
  const name = String(e?.name || "");
  const code = String(e?.code || "");
  const msg  = String(e?.message || "");
  if (name === "AbortError") return true;
  if (code === "UND_ERR_CONNECT_TIMEOUT") return true;
  if (code === "UND_ERR_HEADERS_TIMEOUT") return true;
  if (code === "UND_ERR_BODY_TIMEOUT") return true;
  if (/timeout/i.test(msg)) return true;
  if (/fetch failed/i.test(msg)) return true;
  return false;
}

async function fetchWithRetry(url, opts = {}, retry = {}) {
  const attempts = Math.max(1, Number(retry.attempts ?? AT_RETRY_ATTEMPTS));
  const baseMs   = Math.max(0, Number(retry.baseMs ?? AT_RETRY_BASE_MS));
  const maxMs    = Math.max(250, Number(retry.maxMs ?? AT_RETRY_MAX_MS));

  let lastErr = null;

  for (let i = 1; i <= attempts; i++) {
    try {
      const res = await fetchWithTimeout(url, opts);

      if (res.status === 429 || (res.status >= 500 && res.status <= 599)) {
        if (i === attempts) return res;
        const waitMs = Math.min(maxMs, baseMs * i + Math.floor(Math.random() * 200));
        await sleep(waitMs);
        continue;
      }

      return res;
    } catch (e) {
      lastErr = e;
      if (!isRetryableFetchError(e) || i === attempts) throw e;
      const waitMs = Math.min(maxMs, baseMs * i + Math.floor(Math.random() * 250));
      await sleep(waitMs);
    }
  }

  throw lastErr || new Error("fetchWithRetry failed");
}

async function fetchJson(url) {
  const fetched = await fetchTextWithConfiguredTransport(url, async (endpoint) => {
    const response = await fetchWithRetry(endpoint, { method: "GET" });
    const text = await response.text();
    return { response, text, endpoint };
  });
  const res = fetched.response;
  const txt = fetched.text;
  const endpoint = fetched.endpoint || url;
  let json = {};
  try { json = JSON.parse(txt); } catch {}
  if (!res.ok) {
    throw new Error(`Fetch failed (${res.status}): ${txt.slice(0, 300)}`);
  }
  assertValidPayload({
    payload: json,
    text: txt,
    response: res,
    lane: "tagger",
    endpoint,
    expectedTopLevelKeys: ["show", "show_date", "showDate", "show_days_list"],
  });
  return json;
}

async function airtableListAll({ table, view, fields }) {
  const out = [];
  let offset = null;

  while (true) {
    const url = new URL(airtableUrl(table));
    if (view) url.searchParams.set("view", view);
    if (offset) url.searchParams.set("offset", offset);
    for (const f of fields || []) url.searchParams.append("fields[]", f);

    const res = await fetchWithRetry(url.toString(), {
      method: "GET",
      headers: headers(),
    });

    const txt = await res.text();
    let json = {};
    try { json = JSON.parse(txt); } catch {}

    if (!res.ok) {
      throw new Error(`Airtable list failed (${table}/${view || "-"}) ${res.status} ${txt.slice(0, 300)}`);
    }

    out.push(...(json.records || []));
    offset = json.offset || null;
    if (!offset) break;
  }

  return out;
}

async function airtableListSome({ table, view, fields, maxRecords, sortField, sortDirection = "desc" }) {
  const url = new URL(airtableUrl(table));
  if (view) url.searchParams.set("view", view);
  if (Number.isFinite(Number(maxRecords)) && Number(maxRecords) > 0) {
    url.searchParams.set("maxRecords", String(maxRecords));
  }
  if (sortField) {
    url.searchParams.set("sort[0][field]", sortField);
    url.searchParams.set("sort[0][direction]", sortDirection);
  }
  for (const f of fields || []) url.searchParams.append("fields[]", f);

  const res = await fetchWithRetry(url.toString(), {
    method: "GET",
    headers: headers(),
  });

  const txt = await res.text();
  let json = {};
  try { json = JSON.parse(txt); } catch {}

  if (!res.ok) {
    throw new Error(`Airtable list failed (${table}/${view || "-"}) ${res.status} ${txt.slice(0, 300)}`);
  }

  return json.records || [];
}

async function airtableCreateRecord(table, fields) {
  const res = await fetchWithRetry(airtableUrl(table), {
    method: "POST",
    headers: headers(),
    body: JSON.stringify({ fields }),
  });

  const txt = await res.text();
  let json = {};
  try { json = JSON.parse(txt); } catch {}

  if (!res.ok) {
    throw new Error(`Airtable create failed (${table}) ${res.status} ${txt.slice(0, 300)}`);
  }

  return json;
}

async function airtableUpdateRecord(table, recordId, fields) {
  const res = await fetchWithRetry(`${airtableUrl(table)}/${recordId}`, {
    method: "PATCH",
    headers: headers(),
    body: JSON.stringify({ fields }),
  });

  const txt = await res.text();
  if (!res.ok) {
    throw new Error(`Airtable update failed (${table}/${recordId}) ${res.status} ${txt.slice(0, 300)}`);
  }
}

async function airtableBatchUpdate(table, updates) {
  if (!updates.length) return;

  for (let i = 0; i < updates.length; i += 10) {
    const chunk = updates.slice(i, i + 10);
    const res = await fetchWithRetry(airtableUrl(table), {
      method: "PATCH",
      headers: headers(),
      body: JSON.stringify({ records: chunk }),
    });

    const txt = await res.text();
    if (!res.ok) {
      throw new Error(`Airtable batch update failed (${table}) ${res.status} ${txt.slice(0, 300)}`);
    }
  }
}

function getTzPartsFromMs(ms, timeZone = HB_TZ) {
  const dtf = new Intl.DateTimeFormat("en-US", {
    timeZone,
    hour12: false,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });

  const parts = dtf.formatToParts(new Date(ms));
  const out = { year: 0, month: 0, day: 0, hour: 0, minute: 0, second: 0 };

  for (const p of parts) {
    if (p.type === "year") out.year = Number(p.value);
    if (p.type === "month") out.month = Number(p.value);
    if (p.type === "day") out.day = Number(p.value);
    if (p.type === "hour") out.hour = Number(p.value);
    if (p.type === "minute") out.minute = Number(p.value);
    if (p.type === "second") out.second = Number(p.value);
  }

  return out;
}

function formatSqlDateFromMs(ms, timeZone = HB_TZ) {
  const p = getTzPartsFromMs(ms, timeZone);
  return `${String(p.year).padStart(4, "0")}-${String(p.month).padStart(2, "0")}-${String(p.day).padStart(2, "0")}`;
}

function formatHbTimeFromMs(ms, timeZone = HB_TZ) {
  return new Intl.DateTimeFormat("en-US", {
    timeZone,
    hour12: true,
    hour: "2-digit",
    minute: "2-digit",
  }).format(new Date(ms));
}

function getTzOffsetMinutes(timeZone, ms) {
  const dtf = new Intl.DateTimeFormat("en-US", {
    timeZone,
    timeZoneName: "shortOffset",
    hour: "2-digit",
  });
  const parts = dtf.formatToParts(new Date(ms));
  const tzName = parts.find((p) => p.type === "timeZoneName")?.value || "GMT+0";
  const m = tzName.match(/^GMT([+-])(\d{1,2})(?::?(\d{2}))?$/i);
  if (!m) return 0;
  const sign = m[1] === "-" ? -1 : 1;
  const hh = Number(m[2] || 0);
  const mm = Number(m[3] || 0);
  return sign * ((hh * 60) + mm);
}

function hbDurationSecondsFromMs(ms, timeZone = HB_TZ) {
  const { hour, minute, second } = getTzPartsFromMs(ms, timeZone);
  return (hour * 3600) + (minute * 60) + second;
}

function minuteOfDayFromMs(ms, timeZone = HB_TZ) {
  const { hour, minute } = getTzPartsFromMs(ms, timeZone);
  return (hour * 60) + minute;
}

function dayOfWeekUtc(sqlDate) {
  const d = new Date(`${sqlDate}T00:00:00Z`);
  if (isNaN(d.getTime())) return null;
  return d.getUTCDay();
}

function dowName(dow) {
  return ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"][dow] ?? "";
}

function addDaysSql(sqlDate, days) {
  const d = new Date(`${sqlDate}T00:00:00Z`);
  if (isNaN(d.getTime())) return null;
  d.setUTCDate(d.getUTCDate() + days);
  return d.toISOString().slice(0, 10);
}

function compareSqlDate(left, right) {
  const a = toIsoDateOnly(left);
  const b = toIsoDateOnly(right);
  if (!a || !b) return 0;
  if (a < b) return -1;
  if (a > b) return 1;
  return 0;
}

function buildScheduleEmptyEndpoint(appShowId, customerId = CUSTOMER_ID) {
  if (isBlank(appShowId)) return null;
  return `${SGL_BASE_URL}/schedule?date=00/00/00&show_id=${encodeURIComponent(appShowId)}&customer_id=${encodeURIComponent(customerId)}`;
}

function buildRingEndpoint(customerId = CUSTOMER_ID) {
  return `${SGL_BASE_URL}/ring?customer_id=${encodeURIComponent(customerId)}`;
}

function parseSqlDateSet(value) {
  const out = new Set();
  for (const item of String(value || "").split(/[,\s;]+/)) {
    const iso = toIsoDateOnly(item);
    if (iso) out.add(iso);
  }
  return out;
}

function hasHotpatchScopeOverride() {
  return HOTPATCH_APP_SHOW_ID || HOTPATCH_APP_SQL_DATE;
}

function heartbeatTargetDateSet(decision = null) {
  const values = decision?.target_sql_dates || Array.from(HEARTBEAT_TARGET_SQL_DATES);
  const out = new Set();
  for (const item of values || []) {
    const iso = toIsoDateOnly(item);
    if (iso) out.add(iso);
  }
  return out;
}

function hasHeartbeatTargetDateWindow(decision = null) {
  return Boolean((decision?.show_id || HEARTBEAT_TARGET_APP_SHOW_ID) && heartbeatTargetDateSet(decision).size);
}

function heartbeatTargetShowId(decision = null) {
  const showId = Number(decision?.show_id ?? HEARTBEAT_TARGET_APP_SHOW_ID);
  if (!Number.isFinite(showId)) {
    throw new Error(`Heartbeat target show_id must be numeric: ${decision?.show_id ?? HEARTBEAT_TARGET_APP_SHOW_ID}`);
  }
  return showId;
}

function heartbeatTargetCustomerId(fallback = CUSTOMER_ID) {
  if (!HEARTBEAT_TARGET_CUSTOMER_ID) return fallback;
  const customerId = Number(HEARTBEAT_TARGET_CUSTOMER_ID);
  if (!Number.isFinite(customerId)) {
    throw new Error(`HEARTBEAT_TARGET_CUSTOMER_ID must be numeric: ${HEARTBEAT_TARGET_CUSTOMER_ID}`);
  }
  return customerId;
}

function heartbeatTargetDateForContext(rawSqlDate, candidateAppSqlDate, decision = null) {
  if (!hasHeartbeatTargetDateWindow(decision)) return null;
  const dateSet = heartbeatTargetDateSet(decision);
  const raw = toIsoDateOnly(rawSqlDate);
  if (raw && dateSet.has(raw)) return raw;
  const candidate = toIsoDateOnly(candidateAppSqlDate);
  if (candidate && dateSet.has(candidate)) return candidate;
  return null;
}

function sqlDateInRange(sqlDate, startDate, endDate) {
  const date = toIsoDateOnly(sqlDate);
  const start = toIsoDateOnly(startDate);
  const end = toIsoDateOnly(endDate);
  if (!date || !start || !end) return false;
  return compareSqlDate(date, start) >= 0 && compareSqlDate(date, end) <= 0;
}

function applyHotpatchClockOverride(clock) {
  if (!hasHotpatchScopeOverride()) return clock;
  if (!HOTPATCH_APP_SHOW_ID || !HOTPATCH_APP_SQL_DATE) {
    throw new Error("HOTPATCH_APP_SHOW_ID and HOTPATCH_APP_SQL_DATE must be set together");
  }
  const showId = Number(HOTPATCH_APP_SHOW_ID);
  if (!Number.isFinite(showId)) {
    throw new Error(`HOTPATCH_APP_SHOW_ID must be numeric: ${HOTPATCH_APP_SHOW_ID}`);
  }
  return {
    ...clock,
    showId,
    showDate: HOTPATCH_APP_SQL_DATE,
    source: `${clock?.source || "clock"}+hotpatch_scope_override`,
  };
}

function applyHeartbeatTargetClockOverride(clock, decision = null) {
  if (!hasHeartbeatTargetDateWindow(decision)) return clock;
  const dateSet = heartbeatTargetDateSet(decision);
  const rawSqlDate = toIsoDateOnly(clock?.sqlDate);
  if (!rawSqlDate || !dateSet.has(rawSqlDate)) return clock;
  const showId = heartbeatTargetShowId(decision);
  const customerId = decision?.customer_id ?? heartbeatTargetCustomerId();
  return {
    ...clock,
    showId,
    showDate: rawSqlDate,
    customerId,
    showDecision: decision || null,
    source: `${clock?.source || "clock"}+heartbeat_target_date_window`,
  };
}

function extractScheduleDefaultInfo(payload) {
  const show = payload?.show && typeof payload.show === "object" ? payload.show : {};
  const showAppName = strOrNull(pickFirst(show.show_name, payload?.show_name));
  const showAppSqlStartDate = toIsoDateOnly(pickFirst(show.start_date, payload?.start_date));
  const showAppSqlEndDate = toIsoDateOnly(pickFirst(show.end_date, payload?.end_date));
  const defaultAppSqlDateIs = toIsoDateOnly(pickFirst(payload?.show_date, payload?.showDate));
  const validDates = Array.isArray(payload?.show_days_list)
    ? payload.show_days_list.map((item) => toIsoDateOnly(item?.date)).filter(Boolean)
    : [];

  return {
    showAppName,
    showAppSqlStartDate,
    showAppSqlEndDate,
    defaultAppSqlDateIs,
    validDates,
  };
}

function isValidAppSqlDate(candidateDate, scheduleInfo) {
  if (!candidateDate) return false;
  if (Array.isArray(scheduleInfo?.validDates) && scheduleInfo.validDates.length) {
    return scheduleInfo.validDates.includes(candidateDate);
  }
  if (scheduleInfo?.showAppSqlStartDate && compareSqlDate(candidateDate, scheduleInfo.showAppSqlStartDate) < 0) {
    return false;
  }
  if (scheduleInfo?.showAppSqlEndDate && compareSqlDate(candidateDate, scheduleInfo.showAppSqlEndDate) > 0) {
    return false;
  }
  return true;
}

function normalizeMode(v) {
  return normalizeHeartbeatMode(v, "DAY");
}

function deriveModeFromClock(clock) {
  const minuteOfDay = minuteOfDayFromMs(clock.nowMs, HB_TZ);

  if (minuteOfDay >= 300 && minuteOfDay <= 1019) return "DAY";        // 5:00 AM - 4:59 PM
  if (minuteOfDay >= 1020 && minuteOfDay <= 1439) return "NIGHT";     // 5:00 PM - 11:59 PM
  return "OVERNIGHT";                                                  // 12:00 AM - 4:59 AM
}

function resolveModeFromClock(clock) {
  if (FORCE_MODE) return normalizeMode(FORCE_MODE);
  return deriveModeFromClock(clock);
}

function intervalMinutesForMode(mode) {
  if (mode === "DAY") return DAY_INTERVAL_MIN;
  if (mode === "NIGHT") return NIGHT_INTERVAL_MIN;
  if (mode === "IDLE") return IDLE_INTERVAL_MIN;
  if (mode === "OFF") return OFF_INTERVAL_MIN;
  return OVERNIGHT_INTERVAL_MIN;
}

async function latestHeartbeatModeControl() {
  try {
    const rows = await airtableListSome({
      table: TABLE_HEARTBEAT,
      fields: [
        FIELD_MODE,
        FIELD_CADENCE,
        FIELD_SET_INTERVALS,
        FIELD_INTERVAL,
        FIELD_EPOCH,
        FIELD_HB_AT,
      ],
      maxRecords: 1,
      sortField: FIELD_EPOCH,
      sortDirection: "desc"
    });

    const latest = rows[0] || null;
    const fields = latest?.fields || {};
    const mode = normalizeMode(fields[FIELD_MODE]);

    if (!isHeartbeatControlMode(mode)) return null;

    return {
      mode,
      source: "latest_heartbeat",
      heartbeat_record_id: latest?.id || null,
      cadence_seconds: resolveHeartbeatCadenceSeconds({
        mode,
        cadence: fields[FIELD_CADENCE],
        set_intervals: fields[FIELD_SET_INTERVALS],
        interval: fields[FIELD_INTERVAL],
      })
    };
  } catch (e) {
    logWarn("heartbeat_mode_control_lookup_failed", {
      table: TABLE_HEARTBEAT,
      error_message: String(e?.message || e).slice(0, 240)
    });
    return null;
  }
}

function buildFallbackClock() {
  const nowMs = Date.now();
  return {
    source: "system",
    nowMs,
    nowEpoch: Math.floor(nowMs / 1000),
    tzOffsetMinutes: getTzOffsetMinutes(HB_TZ, nowMs),
    showId: null,
    showDate: null,
    sqlDate: formatSqlDateFromMs(nowMs, HB_TZ),
    time: formatHbTimeFromMs(nowMs, HB_TZ),
    iso: new Date(nowMs).toISOString(),
  };
}

function pickClockFromPayload(payload) {
  const tz = payload?.time_zone_date_time || null;
  const show = payload?.show || null;
  const dateObj = tz?.date_obj || tz?.time_obj || null;
  const nowMs = typeof dateObj === "string" ? Date.parse(dateObj) : NaN;

  if (!tz || !Number.isFinite(nowMs)) return null;

  return {
    source: "endpoint",
    nowMs,
    nowEpoch: Math.floor(nowMs / 1000),
    tzOffsetMinutes: Number.isFinite(Number(tz.time_zone_offset))
      ? Number(tz.time_zone_offset)
      : getTzOffsetMinutes(HB_TZ, nowMs),
    showId: show?.show_id ?? null,
    showDate: show?.show_date ?? null,
    sqlDate: tz?.sql_date || formatSqlDateFromMs(nowMs, HB_TZ),
    time: tz?.time || formatHbTimeFromMs(nowMs, HB_TZ),
    iso: new Date(nowMs).toISOString(),
  };
}

async function getClockSafe(customerId = CUSTOMER_ID) {
  const systemClock = buildFallbackClock();
  const ringEndpoint = buildRingEndpoint(customerId);

  try {
    const fetched = await fetchTextWithConfiguredTransport(ringEndpoint, async (endpoint) => {
      const response = await fetchWithTimeout(endpoint, { method: "GET" });
      const text = await response.text();
      return { response, text, endpoint };
    });
    const res = fetched.response;
    const txt = fetched.text;
    const endpoint = fetched.endpoint || ringEndpoint;

    if (!res.ok) {
      return await fallbackToBestKnownClock(systemClock, "endpoint_fallback_http", {
        endpoint,
        http_status: res.status,
        system_sql_date: systemClock.sqlDate,
        system_time: systemClock.time
      });
    }

    let payload = null;
    try {
      payload = JSON.parse(txt);
    } catch {
      return await fallbackToBestKnownClock(systemClock, "endpoint_fallback_invalid_json", {
        endpoint,
        body_sample: txt.slice(0, 250),
        system_sql_date: systemClock.sqlDate,
        system_time: systemClock.time
      });
    }

    try {
      assertValidPayload({
        payload,
        text: txt,
        response: res,
        lane: "tagger_ring",
        endpoint,
        expectedTopLevelKeys: ["time_zone_date_time", "show", "show_id"],
      });
    } catch (e) {
      if (isSoftPayloadError(e)) {
        return await fallbackToBestKnownClock(systemClock, "endpoint_fallback_soft_payload", {
          ...softPayloadLogFields(e),
          system_sql_date: systemClock.sqlDate,
          system_time: systemClock.time
        });
      }
      throw e;
    }

    const endpointClock = pickClockFromPayload(payload);
    if (!endpointClock) {
      return await fallbackToBestKnownClock(systemClock, "endpoint_fallback_missing_clock_values", {
        endpoint,
        system_sql_date: systemClock.sqlDate,
        system_time: systemClock.time
      });
    }

    const systemSqlDate = systemClock.sqlDate;
    if (String(endpointClock.sqlDate || "") !== String(systemSqlDate || "")) {
      return await fallbackToBestKnownClock(systemClock, "endpoint_fallback_sql_date_mismatch", {
        endpoint,
        endpoint_show_id: endpointClock.showId,
        endpoint_show_date: endpointClock.showDate,
        endpoint_sql_date: endpointClock.sqlDate,
        endpoint_time: endpointClock.time,
        system_sql_date: systemSqlDate,
        system_time: systemClock.time
      });
    }

    persistLastKnownClock(endpointClock, "endpoint", {
      endpoint: ringEndpoint
    });

    if (LOG_ACCEPTED_ENDPOINT) {
      logInfo("endpoint_clock_accepted", {
        endpoint: ringEndpoint,
        endpoint_show_id: endpointClock.showId,
        endpoint_show_date: endpointClock.showDate,
        endpoint_sql_date: endpointClock.sqlDate,
        endpoint_time: endpointClock.time,
        source: endpointClock.source
      });
    }

    return endpointClock;
  } catch (e) {
    return await fallbackToBestKnownClock(systemClock, "endpoint_fallback_fetch_error", {
      endpoint: ringEndpoint,
      error_name: e?.name || null,
      error_message: String(e?.message || e).slice(0, 240),
      system_sql_date: systemClock.sqlDate,
      system_time: systemClock.time
    });
  }
}

async function buildAppContext(clock) {
  const dowRaw = dowName(dayOfWeekUtc(clock.sqlDate));

  let appShowId = clock.showId ?? null;
  let appSqlDate = clock.sqlDate;
  let shiftedToNextDay = false;
  let candidateAppSqlDate = clock.sqlDate;
  let setToDefaultAppSqlDate = false;
  let defaultAppSqlDateIs = null;
  let showAppSqlStartDate = null;
  let showAppSqlEndDate = null;
  let showAppName = null;
  let appSqlDateSource = "raw_day";

  const customerId = clock.customerId ?? CUSTOMER_ID;
  const emptyScheduleEndpoint = buildScheduleEmptyEndpoint(appShowId, customerId);
  if (emptyScheduleEndpoint) {
    try {
      const emptySchedulePayload = await fetchJson(emptyScheduleEndpoint);
      const scheduleInfo = extractScheduleDefaultInfo(emptySchedulePayload);
      defaultAppSqlDateIs = scheduleInfo.defaultAppSqlDateIs;
      showAppSqlStartDate = scheduleInfo.showAppSqlStartDate;
      showAppSqlEndDate = scheduleInfo.showAppSqlEndDate;
      showAppName = scheduleInfo.showAppName;

      const validCandidate = isValidAppSqlDate(candidateAppSqlDate, scheduleInfo);
      if (!validCandidate && scheduleInfo.defaultAppSqlDateIs) {
        appSqlDate = scheduleInfo.defaultAppSqlDateIs;
        shiftedToNextDay = false;
        setToDefaultAppSqlDate = true;
        appSqlDateSource = "default_day";
      }
    } catch (e) {
      logWarn(isSoftPayloadError(e) ? "schedule_default_soft_payload" : "schedule_default_lookup_failed", {
        endpoint: emptyScheduleEndpoint,
        app_show_id: appShowId,
        candidate_app_sql_date: candidateAppSqlDate,
        error_message: String(e?.message || e).slice(0, 240),
        ...(isSoftPayloadError(e) ? softPayloadLogFields(e) : {})
      });
    }
  }

  // Keep heartbeat scope fields self-contained even when the empty schedule
  // endpoint is transiently unavailable or missing expected fields.
  if (!defaultAppSqlDateIs && appSqlDate) {
    defaultAppSqlDateIs = appSqlDate;
  }

  const decision = clock.showDecision || null;
  const heartbeatTargetAppSqlDate = heartbeatTargetDateForContext(clock.sqlDate, candidateAppSqlDate, decision);
  if (heartbeatTargetAppSqlDate) {
    appShowId = heartbeatTargetShowId(decision);
    const sortedDates = Array.from(heartbeatTargetDateSet(decision)).sort();
    showAppSqlStartDate = decision?.start_date || sortedDates[0] || heartbeatTargetAppSqlDate;
    showAppSqlEndDate = decision?.end_date || sortedDates[sortedDates.length - 1] || heartbeatTargetAppSqlDate;
    showAppName = decision?.show_name || showAppName;
    if (decision) {
      shiftedToNextDay = !!decision.shifted_to_next_day;
      setToDefaultAppSqlDate = !!decision.set_to_default_app_sql_date;
      if (setToDefaultAppSqlDate) {
        if (!defaultAppSqlDateIs) {
          throw new Error(`Focused show ${appShowId} requested ${FIELD_SET_TO_DEFAULT_APP_SQL_DATE} but no default app sql date was resolved`);
        }
        appSqlDate = defaultAppSqlDateIs;
        appSqlDateSource = "show_focus_default_day";
      } else {
        appSqlDate = shiftedToNextDay
          ? (addDaysSql(decision.focus_day, 1) || decision.focus_day)
          : decision.focus_day;
        defaultAppSqlDateIs = appSqlDate;
        appSqlDateSource = shiftedToNextDay ? "show_focus_shifted_day" : "show_focus_day";
      }
      if (!sqlDateInRange(appSqlDate, decision.start_date, decision.end_date)) {
        throw new Error(`Focused show ${appShowId} resolved app_sql_date ${appSqlDate} outside ${decision.start_date}..${decision.end_date}`);
      }
    }
  }

  if (hasHotpatchScopeOverride()) {
    appShowId = Number(HOTPATCH_APP_SHOW_ID);
    if (!Number.isFinite(appShowId)) {
      throw new Error(`HOTPATCH_APP_SHOW_ID must be numeric: ${HOTPATCH_APP_SHOW_ID}`);
    }
    appSqlDate = HOTPATCH_APP_SQL_DATE;
    candidateAppSqlDate = HOTPATCH_APP_SQL_DATE;
    shiftedToNextDay = false;
    setToDefaultAppSqlDate = false;
    defaultAppSqlDateIs = HOTPATCH_APP_SQL_DATE;
    appSqlDateSource = "hotpatch_env_override";
  }

  const appDowRaw = dowName(dayOfWeekUtc(appSqlDate));
  const defaultShowDateGuard = computeDefaultShowDateGuard({
    rawSqlDate: clock.sqlDate,
    appSqlDate,
    defaultAppSqlDateIs,
    showAppSqlStartDate,
    showAppSqlEndDate,
    setToDefaultAppSqlDate,
  });

  const appCtx = {
    dowRaw,
    appShowId,
    appSqlDate,
    appDowRaw,
    shiftedToNextDay,
    setToDefaultAppSqlDate,
    defaultAppSqlDateIs,
    showAppSqlStartDate,
    showAppSqlEndDate,
    showAppName,
    appSqlDateSource,
    candidateAppSqlDate,
    defaultShowDateGuard,
  };

  if (LOG_TRANSITIONS) {
    logInfo("app_context_computed", {
      source: clock.source,
      raw_show_id: clock.showId ?? null,
      raw_show_date: clock.showDate ?? null,
      raw_sql_date: clock.sqlDate,
      raw_time: clock.time,
      app_show_id: appShowId,
      app_sql_date: appSqlDate,
      app_dow_raw: appDowRaw,
      shifted_to_next_day: shiftedToNextDay,
      set_to_default_app_sql_date: setToDefaultAppSqlDate,
      default_app_sql_date_is: defaultAppSqlDateIs,
      show_app_sql_start_date: showAppSqlStartDate,
      show_app_sql_end_date: showAppSqlEndDate,
      show_app_name: showAppName,
      app_sql_date_source: appSqlDateSource,
      candidate_app_sql_date: candidateAppSqlDate,
      default_show_date_guard: defaultShowDateGuard,
    });
  }

  return appCtx;
}

async function createHeartbeat(clock, mode, intervalMin, appCtx) {
  const sqlDate = String(clock?.sqlDate || "").trim();
  const epoch = Number(clock?.nowEpoch);

  if (!sqlDate || !Number.isFinite(epoch)) {
    throw new Error("Heartbeat create missing sqlDate/epoch");
  }

  const fields = {
    [HEARTBEAT_ID_FIELD]: `${clock?.showId ?? "unknown"}-${sqlDate}-${epoch}`,
    [HEARTBEAT_SHOW_ID]: clock?.showId ?? null,
    [HEARTBEAT_SHOW_DATE]: clock?.showDate ?? null,
    [HEARTBEAT_SQL_DATE]: sqlDate,
    [HEARTBEAT_TIME]: clock?.time ?? null,

    [FIELD_MODE]: mode,
    [FIELD_EPOCH]: epoch,
    [FIELD_HB_DURATION]: hbDurationSecondsFromMs(clock.nowMs, HB_TZ),
    [FIELD_INTERVAL]: intervalMin,
    [FIELD_HB_AT]: clock?.iso ?? new Date(epoch * 1000).toISOString(),

    [FIELD_APP_SHOW_ID]: appCtx.appShowId,
    [FIELD_APP_SQL_DATE]: appCtx.appSqlDate,
    [FIELD_APP_DOW_RAW]: appCtx.appDowRaw,
    [FIELD_DOW_RAW]: appCtx.dowRaw,
    [FIELD_SHIFTED_NEXT_DAY]: appCtx.shiftedToNextDay,
    [FIELD_SET_TO_DEFAULT_APP_SQL_DATE]: appCtx.setToDefaultAppSqlDate,
  };

  const maybeSet = (fieldName, value) => {
    if (value === undefined) return;
    fields[fieldName] = value;
  };

  maybeSet(FIELD_DEFAULT_APP_SQL_DATE_IS, appCtx.defaultAppSqlDateIs);
  maybeSet(FIELD_SHOW_APP_SQL_START_DATE, appCtx.showAppSqlStartDate);
  maybeSet(FIELD_SHOW_APP_SQL_END_DATE, appCtx.showAppSqlEndDate);
  maybeSet(FIELD_SHOW_APP_NAME, appCtx.showAppName);
  maybeSet(FIELD_APP_SQL_DATE_SOURCE, appCtx.appSqlDateSource);
  maybeSet(FIELD_CLOCK_MODE, appCtx.clockMode);
  maybeSet(FIELD_MODE_SOURCE, appCtx.modeSource);
  maybeSet(FIELD_MODE_REASON, appCtx.modeReason);
  maybeSet(FIELD_DEFAULT_SHOW_DATE_STATUS, appCtx.defaultShowDateStatus);
  maybeSet(FIELD_DEFAULT_SHOW_DATE_REASON, appCtx.defaultShowDateReason);

  if (DRY_RUN) {
    logInfo("heartbeat_create_dry_run", {
      heartbeat_id: fields[HEARTBEAT_ID_FIELD],
      raw_show_id: fields[HEARTBEAT_SHOW_ID],
      raw_sql_date: fields[HEARTBEAT_SQL_DATE],
      app_show_id: fields[FIELD_APP_SHOW_ID],
      app_sql_date: fields[FIELD_APP_SQL_DATE],
      app_dow_raw: fields[FIELD_APP_DOW_RAW],
      shifted_to_next_day: fields[FIELD_SHIFTED_NEXT_DAY],
      set_to_default_app_sql_date: fields[FIELD_SET_TO_DEFAULT_APP_SQL_DATE],
      default_app_sql_date_is: fields[FIELD_DEFAULT_APP_SQL_DATE_IS] ?? null,
      show_app_sql_start_date: fields[FIELD_SHOW_APP_SQL_START_DATE] ?? null,
      show_app_sql_end_date: fields[FIELD_SHOW_APP_SQL_END_DATE] ?? null,
      show_app_name: fields[FIELD_SHOW_APP_NAME] ?? null,
      app_sql_date_source: fields[FIELD_APP_SQL_DATE_SOURCE] ?? null,
      clock_mode: fields[FIELD_CLOCK_MODE] ?? null,
      mode_source: fields[FIELD_MODE_SOURCE] ?? null,
      mode_reason: fields[FIELD_MODE_REASON] ?? null,
      default_show_date_status: fields[FIELD_DEFAULT_SHOW_DATE_STATUS] ?? null,
      default_show_date_reason: fields[FIELD_DEFAULT_SHOW_DATE_REASON] ?? null,
      default_show_date_guard: appCtx.defaultShowDateGuard,
      mode
    });
    return { id: "recDRYRUNHEARTBEAT", fields };
  }

  return await airtableCreateRecord(TABLE_HEARTBEAT, fields);
}

function persistLatestHeartbeatContext(heartbeatRecord, appCtx, mode) {
  const state = loadRuntimeState();
  const nextState = {
    ...state,
    latestHeartbeatContext: {
      version: 1,
      saved_at: new Date().toISOString(),
      heartbeat_record_id: heartbeatRecord?.id || null,
      app_show_id: appCtx?.appShowId ?? null,
      app_sql_date: appCtx?.appSqlDate ?? null,
      app_dow_raw: appCtx?.appDowRaw ?? null,
      shifted_to_next_day: !!appCtx?.shiftedToNextDay,
      mode,
      clock_mode: appCtx?.clockMode ?? null,
      mode_source: appCtx?.modeSource ?? null,
      mode_reason: appCtx?.modeReason ?? null,
      default_show_date_status: appCtx?.defaultShowDateStatus ?? null,
      default_show_date_reason: appCtx?.defaultShowDateReason ?? null,
      scope_key: [
        appCtx?.appShowId ?? "",
        appCtx?.appSqlDate ?? "",
        appCtx?.appDowRaw ?? "",
        appCtx?.shiftedToNextDay ? "1" : "0",
      ].join("|"),
    }
  };
  saveRuntimeState(nextState);
}

function currentHeartbeatLinkIds(v) {
  if (!Array.isArray(v)) return [];
  return v.map(x => typeof x === "string" ? x : x?.id).filter(Boolean);
}

function relinkSupportsArchive(tableName) {
  return tableName === TABLE_WATCH_SCHEDULE || tableName === TABLE_WATCH_TRIPS;
}

function recordIsScopeInactive(fields = {}) {
  if (boolValue(fields?.[FIELD_INACTIVE]) === true) return true;
  const scopeStatus = strOrNull(airtableValueName(fields?.[FIELD_SCOPE_STATUS]));
  return scopeStatus ? scopeStatus.toLowerCase() === "dropped" : false;
}

function archiveFieldPatch(tableName, fields = {}, desiredArchive) {
  if (!relinkSupportsArchive(tableName)) return {};
  const currentArchive = boolValue(fields?.[FIELD_ARCHIVE]) === true;
  return currentArchive === desiredArchive ? {} : { [FIELD_ARCHIVE]: desiredArchive };
}

function relinkFieldsForTable(tableName) {
  if (tableName !== TABLE_WATCH_SCHEDULE && tableName !== TABLE_WATCH_TRIPS) return [FIELD_LINK_HEARTBEAT];
  if (tableName === TABLE_WATCH_TRIPS) {
    return [
      FIELD_LINK_HEARTBEAT,
      FIELD_ARCHIVE,
      FIELD_INACTIVE,
      FIELD_SCOPE_STATUS,
      "show_id",
      "app_show_idv2",
      "app_sql_datev2",
      "app_dow_rawv2",
      "schedule_show_datev2",
      "schedule_show_datev2 (from watch_schedule)",
      "scheduled_date",
      "show_date",
      "app_sql_date (from heartbeat)",
      "app_dow_raw (from heartbeat)",
    ];
  }
  return [
    FIELD_LINK_HEARTBEAT,
    FIELD_ARCHIVE,
    FIELD_INACTIVE,
    FIELD_SCOPE_STATUS,
    "show_id",
    "app_show_idv2",
    "app_sql_datev2",
    "app_dow_rawv2",
    "schedule_show_datev2",
    "scheduled_date",
    "show_date",
    "app_sql_date (from heartbeat)",
    "app_dow_raw (from heartbeat)",
  ];
}

function classifyRelinkForTable(tableName, fields, heartbeatId, appCtx) {
  if (tableName !== TABLE_WATCH_SCHEDULE && tableName !== TABLE_WATCH_TRIPS) {
    const current = currentHeartbeatLinkIds(fields?.[FIELD_LINK_HEARTBEAT]);
    const alreadyCorrect = current.length === 1 && current[0] === heartbeatId;
    return { action: alreadyCorrect ? "keep" : "link", current };
  }
  if (recordIsScopeInactive(fields)) {
    const current = currentHeartbeatLinkIds(fields?.[FIELD_LINK_HEARTBEAT]);
    return {
      action: current.length ? "clear" : "skip",
      current,
      matches_scope: false,
      scope_inactive: true,
    };
  }
  if (tableName === TABLE_WATCH_TRIPS) {
    return classifyWatchTripsHeartbeatRelink(fields, appCtx, heartbeatId);
  }
  return classifyWatchScheduleHeartbeatRelink(fields, appCtx, heartbeatId);
}

async function relinkHeartbeatView(tableName, heartbeatId, appCtx = null) {
  const rows = await airtableListAll({
    table: tableName,
    view: VIEW_HEARTBEAT,
    fields: relinkFieldsForTable(tableName)
  });

  const updates = [];
  let kept = 0;
  let skippedScopeMismatch = 0;
  let clearedScopeMismatch = 0;
  let linkedHeartbeat = 0;
  let markedArchive = 0;
  let clearedArchive = 0;

  for (const r of rows) {
    const decision = classifyRelinkForTable(tableName, r.fields || {}, heartbeatId, appCtx);
    const shouldArchive = decision.action === "skip" || decision.action === "clear";
    const archivePatch = archiveFieldPatch(tableName, r.fields || {}, shouldArchive);
    if (FIELD_ARCHIVE in archivePatch) {
      if (archivePatch[FIELD_ARCHIVE]) markedArchive++;
      else clearedArchive++;
    }

    if (decision.action === "keep") {
      kept++;
      if (Object.keys(archivePatch).length) {
        updates.push({
          id: r.id,
          fields: archivePatch
        });
      }
      continue;
    }
    if (decision.action === "skip") {
      skippedScopeMismatch++;
      if (Object.keys(archivePatch).length) {
        updates.push({
          id: r.id,
          fields: archivePatch
        });
      }
      continue;
    }
    if (decision.action === "clear") {
      clearedScopeMismatch++;
      updates.push({
        id: r.id,
        fields: {
          [FIELD_LINK_HEARTBEAT]: [],
          ...archivePatch
        }
      });
      continue;
    }
    linkedHeartbeat++;
    updates.push({
      id: r.id,
      fields: {
        [FIELD_LINK_HEARTBEAT]: [heartbeatId],
        ...archivePatch
      }
    });
  }

  if (!DRY_RUN && updates.length) {
    await airtableBatchUpdate(tableName, updates);
  }

  const summary = {
    table: tableName,
    found_in_view: rows.length,
    relinked: linkedHeartbeat,
    cleared_scope_mismatch: clearedScopeMismatch,
    skipped_scope_mismatch: skippedScopeMismatch,
    marked_archive: markedArchive,
    cleared_archive: clearedArchive,
    kept
  };

  if (LOG_RELINK_SUMMARY && updates.length) {
    logInfo("relink_summary", summary);
  }

  return summary;
}

function matchesShowRow(fields, appShowId) {
  const target = String(appShowId ?? "").trim();
  const showId = String(fields?.[FIELD_SHOW_ID] ?? "").trim();
  const appId  = String(fields?.[FIELD_APP_SHOW_ID] ?? "").trim();
  return (showId && showId === target) || (appId && appId === target);
}

async function findShowsMatchInView(appShowId) {
  const rows = await airtableListAll({
    table: TABLE_SHOWS,
    view: VIEW_HEARTBEAT,
    fields: [FIELD_SHOW_ID, FIELD_APP_SHOW_ID, FIELD_LINK_HEARTBEAT]
  });

  const matches = rows.filter(r => matchesShowRow(r.fields || {}, appShowId));

  return {
    rows,
    matches,
    match: matches[0] || null
  };
}

async function findShowsMatchAnywhere(appShowId) {
  const rows = await airtableListAll({
    table: TABLE_SHOWS,
    fields: [
      FIELD_SHOW_ID,
      FIELD_APP_SHOW_ID,
      FIELD_LINK_HEARTBEAT,
      FIELD_CUSTOMER_ID,
      FIELD_CUSTOMER_ID_OVERRIDE,
      FIELD_SHOW_START_DATE_BASE,
      FIELD_SHOW_END_DATE_BASE,
      FIELD_SHOW_NAME_BASE,
      FIELD_MODE_CONTROL,
      FIELD_IS_DEFAULT_SHOW_MANUAL_OVERRIDE,
    ]
  });

  const matches = rows.filter(r => matchesShowRow(r.fields || {}, appShowId));

  return {
    rows,
    matches,
    match: matches[0] || null
  };
}

function numericFieldOrNull(value) {
  if (value === null || value === undefined || String(value).trim() === "") return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function resolveShowCustomerId(fields) {
  return numericFieldOrNull(fields?.[FIELD_CUSTOMER_ID]) ??
    numericFieldOrNull(fields?.[FIELD_CUSTOMER_ID_OVERRIDE]);
}

async function findFocusedShowTarget() {
  const fields = [
    FIELD_SHOW_ID,
    FIELD_CUSTOMER_ID,
    FIELD_SHOW_START_DATE_BASE,
    FIELD_SHOW_END_DATE_BASE,
    FIELD_SHOW_FOCUS_DAY,
    FIELD_SHIFTED_NEXT_DAY,
    FIELD_SET_TO_DEFAULT_APP_SQL_DATE,
    FIELD_SHOW_NAME_BASE,
    FIELD_SHOW_TARGET_HEARTBEAT,
    FIELD_MODE_CONTROL,
    FIELD_IS_DEFAULT_SHOW_MANUAL_OVERRIDE,
  ];

  let rows = [];
  try {
    rows = await airtableListAll({ table: TABLE_SHOW_TARGET, view: VIEW_SHOW_TARGET, fields });
  } catch (e) {
    const message = String(e?.message || e);
    if (!/view|INVALID_REQUEST_UNKNOWN/i.test(message)) throw e;
    const allRows = await airtableListAll({ table: TABLE_SHOW_TARGET, fields });
    rows = allRows.filter(row => boolValue(row.fields?.[FIELD_SHOW_TARGET_HEARTBEAT]));
    logWarn("show_target_view_unavailable_using_heartbeat_field", {
      table: TABLE_SHOW_TARGET,
      requested_view: VIEW_SHOW_TARGET,
      matched_rows: rows.length,
      error_message: message.slice(0, 240),
    });
  }

  const heartbeatRows = rows.filter(row => boolValue(row.fields?.[FIELD_SHOW_TARGET_HEARTBEAT]));
  if (heartbeatRows.length) rows = heartbeatRows;

  const selected = rows.filter(row => {
    const fields = row.fields || {};
    return hasValue(fields[FIELD_SHOW_ID]) &&
      hasValue(fields[FIELD_CUSTOMER_ID]) &&
      hasValue(fields[FIELD_SHOW_START_DATE_BASE]) &&
      hasValue(fields[FIELD_SHOW_END_DATE_BASE]);
  });

  if (selected.length !== 1) {
    throw new Error(`${TABLE_SHOW_TARGET}/${VIEW_SHOW_TARGET} must expose exactly one focused show row with show_id, customer_id, start_date, and end_date; found ${selected.length}`);
  }

  return selected[0];
}

async function resolveHeartbeatTargetDecision() {
  let targetRecord = null;
  try {
    targetRecord = await findFocusedShowTarget();
  } catch (e) {
    if (!hasHeartbeatTargetDateWindow()) throw e;
  }

  if (!targetRecord && !hasHeartbeatTargetDateWindow()) return null;

  let fields = targetRecord?.fields || {};
  let appShowId = numericFieldOrNull(fields[FIELD_SHOW_ID]);
  if (appShowId === null && hasHeartbeatTargetDateWindow()) {
    appShowId = heartbeatTargetShowId();
    const lookup = await findShowsMatchAnywhere(appShowId);
    if (!lookup.match) {
      throw new Error(`Heartbeat target show_id ${appShowId} was not found in ${TABLE_SHOWS}`);
    }
    fields = lookup.match.fields || {};
  }

  const customerId = resolveShowCustomerId(fields);
  if (!Number.isFinite(customerId)) {
    throw new Error(`Heartbeat target show_id ${appShowId} has no numeric ${FIELD_CUSTOMER_ID} or ${FIELD_CUSTOMER_ID_OVERRIDE}`);
  }

  const startDate = toIsoDateOnly(fields[FIELD_SHOW_START_DATE_BASE]);
  const endDate = toIsoDateOnly(fields[FIELD_SHOW_END_DATE_BASE]);
  const focusDay = toIsoDateOnly(fields[FIELD_SHOW_FOCUS_DAY]);
  if (!focusDay) {
    throw new Error(`Focused show ${appShowId} has no ${FIELD_SHOW_FOCUS_DAY}`);
  }
  if (!sqlDateInRange(focusDay, startDate, endDate)) {
    throw new Error(`Focused show ${appShowId} ${FIELD_SHOW_FOCUS_DAY} ${focusDay} is outside ${startDate}..${endDate}`);
  }
  const targetDates = new Set();
  targetDates.add(focusDay);
  const sortedDates = Array.from(targetDates).sort();
  return {
    record_id: targetRecord?.id || null,
    table: targetRecord ? TABLE_SHOW_TARGET : TABLE_SHOWS,
    show_id: appShowId,
    customer_id: customerId,
    target_sql_dates: sortedDates,
    start_date: startDate,
    end_date: endDate,
    focus_day: focusDay,
    shifted_to_next_day: boolValue(fields[FIELD_SHIFTED_NEXT_DAY]),
    set_to_default_app_sql_date: boolValue(fields[FIELD_SET_TO_DEFAULT_APP_SQL_DATE]),
    show_name: strOrNull(fields[FIELD_SHOW_NAME_BASE]),
  };
}

async function fetchShowsModeControl(appShowId) {
  if (appShowId === null || appShowId === undefined || String(appShowId).trim() === "") {
    return {
      found: false,
      record_id: null,
      mode_control: null,
      mode_control_reason: null,
      is_default_show_manual_override: false,
    };
  }

  const rows = await airtableListAll({
    table: TABLE_SHOWS,
    fields: [
      FIELD_SHOW_ID,
      FIELD_APP_SHOW_ID,
      FIELD_MODE_CONTROL,
      FIELD_IS_DEFAULT_SHOW_MANUAL_OVERRIDE,
    ],
  });
  const matches = rows.filter(r => matchesShowRow(r.fields || {}, appShowId));
  const match = matches[0] || null;
  const fields = match?.fields || {};
  const modeControl = normalizeControlMode(fields[FIELD_MODE_CONTROL]);

  if (matches.length > 1) {
    logWarn("shows_mode_control_duplicate_matches", {
      app_show_id: appShowId,
      count: matches.length,
      record_ids: matches.map(r => r.id),
    });
  }

  return {
    found: !!match,
    record_id: match?.id || null,
    matched_count: matches.length,
    mode_control: modeControl,
    mode_control_reason: null,
    is_default_show_manual_override: boolValue(fields[FIELD_IS_DEFAULT_SHOW_MANUAL_OVERRIDE]),
  };
}

async function syncShowsHeartbeat(heartbeatRecord, appCtx, mode) {
  const heartbeatId = heartbeatRecord.id;
  const appShowId = appCtx.appShowId;

  if (appShowId === null || appShowId === undefined || String(appShowId).trim() === "") {
    if (LOG_SHOWS_SYNC) {
      logWarn("shows_sync_skipped_missing_app_show_id", {
        heartbeat_record_id: heartbeatId,
        app_show_id: appShowId,
        app_sql_date: appCtx.appSqlDate,
        shifted_to_next_day: appCtx.shiftedToNextDay
      });
    }

    return {
      table: TABLE_SHOWS,
      found_in_view: 0,
      matched_show_id: null,
      updated_existing: 0,
      created_new: 0,
      skipped: "missing_app_show_id"
    };
  }

  const viewLookup = await findShowsMatchInView(appShowId);

  if (viewLookup.matches.length > 1) {
    logWarn("shows_sync_duplicate_matches_in_view", {
      app_show_id: appShowId,
      count: viewLookup.matches.length,
      record_ids: viewLookup.matches.map(r => r.id)
    });
  }

  if (viewLookup.match) {
    const match = viewLookup.match;
    const current = currentHeartbeatLinkIds(match.fields?.[FIELD_LINK_HEARTBEAT]);

    const updateFields = {
      [FIELD_LINK_HEARTBEAT]: [heartbeatId],
      [FIELD_APP_SHOW_ID]: appShowId,
      [FIELD_APP_SQL_DATE]: appCtx.appSqlDate,
      [FIELD_APP_DOW_RAW]: appCtx.appDowRaw,
      [FIELD_DOW_RAW]: appCtx.dowRaw,
      [FIELD_MODE]: mode,
      [FIELD_SHIFTED_NEXT_DAY]: appCtx.shiftedToNextDay,
    };
    if (appCtx.showAppName !== undefined) updateFields[FIELD_SHOW_NAME_BASE] = appCtx.showAppName;
    if (appCtx.showAppSqlStartDate !== undefined) updateFields[FIELD_SHOW_START_DATE_BASE] = appCtx.showAppSqlStartDate;
    if (appCtx.showAppSqlEndDate !== undefined) updateFields[FIELD_SHOW_END_DATE_BASE] = appCtx.showAppSqlEndDate;

    if (LOG_SHOWS_SYNC) {
      logInfo("shows_sync_update_existing_in_view", {
        app_show_id: appShowId,
        app_sql_date: appCtx.appSqlDate,
        heartbeat_record_id: heartbeatId,
        matched_record_id: match.id,
        existing_heartbeat_links_count: current.length
      });
    }

    if (!DRY_RUN) {
      await airtableUpdateRecord(TABLE_SHOWS, match.id, updateFields);
    }

    return {
      table: TABLE_SHOWS,
      found_in_view: viewLookup.rows.length,
      matched_show_id: appShowId,
      matched_record_id: match.id,
      updated_existing: 1,
      created_new: 0
    };
  }

  const allLookup = await findShowsMatchAnywhere(appShowId);

  if (allLookup.matches.length > 1) {
    logWarn("shows_sync_duplicate_matches_anywhere", {
      app_show_id: appShowId,
      count: allLookup.matches.length,
      record_ids: allLookup.matches.map(r => r.id)
    });
  }

  if (allLookup.match) {
    const match = allLookup.match;
    const current = currentHeartbeatLinkIds(match.fields?.[FIELD_LINK_HEARTBEAT]);

    const updateFields = {
      [FIELD_LINK_HEARTBEAT]: [heartbeatId],
      [FIELD_APP_SHOW_ID]: appShowId,
      [FIELD_APP_SQL_DATE]: appCtx.appSqlDate,
      [FIELD_APP_DOW_RAW]: appCtx.appDowRaw,
      [FIELD_DOW_RAW]: appCtx.dowRaw,
      [FIELD_MODE]: mode,
      [FIELD_SHIFTED_NEXT_DAY]: appCtx.shiftedToNextDay,
    };
    if (appCtx.showAppName !== undefined) updateFields[FIELD_SHOW_NAME_BASE] = appCtx.showAppName;
    if (appCtx.showAppSqlStartDate !== undefined) updateFields[FIELD_SHOW_START_DATE_BASE] = appCtx.showAppSqlStartDate;
    if (appCtx.showAppSqlEndDate !== undefined) updateFields[FIELD_SHOW_END_DATE_BASE] = appCtx.showAppSqlEndDate;

    logWarn("shows_sync_guard_found_match_outside_view", {
      app_show_id: appShowId,
      app_sql_date: appCtx.appSqlDate,
      heartbeat_record_id: heartbeatId,
      matched_record_id: match.id,
      existing_heartbeat_links_count: current.length
    });

    if (!DRY_RUN) {
      await airtableUpdateRecord(TABLE_SHOWS, match.id, updateFields);
    }

    return {
      table: TABLE_SHOWS,
      found_in_view: viewLookup.rows.length,
      matched_show_id: appShowId,
      matched_record_id: match.id,
      updated_existing: 1,
      created_new: 0,
      recovered_from_out_of_view_match: 1
    };
  }

  const createFields = {
    [FIELD_SHOW_ID]: appShowId,
    [FIELD_APP_SHOW_ID]: appShowId,
    [FIELD_LINK_HEARTBEAT]: [heartbeatId],
    [FIELD_NEW_APP_SHOW_ID]: true,
    [FIELD_NEW_APP_SHOW_ID_AT]: heartbeatRecord.fields?.[FIELD_HB_AT] || new Date().toISOString(),
    [FIELD_APP_SQL_DATE]: appCtx.appSqlDate,
    [FIELD_APP_DOW_RAW]: appCtx.appDowRaw,
    [FIELD_DOW_RAW]: appCtx.dowRaw,
    [FIELD_MODE]: mode,
    [FIELD_SHIFTED_NEXT_DAY]: appCtx.shiftedToNextDay,
  };
  if (appCtx.showAppName !== undefined) createFields[FIELD_SHOW_NAME_BASE] = appCtx.showAppName;
  if (appCtx.showAppSqlStartDate !== undefined) createFields[FIELD_SHOW_START_DATE_BASE] = appCtx.showAppSqlStartDate;
  if (appCtx.showAppSqlEndDate !== undefined) createFields[FIELD_SHOW_END_DATE_BASE] = appCtx.showAppSqlEndDate;

  if (LOG_SHOWS_SYNC) {
    logWarn("shows_sync_create_new_record", {
      app_show_id: appShowId,
      app_sql_date: appCtx.appSqlDate,
      heartbeat_record_id: heartbeatId,
      create_fields: createFields
    });
  }

  const createdRecord = DRY_RUN ? null : await airtableCreateRecord(TABLE_SHOWS, createFields);

  return {
    table: TABLE_SHOWS,
    found_in_view: viewLookup.rows.length,
    matched_show_id: appShowId,
    matched_record_id: createdRecord?.id || null,
    updated_existing: 0,
    created_new: 1
  };
}

(async () => {
  try {
    const heartbeatTargetDecision = await resolveHeartbeatTargetDecision();
    const clockCustomerId = heartbeatTargetDecision?.customer_id ?? CUSTOMER_ID;
    const rawClock = {
      ...(await getClockSafe(clockCustomerId)),
      customerId: clockCustomerId,
      showDecision: heartbeatTargetDecision,
    };
    const clk = applyHotpatchClockOverride(applyHeartbeatTargetClockOverride(rawClock, heartbeatTargetDecision));
    const clockMode = deriveModeFromClock(clk);
    const preliminaryShowControl = await fetchShowsModeControl(clk.showId);
    const forcedMode = FORCE_MODE ? normalizeMode(FORCE_MODE) : null;
    const appCtx = await buildAppContext(clk);
    const showControl = preliminaryShowControl?.found
      ? preliminaryShowControl
      : await fetchShowsModeControl(appCtx.appShowId);
    const modeDecision = decideEffectiveMode({
      clockMode,
      forcedMode,
      defaultShowDateGuard: appCtx.defaultShowDateGuard,
      showControl,
    });
    const mode = modeDecision.mode;
    const intervalMin = intervalMinutesForMode(mode);
    appCtx.effectiveMode = mode;
    appCtx.clockMode = clockMode;
    appCtx.modeSource = modeDecision.mode_source;
    appCtx.modeReason = modeDecision.mode_reason;
    appCtx.defaultShowDateStatus = modeDecision.default_show_date_status;
    appCtx.defaultShowDateReason = appCtx.defaultShowDateGuard?.default_show_date_reason || "ok";
    appCtx.showsModeControl = showControl;

    logInfo("run_summary_pre_write", {
      mode,
      clock_mode: clockMode,
      mode_source: modeDecision.mode_source,
      mode_reason: modeDecision.mode_reason,
      forced_mode: forcedMode,
      shows_mode_control: showControl?.mode_control || null,
      shows_mode_control_reason: showControl?.mode_control_reason || null,
      is_default_show_manual_override: !!showControl?.is_default_show_manual_override,
      shows_control_record_id: showControl?.record_id || null,
      source: clk.source,
      raw_show_id: clk.showId ?? null,
      raw_show_date: clk.showDate ?? null,
      raw_sql_date: clk.sqlDate,
      raw_time: clk.time,
      app_show_id: appCtx.appShowId,
      app_sql_date: appCtx.appSqlDate,
      app_dow_raw: appCtx.appDowRaw,
      shifted_to_next_day: appCtx.shiftedToNextDay,
      set_to_default_app_sql_date: appCtx.setToDefaultAppSqlDate,
      default_app_sql_date_is: appCtx.defaultAppSqlDateIs,
      show_app_sql_start_date: appCtx.showAppSqlStartDate,
      show_app_sql_end_date: appCtx.showAppSqlEndDate,
      show_app_name: appCtx.showAppName,
      app_sql_date_source: appCtx.appSqlDateSource,
      candidate_app_sql_date: appCtx.candidateAppSqlDate,
      default_show_date_guard: appCtx.defaultShowDateGuard,
      default_show_date_status: appCtx.defaultShowDateStatus,
      default_show_date_reason: appCtx.defaultShowDateReason,
      interval_min: intervalMin
    });

    const heartbeatRecord = await createHeartbeat(clk, mode, intervalMin, appCtx);
    persistLatestHeartbeatContext(heartbeatRecord, appCtx, mode);

    const results = [];
    const warnings = [];

    let showsSyncResult = null;
    try {
      showsSyncResult = await syncShowsHeartbeat(heartbeatRecord, appCtx, mode);
      results.push(showsSyncResult);
    } catch (e) {
      const msg = String(e?.message || e).slice(0, 240);
      warnings.push(`shows: ${msg}`);
      logError("shows_sync_failed", {
        heartbeat_record_id: heartbeatRecord?.id || null,
        app_show_id: appCtx.appShowId,
        app_sql_date: appCtx.appSqlDate,
        error_message: msg
      });
    }

    for (const tableName of [
      TABLE_WATCH_SCHEDULE,
      TABLE_WATCH_TRIPS,
      TABLE_SCHEDULER,
      TABLE_ACTIVE_TENANTS,
      TABLE_ACTIVE_ALERTS,
      TABLE_PUBLISH_QUEUE,
      TABLE_WATCH_RINGS
    ]) {
      try {
        results.push(await relinkHeartbeatView(tableName, heartbeatRecord.id, appCtx));
      } catch (e) {
        const msg = String(e?.message || e).slice(0, 240);
        warnings.push(`${tableName}: ${msg}`);
        logError("relink_failed", {
          table: tableName,
          heartbeat_record_id: heartbeatRecord?.id || null,
          error_message: msg
        });
      }
    }

    console.log(JSON.stringify({
      ok: true,
      heartbeat_record_id: heartbeatRecord.id,
      heartbeat_app_show_id: appCtx.appShowId,
      heartbeat_app_sql_date: appCtx.appSqlDate,
      heartbeat_app_dow_raw: appCtx.appDowRaw,
      default_show_date_guard: appCtx.defaultShowDateGuard,
      heartbeat_scope_key: [
        appCtx.appShowId ?? "",
        appCtx.appSqlDate ?? "",
        appCtx.appDowRaw ?? "",
        appCtx.shiftedToNextDay ? "1" : "0",
      ].join("|"),
      mode,
      clock_mode: appCtx.clockMode,
      mode_source: appCtx.modeSource,
      mode_reason: appCtx.modeReason,
      default_show_date_status: appCtx.defaultShowDateStatus,
      default_show_date_reason: appCtx.defaultShowDateReason,
      source: clk.source,
      results,
      warnings
    }, null, 2));

    process.exit(0);
  } catch (e) {
    const name = e?.name || "error";
    const msg = String(e?.message || e);
    logError("fatal", {
      error_name: name,
      error_message: msg.slice(0, 240)
    });
    process.exit(1);
  }
})();
