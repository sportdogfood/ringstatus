// tagger.js (FULL DROP)
// 3 modes only: DAY / NIGHT / OVERNIGHT
// - raw clock always comes from endpoint, or system time with a bounded last-known show fallback
// - mode logic is based only on app_time as provided by the endpoint/system fallback
// - DAY:        app_show_id = raw show_id, app_sql_date = raw sql_date, shifted_to_next_day = false
// - NIGHT:      app_show_id = raw show_id, app_sql_date = next day,   shifted_to_next_day = true
// - OVERNIGHT:  app_show_id = raw show_id, app_sql_date = raw sql_date, shifted_to_next_day = false
// - shows table: match by show_id/app_show_id or create if missing
// - shows table heartbeat link is overwritten to latest heartbeat only
// - new_app_show_id is checked only when a new show row is created

const fs = require("fs");
const path = require("path");

const AIRTABLE_TOKEN   = process.env.AIRTABLE_TOKEN || "";
const AIRTABLE_BASE_ID = process.env.AIRTABLE_BASE_ID || "";
const CUSTOMER_ID      = Number(process.env.CUSTOMER_ID || "15");

if (!AIRTABLE_TOKEN) throw new Error("Missing AIRTABLE_TOKEN");
if (!AIRTABLE_BASE_ID) throw new Error("Missing AIRTABLE_BASE_ID");

const TABLE_HEARTBEAT      = process.env.TABLE_HEARTBEAT || "heartbeat";
const TABLE_SHOWS          = process.env.TABLE_SHOWS || "shows";
const TABLE_WATCH_SCHEDULE = process.env.TABLE_WATCH_SCHEDULE || "watch_schedule";
const TABLE_WATCH_TRIPS    = process.env.TABLE_WATCH_TRIPS || "watch_trips";
const TABLE_SCHEDULER      = process.env.TABLE_SCHEDULER || "scheduler";
const TABLE_ACTIVE_TENANTS = process.env.TABLE_ACTIVE_TENANTS || "active_tenants";
const TABLE_ACTIVE_ALERTS  = process.env.TABLE_ACTIVE_ALERTS || "active_alerts";
const TABLE_WATCH_RINGS    = process.env.TABLE_WATCH_RINGS || "watch_rings";

const VIEW_HEARTBEAT = process.env.VIEW_HEARTBEAT || "heartbeat";

const RING_ENDPOINT = `https://broad-tooth-b8ed.gombcg.workers.dev/ring?customer_id=${encodeURIComponent(CUSTOMER_ID)}`;

const FIELD_LINK_HEARTBEAT     = process.env.FIELD_LINK_HEARTBEAT || "heartbeat";
const FIELD_SHOW_ID            = process.env.FIELD_SHOW_ID || "show_id";
const FIELD_NEW_APP_SHOW_ID    = process.env.FIELD_NEW_APP_SHOW_ID || "new_app_show_id";
const FIELD_NEW_APP_SHOW_ID_AT = process.env.FIELD_NEW_APP_SHOW_ID_AT || "new_app_show_id_at";

const FIELD_MODE             = process.env.FIELD_MODE || "mode";
const FIELD_EPOCH            = process.env.FIELD_EPOCH || "epoch";
const FIELD_HB_DURATION      = process.env.FIELD_HB_DURATION || "hb_duration";
const FIELD_INTERVAL         = process.env.FIELD_INTERVAL || "interval";
const FIELD_HB_AT            = process.env.FIELD_HB_AT || "hb_at";

const FIELD_APP_SHOW_ID      = process.env.FIELD_APP_SHOW_ID || "app_show_id";
const FIELD_APP_SQL_DATE     = process.env.FIELD_APP_SQL_DATE || "app_sql_date";
const FIELD_APP_DOW_RAW      = process.env.FIELD_APP_DOW_RAW || "app_dow_raw";
const FIELD_DOW_RAW          = process.env.FIELD_DOW_RAW || "dow_raw";
const FIELD_SHIFTED_NEXT_DAY = process.env.FIELD_SHIFTED_NEXT_DAY || "shifted_to_next_day";

const HEARTBEAT_ID_FIELD   = process.env.HEARTBEAT_ID_FIELD || "heartbeat_id";
const HEARTBEAT_SHOW_ID    = process.env.HEARTBEAT_SHOW_ID || "show_id";
const HEARTBEAT_SHOW_DATE  = process.env.HEARTBEAT_SHOW_DATE || "show_date";
const HEARTBEAT_SQL_DATE   = process.env.HEARTBEAT_SQL_DATE || "sql_date";
const HEARTBEAT_TIME       = process.env.HEARTBEAT_TIME || "time";

const DAY_INTERVAL_MIN       = Number(process.env.DAY_INTERVAL_MIN || "6");
const NIGHT_INTERVAL_MIN     = Number(process.env.NIGHT_INTERVAL_MIN || "120");
const OVERNIGHT_INTERVAL_MIN = Number(process.env.OVERNIGHT_INTERVAL_MIN || "99999");

const HTTP_TIMEOUT_MS   = Number(process.env.HTTP_TIMEOUT_MS || "20000");
const AT_RETRY_ATTEMPTS = Number(process.env.AT_RETRY_ATTEMPTS || "3");
const AT_RETRY_BASE_MS  = Number(process.env.AT_RETRY_BASE_MS || "400");
const AT_RETRY_MAX_MS   = Number(process.env.AT_RETRY_MAX_MS || "2000");
const LAST_KNOWN_CLOCK_MAX_AGE_MIN = Math.max(1, Number(process.env.LAST_KNOWN_CLOCK_MAX_AGE_MIN || "360") || 360);
const LAST_KNOWN_HEARTBEAT_LOOKBACK = Math.max(1, Number(process.env.LAST_KNOWN_HEARTBEAT_LOOKBACK || "100") || 100);
const FORCE_MODE        = String(process.env.FORCE_MODE || "").trim().toUpperCase();
const DRY_RUN           = String(process.env.DRY_RUN || "0") === "1";
const HB_TZ             = process.env.HB_TIMEZONE || "America/New_York";
const TAGGER_STATE_PATH = path.resolve(__dirname, process.env.TAGGER_STATE_FILE || "tagger_runtime_state.json");

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

function toFiniteNumber(v) {
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
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

function normalizeMode(v) {
  const s = String(v ?? "").trim().toUpperCase();
  if (s === "DAY" || s === "NIGHT" || s === "OVERNIGHT") return s;
  return "DAY";
}

function resolveModeFromClock(clock) {
  if (FORCE_MODE) return normalizeMode(FORCE_MODE);

  const minuteOfDay = minuteOfDayFromMs(clock.nowMs, HB_TZ);

  if (minuteOfDay >= 300 && minuteOfDay <= 1019) return "DAY";        // 5:00 AM - 4:59 PM
  if (minuteOfDay >= 1020 && minuteOfDay <= 1439) return "NIGHT";     // 5:00 PM - 11:59 PM
  return "OVERNIGHT";                                                  // 12:00 AM - 4:59 AM
}

function intervalMinutesForMode(mode) {
  if (mode === "DAY") return DAY_INTERVAL_MIN;
  if (mode === "NIGHT") return NIGHT_INTERVAL_MIN;
  return OVERNIGHT_INTERVAL_MIN;
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

async function getClockSafe() {
  const systemClock = buildFallbackClock();

  try {
    const res = await fetchWithTimeout(RING_ENDPOINT, { method: "GET" });
    const txt = await res.text();

    if (!res.ok) {
      return await fallbackToBestKnownClock(systemClock, "endpoint_fallback_http", {
        endpoint: RING_ENDPOINT,
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
        endpoint: RING_ENDPOINT,
        body_sample: txt.slice(0, 250),
        system_sql_date: systemClock.sqlDate,
        system_time: systemClock.time
      });
    }

    const endpointClock = pickClockFromPayload(payload);
    if (!endpointClock) {
      return await fallbackToBestKnownClock(systemClock, "endpoint_fallback_missing_clock_values", {
        endpoint: RING_ENDPOINT,
        system_sql_date: systemClock.sqlDate,
        system_time: systemClock.time
      });
    }

    const systemSqlDate = systemClock.sqlDate;
    if (String(endpointClock.sqlDate || "") !== String(systemSqlDate || "")) {
      return await fallbackToBestKnownClock(systemClock, "endpoint_fallback_sql_date_mismatch", {
        endpoint: RING_ENDPOINT,
        endpoint_show_id: endpointClock.showId,
        endpoint_show_date: endpointClock.showDate,
        endpoint_sql_date: endpointClock.sqlDate,
        endpoint_time: endpointClock.time,
        system_sql_date: systemSqlDate,
        system_time: systemClock.time
      });
    }

    persistLastKnownClock(endpointClock, "endpoint", {
      endpoint: RING_ENDPOINT
    });

    if (LOG_ACCEPTED_ENDPOINT) {
      logInfo("endpoint_clock_accepted", {
        endpoint: RING_ENDPOINT,
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
      endpoint: RING_ENDPOINT,
      error_name: e?.name || null,
      error_message: String(e?.message || e).slice(0, 240),
      system_sql_date: systemClock.sqlDate,
      system_time: systemClock.time
    });
  }
}

async function buildAppContext(clock, mode) {
  const dowRaw = dowName(dayOfWeekUtc(clock.sqlDate));

  let appShowId = clock.showId ?? null;
  let appSqlDate = clock.sqlDate;
  let shiftedToNextDay = false;

  if (mode === "DAY") {
    appShowId = clock.showId ?? null;
    appSqlDate = clock.sqlDate;
    shiftedToNextDay = false;
  } else if (mode === "NIGHT") {
    appShowId = clock.showId ?? null;
    appSqlDate = addDaysSql(clock.sqlDate, 1) || clock.sqlDate;
    shiftedToNextDay = true;
  } else if (mode === "OVERNIGHT") {
    appShowId = clock.showId ?? null;
    appSqlDate = clock.sqlDate;
    shiftedToNextDay = false;
  }

  const appDowRaw = dowName(dayOfWeekUtc(appSqlDate));

  const appCtx = {
    effectiveMode: mode,
    dowRaw,
    appShowId,
    appSqlDate,
    appDowRaw,
    shiftedToNextDay,
  };

  if (LOG_TRANSITIONS) {
    logInfo("app_context_computed", {
      source: clock.source,
      mode,
      raw_show_id: clock.showId ?? null,
      raw_show_date: clock.showDate ?? null,
      raw_sql_date: clock.sqlDate,
      raw_time: clock.time,
      app_show_id: appShowId,
      app_sql_date: appSqlDate,
      shifted_to_next_day: shiftedToNextDay
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
  };

  if (DRY_RUN) {
    logInfo("heartbeat_create_dry_run", {
      heartbeat_id: fields[HEARTBEAT_ID_FIELD],
      raw_show_id: fields[HEARTBEAT_SHOW_ID],
      raw_sql_date: fields[HEARTBEAT_SQL_DATE],
      app_show_id: fields[FIELD_APP_SHOW_ID],
      app_sql_date: fields[FIELD_APP_SQL_DATE],
      mode
    });
    return { id: "recDRYRUNHEARTBEAT", fields };
  }

  return await airtableCreateRecord(TABLE_HEARTBEAT, fields);
}

function currentHeartbeatLinkIds(v) {
  if (!Array.isArray(v)) return [];
  return v.map(x => typeof x === "string" ? x : x?.id).filter(Boolean);
}

async function relinkHeartbeatView(tableName, heartbeatId) {
  const rows = await airtableListAll({
    table: tableName,
    view: VIEW_HEARTBEAT,
    fields: [FIELD_LINK_HEARTBEAT]
  });

  const updates = [];

  for (const r of rows) {
    const current = currentHeartbeatLinkIds(r.fields?.[FIELD_LINK_HEARTBEAT]);
    const alreadyCorrect = current.length === 1 && current[0] === heartbeatId;
    if (alreadyCorrect) continue;

    updates.push({
      id: r.id,
      fields: {
        [FIELD_LINK_HEARTBEAT]: [heartbeatId]
      }
    });
  }

  if (!DRY_RUN && updates.length) {
    await airtableBatchUpdate(tableName, updates);
  }

  const summary = {
    table: tableName,
    found_in_view: rows.length,
    relinked: updates.length
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
    fields: [FIELD_SHOW_ID, FIELD_APP_SHOW_ID, FIELD_LINK_HEARTBEAT]
  });

  const matches = rows.filter(r => matchesShowRow(r.fields || {}, appShowId));

  return {
    rows,
    matches,
    match: matches[0] || null
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

  if (LOG_SHOWS_SYNC) {
    logWarn("shows_sync_create_new_record", {
      app_show_id: appShowId,
      app_sql_date: appCtx.appSqlDate,
      heartbeat_record_id: heartbeatId,
      create_fields: createFields
    });
  }

  if (!DRY_RUN) {
    await airtableCreateRecord(TABLE_SHOWS, createFields);
  }

  return {
    table: TABLE_SHOWS,
    found_in_view: viewLookup.rows.length,
    matched_show_id: appShowId,
    updated_existing: 0,
    created_new: 1
  };
}

(async () => {
  try {
    const clk = await getClockSafe();
    const mode = resolveModeFromClock(clk);
    const appCtx = await buildAppContext(clk, mode);
    const intervalMin = intervalMinutesForMode(mode);

    logInfo("run_summary_pre_write", {
      mode,
      source: clk.source,
      raw_show_id: clk.showId ?? null,
      raw_show_date: clk.showDate ?? null,
      raw_sql_date: clk.sqlDate,
      raw_time: clk.time,
      app_show_id: appCtx.appShowId,
      app_sql_date: appCtx.appSqlDate,
      shifted_to_next_day: appCtx.shiftedToNextDay,
      interval_min: intervalMin
    });

    const heartbeatRecord = await createHeartbeat(clk, mode, intervalMin, appCtx);

    const results = [];
    const warnings = [];

    try {
      results.push(await syncShowsHeartbeat(heartbeatRecord, appCtx, mode));
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
      TABLE_WATCH_TRIPS,
      TABLE_WATCH_SCHEDULE,
      TABLE_SCHEDULER,
      TABLE_ACTIVE_TENANTS,
      TABLE_ACTIVE_ALERTS,
      TABLE_WATCH_RINGS
    ]) {
      try {
        results.push(await relinkHeartbeatView(tableName, heartbeatRecord.id));
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
      mode,
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
