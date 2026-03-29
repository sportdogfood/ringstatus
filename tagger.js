// tagger.js (CLEAN HEARTBEAT + RELINK + OVERNIGHT MODE)
// light proactive logging + small shows guardrails
// intended to help diagnose endpoint/show conflicts without overhauling core behavior

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

const VIEW_HEARTBEAT   = process.env.VIEW_HEARTBEAT || "heartbeat";
const VIEW_SHOWS_EPOCH = process.env.VIEW_SHOWS_EPOCH || "epoch";

const RING_ENDPOINT = `https://broad-tooth-b8ed.gombcg.workers.dev/ring?customer_id=${encodeURIComponent(CUSTOMER_ID)}`;

const FIELD_LINK_HEARTBEAT     = process.env.FIELD_LINK_HEARTBEAT || "heartbeat";
const FIELD_SHOW_ID            = process.env.FIELD_SHOW_ID || "show_id";
const FIELD_NEW_APP_SHOW_ID    = process.env.FIELD_NEW_APP_SHOW_ID || "new_app_show_id";
const FIELD_NEW_APP_SHOW_ID_AT = process.env.FIELD_NEW_APP_SHOW_ID_AT || "new_app_show_id_at";

const FIELD_MODE              = process.env.FIELD_MODE || "mode";
const FIELD_EPOCH             = process.env.FIELD_EPOCH || "epoch";
const FIELD_HB_DURATION       = process.env.FIELD_HB_DURATION || "hb_duration";
const FIELD_INTERVAL          = process.env.FIELD_INTERVAL || "interval";
const FIELD_HB_AT             = process.env.FIELD_HB_AT || "hb_at";

const FIELD_APP_SHOW_ID       = process.env.FIELD_APP_SHOW_ID || "app_show_id";
const FIELD_APP_SQL_DATE      = process.env.FIELD_APP_SQL_DATE || "app_sql_date";
const FIELD_APP_DOW_RAW       = process.env.FIELD_APP_DOW_RAW || "app_dow_raw";
const FIELD_DOW_RAW           = process.env.FIELD_DOW_RAW || "dow_raw";
const FIELD_SHIFTED_NEXT_DAY  = process.env.FIELD_SHIFTED_NEXT_DAY || "shifted_to_next_day";
const FIELD_HELDOVER_SUNDAY   = process.env.FIELD_HELDOVER_SUNDAY || "heldover_from_sunday";

const HEARTBEAT_ID_FIELD      = process.env.HEARTBEAT_ID_FIELD || "heartbeat_id";
const HEARTBEAT_SHOW_ID       = process.env.HEARTBEAT_SHOW_ID || "show_id";
const HEARTBEAT_SHOW_DATE     = process.env.HEARTBEAT_SHOW_DATE || "show_date";
const HEARTBEAT_SQL_DATE      = process.env.HEARTBEAT_SQL_DATE || "sql_date";
const HEARTBEAT_TIME          = process.env.HEARTBEAT_TIME || "time";

const FIELD_LAST_SUNDAY_SHOW_ID  = process.env.FIELD_LAST_SUNDAY_SHOW_ID || "last_sunday_show_id";
const FIELD_LAST_SUNDAY_SQL_DATE = process.env.FIELD_LAST_SUNDAY_SQL_DATE || "last_sunday_sql_date";
const FIELD_CUSTOMER_ID          = process.env.FIELD_CUSTOMER_ID || "customer_id";

const DAY_INTERVAL_MIN       = Number(process.env.DAY_INTERVAL_MIN || "6");
const NIGHT_INTERVAL_MIN     = Number(process.env.NIGHT_INTERVAL_MIN || "120");
const HOLDOVER_INTERVAL_MIN  = Number(process.env.HOLDOVER_INTERVAL_MIN || "99999");
const OVERNIGHT_INTERVAL_MIN = Number(process.env.OVERNIGHT_INTERVAL_MIN || String(HOLDOVER_INTERVAL_MIN));

const HTTP_TIMEOUT_MS       = Number(process.env.HTTP_TIMEOUT_MS || "20000");
const AT_RETRY_ATTEMPTS     = Number(process.env.AT_RETRY_ATTEMPTS || "3");
const AT_RETRY_BASE_MS      = Number(process.env.AT_RETRY_BASE_MS || "400");
const AT_RETRY_MAX_MS       = Number(process.env.AT_RETRY_MAX_MS || "2000");
const FORCE_MODE            = String(process.env.FORCE_MODE || "").trim().toUpperCase();
const DRY_RUN               = String(process.env.DRY_RUN || "0") === "1";
const HB_TZ                 = process.env.HB_TIMEZONE || "America/New_York";

// light logging controls
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
  return d.getUTCDay(); // 0 Sun .. 6 Sat
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
  if (s === "DAY" || s === "NIGHT" || s === "HOLDOVER" || s === "OVERNIGHT") return s;
  return "HOLDOVER";
}

function resolveModeFromClock(clock) {
  if (FORCE_MODE) return normalizeMode(FORCE_MODE);

  const sqlDate = String(clock?.sqlDate || "").trim();
  const dow = dayOfWeekUtc(sqlDate);
  const minuteOfDay = minuteOfDayFromMs(clock.nowMs, HB_TZ);

  // Sunday
  if (dow === 0) {
    if (minuteOfDay >= 300 && minuteOfDay <= 1019) return "DAY";      // 5:00 AM - 4:59 PM
    return "HOLDOVER";                                                // all other Sunday times
  }

  // Monday
  if (dow === 1) {
    return "HOLDOVER";
  }

  // Tuesday - Saturday
  if (dow >= 2 && dow <= 6) {
    if (minuteOfDay >= 300 && minuteOfDay <= 1019) return "DAY";       // 5:00 AM - 4:59 PM
    if (minuteOfDay >= 1020 && minuteOfDay <= 1319) return "NIGHT";    // 5:00 PM - 9:59 PM
    return "OVERNIGHT";                                                 // 10:00 PM - 4:59 AM
  }

  return "HOLDOVER";
}

function intervalMinutesForMode(mode) {
  if (mode === "DAY") return DAY_INTERVAL_MIN;
  if (mode === "NIGHT") return NIGHT_INTERVAL_MIN;
  if (mode === "OVERNIGHT") return OVERNIGHT_INTERVAL_MIN;
  return HOLDOVER_INTERVAL_MIN;
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
      logWarn("endpoint_fallback_http", {
        endpoint: RING_ENDPOINT,
        http_status: res.status,
        system_sql_date: systemClock.sqlDate,
        system_time: systemClock.time
      });
      return systemClock;
    }

    let payload = null;
    try {
      payload = JSON.parse(txt);
    } catch {
      logWarn("endpoint_fallback_invalid_json", {
        endpoint: RING_ENDPOINT,
        body_sample: txt.slice(0, 250),
        system_sql_date: systemClock.sqlDate,
        system_time: systemClock.time
      });
      return systemClock;
    }

    const endpointClock = pickClockFromPayload(payload);
    if (!endpointClock) {
      logWarn("endpoint_fallback_missing_clock_values", {
        endpoint: RING_ENDPOINT,
        system_sql_date: systemClock.sqlDate,
        system_time: systemClock.time
      });
      return systemClock;
    }

    const systemSqlDate = systemClock.sqlDate;
    if (String(endpointClock.sqlDate || "") !== String(systemSqlDate || "")) {
      logWarn("endpoint_fallback_sql_date_mismatch", {
        endpoint: RING_ENDPOINT,
        endpoint_show_id: endpointClock.showId,
        endpoint_show_date: endpointClock.showDate,
        endpoint_sql_date: endpointClock.sqlDate,
        endpoint_time: endpointClock.time,
        system_sql_date: systemSqlDate,
        system_time: systemClock.time
      });
      return systemClock;
    }

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
    logWarn("endpoint_fallback_fetch_error", {
      endpoint: RING_ENDPOINT,
      error_name: e?.name || null,
      error_message: String(e?.message || e).slice(0, 240),
      system_sql_date: systemClock.sqlDate,
      system_time: systemClock.time
    });
    return systemClock;
  }
}

async function getLatestLastSundayForCustomer(customerId) {
  const rows = await airtableListAll({
    table: TABLE_SHOWS,
    view: VIEW_SHOWS_EPOCH,
    fields: [FIELD_CUSTOMER_ID, FIELD_LAST_SUNDAY_SHOW_ID, FIELD_LAST_SUNDAY_SQL_DATE]
  });

  let best = null;
  let bestKey = -1;

  for (const r of rows) {
    const f = r.fields || {};
    if (String(f[FIELD_CUSTOMER_ID] ?? "") !== String(customerId)) continue;

    const sid = f[FIELD_LAST_SUNDAY_SHOW_ID];
    const sdt = String(f[FIELD_LAST_SUNDAY_SQL_DATE] || "").trim();

    if (sid === null || sid === undefined || String(sid) === "") continue;
    if (!/^\d{4}-\d{2}-\d{2}$/.test(sdt)) continue;

    const k = Number(sdt.replaceAll("-", ""));
    if (Number.isFinite(k) && k > bestKey) {
      bestKey = k;
      best = {
        lastSundayShowId: sid,
        lastSundaySqlDate: sdt,
      };
    }
  }

  return best;
}

async function buildAppContext(clock, mode) {
  const originalMode = mode;
  const dowRaw = dowName(dayOfWeekUtc(clock.sqlDate));
  let effectiveMode = mode;

  let appShowId = clock.showId ?? null;
  let appSqlDate = clock.sqlDate;
  let shiftedToNextDay = false;
  let heldoverFromSunday = false;

  if (mode === "NIGHT") {
    appSqlDate = addDaysSql(clock.sqlDate, 1) || clock.sqlDate;
    shiftedToNextDay = true;
  } else if (mode === "HOLDOVER") {
    const best = await getLatestLastSundayForCustomer(CUSTOMER_ID);
    if (!best) {
      effectiveMode = "DAY";
      appShowId = clock.showId ?? null;
      appSqlDate = clock.sqlDate;
    } else {
      appShowId = best.lastSundayShowId;
      appSqlDate = best.lastSundaySqlDate;
      heldoverFromSunday = true;
    }
  }

  const appDowRaw = dowName(dayOfWeekUtc(appSqlDate));

  const appCtx = {
    effectiveMode,
    dowRaw,
    appShowId,
    appSqlDate,
    appDowRaw,
    shiftedToNextDay,
    heldoverFromSunday,
  };

  if (LOG_TRANSITIONS) {
    logInfo("app_context_computed", {
      source: clock.source,
      original_mode: originalMode,
      effective_mode: effectiveMode,
      raw_show_id: clock.showId ?? null,
      raw_show_date: clock.showDate ?? null,
      raw_sql_date: clock.sqlDate,
      raw_time: clock.time,
      app_show_id: appShowId,
      app_sql_date: appSqlDate,
      shifted_to_next_day: shiftedToNextDay,
      heldover_from_sunday: heldoverFromSunday
    });
  }

  if (shiftedToNextDay === true && String(appSqlDate) === String(clock.sqlDate)) {
    logWarn("app_context_conflict_shifted_true_same_date", {
      mode: effectiveMode,
      raw_sql_date: clock.sqlDate,
      app_sql_date: appSqlDate,
      raw_show_id: clock.showId ?? null,
      app_show_id: appShowId
    });
  }

  if (shiftedToNextDay === false && String(appSqlDate) !== String(clock.sqlDate) && !heldoverFromSunday) {
    logWarn("app_context_conflict_shifted_false_different_date", {
      mode: effectiveMode,
      raw_sql_date: clock.sqlDate,
      app_sql_date: appSqlDate,
      raw_show_id: clock.showId ?? null,
      app_show_id: appShowId
    });
  }

  return appCtx;
}

  if (LOG_TRANSITIONS) {
    logInfo("app_context_computed", {
      source: clock.source,
      original_mode: originalMode,
      effective_mode: effectiveMode,
      raw_show_id: clock.showId ?? null,
      raw_show_date: clock.showDate ?? null,
      raw_sql_date: clock.sqlDate,
      raw_time: clock.time,
      app_show_id: appShowId,
      app_sql_date: appSqlDate,
      shifted_to_next_day: shiftedToNextDay,
      heldover_from_sunday: heldoverFromSunday
    });
  }

  if (shiftedToNextDay === true && String(appSqlDate) === String(clock.sqlDate)) {
    logWarn("app_context_conflict_shifted_true_same_date", {
      mode: effectiveMode,
      raw_sql_date: clock.sqlDate,
      app_sql_date: appSqlDate,
      raw_show_id: clock.showId ?? null,
      app_show_id: appShowId
    });
  }

  if (shiftedToNextDay === false && String(appSqlDate) !== String(clock.sqlDate) && !heldoverFromSunday) {
    logWarn("app_context_conflict_shifted_false_different_date", {
      mode: effectiveMode,
      raw_sql_date: clock.sqlDate,
      app_sql_date: appSqlDate,
      raw_show_id: clock.showId ?? null,
      app_show_id: appShowId
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
    [FIELD_HELDOVER_SUNDAY]: appCtx.heldoverFromSunday,
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

async function findShowsMatchInView(appShowId) {
  const rows = await airtableListAll({
    table: TABLE_SHOWS,
    view: VIEW_HEARTBEAT,
    fields: [FIELD_SHOW_ID, FIELD_LINK_HEARTBEAT]
  });

  const normalized = String(appShowId).trim();
  const matches = rows.filter(r => String(r.fields?.[FIELD_SHOW_ID] ?? "").trim() === normalized);

  return {
    rows,
    matches,
    match: matches[0] || null
  };
}

async function findShowsMatchAnywhere(appShowId) {
  const rows = await airtableListAll({
    table: TABLE_SHOWS,
    fields: [FIELD_SHOW_ID, FIELD_LINK_HEARTBEAT]
  });

  const normalized = String(appShowId).trim();
  const matches = rows.filter(r => String(r.fields?.[FIELD_SHOW_ID] ?? "").trim() === normalized);

  return {
    rows,
    matches,
    match: matches[0] || null
  };
}

async function syncShowsHeartbeat(heartbeatRecord, appCtx) {
  const heartbeatId = heartbeatRecord.id;
  const appShowId = appCtx.appShowId;

  if (appShowId === null || appShowId === undefined || String(appShowId).trim() === "") {
    if (LOG_SHOWS_SYNC) {
      logWarn("shows_sync_skipped_missing_app_show_id", {
        heartbeat_record_id: heartbeatId,
        app_show_id: appShowId,
        app_sql_date: appCtx.appSqlDate,
        shifted_to_next_day: appCtx.shiftedToNextDay,
        heldover_from_sunday: appCtx.heldoverFromSunday
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
    const alreadyCorrect = current.length === 1 && current[0] === heartbeatId;

    if (LOG_SHOWS_SYNC) {
      logInfo("shows_sync_update_existing_in_view", {
        app_show_id: appShowId,
        app_sql_date: appCtx.appSqlDate,
        heartbeat_record_id: heartbeatId,
        matched_record_id: match.id,
        existing_heartbeat_links: current,
        already_correct: alreadyCorrect,
        found_in_view: viewLookup.rows.length
      });
    }

    if (!alreadyCorrect && !DRY_RUN) {
      await airtableUpdateRecord(TABLE_SHOWS, match.id, {
        [FIELD_LINK_HEARTBEAT]: [heartbeatId]
      });
    }

    return {
      table: TABLE_SHOWS,
      found_in_view: viewLookup.rows.length,
      matched_show_id: appShowId,
      updated_existing: alreadyCorrect ? 0 : 1,
      created_new: 0
    };
  }

  // guardrail: if not found in view, check entire table before creating a new row
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
    const alreadyCorrect = current.length === 1 && current[0] === heartbeatId;

    logWarn("shows_sync_guard_found_match_outside_view", {
      app_show_id: appShowId,
      app_sql_date: appCtx.appSqlDate,
      heartbeat_record_id: heartbeatId,
      matched_record_id: match.id,
      existing_heartbeat_links: current,
      found_in_view: viewLookup.rows.length,
      found_anywhere: allLookup.rows.length
    });

    if (!alreadyCorrect && !DRY_RUN) {
      await airtableUpdateRecord(TABLE_SHOWS, match.id, {
        [FIELD_LINK_HEARTBEAT]: [heartbeatId]
      });
    }

    return {
      table: TABLE_SHOWS,
      found_in_view: viewLookup.rows.length,
      matched_show_id: appShowId,
      updated_existing: alreadyCorrect ? 0 : 1,
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
    [FIELD_MODE]: heartbeatRecord.fields?.[FIELD_MODE] ?? null,
    [FIELD_SHIFTED_NEXT_DAY]: appCtx.shiftedToNextDay,
    [FIELD_HELDOVER_SUNDAY]: appCtx.heldoverFromSunday
  };

  if (LOG_SHOWS_SYNC) {
    logWarn("shows_sync_create_new_record", {
      app_show_id: appShowId,
      app_sql_date: appCtx.appSqlDate,
      heartbeat_record_id: heartbeatId,
      found_in_view: viewLookup.rows.length,
      found_anywhere: allLookup.rows.length,
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
    let mode = resolveModeFromClock(clk);
    const appCtx = await buildAppContext(clk, mode);
    mode = appCtx.effectiveMode;
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
      heldover_from_sunday: appCtx.heldoverFromSunday,
      interval_min: intervalMin
    });

    const heartbeatRecord = await createHeartbeat(clk, mode, intervalMin, appCtx);

    const results = [];
    const warnings = [];

    try {
      results.push(await syncShowsHeartbeat(heartbeatRecord, appCtx));
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