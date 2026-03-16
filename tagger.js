// tagger.js (FULL DROP)

const fs = require("fs");
const path = require("path");

const AIRTABLE_TOKEN   = process.env.AIRTABLE_TOKEN || "";
const AIRTABLE_BASE_ID = process.env.AIRTABLE_BASE_ID || "";
const CUSTOMER_ID      = Number(process.env.CUSTOMER_ID || "15");

const TABLE_SHOWS          = process.env.TABLE_SHOWS || "shows";
const TABLE_PUBLISH_QUEUE  = process.env.TABLE_PUBLISH_QUEUE || "publish_queue";
const TABLE_SCHEDULE       = process.env.TABLE_SCHEDULE || "watch_schedule";
const TABLE_TRIPS          = process.env.TABLE_TRIPS || "watch_trips";
const TABLE_SCHEDULER      = process.env.TABLE_SCHEDULER || "scheduler";
const TABLE_ACTIVE_TENANTS = process.env.TABLE_ACTIVE_TENANTS || "active_tenants";
const TABLE_HEARTBEAT      = process.env.TABLE_HEARTBEAT || "heartbeat";

const VIEW_SHOWS          = process.env.VIEW_SHOWS || "epoch";
const VIEW_PUBLISH_QUEUE  = process.env.VIEW_PUBLISH_QUEUE || "epoch";
const VIEW_SCHEDULE       = process.env.VIEW_SCHEDULE || "epoch";
const VIEW_TRIPS          = process.env.VIEW_TRIPS || "epoch";
const VIEW_SCHEDULER      = process.env.VIEW_SCHEDULER || "epoch";
const VIEW_ACTIVE_TENANTS = process.env.VIEW_ACTIVE_TENANTS || "epoch";

const RING_ENDPOINT = `https://broad-tooth-b8ed.gombcg.workers.dev/ring?customer_id=${encodeURIComponent(CUSTOMER_ID)}`;

const FIELD_MODE        = process.env.FIELD_MODE || "mode";
const FIELD_EPOCH       = process.env.FIELD_EPOCH || "epoch";
const FIELD_TEMP        = process.env.FIELD_TEMP || "temp";
const FIELD_BUCKET      = process.env.FIELD_BUCKET || "bucket";
const FIELD_NEXT_DUE    = process.env.FIELD_NEXT_DUE || "next_due_epoch";
const FIELD_FIRST_PRINT = process.env.FIELD_FIRST_PRINT || "first_print";
const FIELD_HB_DURATION = process.env.FIELD_HB_DURATION || "hb_duration";
const FIELD_INTERVAL    = process.env.FIELD_INTERVAL || "interval";
const FIELD_HB_AT       = process.env.FIELD_HB_AT || "hb_at";
const FIELD_HB_TIME     = process.env.FIELD_HB_TIME || "hb_time";

const HEARTBEAT_ID_FIELD  = process.env.HEARTBEAT_ID_FIELD || "heartbeat_id";
const HEARTBEAT_SHOW_ID   = process.env.HEARTBEAT_SHOW_ID || "show_id";
const HEARTBEAT_SHOW_DATE = process.env.HEARTBEAT_SHOW_DATE || "show_date";
const HEARTBEAT_SQL_DATE  = process.env.HEARTBEAT_SQL_DATE || "sql_date";
const HEARTBEAT_TIME      = process.env.HEARTBEAT_TIME || "time";

const SCHED_SHOW_DATE   = process.env.SCHED_SHOW_DATE || "show_date";
const SCHED_TIME_LATEST = process.env.SCHED_TIME_LATEST || "latest_estimated_start_time";
const SCHED_TIME_BASE   = process.env.SCHED_TIME_BASE || "estimated_start_time";
const SCHED_STATUS      = process.env.SCHED_STATUS || "latestStatus";

const TRIP_DT          = process.env.TRIP_DT || "dt";
const TRIP_GO_LATEST   = process.env.TRIP_GO_LATEST || "latest_estimated_go_time";
const TRIP_GO_BASE     = process.env.TRIP_GO_BASE || "estimated_go_time";
const TRIP_START_FALLB = process.env.TRIP_START_FALLB || "estimated_start_time";
const TRIP_STATUS      = process.env.TRIP_STATUS || "latestStatus";
const TRIP_GONEIN      = process.env.TRIP_GONEIN || "lastGonein";

const DAY_SECOND_PASS_DELAY_SEC = Number(process.env.DAY_SECOND_PASS_DELAY_SEC || "180");
const DAY_INTERVAL_MIN          = Number(process.env.DAY_INTERVAL_MIN || "6");
const NIGHT_INTERVAL_MIN        = Number(process.env.NIGHT_INTERVAL_MIN || "120");
const HOLDOVER_INTERVAL_MIN     = Number(process.env.HOLDOVER_INTERVAL_MIN || "99999");
const HTTP_TIMEOUT_MS           = Number(process.env.HTTP_TIMEOUT_MS || "20000");

const AT_RETRY_ATTEMPTS = Number(process.env.AT_RETRY_ATTEMPTS || "3");
const AT_RETRY_BASE_MS  = Number(process.env.AT_RETRY_BASE_MS || "400");
const AT_RETRY_MAX_MS   = Number(process.env.AT_RETRY_MAX_MS || "2000");

const UNDICI_CONNECT_TIMEOUT_MS = Number(process.env.UNDICI_CONNECT_TIMEOUT_MS || "0");

const FORCE_MODE = (process.env.FORCE_MODE || "").trim().toUpperCase();
const DRY_RUN    = (process.env.DRY_RUN || "0") === "1";

const STATE_FILE = process.env.TAGGER_STATE_FILE || path.join(process.cwd(), "tagger_runtime_state.json");
const HB_TZ      = process.env.HB_TIMEZONE || "America/New_York";

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function requireEnv(name, val) {
  if (!val) throw new Error(`Missing required env: ${name}`);
}

function isCompleted(statusVal) {
  return String(statusVal ?? "").trim().toLowerCase() === "completed";
}

function isUnderway(statusVal) {
  return String(statusVal ?? "").trim().toLowerCase() === "underway";
}

function isGoneIn(v) {
  if (v === true) return true;
  const n = Number(v);
  return Number.isFinite(n) && n === 1;
}

function parseDateParts(dateStr) {
  const s = String(dateStr ?? "").trim();
  if (!s) return null;

  const m1 = s.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (m1) return { y: +m1[1], mo: +m1[2], d: +m1[3] };

  const m2 = s.match(/^(\d{1,2})\/(\d{1,2})\/(\d{2}|\d{4})$/);
  if (m2) {
    const yRaw = +m2[3];
    const y = String(m2[3]).length === 2 ? 2000 + yRaw : yRaw;
    return { y, mo: +m2[1], d: +m2[2] };
  }

  return null;
}

function parseTimeParts(timeStr) {
  const s = String(timeStr ?? "").trim();
  if (!s) return null;

  let m = s.match(/^(\d{1,2}):(\d{2})(?::(\d{2}))?$/);
  if (m) return { h: +m[1], mi: +m[2], se: m[3] ? +m[3] : 0, ampm: null };

  m = s.match(/^(\d{1,2}):(\d{2})(?::(\d{2}))?\s*([AP]M)$/i);
  if (m) return { h: +m[1], mi: +m[2], se: m[3] ? +m[3] : 0, ampm: m[4].toUpperCase() };

  return null;
}

function toEpochSecondsLocal(dateStr, timeStr, tzOffsetMinutes, { allow24Hour = false } = {}) {
  const dp = parseDateParts(dateStr);
  const tp = parseTimeParts(timeStr);
  if (!dp || !tp) return null;

  let { y, mo, d } = dp;
  let { h, mi, se, ampm } = tp;

  if (ampm) {
    if (h === 12) h = 0;
    if (ampm === "PM") h += 12;
  }

  if (allow24Hour && h >= 24) {
    h = h - 24;
    const dt = new Date(Date.UTC(y, mo - 1, d, 0, 0, 0));
    dt.setUTCDate(dt.getUTCDate() + 1);
    y = dt.getUTCFullYear();
    mo = dt.getUTCMonth() + 1;
    d = dt.getUTCDate();
  }

  const ms = Date.UTC(y, mo - 1, d, h, mi, se) - (tzOffsetMinutes * 60_000);
  return Math.floor(ms / 1000);
}

function dayOfWeekUtc(sqlDate) {
  const d = new Date(`${sqlDate}T00:00:00Z`);
  if (isNaN(d.getTime())) return null;
  return d.getUTCDay();
}

(function maybeConfigureUndici() {
  if (!UNDICI_CONNECT_TIMEOUT_MS || !Number.isFinite(UNDICI_CONNECT_TIMEOUT_MS) || UNDICI_CONNECT_TIMEOUT_MS <= 0) return;
  try {
    const undici = require("undici");
    const Agent = undici?.Agent;
    const setGlobalDispatcher = undici?.setGlobalDispatcher;
    if (Agent && typeof setGlobalDispatcher === "function") {
      setGlobalDispatcher(new Agent({ connectTimeout: UNDICI_CONNECT_TIMEOUT_MS }));
      console.log(`undici: set connectTimeout=${UNDICI_CONNECT_TIMEOUT_MS}ms`);
    }
  } catch {
    // ignore
  }
})();

async function fetchWithTimeout(url, opts = {}) {
  const ac = new AbortController();
  const t = setTimeout(() => ac.abort(), HTTP_TIMEOUT_MS);
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
  const attempts = Math.max(1, Math.floor(Number(retry.attempts ?? AT_RETRY_ATTEMPTS)));
  const baseMs   = Math.max(0, Math.floor(Number(retry.baseMs ?? AT_RETRY_BASE_MS)));
  const maxMs    = Math.max(250, Math.floor(Number(retry.maxMs ?? AT_RETRY_MAX_MS)));

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

function normalizeMode(v) {
  const s = String(v ?? "").trim().toUpperCase();
  if (s === "DAY" || s === "NIGHT" || s === "HOLDOVER") return s;
  return "HOLDOVER";
}

function resolveModeFromClock(clock) {
  if (FORCE_MODE) return normalizeMode(FORCE_MODE);

  const sqlDate = String(clock?.sqlDate || "").trim();
  const dow = dayOfWeekUtc(sqlDate);
  const minuteOfDay = minuteOfDayFromMs(clock.nowMs, HB_TZ);

  if (dow === 1) return "HOLDOVER"; // Mon
  if (dow === 2 || dow === 3 || dow === 4 || dow === 5 || dow === 6) {
    return minuteOfDay >= (17 * 60) ? "NIGHT" : "DAY"; // Tue-Sat
  }
  return "DAY"; // Sun
}

function isFirstPrint(mode, clock) {
  if (mode !== "DAY") return false;
  const minuteOfDay = minuteOfDayFromMs(clock.nowMs, HB_TZ);
  return minuteOfDay >= (6 * 60) && minuteOfDay <= ((6 * 60) + 25);
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
    tzOffsetMinutes: Number.isFinite(Number(tz.time_zone_offset)) ? Number(tz.time_zone_offset) : getTzOffsetMinutes(HB_TZ, nowMs),
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
      console.log(`clock warn: endpoint http ${res.status}, using system clock`);
      return systemClock;
    }

    let payload = null;
    try {
      payload = JSON.parse(txt);
    } catch {
      console.log("clock warn: endpoint invalid json, using system clock");
      return systemClock;
    }

    const endpointClock = pickClockFromPayload(payload);
    if (!endpointClock) {
      console.log("clock warn: endpoint missing clock values, using system clock");
      return systemClock;
    }

    const systemSqlDate = systemClock.sqlDate;
    if (String(endpointClock.sqlDate || "") !== String(systemSqlDate || "")) {
      console.log(`clock warn: endpoint sql_date ${endpointClock.sqlDate} != system today ${systemSqlDate}, using system clock`);
      return systemClock;
    }

    return endpointClock;
  } catch (e) {
    console.log(`clock warn: ${String(e?.message || e).slice(0, 180)}, using system clock`);
    return systemClock;
  }
}

function airtableUrl(tableName) {
  return `https://api.airtable.com/v0/${AIRTABLE_BASE_ID}/${encodeURIComponent(tableName)}`;
}

async function airtableList(tableName, viewName) {
  const out = [];
  let offset = null;

  while (true) {
    const url = new URL(airtableUrl(tableName));
    url.searchParams.set("view", viewName);
    url.searchParams.set("pageSize", "100");
    if (offset) url.searchParams.set("offset", offset);

    const res = await fetchWithRetry(url.toString(), {
      headers: { Authorization: `Bearer ${AIRTABLE_TOKEN}` }
    });

    if (!res.ok) {
      const body = await res.text().catch(() => "");
      throw new Error(`Airtable list failed (${res.status}) ${tableName}/${viewName}: ${body}`);
    }

    const json = await res.json().catch(() => ({}));
    out.push(...(json.records || []));
    offset = json.offset;
    if (!offset) break;
  }

  return out;
}

async function airtableBatchUpdate(tableName, updates) {
  if (!updates.length) return;

  for (let i = 0; i < updates.length; i += 10) {
    const chunk = updates.slice(i, i + 10);
    const res = await fetchWithRetry(airtableUrl(tableName), {
      method: "PATCH",
      headers: {
        Authorization: `Bearer ${AIRTABLE_TOKEN}`,
        "Content-Type": "application/json"
      },
      body: JSON.stringify({ records: chunk })
    });

    if (!res.ok) {
      const body = await res.text().catch(() => "");
      throw new Error(`Airtable patch failed (${res.status}) ${tableName}: ${body}`);
    }
  }
}

async function safeList(tableName, viewName) {
  try {
    return await airtableList(tableName, viewName);
  } catch (e) {
    console.log(`table warn: ${tableName}/${viewName} list failed ${String(e?.message || e).slice(0, 200)}`);
    return [];
  }
}

async function safeBatchUpdate(tableName, updates) {
  try {
    if (DRY_RUN) {
      console.log(`DRY_RUN: ${tableName} updates=${updates.length}`);
      return;
    }
    await airtableBatchUpdate(tableName, updates);
  } catch (e) {
    console.log(`table warn: ${tableName} update failed ${String(e?.message || e).slice(0, 200)}`);
  }
}

async function airtableCreateRecord(tableName, fields) {
  const res = await fetchWithRetry(airtableUrl(tableName), {
    method: "POST",
    headers: {
      Authorization: `Bearer ${AIRTABLE_TOKEN}`,
      "Content-Type": "application/json"
    },
    body: JSON.stringify({ fields })
  });

  if (!res.ok) {
    const body = await res.text().catch(() => "");
    throw new Error(`Airtable create failed (${res.status}) ${tableName}: ${body}`);
  }

  return res.json().catch(() => ({}));
}

async function createHeartbeatPassSafe(clock, mode, intervalMin) {
  if (DRY_RUN) return;

  try {
    const sqlDate = String(clock?.sqlDate || "").trim();
    const epoch = Number(clock?.nowEpoch);
    if (!sqlDate || !Number.isFinite(epoch)) {
      console.log("heartbeat warn: missing sqlDate/epoch; skipping");
      return;
    }

    const hbDurationSec = hbDurationSecondsFromMs(clock.nowMs, HB_TZ);
    const heartbeatId = `${clock?.showId ?? "unknown"}-${sqlDate}-${epoch}`;

    const fields = {
      [HEARTBEAT_ID_FIELD]: heartbeatId,
      [HEARTBEAT_SHOW_ID]: clock?.showId ?? null,
      [HEARTBEAT_SHOW_DATE]: clock?.showDate ?? null,
      [HEARTBEAT_SQL_DATE]: sqlDate,
      [HEARTBEAT_TIME]: clock?.time ?? null,
      [FIELD_MODE]: mode,
      [FIELD_HB_DURATION]: hbDurationSec,
      [FIELD_INTERVAL]: intervalMin,
      [FIELD_HB_AT]: clock?.iso ?? new Date(epoch * 1000).toISOString(),
    };

    await airtableCreateRecord(TABLE_HEARTBEAT, fields);
  } catch (e) {
    console.log(`heartbeat warn: ${String(e?.message || e).slice(0, 200)}`);
  }
}

function nextDueSecondsFor(mode, temp) {
  if (temp === "DONE") return null;

  if (mode === "DAY") {
    if (temp === "LIVE" || temp === "HOT") return 180;
    if (temp === "WARM") return 300;
    return 1200;
  }

  if (mode === "NIGHT") {
    return NIGHT_INTERVAL_MIN * 60;
  }

  return null;
}

function intervalMinutesForMode(mode) {
  if (mode === "DAY") return DAY_INTERVAL_MIN;
  if (mode === "NIGHT") return NIGHT_INTERVAL_MIN;
  return HOLDOVER_INTERVAL_MIN;
}

function computeTempSchedule(fields, nowEpoch, tzOffsetMinutes) {
  const status = fields[SCHED_STATUS];

  if (isCompleted(status)) return { temp: "DONE" };
  if (isUnderway(status)) return { temp: "LIVE" };

  const dateStr = fields[SCHED_SHOW_DATE];
  const timeStr = fields[SCHED_TIME_LATEST] || fields[SCHED_TIME_BASE];

  const targetEpoch = toEpochSecondsLocal(dateStr, timeStr, tzOffsetMinutes, { allow24Hour: false });
  if (targetEpoch == null) return { temp: "COLD" };

  const till = targetEpoch - nowEpoch;

  if (till <= 1800) return { temp: "HOT" };
  if (till <= 3600) return { temp: "WARM" };
  return { temp: "COLD" };
}

function computeTempTrip(fields, nowEpoch, tzOffsetMinutes) {
  const status = fields[TRIP_STATUS];

  if (isCompleted(status)) return { temp: "DONE" };
  if (isGoneIn(fields[TRIP_GONEIN])) return { temp: "LIVE" };

  const dateStr = fields[TRIP_DT];
  const tLatest = fields[TRIP_GO_LATEST];
  const tGo = fields[TRIP_GO_BASE];
  const tStart = fields[TRIP_START_FALLB];

  const goCandidate = (tGo && !String(tGo).includes("00:00:00")) ? tGo : null;
  const timeStr = tLatest || goCandidate || tStart;

  const allow24 = Boolean(timeStr && String(timeStr).startsWith("24"));
  const targetEpoch = toEpochSecondsLocal(dateStr, timeStr, tzOffsetMinutes, { allow24Hour: allow24 });
  if (targetEpoch == null) return { temp: "COLD" };

  const till = targetEpoch - nowEpoch;

  if (till <= 1800) return { temp: "HOT" };
  if (till <= 3600) return { temp: "WARM" };
  return { temp: "COLD" };
}

function buildCommonMeta(clock, mode, intervalMin) {
  return {
    epoch: clock.nowEpoch,
    hbDurationSec: hbDurationSecondsFromMs(clock.nowMs, HB_TZ),
    hbAtIso: clock.iso ?? new Date(clock.nowEpoch * 1000).toISOString(),
    hbTime: clock.time || formatHbTimeFromMs(clock.nowMs, HB_TZ),
    firstPrint: isFirstPrint(mode, clock),
    intervalMin,
  };
}

function buildShowsLikeUpdate(recordId, meta, mode) {
  return {
    id: recordId,
    fields: {
      [FIELD_MODE]: mode,
      [FIELD_EPOCH]: meta.epoch,
      [FIELD_HB_DURATION]: meta.hbDurationSec,
      [FIELD_INTERVAL]: meta.intervalMin,
      [FIELD_HB_AT]: meta.hbAtIso,
      [FIELD_FIRST_PRINT]: meta.firstPrint,
      [FIELD_HB_TIME]: meta.hbTime,
    }
  };
}

function buildWatchUpdate(recordId, meta, temp, mode) {
  const nextDueInterval = nextDueSecondsFor(mode, temp);
  const nextDue = nextDueInterval == null ? null : (meta.epoch + nextDueInterval);

  return {
    id: recordId,
    fields: {
      [FIELD_EPOCH]: meta.epoch,
      [FIELD_TEMP]: temp,
      [FIELD_BUCKET]: temp,
      [FIELD_NEXT_DUE]: nextDue,
      [FIELD_FIRST_PRINT]: meta.firstPrint,
      [FIELD_MODE]: mode,
      [FIELD_HB_DURATION]: meta.hbDurationSec,
      [FIELD_INTERVAL]: meta.intervalMin,
      [FIELD_HB_AT]: meta.hbAtIso,
      [FIELD_HB_TIME]: meta.hbTime,
    }
  };
}

function buildModeOnlyUpdate(recordId, meta, mode) {
  return {
    id: recordId,
    fields: {
      [FIELD_MODE]: mode,
      [FIELD_HB_DURATION]: meta.hbDurationSec,
      [FIELD_INTERVAL]: meta.intervalMin,
      [FIELD_HB_AT]: meta.hbAtIso,
      [FIELD_HB_TIME]: meta.hbTime,
    }
  };
}

function readRuntimeState() {
  try {
    if (!fs.existsSync(STATE_FILE)) return {};
    return JSON.parse(fs.readFileSync(STATE_FILE, "utf8") || "{}");
  } catch {
    return {};
  }
}

function writeRuntimeState(nextState) {
  try {
    fs.writeFileSync(STATE_FILE, JSON.stringify(nextState, null, 2), "utf8");
  } catch (e) {
    console.log(`state warn: ${String(e?.message || e).slice(0, 180)}`);
  }
}

function updateRuntimeState(patch) {
  const current = readRuntimeState();
  const next = { ...current, ...patch };
  writeRuntimeState(next);
}

function shouldRunNightFull(nowEpoch) {
  const state = readRuntimeState();
  const lastNightFullRunAt = Number(state.lastNightFullRunAt || 0);
  if (!lastNightFullRunAt) return true;
  return (nowEpoch - lastNightFullRunAt) >= (NIGHT_INTERVAL_MIN * 60);
}

function markNightFullRun(nowEpoch) {
  updateRuntimeState({ lastNightFullRunAt: nowEpoch });
}

async function updateShowsAndQueue(clock, mode, intervalMin) {
  const meta = buildCommonMeta(clock, mode, intervalMin);

  const showRows = await safeList(TABLE_SHOWS, VIEW_SHOWS);
  const showUpdates = showRows.map((r) => buildShowsLikeUpdate(r.id, meta, mode));
  await safeBatchUpdate(TABLE_SHOWS, showUpdates);

  const pqRows = await safeList(TABLE_PUBLISH_QUEUE, VIEW_PUBLISH_QUEUE);
  const pqUpdates = pqRows.map((r) => buildShowsLikeUpdate(r.id, meta, mode));
  await safeBatchUpdate(TABLE_PUBLISH_QUEUE, pqUpdates);
}

async function updateModeTables(clock, mode, intervalMin) {
  const meta = buildCommonMeta(clock, mode, intervalMin);

  const schedulerRows = await safeList(TABLE_SCHEDULER, VIEW_SCHEDULER);
  const schedulerUpdates = schedulerRows.map((r) => buildModeOnlyUpdate(r.id, meta, mode));
  await safeBatchUpdate(TABLE_SCHEDULER, schedulerUpdates);

  const activeTenantRows = await safeList(TABLE_ACTIVE_TENANTS, VIEW_ACTIVE_TENANTS);
  const activeTenantUpdates = activeTenantRows.map((r) => buildModeOnlyUpdate(r.id, meta, mode));
  await safeBatchUpdate(TABLE_ACTIVE_TENANTS, activeTenantUpdates);
}

async function updateWatchTables(clock, mode, intervalMin) {
  const meta = buildCommonMeta(clock, mode, intervalMin);

  const scheduleRows = await safeList(TABLE_SCHEDULE, VIEW_SCHEDULE);
  const scheduleUpdates = scheduleRows.map((r) => {
    const temp = computeTempSchedule(r.fields || {}, clock.nowEpoch, clock.tzOffsetMinutes).temp;
    return buildWatchUpdate(r.id, meta, temp, mode);
  });
  await safeBatchUpdate(TABLE_SCHEDULE, scheduleUpdates);

  const tripRows = await safeList(TABLE_TRIPS, VIEW_TRIPS);
  const tripUpdates = tripRows.map((r) => {
    const temp = computeTempTrip(r.fields || {}, clock.nowEpoch, clock.tzOffsetMinutes).temp;
    return buildWatchUpdate(r.id, meta, temp, mode);
  });
  await safeBatchUpdate(TABLE_TRIPS, tripUpdates);
}

async function runFullPass(clock, mode, intervalMin) {
  await updateShowsAndQueue(clock, mode, intervalMin);
  await updateModeTables(clock, mode, intervalMin);
  await updateWatchTables(clock, mode, intervalMin);
}

(async () => {
  try {
    requireEnv("AIRTABLE_TOKEN", AIRTABLE_TOKEN);
    requireEnv("AIRTABLE_BASE_ID", AIRTABLE_BASE_ID);

    const clk1 = await getClockSafe();
    const mode = resolveModeFromClock(clk1);
    const intervalMin = intervalMinutesForMode(mode);

    console.log(`mode=${mode} source=${clk1.source} dry_run=${DRY_RUN}`);

    // heartbeat every launch
    await createHeartbeatPassSafe(clk1, mode, intervalMin);

    if (mode === "HOLDOVER") {
      console.log("mode=HOLDOVER -> heartbeat only");
      process.exit(0);
    }

    if (mode === "NIGHT") {
      if (!shouldRunNightFull(clk1.nowEpoch)) {
        console.log("mode=NIGHT -> heartbeat only; full run not due yet");
        process.exit(0);
      }

      await runFullPass(clk1, mode, intervalMin);
      markNightFullRun(clk1.nowEpoch);
      process.exit(0);
    }

    // DAY pass 1
    await runFullPass(clk1, mode, intervalMin);

    // DAY pass 2 comes from tagger, not Task Scheduler
    await sleep(DAY_SECOND_PASS_DELAY_SEC * 1000);
    const clk2 = await getClockSafe();
    const mode2 = resolveModeFromClock(clk2);
    const intervalMin2 = intervalMinutesForMode(mode2);

    await createHeartbeatPassSafe(clk2, mode2, intervalMin2);

    if (mode2 === "DAY") {
      await runFullPass(clk2, mode2, intervalMin2);
    } else if (mode2 === "NIGHT") {
      if (shouldRunNightFull(clk2.nowEpoch)) {
        await runFullPass(clk2, mode2, intervalMin2);
        markNightFullRun(clk2.nowEpoch);
      }
    }
  } catch (e) {
    const name = e?.name || "error";
    const msg = String(e?.message || e);
    console.log(`fatal: ${name} ${msg.slice(0, 240)}`);
    process.exit(0);
  }
})();
