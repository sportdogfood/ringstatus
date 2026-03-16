// tagger.js (FULL DROP)

const fs = require("fs");
const path = require("path");

const AIRTABLE_TOKEN   = process.env.AIRTABLE_TOKEN || "";
const AIRTABLE_BASE_ID = process.env.AIRTABLE_BASE_ID || "";

const TABLE_SHOWS          = process.env.TABLE_SHOWS || "shows";
const TABLE_SCHEDULE       = process.env.TABLE_SCHEDULE || "watch_schedule";
const TABLE_TRIPS          = process.env.TABLE_TRIPS || "watch_trips";
const TABLE_SCHEDULER      = process.env.TABLE_SCHEDULER || "scheduler";
const TABLE_ACTIVE_TENANTS = process.env.TABLE_ACTIVE_TENANTS || "active_tenants";
const TABLE_HEARTBEAT      = process.env.TABLE_HEARTBEAT || "heartbeat";

const VIEW_SHOWS          = process.env.VIEW_SHOWS || "epoch";
const VIEW_SCHEDULE       = process.env.VIEW_SCHEDULE || "epoch";
const VIEW_TRIPS          = process.env.VIEW_TRIPS || "epoch";
const VIEW_SCHEDULER      = process.env.VIEW_SCHEDULER || "epoch";
const VIEW_ACTIVE_TENANTS = process.env.VIEW_ACTIVE_TENANTS || "epoch";

const SHOWTIME_URL = process.env.SHOWTIME_URL || "";

const FIELD_MODE        = process.env.FIELD_MODE || "mode";
const FIELD_EPOCH       = process.env.FIELD_EPOCH || "epoch";
const FIELD_TEMP        = process.env.FIELD_TEMP || "temp";
const FIELD_BUCKET      = process.env.FIELD_BUCKET || "bucket";
const FIELD_NEXT_DUE    = process.env.FIELD_NEXT_DUE || "next_due_epoch";
const FIELD_FIRST_PRINT = process.env.FIELD_FIRST_PRINT || "first_print";
const FIELD_HB_DURATION = process.env.FIELD_HB_DURATION || "hb_duration";
const FIELD_INTERVAL    = process.env.FIELD_INTERVAL || "interval";
const FIELD_HB_AT       = process.env.FIELD_HB_AT || "hb_at";

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

const DAY_SECOND_PASS_DELAY_SEC        = Number(process.env.DAY_SECOND_PASS_DELAY_SEC || "180");
const HEARTBEAT_INTERVAL_SEC           = Number(process.env.HEARTBEAT_INTERVAL_SEC || "300");
const NIGHT_FULL_INTERVAL_SEC          = Number(process.env.NIGHT_FULL_INTERVAL_SEC || "3600");
const NIGHT_MODE_CHECK_INTERVAL_SEC    = Number(process.env.NIGHT_MODE_CHECK_INTERVAL_SEC || "3600");
const HOLDOVER_MODE_CHECK_INTERVAL_SEC = Number(process.env.HOLDOVER_MODE_CHECK_INTERVAL_SEC || "36000");
const HTTP_TIMEOUT_MS                  = Number(process.env.HTTP_TIMEOUT_MS || "20000");

const AT_RETRY_ATTEMPTS = Number(process.env.AT_RETRY_ATTEMPTS || "3");
const AT_RETRY_BASE_MS  = Number(process.env.AT_RETRY_BASE_MS || "400");
const AT_RETRY_MAX_MS   = Number(process.env.AT_RETRY_MAX_MS || "2000");

const UNDICI_CONNECT_TIMEOUT_MS = Number(process.env.UNDICI_CONNECT_TIMEOUT_MS || "0");

const FORCE_MODE = (process.env.FORCE_MODE || "").trim().toUpperCase();
const DRY_RUN    = (process.env.DRY_RUN || "0") === "1";

const STATE_FILE = process.env.TAGGER_STATE_FILE || path.join(process.cwd(), "tagger_runtime_state.json");
const HB_TZ      = process.env.HB_TIMEZONE || "America/New_York";

const sleep = (ms) => new Promise(r => setTimeout(r, ms));

function requireEnv(name, val) {
  if (!val) throw new Error(`Missing required env: ${name}`);
}

function normalizeMode(v) {
  const s = String(v ?? "").trim().toUpperCase();
  if (s === "DAY" || s === "NIGHT" || s === "HOLDOVER") return s;
  return "HOLDOVER";
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
    const y = String(m2[3]).length === 2 ? (2000 + yRaw) : yRaw;
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

        let waitMs = Math.min(maxMs, baseMs * i + Math.floor(Math.random() * 200));
        const ra = res.headers?.get?.("retry-after");
        const raNum = ra ? Number(ra) : NaN;
        if (Number.isFinite(raNum) && raNum > 0) waitMs = Math.min(maxMs, raNum * 1000);

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

function pickNowMsAndOffsetFromRingPayload(j) {
  const iso = j?.time_zone_date_time?.date_obj;
  const offset = j?.time_zone_date_time?.time_zone_offset;
  const ms = typeof iso === "string" ? Date.parse(iso) : NaN;
  return {
    nowMs: Number.isFinite(ms) ? ms : NaN,
    tzOffsetMinutes: Number.isFinite(Number(offset)) ? Number(offset) : NaN
  };
}

function getTzPartsFromMs(ms, timeZone = HB_TZ) {
  const dtf = new Intl.DateTimeFormat("en-US", {
    timeZone,
    hour12: false,
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });

  const parts = dtf.formatToParts(new Date(ms));
  const out = { hour: 0, minute: 0, second: 0 };

  for (const p of parts) {
    if (p.type === "hour") out.hour = Number(p.value);
    if (p.type === "minute") out.minute = Number(p.value);
    if (p.type === "second") out.second = Number(p.value);
  }

  return out;
}

function secondsSinceMidnightInTz(ms, timeZone = HB_TZ) {
  const { hour, minute, second } = getTzPartsFromMs(ms, timeZone);
  return (hour * 3600) + (minute * 60) + second;
}

function isFirstPrint(mode, clock) {
  if (mode !== "DAY") return false;
  const secOfDay = secondsSinceMidnightInTz(clock.nowMs, HB_TZ);
  const start = 6 * 3600;           // 06:00:00
  const end   = (6 * 3600) + 1500;  // 06:25:00
  return secOfDay >= start && secOfDay <= end;
}

async function getServerClockStrict() {
  if (!SHOWTIME_URL) return null;

  const backoffs = [0, 600, 1200];

  for (let i = 0; i < backoffs.length; i++) {
    if (backoffs[i]) await sleep(backoffs[i]);

    try {
      const res = await fetchWithTimeout(SHOWTIME_URL, { method: "GET" });
      const txt = await res.text();

      if (!res.ok) {
        console.log(`clock warn: http ${res.status}`);
        continue;
      }

      try {
        const j = JSON.parse(txt);
        const { nowMs, tzOffsetMinutes } = pickNowMsAndOffsetFromRingPayload(j);

        if (!Number.isFinite(nowMs)) {
          console.log("clock warn: date_obj invalid");
          continue;
        }
        if (!Number.isFinite(tzOffsetMinutes)) {
          console.log("clock warn: time_zone_offset invalid");
          continue;
        }

        return {
          nowMs,
          nowEpoch: Math.floor(nowMs / 1000),
          tzOffsetMinutes,
          showId: j?.show_id ?? j?.show?.show_id ?? null,
          showDate: j?.show_date ?? j?.show?.show_date ?? null,
          sqlDate: j?.time_zone_date_time?.sql_date ?? null,
          time: j?.time_zone_date_time?.time ?? null,
          iso: new Date(nowMs).toISOString(),
        };
      } catch {
        const trimmed = txt.trim();
        if (/^\d+$/.test(trimmed)) {
          const ms = Number(trimmed);
          return {
            nowMs: ms,
            nowEpoch: Math.floor(ms / 1000),
            tzOffsetMinutes: 0,
            showId: null,
            showDate: null,
            sqlDate: null,
            time: null,
            iso: new Date(ms).toISOString(),
          };
        }
        console.log("clock warn: non-json response");
      }
    } catch (e) {
      console.log(`clock warn: ${e?.name || "error"} ${String(e?.message || e)}`);
    }
  }

  return null;
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

function intervalSecondsFor(mode, temp) {
  if (mode === "HOLDOVER") return null;
  if (temp === "DONE") return null;

  if (mode === "DAY") {
    if (temp === "COLD") return 1200;
    if (temp === "WARM") return 300;
    if (temp === "HOT" || temp === "LIVE") return 180;
    return 300;
  }

  // NIGHT full-run next_due stays temp-based
  if (temp === "HOT" || temp === "LIVE") return 300;
  return 1200;
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

function buildWatchUpdate(recordId, nowEpoch, temp, mode, hbDurationSec, intervalSec, hbAtIso, firstPrintFlag) {
  const nextDueInterval = intervalSecondsFor(mode, temp);
  const nextDue = nextDueInterval == null ? null : (nowEpoch + nextDueInterval);

  return {
    id: recordId,
    fields: {
      [FIELD_EPOCH]: nowEpoch,
      [FIELD_TEMP]: temp,
      [FIELD_BUCKET]: temp,
      [FIELD_NEXT_DUE]: nextDue,
      [FIELD_FIRST_PRINT]: firstPrintFlag,
      [FIELD_MODE]: mode,
      [FIELD_HB_DURATION]: hbDurationSec,
      [FIELD_INTERVAL]: intervalSec,
      [FIELD_HB_AT]: hbAtIso,
    }
  };
}

function buildModeOnlyUpdate(recordId, mode, hbDurationSec, intervalSec, hbAtIso) {
  return {
    id: recordId,
    fields: {
      [FIELD_MODE]: mode,
      [FIELD_HB_DURATION]: hbDurationSec,
      [FIELD_INTERVAL]: intervalSec,
      [FIELD_HB_AT]: hbAtIso,
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

async function getCurrentModeSafe() {
  try {
    const shows = await airtableList(TABLE_SHOWS, VIEW_SHOWS);
    const top = shows[0];
    return normalizeMode(top?.fields?.[FIELD_MODE]);
  } catch (e) {
    console.log(`mode warn: ${String(e?.message || e).slice(0, 180)}`);
    return null;
  }
}

async function resolveMode(nowEpoch) {
  if (FORCE_MODE) return normalizeMode(FORCE_MODE);

  const state = readRuntimeState();
  const lastKnownMode = normalizeMode(state.lastKnownMode || "");
  const lastModeCheckAt = Number(state.lastModeCheckAt || 0);

  let shouldRefresh = false;

  if (!lastKnownMode || !lastModeCheckAt) {
    shouldRefresh = true;
  } else if (lastKnownMode === "DAY") {
    // always refresh in DAY so stale DAY clears quickly
    shouldRefresh = true;
  } else if (lastKnownMode === "NIGHT") {
    shouldRefresh = (nowEpoch - lastModeCheckAt) >= NIGHT_MODE_CHECK_INTERVAL_SEC;
  } else if (lastKnownMode === "HOLDOVER") {
    shouldRefresh = (nowEpoch - lastModeCheckAt) >= HOLDOVER_MODE_CHECK_INTERVAL_SEC;
  }

  if (!shouldRefresh) {
    return lastKnownMode;
  }

  const freshMode = await getCurrentModeSafe();
  if (freshMode) {
    updateRuntimeState({
      lastKnownMode: freshMode,
      lastModeCheckAt: nowEpoch
    });
    return freshMode;
  }

  return lastKnownMode || null;
}

function shouldRunNightFull(nowEpoch) {
  const state = readRuntimeState();
  const lastNightFullRunAt = Number(state.lastNightFullRunAt || 0);
  if (!lastNightFullRunAt) return true;
  return (nowEpoch - lastNightFullRunAt) >= NIGHT_FULL_INTERVAL_SEC;
}

function markNightFullRun(nowEpoch) {
  updateRuntimeState({ lastNightFullRunAt: nowEpoch });
}

async function createHeartbeatPassSafe(clock, mode, intervalSec) {
  if (DRY_RUN) return;

  try {
    const sqlDate = String(clock?.sqlDate || "").trim();
    const epoch = Number(clock?.nowEpoch);
    if (!sqlDate || !Number.isFinite(epoch)) {
      console.log("heartbeat warn: missing sqlDate/epoch; skipping");
      return;
    }

    const hbDurationSec = secondsSinceMidnightInTz(clock.nowMs, HB_TZ);
    const heartbeatId = `${clock?.showId ?? "unknown"}-${sqlDate}-${epoch}`;

    const fields = {
      [HEARTBEAT_ID_FIELD]: heartbeatId,
      [HEARTBEAT_SHOW_ID]: clock?.showId ?? null,
      [HEARTBEAT_SHOW_DATE]: clock?.showDate ?? null,
      [HEARTBEAT_SQL_DATE]: sqlDate,
      [HEARTBEAT_TIME]: clock?.time ?? null,
      [FIELD_MODE]: mode,
      [FIELD_HB_DURATION]: hbDurationSec,
      [FIELD_INTERVAL]: intervalSec,
      [FIELD_HB_AT]: clock?.iso ?? new Date(epoch * 1000).toISOString(),
    };

    await airtableCreateRecord(TABLE_HEARTBEAT, fields);
  } catch (e) {
    console.log(`heartbeat warn: ${String(e?.message || e).slice(0, 200)}`);
  }
}

async function updateModeTables(clock, mode, intervalSec) {
  const hbAtIso = clock?.iso ?? new Date(clock.nowEpoch * 1000).toISOString();
  const hbDurationSec = secondsSinceMidnightInTz(clock.nowMs, HB_TZ);

  const schedulerRows = await airtableList(TABLE_SCHEDULER, VIEW_SCHEDULER);
  const schedulerUpdates = schedulerRows.map(r => buildModeOnlyUpdate(r.id, mode, hbDurationSec, intervalSec, hbAtIso));

  const activeTenantRows = await airtableList(TABLE_ACTIVE_TENANTS, VIEW_ACTIVE_TENANTS);
  const activeTenantUpdates = activeTenantRows.map(r => buildModeOnlyUpdate(r.id, mode, hbDurationSec, intervalSec, hbAtIso));

  if (DRY_RUN) {
    console.log(`DRY_RUN: scheduler=${schedulerUpdates.length} active_tenants=${activeTenantUpdates.length}`);
  } else {
    await airtableBatchUpdate(TABLE_SCHEDULER, schedulerUpdates);
    await airtableBatchUpdate(TABLE_ACTIVE_TENANTS, activeTenantUpdates);
  }
}

async function updateWatchTables(clock, mode, intervalSec) {
  const hbAtIso = clock?.iso ?? new Date(clock.nowEpoch * 1000).toISOString();
  const hbDurationSec = secondsSinceMidnightInTz(clock.nowMs, HB_TZ);
  const firstPrintFlag = isFirstPrint(mode, clock);

  const scheduleRows = await airtableList(TABLE_SCHEDULE, VIEW_SCHEDULE);
  const scheduleUpdates = scheduleRows.map(r => {
    const temp = computeTempSchedule(r.fields || {}, clock.nowEpoch, clock.tzOffsetMinutes).temp;
    return buildWatchUpdate(r.id, clock.nowEpoch, temp, mode, hbDurationSec, intervalSec, hbAtIso, firstPrintFlag);
  });

  const tripRows = await airtableList(TABLE_TRIPS, VIEW_TRIPS);
  const tripUpdates = tripRows.map(r => {
    const temp = computeTempTrip(r.fields || {}, clock.nowEpoch, clock.tzOffsetMinutes).temp;
    return buildWatchUpdate(r.id, clock.nowEpoch, temp, mode, hbDurationSec, intervalSec, hbAtIso, firstPrintFlag);
  });

  if (DRY_RUN) {
    console.log(`DRY_RUN: watch_schedule=${scheduleUpdates.length} watch_trips=${tripUpdates.length}`);
  } else {
    await airtableBatchUpdate(TABLE_SCHEDULE, scheduleUpdates);
    await airtableBatchUpdate(TABLE_TRIPS, tripUpdates);
  }

  console.log(`tag pass ok | mode=${mode} | watch_schedule=${scheduleRows.length} watch_trips=${tripRows.length}`);
}

async function runFullPass(clock, mode, intervalSec) {
  await updateModeTables(clock, mode, intervalSec);
  await updateWatchTables(clock, mode, intervalSec);
}

(async () => {
  try {
    requireEnv("AIRTABLE_TOKEN", AIRTABLE_TOKEN);
    requireEnv("AIRTABLE_BASE_ID", AIRTABLE_BASE_ID);
    requireEnv("SHOWTIME_URL", SHOWTIME_URL);

    const clk1 = await getServerClockStrict();
    if (!clk1) {
      console.log("clock unavailable: skipping run");
      process.exit(0);
    }

    let mode = await resolveMode(clk1.nowEpoch);
    if (!mode) {
      console.log("mode unavailable: skipping run");
      process.exit(0);
    }

    mode = normalizeMode(mode);
    console.log(`mode=${mode} force=${FORCE_MODE || "none"} dry_run=${DRY_RUN}`);

    // heartbeat every launch
    await createHeartbeatPassSafe(clk1, mode, HEARTBEAT_INTERVAL_SEC);

    if (mode === "HOLDOVER") {
      console.log("mode=HOLDOVER -> heartbeat only");
      process.exit(0);
    }

    if (mode === "NIGHT") {
      if (!shouldRunNightFull(clk1.nowEpoch)) {
        console.log("mode=NIGHT -> heartbeat only; full run not due yet");
        process.exit(0);
      }

      await runFullPass(clk1, mode, NIGHT_FULL_INTERVAL_SEC);
      markNightFullRun(clk1.nowEpoch);
      process.exit(0);
    }

    // DAY pass 1
    await runFullPass(clk1, mode, HEARTBEAT_INTERVAL_SEC);

    // DAY pass 2 comes from tagger itself, not Task Scheduler
    await sleep(DAY_SECOND_PASS_DELAY_SEC * 1000);
    const clk2 = await getServerClockStrict();
    if (!clk2) {
      console.log("clock unavailable: skipping DAY pass2");
      process.exit(0);
    }

    await createHeartbeatPassSafe(clk2, mode, DAY_SECOND_PASS_DELAY_SEC);
    await runFullPass(clk2, mode, DAY_SECOND_PASS_DELAY_SEC);

  } catch (e) {
    const name = e?.name || "error";
    const msg = String(e?.message || e);
    console.log(`fatal: ${name} ${msg.slice(0, 240)}`);
    process.exit(0);
  }
})();
