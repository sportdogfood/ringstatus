// tagger.js
/**
 * RingStatus — Epoch Tagger (external clock, Airtable as state store)
 *
 * Reads:
 *  - shows (view: epoch) -> newest record by created_time -> mode: HOLDOVER|DAY|NIGHT
 *  - watch_schedule (view: epoch)
 *  - watch_trips (view: epoch)
 *
 * Writes ONLY these fields on schedule/trips (unless DRY_RUN=1):
 *  - epoch
 *  - temp
 *  - bucket
 *  - next_due_epoch
 *
 * Overrides:
 *  - FORCE_MODE=DAY|NIGHT|HOLDOVER  (lets you test without changing shows.mode)
 *  - DRY_RUN=1 (no Airtable writes; logs sample of what would be written)
 */

const AIRTABLE_TOKEN   = process.env.AIRTABLE_TOKEN || "";
const AIRTABLE_BASE_ID = process.env.AIRTABLE_BASE_ID || "";

const TABLE_SHOWS      = process.env.TABLE_SHOWS || "shows";
const TABLE_SCHEDULE   = process.env.TABLE_SCHEDULE || "watch_schedule";
const TABLE_TRIPS      = process.env.TABLE_TRIPS || "watch_trips";

const VIEW_SHOWS       = process.env.VIEW_SHOWS || "epoch";
const VIEW_SCHEDULE    = process.env.VIEW_SCHEDULE || "epoch";
const VIEW_TRIPS       = process.env.VIEW_TRIPS || "epoch";

const SHOWTIME_URL     = process.env.SHOWTIME_URL || ""; // ring endpoint (customer_id=15)

const FIELD_MODE       = process.env.FIELD_MODE || "mode";

// Output tag fields (exist in both schedule + trips)
const FIELD_EPOCH      = process.env.FIELD_EPOCH || "epoch";
const FIELD_TEMP       = process.env.FIELD_TEMP || "temp";
const FIELD_BUCKET     = process.env.FIELD_BUCKET || "bucket";
const FIELD_NEXT_DUE   = process.env.FIELD_NEXT_DUE || "next_due_epoch";

// Schedule input fields
const SCHED_SHOW_DATE   = process.env.SCHED_SHOW_DATE || "show_date";
const SCHED_TIME_LATEST = process.env.SCHED_TIME_LATEST || "latest_estimated_start_time";
const SCHED_TIME_BASE   = process.env.SCHED_TIME_BASE || "estimated_start_time";
const SCHED_STATUS      = process.env.SCHED_STATUS || "latestStatus";

// Trips input fields
const TRIP_DT           = process.env.TRIP_DT || "dt";
const TRIP_GO_LATEST    = process.env.TRIP_GO_LATEST || "latest_estimated_go_time";
const TRIP_GO_BASE      = process.env.TRIP_GO_BASE || "estimated_go_time";
const TRIP_START_FALLB  = process.env.TRIP_START_FALLB || "estimated_start_time";
const TRIP_STATUS       = process.env.TRIP_STATUS || "latestStatus";
const TRIP_GONEIN       = process.env.TRIP_GONEIN || "lastGonein";

// Controls
const DAY_SECOND_PASS_DELAY_SEC = Number(process.env.DAY_SECOND_PASS_DELAY_SEC || "180"); // 3 minutes
const HTTP_TIMEOUT_MS = Number(process.env.HTTP_TIMEOUT_MS || "20000");

// TEST OVERRIDES
const FORCE_MODE = (process.env.FORCE_MODE || "").trim().toUpperCase(); // DAY|NIGHT|HOLDOVER
const DRY_RUN    = (process.env.DRY_RUN || "0") === "1";

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

  // YYYY-MM-DD
  const m1 = s.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (m1) return { y: +m1[1], mo: +m1[2], d: +m1[3] };

  // MM/DD/YY or MM/DD/YYYY
  const m2 = s.match(/^(\d{1,2})\/(\d{1,2})\/(\d{2}|\d{4})$/);
  if (m2) {
    const yRaw = +m2[3];
    const y = (String(m2[3]).length === 2) ? (2000 + yRaw) : yRaw;
    return { y, mo: +m2[1], d: +m2[2] };
  }

  return null;
}

function parseTimeParts(timeStr) {
  const s = String(timeStr ?? "").trim();
  if (!s) return null;

  // HH:mm(:ss) 24h
  let m = s.match(/^(\d{1,2}):(\d{2})(?::(\d{2}))?$/);
  if (m) return { h: +m[1], mi: +m[2], se: m[3] ? +m[3] : 0, ampm: null };

  // h:mm(:ss) AM/PM
  m = s.match(/^(\d{1,2}):(\d{2})(?::(\d{2}))?\s*([AP]M)$/i);
  if (m) return { h: +m[1], mi: +m[2], se: m[3] ? +m[3] : 0, ampm: m[4].toUpperCase() };

  return null;
}

function toEpochSecondsLocal(dateStr, timeStr, tzOffsetMinutes, { allow24Hour = false } = {}) {
  const dp = parseDateParts(dateStr);
  let tp = parseTimeParts(timeStr);
  if (!dp || !tp) return null;

  let { y, mo, d } = dp;
  let { h, mi, se, ampm } = tp;

  // Convert AM/PM to 24h
  if (ampm) {
    if (h === 12) h = 0;
    if (ampm === "PM") h += 12;
  }

  // Handle 24:xx rollover
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

async function fetchWithTimeout(url, opts = {}) {
  const ac = new AbortController();
  const t = setTimeout(() => ac.abort(), HTTP_TIMEOUT_MS);
  try {
    const res = await fetch(url, { ...opts, signal: ac.signal });
    return res;
  } finally {
    clearTimeout(t);
  }
}

function pickNowMsAndOffsetFromRingPayload(j) {
  const iso = j?.time_zone_date_time?.date_obj;
  const offset = j?.time_zone_date_time?.time_zone_offset;
  const ms = (typeof iso === "string") ? Date.parse(iso) : NaN;
  return {
    nowMs: Number.isFinite(ms) ? ms : Date.now(),
    tzOffsetMinutes: Number.isFinite(Number(offset)) ? Number(offset) : 0
  };
}

async function getServerClock() {
  if (!SHOWTIME_URL) return { nowEpoch: Math.floor(Date.now() / 1000), tzOffsetMinutes: 0 };

  const res = await fetchWithTimeout(SHOWTIME_URL, { method: "GET" });
  const txt = await res.text();
  try {
    const j = JSON.parse(txt);
    const { nowMs, tzOffsetMinutes } = pickNowMsAndOffsetFromRingPayload(j);
    return { nowEpoch: Math.floor(nowMs / 1000), tzOffsetMinutes };
  } catch {
    const trimmed = txt.trim();
    if (/^\d+$/.test(trimmed)) return { nowEpoch: Math.floor(Number(trimmed) / 1000), tzOffsetMinutes: 0 };
    return { nowEpoch: Math.floor(Date.now() / 1000), tzOffsetMinutes: 0 };
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

    const res = await fetchWithTimeout(url.toString(), {
      headers: { Authorization: `Bearer ${AIRTABLE_TOKEN}` }
    });

    if (!res.ok) {
      const body = await res.text();
      throw new Error(`Airtable list failed (${res.status}) ${tableName}/${viewName}: ${body}`);
    }

    const json = await res.json();
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
    const res = await fetchWithTimeout(airtableUrl(tableName), {
      method: "PATCH",
      headers: {
        Authorization: `Bearer ${AIRTABLE_TOKEN}`,
        "Content-Type": "application/json"
      },
      body: JSON.stringify({ records: chunk })
    });

    if (!res.ok) {
      const body = await res.text();
      throw new Error(`Airtable patch failed (${res.status}) ${tableName}: ${body}`);
    }
  }
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

  // NIGHT
  if (temp === "HOT" || temp === "LIVE") return 300;
  return 1200;
}

function computeTempSchedule(fields, nowEpoch, tzOffsetMinutes) {
  const status = fields[SCHED_STATUS];

  if (isCompleted(status)) return { temp: "DONE" };
  if (isUnderway(status)) return { temp: "LIVE" };

  const dateStr = fields[SCHED_SHOW_DATE];
  const tLatest = fields[SCHED_TIME_LATEST];
  const tBase = fields[SCHED_TIME_BASE];
  const timeStr = tLatest || tBase;

  const targetEpoch = toEpochSecondsLocal(dateStr, timeStr, tzOffsetMinutes, { allow24Hour: false });
  if (targetEpoch == null) return { temp: "COLD" };

  const till = targetEpoch - nowEpoch;

  if (till <= 0) return { temp: "HOT" };
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

  if (till <= 0) return { temp: "HOT" };
  if (till <= 1800) return { temp: "HOT" };
  if (till <= 3600) return { temp: "WARM" };
  return { temp: "COLD" };
}

function buildUpdate(recordId, existingFields, nowEpoch, temp, mode) {
  const bucket = temp;
  const interval = intervalSecondsFor(mode, temp);
  const nextDue = (interval == null) ? null : (nowEpoch + interval);

  const patch = {};
  patch[FIELD_EPOCH] = nowEpoch;
  patch[FIELD_TEMP] = temp;
  patch[FIELD_BUCKET] = bucket;
  patch[FIELD_NEXT_DUE] = nextDue;

  return { id: recordId, fields: patch };
}

async function getCurrentMode() {
  const shows = await airtableList(TABLE_SHOWS, VIEW_SHOWS);
  const top = shows[0];
  return normalizeMode(top?.fields?.[FIELD_MODE]);
}

function sampleLog(label, updates, limit = 3) {
  const sample = updates.slice(0, limit).map(u => ({
    id: u.id,
    temp: u.fields[FIELD_TEMP],
    bucket: u.fields[FIELD_BUCKET],
    next_due_epoch: u.fields[FIELD_NEXT_DUE]
  }));
  console.log(`${label}: sample`, JSON.stringify(sample));
}

async function tagOnce(nowEpoch, tzOffsetMinutes, mode) {
  const sched = await airtableList(TABLE_SCHEDULE, VIEW_SCHEDULE);
  const schedUpdates = sched.map(r => {
    const fields = r.fields || {};
    const { temp } = computeTempSchedule(fields, nowEpoch, tzOffsetMinutes);
    return buildUpdate(r.id, fields, nowEpoch, temp, mode);
  });

  const trips = await airtableList(TABLE_TRIPS, VIEW_TRIPS);
  const tripUpdates = trips.map(r => {
    const fields = r.fields || {};
    const { temp } = computeTempTrip(fields, nowEpoch, tzOffsetMinutes);
    return buildUpdate(r.id, fields, nowEpoch, temp, mode);
  });

  if (DRY_RUN) {
    console.log(`DRY_RUN: would update schedule=${schedUpdates.length} trips=${tripUpdates.length}`);
    sampleLog("schedule", schedUpdates);
    sampleLog("trips", tripUpdates);
  } else {
    await airtableBatchUpdate(TABLE_SCHEDULE, schedUpdates);
    await airtableBatchUpdate(TABLE_TRIPS, tripUpdates);
  }

  console.log(`tag pass ok | mode=${mode} | schedule=${sched.length} trips=${trips.length} | epoch=${nowEpoch} offsetMin=${tzOffsetMinutes}`);
}

(async () => {
  requireEnv("AIRTABLE_TOKEN", AIRTABLE_TOKEN);
  requireEnv("AIRTABLE_BASE_ID", AIRTABLE_BASE_ID);
  requireEnv("SHOWTIME_URL", SHOWTIME_URL);

  const clk1 = await getServerClock();
  let mode = await getCurrentMode();
  if (FORCE_MODE) mode = normalizeMode(FORCE_MODE);

  console.log(`mode=${mode} (force=${FORCE_MODE || "none"}) dry_run=${DRY_RUN}`);

  if (mode === "HOLDOVER") {
    console.log(`mode=HOLDOVER -> no tagging run`);
    process.exit(0);
  }

  await tagOnce(clk1.nowEpoch, clk1.tzOffsetMinutes, mode);

  if (mode === "DAY") {
    await sleep(DAY_SECOND_PASS_DELAY_SEC * 1000);
    const clk2 = await getServerClock();
    await tagOnce(clk2.nowEpoch, clk2.tzOffsetMinutes, mode);
  }
})();
