// tagger.js
/**
 * RingStatus — Epoch Tagger (external clock, Airtable as state store)
 *
 * Reads:
 *  - shows (view: epoch) -> newest record by created_time -> mode: HOLDOVER|DAY|NIGHT
 *  - watch_schedule (view: epoch)
 *  - watch_trips (view: epoch)
 *
 * Writes ONLY these fields on schedule/trips:
 *  - epoch
 *  - temp
 *  - bucket
 *  - next_due_epoch
 *
 * Temp rules (status overrides first):
 *  - DONE: latestStatus == "Completed"
 *  - LIVE:
 *      schedule: latestStatus == "Underway"
 *      trips: lastGonein == 1
 *  - else time-based (till_seconds = target_epoch - epoch):
 *      HOT  : 0 < till <= 1800
 *      WARM : 1800 < till <= 3600
 *      COLD : till > 3600
 *      (if till <= 0 and not DONE/LIVE: HOT)
 *
 * next_due defaults (agreed):
 *  - HOLDOVER: clear next_due_epoch
 *  - NIGHT   : COLD/WARM = +1200, HOT/LIVE = +300, DONE = clear
 *  - DAY     : COLD = +1200, WARM = +300, HOT/LIVE = +180, DONE = clear
 *
 * Run cadence:
 *  - GitHub schedule every 5 min
 *  - If mode=DAY, does 2 passes per run: immediately + ~180s (3 min)
 *  - If mode=NIGHT, does 1 pass per run
 *  - If mode=HOLDOVER, exits
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
const SCHED_SHOW_DATE  = process.env.SCHED_SHOW_DATE || "show_date";
const SCHED_TIME_LATEST= process.env.SCHED_TIME_LATEST || "latest_estimated_start_time";
const SCHED_TIME_BASE  = process.env.SCHED_TIME_BASE || "estimated_start_time";
const SCHED_STATUS     = process.env.SCHED_STATUS || "latestStatus";

// Trips input fields
const TRIP_DT          = process.env.TRIP_DT || "dt";
const TRIP_GO_LATEST   = process.env.TRIP_GO_LATEST || "latest_estimated_go_time";
const TRIP_GO_BASE     = process.env.TRIP_GO_BASE || "estimated_go_time";
const TRIP_START_FALLB = process.env.TRIP_START_FALLB || "estimated_start_time";
const TRIP_STATUS      = process.env.TRIP_STATUS || "latestStatus";
const TRIP_GONEIN      = process.env.TRIP_GONEIN || "lastGonein";

// Controls
const DAY_SECOND_PASS_DELAY_SEC = Number(process.env.DAY_SECOND_PASS_DELAY_SEC || "180"); // 3 minutes
const HTTP_TIMEOUT_MS = Number(process.env.HTTP_TIMEOUT_MS || "20000");

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

  // Handle 24:xx(:xx) rollover (trips)
  if (allow24Hour && h >= 24) {
    h = h - 24;
    // add 1 day
    const dt = new Date(Date.UTC(y, mo - 1, d, 0, 0, 0));
    dt.setUTCDate(dt.getUTCDate() + 1);
    y = dt.getUTCFullYear();
    mo = dt.getUTCMonth() + 1;
    d = dt.getUTCDate();
  }

  // local time -> UTC epoch:
  // epoch_ms = UTC(y,m,d,h,mi,se) - offsetMinutes
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
    // raw number fallback
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
  // updates: [{id, fields}]
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

  if (isCompleted(status)) return { temp: "DONE", targetEpoch: null };
  if (isUnderway(status)) return { temp: "LIVE", targetEpoch: null };

  const dateStr = fields[SCHED_SHOW_DATE];
  const tLatest = fields[SCHED_TIME_LATEST];
  const tBase = fields[SCHED_TIME_BASE];
  const timeStr = tLatest || tBase;

  const targetEpoch = toEpochSecondsLocal(dateStr, timeStr, tzOffsetMinutes, { allow24Hour: false });
  if (targetEpoch == null) return { temp: "COLD", targetEpoch: null };

  const till = targetEpoch - nowEpoch;

  if (till <= 0) return { temp: "HOT", targetEpoch };
  if (till <= 1800) return { temp: "HOT", targetEpoch };
  if (till <= 3600) return { temp: "WARM", targetEpoch };
  return { temp: "COLD", targetEpoch };
}

function computeTempTrip(fields, nowEpoch, tzOffsetMinutes) {
  const status = fields[TRIP_STATUS];

  if (isCompleted(status)) return { temp: "DONE", targetEpoch: null };
  if (isGoneIn(fields[TRIP_GONEIN])) return { temp: "LIVE", targetEpoch: null };

  const dateStr = fields[TRIP_DT];
  const tLatest = fields[TRIP_GO_LATEST];
  const tGo = fields[TRIP_GO_BASE];
  const tStart = fields[TRIP_START_FALLB];

  // replicate "00:00:00 invalid" behavior for estimated_go_time
  const goCandidate = (tGo && !String(tGo).includes("00:00:00")) ? tGo : null;

  let timeStr = tLatest || goCandidate || tStart;

  // allow 24:xx rollover mainly for estimated_go_time
  const allow24 = Boolean(timeStr && String(timeStr).startsWith("24"));

  const targetEpoch = toEpochSecondsLocal(dateStr, timeStr, tzOffsetMinutes, { allow24Hour: allow24 });
  if (targetEpoch == null) return { temp: "COLD", targetEpoch: null };

  const till = targetEpoch - nowEpoch;

  if (till <= 0) return { temp: "HOT", targetEpoch };
  if (till <= 1800) return { temp: "HOT", targetEpoch };
  if (till <= 3600) return { temp: "WARM", targetEpoch };
  return { temp: "COLD", targetEpoch };
}

function buildUpdate(recordId, existingFields, nowEpoch, temp, mode) {
  const bucket = temp;
  const interval = intervalSecondsFor(mode, temp);
  const nextDue = (interval == null) ? null : (nowEpoch + interval);

  const patch = {};
  // always stamp epoch
  patch[FIELD_EPOCH] = nowEpoch;

  // stamp temp/bucket if changed (or blank)
  if (existingFields[FIELD_TEMP] !== temp) patch[FIELD_TEMP] = temp;
  if (existingFields[FIELD_BUCKET] !== bucket) patch[FIELD_BUCKET] = bucket;

  // next_due_epoch: write null to clear
  if (existingFields[FIELD_NEXT_DUE] !== nextDue) patch[FIELD_NEXT_DUE] = nextDue;

  // Only write if anything besides epoch changed OR if epoch field differs
  // (epoch updates every pass by design, so always write it)
  return { id: recordId, fields: patch };
}

async function getCurrentMode() {
  const shows = await airtableList(TABLE_SHOWS, VIEW_SHOWS);
  const top = shows[0];
  const mode = normalizeMode(top?.fields?.[FIELD_MODE]);
  return mode;
}

async function tagOnce(nowEpoch, tzOffsetMinutes, mode) {
  // schedule
  const sched = await airtableList(TABLE_SCHEDULE, VIEW_SCHEDULE);
  const schedUpdates = sched.map(r => {
    const fields = r.fields || {};
    const { temp } = computeTempSchedule(fields, nowEpoch, tzOffsetMinutes);
    return buildUpdate(r.id, fields, nowEpoch, temp, mode);
  });
  await airtableBatchUpdate(TABLE_SCHEDULE, schedUpdates);

  // trips
  const trips = await airtableList(TABLE_TRIPS, VIEW_TRIPS);
  const tripUpdates = trips.map(r => {
    const fields = r.fields || {};
    const { temp } = computeTempTrip(fields, nowEpoch, tzOffsetMinutes);
    return buildUpdate(r.id, fields, nowEpoch, temp, mode);
  });
  await airtableBatchUpdate(TABLE_TRIPS, tripUpdates);

  console.log(`tag pass ok | mode=${mode} | schedule=${sched.length} trips=${trips.length} | epoch=${nowEpoch} offsetMin=${tzOffsetMinutes}`);
}

(async () => {
  requireEnv("AIRTABLE_TOKEN", AIRTABLE_TOKEN);
  requireEnv("AIRTABLE_BASE_ID", AIRTABLE_BASE_ID);
  requireEnv("SHOWTIME_URL", SHOWTIME_URL);

  const { nowEpoch, tzOffsetMinutes } = await getServerClock();
  const mode = await getCurrentMode();

  if (mode === "HOLDOVER") {
    console.log(`mode=HOLDOVER -> no tagging run`);
    process.exit(0);
  }

  // First pass
  await tagOnce(nowEpoch, tzOffsetMinutes, mode);

  // Second pass only in DAY mode (approx 3-minute cycle inside a 5-minute workflow window)
  if (mode === "DAY") {
    await sleep(DAY_SECOND_PASS_DELAY_SEC * 1000);
    const clk2 = await getServerClock();
    await tagOnce(clk2.nowEpoch, clk2.tzOffsetMinutes, mode);
  }
})();
