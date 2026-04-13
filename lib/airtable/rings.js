/**
 * Airtable Automation Script (FULL DROP)
 * RING DRIFT + LOGGING + TRIP ALERT TARGETING
 *
 * SOURCE
 * - watch_trips view = ring_alerts
 *
 * FLOW
 * 1) Read watch_trips/ring_alerts
 * 2) Dedup unique sid|dt|ring_number
 * 3) Map ring_number -> ring_id
 * 4) Build endpoint:
 *    https://sglapi.wellingtoninternational.com/ring/{ring_id}?show_date={dt}&show_id={sid}&customer_id=15
 * 5) Fetch each unique ring once
 * 6) Upsert watch_rings
 * 7) Create ring_alerts rows when status = LATE
 * 8) Update matching watch_trips records from the same source view
 *
 * FAIL BEHAVIOR
 * - silently skip bad/missing records
 * - do not stop remaining work if one ring fails
 * - logs are captured in ring_error_log + console
 */

//////////////////////
// 0) Config
//////////////////////
const TRIPS_TABLE = "watch_trips";
const TRIPS_SOURCE_VIEW = "ring_alerts";

const RINGS_OUT_TABLE = "watch_rings";
const RINGS_OUT_KEY_FIELD = "watch_rings_id";

const RING_ALERTS_TABLE = "ring_alerts";

const BASE_RING_URL = "https://ringstatus-proxy.gombcg.workers.dev/api";
const FIXED_CUSTOMER_ID = 15;

// watch_trips fields (read)
const TRIP_SID_FIELD = "sid";
const TRIP_DT_FIELD = "dt";
const TRIP_RINGNUM_FIELD = "ring_number";
const TRIP_CLASS_GROUP_ID_FIELD = "class_group_id";
const TRIP_SECONDS_TILL_FIELD = "secondsTill";
const TRIP_STATUS_FIELD = "latestStatus";
const TRIP_SHOULD_ALERT_FIELD = "should_alert";
const TRIP_FLAG_DONE_FIELD = "flag_done";
const TRIP_LAST_ALERTED_EPOCH_MS_FIELD = "last_alerted_epoch_ms";
const TRIP_LAST_ALERTED_OFFSET_FIELD = "last_alerted_offset_min";
const TRIP_LAST_ALERTED_ADJ_SECONDS_FIELD = "last_alerted_adj_secondsTill";

// watch_trips fields (write)
const TRIP_RING_OFFSET_FIELD = "ring_offset_min";
const TRIP_ADJ_SECONDS_TILL_FIELD = "adj_secondsTill";
const TRIP_ADJ_GO_DT_FIELD = "adj_go_dt";
const TRIP_ALERT_REASON_FIELD = "alert_reason";
const TRIP_CURRENT_GROUP_MINS_PER_TRIP_FIELD = "current_group_mins_per_trip";
const TRIP_IS_GROUP_MINS_PER_TRIP_FIELD = "is_group_mins_per_trip";

// thresholds
const TOLERANCE_MIN = 10;
const ALERT_OFFSET_MIN = 20;
const ALERT_MIN_LEAD_SEC = 2400;
const STALE_LAG_SEC = 180;

// mins_per_trip guardrails
const MIN_MINS_PER_TRIP = 1.8;
const MAX_MINS_PER_TRIP = 3.8;
const FALLBACK_MINS_PER_TRIP = 3.0;

// current group pace guardrails
const CURRENT_GROUP_MIN_TOTAL_TRIPS = 10;
const CURRENT_GROUP_MIN_COMPLETED_TRIPS = 4;

// re-alert behavior
const ALERT_COOLDOWN_MIN = 60;
const REALERT_DELTA_MIN = 10;
const REALERT_ADJ_SEC_DELTA = 600;

// log cap
const MAX_LOGS = 200;

// ring_number -> ring_id helper
// this is summer ring id -- you muse change for WEF
const RING_ID_BY_RING_NUMBER = {
  1: 10,
  2: 3,
  3: 44,
  4: 13,
  5: 51,
  6: 9,
  7: 37,
  8: 52,
  9: 25,
  10: 56,
  11: 57,
  12: 58,
  13: 53,
  14: 22,
  15: 2
};

//////////////////////
// 1) Helpers
//////////////////////
function toNum(v, fallback = null) {
  const n = Number(v);
  return Number.isFinite(n) ? n : fallback;
}
function safeStr(v) {
  return typeof v === "string" ? v : (v == null ? "" : String(v));
}
function abs(n) {
  return Math.abs(Number(n));
}
function sign(n) {
  return n === 0 ? 0 : (n > 0 ? 1 : -1);
}
function normalizeDayIso(v) {
  if (v == null) return null;
  if (v instanceof Date) return v.toISOString().slice(0, 10);
  const s = safeStr(v).trim();
  if (!s) return null;
  return s.length >= 10 ? s.slice(0, 10) : s;
}
function localTimeToUtcMs(show_date, timeStr, offsetMin) {
  if (!show_date || !timeStr) return null;
  const t = safeStr(timeStr).trim();
  if (!t || t === "00:00:00") return null;

  const baseUtcMs = Date.parse(`${show_date}T${t}Z`);
  if (!Number.isFinite(baseUtcMs)) return null;

  const utcMs = baseUtcMs - (toNum(offsetMin, 0) * 60000);
  return Number.isFinite(utcMs) ? utcMs : null;
}
function diffMinutesMs(aMs, bMs) {
  if (!Number.isFinite(aMs) || !Number.isFinite(bMs)) return null;
  return (aMs - bMs) / 60000;
}
function clampPct(x) {
  if (!Number.isFinite(x)) return null;
  return Math.max(0, Math.min(100, x));
}
function pickGroups(payload) {
  const raw = Array.isArray(payload?.class_groups) ? payload.class_groups : [];
  return raw.filter(g => toNum(g?.cancelled, 0) !== 1);
}
function isTripIrrelevant(latestStatus, secondsTill) {
  const s = safeStr(latestStatus).toLowerCase();
  if (Number.isFinite(secondsTill) && secondsTill <= 0) return true;

  const badTokens = [
    "complete", "completed", "done", "finished",
    "cancel", "cancelled", "canceled",
    "scratch", "scratched",
    "eliminat", "dq", "disqual",
    "retire", "withdraw",
  ];
  for (const t of badTokens) {
    if (s.includes(t)) return true;
  }

  const underwayTokens = ["underway", "in progress", "running", "started"];
  for (const t of underwayTokens) {
    if (s.includes(t)) return true;
  }

  return false;
}
function utcMsToLocalHms(utcMs, offsetMin) {
  if (!Number.isFinite(utcMs)) return "";
  const off = toNum(offsetMin, 0);
  const localMs = utcMs + off * 60000;
  const d = new Date(localMs);
  return d.toISOString().slice(11, 19);
}
function buildFieldsIfExist(table, candidate) {
  const allowed = new Set(table.fields.map(f => f.name));
  const out = {};
  for (const [k, v] of Object.entries(candidate)) {
    if (allowed.has(k)) out[k] = v;
  }
  return out;
}
function minutesToHuman(min) {
  if (!Number.isFinite(min)) return "";
  return `${Math.round(min)} min`;
}
function applyMinsPerTripGuardrail(rawMinsPerTrip) {
  if (!Number.isFinite(rawMinsPerTrip)) return null;
  if (rawMinsPerTrip < MIN_MINS_PER_TRIP || rawMinsPerTrip > MAX_MINS_PER_TRIP) {
    return FALLBACK_MINS_PER_TRIP;
  }
  return rawMinsPerTrip;
}
function buildLateSmsTitle(computed) {
  const lateMin = Math.max(0, Math.round(toNum(computed.ring_offset_min, 0)));
  return `Ring ${computed.ring_number} running ${lateMin} min late`;
}
function buildLateSmsBody(computed) {
  const lateMin = Math.max(0, Math.round(toNum(computed.ring_offset_min, 0)));
  const ringName = safeStr(computed.ring_name);
  const ringLabel = ringName ? ` (${ringName})` : "";
  const asOfText = safeStr(computed.ring_time_text) || safeStr(computed.now_local_text);
  const groupName = safeStr(computed.active_group_name) || safeStr(computed.next_group_name);
  const projectedEnd = safeStr(computed.projected_end_local_text);

  const currentGroupSentence = groupName
    ? `Current group: ${groupName}.`
    : `A class group is currently in progress.`;

  const projectedEndSentence = projectedEnd
    ? `Projected end: ${projectedEnd}.`
    : `Projected end is not yet available.`;

  if (asOfText) {
    return `Ring ${computed.ring_number}${ringLabel} is running about ${lateMin} min late as of ${asOfText}. ${currentGroupSentence} ${projectedEndSentence}`;
  }

  return `Ring ${computed.ring_number}${ringLabel} is running about ${lateMin} min late. ${currentGroupSentence} ${projectedEndSentence}`;
}
function buildRingEndpoint({ sid, show_date, ring_id, customer_id = FIXED_CUSTOMER_ID }) {
  return `${BASE_RING_URL}/ring/${ring_id}?show_date=${encodeURIComponent(show_date)}&show_id=${encodeURIComponent(sid)}&customer_id=${encodeURIComponent(customer_id)}`;
}

const ringErrorLog = [];
function log(msg) {
  const line = `[${new Date().toISOString()}] ${msg}`;
  if (ringErrorLog.length < MAX_LOGS) ringErrorLog.push(line);
  try {
    console.log(line);
  } catch (e) {}
}

//////////////////////
// 2) Compute ring status
//////////////////////
function computeRingStatus(payload, runTimeMs) {
  const tz = payload?.time_zone_date_time || {};
  const offsetMin = toNum(tz.time_zone_offset, 0);

  const dtIso = tz.time_obj || tz.date_obj;
  const nowMs = dtIso ? new Date(dtIso).getTime() : null;

  const show_date = safeStr(payload?.show_date);
  const showObj = payload?.show || {};
  const sid = toNum(showObj?.show_id, toNum(payload?.show_id, null));

  const ringObj = payload?.ring || {};
  const ring_id = toNum(ringObj?.ring_id, null);
  const ring_number = toNum(ringObj?.ring_number, null);
  const ring_name = safeStr(ringObj?.ring_name);

  const show_name = safeStr(showObj?.show_name);

  if (!sid || !show_date || !ring_number || !Number.isFinite(nowMs)) {
    return { ok: false, error: "Missing sid/show_date/ring_number/nowMs in payload." };
  }

  const watch_rings_id = `${sid}|${show_date}|${ring_number}`;
  const run_id = `${watch_rings_id}|${new Date(nowMs).toISOString()}`;

  let lag_seconds = null;
  if (Number.isFinite(runTimeMs) && Number.isFinite(nowMs)) {
    lag_seconds = Math.max(0, (runTimeMs - nowMs) / 1000);
  }
  const is_stale = Number.isFinite(lag_seconds) && lag_seconds > STALE_LAG_SEC;

  const groups = pickGroups(payload);

  let status = "NO_DATA";
  let status_basis = "NONE";
  let status_reason = "No class_groups available.";

  let start_drift_min = null;
  let end_drift_min = null;
  let ring_offset_min = null;

  const ring_epoch_ms = nowMs;
  const now_local_text = utcMsToLocalHms(nowMs, offsetMin);

  let next_group_id = null;
  let next_group_name = "";
  let next_group_sequence = null;

  let next_sched_start_epoch_ms = null;
  let next_sched_start_local_text = "";

  let active_group_id = null;
  let active_group_name = "";
  let active_group_sequence = null;

  let active_actual_start_time_text = "";
  let active_estimated_start_time_text = "";
  let active_estimated_end_time_text = "";

  let active_actual_start_epoch_ms = null;
  let active_est_end_epoch_ms = null;

  let active_total_trips = null;
  let active_completed_trips = null;
  let active_progress_pct = null;

  let elapsed_min = null;
  let mins_per_trip = null;

  let current_group_id = null;
  let current_group_mins_per_trip = null;

  let projected_end_epoch_ms = null;
  let projected_end_local_text = "";

  let est_end_local_text = "";
  let now_vs_est_end_min = null;
  let projected_vs_est_end_min = null;

  if (groups.length > 0) {
    const g0 = groups[0];

    next_group_id = toNum(g0.class_group_id, null);
    next_group_name = safeStr(g0.group_name);
    next_group_sequence = toNum(g0.group_sequence, null);

    active_actual_start_time_text = safeStr(g0.actual_start_time);
    active_estimated_start_time_text = safeStr(g0.estimated_start_time);
    active_estimated_end_time_text = safeStr(g0.estimated_end_time);

    const schedStartStr = safeStr(g0.estimated_start_time) || safeStr(g0.start_time_default);
    next_sched_start_epoch_ms = localTimeToUtcMs(show_date, schedStartStr, offsetMin);
    next_sched_start_local_text = utcMsToLocalHms(next_sched_start_epoch_ms, offsetMin);

    start_drift_min = diffMinutesMs(nowMs, next_sched_start_epoch_ms);

    const actualStartKnown = active_actual_start_time_text && active_actual_start_time_text !== "00:00:00";
    const done = toNum(g0.completed_trips, 0);
    const total = toNum(g0.group_total_trips, toNum(g0.total_trips, null));
    const isActive = actualStartKnown || done > 0;

    if (!isActive) {
      status_basis = "START_DRIFT";

      if (start_drift_min == null) {
        status = "NO_DATA";
        status_reason = "Missing scheduled start for drift computation.";
      } else if (start_drift_min < 0) {
        status = "WAITING";
        status_reason = `Next start in ${minutesToHuman(Math.abs(start_drift_min))}.`;
      } else {
        status = "LATE_TO_START";
        status_reason = `Past scheduled start by ${minutesToHuman(start_drift_min)}.`;
      }
    } else {
      active_group_id = next_group_id;
      active_group_name = next_group_name;
      active_group_sequence = next_group_sequence;

      active_total_trips = total;
      active_completed_trips = done;
      active_progress_pct = total && total > 0 ? clampPct((done / total) * 100) : null;

      active_actual_start_epoch_ms = actualStartKnown
        ? localTimeToUtcMs(show_date, active_actual_start_time_text, offsetMin)
        : null;

      active_est_end_epoch_ms = localTimeToUtcMs(show_date, active_estimated_end_time_text, offsetMin);
      est_end_local_text = utcMsToLocalHms(active_est_end_epoch_ms, offsetMin);

      now_vs_est_end_min = diffMinutesMs(nowMs, active_est_end_epoch_ms);

      current_group_id = active_group_id;

      if (
        Number.isFinite(active_actual_start_epoch_ms) &&
        Number.isFinite(active_total_trips) &&
        Number.isFinite(active_completed_trips) &&
        active_total_trips > CURRENT_GROUP_MIN_TOTAL_TRIPS &&
        active_completed_trips > CURRENT_GROUP_MIN_COMPLETED_TRIPS
      ) {
        const currentElapsedMin = diffMinutesMs(nowMs, active_actual_start_epoch_ms);
        const rawCurrentGroupMinsPerTrip =
          Number.isFinite(currentElapsedMin) && active_completed_trips > 0
            ? currentElapsedMin / active_completed_trips
            : null;

        current_group_mins_per_trip = applyMinsPerTripGuardrail(rawCurrentGroupMinsPerTrip);
      }

      if (Number.isFinite(active_actual_start_epoch_ms) && done > 0 && total && total > 0) {
        elapsed_min = diffMinutesMs(nowMs, active_actual_start_epoch_ms);

        const raw_mins_per_trip = elapsed_min != null ? (elapsed_min / done) : null;
        mins_per_trip = applyMinsPerTripGuardrail(raw_mins_per_trip);

        if (mins_per_trip != null && Number.isFinite(mins_per_trip)) {
          projected_end_epoch_ms = active_actual_start_epoch_ms + (mins_per_trip * total * 60000);
          projected_end_local_text = utcMsToLocalHms(projected_end_epoch_ms, offsetMin);
        }

        if (Number.isFinite(projected_end_epoch_ms) && Number.isFinite(active_est_end_epoch_ms)) {
          projected_vs_est_end_min = diffMinutesMs(projected_end_epoch_ms, active_est_end_epoch_ms);
          end_drift_min = projected_vs_est_end_min;

          status_basis = "PACE";

          if (end_drift_min <= -TOLERANCE_MIN) {
            status = "EARLY";
            status_reason = `Projected end ${minutesToHuman(Math.abs(end_drift_min))} early.`;
          } else if (end_drift_min < TOLERANCE_MIN) {
            status = "ON_TIME";
            status_reason = "Projected end within tolerance.";
          } else {
            status = "LATE";
            status_reason = `Projected end ${minutesToHuman(end_drift_min)} late.`;
          }
        } else {
          status_basis = "START_DRIFT";
          status = "ON_TIME";
          status_reason = "No estimated_end_time; pace not available.";
        }
      } else {
        status_basis = "START_DRIFT";
        status = "ON_TIME";
        status_reason = "No pace yet (completed_trips=0 or actual_start missing).";
      }
    }
  }

  if (is_stale) {
    status_reason = `${status_reason} (STALE payload lag ${Math.round(lag_seconds)}s)`;
  }

  if (!is_stale) {
    if (status === "WAITING") ring_offset_min = null;
    else ring_offset_min = Number.isFinite(end_drift_min) ? end_drift_min : start_drift_min;
  } else {
    ring_offset_min = null;
  }

  return {
    ok: true,
    watch_rings_id,
    run_id,
    sid,
    show_date,
    ring_id,
    ring_number,
    ring_name,
    show_name,
    tolerance_min: TOLERANCE_MIN,
    status,
    status_basis,
    status_reason,
    ring_time_text: safeStr(payload?.time_zone_date_time?.time),
    time_zone_offset_min: toNum(payload?.time_zone_date_time?.time_zone_offset, null),
    ring_epoch_ms,
    next_sched_start_epoch_ms,
    active_actual_start_epoch_ms,
    active_est_end_epoch_ms,
    projected_end_epoch_ms,
    now_local_text,
    next_sched_start_local_text,
    active_actual_start_time_text,
    active_estimated_start_time_text,
    active_estimated_end_time_text,
    est_end_local_text,
    projected_end_local_text,
    start_drift_min,
    end_drift_min,
    now_vs_est_end_min,
    projected_vs_est_end_min,
    next_group_id,
    next_group_name,
    next_group_sequence,
    active_group_id,
    active_group_name,
    active_group_sequence,
    active_total_trips,
    active_completed_trips,
    active_progress_pct,
    elapsed_min,
    mins_per_trip,
    current_group_id,
    current_group_mins_per_trip,
    ring_offset_min,
  };
}

//////////////////////
// 3) Read source view and dedup ring requests
//////////////////////
const runTime = new Date();
const runTimeMs = runTime.getTime();

const tripsTable = base.getTable(TRIPS_TABLE);
const tripsView = tripsTable.getView(TRIPS_SOURCE_VIEW);

const tripsQuery = await tripsView.selectRecordsAsync({
  fields: [
    TRIP_SID_FIELD,
    TRIP_DT_FIELD,
    TRIP_RINGNUM_FIELD,
    TRIP_CLASS_GROUP_ID_FIELD,
    TRIP_SECONDS_TILL_FIELD,
    TRIP_STATUS_FIELD,
    TRIP_SHOULD_ALERT_FIELD,
    TRIP_FLAG_DONE_FIELD,
    TRIP_LAST_ALERTED_EPOCH_MS_FIELD,
    TRIP_LAST_ALERTED_OFFSET_FIELD,
    TRIP_LAST_ALERTED_ADJ_SECONDS_FIELD,
    TRIP_RING_OFFSET_FIELD,
    TRIP_ADJ_SECONDS_TILL_FIELD,
    TRIP_ADJ_GO_DT_FIELD,
    TRIP_ALERT_REASON_FIELD,
    TRIP_CURRENT_GROUP_MINS_PER_TRIP_FIELD,
    TRIP_IS_GROUP_MINS_PER_TRIP_FIELD,
  ],
});

const uniqueRingRequests = new Map();

for (const rec of tripsQuery.records) {
  const sid = toNum(rec.getCellValue(TRIP_SID_FIELD), null);
  const show_date = normalizeDayIso(rec.getCellValue(TRIP_DT_FIELD));
  const ring_number = toNum(rec.getCellValue(TRIP_RINGNUM_FIELD), null);

  if (!sid || !show_date || !ring_number) continue;

  const ring_id = toNum(RING_ID_BY_RING_NUMBER[ring_number], null);
  if (!ring_id) continue;

  const dedupKey = `${sid}|${show_date}|${ring_number}`;
  if (uniqueRingRequests.has(dedupKey)) continue;

  uniqueRingRequests.set(dedupKey, {
    sid,
    show_date,
    ring_number,
    ring_id,
    customer_id: FIXED_CUSTOMER_ID,
    endpoint: buildRingEndpoint({
      sid,
      show_date,
      ring_id,
      customer_id: FIXED_CUSTOMER_ID,
    }),
  });
}

//////////////////////
// 4) Load existing watch_rings keys
//////////////////////
const outRingsTable = base.getTable(RINGS_OUT_TABLE);
const outRingsQuery = await outRingsTable.selectRecordsAsync({
  fields: [RINGS_OUT_KEY_FIELD],
});

const existingRingByKey = new Map();
for (const r of outRingsQuery.records) {
  const k = r.getCellValueAsString(RINGS_OUT_KEY_FIELD);
  if (k) existingRingByKey.set(k, r.id);
}

const ringAlertsTable = base.getTable(RING_ALERTS_TABLE);

//////////////////////
// 5) Fetch rings + upsert watch_rings + build drift map
//////////////////////
const ringDriftByKey = new Map();

let source_trip_records = tripsQuery.records.length;
let unique_ring_requests = uniqueRingRequests.size;

let ringsOk = 0;
let ringsErr = 0;
let ringAlertsCreated = 0;
let ringAlertsErrors = 0;

let ringHttpErrors = 0;
let ringPayloadErrors = 0;
let ringWriteErrors = 0;
let ringOuterErrors = 0;

for (const req of uniqueRingRequests.values()) {
  const tag = `[ring ${req.sid}|${req.show_date}|${req.ring_number}|rid:${req.ring_id}]`;

  try {
    log(`${tag} START ${req.endpoint}`);

    const res = await fetch(req.endpoint);
    if (!res.ok) {
      ringsErr++;
      ringHttpErrors++;
      log(`${tag} HTTP_FAIL status=${res.status} endpoint=${req.endpoint}`);
      continue;
    }

    let payload;
    try {
      payload = await res.json();
    } catch (e) {
      ringsErr++;
      ringPayloadErrors++;
      log(`${tag} JSON_FAIL ${e?.message || e}`);
      continue;
    }

    const computed = computeRingStatus(payload, runTimeMs);
    if (!computed.ok) {
      ringsErr++;
      ringPayloadErrors++;
      log(`${tag} COMPUTE_FAIL ${computed.error || "unknown compute error"}`);
      continue;
    }

    log(
      `${tag} COMPUTE_OK status=${computed.status} offset=${computed.ring_offset_min} ring_name=${computed.ring_name || ""} current_group_id=${computed.current_group_id ?? ""} current_group_mins_per_trip=${computed.current_group_mins_per_trip ?? ""}`
    );

    const candidate = {
      watch_rings_id: computed.watch_rings_id,
      run_id: computed.run_id,
      run_time: runTime,

      sid: computed.sid,
      show_date: computed.show_date,
      customer_id: req.customer_id,
      ring_id: computed.ring_id,
      ring_status: computed.ring_number,
      ring_number: computed.ring_number,
      ring_name: computed.ring_name,
      show_name: computed.show_name,
      endpoint: req.endpoint,

      tolerance_min: computed.tolerance_min,
      status: computed.status,
      status_basis: computed.status_basis,
      status_reason: computed.status_reason,

      ring_time_text: computed.ring_time_text,
      time_zone_offset_min: computed.time_zone_offset_min,
      ring_offset_min: computed.ring_offset_min,

      ring_epoch_ms: computed.ring_epoch_ms,
      next_sched_start_epoch_ms: computed.next_sched_start_epoch_ms,
      active_actual_start_epoch_ms: computed.active_actual_start_epoch_ms,
      active_est_end_epoch_ms: computed.active_est_end_epoch_ms,
      projected_end_epoch_ms: computed.projected_end_epoch_ms,

      now_local_text: computed.now_local_text,
      next_sched_start_local_text: computed.next_sched_start_local_text,
      active_actual_start_time_text: computed.active_actual_start_time_text,
      active_estimated_start_time_text: computed.active_estimated_start_time_text,
      active_estimated_end_time_text: computed.active_estimated_end_time_text,
      est_end_local_text: computed.est_end_local_text,
      projected_end_local_text: computed.projected_end_local_text,

      start_drift_min: computed.start_drift_min,
      end_drift_min: computed.end_drift_min,
      now_vs_est_end_min: computed.now_vs_est_end_min,
      projected_vs_est_end_min: computed.projected_vs_est_end_min,

      next_group_id: computed.next_group_id,
      next_group_name: computed.next_group_name,
      next_group_sequence: computed.next_group_sequence,
      active_group_id: computed.active_group_id,
      active_group_name: computed.active_group_name,
      active_group_sequence: computed.active_group_sequence,
      active_total_trips: computed.active_total_trips,
      active_completed_trips: computed.active_completed_trips,
      active_progress_pct: computed.active_progress_pct,
      elapsed_min: computed.elapsed_min,
      mins_per_trip: computed.mins_per_trip,
      current_group_id: computed.current_group_id,
      current_group_mins_per_trip: computed.current_group_mins_per_trip,

      payload_json: JSON.stringify(payload),
    };

    const fields = buildFieldsIfExist(outRingsTable, candidate);
    const existingId = existingRingByKey.get(computed.watch_rings_id);

    try {
      if (existingId) {
        await outRingsTable.updateRecordAsync(existingId, fields);
        log(`${tag} WRITE_OK update watch_rings_id=${computed.watch_rings_id}`);
      } else {
        const newId = await outRingsTable.createRecordAsync(fields);
        existingRingByKey.set(computed.watch_rings_id, newId);
        log(`${tag} WRITE_OK create watch_rings_id=${computed.watch_rings_id}`);
      }
    } catch (e) {
      ringsErr++;
      ringWriteErrors++;
      log(`${tag} WRITE_FAIL ${e?.message || e}`);
      log(`${tag} WRITE_FIELDS ${JSON.stringify(fields)}`);
      continue;
    }

    if (computed.status === "LATE" && Number.isFinite(computed.ring_offset_min) && computed.ring_offset_min > 0) {
      try {
        const smsTitle = buildLateSmsTitle(computed);
        const smsBody = buildLateSmsBody(computed);
        const lateAlertId = `${computed.watch_rings_id}|${computed.run_id}|late`;

        const alertCandidate = {
          ring_alert_id: lateAlertId,
          alert_id: lateAlertId,
          alert_key: lateAlertId,
          name: lateAlertId,
          title: smsTitle,

          watch_rings_id: computed.watch_rings_id,
          run_id: computed.run_id,
          run_time: runTime,

          sid: computed.sid,
          show_date: computed.show_date,
          customer_id: req.customer_id,
          ring_id: computed.ring_id,
          ring_number: computed.ring_number,
          ring_status: computed.ring_number,
          ring_name: computed.ring_name,
          show_name: computed.show_name,
          endpoint: req.endpoint,

          status: computed.status,
          status_basis: computed.status_basis,
          status_reason: computed.status_reason,
          ring_offset_min: computed.ring_offset_min,
          tolerance_min: computed.tolerance_min,

          sms_title: smsTitle,
          sms_body: smsBody,

          ring_time_text: computed.ring_time_text,
          now_local_text: computed.now_local_text,
          next_sched_start_local_text: computed.next_sched_start_local_text,
          active_actual_start_time_text: computed.active_actual_start_time_text,
          active_estimated_start_time_text: computed.active_estimated_start_time_text,
          active_estimated_end_time_text: computed.active_estimated_end_time_text,
          est_end_local_text: computed.est_end_local_text,
          projected_end_local_text: computed.projected_end_local_text,

          ring_epoch_ms: computed.ring_epoch_ms,
          next_sched_start_epoch_ms: computed.next_sched_start_epoch_ms,
          active_actual_start_epoch_ms: computed.active_actual_start_epoch_ms,
          active_est_end_epoch_ms: computed.active_est_end_epoch_ms,
          projected_end_epoch_ms: computed.projected_end_epoch_ms,

          start_drift_min: computed.start_drift_min,
          end_drift_min: computed.end_drift_min,
          now_vs_est_end_min: computed.now_vs_est_end_min,
          projected_vs_est_end_min: computed.projected_vs_est_end_min,

          next_group_id: computed.next_group_id,
          next_group_name: computed.next_group_name,
          next_group_sequence: computed.next_group_sequence,
          active_group_id: computed.active_group_id,
          active_group_name: computed.active_group_name,
          active_group_sequence: computed.active_group_sequence,
          active_total_trips: computed.active_total_trips,
          active_completed_trips: computed.active_completed_trips,
          active_progress_pct: computed.active_progress_pct,
          elapsed_min: computed.elapsed_min,
          mins_per_trip: computed.mins_per_trip,
          current_group_id: computed.current_group_id,
          current_group_mins_per_trip: computed.current_group_mins_per_trip,

          payload_json: JSON.stringify(payload),
        };

        const alertFields = buildFieldsIfExist(ringAlertsTable, alertCandidate);
        await ringAlertsTable.createRecordAsync(alertFields);
        ringAlertsCreated++;
        log(`${tag} ALERT_OK late_alert_created`);
      } catch (e) {
        ringAlertsErrors++;
        log(`${tag} ALERT_FAIL ${e?.message || e}`);
      }
    }

    const driftKey = `${computed.sid}|${computed.show_date}|${computed.ring_number}`;
    ringDriftByKey.set(driftKey, {
      ring_offset_min: computed.ring_offset_min,
      ring_name: computed.ring_name,
      status: computed.status,
      current_group_id: computed.current_group_id,
      current_group_mins_per_trip: computed.current_group_mins_per_trip,
    });

    ringsOk++;
    log(`${tag} DONE`);
  } catch (e) {
    ringsErr++;
    ringOuterErrors++;
    log(`${tag} OUTER_FAIL ${e?.message || e}`);
  }
}

//////////////////////
// 6) Update source-view trips only
//////////////////////
let tripsEvaluated = 0;
let tripsUpdated = 0;
let tripsFlagged = 0;

const tripUpdates = [];

async function flushTripUpdates() {
  if (tripUpdates.length === 0) return;
  while (tripUpdates.length) {
    const batch = tripUpdates.splice(0, 50);
    await tripsTable.updateRecordsAsync(batch);
    tripsUpdated += batch.length;
  }
}

for (const rec of tripsQuery.records) {
  const sid = toNum(rec.getCellValue(TRIP_SID_FIELD), null);
  const dayIso = normalizeDayIso(rec.getCellValue(TRIP_DT_FIELD));
  const ringNum = toNum(rec.getCellValue(TRIP_RINGNUM_FIELD), null);
  const tripClassGroupId = toNum(rec.getCellValue(TRIP_CLASS_GROUP_ID_FIELD), null);

  if (!sid || !dayIso || !ringNum) continue;

  const driftKey = `${sid}|${dayIso}|${ringNum}`;
  const ringInfo = ringDriftByKey.get(driftKey);
  if (!ringInfo) continue;

  const ring_offset_min = toNum(ringInfo.ring_offset_min, null);
  const currentGroupId = toNum(ringInfo.current_group_id, null);
  const currentGroupMinsPerTrip = toNum(ringInfo.current_group_mins_per_trip, null);

  const secondsTill = toNum(rec.getCellValue(TRIP_SECONDS_TILL_FIELD), null);
  const latestStatus = safeStr(rec.getCellValue(TRIP_STATUS_FIELD));

  const shouldAlertNow = !!rec.getCellValue(TRIP_SHOULD_ALERT_FIELD);
  const flagDone = !!rec.getCellValue(TRIP_FLAG_DONE_FIELD);

  let adj_secondsTill = null;
  let adj_go_dt = null;

  if (Number.isFinite(secondsTill) && Number.isFinite(ring_offset_min)) {
    adj_secondsTill = secondsTill + ring_offset_min * 60;
    adj_go_dt = new Date(runTimeMs + adj_secondsTill * 1000);
  }

  let eligible = true;
  let reason = "";

  if (!Number.isFinite(ring_offset_min)) {
    eligible = false;
    reason = "No ring offset (stale/waiting/unavailable).";
  } else if (!Number.isFinite(secondsTill) || !Number.isFinite(adj_secondsTill)) {
    eligible = false;
    reason = "Missing secondsTill/adj_secondsTill.";
  } else if (isTripIrrelevant(latestStatus, secondsTill)) {
    eligible = false;
    reason = `Irrelevant by status/time (${latestStatus || "no-status"}).`;
  } else if (abs(ring_offset_min) < ALERT_OFFSET_MIN) {
    eligible = false;
    reason = `Offset ${minutesToHuman(ring_offset_min)} < ${ALERT_OFFSET_MIN} min threshold.`;
  } else if (adj_secondsTill < ALERT_MIN_LEAD_SEC) {
    eligible = false;
    reason = `Adjusted lead ${Math.floor(adj_secondsTill / 60)} min < ${Math.floor(ALERT_MIN_LEAD_SEC / 60)} min threshold.`;
  }

  let canRaise = false;
  if (eligible) {
    const lastEpoch = toNum(rec.getCellValue(TRIP_LAST_ALERTED_EPOCH_MS_FIELD), null);
    const lastOffset = toNum(rec.getCellValue(TRIP_LAST_ALERTED_OFFSET_FIELD), null);
    const lastAdj = toNum(rec.getCellValue(TRIP_LAST_ALERTED_ADJ_SECONDS_FIELD), null);

    if (!Number.isFinite(lastEpoch) || !Number.isFinite(lastOffset) || !Number.isFinite(lastAdj)) {
      canRaise = true;
    } else {
      const minsSince = (runTimeMs - lastEpoch) / 60000;
      const deltaOffset = abs(ring_offset_min - lastOffset);
      const deltaAdj = abs(adj_secondsTill - lastAdj);
      const signFlip = sign(ring_offset_min) !== sign(lastOffset);

      const deltaOk = signFlip || deltaOffset >= REALERT_DELTA_MIN || deltaAdj >= REALERT_ADJ_SEC_DELTA;
      const cooldownOk = minsSince >= ALERT_COOLDOWN_MIN;

      canRaise = cooldownOk && deltaOk;

      if (!canRaise) {
        if (!cooldownOk) reason = `Cooldown active (<${ALERT_COOLDOWN_MIN}m since last alert).`;
        else reason = `No meaningful change (need sign flip or >=${REALERT_DELTA_MIN}m offset or >=${REALERT_ADJ_SEC_DELTA}s adj delta).`;
      }
    }

    if (canRaise) {
      const adjMinTill = Math.floor(adj_secondsTill / 60);
      const dir = ring_offset_min > 0 ? "LATE" : "EARLY";
      reason = `ALERT: ring ${dir} ${minutesToHuman(abs(ring_offset_min))}; adj_minutesTill=${adjMinTill}; status=${latestStatus || "n/a"}`;
    }
  }

  const isMatchingCurrentGroup =
    Number.isFinite(tripClassGroupId) &&
    Number.isFinite(currentGroupId) &&
    tripClassGroupId === currentGroupId &&
    Number.isFinite(currentGroupMinsPerTrip);

  const writeFields = {
    [TRIP_RING_OFFSET_FIELD]: ring_offset_min,
    [TRIP_ADJ_SECONDS_TILL_FIELD]: adj_secondsTill,
    [TRIP_ADJ_GO_DT_FIELD]: adj_go_dt,
    [TRIP_CURRENT_GROUP_MINS_PER_TRIP_FIELD]: isMatchingCurrentGroup ? currentGroupMinsPerTrip : null,
    [TRIP_IS_GROUP_MINS_PER_TRIP_FIELD]: isMatchingCurrentGroup,
  };

  const pending = shouldAlertNow && !flagDone;

  if (!pending) {
    if (eligible && canRaise) {
      writeFields[TRIP_SHOULD_ALERT_FIELD] = true;
      writeFields[TRIP_FLAG_DONE_FIELD] = false;
      writeFields[TRIP_ALERT_REASON_FIELD] = reason;
      tripsFlagged++;
    } else {
      writeFields[TRIP_ALERT_REASON_FIELD] = reason;
    }
  }

  tripUpdates.push({ id: rec.id, fields: writeFields });
  if (tripUpdates.length >= 50) {
    await flushTripUpdates();
  }

  tripsEvaluated++;
}

await flushTripUpdates();

//////////////////////
// 7) Outputs
//////////////////////
output.set("ok", true);
output.set("done", true);

output.set("source_trip_records", source_trip_records);
output.set("unique_ring_requests", unique_ring_requests);

output.set("rings_ok", ringsOk);
output.set("rings_errors", ringsErr);

output.set("ring_http_errors", ringHttpErrors);
output.set("ring_payload_errors", ringPayloadErrors);
output.set("ring_write_errors", ringWriteErrors);
output.set("ring_outer_errors", ringOuterErrors);

output.set("ring_alerts_created", ringAlertsCreated);
output.set("ring_alerts_errors", ringAlertsErrors);

output.set("trips_evaluated", tripsEvaluated);
output.set("trips_updated", tripsUpdated);
output.set("trips_flagged", tripsFlagged);

output.set("ring_error_log", ringErrorLog);
