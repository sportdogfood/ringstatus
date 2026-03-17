// trips_tagger.js (STATELESS FULL DROP)
/**
 * RingStatus — trips_tagger
 *
 * PURPOSE
 * - Runs outside Airtable on Task Scheduler cadence, similar to tagger.js
 * - Reads watch_trips from a single view (default: hb_targets)
 * - DOES NOT ping /ring again
 * - Uses app/clock fields already stamped onto each watch_trips row
 * - Pings /classes/{class_id} ONCE per unique (resolved_show_id + class_id)
 * - Matches a single trip by entryxclasses_uuid
 * - Updates the SAME watch_trips row
 *
 * RESOLVED SHOW ID
 * - Uses app_show_id first
 * - Falls back to show_id
 *
 * DEFAULT TABLE / VIEW
 * - watch_trips / hb_targets
 *
 * WRITES
 * Existing/legacy fields (same spirit as the old automation):
 * - trip_id
 * - last_order_of_go, last_score, last_placing
 * - last_status, last_actual_time, last_estimated_time, last_estimated_go_time
 * - lastPlace, lastGoneIn, lastPosition
 * - time_one, time_two, time_three
 * - score1, score2, score3
 * - sql_date, observed_at
 * - cwf_estimated_go_time
 * - results_verified
 *
 * New direct fields on watch_trips:
 * - estimated_end_time
 * - estimated_go_time
 * - estimated_start_time
 * - order_of_go
 * - remaining_trips
 * - status
 * - total_trips
 * - hb_second_pass_at
 *
 * NOTES
 * - safeSet() silently skips fields that do not exist
 * - no /ring ping in this script
 * - no local runtime state
 */

const AIRTABLE_TOKEN   = process.env.AIRTABLE_TOKEN || "";
const AIRTABLE_BASE_ID = process.env.AIRTABLE_BASE_ID || "";
const CUSTOMER_ID      = Number(process.env.CUSTOMER_ID || "15");

const WATCH_TABLE = process.env.WATCH_TABLE || "watch_trips";
const WATCH_VIEW  = process.env.WATCH_VIEW || "hb_targets";
const MAX_RECORDS = Number(process.env.MAX_RECORDS || "500");

const HTTP_TIMEOUT_MS = Number(process.env.HTTP_TIMEOUT_MS || "20000");
const AT_RETRY_ATTEMPTS = Number(process.env.AT_RETRY_ATTEMPTS || "3");
const AT_RETRY_BASE_MS  = Number(process.env.AT_RETRY_BASE_MS || "400");
const AT_RETRY_MAX_MS   = Number(process.env.AT_RETRY_MAX_MS || "2000");
const DRY_RUN           = String(process.env.DRY_RUN || "0") === "1";

function requireEnv(name, val) {
  if (!val) throw new Error(`Missing required env: ${name}`);
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const isBlank = (v) =>
  v === null ||
  v === undefined ||
  (typeof v === "string" && v.trim() === "") ||
  String(v).trim().toLowerCase() === "null" ||
  String(v).trim().toLowerCase() === "nan";

function numOrNull(v) {
  if (isBlank(v)) return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function floatOrNull(v) {
  if (isBlank(v)) return null;
  const n = parseFloat(v);
  return Number.isFinite(n) ? n : null;
}

function strOrNull(v) {
  if (isBlank(v)) return null;
  return String(v).trim();
}

function firstNonBlank(...vals) {
  for (const v of vals) {
    if (!isBlank(v)) return v;
  }
  return null;
}

function pickFrom(obj, keys = []) {
  if (!obj || typeof obj !== "object") return null;
  for (const k of keys) {
    if (!isBlank(obj[k])) return obj[k];
  }
  return null;
}

function boolFrom01(v) {
  if (isBlank(v)) return null;
  if (typeof v === "boolean") return v;

  const n = Number(v);
  if (Number.isFinite(n)) return n === 1;

  const s = String(v).trim().toLowerCase();
  if (["true", "yes", "y", "checked", "on"].includes(s)) return true;
  if (["false", "no", "n", "unchecked", "off"].includes(s)) return false;

  return null;
}

function getFieldSetFromRecords(records = []) {
  const out = new Set();
  for (const r of records) {
    for (const k of Object.keys(r.fields || {})) out.add(k);
  }
  return out;
}

function safeSet(outObj, fieldSet, fieldName, value) {
  if (!fieldSet.has(fieldName)) return;
  if (value === undefined) return;
  outObj[fieldName] = value;
}

function chunk(arr, size) {
  const out = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

function classKey(show_id, class_id) {
  return `${show_id}:${class_id}`;
}

function tripUuid(t) {
  return strOrNull(
    firstNonBlank(
      t?.entryxclasses_uuid,
      t?.entryxclassesUUID,
      t?.entryxclasses_id,
      t?.entry_x_classes_uuid
    )
  );
}

const IGNORE_NUM = {
  time_any: new Set([0]),
  score_any: new Set([0]),
  placing: new Set([0, 10000, 100000]),
  position: new Set([0, 10000, 100000]),
  order_of_go: new Set([0, 10000, 100000]),
  timeallowed: new Set([0, 10000, 100000]),
};

const IGNORE_TIME_STR = new Set(["00:00:00"]);

function normNum(n, ignoreSet) {
  if (n === null || n === undefined) return null;
  if (!Number.isFinite(n)) return null;
  if (ignoreSet && ignoreSet.has(n)) return null;
  return n;
}

function normTimeStr(s) {
  const v = strOrNull(s);
  if (v === null) return null;
  if (IGNORE_TIME_STR.has(v)) return null;
  return v;
}

function normStr(s) {
  return strOrNull(s);
}

function isValidHms(t) {
  return typeof t === "string" && /^\d{2}:\d{2}:\d{2}$/.test(t);
}

function hmsToSeconds(hms) {
  const [h, m, s] = hms.split(":").map(Number);
  return (h * 3600) + (m * 60) + s;
}

function secondsToHms(sec) {
  sec = ((sec % 86400) + 86400) % 86400;
  const h = String(Math.floor(sec / 3600)).padStart(2, "0");
  const m = String(Math.floor((sec % 3600) / 60)).padStart(2, "0");
  const s = String(sec % 60).padStart(2, "0");
  return `${h}:${m}:${s}`;
}

function getSecondsPerTrip(timeallowed_tripone_raw) {
  const v = Number(timeallowed_tripone_raw);
  if (!Number.isFinite(v) || IGNORE_NUM.timeallowed.has(v)) return 80;
  return v;
}

function computeCwfEstimatedGoTime({ class_type, order_of_go, anchor_time, timeallowed_tripone }) {
  if (class_type !== "Jumpers") return null;

  const o = Number(order_of_go);
  if (!Number.isFinite(o) || o <= 0 || IGNORE_NUM.order_of_go.has(o)) return null;

  if (!isValidHms(anchor_time)) return null;

  const secondsPerTrip = getSecondsPerTrip(timeallowed_tripone);
  const outSec = hmsToSeconds(anchor_time) + ((o - 1) * secondsPerTrip);
  return secondsToHms(outSec);
}

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

  for (const batch of chunk(updates, 10)) {
    const res = await fetchWithRetry(airtableUrl(tableName), {
      method: "PATCH",
      headers: {
        Authorization: `Bearer ${AIRTABLE_TOKEN}`,
        "Content-Type": "application/json"
      },
      body: JSON.stringify({ records: batch })
    });

    if (!res.ok) {
      const body = await res.text().catch(() => "");
      throw new Error(`Airtable patch failed (${res.status}) ${tableName}: ${body}`);
    }
  }
}

function resolvedShowIdFromRecord(fields) {
  return numOrNull(firstNonBlank(fields.app_show_id, fields.show_id));
}

(async () => {
  try {
    requireEnv("AIRTABLE_TOKEN", AIRTABLE_TOKEN);
    requireEnv("AIRTABLE_BASE_ID", AIRTABLE_BASE_ID);

    const recordsAll = await airtableList(WATCH_TABLE, WATCH_VIEW);
    const records = MAX_RECORDS > 0 ? recordsAll.slice(0, MAX_RECORDS) : recordsAll;
    const watchFields = getFieldSetFromRecords(records);
    const observedAt = new Date().toISOString();

    const recInputs = [];
    const uniqueKeys = new Set();

    for (const r of records) {
      const f = r.fields || {};
      const resolved_show_id = resolvedShowIdFromRecord(f);
      const class_id = numOrNull(f.class_id);
      const entryxclasses_uuid = normStr(f.entryxclasses_uuid);

      recInputs.push({
        rec: r,
        resolved_show_id,
        class_id,
        entryxclasses_uuid,
        rowFields: f,
      });

      if (resolved_show_id !== null && class_id !== null) {
        uniqueKeys.add(classKey(resolved_show_id, class_id));
      }
    }

    const classCache = new Map();

    for (const key of uniqueKeys) {
      const [show_id_s, class_id_s] = key.split(":");
      const show_id = Number(show_id_s);
      const class_id = Number(class_id_s);

      const classUrl =
        `https://broad-tooth-b8ed.gombcg.workers.dev/classes/${encodeURIComponent(
          class_id
        )}/?show_id=${encodeURIComponent(show_id)}&customer_id=${encodeURIComponent(CUSTOMER_ID)}`;

      try {
        const res = await fetchWithRetry(classUrl, { method: "GET" });
        if (!res.ok) throw new Error(`Class ping failed: ${res.status} ${res.statusText}`);
        const json = await res.json();
        classCache.set(key, { ok: true, json });
      } catch {
        classCache.set(key, { ok: false, json: null });
      }
    }

    const updates = [];

    let processed_in_view = records.length;
    let processed_valid = 0;
    let updated_rows = 0;
    let skipped_missing_required = 0;
    let class_fetch_errors = 0;
    let trip_matched = 0;
    let trip_not_found = 0;

    for (const row of recInputs) {
      const { rec, resolved_show_id, class_id, entryxclasses_uuid } = row;

      if (resolved_show_id === null || class_id === null || !entryxclasses_uuid) {
        skipped_missing_required++;

        const u = {};
        safeSet(u, watchFields, "hb_second_pass_at", observedAt);
        safeSet(u, watchFields, "observed_at", observedAt);

        if (Object.keys(u).length > 0) updates.push({ id: rec.id, fields: u });
        continue;
      }

      processed_valid++;

      const key = classKey(resolved_show_id, class_id);
      const cached = classCache.get(key);

      if (!cached || !cached.ok) {
        class_fetch_errors++;

        const u = {};
        safeSet(u, watchFields, "hb_second_pass_at", observedAt);
        safeSet(u, watchFields, "observed_at", observedAt);

        if (Object.keys(u).length > 0) updates.push({ id: rec.id, fields: u });
        continue;
      }

      const classJson = cached.json;
      const classRelated =
        classJson?.class_related_data && typeof classJson.class_related_data === "object"
          ? classJson.class_related_data
          : null;

      const class_status = normStr(
        firstNonBlank(
          pickFrom(classRelated, ["status", "class_status"]),
          pickFrom(classJson, ["status", "class_status"])
        )
      );

      const actual_time = normTimeStr(
        firstNonBlank(
          pickFrom(classRelated, ["actual_time"]),
          pickFrom(classJson, ["actual_time"])
        )
      );

      const estimated_time = normTimeStr(
        firstNonBlank(
          pickFrom(classRelated, ["estimated_time"]),
          pickFrom(classJson, ["estimated_time"])
        )
      );

      const default_time = normTimeStr(
        firstNonBlank(
          pickFrom(classRelated, ["default_time"]),
          pickFrom(classJson, ["default_time"])
        )
      );

      const estimated_end_time = normTimeStr(
        firstNonBlank(
          pickFrom(classRelated, ["estimated_end_time", "end_time", "estimated_end"]),
          pickFrom(classJson, ["estimated_end_time", "end_time", "estimated_end"])
        )
      );

      const remaining_trips = normNum(
        numOrNull(
          firstNonBlank(
            pickFrom(classRelated, ["remaining_trips"]),
            pickFrom(classJson, ["remaining_trips"])
          )
        ),
        null
      );

      const total_trips = normNum(
        numOrNull(
          firstNonBlank(
            pickFrom(classRelated, ["total_trips"]),
            pickFrom(classJson, ["total_trips"])
          )
        ),
        null
      );

      const class_type = normStr(
        firstNonBlank(
          pickFrom(classRelated, ["class_type", "type", "division_type"]),
          pickFrom(classJson, ["class_type", "type", "division_type"])
        )
      );

      const anchor_time = isValidHms(estimated_time)
        ? estimated_time
        : (isValidHms(default_time) ? default_time : null);

      const trips = Array.isArray(classRelated?.trips)
        ? classRelated.trips
        : Array.isArray(classJson?.trips)
        ? classJson.trips
        : [];

      const matchedTrip =
        trips.find((t) => {
          const k = tripUuid(t);
          return k && k === entryxclasses_uuid;
        }) || null;

      const trip_id_raw = matchedTrip
        ? numOrNull(firstNonBlank(matchedTrip.trip_id, matchedTrip.id, matchedTrip.tripId))
        : null;

      const order_of_go_raw = matchedTrip
        ? numOrNull(firstNonBlank(matchedTrip.order_of_go, matchedTrip.orderOfGo))
        : null;

      const score_raw = matchedTrip
        ? floatOrNull(firstNonBlank(matchedTrip.score, matchedTrip.points))
        : null;

      const placing_raw = matchedTrip
        ? numOrNull(firstNonBlank(matchedTrip.placing, matchedTrip.place))
        : null;

      const estimated_go_time_raw = matchedTrip
        ? strOrNull(firstNonBlank(matchedTrip.estimated_go_time, matchedTrip.estimatedGoTime))
        : null;

      const lastPlace_raw = matchedTrip
        ? numOrNull(firstNonBlank(matchedTrip.place, matchedTrip.placing))
        : null;

      const lastGoneIn_raw = matchedTrip
        ? floatOrNull(firstNonBlank(matchedTrip.gone_in, matchedTrip.goneIn))
        : null;

      const lastPosition_raw = matchedTrip
        ? numOrNull(firstNonBlank(matchedTrip.position))
        : null;

      const time_one_raw = matchedTrip
        ? floatOrNull(firstNonBlank(matchedTrip.time_one, matchedTrip.timeOne, matchedTrip.time1))
        : null;

      const time_two_raw = matchedTrip
        ? floatOrNull(firstNonBlank(matchedTrip.time_two, matchedTrip.timeTwo, matchedTrip.time2))
        : null;

      const time_three_raw = matchedTrip
        ? floatOrNull(firstNonBlank(matchedTrip.time_three, matchedTrip.timeThree, matchedTrip.time3))
        : null;

      const score1_raw = matchedTrip
        ? floatOrNull(firstNonBlank(matchedTrip.score1, matchedTrip.score_1))
        : null;

      const score2_raw = matchedTrip
        ? floatOrNull(firstNonBlank(matchedTrip.score2, matchedTrip.score_2))
        : null;

      const score3_raw = matchedTrip
        ? floatOrNull(firstNonBlank(matchedTrip.score3, matchedTrip.score_3))
        : null;

      const timeallowed_tripone_raw = matchedTrip
        ? numOrNull(
            firstNonBlank(
              matchedTrip.timeallowed_tripone,
              matchedTrip.time_allowed_tripone,
              matchedTrip.timeAllowedTripOne
            )
          )
        : null;

      const results_verified_raw = matchedTrip
        ? firstNonBlank(
            matchedTrip.results_verified,
            matchedTrip.resultsVerified,
            matchedTrip.results_verified_flag,
            matchedTrip.resultsVerifiedFlag
          )
        : null;

      const trip_id = trip_id_raw;
      const order_of_go = normNum(order_of_go_raw, IGNORE_NUM.order_of_go);
      const score = normNum(score_raw, IGNORE_NUM.score_any);
      const placing = normNum(placing_raw, IGNORE_NUM.placing);
      const estimated_go_time = normTimeStr(estimated_go_time_raw);

      const lastPlaceVal = normNum(lastPlace_raw, IGNORE_NUM.placing);
      const lastGoneInVal = lastGoneIn_raw;
      const lastPositionVal = normNum(lastPosition_raw, IGNORE_NUM.position);

      const time_one = normNum(time_one_raw, IGNORE_NUM.time_any);
      const time_two = normNum(time_two_raw, IGNORE_NUM.time_any);
      const time_three = normNum(time_three_raw, IGNORE_NUM.time_any);

      const score1 = normNum(score1_raw, IGNORE_NUM.score_any);
      const score2 = normNum(score2_raw, IGNORE_NUM.score_any);
      const score3 = normNum(score3_raw, IGNORE_NUM.score_any);

      const results_verified_bool = boolFrom01(results_verified_raw);
      const results_verified_checkbox = results_verified_bool === null ? false : results_verified_bool;

      const u = {};

      // Always stamp pass meta
      safeSet(u, watchFields, "hb_second_pass_at", observedAt);
      safeSet(u, watchFields, "observed_at", observedAt);

      // Class-level writes
      safeSet(u, watchFields, "last_status", class_status);
      safeSet(u, watchFields, "last_actual_time", actual_time);
      safeSet(u, watchFields, "last_estimated_time", estimated_time);

      safeSet(u, watchFields, "status", class_status);
      safeSet(u, watchFields, "estimated_start_time", estimated_time);
      safeSet(u, watchFields, "estimated_end_time", estimated_end_time);
      safeSet(u, watchFields, "remaining_trips", remaining_trips);
      safeSet(u, watchFields, "total_trips", total_trips);

      if (matchedTrip) {
        trip_matched++;

        safeSet(u, watchFields, "trip_id", trip_id);
        safeSet(u, watchFields, "last_order_of_go", order_of_go);
        safeSet(u, watchFields, "last_score", score);
        safeSet(u, watchFields, "last_placing", placing);
        safeSet(u, watchFields, "last_estimated_go_time", estimated_go_time);

        safeSet(u, watchFields, "lastPlace", lastPlaceVal);
        safeSet(u, watchFields, "lastGoneIn", lastGoneInVal);
        safeSet(u, watchFields, "lastPosition", lastPositionVal);

        safeSet(u, watchFields, "time_one", time_one);
        safeSet(u, watchFields, "time_two", time_two);
        safeSet(u, watchFields, "time_three", time_three);

        safeSet(u, watchFields, "score1", score1);
        safeSet(u, watchFields, "score2", score2);
        safeSet(u, watchFields, "score3", score3);

        safeSet(u, watchFields, "results_verified", results_verified_checkbox);

        // Direct fields
        safeSet(u, watchFields, "estimated_go_time", estimated_go_time);
        safeSet(u, watchFields, "order_of_go", order_of_go);

        const cwf_estimated_go_time = computeCwfEstimatedGoTime({
          class_type,
          order_of_go: order_of_go_raw,
          anchor_time,
          timeallowed_tripone: timeallowed_tripone_raw,
        });
        safeSet(u, watchFields, "cwf_estimated_go_time", cwf_estimated_go_time);
      } else {
        trip_not_found++;

        safeSet(u, watchFields, "trip_id", null);
        safeSet(u, watchFields, "last_order_of_go", null);
        safeSet(u, watchFields, "last_score", null);
        safeSet(u, watchFields, "last_placing", null);
        safeSet(u, watchFields, "last_estimated_go_time", null);

        safeSet(u, watchFields, "lastPlace", null);
        safeSet(u, watchFields, "lastGoneIn", null);
        safeSet(u, watchFields, "lastPosition", null);

        safeSet(u, watchFields, "time_one", null);
        safeSet(u, watchFields, "time_two", null);
        safeSet(u, watchFields, "time_three", null);

        safeSet(u, watchFields, "score1", null);
        safeSet(u, watchFields, "score2", null);
        safeSet(u, watchFields, "score3", null);

        safeSet(u, watchFields, "results_verified", false);
        safeSet(u, watchFields, "cwf_estimated_go_time", null);

        safeSet(u, watchFields, "estimated_go_time", null);
        safeSet(u, watchFields, "order_of_go", null);
      }

      if (Object.keys(u).length > 0) updates.push({ id: rec.id, fields: u });
    }

    if (DRY_RUN) {
      console.log(`DRY_RUN trips_tagger | rows=${records.length} updates=${updates.length}`);
    } else {
      for (const batch of chunk(updates, 10)) {
        await airtableBatchUpdate(WATCH_TABLE, batch);
        updated_rows += batch.length;
      }
    }

    console.log(
      JSON.stringify(
        {
          watch_table: WATCH_TABLE,
          watch_view: WATCH_VIEW,
          processed_in_view,
          processed_valid,
          updated_rows: DRY_RUN ? updates.length : updated_rows,
          skipped_missing_required,
          class_fetch_errors,
          trip_matched,
          trip_not_found,
          observed_at: observedAt,
        },
        null,
        2
      )
    );
  } catch (e) {
    const name = e?.name || "error";
    const msg = String(e?.message || e);
    console.log(`fatal: ${name} ${msg.slice(0, 240)}`);
    process.exit(0);
  }
})();
