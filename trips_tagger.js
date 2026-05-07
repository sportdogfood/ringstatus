// trips_tagger.js (REMOVE ALL MISMATCH LOGIC)
//
// Locked rules:
// - app_show_id/app_sql_date/app_time come from latest heartbeat by default
// - /ring remains available only when APP_CONTEXT_SOURCE=ring|sgl|auto
// - no timezone conversion
// - no date_obj / time_obj usage
// - no UTC/local math
// - DAY uses raw endpoint sql_date text
// - NIGHT shifts sql_date text to next text date using literal calendar arithmetic
// - OVERNIGHT uses raw endpoint sql_date text
// - shifted_to_next_day is true only in NIGHT
// - no mismatch fields
// - no mismatch comparisons
// - also binds watch_trips.shows by matching shows.show_id === app_show_id

const AIRTABLE_TOKEN   = process.env.AIRTABLE_TOKEN || "";
const AIRTABLE_BASE_ID = process.env.AIRTABLE_BASE_ID || "";

const {
  assertValidPayload,
  isSoftPayloadError,
  softPayloadLogFields,
} = require("./lib/soft_payload_guard");
const {
  fetchTextWithConfiguredTransport,
} = require("./lib/sgl_fetch_adapter");
const {
  buildClassDetailEndpoint,
  buildClassSignupGroupEndpoint,
  classSignupEntries,
  findClassGroupOrderEntry,
  findClassSignupEntry,
  findClassTrip,
  normalizeClassEndpointWithCgid,
} = require("./lib/watch_trips_enrichment");
const {
  buildGroupsLiveMap,
  buildLiveClassDataEndpoint,
  findLiveClassTrip,
  normalizeLiveClassDataPayload,
} = require("./lib/liveclassv2_enrichment");
const {
  recordMatchesAppScope,
} = require("./lib/watch_trips_scope");

const WATCH_TABLE = process.env.WATCH_TABLE || "watch_trips";
const WATCH_VIEW  = process.env.WATCH_VIEW || "heartbeat";
const SHOWS_TABLE = process.env.SHOWS_TABLE || "shows";
const TABLE_GROUPS_LIVE = process.env.TABLE_GROUPS_LIVE || "groups_live";
const TABLE_HEARTBEAT = process.env.TABLE_HEARTBEAT || "heartbeat";
const HEARTBEAT_SORT_FIELD = process.env.HEARTBEAT_SORT_FIELD || "hb_at";
const MAX_RECORDS = Number(process.env.MAX_RECORDS || "500");
const CUSTOMER_ID = Number(process.env.CUSTOMER_ID || "15");
const APP_CONTEXT_SOURCE = String(process.env.APP_CONTEXT_SOURCE || "heartbeat").trim().toLowerCase();
const BASE_URL = String(
  process.env.SGL_DATA_BASE_URL ||
  process.env.SGL_DIRECT_BASE_URL ||
  process.env.SGL_API_BASE_URL ||
  process.env.BASE_URL ||
  "https://sglapi.wellingtoninternational.com"
).trim().replace(/\/+$/, "");

const HTTP_TIMEOUT_MS   = Number(process.env.HTTP_TIMEOUT_MS || "20000");
const AT_RETRY_ATTEMPTS = Number(process.env.AT_RETRY_ATTEMPTS || "3");
const AT_RETRY_BASE_MS  = Number(process.env.AT_RETRY_BASE_MS || "400");
const AT_RETRY_MAX_MS   = Number(process.env.AT_RETRY_MAX_MS || "2000");
const DRY_RUN           = String(process.env.DRY_RUN || "0") === "1";

const APP_RING_ENDPOINT = process.env.APP_RING_ENDPOINT || `${BASE_URL}/ring?customer_id=${encodeURIComponent(CUSTOMER_ID)}`;
const LIVECLASS_BASE_URL = String(
  process.env.LIVECLASS_BASE_URL ||
  "https://sgl.wellingtoninternational.com/iphonev2/index.php/esp/liveclassv2"
).trim().replace(/\/+$/, "");

// watch_trips source fields
const FIELD_CLASS_ENDPOINT        = process.env.FIELD_CLASS_ENDPOINT || "class_endpoint";
const FIELD_ENTRYXCLASSES_UUID    = process.env.FIELD_ENTRYXCLASSES_UUID || "entryxclasses_uuid";
const FIELD_ENTRY_ID              = process.env.FIELD_ENTRY_ID || "entry_id";
const FIELD_ENTRY_NUMBER          = process.env.FIELD_ENTRY_NUMBER || "entry_number";
const FIELD_CLASS_ID              = process.env.FIELD_CLASS_ID || "class_id";
const FIELD_CLASS_NUMBER          = process.env.FIELD_CLASS_NUMBER || "class_number";
const FIELD_CLASS_GROUP_ID        = process.env.FIELD_CLASS_GROUP_ID || "class_group_id";

// app context fields written back
const FIELD_APP_SHOW_ID           = process.env.FIELD_APP_SHOW_ID || "app_show_id";
const FIELD_APP_SQL_DATE          = process.env.FIELD_APP_SQL_DATE || "app_sql_date";
const FIELD_APP_TIME              = process.env.FIELD_APP_TIME || "app_time";

// tag output fields
const FIELD_MODE                  = process.env.FIELD_MODE || "mode";
const FIELD_SHIFTED_NEXT_DAY      = process.env.FIELD_SHIFTED_NEXT_DAY || "shifted_to_next_day";

// shows linkage
const FIELD_SHOW_ID               = process.env.FIELD_SHOW_ID || "show_id";
const FIELD_LINK_SHOWS            = process.env.FIELD_LINK_SHOWS || "shows";

// pass / audit fields
const FIELD_HB_SECOND_PASS_AT     = process.env.FIELD_HB_SECOND_PASS_AT || "hb_second_pass_at";
const FIELD_HB_SECOND_PASS_REASON = process.env.FIELD_HB_SECOND_PASS_REASON || "hb_second_pass_reason";
const FIELD_HB_SECOND_PASS_DONE   = process.env.FIELD_HB_SECOND_PASS_DONE || "hb_second_pass_done";

// enrichment fields
const FIELD_STATUS                = process.env.FIELD_STATUS || "status";
const FIELD_ESTIMATED_START_TIME  = process.env.FIELD_ESTIMATED_START_TIME || "estimated_start_time";
const FIELD_ESTIMATED_END_TIME    = process.env.FIELD_ESTIMATED_END_TIME || "estimated_end_time";
const FIELD_ESTIMATED_GO_TIME     = process.env.FIELD_ESTIMATED_GO_TIME || "estimated_go_time";
const FIELD_ORDER_OF_GO           = process.env.FIELD_ORDER_OF_GO || "order_of_go";
const FIELD_REMAINING_TRIPS       = process.env.FIELD_REMAINING_TRIPS || "remaining_trips";
const FIELD_TOTAL_TRIPS           = process.env.FIELD_TOTAL_TRIPS || "total_trips";
const FIELD_COMPLETED_TRIPS       = process.env.FIELD_COMPLETED_TRIPS || "completed_trips";
const FIELD_ACTUAL_TIME           = process.env.FIELD_ACTUAL_TIME || "actual_time";
const FIELD_ESTIMATED_TIME        = process.env.FIELD_ESTIMATED_TIME || "estimated_time";
const FIELD_RESULTS_VERIFIED      = process.env.FIELD_RESULTS_VERIFIED || "results_verified";
const FIELD_TOTAL_ENTRY_TRIPS     = process.env.FIELD_TOTAL_ENTRY_TRIPS || "total_entry_trips";
const FIELD_ACTUAL_ORDER          = process.env.FIELD_ACTUAL_ORDER || "actual_order";
const FIELD_ACTUAL_GO             = process.env.FIELD_ACTUAL_GO || "actual_go";
const FIELD_H_EID                 = process.env.FIELD_H_EID || "h_eid";
const FIELD_TIME_FAULT_ONE        = process.env.FIELD_TIME_FAULT_ONE || "time_fault_one";
const FIELD_FAULTS_ONE            = process.env.FIELD_FAULTS_ONE || "faults_one";
const FIELD_TIME_FAULTS_TWO       = process.env.FIELD_TIME_FAULTS_TWO || "time_faults_two";
const FIELD_FAULTS_TWO            = process.env.FIELD_FAULTS_TWO || "faults_two";
const FIELD_PLACING               = process.env.FIELD_PLACING || "placing";
const FIELD_GONE_IN               = process.env.FIELD_GONE_IN || "gone_in";
const FIELD_SCORE                 = process.env.FIELD_SCORE || "score";
const FIELD_TIME_ONE              = process.env.FIELD_TIME_ONE || "time_one";
const FIELD_TIME_TWO              = process.env.FIELD_TIME_TWO || "time_two";
const FIELD_TIME_THREE            = process.env.FIELD_TIME_THREE || "time_three";
const FIELD_SCORE1                = process.env.FIELD_SCORE1 || "score1";
const FIELD_SCORE2                = process.env.FIELD_SCORE2 || "score2";
const FIELD_SCORE3                = process.env.FIELD_SCORE3 || "score3";

const PROTECTED_WATCH_TRIPS_FIELDS = new Set([
  FIELD_STATUS,
  FIELD_ESTIMATED_START_TIME,
  FIELD_ESTIMATED_END_TIME,
  FIELD_ORDER_OF_GO,
  FIELD_REMAINING_TRIPS,
  FIELD_TOTAL_TRIPS,
  FIELD_COMPLETED_TRIPS,
  FIELD_ACTUAL_TIME,
  FIELD_ESTIMATED_TIME,
  FIELD_ESTIMATED_GO_TIME,
  FIELD_H_EID,
  FIELD_TIME_ONE,
  FIELD_TIME_TWO,
  FIELD_TIME_THREE,
  FIELD_SCORE,
  FIELD_SCORE1,
  FIELD_SCORE2,
  FIELD_SCORE3,
  FIELD_PLACING,
  FIELD_GONE_IN
]);
let CAN_WRITE_ACTUAL_GO           = false;

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

function boolValue(value) {
  if (typeof value === "boolean") return value;
  if (typeof value === "number") return value !== 0;
  if (typeof value === "string") {
    const text = value.trim().toLowerCase();
    if (["true", "yes", "1", "checked"].includes(text)) return true;
    if (["false", "no", "0", "unchecked"].includes(text)) return false;
  }
  return false;
}

function strOrNull(v) {
  if (isBlank(v)) return null;
  return String(v).trim();
}

function normalizeClassEndpoint(v, classGroupId) {
  const raw = strOrNull(v);
  if (!raw) return null;
  return normalizeClassEndpointWithCgid(raw, classGroupId);
}

function endpointPathKind(endpoint) {
  const raw = String(endpoint || "");
  let path = raw.toLowerCase();
  try {
    path = new URL(raw).pathname.toLowerCase();
  } catch {}
  if (path.includes("/classsignup/")) return "classsignup_detail";
  if (path.includes("/classes/")) return "class";
  return "unknown";
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

function chunk(arr, size) {
  const out = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
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
  order_of_go: new Set([0, 10000, 100000]),
};

const IGNORE_TIME_STR = new Set(["00:00:00"]);

function normNum(n, ignoreSet = null) {
  if (n === null || n === undefined) return null;
  if (!Number.isFinite(n)) return null;
  if (ignoreSet && ignoreSet.has(n)) return null;
  return n;
}

function firstNormNum(ignoreSet, ...values) {
  for (const value of values) {
    const normalized = normNum(numOrNull(value), ignoreSet);
    if (normalized !== null) return normalized;
  }
  return null;
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

function classSignupEntryHasUsableKeys(entry) {
  if (!entry || typeof entry !== "object") return false;
  return numOrNull(firstNonBlank(entry.entry_id, entry.entryId)) !== null ||
    numOrNull(firstNonBlank(entry.entry_number, entry.entryNumber, entry.number)) !== null ||
    numOrNull(firstNonBlank(entry.class_number, entry.classNumber)) !== null ||
    numOrNull(firstNonBlank(entry.class_id, entry.classId)) !== null ||
    !!strOrNull(entry.horse);
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

async function airtableTableFieldSet(tableName) {
  const res = await fetchWithRetry(`https://api.airtable.com/v0/meta/bases/${AIRTABLE_BASE_ID}/tables`, {
    headers: { Authorization: `Bearer ${AIRTABLE_TOKEN}` }
  });

  if (!res.ok) {
    const body = await res.text().catch(() => "");
    throw new Error(`Airtable meta failed (${res.status}) ${tableName}: ${body}`);
  }

  const json = await res.json().catch(() => ({}));
  const table = Array.isArray(json?.tables)
    ? json.tables.find((item) => String(item?.name || "").trim() === tableName)
    : null;

  return new Set(Array.isArray(table?.fields) ? table.fields.map((field) => String(field?.name || "").trim()).filter(Boolean) : []);
}

async function airtableList(tableName, viewName) {
  const out = [];
  let offset = null;

  while (true) {
    const url = new URL(airtableUrl(tableName));
    if (viewName) url.searchParams.set("view", viewName);
    url.searchParams.set("pageSize", "100");
    if (offset) url.searchParams.set("offset", offset);

    const res = await fetchWithRetry(url.toString(), {
      headers: { Authorization: `Bearer ${AIRTABLE_TOKEN}` }
    });

    if (!res.ok) {
      const body = await res.text().catch(() => "");
      throw new Error(`Airtable list failed (${res.status}) ${tableName}/${viewName || "-"}: ${body}`);
    }

    const json = await res.json().catch(() => ({}));
    out.push(...(json.records || []));
    offset = json.offset;
    if (!offset) break;
  }

  return out;
}

async function fetchLiveClassData(endpoint, expectedClassId) {
  const res = await fetchWithRetry(endpoint, {
    method: "GET",
    headers: {
      Accept: "application/json, text/plain, */*",
      "User-Agent": process.env.SGL_USER_AGENT || "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36 Edg/148.0.0.0",
      Referer: "https://www.wellingtoninternational.com/",
    },
  });
  const text = await res.text();

  if (!res.ok) {
    throw new Error(`err:liveclass_http_${res.status} body=${text.slice(0, 200)}`);
  }

  let json = null;
  try {
    json = JSON.parse(text);
  } catch {
    throw new Error(`err:liveclass_invalid_json body_length=${Buffer.byteLength(text || "", "utf8")}`);
  }

  if (!json || typeof json !== "object" || Array.isArray(json) || Object.keys(json).length === 0) {
    throw new Error(`soft_payload_empty body_length=${Buffer.byteLength(text || "", "utf8")}`);
  }

  const normalized = normalizeLiveClassDataPayload(json);
  const expected = numOrNull(expectedClassId);
  if (expected !== null && normalized.class_id !== expected) {
    throw new Error(`err:liveclass_id_mismatch expected=${expected} actual=${normalized.class_id}`);
  }
  if (!Array.isArray(json.rows)) {
    throw new Error(`err:liveclass_missing_rows keys=${Object.keys(json).join(",")}`);
  }

  return {
    ok: true,
    normalized,
    body_length: Buffer.byteLength(text || "", "utf8"),
  };
}

async function airtablePatchRecords(tableName, updates) {
  if (!updates.length) return;

  const res = await fetchWithRetry(airtableUrl(tableName), {
    method: "PATCH",
    headers: {
      Authorization: `Bearer ${AIRTABLE_TOKEN}`,
      "Content-Type": "application/json"
    },
    body: JSON.stringify({ records: updates })
  });

  if (!res.ok) {
    const body = await res.text().catch(() => "");
    throw new Error(`Airtable patch failed (${res.status}) ${tableName}: ${body}`);
  }
}

function isBlankPatchValue(value) {
  return value === null ||
    value === undefined ||
    (typeof value === "string" && value.trim() === "") ||
    (Array.isArray(value) && value.length === 0);
}

function sanitizeWatchTripsPatchUpdates(tableName, updates) {
  if (tableName !== WATCH_TABLE) return updates;

  return updates
    .map((row) => {
      const fields = { ...(row.fields || {}) };
      for (const fieldName of PROTECTED_WATCH_TRIPS_FIELDS) {
        if (Object.prototype.hasOwnProperty.call(fields, fieldName) && isBlankPatchValue(fields[fieldName])) {
          delete fields[fieldName];
        }
      }
      return { ...row, fields };
    })
    .filter((row) => Object.keys(row.fields || {}).length > 0);
}

async function airtablePatchWithFallback(tableName, updates) {
  const safeUpdates = sanitizeWatchTripsPatchUpdates(tableName, updates);
  if (!safeUpdates.length) return { okRows: 0, failedRows: [] };

  let okRows = 0;
  const failedRows = [];

  for (const batch of chunk(safeUpdates, 10)) {
    try {
      await airtablePatchRecords(tableName, batch);
      okRows += batch.length;
    } catch (batchErr) {
      console.log(`patch warn: batch failed, falling back to single-row updates :: ${String(batchErr?.message || batchErr).slice(0, 300)}`);

      for (const row of batch) {
        try {
          await airtablePatchRecords(tableName, [row]);
          okRows += 1;
        } catch (rowErr) {
          failedRows.push({
            record_id: row.id,
            reason: String(rowErr?.message || rowErr).slice(0, 300),
            attempted_fields: Object.keys(row.fields || {})
          });
          console.log(`row warn: ${row.id} :: ${String(rowErr?.message || rowErr).slice(0, 300)}`);
        }
      }
    }
  }

  return { okRows, failedRows };
}

function setBaseFields(updateFields, observedAt, reason) {
  updateFields[FIELD_HB_SECOND_PASS_AT] = observedAt;
  updateFields[FIELD_HB_SECOND_PASS_REASON] = reason;
  updateFields[FIELD_HB_SECOND_PASS_DONE] = true;
}

function setIfPresent(updateFields, fieldName, value) {
  if (isBlank(fieldName) || isBlank(value)) return false;
  updateFields[fieldName] = value;
  return true;
}

function setAppFields(updateFields, appCtx) {
  setIfPresent(updateFields, FIELD_APP_SHOW_ID, appCtx?.app_show_id);
  setIfPresent(updateFields, FIELD_APP_SQL_DATE, appCtx?.app_sql_date);
  setIfPresent(updateFields, FIELD_APP_TIME, appCtx?.app_time);
}

function setModeFields(updateFields, appCtx) {
  setIfPresent(updateFields, FIELD_MODE, appCtx?.mode);
  updateFields[FIELD_SHIFTED_NEXT_DAY] = !!appCtx?.shifted_to_next_day;
}

function setShowsLink(updateFields, showRecordId) {
  if (showRecordId) updateFields[FIELD_LINK_SHOWS] = [showRecordId];
}

function setClassLevelFields(updateFields, data) {
  setIfPresent(updateFields, FIELD_STATUS, data.class_status);
  setIfPresent(updateFields, FIELD_ESTIMATED_START_TIME, data.estimated_start_time);
  setIfPresent(updateFields, FIELD_ESTIMATED_END_TIME, data.estimated_end_time);
  setIfPresent(updateFields, FIELD_REMAINING_TRIPS, data.remaining_trips);
  setIfPresent(updateFields, FIELD_TOTAL_TRIPS, data.total_trips);
  setIfPresent(updateFields, FIELD_COMPLETED_TRIPS, data.completed_trips);
  setIfPresent(updateFields, FIELD_ACTUAL_TIME, data.actual_time);
  setIfPresent(updateFields, FIELD_ESTIMATED_TIME, data.estimated_time);
}

function setTripLevelFields(updateFields, data) {
  setIfPresent(updateFields, FIELD_ESTIMATED_GO_TIME, data.estimated_go_time);
  setIfPresent(updateFields, FIELD_ORDER_OF_GO, data.order_of_go);
  setIfPresent(updateFields, FIELD_TIME_ONE, data.time_one);
  setIfPresent(updateFields, FIELD_TIME_TWO, data.time_two);
  setIfPresent(updateFields, FIELD_TIME_THREE, data.time_three);
  setIfPresent(updateFields, FIELD_SCORE1, data.score1);
  setIfPresent(updateFields, FIELD_SCORE2, data.score2);
  setIfPresent(updateFields, FIELD_SCORE3, data.score3);

  setIfPresent(updateFields, FIELD_RESULTS_VERIFIED, data.results_verified);
  setIfPresent(updateFields, FIELD_TOTAL_ENTRY_TRIPS, data.total_entry_trips);
  setIfPresent(updateFields, FIELD_ACTUAL_ORDER, data.actual_order);
  if (CAN_WRITE_ACTUAL_GO) setIfPresent(updateFields, FIELD_ACTUAL_GO, data.actual_go);
  setIfPresent(updateFields, FIELD_H_EID, data.h_eid);
  setIfPresent(updateFields, FIELD_TIME_FAULT_ONE, data.time_fault_one);
  setIfPresent(updateFields, FIELD_FAULTS_ONE, data.faults_one);
  setIfPresent(updateFields, FIELD_TIME_FAULTS_TWO, data.time_faults_two);
  setIfPresent(updateFields, FIELD_FAULTS_TWO, data.faults_two);
  setIfPresent(updateFields, FIELD_PLACING, data.placing);
  setIfPresent(updateFields, FIELD_GONE_IN, data.gone_in);
  setIfPresent(updateFields, FIELD_SCORE, data.score);
}

function parseTimeToMinutes(appTime) {
  const v = strOrNull(appTime);
  if (!v) return null;

  const m = v.match(/^(\d{1,2}):(\d{2})\s*(AM|PM)$/i);
  if (!m) return null;

  let hh = Number(m[1]);
  const mm = Number(m[2]);
  const ap = m[3].toUpperCase();

  if (!Number.isFinite(hh) || !Number.isFinite(mm)) return null;
  if (hh < 1 || hh > 12 || mm < 0 || mm > 59) return null;

  if (ap === "AM") {
    if (hh === 12) hh = 0;
  } else {
    if (hh !== 12) hh += 12;
  }

  return hh * 60 + mm;
}

function shiftSqlDateText(rawSqlDate, days = 1) {
  const v = strOrNull(rawSqlDate);
  if (!v) return null;

  const m = v.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!m) return null;

  const year = Number(m[1]);
  const month = Number(m[2]);
  const day = Number(m[3]);

  if (!Number.isFinite(year) || !Number.isFinite(month) || !Number.isFinite(day)) return null;
  if (month < 1 || month > 12 || day < 1 || day > 31) return null;

  const date = new Date(Date.UTC(year, month - 1, day));
  if (
    date.getUTCFullYear() !== year ||
    date.getUTCMonth() !== month - 1 ||
    date.getUTCDate() !== day
  ) {
    return null;
  }

  date.setUTCDate(date.getUTCDate() + Number(days || 0));
  return date.toISOString().slice(0, 10);
}

function deriveMode(appTime) {
  const mins = parseTimeToMinutes(appTime);
  if (mins === null) return null;

  // DAY       = 5:00 AM - 4:59 PM
  // NIGHT     = 5:00 PM - 11:59 PM
  // OVERNIGHT = 12:00 AM - 4:59 AM
  if (mins >= 300 && mins <= 1019) return "DAY";
  if (mins >= 1020 && mins <= 1439) return "NIGHT";
  return "OVERNIGHT";
}

async function fetchAppContextFromRing() {
  const fetched = await fetchTextWithConfiguredTransport(APP_RING_ENDPOINT, async (endpoint) => {
    const response = await fetchWithRetry(endpoint, { method: "GET" });
    const text = await response.text();
    return { response, text, endpoint };
  });
  const res = fetched.response;
  const txt = fetched.text;
  const endpoint = fetched.endpoint || APP_RING_ENDPOINT;

  if (!res.ok) {
    throw new Error(`app endpoint failed (${res.status}): ${txt.slice(0, 300)}`);
  }

  let json = null;
  try {
    json = JSON.parse(txt);
  } catch {
    throw new Error(`app endpoint invalid json: ${txt.slice(0, 300)}`);
  }

  assertValidPayload({
    payload: json,
    text: txt,
    response: res,
    lane: "trips_tagger",
    endpoint,
    expectedTopLevelKeys: ["time_zone_date_time", "show", "show_id"],
  });

  const app_show_id = numOrNull(firstNonBlank(json?.show_id, json?.show?.show_id));
  const raw_sql_date = strOrNull(firstNonBlank(json?.time_zone_date_time?.sql_date, json?.show_date));
  const app_time = strOrNull(json?.time_zone_date_time?.time);

  if (app_show_id === null) throw new Error("app endpoint missing show_id");
  if (!raw_sql_date) throw new Error("app endpoint missing sql_date");
  if (!app_time) throw new Error("app endpoint missing time");

  const mode = deriveMode(app_time);
  if (!mode) throw new Error("unable to derive mode from app_time");

  let app_sql_date = raw_sql_date;
  let shifted_to_next_day = false;

  if (mode === "NIGHT") {
    app_sql_date = shiftSqlDateText(raw_sql_date, 1);
    if (!app_sql_date) throw new Error(`unable to shift sql_date text: ${raw_sql_date}`);
    shifted_to_next_day = true;
  }

  if (mode === "DAY" || mode === "OVERNIGHT") {
    app_sql_date = raw_sql_date;
    shifted_to_next_day = false;
  }

  return {
    app_show_id,
    raw_sql_date,
    app_sql_date,
    app_time,
    mode,
    shifted_to_next_day,
    source: "ring"
  };
}

async function fetchAppContextFromHeartbeat() {
  const url = new URL(airtableUrl(TABLE_HEARTBEAT));
  url.searchParams.set("pageSize", "1");
  url.searchParams.set("sort[0][field]", HEARTBEAT_SORT_FIELD);
  url.searchParams.set("sort[0][direction]", "desc");
  for (const fieldName of [
    "record_id",
    "heartbeat_id",
    "show_id",
    "app_show_id",
    "app_sql_date",
    "shifted_to_next_day",
    "mode",
    "time",
  ]) {
    url.searchParams.append("fields[]", fieldName);
  }

  const res = await fetchWithRetry(url.toString(), {
    headers: { Authorization: `Bearer ${AIRTABLE_TOKEN}` },
  });

  if (!res.ok) {
    const body = await res.text().catch(() => "");
    throw new Error(`Airtable heartbeat context failed (${res.status}): ${body.slice(0, 300)}`);
  }

  const json = await res.json().catch(() => ({}));
  const record = Array.isArray(json?.records) ? json.records[0] : null;
  const fields = record?.fields || {};

  const app_show_id = numOrNull(firstNonBlank(fields.app_show_id, fields.show_id));
  const app_sql_date = strOrNull(fields.app_sql_date);
  const app_time = strOrNull(fields.time);
  const mode = strOrNull(fields.mode) || deriveMode(app_time);

  if (!record?.id) throw new Error(`No heartbeat rows found in ${TABLE_HEARTBEAT}`);
  if (app_show_id === null) throw new Error("Latest heartbeat missing app_show_id/show_id");
  if (!app_sql_date) throw new Error("Latest heartbeat missing app_sql_date");

  return {
    app_show_id,
    raw_sql_date: app_sql_date,
    app_sql_date,
    app_time,
    mode,
    shifted_to_next_day: boolValue(fields.shifted_to_next_day),
    source: "heartbeat",
    heartbeat_record_id: record.id,
    scope_run_id: strOrNull(fields.heartbeat_id) || record.id,
  };
}

async function fetchAppContext() {
  if (APP_CONTEXT_SOURCE === "ring" || APP_CONTEXT_SOURCE === "sgl") {
    return fetchAppContextFromRing();
  }

  try {
    return await fetchAppContextFromHeartbeat();
  } catch (heartbeatError) {
    if (APP_CONTEXT_SOURCE === "auto" || APP_CONTEXT_SOURCE === "fallback") {
      console.log(`context warn: heartbeat context failed, falling back to /ring :: ${String(heartbeatError?.message || heartbeatError).slice(0, 300)}`);
      return fetchAppContextFromRing();
    }
    throw heartbeatError;
  }
}

async function fetchShowsMap() {
  const rows = await airtableList(SHOWS_TABLE, null);
  const out = new Map();

  for (const row of rows) {
    const showId = numOrNull(row?.fields?.[FIELD_SHOW_ID]);
    if (showId === null) continue;
    if (!out.has(showId)) out.set(showId, row.id);
  }

  return out;
}

(async () => {
  try {
    requireEnv("AIRTABLE_TOKEN", AIRTABLE_TOKEN);
    requireEnv("AIRTABLE_BASE_ID", AIRTABLE_BASE_ID);

    try {
      const watchTableFields = await airtableTableFieldSet(WATCH_TABLE);
      CAN_WRITE_ACTUAL_GO = watchTableFields.has(FIELD_ACTUAL_GO);
    } catch (e) {
      CAN_WRITE_ACTUAL_GO = false;
      console.log(`meta warn: unable to confirm ${WATCH_TABLE}.${FIELD_ACTUAL_GO} :: ${String(e?.message || e).slice(0, 300)}`);
    }

    const observedAt = new Date().toISOString();

    let appCtx = null;
    try {
      appCtx = await fetchAppContext();
    } catch (e) {
      if (isSoftPayloadError(e)) {
        console.log(JSON.stringify({
          ok: false,
          run_status: "SOFT_PAYLOAD_BLOCKED",
          reason: e?.reason || "soft_payload",
          watch_table: WATCH_TABLE,
          watch_view: WATCH_VIEW,
          app_endpoint: APP_RING_ENDPOINT,
          app_endpoint_failed: true,
          writes_blocked: true,
          observed_at: observedAt,
          ...softPayloadLogFields(e),
        }, null, 2));
        process.exitCode = 1;
        return;
      }

      const allRecords = await airtableList(WATCH_TABLE, WATCH_VIEW);
      const records = MAX_RECORDS > 0 ? allRecords.slice(0, MAX_RECORDS) : allRecords;

      const failUpdates = records.map((rec) => {
        const updateFields = {};
        setAppFields(updateFields, {
          app_show_id: null,
          app_sql_date: null,
          app_time: null
        });
        setModeFields(updateFields, {
          mode: null,
          shifted_to_next_day: false
        });
        setShowsLink(updateFields, null);
        setBaseFields(updateFields, observedAt, "err:app_endpoint_failed");
        return { id: rec.id, fields: updateFields };
      });

      let failedRows = [];
      let updated_rows = 0;

      if (DRY_RUN) {
        updated_rows = failUpdates.length;
        console.log(`DRY_RUN: app endpoint failed, updates=${failUpdates.length}`);
      } else {
        const result = await airtablePatchWithFallback(WATCH_TABLE, failUpdates);
        updated_rows = result.okRows;
        failedRows = result.failedRows;
      }

      console.log(JSON.stringify({
        watch_table: WATCH_TABLE,
        watch_view: WATCH_VIEW,
        processed_in_view: records.length,
        processed_valid: 0,
        updated_rows,
        failed_row_updates: failedRows.length,
        app_endpoint: APP_RING_ENDPOINT,
        app_endpoint_failed: true,
        app_error: String(e?.message || e).slice(0, 400),
        observed_at: observedAt,
        failed_row_samples: failedRows.slice(0, 10)
      }, null, 2));
      return;
    }

    const showsMap = await fetchShowsMap();
    const linkedShowRecordId = showsMap.get(appCtx.app_show_id) || null;

    const allRecords = await airtableList(WATCH_TABLE, WATCH_VIEW);
    const scopeRecords = allRecords.filter((rec) => recordMatchesAppScope(rec.fields || {}, appCtx));
    const records = MAX_RECORDS > 0 ? scopeRecords.slice(0, MAX_RECORDS) : scopeRecords;
    const recordsFilteredOutOfScope = allRecords.length - scopeRecords.length;
    const recordsSkippedByMax = scopeRecords.length - records.length;

    const recInputs = [];
    const uniqueEndpoints = new Set();
    const uniqueClassSignupEndpoints = new Set();

    for (const rec of records) {
      const f = rec.fields || {};
      const entry_id = numOrNull(f[FIELD_ENTRY_ID]);
      const entry_number = numOrNull(f[FIELD_ENTRY_NUMBER]);
      const class_id = numOrNull(f[FIELD_CLASS_ID]);
      const class_number = numOrNull(f[FIELD_CLASS_NUMBER]);
      const class_group_id = numOrNull(f[FIELD_CLASS_GROUP_ID]);
      let classEndpoint = normalizeClassEndpoint(f[FIELD_CLASS_ENDPOINT], class_group_id);
      if (class_id === null) {
        classEndpoint = null;
      }
      if (!classEndpoint && class_id !== null) {
        classEndpoint = buildClassDetailEndpoint({
          baseUrl: BASE_URL,
          classId: class_id,
          showId: appCtx.app_show_id,
          customerId: CUSTOMER_ID,
          classGroupId: class_group_id,
        });
      }
      const classSignupEndpoint = class_group_id !== null
        ? buildClassSignupGroupEndpoint({
          baseUrl: BASE_URL,
          classGroupId: class_group_id,
          entryId: entry_id,
          showId: appCtx.app_show_id,
          customerId: CUSTOMER_ID,
        })
        : null;
      const entryxclasses_uuid = normStr(f[FIELD_ENTRYXCLASSES_UUID]);

      recInputs.push({
        rec,
        classEndpoint,
        classSignupEndpoint,
        entryxclasses_uuid,
        entry_id,
        entry_number,
        class_id,
        class_number,
        class_group_id
      });

      if (classEndpoint) {
        uniqueEndpoints.add(classEndpoint);
      }
      if (classSignupEndpoint) {
        uniqueClassSignupEndpoints.add(classSignupEndpoint);
      }
    }

    let liveGroupsByGroupId = new Map();
    let liveGroupsError = null;
    try {
      const groupsLiveRows = await airtableList(TABLE_GROUPS_LIVE, null);
      liveGroupsByGroupId = buildGroupsLiveMap(groupsLiveRows, appCtx);
    } catch (e) {
      liveGroupsError = String(e?.message || e).slice(0, 300);
      console.log(`endpoint warn: err:groups_live_fetch_failed :: ${liveGroupsError}`);
    }

    const liveClassIds = new Set();
    for (const row of recInputs) {
      const liveGroup = liveGroupsByGroupId.get(String(row.class_group_id));
      if (!liveGroup) continue;
      for (const classId of liveGroup.class_ids || []) liveClassIds.add(String(classId));
    }

    const endpointCache = new Map();
    const endpointErrors = [];

    const liveClassDataCache = new Map();
    for (const classId of liveClassIds) {
      const endpoint = buildLiveClassDataEndpoint({
        baseUrl: LIVECLASS_BASE_URL,
        showId: appCtx.app_show_id,
        classId,
      });
      if (!endpoint) continue;
      try {
        const result = await fetchLiveClassData(endpoint, classId);
        liveClassDataCache.set(String(classId), {
          ok: true,
          endpoint,
          ...result,
        });
      } catch (e) {
        const reason = String(e?.message || e).slice(0, 300);
        liveClassDataCache.set(String(classId), {
          ok: false,
          endpoint,
          reason,
        });
        endpointErrors.push({ endpoint, reason });
        console.log(`endpoint warn: err:liveclass_fetch_failed :: ${endpoint} :: ${reason}`);
      }
    }

    function liveContextFor(row) {
      const liveGroup = liveGroupsByGroupId.get(String(row.class_group_id)) || null;
      if (!liveGroup) return { group: null, trip: null, classId: null };

      const preferredClassIds = [];
      if (row.class_id !== null && row.class_id !== undefined) preferredClassIds.push(String(row.class_id));
      for (const classId of liveGroup.class_ids || []) {
        const key = String(classId);
        if (!preferredClassIds.includes(key)) preferredClassIds.push(key);
      }

      for (const classId of preferredClassIds) {
        const cached = liveClassDataCache.get(String(classId));
        if (!cached?.ok) continue;
        const trip = findLiveClassTrip(cached.normalized, { entryNumber: row.entry_number });
        if (trip) return { group: liveGroup, trip, classId, payload: cached.normalized };
      }

      return { group: liveGroup, trip: null, classId: null };
    }

    for (const endpoint of uniqueEndpoints) {
      try {
        const fetched = await fetchTextWithConfiguredTransport(endpoint, async (targetEndpoint) => {
          const response = await fetchWithRetry(targetEndpoint, { method: "GET" });
          const text = await response.text();
          return { response, text, endpoint: targetEndpoint };
        });
        const res = fetched.response;
        const txt = fetched.text;
        const effectiveEndpoint = fetched.endpoint || endpoint;

        if (!res.ok) {
          const reason = `err:class_http_${res.status}`;
          endpointCache.set(endpoint, {
            ok: false,
            reason,
            detail: txt.slice(0, 300)
          });
          endpointErrors.push({
            endpoint: effectiveEndpoint,
            reason,
            detail: txt.slice(0, 300)
          });
          console.log(`endpoint warn: ${reason} :: ${effectiveEndpoint} :: ${txt.slice(0, 200)}`);
          continue;
        }

        let json = null;
        try {
          json = JSON.parse(txt);
        } catch {
          const reason = "err:class_invalid_json";
          endpointCache.set(endpoint, {
            ok: false,
            reason,
            detail: txt.slice(0, 300)
          });
          endpointErrors.push({
            endpoint: effectiveEndpoint,
            reason,
            detail: txt.slice(0, 300)
          });
          console.log(`endpoint warn: ${reason} :: ${effectiveEndpoint}`);
          continue;
        }

        try {
          const isClassEndpoint = endpointPathKind(effectiveEndpoint) === "class";
          assertValidPayload({
            payload: json,
            text: txt,
            response: res,
            lane: "trips_tagger",
            endpoint: effectiveEndpoint,
            expectedTopLevelKeys: isClassEndpoint
              ? ["class", "class_related_data", "trips", "status", "class_id", "number", "total_trips"]
              : [],
          });
        } catch (e) {
          if (!isSoftPayloadError(e)) throw e;

          const reason = e?.reason || "soft_payload";
          const detail = JSON.stringify(softPayloadLogFields(e)).slice(0, 300);
          endpointCache.set(endpoint, {
            ok: false,
            reason,
            detail
          });
          endpointErrors.push({
            endpoint: effectiveEndpoint,
            reason,
            detail
          });
          console.log(`endpoint warn: ${reason} :: ${effectiveEndpoint} :: ${detail}`);
          continue;
        }

        endpointCache.set(endpoint, { ok: true, json });
      } catch (e) {
        const reason = "err:class_fetch_exception";
        const detail = String(e?.message || e).slice(0, 300);
        endpointCache.set(endpoint, {
          ok: false,
          reason,
          detail
        });
        endpointErrors.push({
          endpoint,
          reason,
          detail
        });
        console.log(`endpoint warn: ${reason} :: ${endpoint} :: ${detail}`);
      }
    }

    const classSignupEndpointCache = new Map();
    for (const endpoint of uniqueClassSignupEndpoints) {
      try {
        const fetched = await fetchTextWithConfiguredTransport(endpoint, async (targetEndpoint) => {
          const response = await fetchWithRetry(targetEndpoint, { method: "GET" });
          const text = await response.text();
          return { response, text, endpoint: targetEndpoint };
        });
        const res = fetched.response;
        const txt = fetched.text;
        const effectiveEndpoint = fetched.endpoint || endpoint;

        if (!res.ok) {
          const reason = `err:classsignup_http_${res.status}`;
          classSignupEndpointCache.set(endpoint, {
            ok: false,
            reason,
            detail: txt.slice(0, 300)
          });
          endpointErrors.push({
            endpoint: effectiveEndpoint,
            reason,
            detail: txt.slice(0, 300)
          });
          console.log(`endpoint warn: ${reason} :: ${effectiveEndpoint} :: ${txt.slice(0, 200)}`);
          continue;
        }

        let json = null;
        try {
          json = JSON.parse(txt);
        } catch {
          const reason = "err:classsignup_invalid_json";
          classSignupEndpointCache.set(endpoint, {
            ok: false,
            reason,
            detail: txt.slice(0, 300)
          });
          endpointErrors.push({
            endpoint: effectiveEndpoint,
            reason,
            detail: txt.slice(0, 300)
          });
          console.log(`endpoint warn: ${reason} :: ${effectiveEndpoint}`);
          continue;
        }

        try {
          assertValidPayload({
            payload: json,
            text: txt,
            response: res,
            lane: "trips_tagger_classsignup",
            endpoint: effectiveEndpoint,
            expectedTopLevelKeys: ["show", "class_group", "entry_x_classes"],
            expectedPredicate(payload) {
              return Array.isArray(payload?.entry_x_classes);
            },
          });
        } catch (e) {
          if (!isSoftPayloadError(e)) throw e;

          const reason = e?.reason || "soft_payload";
          const detail = JSON.stringify(softPayloadLogFields(e)).slice(0, 300);
          classSignupEndpointCache.set(endpoint, {
            ok: false,
            reason,
            detail
          });
          endpointErrors.push({
            endpoint: effectiveEndpoint,
            reason,
            detail
          });
          console.log(`endpoint warn: ${reason} :: ${effectiveEndpoint} :: ${detail}`);
          continue;
        }

        const signupEntries = classSignupEntries(json);
        const usableEntryCount = signupEntries.filter(classSignupEntryHasUsableKeys).length;
        const payloadHasNoUsableEntryKeys = signupEntries.length > 0 && usableEntryCount === 0;
        const metadata = fetched.metadata || {};

        if (payloadHasNoUsableEntryKeys) {
          console.log(`endpoint warn: warn:classsignup_payload_no_usable_entry_keys :: ${effectiveEndpoint} :: entries=${signupEntries.length} authorization_used=${metadata.authorization_used === true}`);
        }

        classSignupEndpointCache.set(endpoint, {
          ok: true,
          json,
          entry_count: signupEntries.length,
          usable_entry_count: usableEntryCount,
          payload_has_no_usable_entry_keys: payloadHasNoUsableEntryKeys,
          authorization_used: metadata.authorization_used === true,
          cookie_header_used: metadata.cookie_header_used === true,
          session_json_used: metadata.session_json_used === true,
        });
      } catch (e) {
        const reason = "err:classsignup_fetch_exception";
        const detail = String(e?.message || e).slice(0, 300);
        classSignupEndpointCache.set(endpoint, {
          ok: false,
          reason,
          detail
        });
        endpointErrors.push({
          endpoint,
          reason,
          detail
        });
        console.log(`endpoint warn: ${reason} :: ${endpoint} :: ${detail}`);
      }
    }

    const softEndpointErrors = endpointErrors.filter((error) =>
      /^soft_payload_/i.test(String(error?.reason || ""))
    );
    if (softEndpointErrors.length) {
      console.log(JSON.stringify({
        ok: false,
        run_status: "SOFT_PAYLOAD_BLOCKED",
        reason: "soft_payload_empty",
        watch_table: WATCH_TABLE,
        watch_view: WATCH_VIEW,
        app_endpoint: APP_RING_ENDPOINT,
        writes_blocked: true,
        observed_at: observedAt,
        soft_endpoint_errors: softEndpointErrors.slice(0, 10),
      }, null, 2));
      process.exitCode = 1;
      return;
    }

    const classSignupEndpointWarnings = [...classSignupEndpointCache.entries()]
      .filter(([, cached]) => cached?.payload_has_no_usable_entry_keys)
      .map(([endpoint, cached]) => ({
        endpoint,
        reason: "classsignup_payload_no_usable_entry_keys",
        entry_count: cached.entry_count,
        usable_entry_count: cached.usable_entry_count,
        authorization_used: cached.authorization_used,
        cookie_header_used: cached.cookie_header_used,
        session_json_used: cached.session_json_used,
      }));

    const updates = [];
    const rowReasonCounts = {};

    let processed_in_view = records.length;
    let processed_valid = 0;
    let updated_rows = 0;
    let skipped_missing_class_endpoint = 0;
    let skipped_missing_entryxclasses_uuid = 0;
    let endpoint_fetch_errors = 0;
    let trip_matched = 0;
    let liveclass_trip_matched = 0;
    let liveclass_group_only = 0;
    let trip_not_found = 0;
    let shows_link_bound = 0;
    let shows_link_missing = 0;

    function bumpReason(reason) {
      rowReasonCounts[reason] = (rowReasonCounts[reason] || 0) + 1;
    }

    for (const row of recInputs) {
      const {
        rec,
        classEndpoint,
        classSignupEndpoint,
        entryxclasses_uuid,
        entry_id,
        entry_number,
        class_id,
        class_number,
        class_group_id
      } = row;

      const updateFields = {};
      setAppFields(updateFields, appCtx);
      setModeFields(updateFields, appCtx);
      setShowsLink(updateFields, linkedShowRecordId);
      const liveCtx = liveContextFor(row);

      if (linkedShowRecordId) shows_link_bound++;
      else shows_link_missing++;

      if (!classEndpoint) {
        if (liveCtx.group) {
          setClassLevelFields(updateFields, {
            class_status: liveCtx.group.status,
            estimated_start_time: liveCtx.group.estimated_start_time,
            total_trips: liveCtx.group.total,
            completed_trips: liveCtx.group.gone,
          });

          if (liveCtx.trip) {
            liveclass_trip_matched++;
            trip_matched++;
            setTripLevelFields(updateFields, {
              order_of_go: normNum(liveCtx.trip.order_of_go, IGNORE_NUM.order_of_go),
              actual_order: normNum(liveCtx.trip.actual_order),
              gone_in: normNum(liveCtx.trip.gone_in),
              h_eid: normNum(liveCtx.trip.entry_number),
            });
          } else {
            liveclass_group_only++;
          }

          let reason = liveCtx.trip ? "ok:liveclassv2_matched" : "warn:liveclassv2_group_only";
          if (!liveCtx.trip) reason = `${reason}|warn:missing_order_of_go`;
          if (!liveCtx.group.status) reason = `${reason}|warn:missing_status`;
          if (!linkedShowRecordId) reason = `${reason}|warn:shows_link_missing`;
          setBaseFields(updateFields, observedAt, reason);
          bumpReason(reason);
          updates.push({ id: rec.id, fields: updateFields });
          continue;
        }

        const classSignupCached = classSignupEndpoint ? classSignupEndpointCache.get(classSignupEndpoint) : null;
        const classSignupEntry = classSignupCached?.ok
          ? findClassSignupEntry(classSignupCached.json, {
            entryId: entry_id,
            entryNumber: entry_number,
            classNumber: class_number,
            classId: class_id,
          })
          : null;

        if (classSignupEntry) {
          const order_of_go = firstNormNum(
            IGNORE_NUM.order_of_go,
            classSignupEntry?.order_of_go,
            classSignupEntry?.orderOfGo
          );
          const h_eid = normNum(firstNonBlank(
            classSignupEntry?.entry_id,
            classSignupEntry?.entryId,
            classSignupEntry?.entry_number,
            classSignupEntry?.entryNumber,
            classSignupEntry?.number
          ));
          setIfPresent(updateFields, FIELD_ORDER_OF_GO, order_of_go);
          setIfPresent(updateFields, FIELD_H_EID, h_eid);

          let reason = "warn:classsignup_only";
          if (order_of_go === null) reason = `${reason}|warn:missing_order_of_go`;
          if (!linkedShowRecordId) reason = `${reason}|warn:shows_link_missing`;
          setBaseFields(updateFields, observedAt, reason);
          bumpReason(reason);
          updates.push({ id: rec.id, fields: updateFields });
          continue;
        }

        let reason = classSignupEndpoint ? "err:missing_class_endpoint|warn:no_classsignup_match" : "err:missing_class_endpoint";
        if (classSignupCached?.payload_has_no_usable_entry_keys) reason = `${reason}|warn:classsignup_payload_no_usable_entry_keys`;
        if (classSignupCached && classSignupCached.authorization_used === false) reason = `${reason}|warn:sgl_auth_not_used`;
        if (!linkedShowRecordId) reason = `${reason}|warn:shows_link_missing`;

        skipped_missing_class_endpoint++;
        setBaseFields(updateFields, observedAt, reason);
        bumpReason(reason);
        updates.push({ id: rec.id, fields: updateFields });
        continue;
      }

      const hasTripIdentity = !!entryxclasses_uuid ||
        (entry_id !== null && (class_number !== null || class_id !== null)) ||
        (entry_number !== null && (class_number !== null || class_id !== null));
      if (!hasTripIdentity) {
        let reason = "err:missing_trip_identity";
        if (!entryxclasses_uuid) reason = `${reason}|debug:missing_entryxclasses_uuid`;
        if (entry_id === null) reason = `${reason}|debug:missing_entry_id`;
        if (class_number === null) reason = `${reason}|debug:missing_class_number`;
        if (class_id === null) reason = `${reason}|debug:missing_class_id`;
        if (!linkedShowRecordId) reason = `${reason}|warn:shows_link_missing`;

        skipped_missing_entryxclasses_uuid++;
        setBaseFields(updateFields, observedAt, reason);
        bumpReason(reason);
        updates.push({ id: rec.id, fields: updateFields });
        continue;
      }

      processed_valid++;

      const cached = endpointCache.get(classEndpoint);
      if (!cached || !cached.ok) {
        let reason = cached?.reason || "err:class_fetch_unknown";
        if (!linkedShowRecordId) reason = `${reason}|warn:shows_link_missing`;

        endpoint_fetch_errors++;
        setBaseFields(updateFields, observedAt, reason);
        bumpReason(reason);
        updates.push({ id: rec.id, fields: updateFields });
        continue;
      }

      const classJson = cached.json;
      const classJsonKeys =
        classJson && typeof classJson === "object"
          ? Object.keys(classJson)
          : [];
      const classRelated =
        classJson?.class_related_data && typeof classJson.class_related_data === "object"
          ? classJson.class_related_data
          : null;
      const trips = Array.isArray(classRelated?.trips)
        ? classRelated.trips
        : Array.isArray(classJson?.trips)
        ? classJson.trips
        : [];

      const classPayloadEmpty =
        classJsonKeys.length === 0 ||
        (
          trips.length === 0 &&
          classJsonKeys.every((k) => k === "show_id")
        );

      if (classPayloadEmpty) {
        let reason = "err:class_empty_payload_preserved";
        if (!linkedShowRecordId) reason = `${reason}|warn:shows_link_missing`;
        endpoint_fetch_errors++;
        setBaseFields(updateFields, observedAt, reason);
        bumpReason(reason);
        endpointErrors.push({
          endpoint: classEndpoint,
          reason,
          detail: "{}"
        });
        updates.push({ id: rec.id, fields: updateFields });
        continue;
      }

      const class_status = normStr(
        firstNonBlank(
          pickFrom(classRelated, ["status", "class_status"]),
          pickFrom(classJson, ["status", "class_status"])
        )
      );

      const estimated_start_time = normTimeStr(
        firstNonBlank(
          pickFrom(classRelated, ["estimated_start_time", "estimated_time", "start_time"]),
          pickFrom(classJson, ["estimated_start_time", "estimated_time", "start_time"])
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
        )
      );

      const total_trips = normNum(
        numOrNull(
          firstNonBlank(
            pickFrom(classRelated, ["total_trips"]),
            pickFrom(classJson, ["total_trips"])
          )
        )
      );

      const completed_trips = normNum(
        numOrNull(
          firstNonBlank(
            pickFrom(classRelated, ["completed_trips"]),
            pickFrom(classJson, ["completed_trips"])
          )
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

      const matchedTrip = findClassTrip(classJson, { entryxclassesUuid: entryxclasses_uuid }) ||
        trips.find((t) => {
          const k = tripUuid(t);
          return k && k === entryxclasses_uuid;
        }) ||
        findClassTrip(classJson, {
          entryId: entry_id,
          entryNumber: entry_number,
          classId: class_id,
        }) ||
        null;
      const groupOrderEntry = findClassGroupOrderEntry(classJson, {
        entryxclassesUuid: entryxclasses_uuid,
        entryId: entry_id,
        entryNumber: entry_number,
        classId: class_id,
      });
      const classSignupCached = classSignupEndpoint ? classSignupEndpointCache.get(classSignupEndpoint) : null;
      const classSignupEntry = classSignupCached?.ok
        ? findClassSignupEntry(classSignupCached.json, {
          entryId: entry_id,
          entryNumber: entry_number,
          classNumber: class_number,
          classId: class_id,
        })
        : null;

      setClassLevelFields(updateFields, {
        class_status,
        estimated_start_time,
        estimated_end_time,
        remaining_trips,
        total_trips,
        completed_trips,
        actual_time,
        estimated_time
      });

      if (matchedTrip) {
        trip_matched++;

        const estimated_go_time = normTimeStr(
          firstNonBlank(matchedTrip.estimated_go_time, matchedTrip.estimatedGoTime)
        );

        const order_of_go = firstNormNum(
          IGNORE_NUM.order_of_go,
          matchedTrip.order_of_go,
          matchedTrip.orderOfGo,
          classSignupEntry?.order_of_go,
          classSignupEntry?.orderOfGo,
          groupOrderEntry?.order_of_go,
          groupOrderEntry?.orderOfGo
        );

        const time_one = normNum(
          floatOrNull(firstNonBlank(matchedTrip.time_one, matchedTrip.timeOne, matchedTrip.time1)),
          IGNORE_NUM.time_any
        );

        const time_two = normNum(
          floatOrNull(firstNonBlank(matchedTrip.time_two, matchedTrip.timeTwo, matchedTrip.time2)),
          IGNORE_NUM.time_any
        );

        const time_three = normNum(
          floatOrNull(firstNonBlank(matchedTrip.time_three, matchedTrip.timeThree, matchedTrip.time3)),
          IGNORE_NUM.time_any
        );

        const score1 = normNum(
          floatOrNull(firstNonBlank(matchedTrip.score1, matchedTrip.score_1)),
          IGNORE_NUM.score_any
        );

        const score2 = normNum(
          floatOrNull(firstNonBlank(matchedTrip.score2, matchedTrip.score_2)),
          IGNORE_NUM.score_any
        );

        const score3 = normNum(
          floatOrNull(firstNonBlank(matchedTrip.score3, matchedTrip.score_3)),
          IGNORE_NUM.score_any
        );

        const results_verified = normNum(
          numOrNull(firstNonBlank(matchedTrip.results_verified, matchedTrip.resultsVerified))
        );

        const total_entry_trips = normNum(
          numOrNull(firstNonBlank(matchedTrip.total_entry_trips, matchedTrip.totalEntryTrips))
        );

        const actual_order = normNum(
          numOrNull(firstNonBlank(matchedTrip.actual_order, matchedTrip.actualOrder))
        );

        const actual_go = normNum(
          numOrNull(firstNonBlank(matchedTrip.actual_go, matchedTrip.actualGo))
        );

        const h_eid = normNum(
          numOrNull(firstNonBlank(matchedTrip.number, matchedTrip.entry_number, matchedTrip.entryNumber))
        );

        const time_fault_one = normNum(
          floatOrNull(firstNonBlank(matchedTrip.time_fault_one, matchedTrip.timeFaultOne))
        );

        const faults_one = normNum(
          numOrNull(firstNonBlank(matchedTrip.faults_one, matchedTrip.faultsOne))
        );

        const time_faults_two = normNum(
          floatOrNull(firstNonBlank(matchedTrip.time_faults_two, matchedTrip.timeFaultsTwo))
        );

        const faults_two = normNum(
          numOrNull(firstNonBlank(matchedTrip.faults_two, matchedTrip.faultsTwo))
        );

        const placing = normNum(
          numOrNull(firstNonBlank(matchedTrip.placing))
        );

        const gone_in = normNum(
          numOrNull(firstNonBlank(matchedTrip.gone_in, matchedTrip.goneIn))
        );

        const score = normNum(
          floatOrNull(firstNonBlank(matchedTrip.score))
        );

        setTripLevelFields(updateFields, {
          estimated_go_time,
          order_of_go,
          time_one,
          time_two,
          time_three,
          score1,
          score2,
          score3,
          results_verified,
          total_entry_trips,
          actual_order,
          actual_go,
          h_eid,
          time_fault_one,
          faults_one,
          time_faults_two,
          faults_two,
          placing,
          gone_in,
          score
        });

        let reason = "ok:matched_trip";
        if (order_of_go === null) reason = `${reason}|warn:missing_order_of_go`;
        if (!class_status) reason = `${reason}|warn:missing_status`;
        if (!linkedShowRecordId) reason = `${reason}|warn:shows_link_missing`;

        setBaseFields(updateFields, observedAt, reason);
        bumpReason(reason);
      } else if (classSignupEntry || groupOrderEntry) {
        trip_not_found++;

        const order_of_go = firstNormNum(
          IGNORE_NUM.order_of_go,
          classSignupEntry?.order_of_go,
          classSignupEntry?.orderOfGo,
          groupOrderEntry?.order_of_go,
          groupOrderEntry?.orderOfGo
        );
        const h_eid = normNum(
          numOrNull(firstNonBlank(
            classSignupEntry?.entry_number,
            classSignupEntry?.entryNumber,
            classSignupEntry?.number
          ))
        );
        setIfPresent(updateFields, FIELD_ORDER_OF_GO, order_of_go);
        setIfPresent(updateFields, FIELD_H_EID, h_eid);

        let reason = classSignupEntry
          ? "warn:no_trip_match_classsignup_order_only"
          : "warn:no_trip_match_group_order_only";
        if (order_of_go === null) reason = `${reason}|warn:missing_order_of_go`;
        if (!class_status) reason = `${reason}|warn:missing_status`;
        if (!linkedShowRecordId) reason = `${reason}|warn:shows_link_missing`;

        setBaseFields(updateFields, observedAt, reason);
        bumpReason(reason);
      } else {
        trip_not_found++;

        let reason = "err:no_trip_match";
        reason = `${reason}|warn:missing_order_of_go`;
        if (entry_id === null) reason = `${reason}|debug:missing_entry_id`;
        if (class_number === null) reason = `${reason}|debug:missing_class_number`;
        if (class_id === null) reason = `${reason}|debug:missing_class_id`;
        if (!classSignupEndpoint) reason = `${reason}|debug:missing_classsignup_endpoint`;
        if (classSignupCached && !classSignupCached.ok) reason = `${reason}|warn:${classSignupCached.reason || "classsignup_fetch_failed"}`;
        if (classSignupCached?.payload_has_no_usable_entry_keys) reason = `${reason}|warn:classsignup_payload_no_usable_entry_keys`;
        if (classSignupCached && classSignupCached.authorization_used === false) reason = `${reason}|warn:sgl_auth_not_used`;
        if (!class_status) reason = `${reason}|warn:missing_status`;
        if (!linkedShowRecordId) reason = `${reason}|warn:shows_link_missing`;

        setBaseFields(updateFields, observedAt, reason);
        bumpReason(reason);
      }

      updates.push({ id: rec.id, fields: updateFields });
    }

    let failedRows = [];

    if (DRY_RUN) {
      updated_rows = updates.length;
      console.log(`DRY_RUN: updates=${updates.length}`);
    } else {
      const result = await airtablePatchWithFallback(WATCH_TABLE, updates);
      updated_rows = result.okRows;
      failedRows = result.failedRows;
    }

    console.log(JSON.stringify({
      watch_table: WATCH_TABLE,
      watch_view: WATCH_VIEW,
      shows_table: SHOWS_TABLE,
      app_endpoint: APP_RING_ENDPOINT,
      app_context_source: appCtx.source,
      app_show_id: appCtx.app_show_id,
      raw_sql_date: appCtx.raw_sql_date,
      app_sql_date: appCtx.app_sql_date,
      app_time: appCtx.app_time,
      mode: appCtx.mode,
      shifted_to_next_day: appCtx.shifted_to_next_day,
      shows_link_record_id: linkedShowRecordId,
      records_fetched_in_view: allRecords.length,
      records_scope_eligible: scopeRecords.length,
      records_filtered_out_of_scope: recordsFilteredOutOfScope,
      records_skipped_by_max_records: recordsSkippedByMax,
      processed_in_view,
      processed_valid,
      updated_rows,
      failed_row_updates: failedRows.length,
      skipped_missing_class_endpoint,
      skipped_missing_entryxclasses_uuid,
      unique_class_endpoints: uniqueEndpoints.size,
      unique_classsignup_endpoints: uniqueClassSignupEndpoints.size,
      endpoint_fetch_errors,
      trip_matched,
      trip_not_found,
      shows_link_bound,
      shows_link_missing,
      row_reason_counts: rowReasonCounts,
      observed_at: observedAt,
      classsignup_endpoint_warnings: classSignupEndpointWarnings.slice(0, 10),
      endpoint_error_samples: endpointErrors.slice(0, 10),
      failed_row_samples: failedRows.slice(0, 10)
    }, null, 2));
  } catch (e) {
    const name = e?.name || "error";
    const msg = String(e?.message || e);
    console.log(`fatal: ${name} ${msg.slice(0, 400)}`);
    process.exit(0);
  }
})();
