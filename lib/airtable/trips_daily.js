/**
 * Airtable Automation Script — WATCH PIPELINE (TRIGGER-RECORD DRIVEN) — FULL DROP
 *
 * Scope rules:
 *  - app_sid = authoritative numeric show scope from trigger
 *  - app_dt  = authoritative text date scope from trigger
 *  - NO date normalization
 *  - continue populating show_id for compatibility
 *  - continue populating date for compatibility
 *  - populate shows on all touched records using the matched record id from table "shows"
 *
 * Runtime rules:
 *  - operational issues should log to automation_errs and continue
 *  - do not hard-stop for normal fetch/write/data issues
 */

//////////////////////
// INPUTS (TRIGGER RECORD)
//////////////////////
const cfg = input.config();

const THIS_TABLE_NAME = String(cfg.thisTableName || "").trim();
const THIS_RECORD_ID  = String(cfg.thisRecordId || "").trim();

const SUCCESS_FIELD = "success";
const AUTOMATION_ERRS_TABLE = "automation_errs";
const AUTOMATION_NAME = String(cfg.automation_name || THIS_TABLE_NAME || "watch_pipeline").trim();

//////////////////////
// SAFE TABLE ACCESS
//////////////////////
function safeGetTable(name) {
  try {
    return base.getTable(name);
  } catch (_) {
    return null;
  }
}

function tableHasField(table, fieldName) {
  try {
    return !!table && table.fields.some(f => f.name === fieldName);
  } catch (_) {
    return false;
  }
}

function getWritableFieldSet(table) {
  try {
    return new Set((table?.fields || []).filter(f => !f.isComputed).map(f => f.name));
  } catch (_) {
    return new Set();
  }
}

function getFieldByName(table, fieldName) {
  try {
    return table?.fields?.find(f => f.name === fieldName) || null;
  } catch (_) {
    return null;
  }
}

const triggerTable = THIS_TABLE_NAME ? safeGetTable(THIS_TABLE_NAME) : null;
const automationErrsTable = safeGetTable(AUTOMATION_ERRS_TABLE);
const W_AUTOMATION_ERRS = getWritableFieldSet(automationErrsTable);

//////////////////////
// HELPERS
//////////////////////
const TZ = "America/New_York";

function ymdFromDateInTZ(dt, tz) {
  const parts = new Intl.DateTimeFormat("en-CA", {
    timeZone: tz,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).formatToParts(dt);

  const map = {};
  for (const p of parts) map[p.type] = p.value;
  return `${map.year}-${map.month}-${map.day}`;
}

function getRunDayYMD(tz) {
  return ymdFromDateInTZ(new Date(), tz);
}

function hasValue(v) {
  if (v === undefined || v === null) return false;
  if (typeof v === "string") return v.trim() !== "";
  return true;
}

function normalizeKeyStr(v) {
  if (v === undefined || v === null) return "";
  return String(v).trim();
}

function normalizeText(v) {
  return String(v ?? "").trim();
}

function getCellValueSafe(rec, fieldName) {
  try {
    return rec?.getCellValue(fieldName);
  } catch (_) {
    return undefined;
  }
}

function getCellValueFirst(rec, fieldNames = []) {
  for (const fieldName of (Array.isArray(fieldNames) ? fieldNames : [])) {
    if (!fieldName) continue;
    const value = getCellValueSafe(rec, fieldName);
    if (value !== undefined && value !== null && value !== "") return value;
  }
  return undefined;
}

function chunk50(arr) {
  const out = [];
  for (let i = 0; i < arr.length; i += 50) out.push(arr.slice(i, i + 50));
  return out;
}

function pushSample(list, value, limit = 10) {
  if (!Array.isArray(list) || list.length >= limit) return;
  const s = String(value ?? "").trim();
  if (!s) return;
  if (list.includes(s)) return;
  list.push(s);
}

function jsonForOutput(value, maxLen = 8000) {
  try {
    const s = JSON.stringify(value);
    if (s.length <= maxLen) return s;
    return `${s.slice(0, maxLen)}...(truncated)`;
  } catch (err) {
    return String(err?.message || err || value);
  }
}

function parseRunTimeMs(v) {
  if (!v) return 0;
  const s = String(v).trim();
  const ms = Date.parse(s);
  return Number.isFinite(ms) ? ms : 0;
}

function normalizeEntryNumber(v) {
  if (v === undefined || v === null) return undefined;
  if (typeof v === "number" && Number.isFinite(v)) return v;
  const s = String(v).trim();
  if (!s) return undefined;
  if (/^\d+$/.test(s)) return Number(s);
  return s;
}

function toIsoDateOnly(v) {
  if (v === undefined || v === null || v === "") return "";
  if (v instanceof Date) return v.toISOString().slice(0, 10);
  const s = String(v).trim();
  if (!s) return "";
  if (/^\d{4}-\d{2}-\d{2}$/.test(s)) return s;
  if (/^\d{4}-\d{2}-\d{2}T/.test(s)) return s.slice(0, 10);
  const ms = Date.parse(s);
  return Number.isFinite(ms) ? new Date(ms).toISOString().slice(0, 10) : "";
}

function dowNameFromSqlDate(sqlDate) {
  const iso = toIsoDateOnly(sqlDate);
  if (!iso) return "";
  const ms = Date.parse(`${iso}T00:00:00.000Z`);
  if (!Number.isFinite(ms)) return "";
  return ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"][new Date(ms).getUTCDay()] || "";
}

function buildMissingPayloadIds(pidTok) {
  const pidNum = Number(pidTok);
  if (!Number.isFinite(pidNum) || pidNum <= 0) return null;

  return {
    pidNum,
    class_group_id: -((pidNum * 10) + 1),
    class_id: -((pidNum * 10) + 2),
    entry_id: -((pidNum * 10) + 3),
  };
}

function toFiniteNumberOrNull(v) {
  if (v === undefined || v === null || v === "") return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function sameScopeSid(cellVal, appSidNum) {
  if (!Number.isFinite(appSidNum)) return false;
  const n = toFiniteNumberOrNull(cellVal);
  return n !== null && n === appSidNum;
}

function sameScopeDt(cellVal, appDtText) {
  return normalizeText(cellVal) === appDtText;
}

function linkedRecordIds(cellVal) {
  if (!Array.isArray(cellVal)) return [];
  return cellVal
    .map(x => String(x?.id || "").trim())
    .filter(Boolean);
}

function getScopeSpec(table, showRecordId = null) {
  return {
    hasAppSid: tableHasField(table, "app_sid"),
    hasAppDt: tableHasField(table, "app_dt"),
    hasShowId: tableHasField(table, "show_id"),
    hasDate: tableHasField(table, "date"),
    hasSid: tableHasField(table, "sid"),
    hasDt: tableHasField(table, "dt"),
    hasShows: !!showRecordId && tableHasField(table, "shows"),
  };
}

function getScopeFieldsToLoad(keyField, scopeSpec, extraFields = []) {
  const out = [];

  if (keyField) out.push(keyField);
  if (scopeSpec.hasAppSid) out.push("app_sid");
  if (scopeSpec.hasAppDt) out.push("app_dt");
  if (scopeSpec.hasShowId) out.push("show_id");
  if (scopeSpec.hasDate) out.push("date");
  if (scopeSpec.hasSid) out.push("sid");
  if (scopeSpec.hasDt) out.push("dt");
  if (scopeSpec.hasShows) out.push("shows");

  for (const fieldName of (Array.isArray(extraFields) ? extraFields : [])) {
    if (fieldName && !out.includes(fieldName)) out.push(fieldName);
  }

  return out;
}

function recordMatchesScope(rec, scopeSpec, scopeAppSid, scopeAppDt, showRecordId = null) {
  let sawScopeField = false;

  if (scopeSpec.hasShows && showRecordId) {
    sawScopeField = true;
    const showIds = linkedRecordIds(rec.getCellValue("shows"));
    if (showIds.includes(showRecordId)) return true;
  }

  if (scopeSpec.hasAppSid || scopeSpec.hasAppDt) {
    sawScopeField = true;
    const sidMatch = scopeSpec.hasAppSid ? sameScopeSid(rec.getCellValue("app_sid"), scopeAppSid) : true;
    const dtMatch = scopeSpec.hasAppDt ? sameScopeDt(rec.getCellValue("app_dt"), scopeAppDt) : true;
    if (sidMatch && dtMatch) return true;
  }

  if (scopeSpec.hasShowId || scopeSpec.hasDate) {
    sawScopeField = true;
    const sidMatch = scopeSpec.hasShowId ? sameScopeSid(rec.getCellValue("show_id"), scopeAppSid) : true;
    const dtMatch = scopeSpec.hasDate ? sameScopeDt(rec.getCellValue("date"), scopeAppDt) : true;
    if (sidMatch && dtMatch) return true;
  }

  if (scopeSpec.hasSid || scopeSpec.hasDt) {
    sawScopeField = true;
    const sidMatch = scopeSpec.hasSid ? sameScopeSid(rec.getCellValue("sid"), scopeAppSid) : true;
    const dtMatch = scopeSpec.hasDt ? sameScopeDt(rec.getCellValue("dt"), scopeAppDt) : true;
    if (sidMatch && dtMatch) return true;
  }

  return !sawScopeField;
}

function parseArrayishCell(cellVal) {
  const s = String(cellVal ?? "").trim();
  if (!s) return [];

  try {
    const v = JSON.parse(s);
    if (Array.isArray(v)) return v;
  } catch (_) {}

  return s
    .split(/[\n,]/g)
    .map(x => String(x || "").trim())
    .filter(Boolean);
}

function normalizePidToken(v) {
  const s = String(v ?? "").trim();
  if (!s) return "";
  if (s === "0") return "";
  if (s.toLowerCase() === "null") return "";
  const n = Number(s);
  if (!Number.isFinite(n) || n <= 0) return "";
  return String(Math.trunc(n));
}

function normalizePidList(list) {
  const out = [];
  for (const it of (Array.isArray(list) ? list : [])) {
    const tok = normalizePidToken(it);
    if (tok) out.push(tok);
  }
  return [...new Set(out)];
}

function normalizeEntryIdKey(v) {
  if (v === undefined || v === null) return "";
  if (typeof v === "number" && Number.isFinite(v)) return String(Math.trunc(v));
  const s = String(v).trim();
  if (!s) return "";
  const m1 = s.match(/^\d+$/);
  if (m1) return s;
  const m2 = s.match(/(\d+)/);
  return m2 ? m2[1] : "";
}

function normalizeEntryIdSet(list) {
  const set = new Set();
  for (const it of (Array.isArray(list) ? list : [])) {
    const k = normalizeEntryIdKey(it);
    if (k) set.add(k);
  }
  return set;
}

function parseLoanTokenList(list) {
  const out = [];
  for (const raw of (Array.isArray(list) ? list : [])) {
    const s = String(raw ?? "").trim();
    if (!s) continue;

    const m = s.match(/^(\d+)\.(\d+)$/);
    if (!m) continue;

    const sourcePid = normalizePidToken(m[1]);
    const entryId   = normalizeEntryIdKey(m[2]);
    if (!sourcePid || !entryId) continue;

    out.push({ sourcePid, entryId, pairKey: `${sourcePid}.${entryId}` });
  }
  return out;
}

function pickWritable(tableWritableSet, obj) {
  const out = {};
  for (const [k, v] of Object.entries(obj || {})) {
    if (!tableWritableSet.has(k)) continue;
    if (v === undefined || v === null) continue;

    if (typeof v === "string") {
      const vv = v.trim();
      if (!vv) continue;
      out[k] = vv;
      continue;
    }

    if (Array.isArray(v)) {
      if (v.length === 0) continue;
      out[k] = v;
      continue;
    }

    out[k] = v;
  }
  return out;
}

function linkOne(recordId) {
  return recordId ? [{ id: recordId }] : undefined;
}

function linkMany(recordIds) {
  if (!Array.isArray(recordIds) || recordIds.length === 0) return undefined;
  return recordIds.filter(Boolean).map(id => ({ id }));
}

async function runPool(items, concurrency, handler) {
  const queue = [...items];
  const workers = new Array(Math.max(1, concurrency)).fill(0).map(async () => {
    while (queue.length) {
      const item = queue.shift();
      await handler(item);
    }
  });
  await Promise.all(workers);
}

async function safeSetSuccess(msg) {
  try {
    if (!triggerTable || !THIS_RECORD_ID || !tableHasField(triggerTable, SUCCESS_FIELD)) return;
    await triggerTable.updateRecordAsync(THIS_RECORD_ID, { [SUCCESS_FIELD]: String(msg) });
  } catch (_) {}
}

const RUN_DAY = getRunDayYMD(TZ);
const RUN_ID  = RUN_DAY.replace(/-/g, "");
const CURRENT_RUN_TIME = new Date().toISOString();
const t0 = Date.now();
const MAX_FETCH_REQUESTS = 50;

let APP_DT_FOR_ERRS = "";
const automationErrQueue = [];
let automationErrWriteAttempts = 0;
let automationErrWriteSuccess = 0;
let automationErrWriteFailures = 0;
const automationErrWriteFailureMessages = [];
let fetchRequestCount = 0;
let fetchQuotaExceeded = false;
let fetchQuotaFirstBlockedUrl = "";

function coerceScalarForField(table, fieldName, value) {
  if (value === undefined || value === null || value === "") return undefined;

  const fieldType = String(getFieldByName(table, fieldName)?.type || "").trim();
  if (["number", "currency", "percent", "duration", "rating", "count", "autoNumber"].includes(fieldType)) {
    const n = toFiniteNumberOrNull(value);
    return n === null ? undefined : n;
  }

  if (fieldType === "checkbox") return !!value;

  const s = String(value).trim();
  return s || undefined;
}

function buildAutomationErrRow({
  error_type = "",
  message = "",
  pid = "",
  app_show_id = "",
  people_show_id = "",
  automation_key = "",
}) {
  if (!automationErrsTable) return null;

  const safeErrorType = String(error_type || "runtime_error").trim() || "runtime_error";
  const safePid = String(pid ?? "").trim();
  const safeAppShowId = app_show_id;
  const safePeopleShowId = people_show_id;
  const safeKey =
    String(automation_key || `${AUTOMATION_NAME}|${safePid || app_show_id || "na"}|${RUN_ID}|${safeErrorType}`)
      .trim();

  return pickWritable(W_AUTOMATION_ERRS, {
    automation_key: coerceScalarForField(automationErrsTable, "automation_key", safeKey),
    automation_name: coerceScalarForField(automationErrsTable, "automation_name", AUTOMATION_NAME),
    error_type: coerceScalarForField(automationErrsTable, "error_type", safeErrorType),
    app_sql_date: coerceScalarForField(automationErrsTable, "app_sql_date", APP_DT_FOR_ERRS),
    run_id: coerceScalarForField(automationErrsTable, "run_id", RUN_ID),
    last_run: coerceScalarForField(automationErrsTable, "last_run", APP_DT_FOR_ERRS),
    message: coerceScalarForField(automationErrsTable, "message", String(message || "").trim() || safeErrorType),
    pid: coerceScalarForField(automationErrsTable, "pid", safePid || undefined),
    app_show_id: coerceScalarForField(automationErrsTable, "app_show_id", safeAppShowId),
    people_show_id: coerceScalarForField(automationErrsTable, "people_show_id", safePeopleShowId),
  });
}

async function flushAutomationErrQueue() {
  if (!automationErrsTable) return;

  while (automationErrQueue.length) {
    const row = automationErrQueue.shift();
    if (!row || Object.keys(row).length === 0) continue;

    automationErrWriteAttempts += 1;
    try {
      await automationErrsTable.createRecordAsync(row);
      automationErrWriteSuccess += 1;
    } catch (err) {
      automationErrWriteFailures += 1;
      pushSample(automationErrWriteFailureMessages, String(err?.message || err), 10);
    }
  }
}

async function logAutomationErr({
  error_type = "",
  message = "",
  pid = "",
  app_show_id = "",
  people_show_id = "",
  automation_key = "",
}) {
  const row = buildAutomationErrRow({
    error_type,
    message,
    pid,
    app_show_id,
    people_show_id,
    automation_key,
  });
  if (!row || Object.keys(row).length === 0) return;
  automationErrQueue.push(row);
}

async function fetchJson(url) {
  if (fetchQuotaExceeded || fetchRequestCount >= MAX_FETCH_REQUESTS) {
    fetchQuotaExceeded = true;
    if (!fetchQuotaFirstBlockedUrl) fetchQuotaFirstBlockedUrl = String(url || "").trim();
    const err = new Error(`Fetch quota guard blocked request after ${fetchRequestCount}/${MAX_FETCH_REQUESTS} fetches.`);
    err.code = "fetch_quota_guard";
    throw err;
  }

  fetchRequestCount += 1;
  const res = await fetch(url);
  if (!res.ok) {
    const txt = await res.text().catch(() => "");
    throw new Error(`Fetch failed ${res.status} ${res.statusText} for ${url}\n${txt.slice(0, 400)}`);
  }
  return await res.json();
}

//////////////////////
// BOOT CHECKS
//////////////////////
if (!THIS_TABLE_NAME || !THIS_RECORD_ID || !triggerTable) {
  APP_DT_FOR_ERRS = "";
  await logAutomationErr({
    error_type: "boot_failure",
    message: `Missing or bad trigger context. thisTableName="${THIS_TABLE_NAME}" thisRecordId="${THIS_RECORD_ID}"`,
  });
  output.set("ok", false);
  output.set("reason", "boot_failure");
  await flushAutomationErrQueue();
  return;
}

const triggerRec = await triggerTable.selectRecordAsync(THIS_RECORD_ID);
if (!triggerRec) {
  APP_DT_FOR_ERRS = "";
  await logAutomationErr({
    error_type: "trigger_record_missing",
    message: `Trigger record not found: ${THIS_TABLE_NAME}/${THIS_RECORD_ID}`,
  });
  output.set("ok", false);
  output.set("reason", "trigger_record_missing");
  await flushAutomationErrQueue();
  return;
}

const appSidRaw = triggerRec.getCellValue("app_sid");
const appDtRaw  = triggerRec.getCellValue("app_dt");

const appSidNum = Number(appSidRaw);
const appDtText = String(appDtRaw ?? "").trim();
APP_DT_FOR_ERRS = appDtText;

if (!Number.isFinite(appSidNum) || appSidNum <= 0) {
  await safeSetSuccess("fail: no app_sid");
  await logAutomationErr({
    error_type: "missing_app_sid",
    message: "Missing app_sid on trigger record.",
  });
  output.set("ok", false);
  output.set("reason", "missing_app_sid");
  await flushAutomationErrQueue();
  return;
}

if (!appDtText) {
  await safeSetSuccess("fail: no app_dt");
  await logAutomationErr({
    error_type: "missing_app_dt",
    message: "Missing app_dt on trigger record.",
    app_show_id: appSidNum,
    people_show_id: appSidNum,
  });
  output.set("ok", false);
  output.set("reason", "missing_app_dt");
  await flushAutomationErrQueue();
  return;
}

await safeSetSuccess("running");

const base_url_raw = triggerRec.getCellValue("base_url");
const base_url = (String(base_url_raw || "https://broad-tooth-b8ed.gombcg.workers.dev"))
  .trim()
  .replace(/\/+$/, "");

const pull_pip_raw = triggerRec.getCellValue("pull_pid");
const pull_pip = Math.max(1, Number(pull_pip_raw || 2));

const pid1 = String(triggerRec.getCellValue("pid1") ?? "").trim();
const pid2 = String(triggerRec.getCellValue("pid2") ?? "").trim();
const pid3 = String(triggerRec.getCellValue("pid3") ?? "").trim();
const legacyPids = [pid1, pid2, pid3].filter(v => v && v !== "0" && String(v).toLowerCase() !== "null");

let pids_allowed = [];
if (tableHasField(triggerTable, "pids_allowed")) {
  const pids_allowed_raw = triggerRec.getCellValue("pids_allowed");
  pids_allowed = normalizePidList(parseArrayishCell(pids_allowed_raw));
}
const pids = (pids_allowed.length > 0)
  ? pids_allowed
  : normalizePidList(legacyPids);

const show_id = appSidNum;
const date = appDtText;
const customer_id = 15;

//////////////////////
// TABLES + KEYS
//////////////////////
const T_TRIPS      = "watch_trips";
const T_GROUPS     = "watch_groups";
const T_CLASSES    = "watch_classes";
const T_ENTRIES    = "watch_entries";
const T_SCHEDULE   = "watch_schedule";
const T_WW_TRAINER = "ww_trainers";
const T_SHOWS      = "shows";

const KEY_TRIPS   = "entryxclasses_uuid";
const KEY_GROUPS  = "class_group_id";
const KEY_CLASSES = "class_id";
const KEY_ENTRIES = "entry_id";

const tripsTable     = safeGetTable(T_TRIPS);
const groupsTable    = safeGetTable(T_GROUPS);
const classesTable   = safeGetTable(T_CLASSES);
const entriesTable   = safeGetTable(T_ENTRIES);
const watchScheduleTable = safeGetTable(T_SCHEDULE);
const wwTrainerTable = safeGetTable(T_WW_TRAINER);
const showsTable     = safeGetTable(T_SHOWS);

if (!tripsTable || !groupsTable || !classesTable || !entriesTable || !wwTrainerTable || !showsTable) {
  await logAutomationErr({
    error_type: "missing_core_table",
    message: `One or more core tables are missing.`,
    app_show_id: appSidNum,
    people_show_id: appSidNum,
  });
  output.set("ok", false);
  output.set("reason", "missing_core_table");
  await flushAutomationErrQueue();
  return;
}

const W_TRIPS   = getWritableFieldSet(tripsTable);
const W_GROUPS  = getWritableFieldSet(groupsTable);
const W_CLASSES = getWritableFieldSet(classesTable);
const W_ENTRIES = getWritableFieldSet(entriesTable);

//////////////////////
// SHOWS RESOLVE
//////////////////////
let SHOWS_RECORD_ID = null;

try {
  if (!tableHasField(showsTable, "show_id")) {
    await logAutomationErr({
      error_type: "missing_shows_show_id",
      message: `shows table missing field "show_id"`,
      app_show_id: appSidNum,
      people_show_id: appSidNum,
    });
  } else {
    const showsQuery = await showsTable.selectRecordsAsync({ fields: ["show_id"] });
    for (const rec of showsQuery.records) {
      const recShowId = rec.getCellValue("show_id");
      if (sameScopeSid(recShowId, appSidNum)) {
        SHOWS_RECORD_ID = rec.id;
        break;
      }
    }

    if (!SHOWS_RECORD_ID) {
      await logAutomationErr({
        error_type: "shows_record_not_found",
        message: `No shows record found where show_id = ${appSidNum}`,
        app_show_id: appSidNum,
        people_show_id: appSidNum,
      });
    }
  }
} catch (err) {
  await logAutomationErr({
    error_type: "shows_lookup_error",
    message: String(err?.message || err),
    app_show_id: appSidNum,
    people_show_id: appSidNum,
  });
}

function collectTripCandidates(obj, depth = 0, out = []) {
  if (depth > 6) return out;
  if (Array.isArray(obj)) {
    for (const it of obj) collectTripCandidates(it, depth + 1, out);
    return out;
  }
  if (!obj || typeof obj !== "object") return out;

  const hasClass = ("class_id" in obj) || ("classId" in obj);
  const hasHorse = ("horse" in obj) || ("Horse" in obj);
  const hasEntry = ("entry_id" in obj) || ("entryId" in obj) || ("entryxclasses_uuid" in obj);

  if (hasClass && hasEntry && hasHorse) out.push(obj);

  for (const v of Object.values(obj)) {
    collectTripCandidates(v, depth + 1, out);
  }
  return out;
}

function normalizePeopleTripRow(raw, ownerPid, sourcePid = null) {
  const class_id = raw?.class_id ?? raw?.classId ?? null;
  const entry_id = raw?.entry_id ?? raw?.entryId ?? null;
  const horse    = raw?.horse ?? raw?.Horse ?? "";
  const class_name = raw?.class_name ?? raw?.className ?? "";
  const class_number = raw?.class_number ?? raw?.classNumber ?? undefined;

  const entryxclasses_uuid = raw?.entryxclasses_uuid ?? raw?.entryxclassesUUID ?? raw?.uuid ?? "";

  const entry_number =
    raw?.entry_number ??
    raw?.entryNumber ??
    raw?.entry_no ??
    raw?.entryNo ??
    raw?.number ??
    undefined;

  if (!class_id || !entry_id) return null;
  if (!horse || String(horse).trim() === "") return null;

  return {
    pid: Number(ownerPid),
    source_pid: sourcePid ? Number(sourcePid) : undefined,
    class_id: Number(class_id),
    entry_id: Number(entry_id),
    entry_number: normalizeEntryNumber(entry_number),
    class_number: normalizeEntryNumber(class_number),
    class_name: String(class_name || "").trim(),
    horse: String(horse).trim(),
    entryxclasses_uuid: String(entryxclasses_uuid || "").trim(),
    rider_name: String(raw?.rider_name ?? raw?.riderName ?? "").trim(),
    rider_id: toFiniteNumberOrNull(raw?.rider_id ?? raw?.riderId) ?? undefined,
    placing: toFiniteNumberOrNull(raw?.placing) ?? undefined,
  };
}

//////////////////////
// UPSERT BY KEY
//////////////////////
async function upsertByKeyScoped(table, keyField, rows, writableSet, scopeAppSid, scopeAppDt, tableNameForErr, scopeShowRecordId = null) {
  const keyName = keyField;
  const diag = {
    table: table?.name || tableNameForErr,
    key_field: keyName,
    input_rows: Array.isArray(rows) ? rows.length : 0,
    writable_field_count: writableSet?.size || 0,
    writable_field_sample: [...(writableSet || new Set())].slice(0, 25),
    scope: null,
    existing_in_scope: 0,
    rows_missing_key: 0,
    rows_empty_after_pick: 0,
    to_create: 0,
    to_update: 0,
    created: 0,
    updated: 0,
    key_to_id_size: 0,
    sample_input_keys: [],
    sample_create_keys: [],
    sample_update_keys: [],
    sample_empty_field_keys: [],
    dropped_nonwritable_fields: [],
    create_errors: [],
    update_errors: [],
    created_after_batch_fallback: 0,
    updated_after_batch_fallback: 0,
    fatal_error: "",
  };

  if (!Array.isArray(rows) || rows.length === 0) {
    return { created: 0, updated: 0, keyToId: new Map(), diag };
  }

  try {
    const scopeSpec = getScopeSpec(table, scopeShowRecordId);
    const fieldsToLoad = getScopeFieldsToLoad(keyName, scopeSpec);
    diag.scope = {
      app_sid: scopeSpec.hasAppSid,
      app_dt: scopeSpec.hasAppDt,
      show_id: scopeSpec.hasShowId,
      date: scopeSpec.hasDate,
      shows: scopeSpec.hasShows,
      shows_record_id: scopeShowRecordId || "",
    };

    const q = await table.selectRecordsAsync({ fields: fieldsToLoad });

    const existing = new Map();
    for (const rec of q.records) {
      if (!recordMatchesScope(rec, scopeSpec, scopeAppSid, scopeAppDt, scopeShowRecordId)) continue;
      diag.existing_in_scope += 1;

      const kk = normalizeKeyStr(rec.getCellValue(keyName));
      if (!kk) continue;
      if (!existing.has(kk)) existing.set(kk, rec.id);
    }

    const toCreate = [];
    const toUpdate = [];
    const droppedNonWritable = new Set();

    for (const r of rows) {
      const kk = normalizeKeyStr(r?.[keyName]);
      if (!kk) {
        diag.rows_missing_key += 1;
        continue;
      }
      pushSample(diag.sample_input_keys, kk);

      for (const fieldName of Object.keys(r || {})) {
        if (!writableSet.has(fieldName)) droppedNonWritable.add(fieldName);
      }

      const fields = pickWritable(writableSet, r);
      if (Object.keys(fields).length === 0) {
        diag.rows_empty_after_pick += 1;
        pushSample(diag.sample_empty_field_keys, kk);
      }

      if (existing.has(kk)) {
        toUpdate.push({ id: existing.get(kk), fields });
        pushSample(diag.sample_update_keys, kk);
      } else {
        toCreate.push({ fields });
        pushSample(diag.sample_create_keys, kk);
      }
    }

    diag.to_create = toCreate.length;
    diag.to_update = toUpdate.length;
    diag.dropped_nonwritable_fields = [...droppedNonWritable].sort().slice(0, 40);

    let created = 0;
    for (const batch of chunk50(toCreate)) {
      try {
        if (!batch.length) continue;
        await table.createRecordsAsync(batch);
        created += batch.length;
      } catch (err) {
        const batchKeys = batch
          .map(item => normalizeKeyStr(item?.fields?.[keyName]))
          .filter(Boolean)
          .slice(0, 10);
        const errMsg = String(err?.message || err);
        diag.create_errors.push({
          message: errMsg,
          batch_size: batch.length,
          sample_keys: batchKeys,
          sample_field_names: Object.keys(batch[0]?.fields || {}).slice(0, 25),
        });
        await logAutomationErr({
          error_type: "create_batch_failed",
          message: `${tableNameForErr}: ${errMsg} | sample_keys=${batchKeys.join(",")} | sample_fields=${Object.keys(batch[0]?.fields || {}).slice(0, 25).join(",")}`,
          app_show_id: scopeAppSid,
          people_show_id: scopeAppSid,
        });

        for (const item of batch) {
          try {
            await table.createRecordAsync(item.fields);
            created += 1;
            diag.created_after_batch_fallback += 1;
          } catch (singleErr) {
            const itemKey = normalizeKeyStr(item?.fields?.[keyName]);
            await logAutomationErr({
              error_type: "create_record_failed",
              message: `${tableNameForErr}: ${String(singleErr?.message || singleErr)} | key=${itemKey}`,
              app_show_id: scopeAppSid,
              people_show_id: scopeAppSid,
            });
          }
        }
      }
    }

    let updated = 0;
    for (const batch of chunk50(toUpdate)) {
      try {
        if (!batch.length) continue;
        await table.updateRecordsAsync(batch);
        updated += batch.length;
      } catch (err) {
        const errMsg = String(err?.message || err);
        diag.update_errors.push({
          message: errMsg,
          batch_size: batch.length,
          sample_ids: batch.map(item => String(item?.id || "").trim()).filter(Boolean).slice(0, 10),
          sample_field_names: Object.keys(batch[0]?.fields || {}).slice(0, 25),
        });
        await logAutomationErr({
          error_type: "update_batch_failed",
          message: `${tableNameForErr}: ${errMsg} | sample_ids=${batch.map(item => String(item?.id || "").trim()).filter(Boolean).slice(0, 10).join(",")} | sample_fields=${Object.keys(batch[0]?.fields || {}).slice(0, 25).join(",")}`,
          app_show_id: scopeAppSid,
          people_show_id: scopeAppSid,
        });

        for (const item of batch) {
          try {
            await table.updateRecordAsync(item.id, item.fields);
            updated += 1;
            diag.updated_after_batch_fallback += 1;
          } catch (singleErr) {
            await logAutomationErr({
              error_type: "update_record_failed",
              message: `${tableNameForErr}: ${String(singleErr?.message || singleErr)} | id=${String(item?.id || "").trim()}`,
              app_show_id: scopeAppSid,
              people_show_id: scopeAppSid,
            });
          }
        }
      }
    }

    const q2 = await table.selectRecordsAsync({ fields: fieldsToLoad });
    const keyToId = new Map();
    for (const rec of q2.records) {
      if (!recordMatchesScope(rec, scopeSpec, scopeAppSid, scopeAppDt, scopeShowRecordId)) continue;

      const kk = normalizeKeyStr(rec.getCellValue(keyName));
      if (!kk) continue;
      keyToId.set(kk, rec.id);
    }

    diag.created = created;
    diag.updated = updated;
    diag.key_to_id_size = keyToId.size;

    return { created, updated, keyToId, diag };
  } catch (err) {
    diag.fatal_error = String(err?.message || err);
    await logAutomationErr({
      error_type: "upsert_failed",
      message: `${tableNameForErr}: ${diag.fatal_error}`,
      app_show_id: scopeAppSid,
      people_show_id: scopeAppSid,
    });
    return { created: 0, updated: 0, keyToId: new Map(), diag };
  }
}

//////////////////////
// DELETE HELPERS
//////////////////////
const MAX_DELETE_PER_TABLE = 20000;
const FIELD_RUN_TIME = "run_time";

async function deleteOutOfScopeGlobal({ table, keepAppSid, keepAppDt, keepShowRecordId = null, tableNameForErr }) {
  try {
    const scopeSpec = getScopeSpec(table, keepShowRecordId);
    const fieldsToLoad = getScopeFieldsToLoad(null, scopeSpec);

    const q = await table.selectRecordsAsync({ fields: fieldsToLoad });

    const deleteIds = [];
    let scanned = q.records.length;

    for (const rec of q.records) {
      if (!recordMatchesScope(rec, scopeSpec, keepAppSid, keepAppDt, keepShowRecordId)) {
        deleteIds.push(rec.id);
      }
    }

    if (deleteIds.length > MAX_DELETE_PER_TABLE) {
      await logAutomationErr({
        error_type: "delete_cap_exceeded",
        message: `${tableNameForErr}: ${deleteIds.length} > ${MAX_DELETE_PER_TABLE}`,
        app_show_id: keepAppSid,
        people_show_id: keepAppSid,
      });
      return { table: table.name, scanned, deleted: 0, skipped: true, reason: "delete cap exceeded" };
    }

    let deleted = 0;
    for (const batch of chunk50(deleteIds)) {
      try {
        if (!batch.length) continue;
        await table.deleteRecordsAsync(batch);
        deleted += batch.length;
      } catch (err) {
        await logAutomationErr({
          error_type: "delete_batch_failed",
          message: `${tableNameForErr}: ${String(err?.message || err)}`,
          app_show_id: keepAppSid,
          people_show_id: keepAppSid,
        });
      }
    }

    return { table: table.name, scanned, deleted };
  } catch (err) {
    await logAutomationErr({
      error_type: "delete_out_of_scope_failed",
      message: `${tableNameForErr}: ${String(err?.message || err)}`,
      app_show_id: keepAppSid,
      people_show_id: keepAppSid,
    });
    return { table: table?.name || tableNameForErr, scanned: 0, deleted: 0, skipped: true, reason: "error" };
  }
}

async function deleteInScopeNotKeptGlobal({
  table,
  keyField,
  keepKeySet,
  keepAppSid,
  keepAppDt,
  keepShowRecordId,
  currentRunTime,
  tableNameForErr,
}) {
  try {
    const hasRunTime = table.fields.some(f => f.name === FIELD_RUN_TIME);
    const scopeSpec = getScopeSpec(table, keepShowRecordId);
    const fieldsToLoad = getScopeFieldsToLoad(keyField, scopeSpec, hasRunTime ? [FIELD_RUN_TIME] : []);

    const q = await table.selectRecordsAsync({ fields: fieldsToLoad });

    const keepCandidatesByKey = new Map();
    const deleteIds = [];

    let scanned = q.records.length;
    let inScope = 0;
    let keepCandidates = 0;

    for (const rec of q.records) {
      if (!recordMatchesScope(rec, scopeSpec, keepAppSid, keepAppDt, keepShowRecordId)) continue;
      inScope += 1;

      const key = normalizeKeyStr(rec.getCellValue(keyField));
      const keyMatch = key && keepKeySet.has(key);

      if (keyMatch) {
        if (!keepCandidatesByKey.has(key)) keepCandidatesByKey.set(key, []);
        keepCandidatesByKey.get(key).push(rec);
        keepCandidates += 1;
        continue;
      }

      deleteIds.push(rec.id);
    }

    let dupLosers = 0;

    for (const [, list] of keepCandidatesByKey.entries()) {
      if (!list || list.length <= 1) continue;

      let winner = list[0];
      let bestScore = -1;

      for (const r of list) {
        if (!hasRunTime) break;

        const rt = normalizeKeyStr(r.getCellValue(FIELD_RUN_TIME));
        if (rt && rt === currentRunTime) {
          winner = r;
          bestScore = Number.MAX_SAFE_INTEGER;
          break;
        }
        const ms = parseRunTimeMs(rt);
        if (ms > bestScore) {
          bestScore = ms;
          winner = r;
        }
      }

      for (const r of list) {
        if (r.id === winner.id) continue;
        deleteIds.push(r.id);
        dupLosers += 1;
      }
    }

    if (deleteIds.length > MAX_DELETE_PER_TABLE) {
      await logAutomationErr({
        error_type: "delete_cap_exceeded",
        message: `${tableNameForErr}: ${deleteIds.length} > ${MAX_DELETE_PER_TABLE}`,
        app_show_id: keepAppSid,
        people_show_id: keepAppSid,
      });
      return { table: table.name, scanned, in_scope: inScope, keep_candidates: keepCandidates, dup_losers: dupLosers, deleted: 0, skipped: true, reason: "delete cap exceeded" };
    }

    let deleted = 0;
    for (const batch of chunk50(deleteIds)) {
      try {
        if (!batch.length) continue;
        await table.deleteRecordsAsync(batch);
        deleted += batch.length;
      } catch (err) {
        await logAutomationErr({
          error_type: "delete_batch_failed",
          message: `${tableNameForErr}: ${String(err?.message || err)}`,
          app_show_id: keepAppSid,
          people_show_id: keepAppSid,
        });
      }
    }

    return {
      table: table.name,
      scanned,
      in_scope: inScope,
      keep_candidates: keepCandidates,
      dup_losers: dupLosers,
      deleted,
    };
  } catch (err) {
    await logAutomationErr({
      error_type: "delete_in_scope_failed",
      message: `${tableNameForErr}: ${String(err?.message || err)}`,
      app_show_id: keepAppSid,
      people_show_id: keepAppSid,
    });
    return { table: table?.name || tableNameForErr, skipped: true, reason: "error" };
  }
}

//////////////////////
// FETCH SCHEDULE
//////////////////////
let schedRows = [];
let schedGroupRows = [];
let schedulePayload = { source: "watch_schedule" };
let scheduleUrl = "watch_schedule";
let scheduleFetchOk = false;

function buildClassRowFromWatchScheduleRecord(rec) {
  const classIdNum = toFiniteNumberOrNull(getCellValueSafe(rec, "class_id"));
  if (classIdNum === null) return null;

  const classGroupId = toFiniteNumberOrNull(getCellValueSafe(rec, "class_group_id"));
  const ringNumber = toFiniteNumberOrNull(getCellValueSafe(rec, "ring_number"));
  const estimatedStartTime = normalizeText(
    getCellValueFirst(rec, ["estimated_start_time", "latestStart"])
  );
  const scheduledDate = toIsoDateOnly(
    getCellValueFirst(rec, ["scheduled_date", " schedule_date", "schedule_date", "schedule_show_datev2", "show_date", "date"])
  );

  return {
    class_id: Number(classIdNum),
    class_number: normalizeEntryNumber(getCellValueSafe(rec, "class_number")),
    class_name: normalizeText(getCellValueSafe(rec, "class_name")),
    schedule_sequencetype: normalizeText(getCellValueSafe(rec, "schedule_sequencetype")),
    class_type: normalizeText(getCellValueSafe(rec, "class_type")),
    class_group_id: classGroupId === null ? undefined : Number(classGroupId),
    group_name: normalizeText(getCellValueSafe(rec, "group_name")),
    class_groupxclasses_id: toFiniteNumberOrNull(getCellValueSafe(rec, "class_groupxclasses_id")) ?? undefined,
    ring_number: ringNumber === null ? undefined : Number(ringNumber),
    ring_name: normalizeText(getCellValueFirst(rec, ["ring_name", "ringName"])),
    estimated_start_time: estimatedStartTime || "",
    estimated_end_time: normalizeText(getCellValueSafe(rec, "estimated_end_time")) || undefined,
    class_group_sequence: toFiniteNumberOrNull(getCellValueSafe(rec, "class_group_sequence")) ?? undefined,
    scheduled_date: scheduledDate || undefined,
    status: normalizeText(getCellValueSafe(rec, "status")) || undefined,
  };
}

function buildGroupRowsFromClassRows(rows) {
  const groupById = new Map();

  for (const row of (Array.isArray(rows) ? rows : [])) {
    const groupId = toFiniteNumberOrNull(row?.class_group_id);
    if (groupId === null) continue;

    const key = String(groupId);
    const prev = groupById.get(key) || {};
    const nextStart = normalizeText(row?.estimated_start_time);
    const prevStart = normalizeText(prev?.estimated_start_time);

    groupById.set(key, {
      class_group_id: Number(groupId),
      group_name: normalizeText(row?.group_name) || normalizeText(prev?.group_name),
      ring_number: hasValue(row?.ring_number) ? Number(row.ring_number) : (hasValue(prev?.ring_number) ? Number(prev.ring_number) : undefined),
      ring_name: normalizeText(row?.ring_name) || normalizeText(prev?.ring_name),
      estimated_start_time: prevStart || nextStart || "",
      class_group_sequence: toFiniteNumberOrNull(row?.class_group_sequence ?? prev?.class_group_sequence) ?? undefined,
    });
  }

  const rowsOut = [...groupById.values()];
  rowsOut.sort((a, b) => {
    const ringA = toFiniteNumberOrNull(a?.ring_number) ?? Number.MAX_SAFE_INTEGER;
    const ringB = toFiniteNumberOrNull(b?.ring_number) ?? Number.MAX_SAFE_INTEGER;
    if (ringA !== ringB) return ringA - ringB;

    const startA = normalizeText(a?.estimated_start_time);
    const startB = normalizeText(b?.estimated_start_time);
    if (startA && startB && startA !== startB) return startA.localeCompare(startB);
    if (startA && !startB) return -1;
    if (!startA && startB) return 1;

    return String(a?.class_group_id ?? "").localeCompare(String(b?.class_group_id ?? ""));
  });

  return rowsOut.map((row, idx) => ({
    ...row,
    class_group_sequence: hasValue(row?.class_group_sequence) ? Number(row.class_group_sequence) : idx + 1,
  }));
}

const watchScheduleByClassId = new Map();
let watchScheduleRowsInScope = 0;

if (!watchScheduleTable) {
  await logAutomationErr({
    error_type: "missing_watch_schedule_table",
    message: `Table "${T_SCHEDULE}" not found.`,
    app_show_id: appSidNum,
    people_show_id: appSidNum,
  });
} else {
  try {
    const watchScheduleScopeSpec = getScopeSpec(watchScheduleTable, SHOWS_RECORD_ID);
    const watchScheduleFields = getScopeFieldsToLoad("class_id", watchScheduleScopeSpec, [
      "class_number",
      "class_name",
      "schedule_sequencetype",
      "class_type",
      "class_group_id",
      "group_name",
      "class_groupxclasses_id",
      "ring_number",
      "ring_name",
      "ringName",
      "estimated_start_time",
      "estimated_end_time",
      "latestStart",
      "class_group_sequence",
    ]).filter(fieldName => tableHasField(watchScheduleTable, fieldName));

    const watchScheduleQuery = await watchScheduleTable.selectRecordsAsync({ fields: watchScheduleFields });
    for (const rec of watchScheduleQuery.records) {
      if (!recordMatchesScope(rec, watchScheduleScopeSpec, appSidNum, appDtText, SHOWS_RECORD_ID)) continue;
      watchScheduleRowsInScope += 1;

      const row = buildClassRowFromWatchScheduleRecord(rec);
      if (!row) continue;

      const key = String(row.class_id);
      if (!watchScheduleByClassId.has(key)) watchScheduleByClassId.set(key, row);
    }

    scheduleFetchOk = true;
  } catch (err) {
    await logAutomationErr({
      error_type: "watch_schedule_read_failed",
      message: String(err?.message || err),
      app_show_id: appSidNum,
      people_show_id: appSidNum,
    });
  }
}

const hasScopedWatchSchedule = watchScheduleByClassId.size > 0;
if (watchScheduleTable && !hasScopedWatchSchedule) {
  await logAutomationErr({
    error_type: "watch_schedule_empty_for_scope",
    message: `No watch_schedule rows found for sid=${appSidNum} dt=${appDtText}.`,
    app_show_id: appSidNum,
    people_show_id: appSidNum,
  });
}

// People payloads are week-scoped and do not contain dates.
// Daily validity comes only from watch_schedule.class_id for the current sid/dt scope.
const validDailyClassIdSet = hasScopedWatchSchedule
  ? new Set([...watchScheduleByClassId.keys()])
  : new Set();
const validDailyClassSource = hasScopedWatchSchedule ? "watch_schedule" : "watch_schedule_empty";

schedRows = [...watchScheduleByClassId.values()];
schedGroupRows = buildGroupRowsFromClassRows(schedRows);
schedulePayload = {
  source: "watch_schedule",
  rows_in_scope: watchScheduleRowsInScope,
  class_ids_in_scope: watchScheduleByClassId.size,
};

const classById = new Map();
for (const r of schedRows) classById.set(String(r.class_id), r);

const groupScheduleById = new Map();
for (const r of schedGroupRows) {
  if (!hasValue(r?.class_group_id)) continue;
  const key = String(r.class_group_id);
  if (!groupScheduleById.has(key)) groupScheduleById.set(key, r);
}

function ringKey(n) {
  if (n === undefined || n === null || Number.isNaN(Number(n))) return "";
  return String(Number(n));
}
function groupEndKey(ringNum, groupId) {
  const rk = ringKey(ringNum);
  const gid = (groupId === undefined || groupId === null) ? "" : String(Number(groupId));
  if (!rk || !gid) return "";
  return `${rk}|${gid}`;
}

const ringGroupOrder = new Map();
const groupStartTime = new Map();
const groupEndTime   = new Map();

for (const r of schedGroupRows) {
  const rk = ringKey(r.ring_number);
  const gid = r.class_group_id;
  const k = groupEndKey(rk, gid);
  if (!k) continue;

  if (!ringGroupOrder.has(rk)) ringGroupOrder.set(rk, []);
  const arr = ringGroupOrder.get(rk);
  if (!groupStartTime.has(k)) {
    groupStartTime.set(k, String(r.estimated_start_time || "").trim());
    arr.push(k);
  }
}
for (const [, arr] of ringGroupOrder.entries()) {
  for (let i = 0; i < arr.length; i++) {
    const k = arr[i];
    const nextK = arr[i + 1];
    if (!nextK) continue;
    const nextStart = String(groupStartTime.get(nextK) || "").trim();
    if (nextStart) groupEndTime.set(k, nextStart);
  }
}

//////////////////////
// ww_trainer RULES
//////////////////////
const trainerIdField =
  ["pid", "trainer_id", "trainerId"].find(name => tableHasField(wwTrainerTable, name)) || null;

if (!trainerIdField) {
  await logAutomationErr({
    error_type: "missing_trainer_id_field",
    message: `ww_trainers must contain one of: pid, trainer_id, trainerId`,
    app_show_id: appSidNum,
    people_show_id: appSidNum,
  });
}

const wwTrainerByPid = new Map();
try {
  const wwTrainerQuery = await wwTrainerTable.selectRecordsAsync();
  for (const rec of wwTrainerQuery.records) {
    const pidTok = trainerIdField ? normalizePidToken(rec.getCellValue(trainerIdField)) : "";
    if (!pidTok) continue;
    if (!wwTrainerByPid.has(pidTok)) wwTrainerByPid.set(pidTok, rec);
  }
} catch (err) {
  await logAutomationErr({
    error_type: "ww_trainers_read_failed",
    message: String(err?.message || err),
    app_show_id: appSidNum,
    people_show_id: appSidNum,
  });
}

const primaryRuleByPid = new Map();
const allLoanSourcePids = new Set();

for (const primaryPid of pids) {
  const rec = wwTrainerByPid.get(primaryPid);

  const rawLoanList = rec && tableHasField(wwTrainerTable, "eid_loans")
    ? parseArrayishCell(rec.getCellValue("eid_loans"))
    : [];
  const rawIgnoreList = rec && tableHasField(wwTrainerTable, "eid_ignores")
    ? parseArrayishCell(rec.getCellValue("eid_ignores"))
    : [];

  const loanPairs = parseLoanTokenList(rawLoanList);
  const loanPairKeySet = new Set(loanPairs.map(x => x.pairKey));
  const ignoreEntrySet = normalizeEntryIdSet(rawIgnoreList);
  const loanSourcePidSet = new Set(loanPairs.map(x => x.sourcePid));

  for (const src of loanSourcePidSet) allLoanSourcePids.add(src);

  primaryRuleByPid.set(primaryPid, {
    primaryPid,
    hasTrainerRow: !!rec,
    loanPairs,
    loanPairKeySet,
    ignoreEntrySet,
    loanSourcePidSet,
  });
}

//////////////////////
// FETCH PEOPLE PAYLOADS
//////////////////////
const peoplePayloadCache = new Map();
const emptyTripsPayloadByPid = new Map();
const peoplePayloadStatusByPid = new Map();

async function getPeoplePayload(pid) {
  const pidTok = normalizePidToken(pid);
  if (!pidTok) return null;
  if (peoplePayloadCache.has(pidTok)) return peoplePayloadCache.get(pidTok);

  const peopleUrl = `${base_url}/people/${encodeURIComponent(pidTok)}?pid=${encodeURIComponent(pidTok)}&show_id=${encodeURIComponent(appSidNum)}&customer_id=${encodeURIComponent(customer_id)}`;

  try {
    const payload = await fetchJson(peopleUrl);
    peoplePayloadCache.set(pidTok, payload);

    const trips = Array.isArray(payload?.trips) ? payload.trips : [];
    peoplePayloadStatusByPid.set(pidTok, {
      pid: pidTok,
      status: trips.length === 0 ? "empty_trips_payload" : "ok",
      trips_count: trips.length,
      people_show_id: payload?.show_id ?? appSidNum,
    });
    if (trips.length === 0) {
      emptyTripsPayloadByPid.set(pidTok, {
        pid: pidTok,
        people_show_id: payload?.show_id ?? appSidNum,
      });
      await logAutomationErr({
        error_type: "empty_trips_payload",
        message: `Empty trips payload for pid ${pidTok}; creating placeholder watch records.`,
        pid: pidTok,
        app_show_id: appSidNum,
        people_show_id: payload?.show_id ?? appSidNum,
        automation_key: `${AUTOMATION_NAME}|${pidTok}|${RUN_ID}|empty_trips_payload`,
      });
    }

    return payload;
  } catch (err) {
    peoplePayloadStatusByPid.set(pidTok, {
      pid: pidTok,
      status: "people_fetch_failed",
      error: String(err?.message || err).slice(0, 300),
    });
    await logAutomationErr({
      error_type: "people_fetch_failed",
      message: String(err?.message || err),
      pid: pidTok,
      app_show_id: appSidNum,
      people_show_id: appSidNum,
      automation_key: `${AUTOMATION_NAME}|${pidTok}|${RUN_ID}|people_fetch_failed`,
    });
    peoplePayloadCache.set(pidTok, null);
    return null;
  }
}

const basePidList = [...new Set(pids)];
const allPeoplePids = [...new Set([...basePidList, ...allLoanSourcePids])];

await runPool(allPeoplePids, pull_pip, async (pid) => {
  await getPeoplePayload(pid);
});

//////////////////////
// BUILD PEOPLE TRIPS
//////////////////////
const peopleTripsByUuid = new Map();

function addPeopleTrip(norm) {
  if (!norm) return;
  if (!norm.entryxclasses_uuid) return;
  const uuid = String(norm.entryxclasses_uuid).trim();
  if (!uuid) return;
  if (!peopleTripsByUuid.has(uuid)) peopleTripsByUuid.set(uuid, norm);
}

for (const primaryPid of pids) {
  const rules = primaryRuleByPid.get(primaryPid) || {
    primaryPid,
    hasTrainerRow: false,
    loanPairs: [],
    loanPairKeySet: new Set(),
    ignoreEntrySet: new Set(),
    loanSourcePidSet: new Set(),
  };

  {
    const payload = await getPeoplePayload(primaryPid);
    const candidates = collectTripCandidates(payload);

    for (const raw of candidates) {
      const norm = normalizePeopleTripRow(raw, primaryPid, primaryPid);
      if (!norm) continue;

      const entryKey = normalizeEntryIdKey(norm.entry_id);
      if (entryKey && rules.ignoreEntrySet.has(entryKey)) continue;

      addPeopleTrip(norm);
    }
  }

  for (const sourcePid of rules.loanSourcePidSet) {
    const payload = await getPeoplePayload(sourcePid);
    const candidates = collectTripCandidates(payload);

    for (const raw of candidates) {
      const entryKey = normalizeEntryIdKey(raw?.entry_id ?? raw?.entryId);
      if (!entryKey) continue;

      const pairKey = `${sourcePid}.${entryKey}`;
      if (!rules.loanPairKeySet.has(pairKey)) continue;
      if (rules.ignoreEntrySet.has(entryKey)) continue;

      const norm = normalizePeopleTripRow(raw, primaryPid, sourcePid);
      if (!norm) continue;

      addPeopleTrip(norm);
    }
  }
}

const peopleTrips = [...peopleTripsByUuid.values()];

//////////////////////
// JOIN peopleTrips -> schedule
//////////////////////
const peopleTripsOutsideScheduleClassIds = [...new Set(
  peopleTrips
    .map(pt => normalizeKeyStr(pt.class_id))
    .filter(classId => classId && !validDailyClassIdSet.has(classId))
)];

function buildMissingPayloadWatchTripRow(pidTok) {
  const ids = buildMissingPayloadIds(pidTok);
  if (!ids) return null;

  return {
    app_sid: appSidNum,
    app_dt: appDtText,
    show_id: appSidNum,
    date: appDtText,
    shows: linkOne(SHOWS_RECORD_ID),

    pid: ids.pidNum,

    run_id: RUN_ID,
    run_time: CURRENT_RUN_TIME,

    entryxclasses_uuid: `missing-trip|${appSidNum}|${appDtText}|${pidTok}`,

    entry_id: ids.entry_id,
    horse: `EMPTY TRIPS PAYLOAD PID ${pidTok}`,
    class_id: ids.class_id,

    class_name: "empty trips payload",
    schedule_sequencetype: "empty_trips_payload",
    class_type: "empty_trips_payload",
    class_group_id: ids.class_group_id,
    group_name: "empty trips payload",

    is_missing: true,
  };
}

const watchTripsRows = [];
let peopleTripsMissingSchedule = 0;
const peopleTripsMissingScheduleSample = [];
for (const pt of peopleTrips) {
  const classIdKey = String(pt.class_id);
  if (!validDailyClassIdSet.has(classIdKey)) {
    peopleTripsMissingSchedule += 1;
    pushSample(peopleTripsMissingScheduleSample, `${pt.class_id}|${pt.entryxclasses_uuid}`);
    continue;
  }

  const s = watchScheduleByClassId.get(classIdKey) || classById.get(classIdKey);
  if (!s) {
    peopleTripsMissingSchedule += 1;
    pushSample(peopleTripsMissingScheduleSample, `${pt.class_id}|${pt.entryxclasses_uuid}`);
    continue;
  }

  const endKey = groupEndKey(s.ring_number, s.class_group_id);
  const estimated_end_time = endKey
    ? (groupEndTime.get(endKey) || s.estimated_end_time || undefined)
    : (s.estimated_end_time || s.estimated_start_time || undefined);

  watchTripsRows.push({
    app_sid: appSidNum,
    app_dt: appDtText,
    show_id: appSidNum,
    date: appDtText,
    shows: linkOne(SHOWS_RECORD_ID),

    pid: Number(pt.pid),
    source_pid: hasValue(pt.source_pid) ? Number(pt.source_pid) : undefined,

    run_id: RUN_ID,
    run_time: CURRENT_RUN_TIME,

    entryxclasses_uuid: pt.entryxclasses_uuid,

    entry_id: Number(pt.entry_id),
    entry_number: pt.entry_number,
    horse: pt.horse,
    class_id: Number(pt.class_id),

    class_number: hasValue(s.class_number) ? Number(s.class_number) : pt.class_number,
    class_name: s.class_name || pt.class_name || "",
    schedule_sequencetype: s.schedule_sequencetype || "",
    class_type: s.class_type || "",
    class_group_id: hasValue(s.class_group_id) ? Number(s.class_group_id) : undefined,
    group_name: s.group_name || "",
    class_groupxclasses_id: hasValue(s.class_groupxclasses_id) ? Number(s.class_groupxclasses_id) : undefined,
    ring_number: hasValue(s.ring_number) ? Number(s.ring_number) : undefined,
    estimated_start_time: s.estimated_start_time || "",
    estimated_end_time: estimated_end_time,
    class_group_sequence: hasValue(s.class_group_sequence) ? Number(s.class_group_sequence) : undefined,
    scheduled_date: s.scheduled_date || appDtText,
    status: s.status || undefined,

    rider_name: pt.rider_name || undefined,
    rider_id: hasValue(pt.rider_id) ? Number(pt.rider_id) : undefined,
    placing: hasValue(pt.placing) ? Number(pt.placing) : undefined,

    is_missing: false,
  });
}

const emptyPayloadPlaceholderPids = [];
const suppressedEmptyPayloadPlaceholderPids = [];
const scheduleHasRows = schedRows.length > 0 || schedGroupRows.length > 0 || watchScheduleByClassId.size > 0;
const suppressEmptyPayloadPlaceholders = false;
for (const primaryPid of basePidList) {
  const pidTok = normalizePidToken(primaryPid);
  if (!pidTok) continue;
  if (!emptyTripsPayloadByPid.has(pidTok)) continue;
  if (suppressEmptyPayloadPlaceholders) {
    suppressedEmptyPayloadPlaceholderPids.push(pidTok);
    continue;
  }

  const row = buildMissingPayloadWatchTripRow(pidTok);
  if (!row) continue;

  watchTripsRows.push(row);
  emptyPayloadPlaceholderPids.push(pidTok);
}

//////////////////////
// KEEP SETS
//////////////////////
const payloadTripsKeySet = new Set(
  watchTripsRows.map(r => normalizeKeyStr(r.entryxclasses_uuid)).filter(Boolean)
);

const hasTripsPayload = payloadTripsKeySet.size > 0;
const hasRealTripsPayload = watchTripsRows.some(r => !r.is_missing);
const forceClearInScopeBecauseNoSchedule = false;
// Placeholder-only rows still represent the authoritative in-scope dataset for this run.
const shouldDeleteInScope = hasTripsPayload || forceClearInScopeBecauseNoSchedule;

//////////////////////
// UPSERT CURRENT DATASET
//////////////////////
const upTrips = await upsertByKeyScoped(
  tripsTable,
  KEY_TRIPS,
  watchTripsRows,
  W_TRIPS,
  appSidNum,
  appDtText,
  T_TRIPS,
  SHOWS_RECORD_ID
);

const wantGroup = new Map();
const wantClass = new Map();
const wantEntry = new Map();
const appDowRawV2 = dowNameFromSqlDate(appDtText);

for (const r of watchTripsRows) {
  if (r.class_group_id) {
    const k = String(r.class_group_id);
    if (!wantGroup.has(k)) {
      wantGroup.set(k, {
        app_sid: appSidNum,
        app_dt: appDtText,
        show_id: appSidNum,
        date: appDtText,
        shows: linkOne(SHOWS_RECORD_ID),

        class_group_id: Number(r.class_group_id),
        group_name: r.group_name || "",
        pid: hasValue(r.pid) ? Number(r.pid) : undefined,
        ring_number: hasValue(r.ring_number) ? Number(r.ring_number) : undefined,
        estimated_start_time: r.estimated_start_time || "",
        estimated_end_time: r.estimated_end_time,
        class_groupxclasses_id: hasValue(r.class_groupxclasses_id) ? Number(r.class_groupxclasses_id) : undefined,
        class_group_sequence: hasValue(r.class_group_sequence) ? Number(r.class_group_sequence) : undefined,

        run_id: RUN_ID,
        run_time: CURRENT_RUN_TIME,
        is_missing: !!r.is_missing,
      });
    }
  }

  if (r.class_id) {
    const k = String(r.class_id);
    if (!wantClass.has(k)) {
      const scheduledDate = toIsoDateOnly(r.scheduled_date || appDtText) || appDtText;
      const inactive = !!scheduledDate && scheduledDate !== appDtText;
      wantClass.set(k, {
        app_sid: appSidNum,
        app_dt: appDtText,
        show_id: appSidNum,
        date: appDtText,
        shows: linkOne(SHOWS_RECORD_ID),
        app_show_idv2: appSidNum,
        app_sql_datev2: appDtText,
        app_dow_rawv2: appDowRawV2,
        scheduled_date: scheduledDate,
        inactive,
        is_current_scope: !inactive,
        scope_status: inactive ? "dropped" : "current",

        class_id: Number(r.class_id),
        class_number: hasValue(r.class_number) ? Number(r.class_number) : undefined,
        class_name: r.class_name || "",
        group_name: r.group_name || "",
        ring_number: hasValue(r.ring_number) ? Number(r.ring_number) : undefined,
        pid: hasValue(r.pid) ? Number(r.pid) : undefined,
        schedule_sequencetype: r.schedule_sequencetype || "",
        class_type: r.class_type || "",
        class_group_id: hasValue(r.class_group_id) ? Number(r.class_group_id) : undefined,
        class_groupxclasses_id: hasValue(r.class_groupxclasses_id) ? Number(r.class_groupxclasses_id) : undefined,
        class_group_sequence: hasValue(r.class_group_sequence) ? Number(r.class_group_sequence) : undefined,
        estimated_start_time: r.estimated_start_time || "",
        estimated_end_time: r.estimated_end_time,
        status: r.status || "",

        run_id: RUN_ID,
        run_time: CURRENT_RUN_TIME,
        is_missing: !!r.is_missing,
      });
    }
  }

  if (r.entry_id) {
    const k = String(r.entry_id);
    if (!wantEntry.has(k)) {
      wantEntry.set(k, {
        app_sid: appSidNum,
        app_dt: appDtText,
        show_id: appSidNum,
        date: appDtText,
        shows: linkOne(SHOWS_RECORD_ID),

        entry_id: Number(r.entry_id),
        entry_number: r.entry_number,
        horse: r.horse || "",
        pid: hasValue(r.pid) ? Number(r.pid) : undefined,
        source_pid: hasValue(r.source_pid) ? Number(r.source_pid) : undefined,

        run_id: RUN_ID,
        run_time: CURRENT_RUN_TIME,
        is_missing: !!r.is_missing,
      });
    }
  }
}

const upGroups = await upsertByKeyScoped(groupsTable, KEY_GROUPS, [...wantGroup.values()], W_GROUPS, appSidNum, appDtText, T_GROUPS, SHOWS_RECORD_ID);
const upClasses = await upsertByKeyScoped(classesTable, KEY_CLASSES, [...wantClass.values()], W_CLASSES, appSidNum, appDtText, T_CLASSES, SHOWS_RECORD_ID);
const upEntries = await upsertByKeyScoped(entriesTable, KEY_ENTRIES, [...wantEntry.values()], W_ENTRIES, appSidNum, appDtText, T_ENTRIES, SHOWS_RECORD_ID);

//////////////////////
// LINKING
//////////////////////
const LINK_TRIPS_EID = "eid";
const LINK_TRIPS_CID = "cid";
const LINK_TRIPS_CGI = "cgi";

const LINK_GROUPS_CIDS  = "cids";
const LINK_GROUPS_EIDS  = "eids";
const LINK_GROUPS_TRIPS = "trips";

const LINK_CLASSES_CGI   = "cgi";
const LINK_CLASSES_EIDS  = "eids";
const LINK_CLASSES_TRIPS = "trips";

const LINK_ENTRIES_CID   = "cid";
const LINK_ENTRIES_CGI   = "cgi";
const LINK_ENTRIES_TRIPS = "trips";

async function safeBatchUpdate(table, updates, tableNameForErr) {
  for (const batch of chunk50(updates)) {
    try {
      if (batch.length) await table.updateRecordsAsync(batch);
    } catch (err) {
      await logAutomationErr({
        error_type: "link_update_failed",
        message: `${tableNameForErr}: ${String(err?.message || err)}`,
        app_show_id: appSidNum,
        people_show_id: appSidNum,
      });
    }
  }
}

const tripLinkUpdates = [];
let tripLinksMissingEntryTarget = 0;
let tripLinksMissingClassTarget = 0;
let tripLinksMissingGroupTarget = 0;
for (const r of watchTripsRows) {
  const uuid = normalizeKeyStr(r.entryxclasses_uuid);
  const tripRecId = upTrips.keyToId.get(uuid);
  if (!tripRecId) continue;

  const eidId = upEntries.keyToId.get(String(r.entry_id));
  const cidId = upClasses.keyToId.get(String(r.class_id));
  const cgiId = r.class_group_id ? upGroups.keyToId.get(String(r.class_group_id)) : null;
  if (!eidId) tripLinksMissingEntryTarget += 1;
  if (!cidId) tripLinksMissingClassTarget += 1;
  if (r.class_group_id && !cgiId) tripLinksMissingGroupTarget += 1;

  const fields = {};
  if (W_TRIPS.has(LINK_TRIPS_EID) && eidId) fields[LINK_TRIPS_EID] = linkOne(eidId);
  if (W_TRIPS.has(LINK_TRIPS_CID) && cidId) fields[LINK_TRIPS_CID] = linkOne(cidId);
  if (W_TRIPS.has(LINK_TRIPS_CGI) && cgiId) fields[LINK_TRIPS_CGI] = linkOne(cgiId);
  if (W_TRIPS.has("shows") && SHOWS_RECORD_ID) fields["shows"] = linkOne(SHOWS_RECORD_ID);
  if (W_TRIPS.has("app_sid")) fields["app_sid"] = appSidNum;
  if (W_TRIPS.has("app_dt")) fields["app_dt"] = appDtText;
  if (W_TRIPS.has("show_id")) fields["show_id"] = appSidNum;
  if (W_TRIPS.has("date")) fields["date"] = appDtText;

  if (Object.keys(fields).length) tripLinkUpdates.push({ id: tripRecId, fields });
}
await safeBatchUpdate(tripsTable, tripLinkUpdates, T_TRIPS);

const groupAgg = new Map();
const classAgg = new Map();
const entryAgg = new Map();

for (const r of watchTripsRows) {
  const uuid = normalizeKeyStr(r.entryxclasses_uuid);
  const tripRecId = upTrips.keyToId.get(uuid);
  if (!tripRecId) continue;

  const gid = r.class_group_id ? String(r.class_group_id) : "";
  const cid = r.class_id ? String(r.class_id) : "";
  const eid = r.entry_id ? String(r.entry_id) : "";

  const groupRecId = gid ? upGroups.keyToId.get(gid) : null;
  const classRecId = cid ? upClasses.keyToId.get(cid) : null;
  const entryRecId = eid ? upEntries.keyToId.get(eid) : null;

  if (gid && groupRecId) {
    if (!groupAgg.has(gid)) groupAgg.set(gid, { cids: new Set(), eids: new Set(), trips: new Set() });
    const g = groupAgg.get(gid);
    if (classRecId) g.cids.add(classRecId);
    if (entryRecId) g.eids.add(entryRecId);
    g.trips.add(tripRecId);
  }

  if (cid && classRecId) {
    if (!classAgg.has(cid)) classAgg.set(cid, { cgi: new Set(), eids: new Set(), trips: new Set() });
    const c = classAgg.get(cid);
    if (groupRecId) c.cgi.add(groupRecId);
    if (entryRecId) c.eids.add(entryRecId);
    c.trips.add(tripRecId);
  }

  if (eid && entryRecId) {
    if (!entryAgg.has(eid)) entryAgg.set(eid, { cid: new Set(), cgi: new Set(), trips: new Set() });
    const e = entryAgg.get(eid);
    if (classRecId) e.cid.add(classRecId);
    if (groupRecId) e.cgi.add(groupRecId);
    e.trips.add(tripRecId);
  }
}

const groupLinkUpdates = [];
for (const [gid, agg] of groupAgg.entries()) {
  const recId = upGroups.keyToId.get(gid);
  if (!recId) continue;

  const fields = {};
  if (W_GROUPS.has(LINK_GROUPS_CIDS) && agg.cids.size) fields[LINK_GROUPS_CIDS] = linkMany([...agg.cids]);
  if (W_GROUPS.has(LINK_GROUPS_EIDS) && agg.eids.size) fields[LINK_GROUPS_EIDS] = linkMany([...agg.eids]);
  if (W_GROUPS.has(LINK_GROUPS_TRIPS) && agg.trips.size) fields[LINK_GROUPS_TRIPS] = linkMany([...agg.trips]);
  if (W_GROUPS.has("shows") && SHOWS_RECORD_ID) fields["shows"] = linkOne(SHOWS_RECORD_ID);
  if (W_GROUPS.has("app_sid")) fields["app_sid"] = appSidNum;
  if (W_GROUPS.has("app_dt")) fields["app_dt"] = appDtText;
  if (W_GROUPS.has("show_id")) fields["show_id"] = appSidNum;
  if (W_GROUPS.has("date")) fields["date"] = appDtText;

  if (Object.keys(fields).length) groupLinkUpdates.push({ id: recId, fields });
}
await safeBatchUpdate(groupsTable, groupLinkUpdates, T_GROUPS);

const classLinkUpdates = [];
for (const [cid, agg] of classAgg.entries()) {
  const recId = upClasses.keyToId.get(cid);
  if (!recId) continue;

  const fields = {};
  if (W_CLASSES.has(LINK_CLASSES_CGI) && agg.cgi.size) fields[LINK_CLASSES_CGI] = linkMany([...agg.cgi]);
  if (W_CLASSES.has(LINK_CLASSES_EIDS) && agg.eids.size) fields[LINK_CLASSES_EIDS] = linkMany([...agg.eids]);
  if (W_CLASSES.has(LINK_CLASSES_TRIPS) && agg.trips.size) fields[LINK_CLASSES_TRIPS] = linkMany([...agg.trips]);
  if (W_CLASSES.has("shows") && SHOWS_RECORD_ID) fields["shows"] = linkOne(SHOWS_RECORD_ID);
  if (W_CLASSES.has("app_sid")) fields["app_sid"] = appSidNum;
  if (W_CLASSES.has("app_dt")) fields["app_dt"] = appDtText;
  if (W_CLASSES.has("show_id")) fields["show_id"] = appSidNum;
  if (W_CLASSES.has("date")) fields["date"] = appDtText;

  if (Object.keys(fields).length) classLinkUpdates.push({ id: recId, fields });
}
await safeBatchUpdate(classesTable, classLinkUpdates, T_CLASSES);

const entryLinkUpdates = [];
for (const [eid, agg] of entryAgg.entries()) {
  const recId = upEntries.keyToId.get(eid);
  if (!recId) continue;

  const fields = {};
  if (W_ENTRIES.has(LINK_ENTRIES_CID) && agg.cid.size) fields[LINK_ENTRIES_CID] = linkMany([...agg.cid]);
  if (W_ENTRIES.has(LINK_ENTRIES_CGI) && agg.cgi.size) fields[LINK_ENTRIES_CGI] = linkMany([...agg.cgi]);
  if (W_ENTRIES.has(LINK_ENTRIES_TRIPS) && agg.trips.size) fields[LINK_ENTRIES_TRIPS] = linkMany([...agg.trips]);
  if (W_ENTRIES.has("shows") && SHOWS_RECORD_ID) fields["shows"] = linkOne(SHOWS_RECORD_ID);
  if (W_ENTRIES.has("app_sid")) fields["app_sid"] = appSidNum;
  if (W_ENTRIES.has("app_dt")) fields["app_dt"] = appDtText;
  if (W_ENTRIES.has("show_id")) fields["show_id"] = appSidNum;
  if (W_ENTRIES.has("date")) fields["date"] = appDtText;

  if (Object.keys(fields).length) entryLinkUpdates.push({ id: recId, fields });
}
await safeBatchUpdate(entriesTable, entryLinkUpdates, T_ENTRIES);

//////////////////////
// CLASS ENRICHMENT
//////////////////////
// Daily rows come from /people payloads filtered by scoped watch_schedule.class_id.
// Do not fan out into /classes/{class_id} requests here.
const uniqueClassIds = [...new Set(
  watchTripsRows
    .filter(r => !r.is_missing)
    .map(r => String(r.class_id))
    .filter(Boolean)
)];
const classEnrichEnabled = false;
const classEnrichDisabledReason = "people_payload_plus_watch_schedule_only";
const enrichErrors = [];
const enrichUpdates = [];
const remainingFetchBudgetBeforeClassEnrich = Math.max(0, MAX_FETCH_REQUESTS - fetchRequestCount);
const classEnrichSkippedByBudgetGuard = 0;
const classEnrichSkippedByBudgetGuardSample = [];
const classEnrichSkippedDueToFetchQuota = 0;
const classEnrichSkippedDueToFetchQuotaSample = [];

//////////////////////
// DELETE PHASE
//////////////////////
const payloadGroupsKeySet = new Set([...wantGroup.keys()].map(normalizeKeyStr).filter(Boolean));
const payloadClassesKeySet = new Set([...wantClass.keys()].map(normalizeKeyStr).filter(Boolean));
const payloadEntriesKeySet = new Set([...wantEntry.keys()].map(normalizeKeyStr).filter(Boolean));

const delTripsOut = await deleteOutOfScopeGlobal({
  table: tripsTable,
  keepAppSid: appSidNum,
  keepAppDt: appDtText,
  keepShowRecordId: SHOWS_RECORD_ID,
  tableNameForErr: T_TRIPS,
});

const delGroupsOut = await deleteOutOfScopeGlobal({
  table: groupsTable,
  keepAppSid: appSidNum,
  keepAppDt: appDtText,
  keepShowRecordId: SHOWS_RECORD_ID,
  tableNameForErr: T_GROUPS,
});

const delClassesOut = await deleteOutOfScopeGlobal({
  table: classesTable,
  keepAppSid: appSidNum,
  keepAppDt: appDtText,
  keepShowRecordId: SHOWS_RECORD_ID,
  tableNameForErr: T_CLASSES,
});

const delEntriesOut = await deleteOutOfScopeGlobal({
  table: entriesTable,
  keepAppSid: appSidNum,
  keepAppDt: appDtText,
  keepShowRecordId: SHOWS_RECORD_ID,
  tableNameForErr: T_ENTRIES,
});

let delTripsIn = { table: tripsTable.name, skipped: true, reason: "empty keep-set" };
let delGroupsIn = { table: groupsTable.name, skipped: true, reason: "empty keep-set" };
let delClassesIn = { table: classesTable.name, skipped: true, reason: "empty keep-set" };
let delEntriesIn = { table: entriesTable.name, skipped: true, reason: "empty keep-set" };

if (shouldDeleteInScope) {
  delTripsIn = await deleteInScopeNotKeptGlobal({
    table: tripsTable,
    keyField: KEY_TRIPS,
    keepKeySet: payloadTripsKeySet,
    keepAppSid: appSidNum,
    keepAppDt: appDtText,
    keepShowRecordId: SHOWS_RECORD_ID,
    currentRunTime: CURRENT_RUN_TIME,
    tableNameForErr: T_TRIPS,
  });

  delGroupsIn = await deleteInScopeNotKeptGlobal({
    table: groupsTable,
    keyField: KEY_GROUPS,
    keepKeySet: payloadGroupsKeySet,
    keepAppSid: appSidNum,
    keepAppDt: appDtText,
    keepShowRecordId: SHOWS_RECORD_ID,
    currentRunTime: CURRENT_RUN_TIME,
    tableNameForErr: T_GROUPS,
  });

  delClassesIn = await deleteInScopeNotKeptGlobal({
    table: classesTable,
    keyField: KEY_CLASSES,
    keepKeySet: payloadClassesKeySet,
    keepAppSid: appSidNum,
    keepAppDt: appDtText,
    keepShowRecordId: SHOWS_RECORD_ID,
    currentRunTime: CURRENT_RUN_TIME,
    tableNameForErr: T_CLASSES,
  });

  delEntriesIn = await deleteInScopeNotKeptGlobal({
    table: entriesTable,
    keyField: KEY_ENTRIES,
    keepKeySet: payloadEntriesKeySet,
    keepAppSid: appSidNum,
    keepAppDt: appDtText,
    keepShowRecordId: SHOWS_RECORD_ID,
    currentRunTime: CURRENT_RUN_TIME,
    tableNameForErr: T_ENTRIES,
  });
} else if (scheduleFetchOk && !hasTripsPayload) {
  delTripsIn = { table: tripsTable.name, skipped: true, reason: "empty keep-set after successful schedule fetch" };
  delGroupsIn = { table: groupsTable.name, skipped: true, reason: "empty keep-set after successful schedule fetch" };
  delClassesIn = { table: classesTable.name, skipped: true, reason: "empty keep-set after successful schedule fetch" };
  delEntriesIn = { table: entriesTable.name, skipped: true, reason: "empty keep-set after successful schedule fetch" };
}

//////////////////////
// SUCCESS + OUTPUTS
//////////////////////
await flushAutomationErrQueue();
await safeSetSuccess("ok");

const t1 = Date.now();

output.set("ok", true);
output.set("run_time", CURRENT_RUN_TIME);
output.set("run_day", RUN_DAY);
output.set("run_id", RUN_ID);

output.set("app_sid", appSidNum);
output.set("app_dt", appDtText);
output.set("show_id", appSidNum);
output.set("date", appDtText);
output.set("shows_record_id", SHOWS_RECORD_ID || "");
output.set("shows_link_found", !!SHOWS_RECORD_ID);

output.set("schedule_url", scheduleUrl);
output.set("schedule_fetch_ok", scheduleFetchOk);
output.set("schedule_rows", schedRows.length);
output.set("schedule_group_rows", schedGroupRows.length);
output.set("watch_schedule_rows_in_scope", watchScheduleRowsInScope);
output.set("watch_schedule_class_ids_in_scope", watchScheduleByClassId.size);
output.set("valid_daily_class_source", validDailyClassSource);
output.set("suppress_empty_payload_placeholders", suppressEmptyPayloadPlaceholders);
output.set("force_clear_in_scope_because_no_schedule", forceClearInScopeBecauseNoSchedule);
output.set("pids", JSON.stringify(pids));
output.set("automation_errs_preflight", jsonForOutput({
  table_found: !!automationErrsTable,
  writable_field_count: W_AUTOMATION_ERRS.size,
  field_types: {
    pid: String(getFieldByName(automationErrsTable, "pid")?.type || ""),
    app_show_id: String(getFieldByName(automationErrsTable, "app_show_id")?.type || ""),
    people_show_id: String(getFieldByName(automationErrsTable, "people_show_id")?.type || ""),
    message: String(getFieldByName(automationErrsTable, "message")?.type || ""),
    app_sql_date: String(getFieldByName(automationErrsTable, "app_sql_date")?.type || ""),
    last_run: String(getFieldByName(automationErrsTable, "last_run")?.type || ""),
  },
}));
output.set("automation_errs_write_attempts", automationErrWriteAttempts);
output.set("automation_errs_write_success", automationErrWriteSuccess);
output.set("automation_errs_write_failures", automationErrWriteFailures);
output.set("automation_errs_write_failure_messages", JSON.stringify(automationErrWriteFailureMessages));
output.set("fetch_requests_used", fetchRequestCount);
output.set("fetch_requests_limit", MAX_FETCH_REQUESTS);
output.set("fetch_quota_exceeded", fetchQuotaExceeded);
output.set("fetch_quota_first_blocked_url", fetchQuotaFirstBlockedUrl);
output.set("people_payload_statuses", jsonForOutput([...peoplePayloadStatusByPid.values()]));
output.set("primary_rules", JSON.stringify(
  [...primaryRuleByPid.values()].map(r => ({
    primaryPid: r.primaryPid,
    hasTrainerRow: r.hasTrainerRow,
    loanPairs: r.loanPairs.map(x => x.pairKey),
    ignores: [...r.ignoreEntrySet],
  }))
));

output.set("watch_trips_rows", watchTripsRows.length);
output.set("has_trips_payload", hasTripsPayload);
output.set("has_real_trips_payload", hasRealTripsPayload);
output.set("empty_payload_placeholder_rows", emptyPayloadPlaceholderPids.length);
output.set("empty_payload_placeholder_pids", JSON.stringify(emptyPayloadPlaceholderPids));
output.set("suppressed_empty_payload_placeholder_pids", JSON.stringify(suppressedEmptyPayloadPlaceholderPids));
output.set("people_trips_rows", peopleTrips.length);
output.set("class_schedule_fallback_ids", JSON.stringify([]));
output.set("class_schedule_fallback_loaded", 0);
output.set("class_schedule_fallback_failures", 0);
output.set("class_schedule_fallback_failure_sample", JSON.stringify([]));
output.set("people_trips_outside_schedule_class_count", peopleTripsOutsideScheduleClassIds.length);
output.set("people_trips_outside_schedule_class_ids", JSON.stringify(peopleTripsOutsideScheduleClassIds));
output.set("people_trips_missing_schedule", peopleTripsMissingSchedule);
output.set("people_trips_missing_schedule_sample", JSON.stringify(peopleTripsMissingScheduleSample));

output.set("watch_trips_created", upTrips.created);
output.set("watch_trips_updated", upTrips.updated);

output.set("watch_groups_wanted", wantGroup.size);
output.set("watch_groups_created", upGroups.created);
output.set("watch_groups_updated", upGroups.updated);

output.set("watch_classes_wanted", wantClass.size);
output.set("watch_classes_created", upClasses.created);
output.set("watch_classes_updated", upClasses.updated);

output.set("watch_entries_wanted", wantEntry.size);
output.set("watch_entries_created", upEntries.created);
output.set("watch_entries_updated", upEntries.updated);

output.set("trip_links_missing_entry_target", tripLinksMissingEntryTarget);
output.set("trip_links_missing_class_target", tripLinksMissingClassTarget);
output.set("trip_links_missing_group_target", tripLinksMissingGroupTarget);

output.set("diag_watch_groups", jsonForOutput(upGroups.diag));
output.set("diag_watch_classes", jsonForOutput(upClasses.diag));
output.set("diag_watch_entries", jsonForOutput(upEntries.diag));

output.set("class_enrich_enabled", classEnrichEnabled);
output.set("class_enrich_disabled_reason", classEnrichDisabledReason);
output.set("class_enrich_unique_class_ids", uniqueClassIds.length);
output.set("class_enrich_remaining_fetch_budget", remainingFetchBudgetBeforeClassEnrich);
output.set("class_enrich_updates", enrichUpdates.length);
output.set("class_enrich_skipped_by_budget_guard", classEnrichSkippedByBudgetGuard);
output.set("class_enrich_skipped_by_budget_guard_sample", JSON.stringify(classEnrichSkippedByBudgetGuardSample));
output.set("class_enrich_skipped_due_to_fetch_quota", classEnrichSkippedDueToFetchQuota);
output.set("class_enrich_skipped_due_to_fetch_quota_sample", JSON.stringify(classEnrichSkippedDueToFetchQuotaSample));
output.set("class_enrich_errors", JSON.stringify(enrichErrors.slice(0, 25)));

output.set("delete_results_trips_out_of_scope", JSON.stringify(delTripsOut));
output.set("delete_results_groups_out_of_scope", JSON.stringify(delGroupsOut));
output.set("delete_results_classes_out_of_scope", JSON.stringify(delClassesOut));
output.set("delete_results_entries_out_of_scope", JSON.stringify(delEntriesOut));

output.set("delete_results_trips_in_scope", JSON.stringify(delTripsIn));
output.set("delete_results_groups_in_scope", JSON.stringify(delGroupsIn));
output.set("delete_results_classes_in_scope", JSON.stringify(delClassesIn));
output.set("delete_results_entries_in_scope", JSON.stringify(delEntriesIn));

output.set("ms_runtime", t1 - t0);
