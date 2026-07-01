const fs = require("fs");
const path = require("path");
const crypto = require("crypto");
const { spawn, spawnSync } = require("child_process");
const {
  normalizeHeartbeatMode,
  modeAllowsHeavy,
  resolveHeartbeatCadenceSeconds,
} = require("./lib/heartbeat_mode");
const {
  computeDefaultShowDateGuard,
} = require("./lib/default_show_date_guard");

const AIRTABLE_TOKEN = process.env.AIRTABLE_TOKEN || "";
const AIRTABLE_BASE_ID = process.env.AIRTABLE_BASE_ID || "";

const TABLE_HEARTBEAT = process.env.TABLE_HEARTBEAT || "heartbeat";
const TABLE_SHOWS = process.env.TABLE_SHOWS || "shows";
const HEARTBEAT_CREATED_FIELD = process.env.HEARTBEAT_CREATED_FIELD || "created_time";
const HEARTBEAT_ISA_FIELD = process.env.HEARTBEAT_ISA_FIELD || "isA";
const HEARTBEAT_ISB_FIELD = process.env.HEARTBEAT_ISB_FIELD || "isB";
const HEARTBEAT_ISC_FIELD = process.env.HEARTBEAT_ISC_FIELD || "isC";
const HEARTBEAT_ISD_FIELD = process.env.HEARTBEAT_ISD_FIELD || "isD";
const HEARTBEAT_MODE_FIELD = process.env.HEARTBEAT_MODE_FIELD || process.env.FIELD_MODE || "mode";
const HEARTBEAT_CADENCE_FIELD = process.env.HEARTBEAT_CADENCE_FIELD || process.env.FIELD_CADENCE || "cadence";
const HEARTBEAT_SET_INTERVALS_FIELD = process.env.HEARTBEAT_SET_INTERVALS_FIELD || process.env.FIELD_SET_INTERVALS || "set_intervals";
const HEARTBEAT_INTERVAL_FIELD = process.env.HEARTBEAT_INTERVAL_FIELD || process.env.FIELD_INTERVAL || "interval";
const HEARTBEAT_SHIFTED_NEXT_DAY_FIELD = process.env.HEARTBEAT_SHIFTED_NEXT_DAY_FIELD || process.env.FIELD_SHIFTED_NEXT_DAY || "shifted_to_next_day";

const DEFAULT_TRIPS_DAILY_SLOTS = "A,B,C,D";
const DEFAULT_TRIPS_TAGGER_SLOTS = "A,C";
const DEFAULT_TRIPS_CALCULATOR_SLOTS = "A,C";
const DEFAULT_SCHEDULES_DAILY_SLOTS = "B,D";
const DEFAULT_SCHEDULES_DAILY_NIGHT_SLOTS = "A,C";
const DEFAULT_SCHEDULES_CALCULATOR_SLOTS = "A,B,C,D";
const DEFAULT_LIVE_GROUPS_SLOTS = "A,B,C,D";
const DEFAULT_LIVE_RINGS_SLOTS = "A,B,C,D";
const DEFAULT_LIVE_CLASS_DETAIL_SLOTS = "A,B,C,D";
const DEFAULT_PUBLISHER_SLOTS = "A,B,C,D";
const DEFAULT_WEC_HEARTBEAT_SLOTS = "A,B,C,D";

const HTTP_TIMEOUT_MS = Number(process.env.HTTP_TIMEOUT_MS || "20000");
const LOG_DIR = process.env.RUNNER_LOG_DIR || "C:\\actions-runner\\ringstatus";
const LOG_PATH = process.env.ORCH_LOG_PATH || path.join(LOG_DIR, "heartbeat-slot-orchestrator.log");
const LOCK_PATH = process.env.ORCH_LOCK_PATH || path.join(LOG_DIR, "heartbeat-slot-orchestrator.lock");
const LOCK_STALE_MINUTES = Math.max(1, Number(process.env.ORCH_LOCK_STALE_MINUTES || "8") || 8);
const POWERSHELL_STEP_TIMEOUT_MS = Math.max(60000, Number(process.env.ORCH_POWERSHELL_STEP_TIMEOUT_MS || "180000") || 180000);
const DISABLE_HEAVY = String(process.env.ORCH_DISABLE_HEAVY || "0") === "1";
const DISABLE_LIVE_CLASS_DETAIL = String(process.env.ORCH_DISABLE_LIVE_CLASS_DETAIL || "0") === "1";
const ENABLE_WEC_HEARTBEAT = String(process.env.ORCH_WEC_ENABLED || "1") === "1";
const WEC_HELPER_ONLY_CADENCE = String(process.env.ORCH_WEC_HELPER_ONLY || "0") === "1";
const WEC_FOCUS_WORKFLOW_INTERVAL_MINUTES = Math.max(1, Number(process.env.ORCH_WEC_FOCUS_WORKFLOW_INTERVAL_MINUTES || "12") || 12);
const WEC_AIRTABLE_CONTROLS_INTERVAL_MINUTES = Math.max(1, Number(process.env.ORCH_WEC_AIRTABLE_CONTROLS_INTERVAL_MINUTES || "30") || 30);
const WEC_STAGE2_UPDATE_SCHEDULE_WRAPPER = process.env.WEC_STAGE2_UPDATE_SCHEDULE_WRAPPER || "C:\\Users\\gombc\\OneDrive - Sport Dog Food\\github\\repos\\ringstatus-data\\catalyst-workspaces\\horseshowing\\runners\\sync_focus_update_schedule_to_staging.js";
const WEC_HELPER_REPAIR_WRAPPER = process.env.WEC_HELPER_REPAIR_WRAPPER || "C:\\Users\\gombc\\OneDrive - Sport Dog Food\\github\\repos\\ringstatus-data\\catalyst-workspaces\\horseshowing\\runners\\repair_update_schedule_staging_links.js";
const WEC_ACTIVE_ENTRIES_WRAPPER = process.env.WEC_ACTIVE_ENTRIES_WRAPPER || "C:\\Users\\gombc\\OneDrive - Sport Dog Food\\github\\repos\\ringstatus-data\\catalyst-workspaces\\horseshowing\\runners\\process_active_entries.js";
const WEC_STACKED_WORKFLOW_WRAPPER = process.env.WEC_STACKED_WORKFLOW_WRAPPER || "C:\\Users\\gombc\\OneDrive - Sport Dog Food\\github\\repos\\ringstatus-data\\catalyst-workspaces\\horseshowing\\runners\\run_wec_stacked_workflow.js";
const WEC_CLASS_OOG_ONLY_WRAPPER = process.env.WEC_CLASS_OOG_ONLY_WRAPPER || "C:\\Users\\gombc\\OneDrive - Sport Dog Food\\github\\repos\\ringstatus-data\\catalyst-workspaces\\horseshowing\\runners\\sync_class_oog_and_class_start_times.js";
const WEC_CLASS_LANE_RUNNER_URL = process.env.WEC_CLASS_LANE_RUNNER_URL || "https://horseshowing-700800454.development.catalystserverless.com/server/horseshowing_class_lane_runner/";
const WEC_AIRTABLE_BASE_ID = process.env.WEC_AIRTABLE_BASE_ID || "app6XS1RvsPNRT6os";
const WEC_WORKFLOWV4_CLASS_OOG_MAX_UNITS = Math.max(1, Number(process.env.WEC_WORKFLOWV4_CLASS_OOG_MAX_UNITS || "250") || 250);
const DEFAULT_SHOW_DATE_GUARD_BLOCK_HEAVY = String(process.env.DEFAULT_SHOW_DATE_GUARD_BLOCK_HEAVY || "1") === "1";
const RUN_INLINE = String(process.env.ORCH_RUN_INLINE || "0") === "1";
const DETACHED_CHILD = String(process.env.ORCH_DETACHED_CHILD || "0") === "1";
const WEC_ALERTS_VERIFY_ONLY = String(process.env.WEC_ALERTS_VERIFY_ONLY || "0") === "1"
  || process.argv.includes("--wec-alerts-verify");
const TABLE_AUTOMATION_ERRS = process.env.TABLE_AUTOMATION_ERRS || "automation_errs";
const ORCH_ALERT_NO_ACTIVE_FEEDS = String(process.env.ORCH_ALERT_NO_ACTIVE_FEEDS || "1") === "1";
const ORCH_STEP_OVERRUN_ALERT_MS = Math.max(60000, Number(process.env.ORCH_STEP_OVERRUN_ALERT_MS || "240000") || 240000);
const WRITABLE_AUTOMATION_ERR_TYPES = new Set([
  "singleLineText",
  "multilineText",
  "number",
  "date",
  "dateTime",
  "checkbox",
  "singleSelect",
  "multipleSelects",
]);

const SCRIPT_LOG_FILES = {
  "schedules_dailyv2.js": "schedules-dailyv2.log",
  "schedules_calculatorv2.js": "schedules-calculatorv2.log",
  "trips_dailyv2.js": "trips-dailyv2.log",
  "trips_tagger.js": "trips-tagger.log",
  "trips_calculatorv2.js": "trips-calculatorv2.log",
  "live_groups_daily.js": "live-groups-daily.log",
  "live_rings_daily.js": "live-rings-daily.log",
  "live_class_detail.js": "live-class-detail.log",
  "docs/horseshowing/wec-heartbeat.js": "wec-heartbeat.log",
  "docs/horseshowing/run-wec-catalyst-workflow.ps1": "wec-catalyst-workflow.log",
  [WEC_STAGE2_UPDATE_SCHEDULE_WRAPPER]: "sync_focus_update_schedule_to_staging.log",
  [WEC_HELPER_REPAIR_WRAPPER]: "repair_update_schedule_staging_links.log",
  [WEC_ACTIVE_ENTRIES_WRAPPER]: "process_active_entries.log",
  [WEC_STACKED_WORKFLOW_WRAPPER]: "run_wec_stacked_workflow.log",
  [WEC_CLASS_OOG_ONLY_WRAPPER]: "sync_class_oog_and_class_start_times.log",
  "docs/horseshowing/sync-airtable-controls.js": "wec-airtable-controls.log",
  "publisher.js": "publisher.log",
};

if (!AIRTABLE_TOKEN) throw new Error("Missing AIRTABLE_TOKEN");
if (!AIRTABLE_BASE_ID) throw new Error("Missing AIRTABLE_BASE_ID");

function ensureLogDir() {
  fs.mkdirSync(path.dirname(LOG_PATH), { recursive: true });
}

function appendEvent(event) {
  ensureLogDir();
  const payload = {
    ts: new Date().toISOString(),
    ...event,
  };
  fs.appendFileSync(LOG_PATH, `${JSON.stringify(payload)}\r\n`, "utf8");
  console.log(JSON.stringify(payload));
}

function appendScriptLog(scriptName, text) {
  const logFileName = SCRIPT_LOG_FILES[scriptName] || "heartbeat-slot-orchestrator-steps.log";
  const logPath = path.join(LOG_DIR, logFileName);
  fs.mkdirSync(path.dirname(logPath), { recursive: true });
  fs.appendFileSync(logPath, text, "utf8");
}

function parseSlotSet(value, fallback) {
  return new Set(
    String(value || fallback || "")
      .split(",")
      .map((item) => item.trim().toUpperCase())
      .filter(Boolean)
  );
}

function slotIsDue(slot, value, fallback) {
  if (!slot) return false;
  return parseSlotSet(value, fallback).has(String(slot).toUpperCase());
}

function todayIsoDate() {
  return new Date().toISOString().slice(0, 10);
}

function safeJson(value, maxLength = 4000) {
  try {
    return JSON.stringify(value).slice(0, maxLength);
  } catch {
    return String(value || "").slice(0, maxLength);
  }
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

function firstValue(value) {
  if (Array.isArray(value)) return value.length ? firstValue(value[0]) : undefined;
  if (value && typeof value === "object" && "name" in value) return value.name;
  return value;
}

function strOrNull(value) {
  const v = firstValue(value);
  if (v === null || v === undefined) return null;
  const text = String(v).trim();
  if (!text || text.toLowerCase() === "null" || text.toLowerCase() === "nan") return null;
  return text;
}

function numOrNull(value) {
  const text = strOrNull(value);
  if (text === null) return null;
  const num = Number(text);
  return Number.isFinite(num) ? num : null;
}

function airtableUrl(tableName, params = {}) {
  return airtableUrlForBase(AIRTABLE_BASE_ID, tableName, params);
}

function airtableUrlForBase(baseId, tableName, params = {}) {
  const url = new URL(`https://api.airtable.com/v0/${baseId}/${encodeURIComponent(tableName)}`);
  for (const [key, value] of Object.entries(params)) {
    if (Array.isArray(value)) {
      for (const item of value) url.searchParams.append(key, item);
    } else if (value !== undefined && value !== null) {
      url.searchParams.set(key, String(value));
    }
  }
  return url;
}

async function fetchWithTimeout(url, options = {}) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), HTTP_TIMEOUT_MS);
  try {
    return await fetch(url, { ...options, signal: controller.signal });
  } finally {
    clearTimeout(timeout);
  }
}

async function airtableList(tableName, params = {}) {
  return airtableListForBase(AIRTABLE_BASE_ID, tableName, params);
}

async function airtableListForBase(baseId, tableName, params = {}) {
  const out = [];
  let offset = null;

  do {
    const url = airtableUrlForBase(baseId, tableName, { ...params, offset });
    const response = await fetchWithTimeout(url, {
      headers: { Authorization: `Bearer ${AIRTABLE_TOKEN}` },
    });
    const body = await response.text();
    if (!response.ok) {
      throw new Error(`Airtable list failed (${response.status}) ${tableName}: ${body.slice(0, 500)}`);
    }
    const json = JSON.parse(body);
    out.push(...(json.records || []));
    offset = json.offset || null;
  } while (offset);

  return out;
}

async function latestHeartbeat() {
  const rows = await airtableList(TABLE_HEARTBEAT, {
    maxRecords: 1,
    "sort[0][field]": HEARTBEAT_CREATED_FIELD,
    "sort[0][direction]": "desc",
    "fields[]": [
      HEARTBEAT_CREATED_FIELD,
      HEARTBEAT_ISA_FIELD,
      HEARTBEAT_ISB_FIELD,
      HEARTBEAT_ISC_FIELD,
      HEARTBEAT_ISD_FIELD,
      HEARTBEAT_MODE_FIELD,
      HEARTBEAT_CADENCE_FIELD,
      HEARTBEAT_SET_INTERVALS_FIELD,
      HEARTBEAT_INTERVAL_FIELD,
      "show_id",
      "app_show_id",
      "sql_date",
      "app_sql_date",
      "app_dow_raw",
      "set_to_default_app_sql_date",
      "default_app_sql_date_is",
      "show_app_sql_start_date",
      "show_app_sql_end_date",
      "check_show_date",
      "default_show_date_status",
      HEARTBEAT_SHIFTED_NEXT_DAY_FIELD,
      "time",
    ],
  });
  return rows[0] || null;
}

async function airtableCreate(tableName, records) {
  if (!records.length) return [];
  const response = await fetchWithTimeout(airtableUrl(tableName), {
    method: "POST",
    headers: {
      Authorization: `Bearer ${AIRTABLE_TOKEN}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ records }),
  });
  const body = await response.text();
  if (!response.ok) {
    throw new Error(`Airtable create failed (${response.status}) ${tableName}: ${body.slice(0, 500)}`);
  }
  return body ? JSON.parse(body).records || [] : [];
}

async function airtableUpdateForBase(baseId, tableName, records) {
  if (!records.length) return [];
  const response = await fetchWithTimeout(airtableUrlForBase(baseId, tableName), {
    method: "PATCH",
    headers: {
      Authorization: `Bearer ${AIRTABLE_TOKEN}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ records }),
  });
  const body = await response.text();
  if (!response.ok) {
    throw new Error(`Airtable update failed (${response.status}) ${tableName}: ${body.slice(0, 500)}`);
  }
  return body ? JSON.parse(body).records || [] : [];
}

async function tableFieldMapForBase(baseId, tableName) {
  const url = new URL(`https://api.airtable.com/v0/meta/bases/${baseId}/tables`);
  const response = await fetchWithTimeout(url, {
    headers: { Authorization: `Bearer ${AIRTABLE_TOKEN}` },
  });
  const body = await response.text();
  if (!response.ok) {
    throw new Error(`Airtable meta failed (${response.status}) ${tableName}: ${body.slice(0, 500)}`);
  }
  const json = body ? JSON.parse(body) : {};
  const table = (json.tables || []).find((item) => item.name === tableName);
  if (!table) throw new Error(`Airtable table not found: ${tableName}`);
  return new Map((table.fields || []).map((field) => [field.name, field]));
}

async function tableMetaForBase(baseId, tableName) {
  const url = new URL(`https://api.airtable.com/v0/meta/bases/${baseId}/tables`);
  const response = await fetchWithTimeout(url, {
    headers: { Authorization: `Bearer ${AIRTABLE_TOKEN}` },
  });
  const body = await response.text();
  if (!response.ok) {
    throw new Error(`Airtable meta failed (${response.status}) ${tableName}: ${body.slice(0, 500)}`);
  }
  const json = body ? JSON.parse(body) : {};
  const table = (json.tables || []).find((item) => item.name === tableName);
  if (!table) throw new Error(`Airtable table not found: ${tableName}`);
  return table;
}

async function tableFieldMap(tableName) {
  return tableFieldMapForBase(AIRTABLE_BASE_ID, tableName);
}

async function automationErrWritableFields() {
  const fieldMap = await tableFieldMap(TABLE_AUTOMATION_ERRS);
  const names = new Set();
  for (const [name, field] of fieldMap.entries()) {
    if (WRITABLE_AUTOMATION_ERR_TYPES.has(field.type)) names.add(name);
  }
  return names;
}

function pickWritable(fields, writable) {
  const out = {};
  for (const [key, value] of Object.entries(fields || {})) {
    if (!writable.has(key)) continue;
    if (value === undefined || value === null) continue;
    if (typeof value === "string" && value.trim() === "") continue;
    out[key] = value;
  }
  return out;
}

async function createAutomationErr(fields) {
  try {
    const writable = await automationErrWritableFields();
    const safeFields = pickWritable(fields, writable);
    if (!Object.keys(safeFields).length) return { skipped: true, reason: "empty_fields" };
    const created = await airtableCreate(TABLE_AUTOMATION_ERRS, [{ fields: safeFields }]);
    appendEvent({
      ok: true,
      event: "automation_err_recorded",
      error_type: fields?.error_type || null,
      automation_err_id: created[0]?.id || null,
    });
    return { ok: true, id: created[0]?.id || null };
  } catch (error) {
    appendEvent({
      ok: false,
      event: "automation_err_write_failed",
      error_type: fields?.error_type || null,
      error: String(error?.message || error).slice(0, 500),
    });
    return { ok: false, error: String(error?.message || error).slice(0, 500) };
  }
}

async function recordOrchestratorAlert({
  errorType,
  message,
  heartbeat = null,
  scriptName = null,
  resolved = false,
  pid = null,
  extra = {},
}) {
  const fields = heartbeat?.fields || {};
  const appShowId = numOrNull(fields.app_show_id) ?? numOrNull(fields.show_id);
  const appSqlDate = strOrNull(fields.app_sql_date) || strOrNull(fields.sql_date);
  return createAutomationErr({
    automation_key: [
      "heartbeat_slot_orchestrator",
      errorType || "notice",
      appShowId || "show",
      appSqlDate || "date",
      scriptName || "orchestrator",
      Date.now(),
    ].join("|").slice(0, 1000),
    automation_name: "heartbeat_slot_orchestrator",
    error_type: errorType || "notice",
    app_sql_date: appSqlDate,
    run_id: Date.now(),
    last_run: todayIsoDate(),
    resolved: !!resolved,
    message: String(message || safeJson(extra)).slice(0, 10000),
    app_show_id: appShowId,
    pid: numOrNull(pid),
  });
}

async function showManualOverride(appShowId) {
  const showId = numOrNull(appShowId);
  if (showId === null) return { found: false, is_default_show_manual_override: false };
  const rows = await airtableList(TABLE_SHOWS, {
    maxRecords: 10,
    filterByFormula: `OR({show_id}=${showId},{app_show_id}=${showId})`,
    "fields[]": [
      "show_id",
      "app_show_id",
      "is_default_show_manual_override",
      "check_show_date (from heartbeat)",
    ],
  });
  const row = rows[0] || null;
  return {
    found: !!row,
    record_id: row?.id || null,
    matched_count: rows.length,
    is_default_show_manual_override: boolValue(row?.fields?.is_default_show_manual_override),
  };
}

function slotFromFields(fields = {}) {
  const active = [
    fields[HEARTBEAT_ISA_FIELD] ? "A" : null,
    fields[HEARTBEAT_ISB_FIELD] ? "B" : null,
    fields[HEARTBEAT_ISC_FIELD] ? "C" : null,
    fields[HEARTBEAT_ISD_FIELD] ? "D" : null,
  ].filter(Boolean);
  return active.length === 1 ? active[0] : null;
}

function runNodeScript(scriptName, extraEnv = {}) {
  const startedAt = Date.now();
  const scriptPath = path.resolve(__dirname, scriptName);
  const label = scriptName.replace(/\.js$/i, "").toUpperCase();
  const timestamp = new Date().toISOString().replace("T", " ").slice(0, 19);
  appendEvent({ ok: true, event: "step_started", script: scriptName });

  const result = spawnSync(process.execPath, [scriptPath], {
    cwd: __dirname,
    env: { ...process.env, ...extraEnv },
    encoding: "utf8",
    windowsHide: true,
  });

  const exitCode = Number(result.status ?? (result.error ? -1 : 0));
  const ok = exitCode === 0;
  const durationMs = Date.now() - startedAt;
  const output = [
    `[${timestamp}] ${label} RUN`,
    result.stdout || "",
    result.stderr || "",
    JSON.stringify({
      ok,
      event: "step_completed",
      script: scriptName,
      exit_code: exitCode,
      duration_ms: durationMs,
      pipeline: "heartbeat_slot_orchestrator",
      error: result.error ? String(result.error.message || result.error).slice(0, 500) : undefined,
    }),
    "",
  ].join("\r\n");
  appendScriptLog(scriptName, output);

  if (result.stdout) process.stdout.write(result.stdout);
  if (result.stderr) process.stderr.write(result.stderr);

  appendEvent({
    ok,
    event: "step_completed",
    script: scriptName,
    exit_code: exitCode,
    duration_ms: durationMs,
    error: result.error ? String(result.error.message || result.error).slice(0, 500) : undefined,
  });

  return {
    ok,
    exitCode,
    durationMs,
    stdout: result.stdout || "",
    stderr: result.stderr || "",
  };
}

function runNodeScriptAbsolute(scriptPath, extraEnv = {}, args = []) {
  const startedAt = Date.now();
  const label = path.basename(scriptPath).replace(/\.js$/i, "").toUpperCase();
  const timestamp = new Date().toISOString().replace("T", " ").slice(0, 19);
  appendEvent({ ok: true, event: "step_started", script: scriptPath, args });

  const result = spawnSync(process.execPath, [scriptPath, ...args], {
    cwd: path.dirname(scriptPath),
    env: { ...process.env, ...extraEnv },
    encoding: "utf8",
    windowsHide: true,
  });

  const exitCode = Number(result.status ?? (result.error ? -1 : 0));
  const ok = exitCode === 0;
  const durationMs = Date.now() - startedAt;
  const output = [
    `[${timestamp}] ${label} RUN`,
    result.stdout || "",
    result.stderr || "",
    JSON.stringify({
      ok,
      event: "step_completed",
      script: scriptPath,
      exit_code: exitCode,
      duration_ms: durationMs,
      pipeline: "heartbeat_slot_orchestrator",
      error: result.error ? String(result.error.message || result.error).slice(0, 500) : undefined,
    }),
    "",
  ].join("\r\n");
  appendScriptLog(scriptPath, output);

  if (result.stdout) process.stdout.write(result.stdout);
  if (result.stderr) process.stderr.write(result.stderr);

  appendEvent({
    ok,
    event: "step_completed",
    script: scriptPath,
    exit_code: exitCode,
    duration_ms: durationMs,
    error: result.error ? String(result.error.message || result.error).slice(0, 500) : undefined,
  });

  return {
    ok,
    exitCode,
    durationMs,
    stdout: result.stdout || "",
    stderr: result.stderr || "",
  };
}

function runPowerShellScript(scriptName, extraEnv = {}) {
  const startedAt = Date.now();
  const scriptPath = path.resolve(__dirname, scriptName);
  const label = scriptName.replace(/\.ps1$/i, "").toUpperCase();
  const timestamp = new Date().toISOString().replace("T", " ").slice(0, 19);
  appendEvent({ ok: true, event: "step_started", script: scriptName });

  const result = spawnSync("powershell.exe", [
    "-NoProfile",
    "-ExecutionPolicy",
    "Bypass",
    "-File",
    scriptPath,
  ], {
    cwd: __dirname,
    env: { ...process.env, ...extraEnv },
    encoding: "utf8",
    windowsHide: true,
    timeout: POWERSHELL_STEP_TIMEOUT_MS,
  });

  const exitCode = Number(result.status ?? (result.error ? -1 : 0));
  const ok = exitCode === 0;
  const durationMs = Date.now() - startedAt;
  const output = [
    `[${timestamp}] ${label} RUN`,
    result.stdout || "",
    result.stderr || "",
    JSON.stringify({
      ok,
      event: "step_completed",
      script: scriptName,
      exit_code: exitCode,
      duration_ms: durationMs,
      pipeline: "heartbeat_slot_orchestrator",
      error: result.error ? String(result.error.message || result.error).slice(0, 500) : undefined,
    }),
    "",
  ].join("\r\n");
  appendScriptLog(scriptName, output);

  if (result.stdout) process.stdout.write(result.stdout);
  if (result.stderr) process.stderr.write(result.stderr);

  appendEvent({
    ok,
    event: "step_completed",
    script: scriptName,
    exit_code: exitCode,
    duration_ms: durationMs,
    error: result.error ? String(result.error.message || result.error).slice(0, 500) : undefined,
  });

  return { ok, exitCode, durationMs };
}

function acquireLock() {
  fs.mkdirSync(path.dirname(LOCK_PATH), { recursive: true });
  let existingLock = null;

  if (fs.existsSync(LOCK_PATH)) {
    const stat = fs.statSync(LOCK_PATH);
    const ageMs = Date.now() - stat.mtimeMs;
    try {
      existingLock = JSON.parse(fs.readFileSync(LOCK_PATH, "utf8"));
    } catch {
      existingLock = { parse_error: true };
    }
    if (ageMs > LOCK_STALE_MINUTES * 60 * 1000) {
      fs.rmSync(LOCK_PATH, { force: true });
      appendEvent({ ok: true, event: "stale_lock_removed", lock_path: LOCK_PATH, age_ms: Math.round(ageMs) });
      appendEvent({
        ok: true,
        event: "stale_lock_removed_details",
        lock_path: LOCK_PATH,
        lock_age_ms: Math.round(ageMs),
        lock: existingLock,
      });
      existingLock = null;
    }
  }

  try {
    const fd = fs.openSync(LOCK_PATH, "wx");
    fs.writeFileSync(fd, JSON.stringify({ pid: process.pid, started_at: new Date().toISOString() }));
    fs.closeSync(fd);
    return { acquired: true };
  } catch {
    let lockAgeMs = null;
    try {
      lockAgeMs = Math.round(Date.now() - fs.statSync(LOCK_PATH).mtimeMs);
    } catch {
      lockAgeMs = null;
    }
    appendEvent({
      ok: true,
      event: "orchestrator_skipped_locked",
      lock_path: LOCK_PATH,
      lock_age_ms: lockAgeMs,
      lock: existingLock,
    });
    return {
      acquired: false,
      staleRemoved: false,
      lockPath: LOCK_PATH,
      lockAgeMs,
      existingLock,
    };
  }
}

function releaseLock() {
  fs.rmSync(LOCK_PATH, { force: true });
}

function intervalStatePath(name) {
  return path.join(LOG_DIR, `${name}.json`);
}

function readIntervalState(name) {
  try {
    return JSON.parse(fs.readFileSync(intervalStatePath(name), "utf8"));
  } catch {
    return {};
  }
}

function writeIntervalState(name, state) {
  ensureLogDir();
  fs.writeFileSync(intervalStatePath(name), `${JSON.stringify(state, null, 2)}\n`, "utf8");
}

function intervalDue(name, minutes) {
  const state = readIntervalState(name);
  const lastRunAt = state.last_run_at ? new Date(state.last_run_at) : null;
  if (!lastRunAt || Number.isNaN(lastRunAt.getTime())) return true;
  return Date.now() - lastRunAt.getTime() >= minutes * 60 * 1000;
}

function markIntervalRun(name) {
  writeIntervalState(name, { last_run_at: new Date().toISOString() });
}

function readWecWorkflowV4FocusState() {
  return readIntervalState("wec-workflowv4-focus-day");
}

function writeWecWorkflowV4FocusState(state) {
  writeIntervalState("wec-workflowv4-focus-day", state);
}

async function runOrchestrator() {
  if (WEC_HELPER_ONLY_CADENCE) {
    const lockResult = acquireLock();
    if (!lockResult.acquired) return;
    try {
      appendEvent({
        ok: true,
        event: "wec_helper_only_cadence_started",
        script: WEC_HELPER_REPAIR_WRAPPER,
      });
      const helperResult = runNodeScriptAbsolute(WEC_HELPER_REPAIR_WRAPPER, {
        WEC_AIRTABLE_BASE_ID: process.env.WEC_AIRTABLE_BASE_ID || "app6XS1RvsPNRT6os",
      });
      appendEvent({
        ok: helperResult.ok,
        event: "wec_helper_only_cadence_completed",
        script: WEC_HELPER_REPAIR_WRAPPER,
        exit_code: helperResult.exitCode,
        duration_ms: helperResult.durationMs,
      });
      if (!helperResult.ok) process.exitCode = 1;
      return;
    } finally {
      releaseLock();
    }
  }

  if (DISABLE_HEAVY) {
    appendEvent({ ok: true, event: "orchestrator_disabled" });
    return;
  }

  const lockResult = acquireLock();
  if (!lockResult.acquired) {
    const heartbeat = await latestHeartbeat().catch(() => null);
    await recordOrchestratorAlert({
      errorType: lockResult.staleRemoved ? "heartbeat_orchestrator_stale_lock_removed" : "heartbeat_orchestrator_locked",
      heartbeat,
      resolved: !!lockResult.staleRemoved,
      pid: lockResult.existingLock?.pid,
      message: `${lockResult.staleRemoved ? "Removed stale heartbeat orchestrator lock" : "Heartbeat orchestrator skipped because previous run still held the lock"} | lock_age_ms=${lockResult.lockAgeMs ?? ""} | lock=${safeJson(lockResult.existingLock, 1000)}`,
      extra: lockResult,
    });
    return;
  }

  try {
    const heartbeat = await latestHeartbeat();
    const slot = slotFromFields(heartbeat?.fields || {});
    const mode = normalizeHeartbeatMode(heartbeat?.fields?.[HEARTBEAT_MODE_FIELD]);
    const cadenceSeconds = resolveHeartbeatCadenceSeconds({
      mode,
      cadence: heartbeat?.fields?.[HEARTBEAT_CADENCE_FIELD],
      set_intervals: heartbeat?.fields?.[HEARTBEAT_SET_INTERVALS_FIELD],
      interval: heartbeat?.fields?.[HEARTBEAT_INTERVAL_FIELD],
    });
    appendEvent({
      ok: true,
      event: "orchestrator_started",
      heartbeat_id: heartbeat?.id || null,
      slot,
      mode,
      cadence_seconds: cadenceSeconds,
      heartbeat_fields: heartbeat?.fields || null,
    });

    if (!modeAllowsHeavy(mode)) {
      appendEvent({
        ok: true,
        event: "orchestrator_mode_noop",
        reason: "mode_blocks_heavy_lanes",
        mode,
        cadence_seconds: cadenceSeconds,
      });
      return;
    }

    function runWecStage2UpdateScheduleWrapper(reason) {
      return runWecStage2UpdateScheduleWrapperWithRun(reason, null);
    }

    function runWecStage2UpdateScheduleWrapperWithRun(reason, runMeta) {
      const args = ["--mirror-only"];
      if (runMeta?.run_id) args.push(`--run-id=${runMeta.run_id}`);
      if (runMeta?.run_time) args.push(`--run-time=${runMeta.run_time}`);
      appendEvent({
        ok: true,
        event: "wec_stage2_mirror_attempt",
        reason,
        script: WEC_STAGE2_UPDATE_SCHEDULE_WRAPPER,
        mode: "mirror-only",
        run_id: runMeta?.run_id || null,
        run_time: runMeta?.run_time || null,
      });
      return runNodeScriptAbsolute(WEC_STAGE2_UPDATE_SCHEDULE_WRAPPER, {
        WEC_AIRTABLE_BASE_ID: process.env.WEC_AIRTABLE_BASE_ID || "app6XS1RvsPNRT6os",
        WEC_RUN_ID: runMeta?.run_id || "",
        WEC_RUN_TIME: runMeta?.run_time || "",
      }, args);
    }

    function runWecStage2StagingWrapperWithRun(reason, runMeta) {
      const args = [];
      if (runMeta?.run_id) args.push(`--run-id=${runMeta.run_id}`);
      if (runMeta?.run_time) args.push(`--run-time=${runMeta.run_time}`);
      appendEvent({
        ok: true,
        event: "wec_stage2_staging_attempt",
        reason,
        script: WEC_STAGE2_UPDATE_SCHEDULE_WRAPPER,
        mode: "staging",
        run_id: runMeta?.run_id || null,
        run_time: runMeta?.run_time || null,
      });
      return runNodeScriptAbsolute(WEC_STAGE2_UPDATE_SCHEDULE_WRAPPER, {
        WEC_AIRTABLE_BASE_ID: process.env.WEC_AIRTABLE_BASE_ID || "app6XS1RvsPNRT6os",
        WEC_RUN_ID: runMeta?.run_id || "",
        WEC_RUN_TIME: runMeta?.run_time || "",
      }, args);
    }

    async function verifyWecApprovedStagingViews({ eventName = "wec_workflowv4_staging_views_checked" } = {}) {
      const requiredViews = [
        { table: "class_start_times", view: "active_entries" },
        { table: "entry_go_times", view: "active_entries" },
      ];
      const missingViews = [];
      for (const requirement of requiredViews) {
        const table = await tableMetaForBase(WEC_AIRTABLE_BASE_ID, requirement.table);
        const viewNames = new Set((table.views || []).map((view) => view.name));
        if (!viewNames.has(requirement.view)) missingViews.push(`${requirement.table}.${requirement.view}`);
      }
      appendEvent({
        ok: missingViews.length === 0,
        event: eventName,
        required_views: requiredViews.map((requirement) => `${requirement.table}.${requirement.view}`),
        missing_views: missingViews,
      });
      return {
        ok: missingViews.length === 0,
        required_views: requiredViews.map((requirement) => `${requirement.table}.${requirement.view}`),
        missing_views: missingViews,
      };
    }

    function parseJsonFromStepOutput(result) {
      const text = String(result?.stdout || "").trim();
      if (!text) return null;
      const start = text.indexOf("{");
      const end = text.lastIndexOf("}");
      if (start < 0 || end < start) return null;
      try {
        return JSON.parse(text.slice(start, end + 1));
      } catch {
        return null;
      }
    }

    function runWecClassOogOnlyUntilComplete(reason) {
      let lastPayload = null;
      for (let unit = 1; unit <= WEC_WORKFLOWV4_CLASS_OOG_MAX_UNITS; unit += 1) {
        appendEvent({
          ok: true,
          event: "wec_class_oog_only_attempt",
          reason,
          script: WEC_CLASS_OOG_ONLY_WRAPPER,
          unit,
        });
        const result = runNodeScriptAbsolute(WEC_CLASS_OOG_ONLY_WRAPPER, {
          WEC_AIRTABLE_BASE_ID: WEC_AIRTABLE_BASE_ID,
        }, ["--class-oog-only"]);
        lastPayload = parseJsonFromStepOutput(result);
        appendEvent({
          ok: result.ok,
          event: result.ok ? "wec_class_oog_only_unit_exit" : "wec_class_oog_only_unit_fail",
          reason,
          unit,
          exit_code: result.exitCode,
          bounded_unit: lastPayload?.bounded_unit || null,
          show_no: lastPayload?.show_no || null,
          focus_day: lastPayload?.focus_day || null,
        });
        if (!result.ok) return { ok: false, result, payload: lastPayload };
        if (lastPayload?.bounded_unit === "complete") {
          appendEvent({
            ok: true,
            event: "wec_workflowv4_class_oog_complete",
            reason,
            units: unit,
            show_no: lastPayload.show_no || null,
            focus_day: lastPayload.focus_day || null,
            source_count: lastPayload.source_count ?? null,
            class_oog_count: lastPayload.class_oog_count ?? null,
          });
          return { ok: true, result, payload: lastPayload };
        }
      }
      appendEvent({
        ok: false,
        event: "wec_class_oog_only_unit_limit",
        reason,
        max_units: WEC_WORKFLOWV4_CLASS_OOG_MAX_UNITS,
      });
      return { ok: false, result: null, payload: lastPayload, blocker: "class_oog bounded unit limit reached" };
    }

    async function runWecAlertsAfterClassOog(reason, focus, options = {}) {
      const url = new URL(WEC_CLASS_LANE_RUNNER_URL);
      url.searchParams.set("action", "sync-class-alerts");
      if (focus?.show_no) url.searchParams.set("show_no", focus.show_no);
      if (options.dryRun) url.searchParams.set("dry_run", "1");
      if (options.noSend) url.searchParams.set("no_send", "1");
      if (options.candidateOnly) url.searchParams.set("candidate_only", "1");
      appendEvent({
        ok: true,
        event: "wec_alerts_attempt",
        reason,
        url: `${url.origin}${url.pathname}?${url.searchParams.toString()}`,
        show_no: focus?.show_no || null,
        focus_day: focus?.focus_day || null,
        dry_run: Boolean(options.dryRun),
        no_send: Boolean(options.noSend),
        candidate_only: Boolean(options.candidateOnly),
      });
      const response = await fetchWithTimeout(url, { method: "GET" });
      const body = await response.text();
      let payload = null;
      try {
        payload = body ? JSON.parse(body) : null;
      } catch {
        payload = { raw: body.slice(0, 1000) };
      }
      const alertResult = payload?.result || payload || {};
      appendEvent({
        ok: response.ok && payload?.ok === true,
        event: "wec_alerts_completed",
        reason,
        status: response.status,
        show_no: payload?.show_no || focus?.show_no || null,
        focus_day: payload?.focus_day || focus?.focus_day || null,
        class_start_times: alertResult.class_start_times ?? null,
        entry_go_times: alertResult.entry_go_times ?? null,
        class_alerts: alertResult.class_alerts ?? null,
        entry_alerts: alertResult.entry_alerts ?? null,
        total_candidates: alertResult.total_candidates ?? (Array.isArray(alertResult.candidates) ? alertResult.candidates.length : null),
        notifications_sent: alertResult.notifications_sent ?? null,
        records_changed: alertResult.records_changed ?? null,
        airtable_upsert_skipped: alertResult.airtable_upsert_skipped ?? null,
        stale_resolution_skipped: alertResult.stale_resolution_skipped ?? null,
        alerts_upserted: alertResult.alerts_upserted ?? null,
        alerts_resolved: alertResult.alerts_resolved ?? null,
      });
      return { ok: response.ok && payload?.ok === true, status: response.status, payload };
    }

    async function readActiveWecFocusForAlertsVerify() {
      const focusRows = await airtableListForBase(WEC_AIRTABLE_BASE_ID, "focus_show", {
        maxRecords: 10,
        filterByFormula: "{active}=1",
        "fields[]": ["show_no", "focus_day", "active"],
      });
      if (focusRows.length !== 1) {
        throw new Error(`active focus_show not unique for alerts verification: ${focusRows.length}`);
      }
      const fields = focusRows[0].fields || {};
      return {
        focus_show_id: focusRows[0].id,
        show_no: strOrNull(fields.show_no),
        focus_day: strOrNull(fields.focus_day)?.slice(0, 10) || null,
      };
    }

    async function runWecAlertsVerifyOnly() {
      appendEvent({
        ok: true,
        event: "wec_alerts_verify_start",
        mode: "alerts-only",
        trigger: process.argv.includes("--wec-alerts-verify") ? "--wec-alerts-verify" : "WEC_ALERTS_VERIFY_ONLY",
      });
      appendEvent({
        ok: true,
        event: "wec_branch_entered",
        reason: "wec_alerts_verify_only",
        upstream_stage1_run: false,
        upstream_stage2_run: false,
        class_oog_run: false,
        focus_show_metadata_write: false,
      });
      const views = await verifyWecApprovedStagingViews({ eventName: "active_entries_views_checked" });
      if (!views.ok) {
        appendEvent({
          ok: false,
          event: "wec_alerts_verify_completed",
          reason: "active_entries_views_missing",
          missing_views: views.missing_views,
        });
        return { ok: false, views };
      }
      const focus = await readActiveWecFocusForAlertsVerify();
      const alerts = await runWecAlertsAfterClassOog("wec_alerts_verify_only", focus, {
        dryRun: true,
        noSend: true,
        candidateOnly: true,
      });
      const alertResult = alerts.payload?.result || alerts.payload || {};
      appendEvent({
        ok: alerts.ok,
        event: "wec_alerts_verify_completed",
        show_no: alerts.payload?.show_no || focus.show_no || null,
        focus_day: alerts.payload?.focus_day || focus.focus_day || null,
        status: alerts.status,
        class_start_times: alertResult.class_start_times ?? null,
        entry_go_times: alertResult.entry_go_times ?? null,
        class_alerts: alertResult.class_alerts ?? null,
        entry_alerts: alertResult.entry_alerts ?? null,
        total_candidates: alertResult.total_candidates ?? (Array.isArray(alertResult.candidates) ? alertResult.candidates.length : null),
        notifications_sent: alertResult.notifications_sent ?? null,
        records_changed: alertResult.records_changed ?? null,
        airtable_upsert_skipped: alertResult.airtable_upsert_skipped ?? null,
        stale_resolution_skipped: alertResult.stale_resolution_skipped ?? null,
      });
      return { ok: alerts.ok, views, alerts };
    }

    if (WEC_ALERTS_VERIFY_ONLY) {
      const verifyResult = await runWecAlertsVerifyOnly();
      if (!verifyResult.ok) process.exitCode = 1;
      return;
    }

    async function runWecWorkflowV4CoreAfterStage1(reason, runMeta) {
      const stage2Result = runWecStage2StagingWrapperWithRun(reason, runMeta);
      appendEvent({
        ok: stage2Result.ok,
        event: "wec_workflowv4_stage2_staging_exit",
        reason,
        run_id: runMeta?.run_id || null,
        run_time: runMeta?.run_time || null,
      });
      if (!stage2Result.ok) return { ok: false, stage2Result };

      const views = await verifyWecApprovedStagingViews();
      if (!views.ok) return { ok: false, views };

      const currentFocusDay = strOrNull(runMeta?.focus_day)?.slice(0, 10) || null;
      const previousFocusState = readWecWorkflowV4FocusState();
      const previousFocusDay = strOrNull(previousFocusState.focus_day)?.slice(0, 10) || null;
      const focusDayChanged = Boolean(currentFocusDay && previousFocusDay !== currentFocusDay);
      appendEvent({
        ok: true,
        event: "wec_workflowv4_focus_day_change_checked",
        reason,
        show_no: runMeta?.show_no || null,
        focus_day: currentFocusDay,
        previous_focus_day: previousFocusDay,
        focus_day_changed: focusDayChanged,
        pause_control: "focus_show.is_pause",
        focus_show_is_pause: runMeta?.focus_show_is_pause ?? null,
      });
      if (runMeta?.focus_show_is_pause === true) {
        writeWecWorkflowV4FocusState({
          show_no: runMeta?.show_no || null,
          focus_day: currentFocusDay,
          focus_show_is_pause: true,
          last_staging_prepared_at: new Date().toISOString(),
          class_oog_allowed: false,
        });
        appendEvent({
          ok: true,
          event: "wec_workflowv4_focus_day_changed_paused_stop_after_staging",
          reason,
          show_no: runMeta?.show_no || null,
          focus_day: currentFocusDay,
          focus_day_changed: focusDayChanged,
          pause_control: "focus_show.is_pause",
          focus_show_is_pause: true,
          class_oog_run: false,
          downstream_run: false,
        });
        return { ok: true, stage2Result, views, focusDayChanged, stopped_after_staging: true };
      }

      const gate = await readWecDownstreamReleaseGate();
      appendEvent({
        ok: gate.open,
        event: gate.open ? "wec_workflowv4_class_oog_gate_open" : "wec_workflowv4_class_oog_gate_closed",
        reason: gate.reason,
        show_no: gate.show_no || null,
        focus_day: gate.focus_day || null,
        pause_control: "focus_show.is_pause",
        focus_show_is_pause: gate.focus_show_is_pause ?? null,
        focus_day_is_lock: gate.focus_day_is_lock ?? null,
        locked_staging_rows: gate.locked_staging_rows ?? null,
      });
      if (!gate.open) return { ok: false, gate };

      const classOog = runWecClassOogOnlyUntilComplete(`workflowv4_${reason}`);
      if (classOog.ok) {
        writeWecWorkflowV4FocusState({
          show_no: gate.show_no || runMeta?.show_no || null,
          focus_day: gate.focus_day || currentFocusDay,
          focus_show_is_pause: gate.focus_show_is_pause ?? null,
          last_class_oog_completed_at: new Date().toISOString(),
          class_oog_allowed: true,
        });
      }
      const alerts = classOog.ok
        ? await runWecAlertsAfterClassOog(`workflowv4_${reason}`, {
          show_no: gate.show_no || runMeta?.show_no || null,
          focus_day: gate.focus_day || currentFocusDay,
        })
        : null;
      return { ok: classOog.ok && (!alerts || alerts.ok), stage2Result, views, gate, classOog, alerts };
    }

    async function startWecStage1Stage2RunMetadata(reason) {
      const runTime = new Date().toISOString();
      const runId = `wec-stage1-stage2-${runTime.replace(/[^0-9A-Za-z]/g, "")}-${crypto.randomBytes(4).toString("hex")}`;
      const fieldMap = await tableFieldMapForBase(WEC_AIRTABLE_BASE_ID, "focus_show");
      const missingFields = ["run_id", "run_time"].filter((fieldName) => !fieldMap.has(fieldName));
      if (missingFields.length) {
        throw new Error(`focus_show missing run metadata fields: ${missingFields.join(", ")}`);
      }
      const focusRows = await airtableListForBase(WEC_AIRTABLE_BASE_ID, "focus_show", {
        maxRecords: 10,
        filterByFormula: "{active}=1",
        "fields[]": ["show_no", "focus_day", "run_id", "run_time", "is_lock", "is_pause", "active"],
      });
      if (focusRows.length !== 1) {
        throw new Error(`active focus_show not unique for run metadata: ${focusRows.length}`);
      }
      const focus = focusRows[0];
      const updated = await airtableUpdateForBase(WEC_AIRTABLE_BASE_ID, "focus_show", [{
        id: focus.id,
        fields: {
          run_id: runId,
          run_time: runTime,
        },
      }]);
      appendEvent({
        ok: true,
        event: "wec_stage1_stage2_run_metadata_written",
        reason,
        focus_show_id: focus.id,
        run_id: runId,
        run_time: runTime,
      });
      return {
        run_id: runId,
        run_time: runTime,
        focus_show_id: focus.id,
        show_no: String(focus.fields?.show_no ?? ""),
        focus_day: focus.fields?.focus_day || null,
        focus_show_is_pause: boolValue(focus.fields?.is_pause),
        focus_show_run_id_updated: updated[0]?.fields?.run_id === runId,
        focus_show_run_time_updated: Boolean(updated[0]?.fields?.run_time),
      };
    }

    function runWecHelperRepairWrapper(reason) {
      appendEvent({
        ok: true,
        event: "wec_helper_repair_attempt",
        reason,
        script: WEC_HELPER_REPAIR_WRAPPER,
      });
      return runNodeScriptAbsolute(WEC_HELPER_REPAIR_WRAPPER, {
        WEC_AIRTABLE_BASE_ID: process.env.WEC_AIRTABLE_BASE_ID || "app6XS1RvsPNRT6os",
      });
    }

    function runWecActiveEntriesWrapper(reason) {
      appendEvent({
        ok: true,
        event: "wec_active_entries_attempt",
        reason,
        script: WEC_ACTIVE_ENTRIES_WRAPPER,
      });
      const activeEntriesResult = runNodeScriptAbsolute(WEC_ACTIVE_ENTRIES_WRAPPER, {
        WEC_AIRTABLE_BASE_ID: process.env.WEC_AIRTABLE_BASE_ID || "app6XS1RvsPNRT6os",
      });
      if (!activeEntriesResult.ok) {
        appendEvent({
          ok: false,
          event: "wec_active_entries_nonblocking_fail",
          reason,
          script: WEC_ACTIVE_ENTRIES_WRAPPER,
          exit_code: activeEntriesResult.exitCode,
          duration_ms: activeEntriesResult.durationMs,
        });
      }
      return activeEntriesResult;
    }

    function runWecStackedWorkflowWrapper(reason, gate) {
      appendEvent({
        ok: true,
        event: "wec_downstream_stacked_attempt",
        reason,
        script: WEC_STACKED_WORKFLOW_WRAPPER,
        show_no: gate?.show_no || null,
        focus_day: gate?.focus_day || null,
        locked_staging_rows: gate?.locked_staging_rows ?? null,
      });
      return runNodeScriptAbsolute(WEC_STACKED_WORKFLOW_WRAPPER, {
        WEC_AIRTABLE_BASE_ID: WEC_AIRTABLE_BASE_ID,
      });
    }

    async function readWecDownstreamReleaseGate() {
      const focusRows = await airtableListForBase(WEC_AIRTABLE_BASE_ID, "focus_show", {
        maxRecords: 10,
        filterByFormula: "{active}=1",
        "fields[]": ["show_no", "focus_day", "is_lock", "is_pause", "active"],
      });
      if (focusRows.length !== 1) {
        return {
          open: false,
          reason: "active_focus_show_not_unique",
          active_focus_show_count: focusRows.length,
        };
      }

      const focus = focusRows[0];
      const focusFields = focus.fields || {};
      const showNo = strOrNull(focusFields.show_no);
      const focusDay = strOrNull(focusFields.focus_day)?.slice(0, 10) || null;
      const isPause = boolValue(focusFields.is_pause);
      const focusDayIsLock = boolValue(focusFields.is_lock);
      if (!showNo || !focusDay) {
        return {
          open: false,
          reason: "active_focus_show_missing_show_no_or_focus_day",
          focus_show_id: focus.id,
          focus_show_is_pause: isPause,
        };
      }

      const stagingRows = await airtableListForBase(WEC_AIRTABLE_BASE_ID, "update_schedule_staging", {
        filterByFormula: `AND({show_no}=${Number(showNo)},IS_SAME({iso_date},DATETIME_PARSE("${focusDay}"),"day"),OR({is_lock}=1,{lock}=1,{confirm_lock}=1))`,
        "fields[]": ["show_no", "iso_date", "is_lock", "lock", "confirm_lock", "class_no"],
      });
      const lockedStagingRows = stagingRows.filter((row) => {
        const fields = row.fields || {};
        const classNo = numOrNull(fields.class_no);
        return classNo !== null && classNo > 0;
      }).length;

      return {
        open: !isPause && focusDayIsLock && lockedStagingRows > 0,
        reason: isPause
          ? "focus_show_is_pause"
          : !focusDayIsLock
            ? "focus_day_is_lock_false"
            : lockedStagingRows <= 0
              ? "no_locked_staging_rows"
              : "release_gate_open",
        show_no: showNo,
        focus_day: focusDay,
        focus_show_id: focus.id,
        day_lock_source: "focus_show.is_lock",
        focus_show_is_pause: isPause,
        focus_day_is_lock: focusDayIsLock,
        locked_staging_rows: lockedStagingRows,
      };
    }

    async function runWecStackedWorkflowIfReleased(stage2Result, reason) {
      if (!stage2Result?.ok) {
        appendEvent({
          ok: false,
          event: "wec_downstream_release_skipped",
          reason: "stage2_wrapper_failed",
          stage2_reason: reason,
        });
        return null;
      }

      let gate;
      try {
        gate = await readWecDownstreamReleaseGate();
      } catch (error) {
        appendEvent({
          ok: false,
          event: "wec_downstream_release_gate_error",
          reason,
          error: String(error?.message || error).slice(0, 1000),
        });
        return { ok: false, gate_error: true, error };
      }

      appendEvent({
        ok: gate.open,
        event: gate.open ? "wec_downstream_release_gate_open" : "wec_downstream_release_gate_closed",
        reason: gate.reason,
        show_no: gate.show_no || null,
        focus_day: gate.focus_day || null,
        day_lock_source: gate.day_lock_source || null,
        focus_show_is_pause: gate.focus_show_is_pause ?? null,
        focus_day_is_lock: gate.focus_day_is_lock ?? null,
        locked_staging_rows: gate.locked_staging_rows ?? null,
      });

      if (!gate.open) return { ok: true, skipped: true, gate };

      const result = runWecStackedWorkflowWrapper(`after_stage2c_${reason}`, gate);
      if (!result.ok) {
        appendEvent({
          ok: false,
          event: "wec_downstream_stacked_fail",
          reason,
          script: WEC_STACKED_WORKFLOW_WRAPPER,
          exit_code: result.exitCode,
          duration_ms: result.durationMs,
          show_no: gate.show_no,
          focus_day: gate.focus_day,
        });
      }
      return result;
    }

    function runWecHelperRepairAfterStage2(stage2Result, reason) {
      if (!stage2Result?.ok) {
        appendEvent({
          ok: false,
          event: "wec_helper_repair_skipped",
          reason: "stage2_wrapper_failed",
          stage2_reason: reason,
        });
        return null;
      }
      const helperResult = runWecHelperRepairWrapper(`after_stage2c_${reason}`);
      if (!helperResult.ok) {
        appendEvent({
          ok: false,
          event: "wec_helper_repair_nonblocking_fail",
          reason,
          script: WEC_HELPER_REPAIR_WRAPPER,
          exit_code: helperResult.exitCode,
          duration_ms: helperResult.durationMs,
        });
      }
      return helperResult;
    }

    function runWecClassStartTimesRetryAfterStage1Failure(stage1Result, reason) {
      if (stage1Result?.ok) {
        return null;
      }
      appendEvent({
        ok: true,
        event: "wec_class_start_times_retry_attempt",
        reason,
        script: "docs/horseshowing/run-wec-catalyst-workflow.ps1",
      });
      const retryResult = runPowerShellScript("docs/horseshowing/run-wec-catalyst-workflow.ps1", {
        WEC_SHOW_NO: process.env.WEC_SHOW_NO || "",
        WEC_SHOW_TITLE: process.env.WEC_SHOW_TITLE || "WEC Ocala Summer Series 1 CSI2*",
        WEC_CLASS_START_TIMES_ONLY: "1",
        WEC_FORCE_SYNC: "1",
      });
      if (!retryResult.ok) {
        appendEvent({
          ok: false,
          event: "wec_class_start_times_retry_nonblocking_fail",
          reason,
          exit_code: retryResult.exitCode,
          duration_ms: retryResult.durationMs,
        });
      }
      return retryResult;
    }

    async function runWecWithoutShowScopeIfDue() {
      let ran = false;
      if (
        ENABLE_WEC_HEARTBEAT
        && slot
        && slotIsDue(slot, process.env.ORCH_WEC_HEARTBEAT_SLOTS, DEFAULT_WEC_HEARTBEAT_SLOTS)
        && intervalDue("wec-focus-workflow", WEC_FOCUS_WORKFLOW_INTERVAL_MINUTES)
      ) {
        const runMeta = await startWecStage1Stage2RunMetadata("wec-focus-workflow");
        const result = runPowerShellScript("docs/horseshowing/run-wec-catalyst-workflow.ps1", {
          WEC_SHOW_NO: process.env.WEC_SHOW_NO || "",
          WEC_SHOW_TITLE: process.env.WEC_SHOW_TITLE || "WEC Ocala Summer Series 1 CSI2*",
          WEC_RUN_ID: runMeta.run_id,
          WEC_RUN_TIME: runMeta.run_time,
          WEC_WORKFLOWV4_STAGE1_ONLY: "1",
        });
        if (!result.ok) {
          appendEvent({
            ok: false,
            event: "wec_workflowv4_stage2_staging_skipped",
            reason: "stage1_failed",
            run_id: runMeta.run_id,
            run_time: runMeta.run_time,
            helper_repair_run: false,
            class_start_times_run: false,
            entry_go_times_run: false,
            downstream_run: false,
            active_entries_run: false,
          });
          ran = true;
        } else {
        const stageReason = result.ok ? "after_stage1_pass" : "after_stage1_fail";
        const workflowV4Result = await runWecWorkflowV4CoreAfterStage1(stageReason, runMeta);
        appendEvent({
          ok: workflowV4Result.ok,
          event: "wec_workflowv4_core_stop_after_class_oog",
          reason: stageReason,
          run_id: runMeta.run_id,
          run_time: runMeta.run_time,
          helper_repair_run: false,
          class_start_times_run: false,
          entry_go_times_run: false,
          downstream_run: false,
          active_entries_run: false,
        });
        if (workflowV4Result.ok) markIntervalRun("wec-focus-workflow");
        ran = true;
        }
      }

      if (ENABLE_WEC_HEARTBEAT && intervalDue("wec-airtable-controls", WEC_AIRTABLE_CONTROLS_INTERVAL_MINUTES)) {
        const result = runNodeScript("docs/horseshowing/sync-airtable-controls.js", {
          WEC_AIRTABLE_BASE_ID: process.env.WEC_AIRTABLE_BASE_ID || "app6XS1RvsPNRT6os",
        });
        if (result.ok) markIntervalRun("wec-airtable-controls");
        ran = true;
      }
      return ran;
    }

    const defaultShowDateGuard = computeDefaultShowDateGuard({
      rawSqlDate: heartbeat?.fields?.sql_date,
      appSqlDate: heartbeat?.fields?.app_sql_date,
      defaultAppSqlDateIs: heartbeat?.fields?.default_app_sql_date_is,
      showAppSqlStartDate: heartbeat?.fields?.show_app_sql_start_date,
      showAppSqlEndDate: heartbeat?.fields?.show_app_sql_end_date,
      setToDefaultAppSqlDate: heartbeat?.fields?.set_to_default_app_sql_date,
    });

    if (numOrNull(heartbeat?.fields?.app_show_id) === null) {
      const wecRan = await runWecWithoutShowScopeIfDue();
      appendEvent({
        ok: true,
        event: "orchestrator_noop",
        reason: "no_active_show_scope",
        heartbeat_id: heartbeat?.id || null,
        mode,
        cadence_seconds: cadenceSeconds,
        wec_ran: wecRan,
      });
      return;
    }

    async function runDueScript(scriptName, extraEnv = {}) {
      const result = runNodeScript(scriptName, extraEnv);
      const overrunThresholdMs = Math.min(ORCH_STEP_OVERRUN_ALERT_MS, Math.max(60000, cadenceSeconds * 1000));
      if (result.durationMs >= overrunThresholdMs) {
        await recordOrchestratorAlert({
          errorType: "heartbeat_lane_step_overrun",
          heartbeat,
          scriptName,
          resolved: false,
          message: `Heartbeat lane step overran threshold. script=${scriptName} duration_ms=${result.durationMs} threshold_ms=${overrunThresholdMs} slot=${slot} mode=${mode} cadence_seconds=${cadenceSeconds}`,
          extra: { scriptName, durationMs: result.durationMs, thresholdMs: overrunThresholdMs, slot, mode, cadenceSeconds },
        });
      }
      return result;
    }

    async function runDuePowerShell(scriptName, extraEnv = {}) {
      const result = runPowerShellScript(scriptName, extraEnv);
      const overrunThresholdMs = Math.min(ORCH_STEP_OVERRUN_ALERT_MS, Math.max(60000, cadenceSeconds * 1000));
      if (result.durationMs >= overrunThresholdMs) {
        await recordOrchestratorAlert({
          errorType: "heartbeat_lane_step_overrun",
          heartbeat,
          scriptName,
          resolved: false,
          message: `Heartbeat lane step overran threshold. script=${scriptName} duration_ms=${result.durationMs} threshold_ms=${overrunThresholdMs} slot=${slot} mode=${mode} cadence_seconds=${cadenceSeconds}`,
          extra: { scriptName, durationMs: result.durationMs, thresholdMs: overrunThresholdMs, slot, mode, cadenceSeconds },
        });
      }
      return result;
    }

    if (DEFAULT_SHOW_DATE_GUARD_BLOCK_HEAVY && defaultShowDateGuard.check_show_date) {
      const showOverride = await showManualOverride(heartbeat?.fields?.app_show_id);
      if (!showOverride.is_default_show_manual_override) {
        appendEvent({
          ok: true,
          event: "orchestrator_mode_noop",
          reason: "default_show_date_needs_manual_confirmation",
          mode,
          cadence_seconds: cadenceSeconds,
          default_show_date_status: defaultShowDateGuard.default_show_date_status,
          default_show_date_reason: defaultShowDateGuard.default_show_date_reason,
          default_show_date_metrics: defaultShowDateGuard.default_show_date_metrics,
          show_override: showOverride,
        });
        return;
      }
    }

    if (!slot) {
      appendEvent({ ok: true, event: "orchestrator_noop", reason: "no_single_active_slot" });
      return;
    }

    const shiftedToNextDay = boolValue(heartbeat?.fields?.[HEARTBEAT_SHIFTED_NEXT_DAY_FIELD]);

    const schedulesDailyDefaultSlots = mode === "NIGHT"
      ? DEFAULT_SCHEDULES_DAILY_NIGHT_SLOTS
      : DEFAULT_SCHEDULES_DAILY_SLOTS;
    const schedulesDailySlots = mode === "NIGHT"
      ? (process.env.ORCH_SCHEDULES_DAILY_NIGHT_SLOTS || process.env.ORCH_SCHEDULES_DAILY_SLOTS)
      : process.env.ORCH_SCHEDULES_DAILY_SLOTS;
    const schedulesDailyDue = slotIsDue(slot, schedulesDailySlots, schedulesDailyDefaultSlots);
    const schedulesCalcDue = mode === "DAY"
      && slotIsDue(slot, process.env.ORCH_SCHEDULES_CALCULATOR_SLOTS, DEFAULT_SCHEDULES_CALCULATOR_SLOTS);
    const tripsDailyDefaultSlots = DEFAULT_TRIPS_DAILY_SLOTS;
    const tripsDailySlots = process.env.ORCH_TRIPS_DAILY_SLOTS;
    const tripsDailyDue = slotIsDue(slot, tripsDailySlots, tripsDailyDefaultSlots);
    const tripsTaggerDue = slotIsDue(slot, process.env.ORCH_TRIPS_TAGGER_SLOTS, DEFAULT_TRIPS_TAGGER_SLOTS);
    const tripsCalcDue = slotIsDue(slot, process.env.ORCH_TRIPS_CALCULATOR_SLOTS, DEFAULT_TRIPS_CALCULATOR_SLOTS);
    const liveGroupsDue = mode === "DAY"
      && slotIsDue(slot, process.env.ORCH_LIVE_GROUPS_SLOTS, DEFAULT_LIVE_GROUPS_SLOTS);
    const liveRingsDue = mode === "DAY"
      && slotIsDue(slot, process.env.ORCH_LIVE_RINGS_SLOTS, DEFAULT_LIVE_RINGS_SLOTS);
    const liveClassDetailDue = mode === "DAY"
      && !DISABLE_LIVE_CLASS_DETAIL
      && slotIsDue(slot, process.env.ORCH_LIVE_CLASS_DETAIL_SLOTS, DEFAULT_LIVE_CLASS_DETAIL_SLOTS);
    const wecHeartbeatDue = ENABLE_WEC_HEARTBEAT
      && slotIsDue(slot, process.env.ORCH_WEC_HEARTBEAT_SLOTS, DEFAULT_WEC_HEARTBEAT_SLOTS)
      && intervalDue("wec-focus-workflow", WEC_FOCUS_WORKFLOW_INTERVAL_MINUTES);
    const wecAirtableControlsDue = ENABLE_WEC_HEARTBEAT
      && intervalDue("wec-airtable-controls", WEC_AIRTABLE_CONTROLS_INTERVAL_MINUTES);
    const publisherDue = slotIsDue(slot, process.env.ORCH_PUBLISHER_SLOTS, DEFAULT_PUBLISHER_SLOTS);

    let upstreamOk = true;
    let scheduleDueFailed = false;

    if (schedulesDailyDue) {
      const schedulesDailyResult = await runDueScript("schedules_dailyv2.js");
      if (!schedulesDailyResult.ok) {
        upstreamOk = false;
        scheduleDueFailed = true;
        appendEvent({ ok: false, event: "schedule_downstream_blocked", reason: "schedules_dailyv2_failed" });
      }
    }

    if (!scheduleDueFailed && schedulesCalcDue) {
      const schedulesCalcResult = await runDueScript("schedules_calculatorv2.js");
      if (!schedulesCalcResult.ok) upstreamOk = false;
    }

    let tripsOk = true;
    let tripsRan = false;
    if (scheduleDueFailed && (tripsDailyDue || tripsTaggerDue || tripsCalcDue)) {
      tripsOk = false;
      appendEvent({ ok: false, event: "trips_downstream_blocked", reason: "schedules_dailyv2_failed" });
    }

    if (tripsOk && tripsDailyDue) {
      tripsRan = true;
      const tripsDailyResult = await runDueScript("trips_dailyv2.js");
      if (!tripsDailyResult.ok) {
        tripsOk = false;
        upstreamOk = false;
        appendEvent({ ok: false, event: "trips_downstream_blocked", reason: "trips_dailyv2_failed" });
      }
    }

    if (tripsOk && tripsTaggerDue) {
      tripsRan = true;
      const tripsTaggerResult = await runDueScript("trips_tagger.js");
      if (!tripsTaggerResult.ok) {
        tripsOk = false;
        upstreamOk = false;
        appendEvent({ ok: false, event: "trips_downstream_blocked", reason: "trips_tagger_failed" });
      }
    }

    if (tripsOk && tripsCalcDue && tripsRan) {
      const tripsCalcResult = await runDueScript("trips_calculatorv2.js");
      if (!tripsCalcResult.ok) upstreamOk = false;
    }

    if (liveGroupsDue) {
      const liveGroupsResult = await runDueScript("live_groups_daily.js");
      if (!liveGroupsResult.ok) upstreamOk = false;
    }

    if (liveRingsDue) {
      const liveRingsResult = await runDueScript("live_rings_daily.js");
      if (!liveRingsResult.ok) upstreamOk = false;
    }

    if (liveClassDetailDue) {
      const liveClassDetailResult = await runDueScript("live_class_detail.js", {
        ORCH_CURRENT_MODE: mode,
        ORCH_CURRENT_SLOT: slot,
      });
      if (!liveClassDetailResult.ok) upstreamOk = false;
    }

    if (wecHeartbeatDue) {
      const wecRunMeta = await startWecStage1Stage2RunMetadata("wec-focus-workflow");
      const wecHeartbeatResult = await runDuePowerShell("docs/horseshowing/run-wec-catalyst-workflow.ps1", {
        WEC_SHOW_NO: process.env.WEC_SHOW_NO || "",
        WEC_SHOW_TITLE: process.env.WEC_SHOW_TITLE || "WEC Ocala Summer Series 1 CSI2*",
        WEC_RUN_ID: wecRunMeta.run_id,
        WEC_RUN_TIME: wecRunMeta.run_time,
        WEC_WORKFLOWV4_STAGE1_ONLY: "1",
      });
      if (!wecHeartbeatResult.ok) {
        appendEvent({
          ok: false,
          event: "wec_workflowv4_stage2_staging_skipped",
          reason: "stage1_failed",
          run_id: wecRunMeta.run_id,
          run_time: wecRunMeta.run_time,
          helper_repair_run: false,
          class_start_times_run: false,
          entry_go_times_run: false,
          downstream_run: false,
          active_entries_run: false,
        });
        upstreamOk = false;
      } else {
      const wecStageReason = wecHeartbeatResult.ok ? "after_stage1_pass" : "after_stage1_fail";
      const workflowV4Result = await runWecWorkflowV4CoreAfterStage1(wecStageReason, wecRunMeta);
      appendEvent({
        ok: workflowV4Result.ok,
        event: "wec_workflowv4_core_stop_after_class_oog",
        reason: wecStageReason,
        run_id: wecRunMeta.run_id,
        run_time: wecRunMeta.run_time,
        helper_repair_run: false,
        class_start_times_run: false,
        entry_go_times_run: false,
        downstream_run: false,
        active_entries_run: false,
      });
      if (workflowV4Result.ok) {
        markIntervalRun("wec-focus-workflow");
      } else {
        upstreamOk = false;
      }
      }
    }

    if (wecAirtableControlsDue) {
      const wecControlsResult = await runDueScript("docs/horseshowing/sync-airtable-controls.js", {
        WEC_AIRTABLE_BASE_ID: process.env.WEC_AIRTABLE_BASE_ID || "app6XS1RvsPNRT6os",
      });
      if (wecControlsResult.ok) {
        markIntervalRun("wec-airtable-controls");
      } else {
        upstreamOk = false;
      }
    }

    if (publisherDue && upstreamOk) {
      await runDueScript("publisher.js");
    } else if (publisherDue) {
      appendEvent({ ok: false, event: "publisher_blocked", reason: "upstream_due_lane_failed" });
    }

    appendEvent({
      ok: true,
      event: "orchestrator_completed",
      slot,
      mode,
      cadence_seconds: cadenceSeconds,
      shifted_to_next_day: shiftedToNextDay,
      due: {
        schedules_daily: schedulesDailyDue,
        schedules_calculator: schedulesCalcDue,
        trips_daily: tripsDailyDue,
        trips_tagger: tripsTaggerDue,
        trips_calculator: tripsCalcDue,
        live_groups: liveGroupsDue,
        live_class_detail: liveClassDetailDue,
        wec_heartbeat: wecHeartbeatDue,
        wec_airtable_controls: wecAirtableControlsDue,
        publisher: publisherDue,
      },
    });
  } finally {
    releaseLock();
  }
}

function launchDetachedChild() {
  ensureLogDir();
  const child = spawn(process.execPath, [__filename], {
    cwd: __dirname,
    env: {
      ...process.env,
      ORCH_DETACHED_CHILD: "1",
      ORCH_RUN_INLINE: "0",
    },
    detached: true,
    stdio: "ignore",
    windowsHide: true,
  });
  child.unref();
  appendEvent({ ok: true, event: "orchestrator_child_launched", pid: child.pid });
}

if (!DETACHED_CHILD && !RUN_INLINE) {
  launchDetachedChild();
} else {
  runOrchestrator().catch((error) => {
    appendEvent({
      ok: false,
      event: "orchestrator_failed",
      error: String(error?.stack || error?.message || error).slice(0, 1000),
    });
    process.exit(1);
  });
}
