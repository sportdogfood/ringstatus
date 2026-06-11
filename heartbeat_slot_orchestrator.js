const fs = require("fs");
const path = require("path");
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
const LOCK_STALE_MINUTES = Math.max(1, Number(process.env.ORCH_LOCK_STALE_MINUTES || "30") || 30);
const DISABLE_HEAVY = String(process.env.ORCH_DISABLE_HEAVY || "0") === "1";
const DISABLE_LIVE_CLASS_DETAIL = String(process.env.ORCH_DISABLE_LIVE_CLASS_DETAIL || "0") === "1";
const ENABLE_WEC_HEARTBEAT = String(process.env.ORCH_WEC_ENABLED || "1") === "1";
const WEC_FOCUS_WORKFLOW_INTERVAL_MINUTES = Math.max(1, Number(process.env.ORCH_WEC_FOCUS_WORKFLOW_INTERVAL_MINUTES || "12") || 12);
const WEC_AIRTABLE_CONTROLS_INTERVAL_MINUTES = Math.max(1, Number(process.env.ORCH_WEC_AIRTABLE_CONTROLS_INTERVAL_MINUTES || "30") || 30);
const DEFAULT_SHOW_DATE_GUARD_BLOCK_HEAVY = String(process.env.DEFAULT_SHOW_DATE_GUARD_BLOCK_HEAVY || "1") === "1";
const RUN_INLINE = String(process.env.ORCH_RUN_INLINE || "0") === "1";
const DETACHED_CHILD = String(process.env.ORCH_DETACHED_CHILD || "0") === "1";
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
  const url = new URL(`https://api.airtable.com/v0/${AIRTABLE_BASE_ID}/${encodeURIComponent(tableName)}`);
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
  const out = [];
  let offset = null;

  do {
    const url = airtableUrl(tableName, { ...params, offset });
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

async function tableFieldMap(tableName) {
  const url = new URL(`https://api.airtable.com/v0/meta/bases/${AIRTABLE_BASE_ID}/tables`);
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

  return { ok, exitCode, durationMs };
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

async function runOrchestrator() {
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

    const defaultShowDateGuard = computeDefaultShowDateGuard({
      rawSqlDate: heartbeat?.fields?.sql_date,
      appSqlDate: heartbeat?.fields?.app_sql_date,
      defaultAppSqlDateIs: heartbeat?.fields?.default_app_sql_date_is,
      showAppSqlStartDate: heartbeat?.fields?.show_app_sql_start_date,
      showAppSqlEndDate: heartbeat?.fields?.show_app_sql_end_date,
      setToDefaultAppSqlDate: heartbeat?.fields?.set_to_default_app_sql_date,
    });

    if (numOrNull(heartbeat?.fields?.app_show_id) === null) {
      appendEvent({
        ok: true,
        event: "orchestrator_noop",
        reason: "no_active_show_scope",
        heartbeat_id: heartbeat?.id || null,
        mode,
        cadence_seconds: cadenceSeconds,
      });
      if (ORCH_ALERT_NO_ACTIVE_FEEDS) {
        await recordOrchestratorAlert({
          errorType: "heartbeat_no_active_feeds",
          heartbeat,
          resolved: false,
          message: `Heartbeat has no active show scope; downstream lanes skipped. heartbeat_id=${heartbeat?.id || ""} mode=${mode} cadence_seconds=${cadenceSeconds}`,
        });
      }
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
      const wecHeartbeatResult = await runDuePowerShell("docs/horseshowing/run-wec-catalyst-workflow.ps1", {
        WEC_SHOW_NO: process.env.WEC_SHOW_NO || "14906",
        WEC_SHOW_TITLE: process.env.WEC_SHOW_TITLE || "WEC Ocala Summer Series 1 CSI2*",
      });
      if (wecHeartbeatResult.ok) {
        markIntervalRun("wec-focus-workflow");
      } else {
        upstreamOk = false;
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
