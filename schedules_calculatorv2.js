const AIRTABLE_TOKEN = process.env.AIRTABLE_TOKEN || "";
const AIRTABLE_BASE_ID = process.env.AIRTABLE_BASE_ID || "";
const fs = require("fs");
const path = require("path");

const TABLE_HEARTBEAT = process.env.TABLE_HEARTBEAT || "heartbeat";
const TABLE_WATCH_SCHEDULE = process.env.TABLE_WATCH_SCHEDULE || "watch_schedule";
const TABLE_GROUPS_LIVE = process.env.TABLE_GROUPS_LIVE || "groups_live";
const TABLE_SCHEDULE_LOGS = process.env.TABLE_SCHEDULE_LOGS || "schedule_logs";
const TABLE_TRIGGER_TAGS = process.env.TABLE_TRIGGER_TAGS || "trigger_tags";

const VIEW_WATCH_SCHEDULE = process.env.VIEW_WATCH_SCHEDULE || "enrich";
const HEARTBEAT_SORT_FIELD = process.env.HEARTBEAT_SORT_FIELD || "hb_at";
const MAX_RECORDS = Number(process.env.MAX_RECORDS || "500");

const HTTP_TIMEOUT_MS = Number(process.env.HTTP_TIMEOUT_MS || "20000");
const AT_RETRY_ATTEMPTS = Number(process.env.AT_RETRY_ATTEMPTS || "3");
const AT_RETRY_BASE_MS = Number(process.env.AT_RETRY_BASE_MS || "400");
const AT_RETRY_MAX_MS = Number(process.env.AT_RETRY_MAX_MS || "2000");

const DRY_RUN = String(process.env.DRY_RUN || "0") === "1";
const CALC_MODE = String(process.env.CALC_MODE || "shadow").trim().toLowerCase() === "promote"
  ? "promote"
  : "shadow";
const CALC_VERSION = String(process.env.CALC_VERSION || "schedules_calculator_v2_1").trim();
const DEFAULT_TRIP_MINUTES = Number(process.env.DEFAULT_TRIP_MINUTES || "3");
const TRACE_ENABLED = String(process.env.SCHEDULES_CALCULATOR_TRACE || "0") === "1";
const TRACE_LOG_PATH = path.join(
  process.env.RUNNER_LOG_DIR || "C:\\actions-runner\\ringstatus",
  "schedules-calculatorv2-progress.log"
);

function traceStage(stage, extra = {}) {
  if (!TRACE_ENABLED) return;
  try {
    fs.mkdirSync(path.dirname(TRACE_LOG_PATH), { recursive: true });
    fs.appendFileSync(TRACE_LOG_PATH, `${JSON.stringify({
      ts: new Date().toISOString(),
      stage,
      ...extra,
    })}\r\n`, "utf8");
  } catch {
    // Tracing must never change calculator behavior.
  }
}

function requireEnv(name, value) {
  if (!value) throw new Error(`Missing required env: ${name}`);
}

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

function isBlank(value) {
  return value === null ||
    value === undefined ||
    (typeof value === "string" && value.trim() === "") ||
    String(value).trim().toLowerCase() === "null" ||
    String(value).trim().toLowerCase() === "nan";
}

function strOrNull(value) {
  if (isBlank(value)) return null;
  return String(value).trim();
}

function numOrNull(value) {
  if (isBlank(value)) return null;
  const num = Number(value);
  return Number.isFinite(num) ? num : null;
}

function pickFirst(...values) {
  for (const value of values) {
    if (!isBlank(value)) return value;
  }
  return undefined;
}

function splitNumericStrings(value) {
  const raw = Array.isArray(value) ? value : String(value || "").split(",");
  return raw
    .map((item) => String(item).trim())
    .filter(Boolean)
    .filter((item) => numOrNull(item) !== null);
}

function resolveGroupsLiveClassId(groupRow, classNumber) {
  const wantedClassNumber = numOrNull(classNumber);
  if (wantedClassNumber === null || !groupRow) return null;

  const classIds = groupRow.class_ids || splitNumericStrings(groupRow.classes);
  const classNumbers = groupRow.class_numbers || splitNumericStrings(
    pickFirst(groupRow.classNumbers, groupRow.class_numbers_list)
  );

  for (let index = 0; index < classNumbers.length; index += 1) {
    if (numOrNull(classNumbers[index]) === wantedClassNumber) {
      return numOrNull(classIds[index]);
    }
  }

  return null;
}

function firstValue(value) {
  if (Array.isArray(value)) {
    for (const item of value) {
      if (!isBlank(item)) return item;
    }
    return null;
  }
  return value;
}

function firstLinkId(value) {
  const raw = firstValue(value);
  if (isBlank(raw)) return null;
  return String(raw).trim();
}

function boolValue(value) {
  const raw = firstValue(value);
  if (raw === true || raw === 1) return true;
  if (raw === false || raw === 0 || raw === null || raw === undefined) return false;
  const text = String(raw).trim().toLowerCase();
  return text === "true" || text === "1" || text === "yes" || text === "checked";
}

function normalizeKey(value) {
  if (isBlank(value)) return "";
  return String(value).trim();
}

function chunk(items, size) {
  const out = [];
  for (let i = 0; i < items.length; i += size) out.push(items.slice(i, i + size));
  return out;
}

function sameValue(left, right) {
  if (Array.isArray(left) || Array.isArray(right)) {
    return JSON.stringify(left ?? null) === JSON.stringify(right ?? null);
  }
  return left === right;
}

function roundNumber(value, places = 6) {
  if (!Number.isFinite(value)) return null;
  const factor = 10 ** places;
  return Math.round(value * factor) / factor;
}

function isUnderwayStatus(value) {
  return strOrNull(value)?.toLowerCase() === "underway";
}

function isTerminalClassStatus(value) {
  const status = strOrNull(value)?.toLowerCase();
  return status === "completed" || status === "cancelled" || status === "canceled";
}

function compareTriggerValue(left, right) {
  const leftNum = numOrNull(left);
  const rightNum = numOrNull(right);
  if (leftNum !== null && rightNum !== null) return leftNum === rightNum;
  return normalizeKey(left).toLowerCase() === normalizeKey(right).toLowerCase();
}

function parseTriggerNumber(value) {
  const num = numOrNull(value);
  return num !== null ? num : null;
}

function setIfPresent(target, fieldName, value) {
  if (!fieldName) return;
  if (value === undefined || value === null) return;
  if (typeof value === "string" && value.trim() === "") return;
  target[fieldName] = value;
}

function formatDiffDisplayValue(value) {
  if (value === undefined || value === null) return "blank";
  if (typeof value === "string") {
    const text = value.trim();
    return text === "" ? "blank" : text;
  }
  if (typeof value === "number") {
    return Number.isFinite(value) ? String(roundNumber(value, 6)) : "blank";
  }
  if (typeof value === "boolean") return value ? "true" : "false";
  return String(value);
}

function buildDiffText(priorValue, currentValue) {
  if (sameValue(priorValue ?? null, currentValue ?? null)) return null;
  return `${formatDiffDisplayValue(priorValue)} -> ${formatDiffDisplayValue(currentValue)}`;
}

function fieldsHasTruthy(fields, fieldName) {
  if (!fieldName) return false;
  return boolValue(fields?.[fieldName]);
}

function toIsoDateOnly(value) {
  if (isBlank(value)) return null;
  if (value instanceof Date) return value.toISOString().slice(0, 10);
  const text = String(value).trim();
  if (/^\d{4}-\d{2}-\d{2}$/.test(text)) return text;
  if (/^\d{4}-\d{2}-\d{2}T/.test(text)) return text.slice(0, 10);
  const us = text.match(/^(\d{1,2})\/(\d{1,2})\/(\d{2}|\d{4})$/);
  if (us) {
    const mm = us[1].padStart(2, "0");
    const dd = us[2].padStart(2, "0");
    const yy = us[3].length === 2 ? `20${us[3]}` : us[3];
    return `${yy}-${mm}-${dd}`;
  }
  const parsed = Date.parse(text);
  return Number.isFinite(parsed) ? new Date(parsed).toISOString().slice(0, 10) : null;
}

function parseTimeTextToMinutes(value) {
  const text = strOrNull(value);
  if (!text) return null;

  let match = text.match(/^(\d{1,2}):(\d{2})(?::(\d{2}))?\s*([AP]M)$/i);
  if (match) {
    let hour = Number(match[1]);
    const minute = Number(match[2]);
    const second = Number(match[3] || "0");
    const meridiem = match[4].toUpperCase();
    if (hour === 12) hour = 0;
    if (meridiem === "PM") hour += 12;
    return hour * 60 + minute + second / 60;
  }

  match = text.match(/^(\d{1,2}):(\d{2})(?::(\d{2}))?$/);
  if (match) {
    const hour = Number(match[1]);
    const minute = Number(match[2]);
    const second = Number(match[3] || "0");
    if (hour < 0 || hour > 23 || minute < 0 || minute > 59 || second < 0 || second > 59) return null;
    return hour * 60 + minute + second / 60;
  }

  return null;
}

function formatMinutesToTimeText(totalMinutes) {
  if (!Number.isFinite(totalMinutes)) return null;
  let minutes = totalMinutes % (24 * 60);
  if (minutes < 0) minutes += 24 * 60;
  const wholeMinutes = Math.floor(minutes);
  const seconds = Math.round((minutes - wholeMinutes) * 60);
  const hours = Math.floor(wholeMinutes / 60);
  const mins = wholeMinutes % 60;
  const hh = String(hours).padStart(2, "0");
  const mm = String(mins).padStart(2, "0");
  const ss = String(seconds === 60 ? 0 : seconds).padStart(2, "0");
  return `${hh}:${mm}:${ss}`;
}

function compareRecordRecency(left, right) {
  const leftEpoch = Date.parse(pickFirst(left.curr_updated_at, left.ingested_at, left.created_time) || "") || 0;
  const rightEpoch = Date.parse(pickFirst(right.curr_updated_at, right.ingested_at, right.created_time) || "") || 0;
  return rightEpoch - leftEpoch;
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

function isRetryableFetchError(error) {
  const name = String(error?.name || "");
  const code = String(error?.code || "");
  const message = String(error?.message || "");
  if (name === "AbortError") return true;
  if (code === "UND_ERR_CONNECT_TIMEOUT") return true;
  if (code === "UND_ERR_HEADERS_TIMEOUT") return true;
  if (code === "UND_ERR_BODY_TIMEOUT") return true;
  if (/timeout/i.test(message)) return true;
  if (/fetch failed/i.test(message)) return true;
  return false;
}

async function fetchWithRetry(url, options = {}, retry = {}) {
  const attempts = Math.max(1, Math.floor(Number(retry.attempts ?? AT_RETRY_ATTEMPTS)));
  const baseMs = Math.max(0, Math.floor(Number(retry.baseMs ?? AT_RETRY_BASE_MS)));
  const maxMs = Math.max(250, Math.floor(Number(retry.maxMs ?? AT_RETRY_MAX_MS)));

  let lastError = null;

  for (let attempt = 1; attempt <= attempts; attempt += 1) {
    try {
      const response = await fetchWithTimeout(url, options);
      if (response.status === 429 || (response.status >= 500 && response.status <= 599)) {
        if (attempt === attempts) return response;
        const waitMs = Math.min(maxMs, baseMs * attempt + Math.floor(Math.random() * 200));
        await sleep(waitMs);
        continue;
      }
      return response;
    } catch (error) {
      lastError = error;
      if (!isRetryableFetchError(error) || attempt === attempts) throw error;
      const waitMs = Math.min(maxMs, baseMs * attempt + Math.floor(Math.random() * 250));
      await sleep(waitMs);
    }
  }

  throw lastError || new Error("fetchWithRetry failed");
}

function airtableUrl(tableName) {
  return `https://api.airtable.com/v0/${AIRTABLE_BASE_ID}/${encodeURIComponent(tableName)}`;
}

function extractUnknownFieldNameFromAirtableError(error) {
  const message = String(error?.message || error || "");
  const match = message.match(/Unknown field name:\s*\\?"([^"]+)\\?"/i);
  return match ? String(match[1] || "").trim() : null;
}

async function airtableList(tableName, queryParams = {}) {
  const records = [];
  let offset = null;

  while (true) {
    const url = new URL(airtableUrl(tableName));
    for (const [key, value] of Object.entries(queryParams)) {
      if (value === undefined || value === null || value === "") continue;
      if (Array.isArray(value)) {
        for (const item of value) {
          if (item === undefined || item === null || item === "") continue;
          url.searchParams.append(key, String(item));
        }
        continue;
      }
      url.searchParams.set(key, String(value));
    }
    if (offset) url.searchParams.set("offset", offset);

    const response = await fetchWithRetry(url.toString(), {
      headers: { Authorization: `Bearer ${AIRTABLE_TOKEN}` },
    });

    if (!response.ok) {
      const body = await response.text().catch(() => "");
      throw new Error(`Airtable list failed (${response.status}) ${tableName}: ${body}`);
    }

    const json = await response.json().catch(() => ({}));
    records.push(...(json.records || []));
    offset = json.offset;
    if (!offset) break;
  }

  return records;
}

async function airtableCreateRecords(tableName, records) {
  if (!records.length) return { okRows: 0, failedRows: [], createdRecords: [] };

  let okRows = 0;
  const failedRows = [];
  const createdRecords = [];

  for (const batch of chunk(records, 10)) {
    try {
      const response = await fetchWithRetry(airtableUrl(tableName), {
        method: "POST",
        headers: {
          Authorization: `Bearer ${AIRTABLE_TOKEN}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ records: batch }),
      });

      if (!response.ok) {
        const body = await response.text().catch(() => "");
        throw new Error(`Airtable create failed (${response.status}) ${tableName}: ${body}`);
      }

      const json = await response.json().catch(() => ({}));
      const rows = Array.isArray(json?.records) ? json.records : [];
      createdRecords.push(...rows);
      okRows += rows.length || batch.length;
    } catch (error) {
      for (const row of batch) {
        failedRows.push({
          key: row?.fields?.calc_log_key ?? row?.fields?.class_groupxclasses_id ?? null,
          reason: String(error?.message || error).slice(0, 300),
        });
      }
    }
  }

  return { okRows, failedRows, createdRecords };
}

async function airtablePatchRecords(tableName, updates) {
  if (!updates.length) return { okRows: 0, failedRows: [] };

  let okRows = 0;
  const failedRows = [];

  for (const batch of chunk(updates, 10)) {
    try {
      const response = await fetchWithRetry(airtableUrl(tableName), {
        method: "PATCH",
        headers: {
          Authorization: `Bearer ${AIRTABLE_TOKEN}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ records: batch }),
      });

      if (!response.ok) {
        const body = await response.text().catch(() => "");
        throw new Error(`Airtable patch failed (${response.status}) ${tableName}: ${body}`);
      }

      okRows += batch.length;
    } catch (error) {
      for (const row of batch) {
        failedRows.push({
          record_id: row.id,
          key: row?.fields?.class_groupxclasses_id ?? null,
          reason: String(error?.message || error).slice(0, 300),
        });
      }
    }
  }

  return { okRows, failedRows };
}

async function fetchTableFieldSet(tableName) {
  const response = await fetchWithRetry(`https://api.airtable.com/v0/meta/bases/${AIRTABLE_BASE_ID}/tables`, {
    headers: { Authorization: `Bearer ${AIRTABLE_TOKEN}` },
  });

  if (!response.ok) {
    const body = await response.text().catch(() => "");
    throw new Error(`Airtable meta failed (${response.status}) ${tableName}: ${body}`);
  }

  const json = await response.json().catch(() => ({}));
  const table = Array.isArray(json?.tables)
    ? json.tables.find((item) => String(item?.name || "").trim() === tableName)
    : null;
  return new Set(Array.isArray(table?.fields) ? table.fields.map((field) => String(field?.name || "").trim()).filter(Boolean) : []);
}

async function fetchActiveTriggerTags(sourceTableName) {
  const rows = await airtableList(TABLE_TRIGGER_TAGS, {
    maxRecords: MAX_RECORDS,
    "fields[]": [
      "trigger_name",
      "query",
      "query_select",
      "this_field",
      "argument",
      "from",
      "to",
      "is_active",
      "output_field",
      "priority",
      "source_table",
      "source_view",
      "trigger_lane",
      "trigger_type",
      "expected_when",
    ],
  });

  return rows
    .map((row) => {
      const fields = row?.fields || {};
      return {
        recordId: row.id,
        trigger_name: strOrNull(fields.trigger_name),
        query: strOrNull(pickFirst(fields.query_select, fields.query)),
        this_field: strOrNull(fields.this_field),
        argument: strOrNull(fields.argument),
        from: strOrNull(fields.from),
        to: strOrNull(fields.to),
        is_active: boolValue(fields.is_active),
        output_field: strOrNull(fields.output_field),
        priority: numOrNull(fields.priority) ?? 9999,
        source_table: strOrNull(fields.source_table),
        source_view: strOrNull(fields.source_view),
        trigger_lane: strOrNull(fields.trigger_lane),
        trigger_type: strOrNull(fields.trigger_type),
        expected_when: strOrNull(fields.expected_when),
      };
    })
    .filter((row) => row.is_active)
    .filter((row) => !sourceTableName || compareTriggerValue(row.source_table, sourceTableName))
    .filter((row) => row.output_field)
    .sort((a, b) => a.priority - b.priority || String(a.trigger_name || "").localeCompare(String(b.trigger_name || "")));
}

async function fetchPriorScheduleLogMap(triggerTags) {
  const requestedFields = new Set(["watch_schedule", "created_at"]);
  requestedFields.add("rs_start_time");
  for (const trigger of triggerTags || []) {
    const fieldName = strOrNull(trigger?.this_field);
    if (fieldName) requestedFields.add(fieldName);
    const outputField = strOrNull(trigger?.output_field);
    if (outputField) requestedFields.add(outputField);
  }

  const rows = await airtableList(TABLE_SCHEDULE_LOGS, {
    maxRecords: Math.max(MAX_RECORDS * 10, 1000),
    "sort[0][field]": "created_at",
    "sort[0][direction]": "desc",
    "fields[]": Array.from(requestedFields),
  });

  const byWatchScheduleId = new Map();
  for (const row of rows) {
    const watchScheduleId = firstLinkId(row?.fields?.watch_schedule);
    if (!watchScheduleId || byWatchScheduleId.has(watchScheduleId)) continue;
    byWatchScheduleId.set(watchScheduleId, row.fields || {});
  }

  return byWatchScheduleId;
}

async function fetchLatestHeartbeat() {
  const rows = await airtableList(TABLE_HEARTBEAT, {
    maxRecords: 1,
    pageSize: 1,
    "sort[0][field]": HEARTBEAT_SORT_FIELD,
    "sort[0][direction]": "desc",
    "fields[]": [
      "record_id",
      "heartbeat_id",
      "hb_at",
      "show_id",
      "app_show_id",
      "app_sql_date",
      "app_dow_raw",
      "shifted_to_next_day",
      "mode",
      "time",
    ],
  });

  if (!rows.length) {
    throw new Error(`No heartbeat rows found in ${TABLE_HEARTBEAT}`);
  }

  return rows[0];
}

async function fetchWatchScheduleRows() {
  return airtableList(TABLE_WATCH_SCHEDULE, {
    view: VIEW_WATCH_SCHEDULE,
    maxRecords: MAX_RECORDS,
    "fields[]": [
      "record_id",
      "heartbeat",
      "shows",
      "class_groupxclasses_id",
      "class_group_id",
      "class_id",
      "class_number",
      "class_name",
      "group_name",
      "ring_number",
      "app_show_idv2",
      "app_sql_datev2",
      "app_dow_rawv2",
      "shifted_to_next_dayv2",
      "scope_key",
      "scope_run_id",
      "schedule_show_datev2",
      "estimated_start_time",
      "estimated_end_time",
      "latest_status",
      "status",
      "completed_trips",
      "total_trips",
      "latest_estimated_start_time",
      "___latest_estimated_start_time",
      "groups_live",
      "schedule_endpoint",
      "schedule_empty_endpoint",
      "classes_endpointv2",
      "tripTarget",
      "focusTargetClassId",
      "nextTargetClassId",
      "perTrip",
      "latest_ingested_at",
      "last_updated_at",
      "schedule_logs",
    ],
  });
}

async function fetchGroupsLiveRows(appShowId, targetDays) {
  const fieldSet = await fetchTableFieldSet(TABLE_GROUPS_LIVE);
  const baseFields = [
    "class_group_id",
    "show_id",
    "day",
    "estimated_start_time",
    "gone",
    "total",
    "status",
    "curr_updated_at",
    "ingested_at",
    "created_time",
    "is_live",
    "classes",
    "classNumbers",
    "class_numbers",
    "class_numbers_list",
  ];
  const requestedFields = baseFields.filter((fieldName) => fieldSet.has(fieldName));
  let includeStopUpdating = fieldSet.has("stop_updating");

  let rows;
  try {
    rows = await airtableList(TABLE_GROUPS_LIVE, {
      maxRecords: MAX_RECORDS,
      "fields[]": requestedFields,
    });
  } catch (error) {
    const unknownField = extractUnknownFieldNameFromAirtableError(error);
    if (!includeStopUpdating || unknownField !== "stop_updating") throw error;

    includeStopUpdating = false;
    console.log(
      JSON.stringify({
        ok: true,
        event: "groups_live_optional_field_skipped",
        field_name: "stop_updating",
        table: TABLE_GROUPS_LIVE,
      })
    );

    rows = await airtableList(TABLE_GROUPS_LIVE, {
      maxRecords: MAX_RECORDS,
      "fields[]": requestedFields.filter((fieldName) => fieldName !== "stop_updating"),
    });
  }

  const normalizedDays = new Set(Array.from(targetDays || []).map((value) => toIsoDateOnly(value)).filter(Boolean));
  return rows
    .map((row) => {
      const fields = row?.fields || {};
      return {
        recordId: row.id,
        class_group_id: numOrNull(fields.class_group_id),
        show_id: numOrNull(fields.show_id),
        day: toIsoDateOnly(fields.day),
        estimated_start_time: strOrNull(fields.estimated_start_time),
        gone: numOrNull(fields.gone),
        total: numOrNull(fields.total),
        status: strOrNull(fields.status),
        curr_updated_at: strOrNull(fields.curr_updated_at),
        ingested_at: strOrNull(fields.ingested_at),
        created_time: strOrNull(fields.created_time),
        is_live: boolValue(fields.is_live),
        stop_updating: includeStopUpdating ? boolValue(fields.stop_updating) : false,
        class_ids: splitNumericStrings(fields.classes),
        class_numbers: splitNumericStrings(pickFirst(fields.classNumbers, fields.class_numbers, fields.class_numbers_list)),
      };
    })
    .filter((row) => row.class_group_id !== null)
    .filter((row) => !row.stop_updating)
    .filter((row) => row.show_id === appShowId)
    .filter((row) => !normalizedDays.size || !row.day || normalizedDays.has(row.day));
}

function buildGroupsLiveMap(rows) {
  const byGroupId = new Map();

  for (const row of rows) {
    const key = normalizeKey(row.class_group_id);
    if (!key) continue;
    const existing = byGroupId.get(key);
    if (!existing || compareRecordRecency(row, existing) <= 0) {
      byGroupId.set(key, row);
    }
  }

  return byGroupId;
}

function normalizeWatchScheduleRow(record) {
  const fields = record?.fields || {};
  return {
    recordId: record.id,
    record_id: strOrNull(fields.record_id) || record.id,
    heartbeatLinkId: firstLinkId(fields.heartbeat),
    showsLinkId: firstLinkId(fields.shows),
    class_groupxclasses_id: numOrNull(fields.class_groupxclasses_id),
    class_group_id: numOrNull(fields.class_group_id),
    class_id: numOrNull(fields.class_id),
    class_number: numOrNull(fields.class_number),
    class_name: strOrNull(fields.class_name),
    group_name: strOrNull(fields.group_name),
    ring_number: numOrNull(fields.ring_number),
    app_show_idv2: numOrNull(fields.app_show_idv2),
    app_sql_datev2: strOrNull(fields.app_sql_datev2),
    app_dow_rawv2: strOrNull(fields.app_dow_rawv2),
    shifted_to_next_dayv2: boolValue(fields.shifted_to_next_dayv2),
    scope_key: strOrNull(fields.scope_key),
    scope_run_id: strOrNull(fields.scope_run_id),
    schedule_show_datev2: strOrNull(fields.schedule_show_datev2),
    estimated_start_time: strOrNull(fields.estimated_start_time),
    estimated_end_time: strOrNull(fields.estimated_end_time),
    latest_status: strOrNull(fields.latest_status),
    status: strOrNull(fields.status),
    completed_trips: numOrNull(fields.completed_trips),
    total_trips: numOrNull(fields.total_trips),
    latest_estimated_start_time: strOrNull(fields.latest_estimated_start_time),
    latest_estimated_start_hidden: strOrNull(fields.___latest_estimated_start_time),
    groups_live_link: firstLinkId(fields.groups_live),
    schedule_endpoint: strOrNull(fields.schedule_endpoint),
    schedule_empty_endpoint: strOrNull(fields.schedule_empty_endpoint),
    classes_endpointv2: strOrNull(fields.classes_endpointv2),
    tripTarget: boolValue(fields.tripTarget),
    focusTargetClassId: numOrNull(fields.focusTargetClassId),
    nextTargetClassId: numOrNull(fields.nextTargetClassId),
    perTrip: numOrNull(fields.perTrip),
    latest_ingested_at: strOrNull(fields.latest_ingested_at),
    last_updated_at: strOrNull(fields.last_updated_at),
  };
}

function deriveRowComputation(row, groupRow, heartbeatContext) {
  const appTimeText = heartbeatContext.app_time;
  const appTimeMinutes = parseTimeTextToMinutes(appTimeText);
  const startLiveText = strOrNull(groupRow?.estimated_start_time);
  const startAnchorText = startLiveText || row.estimated_start_time;
  const startAnchorMins = parseTimeTextToMinutes(startAnchorText);
  const estimatedEndTimeMinutes = parseTimeTextToMinutes(row.estimated_end_time);
  const totalTripsLive = numOrNull(groupRow?.total);
  const completedTripsLive = numOrNull(groupRow?.gone);
  const totalTripsFinal = totalTripsLive ?? row.total_trips;
  const completedTripsFinal = completedTripsLive ?? row.completed_trips;
  const rowStatus = strOrNull(row.status);
  const groupStatus = strOrNull(groupRow?.status);
  const latestStatusFinal = rowStatus || groupStatus || row.latest_status || null;
  const priorStatus = row.latest_status || row.status || null;
  const tripMinutesConfigured = numOrNull(row.perTrip);
  const tripMinutesFinal = tripMinutesConfigured && tripMinutesConfigured > 0
    ? tripMinutesConfigured
    : DEFAULT_TRIP_MINUTES;
  const tripMinutesUsedDefault = !(tripMinutesConfigured && tripMinutesConfigured > 0);
  const projectedClassMinutes = totalTripsFinal !== null
    ? roundNumber(totalTripsFinal * tripMinutesFinal, 6)
    : null;
  const endFromProjection = startAnchorMins !== null && projectedClassMinutes !== null
    ? formatMinutesToTimeText(startAnchorMins + projectedClassMinutes)
    : row.estimated_end_time;
  const startDeltaMins = startAnchorMins !== null && appTimeMinutes !== null
    ? roundNumber(startAnchorMins - appTimeMinutes, 6)
    : null;
  const minsTillStart = startDeltaMins !== null ? Math.max(0, startDeltaMins) : null;
  const minsSinceStart = startDeltaMins !== null ? Math.max(0, -startDeltaMins) : null;
  const resolvedClassId = resolveGroupsLiveClassId(groupRow, row.class_number);

  const watchScheduleFields = {
    groups_live: groupRow ? [groupRow.recordId] : undefined,
    class_id: resolvedClassId !== null && isBlank(row.class_id) ? resolvedClassId : undefined,
    estimated_start_time: startLiveText || undefined,
    latest_estimated_start_time: startLiveText || undefined,
    ___latest_estimated_start_time: startLiveText || undefined,
    latest_status: latestStatusFinal || undefined,
    status: rowStatus || undefined,
    completed_trips: completedTripsLive ?? undefined,
    total_trips: totalTripsLive ?? undefined,
    latest_ingested_at: strOrNull(pickFirst(groupRow?.ingested_at, groupRow?.curr_updated_at)) || undefined,
  };

  const changedFields = [];
  if (groupRow && !sameValue(row.groups_live_link ? [row.groups_live_link] : null, [groupRow.recordId])) changedFields.push("groups_live");
  if (watchScheduleFields.class_id !== undefined && !sameValue(row.class_id, watchScheduleFields.class_id)) changedFields.push("class_id");
  if (startLiveText && !sameValue(row.estimated_start_time, startLiveText)) changedFields.push("estimated_start_time");
  if (startLiveText && !sameValue(row.latest_estimated_start_time, startLiveText)) changedFields.push("latest_estimated_start_time");
  if (startLiveText && !sameValue(row.latest_estimated_start_hidden, startLiveText)) changedFields.push("___latest_estimated_start_time");
  if (latestStatusFinal && !sameValue(row.latest_status, latestStatusFinal)) changedFields.push("latest_status");
  if (rowStatus && !sameValue(row.status, rowStatus)) changedFields.push("status");
  if (completedTripsLive !== null && !sameValue(row.completed_trips, completedTripsLive)) changedFields.push("completed_trips");
  if (totalTripsLive !== null && !sameValue(row.total_trips, totalTripsLive)) changedFields.push("total_trips");
  if (!sameValue(row.latest_ingested_at, pickFirst(groupRow?.ingested_at, groupRow?.curr_updated_at) || null)) changedFields.push("latest_ingested_at");

  const priorOutputs = {
    groups_live: row.groups_live_link ? [row.groups_live_link] : [],
    class_id: row.class_id,
    estimated_start_time: row.estimated_start_time,
    latest_estimated_start_time: row.latest_estimated_start_time,
    latest_status: row.latest_status,
    status: row.status,
    completed_trips: row.completed_trips,
    total_trips: row.total_trips,
    latest_ingested_at: row.latest_ingested_at,
  };

  const computedOutputs = {
    ...watchScheduleFields,
    rs_start_time: startAnchorText || null,
    rs_end_time: endFromProjection || null,
    rs_length: projectedClassMinutes,
    rs_mins_till_start: minsTillStart,
    rs_mins_since_start: minsSinceStart,
    rs_status: latestStatusFinal || null,
    rs_completed_trips: completedTripsLive,
    rs_trip_default: DEFAULT_TRIP_MINUTES,
    rs_trip_time: tripMinutesFinal,
    rs_trip_time2: tripMinutesFinal,
    calc_trip_minutes_final: tripMinutesFinal,
    calc_trip_minutes_used_default: tripMinutesUsedDefault,
    calc_projected_class_minutes: projectedClassMinutes,
    calc_start_anchor_text: startAnchorText || null,
    calc_start_anchor_mins: startAnchorMins,
  };

  const underwayFlip = Boolean(groupRow) && (
    (isUnderwayStatus(latestStatusFinal) && !isUnderwayStatus(priorStatus)) ||
    (
      isUnderwayStatus(latestStatusFinal) &&
      (numOrNull(row.completed_trips) ?? 0) === 0 &&
      (completedTripsLive ?? 0) > 0
    )
  );

  return {
    appTimeText,
    appTimeMinutes,
    estimatedEndTimeMinutes,
    startLiveText,
    startAnchorText,
    startAnchorMins,
    latestStatusFinal,
    totalTripsLive,
    totalTripsFinal,
    completedTripsFinal,
    completedTripsLive,
    tripMinutesFinal,
    tripMinutesUsedDefault,
    projectedClassMinutes,
    endFromProjection,
    minsTillStart,
    minsSinceStart,
    watchScheduleFields,
    priorStatus,
    underwayFlip,
    priorOutputs,
    computedOutputs,
    changedFields,
  };
}

function buildTriggerEvaluationContext(row, groupRow, heartbeatContext, computation, priorLogFields) {
  const rowStatus = strOrNull(row.status);
  const currentByField = {
    status: rowStatus,
    latest_status: computation.latestStatusFinal,
    rs_status: computation.latestStatusFinal,
    completed_trips: computation.completedTripsFinal,
    completed_trips_live: computation.completedTripsLive,
    rs_completed_trips: computation.completedTripsLive,
    rs_completed_trips_live: computation.completedTripsLive,
    rs_total_trips: computation.totalTripsFinal,
    total_trips: computation.totalTripsFinal,
    total_trips_live: computation.totalTripsLive,
    rs_mins_till_start: computation.minsTillStart,
    rs_mins_since_start: computation.minsSinceStart,
    rs_start_time: computation.startAnchorText,
    rs_end_time: computation.endFromProjection,
    estimated_start_time: row.estimated_start_time,
    estimated_start_live: computation.startLiveText,
    app_sql_datev2: row.app_sql_datev2,
    app_dow_rawv2: row.app_dow_rawv2,
    class_id: row.class_id,
    class_group_id: row.class_group_id,
    ring_number: row.ring_number,
    tripTarget: row.tripTarget,
  };

  const priorByField = {
    status: pickFirst(priorLogFields?.status, row.status),
    latest_status: pickFirst(priorLogFields?.latest_status, row.latest_status),
    rs_status: pickFirst(priorLogFields?.rs_status, row.latest_status, row.status),
    completed_trips: pickFirst(priorLogFields?.completed_trips, row.completed_trips),
    completed_trips_live: pickFirst(priorLogFields?.completed_trips_live, null),
    rs_completed_trips: pickFirst(priorLogFields?.rs_completed_trips, null),
    rs_completed_trips_live: pickFirst(priorLogFields?.rs_completed_trips_live, null),
    rs_total_trips: pickFirst(priorLogFields?.rs_total_trips, priorLogFields?.total_trips, row.total_trips),
    total_trips: pickFirst(priorLogFields?.total_trips, row.total_trips),
    estimated_start_time: pickFirst(priorLogFields?.estimated_start_time, row.estimated_start_time),
  };

  return { currentByField, priorByField };
}

function evaluateTriggerTag(trigger, triggerContext) {
  const query = normalizeKey(trigger?.query).toLowerCase();
  const argument = normalizeKey(trigger?.argument).toLowerCase();
  const fieldName = strOrNull(trigger?.this_field);
  if (!query || !fieldName) return false;

  const currentValue = triggerContext.currentByField[fieldName];
  const priorValue = triggerContext.priorByField[fieldName];
  const outputField = strOrNull(trigger?.output_field);
  const priorTriggered = outputField ? boolValue(triggerContext.priorByField[outputField]) : false;

  if (query === "first_transition") {
    if (priorTriggered) return false;
    const targetValue = pickFirst(trigger?.to, trigger?.from);
    if (isBlank(targetValue)) return false;
    if (argument === "from_not") {
      return compareTriggerValue(currentValue, targetValue) && !compareTriggerValue(priorValue, targetValue);
    }
    const fromValue = trigger?.from;
    return compareTriggerValue(currentValue, targetValue) && compareTriggerValue(priorValue, fromValue);
  }

  if (query === "in_range") {
    const currentNum = parseTriggerNumber(currentValue);
    const fromNum = parseTriggerNumber(trigger?.from);
    const toNum = parseTriggerNumber(trigger?.to);
    if (currentNum === null || fromNum === null || toNum === null) return false;
    const low = Math.min(fromNum, toNum);
    const high = Math.max(fromNum, toNum);
    return currentNum >= low && currentNum <= high;
  }

  if (query === "first_in_range") {
    if (priorTriggered) return false;
    const currentNum = parseTriggerNumber(currentValue);
    const priorNum = parseTriggerNumber(priorValue);
    const fromNum = parseTriggerNumber(trigger?.from);
    const toNum = parseTriggerNumber(trigger?.to);
    if (currentNum === null || fromNum === null || toNum === null) return false;
    const low = Math.min(fromNum, toNum);
    const high = Math.max(fromNum, toNum);
    const currentInRange = currentNum >= low && currentNum <= high;
    const priorInRange = priorNum !== null && priorNum >= low && priorNum <= high;
    return currentInRange && !priorInRange;
  }

  if (query === "equals") {
    return compareTriggerValue(currentValue, pickFirst(trigger?.to, trigger?.from));
  }

  if (query === "gt" || query === "gte" || query === "lt" || query === "lte") {
    const currentNum = parseTriggerNumber(currentValue);
    const targetNum = parseTriggerNumber(pickFirst(trigger?.to, trigger?.from));
    if (currentNum === null || targetNum === null) return false;
    if (query === "gt") return currentNum > targetNum;
    if (query === "gte") return currentNum >= targetNum;
    if (query === "lt") return currentNum < targetNum;
    return currentNum <= targetNum;
  }

  return false;
}

function applyTriggerTags(fields, triggerTags, triggerContext, scheduleLogFieldSet) {
  const fired = [];
  for (const trigger of triggerTags || []) {
    const outputField = strOrNull(trigger?.output_field);
    if (!outputField || !scheduleLogFieldSet.has(outputField)) continue;
    if (!evaluateTriggerTag(trigger, triggerContext)) continue;
    fields[outputField] = true;
    fired.push(outputField);
  }
  return fired;
}

function buildScheduleLogFields(row, groupRow, heartbeatContext, computation, calcStatus, skipReason, scheduleLogFieldSet, triggerTags, priorLogFields) {
  const fields = {};
  const nowIso = new Date().toISOString();
  const rowStatus = strOrNull(row.status);
  const rsStartTimeDiff = buildDiffText(priorLogFields?.rs_start_time, computation.startAnchorText);
  const calcLogKey = [
    heartbeatContext.scope_run_id || heartbeatContext.heartbeat_record_id,
    row.record_id,
    row.class_groupxclasses_id ?? row.class_group_id ?? row.class_id ?? "row",
  ].join("|");

  setIfPresent(fields, scheduleLogFieldSet.has("calc_log_key") ? "calc_log_key" : "", calcLogKey);
  setIfPresent(fields, scheduleLogFieldSet.has("heartbeat") ? "heartbeat" : "", heartbeatContext.heartbeat_record_id ? [heartbeatContext.heartbeat_record_id] : undefined);
  setIfPresent(fields, scheduleLogFieldSet.has("watch_schedule") ? "watch_schedule" : "", [row.recordId]);
  setIfPresent(fields, scheduleLogFieldSet.has("shows") ? "shows" : "", row.showsLinkId ? [row.showsLinkId] : undefined);
  setIfPresent(fields, scheduleLogFieldSet.has("created_at") ? "created_at" : "", nowIso);
  setIfPresent(fields, scheduleLogFieldSet.has("app_show_id") ? "app_show_id" : "", heartbeatContext.app_show_id);
  setIfPresent(fields, scheduleLogFieldSet.has("app_sql_date") ? "app_sql_date" : "", heartbeatContext.app_sql_date);
  setIfPresent(fields, scheduleLogFieldSet.has("app_show_idv2") ? "app_show_idv2" : "", row.app_show_idv2);
  setIfPresent(fields, scheduleLogFieldSet.has("app_sql_datev2") ? "app_sql_datev2" : "", row.app_sql_datev2);
  setIfPresent(fields, scheduleLogFieldSet.has("app_dow_rawv2") ? "app_dow_rawv2" : "", row.app_dow_rawv2);
  setIfPresent(fields, scheduleLogFieldSet.has("shifted_to_next_dayv2") ? "shifted_to_next_dayv2" : "", row.shifted_to_next_dayv2);
  setIfPresent(fields, scheduleLogFieldSet.has("scope_key") ? "scope_key" : "", row.scope_key);
  setIfPresent(fields, scheduleLogFieldSet.has("scope_run_id") ? "scope_run_id" : "", row.scope_run_id);
  setIfPresent(fields, scheduleLogFieldSet.has("schedule_show_datev2") ? "schedule_show_datev2" : "", row.schedule_show_datev2);
  setIfPresent(fields, scheduleLogFieldSet.has("class_groupxclasses_id") ? "class_groupxclasses_id" : "", row.class_groupxclasses_id);
  setIfPresent(fields, scheduleLogFieldSet.has("class_group_id") ? "class_group_id" : "", row.class_group_id);
  setIfPresent(fields, scheduleLogFieldSet.has("class_id") ? "class_id" : "", row.class_id);
  setIfPresent(fields, scheduleLogFieldSet.has("class_number") ? "class_number" : "", row.class_number);
  setIfPresent(fields, scheduleLogFieldSet.has("class_name") ? "class_name" : "", row.class_name);
  setIfPresent(fields, scheduleLogFieldSet.has("group_name") ? "group_name" : "", row.group_name);
  setIfPresent(fields, scheduleLogFieldSet.has("ring_number") ? "ring_number" : "", row.ring_number);
  setIfPresent(fields, scheduleLogFieldSet.has("app_time") ? "app_time" : "", computation.appTimeText);
  setIfPresent(fields, scheduleLogFieldSet.has("app_time_minutes") ? "app_time_minutes" : "", computation.appTimeMinutes);
  setIfPresent(fields, scheduleLogFieldSet.has("estimated_start_time") ? "estimated_start_time" : "", row.estimated_start_time);
  setIfPresent(fields, scheduleLogFieldSet.has("estimated_start_time_minutes") ? "estimated_start_time_minutes" : "", parseTimeTextToMinutes(row.estimated_start_time));
  setIfPresent(fields, scheduleLogFieldSet.has("estimated_start_time_text") ? "estimated_start_time_text" : "", row.estimated_start_time);
  setIfPresent(fields, scheduleLogFieldSet.has("estimated_start_live") ? "estimated_start_live" : "", computation.startLiveText);
  setIfPresent(fields, scheduleLogFieldSet.has("estimated_end_time") ? "estimated_end_time" : "", row.estimated_end_time);
  setIfPresent(fields, scheduleLogFieldSet.has("estimated_end_time_minutes") ? "estimated_end_time_minutes" : "", computation.estimatedEndTimeMinutes);
  setIfPresent(fields, scheduleLogFieldSet.has("status") ? "status" : "", row.status);
  setIfPresent(fields, scheduleLogFieldSet.has("latest_status") ? "latest_status" : "", computation.latestStatusFinal);
  setIfPresent(fields, scheduleLogFieldSet.has("total_trips") ? "total_trips" : "", computation.totalTripsFinal);
  setIfPresent(fields, scheduleLogFieldSet.has("total_trips_live") ? "total_trips_live" : "", computation.totalTripsLive);
  setIfPresent(fields, scheduleLogFieldSet.has("completed_trips") ? "completed_trips" : "", computation.completedTripsFinal);
  setIfPresent(fields, scheduleLogFieldSet.has("completed_trips_live") ? "completed_trips_live" : "", computation.completedTripsLive);
  setIfPresent(fields, scheduleLogFieldSet.has("secondsTill") ? "secondsTill" : "", computation.startAnchorMins !== null && computation.appTimeMinutes !== null ? roundNumber((computation.startAnchorMins - computation.appTimeMinutes) * 60, 3) : null);
  setIfPresent(fields, scheduleLogFieldSet.has("tripTarget") ? "tripTarget" : "", row.tripTarget);
  setIfPresent(fields, scheduleLogFieldSet.has("focusTargetClassId") ? "focusTargetClassId" : "", row.focusTargetClassId);
  setIfPresent(fields, scheduleLogFieldSet.has("nextTargetClassId") ? "nextTargetClassId" : "", row.nextTargetClassId);
  setIfPresent(fields, scheduleLogFieldSet.has("rs_mins_till_start") ? "rs_mins_till_start" : "", computation.minsTillStart);
  setIfPresent(fields, scheduleLogFieldSet.has("rs_start_time") ? "rs_start_time" : "", computation.startAnchorText);
  setIfPresent(fields, scheduleLogFieldSet.has("rs_start_time_diff") ? "rs_start_time_diff" : "", rsStartTimeDiff);
  setIfPresent(fields, scheduleLogFieldSet.has("rs_length") ? "rs_length" : "", computation.projectedClassMinutes);
  setIfPresent(fields, scheduleLogFieldSet.has("rs_end_time") ? "rs_end_time" : "", computation.endFromProjection);
  setIfPresent(fields, scheduleLogFieldSet.has("rs_mins_since_start") ? "rs_mins_since_start" : "", computation.minsSinceStart);
  setIfPresent(fields, scheduleLogFieldSet.has("rs_status") ? "rs_status" : "", computation.latestStatusFinal);
  setIfPresent(fields, scheduleLogFieldSet.has("rs_completed_trips") ? "rs_completed_trips" : "", computation.completedTripsLive);
  setIfPresent(fields, scheduleLogFieldSet.has("rs_completed_trips_live") ? "rs_completed_trips_live" : "", computation.completedTripsLive);
  setIfPresent(fields, scheduleLogFieldSet.has("rs_total_trips") ? "rs_total_trips" : "", computation.totalTripsFinal);
  setIfPresent(fields, scheduleLogFieldSet.has("rs_trip_default") ? "rs_trip_default" : "", DEFAULT_TRIP_MINUTES);
  setIfPresent(fields, scheduleLogFieldSet.has("rs_trip_time") ? "rs_trip_time" : "", computation.tripMinutesFinal);
  setIfPresent(fields, scheduleLogFieldSet.has("rs_trip_time2") ? "rs_trip_time2" : "", computation.tripMinutesFinal);
  setIfPresent(fields, scheduleLogFieldSet.has("calc_mode") ? "calc_mode" : "", CALC_MODE);
  setIfPresent(fields, scheduleLogFieldSet.has("calc_version") ? "calc_version" : "", CALC_VERSION);
  setIfPresent(fields, scheduleLogFieldSet.has("calc_status") ? "calc_status" : "", calcStatus);
  setIfPresent(fields, scheduleLogFieldSet.has("skip_reason") ? "skip_reason" : "", skipReason);
  setIfPresent(fields, scheduleLogFieldSet.has("changed_fields") ? "changed_fields" : "", computation.changedFields.join(", "));
  setIfPresent(fields, scheduleLogFieldSet.has("schedule_endpoint") ? "schedule_endpoint" : "", row.schedule_endpoint);
  setIfPresent(fields, scheduleLogFieldSet.has("schedule_empty_endpoint") ? "schedule_empty_endpoint" : "", row.schedule_empty_endpoint);
  setIfPresent(fields, scheduleLogFieldSet.has("classes_endpointv2") ? "classes_endpointv2" : "", row.classes_endpointv2);
  setIfPresent(fields, scheduleLogFieldSet.has("calc_trip_minutes_final") ? "calc_trip_minutes_final" : "", computation.tripMinutesFinal);
  setIfPresent(fields, scheduleLogFieldSet.has("calc_trip_minutes_used_default") ? "calc_trip_minutes_used_default" : "", computation.tripMinutesUsedDefault);
  setIfPresent(fields, scheduleLogFieldSet.has("calc_projected_class_minutes") ? "calc_projected_class_minutes" : "", computation.projectedClassMinutes);
  setIfPresent(fields, scheduleLogFieldSet.has("calc_start_anchor_text") ? "calc_start_anchor_text" : "", computation.startAnchorText);
  setIfPresent(fields, scheduleLogFieldSet.has("calc_start_anchor_mins") ? "calc_start_anchor_mins" : "", computation.startAnchorMins);
  setIfPresent(fields, scheduleLogFieldSet.has("rs_run_id") ? "rs_run_id" : "", row.scope_run_id || heartbeatContext.scope_run_id);

  const inputsJson = {
    watch_schedule: {
      record_id: row.record_id,
      class_groupxclasses_id: row.class_groupxclasses_id,
      class_group_id: row.class_group_id,
      class_id: row.class_id,
      estimated_start_time: row.estimated_start_time,
      estimated_end_time: row.estimated_end_time,
      latest_status: row.latest_status,
      status: rowStatus,
      completed_trips: row.completed_trips,
      completed_trips_live: computation.completedTripsLive,
      total_trips: row.total_trips,
      tripTarget: row.tripTarget,
      focusTargetClassId: row.focusTargetClassId,
      nextTargetClassId: row.nextTargetClassId,
      perTrip: row.perTrip,
    },
    groups_live: groupRow ? {
      record_id: groupRow.recordId,
      class_group_id: groupRow.class_group_id,
      day: groupRow.day,
      estimated_start_time: groupRow.estimated_start_time,
      gone: groupRow.gone,
      total: groupRow.total,
      status: groupRow.status,
      curr_updated_at: groupRow.curr_updated_at,
      ingested_at: groupRow.ingested_at,
    } : null,
    heartbeat: {
      record_id: heartbeatContext.heartbeat_record_id,
      app_show_id: heartbeatContext.app_show_id,
      app_sql_date: heartbeatContext.app_sql_date,
      app_dow_raw: heartbeatContext.app_dow_raw,
      shifted_to_next_day: heartbeatContext.shifted_to_next_day,
      mode: heartbeatContext.mode,
      time: heartbeatContext.app_time,
    },
  };

  setIfPresent(fields, scheduleLogFieldSet.has("inputs_json") ? "inputs_json" : "", JSON.stringify(inputsJson));
  setIfPresent(fields, scheduleLogFieldSet.has("prior_outputs_json") ? "prior_outputs_json" : "", JSON.stringify(computation.priorOutputs));
  setIfPresent(fields, scheduleLogFieldSet.has("computed_outputs_json") ? "computed_outputs_json" : "", JSON.stringify(computation.computedOutputs));

  const triggerContext = buildTriggerEvaluationContext(row, groupRow, heartbeatContext, computation, priorLogFields);
  const firedTriggerFields = applyTriggerTags(fields, triggerTags, triggerContext, scheduleLogFieldSet);
  if (firedTriggerFields.length && scheduleLogFieldSet.has("changed_fields")) {
    const baseChanged = strOrNull(fields.changed_fields);
    fields.changed_fields = [baseChanged, ...firedTriggerFields].filter(Boolean).join(", ");
  }

  return fields;
}

async function main() {
  requireEnv("AIRTABLE_TOKEN", AIRTABLE_TOKEN);
  requireEnv("AIRTABLE_BASE_ID", AIRTABLE_BASE_ID);

  traceStage("fetch_heartbeat_start");
  const heartbeatRecord = await fetchLatestHeartbeat();
  traceStage("fetch_heartbeat_done", { heartbeat_id: heartbeatRecord?.id || null });
  const heartbeatFields = heartbeatRecord?.fields || {};
  const heartbeatContext = {
    heartbeat_record_id: heartbeatRecord.id,
    heartbeat_rid: strOrNull(heartbeatFields.record_id) || heartbeatRecord.id,
    scope_run_id: strOrNull(heartbeatFields.heartbeat_id) || heartbeatRecord.id,
    app_show_id: numOrNull(heartbeatFields.app_show_id) ?? numOrNull(heartbeatFields.show_id),
    app_sql_date: strOrNull(heartbeatFields.app_sql_date),
    app_dow_raw: strOrNull(heartbeatFields.app_dow_raw),
    shifted_to_next_day: boolValue(heartbeatFields.shifted_to_next_day),
    mode: strOrNull(heartbeatFields.mode),
    app_time: strOrNull(heartbeatFields.time),
  };

  if (heartbeatContext.app_show_id === null) {
    throw new Error("Latest heartbeat is missing app_show_id/show_id");
  }
  if (!heartbeatContext.app_sql_date) {
    throw new Error("Latest heartbeat is missing app_sql_date");
  }
  if (!heartbeatContext.app_dow_raw) {
    throw new Error("Latest heartbeat is missing app_dow_raw");
  }

  traceStage("heartbeat_context", {
    mode: heartbeatContext.mode,
    app_show_id: heartbeatContext.app_show_id,
    app_sql_date: heartbeatContext.app_sql_date,
  });
  if (heartbeatContext.mode !== "DAY") {
    console.log(JSON.stringify({
      ok: true,
      run_status: "NOOP",
      reason: "groups_live overlay only runs in DAY mode",
      heartbeat_mode: heartbeatContext.mode,
      app_show_id: heartbeatContext.app_show_id,
      app_sql_date: heartbeatContext.app_sql_date,
      calc_mode: CALC_MODE,
      calc_version: CALC_VERSION,
    }));
    traceStage("noop_non_day");
    return;
  }

  traceStage("fetch_watch_rows_start");
  const watchRows = (await fetchWatchScheduleRows()).map(normalizeWatchScheduleRow);
  traceStage("fetch_watch_rows_done", { watch_rows: watchRows.length });
  if (!watchRows.length) {
    console.log(JSON.stringify({
      ok: true,
      run_status: "NOOP",
      reason: "No watch_schedule rows found in heartbeat view",
      calc_mode: CALC_MODE,
      calc_version: CALC_VERSION,
    }));
    return;
  }

  traceStage("fetch_metadata_start");
  const [watchScheduleFieldSet, scheduleLogFieldSet, activeTriggerTags] = await Promise.all([
    fetchTableFieldSet(TABLE_WATCH_SCHEDULE),
    fetchTableFieldSet(TABLE_SCHEDULE_LOGS),
    fetchActiveTriggerTags(TABLE_SCHEDULE_LOGS).catch(() => []),
  ]);
  const priorScheduleLogByWatchId = await fetchPriorScheduleLogMap(activeTriggerTags).catch(() => new Map());
  traceStage("fetch_metadata_done", { trigger_tags_active: activeTriggerTags.length });

  const targetDays = new Set(
    watchRows
      .map((row) => toIsoDateOnly(pickFirst(row.schedule_show_datev2, row.app_sql_datev2)))
      .filter(Boolean)
  );

  const groupsRows = await fetchGroupsLiveRows(heartbeatContext.app_show_id, targetDays);
  if (!groupsRows.length) {
    console.log(JSON.stringify({
      ok: true,
      run_status: "NOOP",
      reason: "No groups_live rows available for current show/day",
      heartbeat_mode: heartbeatContext.mode,
      app_show_id: heartbeatContext.app_show_id,
      target_days: Array.from(targetDays),
      watch_rows: watchRows.length,
      calc_mode: CALC_MODE,
      calc_version: CALC_VERSION,
    }));
    return;
  }

  const groupsById = buildGroupsLiveMap(groupsRows);
  const logRecords = [];
  const patchWork = [];
  let matchedRows = 0;
  let skippedRows = 0;
  let changedRows = 0;
  let triggerHits = 0;

  for (const row of watchRows) {
    const groupRow = groupsById.get(normalizeKey(row.class_group_id)) || null;
    const computation = deriveRowComputation(row, groupRow, heartbeatContext);
    const calcStatus = groupRow
      ? (computation.changedFields.length ? "updated" : "unchanged")
      : "skipped";
    const skipReason = groupRow ? null : "group_not_live";

    if (groupRow) matchedRows += 1;
    else skippedRows += 1;
    if (groupRow && computation.changedFields.length) changedRows += 1;

    const logFields = buildScheduleLogFields(
      row,
      groupRow,
      heartbeatContext,
      computation,
      calcStatus,
      skipReason,
      scheduleLogFieldSet,
      activeTriggerTags,
      priorScheduleLogByWatchId.get(row.recordId) || null
    );
    triggerHits += activeTriggerTags.filter((trigger) => {
      const outputField = strOrNull(trigger?.output_field);
      return outputField && fieldsHasTruthy(logFields, outputField);
    }).length;
    logRecords.push({ fields: logFields });

    patchWork.push({ row, groupRow, computation });
  }

  let logCreateResult = { okRows: 0, failedRows: [], createdRecords: [] };
  if (!DRY_RUN) {
    logCreateResult = await airtableCreateRecords(TABLE_SCHEDULE_LOGS, logRecords);
  }

  const patchUpdates = [];
  if (CALC_MODE === "promote") {
    const logIdByKey = new Map(
      (logCreateResult.createdRecords || [])
        .map((record) => [strOrNull(record?.fields?.calc_log_key), record?.id])
        .filter(([key, id]) => Boolean(key && id))
    );

    for (let index = 0; index < patchWork.length; index += 1) {
      const item = patchWork[index];
      const logId = logIdByKey.get(strOrNull(logRecords[index]?.fields?.calc_log_key)) || null;
      const fields = {};

      if (item.groupRow) {
        for (const [fieldName, value] of Object.entries(item.computation.watchScheduleFields)) {
          if (!watchScheduleFieldSet.has(fieldName)) continue;
          if (value === undefined) continue;
          fields[fieldName] = value;
        }
        if (item.computation.changedFields.length && watchScheduleFieldSet.has("last_updated_at")) {
          fields.last_updated_at = new Date().toISOString();
        }
      }

      if (logId && watchScheduleFieldSet.has("schedule_logs")) {
        fields.schedule_logs = [logId];
      }

      if (!Object.keys(fields).length) continue;
      patchUpdates.push({
        id: item.row.recordId,
        fields,
      });
    }
  }

  let patchResult = { okRows: 0, failedRows: [] };
  if (!DRY_RUN && patchUpdates.length) {
    patchResult = await airtablePatchRecords(TABLE_WATCH_SCHEDULE, patchUpdates);
  }

  console.log(JSON.stringify({
    ok: true,
    run_status: DRY_RUN ? "DRY_RUN" : "OK",
    calc_mode: CALC_MODE,
    calc_version: CALC_VERSION,
    heartbeat_mode: heartbeatContext.mode,
    app_show_id: heartbeatContext.app_show_id,
    app_sql_date: heartbeatContext.app_sql_date,
    target_days: Array.from(targetDays),
    watch_rows: watchRows.length,
    groups_live_rows: groupsRows.length,
    matched_rows: matchedRows,
    skipped_rows: skippedRows,
    changed_rows: changedRows,
    trigger_tags_active: activeTriggerTags.length,
    trigger_hits: triggerHits,
    logs_planned: logRecords.length,
    logs_created: DRY_RUN ? 0 : logCreateResult.okRows,
    log_failures: logCreateResult.failedRows.length,
    patches_planned: patchUpdates.length,
    patches_applied: DRY_RUN ? 0 : patchResult.okRows,
    patch_failures: patchResult.failedRows.length,
  }));
}

main()
  .then(() => {
    process.exit(0);
  })
  .catch((error) => {
    const message = String(error?.stack || error?.message || error);
    console.error(JSON.stringify({
      ok: false,
      error: message.slice(0, 4000),
      calc_mode: CALC_MODE,
      calc_version: CALC_VERSION,
    }));
    process.exit(1);
  });
