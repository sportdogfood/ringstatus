const {
  normalizeSchedulePayload,
  chooseScheduleVariant,
} = require("./schedule_normalizer_v2");

const AIRTABLE_TOKEN = process.env.AIRTABLE_TOKEN || "";
const AIRTABLE_BASE_ID = process.env.AIRTABLE_BASE_ID || "";
const CUSTOMER_ID = Number(process.env.CUSTOMER_ID || "15");

const TABLE_HEARTBEAT = process.env.TABLE_HEARTBEAT || "heartbeat";
const TABLE_SHOWS = process.env.TABLE_SHOWS || "shows";
const TABLE_WATCH_SCHEDULE = process.env.TABLE_WATCH_SCHEDULE || "watch_schedule";
const TABLE_ACTIVE_GROUPS = process.env.TABLE_ACTIVE_GROUPS || "active_groups";

const VIEW_HEARTBEAT = process.env.VIEW_HEARTBEAT || "heartbeat";
const VIEW_WATCH_SCHEDULE_HEARTBEAT = process.env.VIEW_WATCH_SCHEDULE_HEARTBEAT || "heartbeat";
const HEARTBEAT_SORT_FIELD = process.env.HEARTBEAT_SORT_FIELD || "hb_at";

const HTTP_TIMEOUT_MS = Number(process.env.HTTP_TIMEOUT_MS || "20000");
const AT_RETRY_ATTEMPTS = Number(process.env.AT_RETRY_ATTEMPTS || "3");
const AT_RETRY_BASE_MS = Number(process.env.AT_RETRY_BASE_MS || "400");
const AT_RETRY_MAX_MS = Number(process.env.AT_RETRY_MAX_MS || "2000");
const FETCH_CONCURRENCY = Math.max(1, Number(process.env.FETCH_CONCURRENCY || "4"));
const DRY_RUN = String(process.env.DRY_RUN || "0") === "1";
const SYNC_ACTIVE_GROUPS_FROM_SCHEDULE = String(process.env.SYNC_ACTIVE_GROUPS_FROM_SCHEDULE || "0") === "1";
const VALID_DOW_RAW = new Set(["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]);
const VALID_MODES = new Set(["DAY", "NIGHT", "OVERNIGHT"]);

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

function toIsoDateOnly(value) {
  if (isBlank(value)) return null;
  if (value instanceof Date) return value.toISOString().slice(0, 10);
  const text = String(value).trim();
  if (/^\d{4}-\d{2}-\d{2}$/.test(text)) return text;
  if (/^\d{4}-\d{2}-\d{2}T/.test(text)) return text.slice(0, 10);
  const parsed = Date.parse(text);
  return Number.isFinite(parsed) ? new Date(parsed).toISOString().slice(0, 10) : null;
}

function dayOfWeekUtc(sqlDate) {
  const iso = toIsoDateOnly(sqlDate);
  if (!iso) return null;
  const ms = Date.parse(`${iso}T00:00:00.000Z`);
  if (!Number.isFinite(ms)) return null;
  return new Date(ms).getUTCDay();
}

function dowName(dow) {
  return ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"][dow] || null;
}

function buildScopeKey(appShowId, appSqlDate, appDowRaw, shiftedToNextDay) {
  return [
    isBlank(appShowId) ? "" : String(appShowId),
    strOrNull(appSqlDate) || "",
    strOrNull(appDowRaw) || "",
    shiftedToNextDay ? "1" : "0",
  ].join("|");
}

function buildScheduleEndpoint(appSqlDate, appShowId) {
  if (isBlank(appSqlDate) || isBlank(appShowId)) return null;
  return `https://broad-tooth-b8ed.gombcg.workers.dev/schedule?date=${encodeURIComponent(appSqlDate)}&show_id=${encodeURIComponent(appShowId)}&customer_id=${encodeURIComponent(CUSTOMER_ID)}`;
}

function buildScheduleEmptyEndpoint(appShowId) {
  if (isBlank(appShowId)) return null;
  return `https://broad-tooth-b8ed.gombcg.workers.dev/schedule?date=00/00/00&show_id=${encodeURIComponent(appShowId)}&customer_id=${encodeURIComponent(CUSTOMER_ID)}`;
}

function buildClassesEndpoint(classId, showId) {
  if (isBlank(classId) || isBlank(showId)) return null;
  return `https://broad-tooth-b8ed.gombcg.workers.dev/classes/${encodeURIComponent(classId)}/?show_id=${encodeURIComponent(showId)}&customer_id=${encodeURIComponent(CUSTOMER_ID)}`;
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

function buildNumericRunId(nowIso) {
  const ms = Date.parse(nowIso);
  return Number.isFinite(ms) ? ms : Date.now();
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

function boolValue(value) {
  const raw = firstValue(value);
  if (raw === true || raw === 1) return true;
  if (raw === false || raw === 0 || raw === null || raw === undefined) return false;
  const text = String(raw).trim().toLowerCase();
  return text === "true" || text === "1";
}

function strictSqlDate(value, fieldName) {
  const text = strOrNull(value);
  if (!text) throw new Error(`Missing required heartbeat field: ${fieldName}`);
  if (!/^\d{4}-\d{2}-\d{2}$/.test(text)) {
    throw new Error(`Invalid heartbeat ${fieldName}: ${text}`);
  }
  return text;
}

function strictDowRaw(value, fieldName) {
  const text = strOrNull(value);
  if (!text) throw new Error(`Missing required heartbeat field: ${fieldName}`);
  if (!VALID_DOW_RAW.has(text)) {
    throw new Error(`Invalid heartbeat ${fieldName}: ${text}`);
  }
  return text;
}

function strictMode(value, fieldName) {
  const text = strOrNull(value);
  if (!text) throw new Error(`Missing required heartbeat field: ${fieldName}`);
  if (!VALID_MODES.has(text)) {
    throw new Error(`Invalid heartbeat ${fieldName}: ${text}`);
  }
  return text;
}

function addDaysSql(sqlDate, days) {
  const base = strictSqlDate(sqlDate, "sql_date");
  const ms = Date.parse(`${base}T00:00:00.000Z`);
  if (!Number.isFinite(ms)) throw new Error(`Invalid heartbeat sql_date: ${base}`);
  const next = new Date(ms + days * 86400000);
  return next.toISOString().slice(0, 10);
}

function compareSqlDate(left, right) {
  const a = strictSqlDate(left, "left_sql_date");
  const b = strictSqlDate(right, "right_sql_date");
  if (a < b) return -1;
  if (a > b) return 1;
  return 0;
}

function sameValue(left, right) {
  if (Array.isArray(left) || Array.isArray(right)) {
    return JSON.stringify(left ?? null) === JSON.stringify(right ?? null);
  }
  return left === right;
}

async function runPool(items, concurrency, worker) {
  const queue = [...items];
  const threads = [];
  const width = Math.max(1, concurrency);

  for (let i = 0; i < width; i += 1) {
    threads.push((async () => {
      while (queue.length) {
        const item = queue.shift();
        if (item === undefined) break;
        await worker(item);
      }
    })());
  }

  await Promise.all(threads);
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
  if (!records.length) return { okRows: 0, failedRows: [] };

  let okRows = 0;
  const failedRows = [];

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

      okRows += batch.length;
    } catch (error) {
      for (const row of batch) {
        failedRows.push({
          key: row?.fields?.class_groupxclasses_id ?? null,
          reason: String(error?.message || error).slice(0, 300),
        });
      }
    }
  }

  return { okRows, failedRows };
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

async function fetchJson(url) {
  const response = await fetchWithRetry(url, {
    method: "GET",
    headers: { Accept: "application/json" },
  });

  const text = await response.text().catch(() => "");
  if (!response.ok) {
    throw new Error(`Fetch failed (${response.status}): ${text.slice(0, 1200)}`);
  }

  try {
    return JSON.parse(text);
  } catch {
    throw new Error(`Response was not valid JSON. First 1200 chars:\n${text.slice(0, 1200)}`);
  }
}

function normalizeClassEndpointPayload(payload) {
  const classObj = payload?.class && typeof payload.class === "object" ? payload.class : {};
  const related = payload?.class_related_data && typeof payload.class_related_data === "object"
    ? payload.class_related_data
    : {};

  return {
    class_id: numOrNull(pickFirst(payload?.class_id, classObj?.class_id)),
    status: strOrNull(pickFirst(payload?.status, related?.status)),
    schedule_date: toIsoDateOnly(pickFirst(payload?.date, related?.date)),
    estimated_start_time: strOrNull(pickFirst(payload?.estimated_time, related?.estimated_time)),
    total_trips: numOrNull(pickFirst(
      payload?.total_trips,
      payload?.totalTrips,
      classObj?.total_trips,
      classObj?.totalTrips,
      related?.total_trips,
      related?.totalTrips,
      payload?.total,
      related?.total
    )),
    completed_trips: numOrNull(pickFirst(
      payload?.completed_trips,
      payload?.completedTrips,
      classObj?.completed_trips,
      classObj?.completedTrips,
      related?.completed_trips,
      related?.completedTrips,
      payload?.gone,
      classObj?.gone,
      related?.gone
    )),
  };
}

async function enrichScheduleRowsWithClassDetails(rows, scope) {
  const detailById = new Map();
  const failures = [];
  const classIds = [...new Set((rows || [])
    .map((row) => numOrNull(row?.fields?.class_id))
    .filter((value) => value !== null))];

  await runPool(classIds, FETCH_CONCURRENCY, async (classId) => {
    const endpoint = buildClassesEndpoint(classId, scope.app_show_idv2);
    if (!endpoint) return;
    try {
      const payload = await fetchJson(endpoint);
      detailById.set(String(classId), normalizeClassEndpointPayload(payload));
    } catch (error) {
      failures.push({
        class_id: classId,
        endpoint,
        reason: String(error?.message || error).slice(0, 300),
      });
    }
  });

  return {
    rows: (rows || []).map((row) => {
      const classId = numOrNull(row?.fields?.class_id);
      return {
        ...row,
        class_detail: classId !== null ? detailById.get(String(classId)) || null : null,
      };
    }),
    class_endpoint_failures: failures,
    class_endpoint_enriched: detailById.size,
  };
}

async function fetchLatestHeartbeat() {
  const rows = await airtableList(TABLE_HEARTBEAT, {
    pageSize: 1,
    "sort[0][field]": HEARTBEAT_SORT_FIELD,
    "sort[0][direction]": "desc",
    "fields[]": [
      "record_id",
      "heartbeat_id",
      "hb_at",
      "show_id",
      "sql_date",
      "app_show_id",
      "app_sql_date",
      "app_dow_raw",
      "shifted_to_next_day",
      "mode",
      "show_date",
      "time",
      "set_to_default_app_sql_date",
      "default_app_sql_date_is",
      "show_app_sql_end_date",
      "show_app_sql_start_date",
      "app_sql_date_source",
    ],
  });

  if (!rows.length) {
    throw new Error(`No heartbeat rows found in ${TABLE_HEARTBEAT}`);
  }

  return rows[0];
}

async function fetchWatchScheduleScopeStatusChoices() {
  const response = await fetchWithRetry(`https://api.airtable.com/v0/meta/bases/${AIRTABLE_BASE_ID}/tables`, {
    headers: { Authorization: `Bearer ${AIRTABLE_TOKEN}` },
  });

  if (!response.ok) {
    const body = await response.text().catch(() => "");
    throw new Error(`Airtable meta failed (${response.status}) watch_schedule: ${body}`);
  }

  const json = await response.json().catch(() => ({}));
  const table = Array.isArray(json?.tables)
    ? json.tables.find((item) => String(item?.name || "").trim() === TABLE_WATCH_SCHEDULE)
    : null;
  const field = Array.isArray(table?.fields)
    ? table.fields.find((item) => String(item?.name || "").trim() === "scope_status")
    : null;
  const choices = Array.isArray(field?.options?.choices) ? field.options.choices : [];
  return new Set(choices.map((choice) => String(choice?.name || "").trim()).filter(Boolean));
}

async function fetchHeartbeatFieldSet() {
  const response = await fetchWithRetry(`https://api.airtable.com/v0/meta/bases/${AIRTABLE_BASE_ID}/tables`, {
    headers: { Authorization: `Bearer ${AIRTABLE_TOKEN}` },
  });

  if (!response.ok) {
    const body = await response.text().catch(() => "");
    throw new Error(`Airtable meta failed (${response.status}) heartbeat: ${body}`);
  }

  const json = await response.json().catch(() => ({}));
  const table = Array.isArray(json?.tables)
    ? json.tables.find((item) => String(item?.name || "").trim() === TABLE_HEARTBEAT)
    : null;
  return new Set(Array.isArray(table?.fields) ? table.fields.map((field) => String(field?.name || "").trim()).filter(Boolean) : []);
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

async function fetchTableFieldMeta(tableName) {
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

  const names = new Set();
  const actualByTrim = new Map();

  for (const field of Array.isArray(table?.fields) ? table.fields : []) {
    const actualName = String(field?.name || "");
    const trimmedName = actualName.trim();
    if (!trimmedName) continue;
    names.add(trimmedName);
    if (!actualByTrim.has(trimmedName)) actualByTrim.set(trimmedName, actualName);
  }

  return { names, actualByTrim };
}

function resolveFieldName(fieldMeta, logicalName) {
  if (!fieldMeta || !logicalName) return null;
  return fieldMeta.actualByTrim?.get(logicalName) || (fieldMeta.names?.has(logicalName) ? logicalName : null);
}

function setResolvedField(fields, fieldMeta, logicalName, value) {
  const actualName = resolveFieldName(fieldMeta, logicalName);
  if (!actualName || value === undefined) return;
  fields[actualName] = value;
}

function clearResolvedField(fields, fieldMeta, logicalName) {
  const actualName = resolveFieldName(fieldMeta, logicalName);
  if (!actualName) return;
  fields[actualName] = null;
}

function buildBaseHeartbeatContext(record) {
  const fields = record?.fields || {};
  const rawShowId = numOrNull(fields.show_id);
  const appShowId = numOrNull(fields.app_show_id);
  const rawSqlDate = strictSqlDate(fields.sql_date, "sql_date");
  const mode = strictMode(fields.mode, "mode");
  const recordId = strOrNull(fields.record_id);

  if (rawShowId === null) throw new Error("Latest heartbeat is missing show_id");
  if (appShowId === null) throw new Error("Latest heartbeat is missing app_show_id");
  if (rawShowId !== appShowId) throw new Error(`Heartbeat app_show_id mismatch: show_id=${rawShowId} app_show_id=${appShowId}`);
  if (!recordId) throw new Error("Latest heartbeat is missing record_id");
  if (recordId !== record.id) {
    throw new Error(`Heartbeat record_id mismatch: field=${recordId} actual=${record.id}`);
  }

  return {
    heartbeat_record_id: record.id,
    heartbeat_rid: recordId,
    hb_at: strOrNull(fields.hb_at),
    app_show_idv2: appShowId,
    scope_run_id: strOrNull(fields.heartbeat_id) || record.id,
    heartbeat_time: strOrNull(fields.time),
    heartbeat_show_date: toIsoDateOnly(fields.show_date),
    raw_sql_date: rawSqlDate,
    mode,
    current_app_sql_date: strOrNull(fields.app_sql_date),
    current_app_dow_raw: strOrNull(fields.app_dow_raw),
    current_shifted_to_next_day: boolValue(fields.shifted_to_next_day),
    current_set_to_default_app_sql_date: boolValue(fields.set_to_default_app_sql_date),
    current_default_app_sql_date_is: strOrNull(fields.default_app_sql_date_is),
    current_show_app_sql_start_date: strOrNull(fields.show_app_sql_start_date),
    current_show_app_sql_end_date: strOrNull(fields.show_app_sql_end_date),
    current_app_sql_date_source: strOrNull(fields.app_sql_date_source),
  };
}

function extractScheduleDefaultInfo(payload) {
  const show = payload?.show && typeof payload.show === "object" ? payload.show : {};
  const showAppName = strOrNull(pickFirst(show.show_name, payload?.show_name));
  const showAppSqlStartDate = toIsoDateOnly(pickFirst(show.start_date, payload?.start_date));
  const showAppSqlEndDate = toIsoDateOnly(pickFirst(show.end_date, payload?.end_date));
  const defaultAppSqlDateIs = strictSqlDate(pickFirst(payload?.show_date, payload?.showDate), "default_app_sql_date_is");
  const validDates = Array.isArray(payload?.show_days_list)
    ? payload.show_days_list.map((item) => toIsoDateOnly(item?.date)).filter(Boolean)
    : [];

  return {
    show_app_name: showAppName,
    show_app_sql_start_date: showAppSqlStartDate,
    show_app_sql_end_date: showAppSqlEndDate,
    default_app_sql_date_is: defaultAppSqlDateIs,
    valid_dates: validDates,
  };
}

function candidateDateFromMode(rawSqlDate, mode) {
  if (mode === "NIGHT") return addDaysSql(rawSqlDate, 1);
  return strictSqlDate(rawSqlDate, "sql_date");
}

function isValidAppSqlDate(candidateDate, scheduleInfo) {
  if (scheduleInfo.valid_dates.length) {
    return scheduleInfo.valid_dates.includes(candidateDate);
  }
  if (scheduleInfo.show_app_sql_start_date && compareSqlDate(candidateDate, scheduleInfo.show_app_sql_start_date) < 0) {
    return false;
  }
  if (scheduleInfo.show_app_sql_end_date && compareSqlDate(candidateDate, scheduleInfo.show_app_sql_end_date) > 0) {
    return false;
  }
  return true;
}

function resolveHeartbeatScope(baseContext, emptyPayload) {
  const scheduleInfo = extractScheduleDefaultInfo(emptyPayload);
  const candidateAppSqlDate = candidateDateFromMode(baseContext.raw_sql_date, baseContext.mode);
  const validCandidate = isValidAppSqlDate(candidateAppSqlDate, scheduleInfo);
  const setToDefault = !validCandidate;
  const finalAppSqlDate = setToDefault
    ? scheduleInfo.default_app_sql_date_is
    : candidateAppSqlDate;
  const finalAppDowRaw = strictDowRaw(dowName(dayOfWeekUtc(finalAppSqlDate)), "derived_app_dow_raw");
  const shiftedToNextDay = setToDefault ? false : baseContext.mode === "NIGHT";
  const appSqlDateSource = setToDefault
    ? "default_day"
    : baseContext.mode === "NIGHT"
    ? "night_shift"
    : "raw_day";

  return {
    heartbeat_record_id: baseContext.heartbeat_record_id,
    heartbeat_rid: baseContext.heartbeat_rid,
    hb_at: baseContext.hb_at,
    app_show_idv2: baseContext.app_show_idv2,
    app_sql_datev2: finalAppSqlDate,
    app_dow_rawv2: finalAppDowRaw,
    shifted_to_next_dayv2: shiftedToNextDay,
    scope_key: buildScopeKey(baseContext.app_show_idv2, finalAppSqlDate, finalAppDowRaw, shiftedToNextDay),
    scope_run_id: baseContext.scope_run_id,
    heartbeat_time: baseContext.heartbeat_time,
    heartbeat_show_date: baseContext.heartbeat_show_date,
    raw_sql_date: baseContext.raw_sql_date,
    mode: baseContext.mode,
    set_to_default_app_sql_date: setToDefault,
    default_app_sql_date_is: scheduleInfo.default_app_sql_date_is,
    show_app_sql_start_date: scheduleInfo.show_app_sql_start_date,
    show_app_sql_end_date: scheduleInfo.show_app_sql_end_date,
    show_app_name: scheduleInfo.show_app_name,
    app_sql_date_source: appSqlDateSource,
    candidate_app_sql_date: candidateAppSqlDate,
  };
}

function buildHeartbeatPatchFields(resolvedScope, heartbeatFieldSet) {
  const fields = {};
  const maybeSet = (name, value) => {
    if (!heartbeatFieldSet.has(name)) return;
    fields[name] = value;
  };

  maybeSet("app_sql_date", resolvedScope.app_sql_datev2);
  maybeSet("app_dow_raw", resolvedScope.app_dow_rawv2);
  maybeSet("shifted_to_next_day", resolvedScope.shifted_to_next_dayv2);
  maybeSet("set_to_default_app_sql_date", resolvedScope.set_to_default_app_sql_date);
  maybeSet("default_app_sql_date_is", resolvedScope.default_app_sql_date_is);
  maybeSet("show_app_sql_start_date", resolvedScope.show_app_sql_start_date);
  maybeSet("show_app_sql_end_date", resolvedScope.show_app_sql_end_date);
  maybeSet("show_app_name", resolvedScope.show_app_name);
  maybeSet("app_sql_date_source", resolvedScope.app_sql_date_source);

  return fields;
}

function diffHeartbeatFields(currentFields, nextFields) {
  const diff = {};
  for (const [name, value] of Object.entries(nextFields)) {
    if (!sameValue(currentFields?.[name], value)) diff[name] = value;
  }
  return diff;
}

async function fetchShowRecordId(appShowId) {
  const rows = await airtableList(TABLE_SHOWS, {
    pageSize: 1,
    filterByFormula: `{show_id}=${Number(appShowId)}`,
    "fields[]": ["show_id"],
  });

  return rows[0]?.id || null;
}

async function fetchExistingRowsForShow(appShowId) {
  const rows = await airtableList(TABLE_WATCH_SCHEDULE, {
    pageSize: 100,
    "fields[]": [
      "class_groupxclasses_id",
      "show_id",
      "show_date",
      "app_show_idv2",
      "app_sql_datev2",
      "schedule_show_datev2",
      "heartbeat",
      "is_current_scope",
      "scope_status",
    ],
  });

  return rows.filter((row) => {
    const fields = row?.fields || {};
    const rawShowId = numOrNull(fields.show_id);
    const appShowIdV2 = numOrNull(fields.app_show_idv2);
    return rawShowId === appShowId || appShowIdV2 === appShowId;
  });
}

async function fetchHeartbeatViewRows() {
  return airtableList(TABLE_WATCH_SCHEDULE, {
    view: VIEW_WATCH_SCHEDULE_HEARTBEAT,
    pageSize: 100,
    "fields[]": ["class_groupxclasses_id", "heartbeat", "is_current_scope"],
  });
}

function chooseExistingWinner(rows, heartbeatViewIdSet) {
  if (!rows.length) return null;

  const scored = rows.map((row, index) => {
    let score = 0;
    if (heartbeatViewIdSet.has(row.id)) score += 10;
    if (boolValue(row?.fields?.is_current_scope)) score += 5;
    if (firstValue(row?.fields?.heartbeat)) score += 3;
    score -= index;
    return { row, score };
  });

  scored.sort((a, b) => b.score - a.score);
  return scored[0].row;
}

function buildCurrentFields(normalizedRow, scope, heartbeatRecordId, showRecordId, nowIso, dateOnly, recordState, scopeStatusValue, watchScheduleFieldMeta) {
  const fields = { ...normalizedRow.fields };
  const classDetail = normalizedRow?.class_detail || null;
  const resolvedScheduledDate = toIsoDateOnly(
    pickFirst(classDetail?.schedule_date, fields.scheduled_date, fields.schedule_show_datev2, fields.show_date)
  );
  delete fields.scope_status;
  if (resolvedScheduledDate) {
    setResolvedField(fields, watchScheduleFieldMeta, "schedule_date", resolvedScheduledDate);
    setResolvedField(fields, watchScheduleFieldMeta, "scheduled_date", resolvedScheduledDate);
  }
  if (classDetail?.status) {
    setResolvedField(fields, watchScheduleFieldMeta, "status", classDetail.status);
  }
  if (classDetail?.total_trips !== null && classDetail?.total_trips !== undefined) {
    setResolvedField(fields, watchScheduleFieldMeta, "total_trips", classDetail.total_trips);
  } else if (fields.total_trips !== undefined) {
    setResolvedField(fields, watchScheduleFieldMeta, "total_trips", fields.total_trips);
  }
  if (watchScheduleFieldMeta?.names?.has("completed_trips") || watchScheduleFieldMeta?.actualByTrim?.has("completed_trips")) {
    setResolvedField(fields, watchScheduleFieldMeta, "completed_trips", classDetail?.completed_trips ?? fields.completed_trips ?? null);
  }
  fields.heartbeat = heartbeatRecordId ? [heartbeatRecordId] : [];
  fields.record_state = recordState;
  fields.run_tag = scope.app_sql_datev2;
  fields.last_updated_at = nowIso;
  fields.is_current_scope = true;
  fields.scope_run_id = scope.scope_run_id;
  setResolvedField(fields, watchScheduleFieldMeta, "inactive", false);
  if (scopeStatusValue) fields.scope_status = scopeStatusValue;
  fields.last_seen_at = dateOnly;
  fields.dropped_at = null;
  fields.is_gotcha = false;
  if (showRecordId) fields.shows = [showRecordId];
  return fields;
}

function resolveActualScheduleShowDate(normalizedRow) {
  const fields = normalizedRow?.fields || {};
  const classDetail = normalizedRow?.class_detail || null;
  return toIsoDateOnly(
    pickFirst(fields.schedule_show_datev2, classDetail?.schedule_date, fields.show_date)
  );
}

function resolveExistingScheduleShowDate(row) {
  const fields = row?.fields || {};
  return toIsoDateOnly(
    pickFirst(fields.schedule_show_datev2, fields.show_date)
  );
}

function rowScheduledDateMatchesScope(normalizedRow, scope) {
  const fields = normalizedRow?.fields || {};
  const classDetail = normalizedRow?.class_detail || null;
  const resolvedScheduledDate = toIsoDateOnly(
    pickFirst(classDetail?.schedule_date, fields.scheduled_date, fields.schedule_show_datev2, fields.show_date)
  );
  return resolvedScheduledDate === scope.app_sql_datev2;
}

function buildDroppedFields(scope, nowIso, dateOnly, scopeStatusValue, watchScheduleFieldMeta) {
  const fields = {
    heartbeat: [],
    is_current_scope: false,
    dropped_at: dateOnly,
    last_updated_at: nowIso,
    run_tag: scope.app_sql_datev2,
    record_state: "existing",
  };
  setResolvedField(fields, watchScheduleFieldMeta, "inactive", true);
  if (scopeStatusValue) fields.scope_status = scopeStatusValue;
  return fields;
}

function minTimeText(left, right) {
  if (!left) return right || null;
  if (!right) return left || null;
  return String(left) <= String(right) ? left : right;
}

function maxTimeText(left, right) {
  if (!left) return right || null;
  if (!right) return left || null;
  return String(left) >= String(right) ? left : right;
}

function buildActiveGroupRows(rows, scope, runId, dateOnly, activeGroupsFieldSet) {
  if (!activeGroupsFieldSet.size) return [];

  const byGroup = new Map();

  for (const row of rows || []) {
    const fields = row?.fields || {};
    const classGroupId = numOrNull(fields.class_group_id);
    if (classGroupId === null) continue;

    const key = `${scope.app_show_idv2}|${classGroupId}`;
    const existing = byGroup.get(key) || {
      key,
      app_sid: scope.app_show_idv2,
      app_sql_date: scope.app_sql_datev2,
      class_group_id: classGroupId,
      class_group_sequence: undefined,
      group_name: null,
      estimated_start_time: null,
      estimated_end_time: null,
      ring_number: undefined,
      run_id: runId,
      last_run: dateOnly,
      inactive: false,
      schedule_date: null,
      scheduled_date: null,
      scheduled_estimated_start_time: null,
    };

    existing.class_group_sequence = existing.class_group_sequence ?? numOrNull(fields.class_group_sequence);
    existing.group_name = existing.group_name || strOrNull(fields.group_name);
    existing.ring_number = existing.ring_number ?? numOrNull(fields.ring_number);
    existing.estimated_start_time = minTimeText(existing.estimated_start_time, strOrNull(fields.estimated_start_time));
    existing.estimated_end_time = maxTimeText(existing.estimated_end_time, strOrNull(fields.estimated_end_time));

    const resolvedScheduleDate = toIsoDateOnly(pickFirst(fields.schedule_show_datev2, fields.show_date));
    existing.schedule_date = existing.schedule_date || resolvedScheduleDate;
    existing.scheduled_date = existing.scheduled_date || resolvedScheduleDate;
    existing.scheduled_estimated_start_time = existing.scheduled_estimated_start_time || strOrNull(fields.estimated_start_time);

    byGroup.set(key, existing);
  }

  return [...byGroup.values()].map((row) => {
    const fields = {};
    for (const [name, value] of Object.entries(row)) {
      if (!activeGroupsFieldSet.has(name)) continue;
      if (value === undefined || value === null) continue;
      if (typeof value === "string" && value.trim() === "") continue;
      fields[name] = value;
    }
    return fields;
  });
}

async function upsertActiveGroups({
  fieldSet,
  rows,
  scopeAppSid,
  runId,
  lastRun,
}) {
  if (!fieldSet.size) {
    return {
      table: TABLE_ACTIVE_GROUPS,
      created_planned: 0,
      updated_planned: 0,
      inactivated_planned: 0,
      writes: { created: 0, updated: 0, inactivated: 0, create_failures: [], update_failures: [], inactivate_failures: [] },
      skipped: true,
    };
  }

  const existingRows = await airtableList(TABLE_ACTIVE_GROUPS, {
    pageSize: 100,
    "fields[]": ["key", "app_sid"],
  });

  const relevantExisting = existingRows.filter((row) => numOrNull(row?.fields?.app_sid) === scopeAppSid);
  const existingByKey = new Map();
  for (const row of relevantExisting) {
    const key = normalizeKey(row?.fields?.key);
    if (!key || existingByKey.has(key)) continue;
    existingByKey.set(key, row);
  }

  const keepKeys = new Set();
  const creates = [];
  const updates = [];

  for (const row of rows) {
    const key = normalizeKey(row?.key);
    if (!key) continue;
    keepKeys.add(key);
    const existing = existingByKey.get(key);
    if (!existing) {
      creates.push({ fields: row });
      continue;
    }
    updates.push({
      id: existing.id,
      fields: row,
    });
  }

  const inactivations = [];
  for (const row of relevantExisting) {
    const key = normalizeKey(row?.fields?.key);
    if (!key || keepKeys.has(key)) continue;
    inactivations.push({
      id: row.id,
      fields: {
        inactive: true,
        run_id: runId,
        last_run: lastRun,
      },
    });
  }

  const summary = {
    table: TABLE_ACTIVE_GROUPS,
    created_planned: creates.length,
    updated_planned: updates.length,
    inactivated_planned: inactivations.length,
    writes: { created: 0, updated: 0, inactivated: 0, create_failures: [], update_failures: [], inactivate_failures: [] },
    skipped: false,
  };

  if (DRY_RUN) return summary;

  const createResult = await airtableCreateRecords(TABLE_ACTIVE_GROUPS, creates);
  const updateResult = await airtablePatchRecords(TABLE_ACTIVE_GROUPS, updates);
  const inactivateResult = await airtablePatchRecords(TABLE_ACTIVE_GROUPS, inactivations);

  summary.writes.created = createResult.okRows;
  summary.writes.updated = updateResult.okRows;
  summary.writes.inactivated = inactivateResult.okRows;
  summary.writes.create_failures = createResult.failedRows;
  summary.writes.update_failures = updateResult.failedRows;
  summary.writes.inactivate_failures = inactivateResult.failedRows;

  return summary;
}

async function runDaily() {
  requireEnv("AIRTABLE_TOKEN", AIRTABLE_TOKEN);
  requireEnv("AIRTABLE_BASE_ID", AIRTABLE_BASE_ID);

  const nowIso = new Date().toISOString();
  const dateOnly = nowIso.slice(0, 10);
  const runId = buildNumericRunId(nowIso);

  const heartbeatRecord = await fetchLatestHeartbeat();
  const baseHeartbeatContext = buildBaseHeartbeatContext(heartbeatRecord);
  const scopeStatusChoices = await fetchWatchScheduleScopeStatusChoices().catch(() => new Set());
  const currentScopeStatus = scopeStatusChoices.has("current") ? "current" : null;
  const droppedScopeStatus = scopeStatusChoices.has("dropped") ? "dropped" : null;
  const heartbeatFieldSet = await fetchHeartbeatFieldSet().catch(() => new Set());
  const watchScheduleFieldMeta = await fetchTableFieldMeta(TABLE_WATCH_SCHEDULE);
  const activeGroupsFieldSet = SYNC_ACTIVE_GROUPS_FROM_SCHEDULE
    ? await fetchTableFieldSet(TABLE_ACTIVE_GROUPS).catch(() => new Set())
    : new Set();

  const emptyUrl = buildScheduleEmptyEndpoint(baseHeartbeatContext.app_show_idv2);
  const emptyPayload = await fetchJson(emptyUrl);
  const scope = resolveHeartbeatScope(baseHeartbeatContext, emptyPayload);
  const heartbeatPatchFields = buildHeartbeatPatchFields(scope, heartbeatFieldSet);
  const heartbeatChangedFields = diffHeartbeatFields(heartbeatRecord?.fields || {}, heartbeatPatchFields);

  if (!DRY_RUN && Object.keys(heartbeatChangedFields).length) {
    await airtablePatchRecords(TABLE_HEARTBEAT, [{
      id: heartbeatRecord.id,
      fields: heartbeatChangedFields,
    }]);
  }

  const datedUrl = buildScheduleEndpoint(scope.app_sql_datev2, scope.app_show_idv2);
  const datedPayload = await fetchJson(datedUrl);

  const datedResult = {
    ok: true,
    source: "dated_schedule",
    url: datedUrl,
    ...normalizeSchedulePayload(datedPayload, {
      scope,
      source: "dated_schedule",
      generatedAt: nowIso,
      generatedDate: dateOnly,
    }),
  };

  const emptyResult = {
    ok: true,
    source: "empty_ping_schedule",
    url: emptyUrl,
    ...normalizeSchedulePayload(emptyPayload, {
      scope,
      source: "empty_ping_schedule",
      generatedAt: nowIso,
      generatedDate: dateOnly,
    }),
  };

  const chosen = chooseScheduleVariant(datedResult, emptyResult);
  const classEnrichment = await enrichScheduleRowsWithClassDetails(chosen.rows, scope);
  const scopedRows = classEnrichment.rows.filter((row) => rowScheduledDateMatchesScope(row, scope));
  const showRecordId = await fetchShowRecordId(scope.app_show_idv2).catch(() => null);
  const existingRows = await fetchExistingRowsForShow(scope.app_show_idv2);
  const heartbeatViewRows = await fetchHeartbeatViewRows().catch(() => []);
  const heartbeatViewIdSet = new Set(heartbeatViewRows.map((row) => row.id));

  const groupedExisting = new Map();
  for (const row of existingRows) {
    const key = normalizeKey(row?.fields?.class_groupxclasses_id);
    if (!key) continue;
    if (!groupedExisting.has(key)) groupedExisting.set(key, []);
    groupedExisting.get(key).push(row);
  }

  const existingByKey = new Map();
  for (const [key, rows] of groupedExisting.entries()) {
    const winner = chooseExistingWinner(rows, heartbeatViewIdSet);
    if (winner) existingByKey.set(key, winner);
  }

  const createRecords = [];
  const updateRecords = [];
  const keepKeySet = new Set();
  const keepRecordIdSet = new Set();
  const actualScheduleShowDateByKey = new Map();

  for (const row of scopedRows) {
    const key = normalizeKey(row.key);
    if (!key) continue;
    keepKeySet.add(key);
    actualScheduleShowDateByKey.set(key, resolveActualScheduleShowDate(row));

    const existing = existingByKey.get(key);
    if (existing) keepRecordIdSet.add(existing.id);
    const fields = buildCurrentFields(
      row,
      scope,
      heartbeatRecord.id,
      showRecordId,
      nowIso,
      dateOnly,
      existing ? "existing" : "new",
      currentScopeStatus,
      watchScheduleFieldMeta
    );

    if (existing) {
      updateRecords.push({ id: existing.id, fields });
    } else {
      createRecords.push({ fields });
    }
  }

  const dropUpdates = [];
  let droppedForScheduleShowDateMismatch = 0;
  for (const row of existingRows) {
    const key = normalizeKey(row?.fields?.class_groupxclasses_id);
    if (!key) continue;

    const hasCurrentMarkers =
      heartbeatViewIdSet.has(row.id) ||
      boolValue(row?.fields?.is_current_scope) ||
      !!firstValue(row?.fields?.heartbeat);
    if (!hasCurrentMarkers) continue;

    if (keepRecordIdSet.has(row.id)) continue;

    if (keepKeySet.has(key)) {
      const actualScheduleShowDate = actualScheduleShowDateByKey.get(key);
      const existingScheduleShowDate = resolveExistingScheduleShowDate(row);
      if (!actualScheduleShowDate || existingScheduleShowDate === actualScheduleShowDate) continue;
      droppedForScheduleShowDateMismatch += 1;
    }

    dropUpdates.push({
      id: row.id,
      fields: buildDroppedFields(scope, nowIso, dateOnly, droppedScopeStatus, watchScheduleFieldMeta),
    });
  }

  const summary = {
    ok: true,
    dry_run: DRY_RUN,
    scope,
    chosen_source: chosen.source,
    heartbeat_patch_fields: heartbeatChangedFields,
    row_count: scopedRows.length,
    filtered_out_scheduled_date_mismatch: chosen.rows.length - scopedRows.length,
    dropped_due_to_schedule_show_date_mismatch: droppedForScheduleShowDateMismatch,
    creates_planned: createRecords.length,
    updates_planned: updateRecords.length,
    drops_planned: dropUpdates.length,
    existing_show_rows: existingRows.length,
    heartbeat_view_rows: heartbeatViewRows.length,
    show_record_bound: !!showRecordId,
    fetches: {
      dated_schedule: {
        url: datedUrl,
        rows: datedResult.rows.length,
        schedule_show_datev2: datedResult.schedule_show_datev2 || null,
      },
      empty_ping_schedule: {
        url: emptyUrl,
        rows: emptyResult.rows.length,
        schedule_show_datev2: emptyResult.schedule_show_datev2 || null,
      },
      classes_endpoint: {
        enriched_rows: classEnrichment.class_endpoint_enriched,
        failures: classEnrichment.class_endpoint_failures,
      },
    },
    writes: {
      created: 0,
      updated: 0,
      dropped: 0,
      create_failures: [],
      update_failures: [],
      drop_failures: [],
    },
    active_groups: {
      table: TABLE_ACTIVE_GROUPS,
      created_planned: 0,
      updated_planned: 0,
      inactivated_planned: 0,
      writes: { created: 0, updated: 0, inactivated: 0, create_failures: [], update_failures: [], inactivate_failures: [] },
      skipped: true,
    },
  };

  const activeGroupRows = buildActiveGroupRows(scopedRows, scope, runId, dateOnly, activeGroupsFieldSet);
  summary.active_groups.created_planned = activeGroupRows.length;

  if (DRY_RUN) {
    if (activeGroupsFieldSet.size) {
      summary.active_groups = await upsertActiveGroups({
        fieldSet: activeGroupsFieldSet,
        rows: activeGroupRows,
        scopeAppSid: scope.app_show_idv2,
        runId,
        lastRun: dateOnly,
      });
    }
    return summary;
  }

  const createResult = await airtableCreateRecords(TABLE_WATCH_SCHEDULE, createRecords);
  const updateResult = await airtablePatchRecords(TABLE_WATCH_SCHEDULE, updateRecords);
  const dropResult = await airtablePatchRecords(TABLE_WATCH_SCHEDULE, dropUpdates);

  summary.writes.created = createResult.okRows;
  summary.writes.updated = updateResult.okRows;
  summary.writes.dropped = dropResult.okRows;
  summary.writes.create_failures = createResult.failedRows;
  summary.writes.update_failures = updateResult.failedRows;
  summary.writes.drop_failures = dropResult.failedRows;
  if (activeGroupsFieldSet.size) {
    summary.active_groups = await upsertActiveGroups({
      fieldSet: activeGroupsFieldSet,
      rows: activeGroupRows,
      scopeAppSid: scope.app_show_idv2,
      runId,
      lastRun: dateOnly,
    });
  }

  return summary;
}

async function main() {
  const result = await runDaily();
  process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
}

if (require.main === module) {
  main().catch((error) => {
    process.stdout.write(`${JSON.stringify({
      ok: false,
      error: String(error?.message || error),
      dry_run: DRY_RUN,
    }, null, 2)}\n`);
    process.exit(1);
  });
}

module.exports = {
  runDaily,
};
