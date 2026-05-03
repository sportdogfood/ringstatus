const AIRTABLE_TOKEN = process.env.AIRTABLE_TOKEN || "";
const AIRTABLE_BASE_ID = process.env.AIRTABLE_BASE_ID || "";
const CUSTOMER_ID = Number(process.env.CUSTOMER_ID || "15");

const {
  buildScheduleMap,
  collectTripCandidates,
  normalizeTripsForScope,
} = require("./trips_normalizer_v2");
const {
  assertValidPayload,
  isSoftPayloadError,
} = require("./lib/soft_payload_guard");
const {
  fetchTextWithConfiguredTransport,
} = require("./lib/sgl_fetch_adapter");
const {
  buildClassDetailEndpoint,
} = require("./lib/watch_trips_enrichment");

const BASE_URL = String(process.env.BASE_URL || "https://broad-tooth-b8ed.gombcg.workers.dev").trim().replace(/\/+$/, "");

const TABLE_HEARTBEAT = process.env.TABLE_HEARTBEAT || "heartbeat";
const TABLE_SHOWS = process.env.TABLE_SHOWS || "shows";
const TABLE_WATCH_SCHEDULE = process.env.TABLE_WATCH_SCHEDULE || "watch_schedule";
const TABLE_WATCH_TRIPS = process.env.TABLE_WATCH_TRIPS || "watch_trips";
const TABLE_ACTIVE_TENANTS = process.env.TABLE_ACTIVE_TENANTS || "active_tenants";
const TABLE_ACTIVE_CLASSES = process.env.TABLE_ACTIVE_CLASSES || "active_classes";
const TABLE_ACTIVE_ENTRIES = process.env.TABLE_ACTIVE_ENTRIES || "active_entries";
const TABLE_ACTIVE_GROUPS = process.env.TABLE_ACTIVE_GROUPS || "active_groups";
const TABLE_WW_RIDERS = process.env.TABLE_WW_RIDERS || "ww_riders";
const TABLE_WW_HORSES = process.env.TABLE_WW_HORSES || "ww_horses";
const TABLE_WW_TRAINERS = process.env.TABLE_WW_TRAINERS || "ww_trainers";

const VIEW_WATCH_SCHEDULE = process.env.VIEW_WATCH_SCHEDULE || "heartbeat";
const VIEW_WATCH_TRIPS = process.env.VIEW_WATCH_TRIPS || "heartbeat";
const VIEW_ACTIVE_TENANTS = process.env.VIEW_ACTIVE_TENANTS || "active_tenants";
const HEARTBEAT_SORT_FIELD = process.env.HEARTBEAT_SORT_FIELD || "hb_at";

const HTTP_TIMEOUT_MS = Number(process.env.HTTP_TIMEOUT_MS || "20000");
const AT_RETRY_ATTEMPTS = Number(process.env.AT_RETRY_ATTEMPTS || "3");
const AT_RETRY_BASE_MS = Number(process.env.AT_RETRY_BASE_MS || "400");
const AT_RETRY_MAX_MS = Number(process.env.AT_RETRY_MAX_MS || "2000");
const DRY_RUN = String(process.env.DRY_RUN || "0") === "1";
const FETCH_CONCURRENCY = Math.max(1, Number(process.env.FETCH_CONCURRENCY || "4"));

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

function normalizePidToken(value) {
  const num = numOrNull(value);
  return num === null || num <= 0 ? "" : String(num);
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

function normalizeEntryNumber(value) {
  if (value === undefined || value === null) return undefined;
  if (typeof value === "number" && Number.isFinite(value)) return value;
  const text = String(value).trim();
  if (!text) return undefined;
  if (/^\d+$/.test(text)) return Number(text);
  return text;
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

function toIsoDateOnly(value) {
  if (isBlank(value)) return null;
  if (value instanceof Date) return value.toISOString().slice(0, 10);
  const text = String(value).trim();
  if (/^\d{4}-\d{2}-\d{2}$/.test(text)) return text;
  if (/^\d{4}-\d{2}-\d{2}T/.test(text)) return text.slice(0, 10);
  const parsed = Date.parse(text);
  return Number.isFinite(parsed) ? new Date(parsed).toISOString().slice(0, 10) : null;
}

function resolveTripScheduleDate(source) {
  if (!source || typeof source !== "object") return null;
  return toIsoDateOnly(pickFirst(
    firstValue(source.schedule_show_datev2),
    firstValue(source.scheduled_date),
    firstValue(source["schedule_show_datev2 (from watch_schedule)"]),
    firstValue(source.show_date),
    firstValue(source.date)
  ));
}

function setIfPresent(target, fieldName, value) {
  if (!fieldName) return;
  if (value === undefined || value === null) return;
  if (typeof value === "string" && value.trim() === "") return;
  target[fieldName] = value;
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
        await sleep(Math.min(maxMs, baseMs * attempt + Math.floor(Math.random() * 200)));
        continue;
      }
      return response;
    } catch (error) {
      lastError = error;
      if (!isRetryableFetchError(error) || attempt === attempts) throw error;
      await sleep(Math.min(maxMs, baseMs * attempt + Math.floor(Math.random() * 250)));
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
        body: JSON.stringify({ records: batch, typecast: true }),
      });

      if (!response.ok) {
        const body = await response.text().catch(() => "");
        throw new Error(`Airtable create failed (${response.status}) ${tableName}: ${body}`);
      }

      okRows += batch.length;
    } catch (error) {
      for (const row of batch) {
        failedRows.push({
          key: row?.fields?.entryxclasses_uuid ?? null,
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
        body: JSON.stringify({ records: batch, typecast: true }),
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
          key: row?.fields?.entryxclasses_uuid ?? null,
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

async function fetchScopeStatusChoices(tableName) {
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
  const field = Array.isArray(table?.fields)
    ? table.fields.find((item) => String(item?.name || "").trim() === "scope_status")
    : null;
  const choices = Array.isArray(field?.options?.choices) ? field.options.choices : [];
  return new Set(choices.map((choice) => String(choice?.name || "").trim()).filter(Boolean));
}

async function fetchLatestHeartbeat() {
  const rows = await airtableList(TABLE_HEARTBEAT, {
    pageSize: 1,
    "sort[0][field]": HEARTBEAT_SORT_FIELD,
    "sort[0][direction]": "desc",
    "fields[]": [
      "record_id",
      "heartbeat_id",
      "show_id",
      "app_show_id",
      "app_sql_date",
      "app_dow_raw",
      "shifted_to_next_day",
      "mode",
      "time",
    ],
  });

  if (!rows.length) throw new Error(`No heartbeat rows found in ${TABLE_HEARTBEAT}`);

  const record = rows[0];
  const fields = record?.fields || {};
  const appShowId = numOrNull(fields.app_show_id) ?? numOrNull(fields.show_id);
  const appSqlDate = strOrNull(fields.app_sql_date);
  const appDowRaw = strOrNull(fields.app_dow_raw);
  const recordId = strOrNull(fields.record_id) || record.id;

  if (appShowId === null) throw new Error("Latest heartbeat missing app_show_id/show_id");
  if (!appSqlDate) throw new Error("Latest heartbeat missing app_sql_date");
  if (!appDowRaw) throw new Error("Latest heartbeat missing app_dow_raw");
  if (recordId !== record.id) throw new Error(`Heartbeat record_id mismatch: field=${recordId} actual=${record.id}`);

  return {
    recordId: record.id,
    scope_run_id: strOrNull(fields.heartbeat_id) || record.id,
    app_show_id: appShowId,
    app_sql_date: appSqlDate,
    app_dow_raw: appDowRaw,
    shifted_to_next_day: boolValue(fields.shifted_to_next_day),
    mode: strOrNull(fields.mode),
    app_time: strOrNull(fields.time),
  };
}

async function fetchShowRecordId(appShowId) {
  const rows = await airtableList(TABLE_SHOWS, {
    pageSize: 1,
    "fields[]": ["show_id"],
  });

  return rows.find((row) => numOrNull(row?.fields?.show_id) === appShowId)?.id || null;
}

async function fetchWatchScheduleRows() {
  return airtableList(TABLE_WATCH_SCHEDULE, {
    view: VIEW_WATCH_SCHEDULE,
    pageSize: 100,
    "fields[]": [
      "record_id",
      "class_groupxclasses_id",
      "class_group_id",
      "class_id",
      "class_number",
      "class_name",
      "schedule_sequencetype",
      "class_type",
      "group_name",
      "ring_number",
      "estimated_start_time",
      "estimated_end_time",
      "class_group_sequence",
      "schedule_show_datev2",
      "show_date",
    ],
  });
}

async function fetchActiveTenantRows() {
  const rows = await airtableList(TABLE_ACTIVE_TENANTS, {
    view: VIEW_ACTIVE_TENANTS,
    pageSize: 100,
    "fields[]": ["tenant_id", "tenant_active"],
  });

  return rows
    .filter((row) => boolValue(row?.fields?.tenant_active))
    .map((row) => ({
      recordId: row.id,
      tenant_id: normalizePidToken(row?.fields?.tenant_id),
    }));
}

async function fetchWwTrainerRecordIdByPid() {
  const rows = await airtableList(TABLE_WW_TRAINERS, {
    pageSize: 100,
    "fields[]": ["pid"],
  });

  return new Map(
    rows
      .map((row) => [normalizePidToken(row?.fields?.pid), row.id])
      .filter(([pid]) => Boolean(pid))
  );
}

async function fetchExistingTripsForShow(appShowId) {
  const rows = await airtableList(TABLE_WATCH_TRIPS, {
    pageSize: 100,
    "fields[]": [
      "entryxclasses_uuid",
      "show_id",
      "app_show_id",
      "app_show_idv2",
      "heartbeat",
      "is_current_scope",
      "scope_status",
    ],
  });

  return rows.filter((row) => {
    const fields = row?.fields || {};
    return numOrNull(fields.show_id) === appShowId ||
      numOrNull(fields.app_show_id) === appShowId ||
      numOrNull(fields.app_show_idv2) === appShowId;
  });
}

async function fetchHeartbeatViewTripRows() {
  return airtableList(TABLE_WATCH_TRIPS, {
    view: VIEW_WATCH_TRIPS,
    pageSize: 100,
    "fields[]": ["entryxclasses_uuid", "heartbeat", "is_current_scope"],
  });
}

async function fetchTripScheduleBackfillRows(appShowId) {
  const rows = await airtableList(TABLE_WATCH_TRIPS, {
    pageSize: 100,
    "fields[]": [
      "show_id",
      "app_show_id",
      "app_show_idv2",
      "schedule_show_datev2",
      "scheduled_date",
      "schedule_show_datev2 (from watch_schedule)",
      "show_date",
      "date",
    ],
  });

  return rows.filter((row) => {
    const fields = row?.fields || {};
    return numOrNull(fields.show_id) === appShowId ||
      numOrNull(fields.app_show_id) === appShowId ||
      numOrNull(fields.app_show_idv2) === appShowId;
  });
}

function buildTripScheduleBackfillUpdates(rows, watchTripsFieldSet) {
  const updates = [];

  for (const row of rows) {
    const fields = row?.fields || {};
    const resolvedScheduleDate = resolveTripScheduleDate(fields);
    if (!resolvedScheduleDate) continue;

    const directScheduleShowDate = toIsoDateOnly(firstValue(fields.schedule_show_datev2));
    const directScheduledDate = strOrNull(firstValue(fields.scheduled_date));
    const patchFields = {};

    if (watchTripsFieldSet.has("schedule_show_datev2") && directScheduleShowDate !== resolvedScheduleDate) {
      patchFields.schedule_show_datev2 = resolvedScheduleDate;
    }

    if (watchTripsFieldSet.has("scheduled_date") && directScheduledDate !== resolvedScheduleDate) {
      patchFields.scheduled_date = resolvedScheduleDate;
    }

    if (Object.keys(patchFields).length) {
      updates.push({
        id: row.id,
        fields: patchFields,
      });
    }
  }

  return updates;
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

async function fetchJson(url) {
  const fetched = await fetchTextWithConfiguredTransport(url, async (endpoint) => {
    const response = await fetchWithRetry(endpoint, {
      method: "GET",
      headers: { Accept: "application/json" },
    });
    const text = await response.text().catch(() => "");
    return { response, text, endpoint };
  });

  const { response, text } = fetched;
  const endpoint = fetched.endpoint || url;
  if (!response.ok) throw new Error(`Fetch failed (${response.status}): ${text.slice(0, 1200)}`);
  let json = null;
  try {
    json = JSON.parse(text);
  } catch {
    throw new Error(`Response was not valid JSON. First 1200 chars:\n${text.slice(0, 1200)}`);
  }

  const isClassEndpoint = String(endpoint || "").includes("/classes/");
  assertValidPayload({
    payload: json,
    text,
    response,
    lane: "trips_dailyv2",
    endpoint,
    expectedTopLevelKeys: isClassEndpoint
      ? ["class", "class_related_data", "trips", "status", "class_id", "number", "total_trips"]
      : [],
    expectedPredicate: isClassEndpoint
      ? null
      : (payload) => Array.isArray(payload?.trips) ||
          collectTripCandidates(payload).length > 0 ||
          payload?.people !== undefined ||
          payload?.entries !== undefined ||
          payload?.classes !== undefined ||
          payload?.show_id !== undefined,
  });
  return json;
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

function linkOne(recordId) {
  return recordId ? [recordId] : undefined;
}

function pickWritableFields(fieldSet, values) {
  const fields = {};
  for (const [name, value] of Object.entries(values || {})) {
    if (!fieldSet.has(name)) continue;
    if (value === undefined || value === null) continue;
    if (typeof value === "string" && value.trim() === "") continue;
    fields[name] = value;
  }
  return fields;
}

function buildPeopleEndpoint(sourceId, heartbeat) {
  return `${BASE_URL}/people/${encodeURIComponent(sourceId)}?pid=${encodeURIComponent(sourceId)}&show_id=${encodeURIComponent(heartbeat.app_show_id)}&customer_id=${encodeURIComponent(CUSTOMER_ID)}`;
}

function buildClassesEndpoint(classId, showId, classGroupId = null) {
  return buildClassDetailEndpoint({
    baseUrl: BASE_URL,
    classId,
    showId,
    customerId: CUSTOMER_ID,
    classGroupId,
  });
}

function extractPeopleShowId(payload) {
  return numOrNull(pickFirst(payload?.show_id, payload?.people?.show_id));
}

function extractPayloadTrips(payload) {
  if (Array.isArray(payload?.trips)) return payload.trips;
  return collectTripCandidates(payload);
}

function normalizeClassEndpointPayload(payload) {
  const classObj = payload?.class && typeof payload.class === "object" ? payload.class : {};
  const related = payload?.class_related_data && typeof payload.class_related_data === "object"
    ? payload.class_related_data
    : {};

  return {
    class_id: numOrNull(pickFirst(payload?.class_id, classObj?.class_id)),
    class_number: normalizeEntryNumber(pickFirst(payload?.number, payload?.class_number, classObj?.number)),
    class_name: strOrNull(pickFirst(classObj?.name, payload?.class_name, payload?.name)),
    class_group_id: numOrNull(pickFirst(payload?.class_group_id, related?.class_group_id)),
    ring_number: numOrNull(pickFirst(payload?.ring, related?.ring, payload?.ring_number)),
    total_trips: numOrNull(pickFirst(payload?.total_trips, related?.total_trips)),
    status: strOrNull(pickFirst(payload?.status, related?.status)),
    class_type: strOrNull(pickFirst(payload?.class_type, classObj?.class_type)),
    schedule_sequencetype: strOrNull(pickFirst(payload?.schedule_sequencetype, classObj?.schedule_sequencetype)),
    show_id: numOrNull(pickFirst(payload?.show_id, classObj?.show_id)),
    schedule_date: toIsoDateOnly(pickFirst(payload?.date, related?.date)),
    scheduled_estimated_start_time: strOrNull(pickFirst(payload?.estimated_time, related?.estimated_time)),
  };
}

async function fetchClassEndpointDetailsById(classRows, heartbeat) {
  const detailById = new Map();
  const failures = [];
  const uniqueByKey = new Map();

  for (const item of classRows || []) {
    const classId = numOrNull(typeof item === "object" ? item?.class_id : item);
    const classGroupId = numOrNull(typeof item === "object" ? item?.class_group_id : null);
    if (classId === null) continue;
    const key = `${classId}|${classGroupId ?? ""}`;
    if (!uniqueByKey.has(key)) uniqueByKey.set(key, { classId, classGroupId });
  }

  await runPool([...uniqueByKey.values()], FETCH_CONCURRENCY, async ({ classId, classGroupId }) => {
    const endpoint = buildClassesEndpoint(classId, heartbeat.app_show_id, classGroupId);
    if (!endpoint) return;
    try {
      const payload = await fetchJson(endpoint);
      detailById.set(String(classId), normalizeClassEndpointPayload(payload));
    } catch (error) {
      failures.push({
        class_id: classId,
        class_group_id: classGroupId,
        endpoint,
        reason: String(error?.message || error).slice(0, 300),
      });
    }
  });

  return { detailById, failures };
}

function buildNumericRunId(nowIso) {
  const ms = Date.parse(nowIso);
  return Number.isFinite(ms) ? ms : Date.now();
}

async function upsertByNumberId({
  tableName,
  fieldSet,
  fieldId,
  fieldPid,
  fieldInactive,
  fieldRunId,
  fieldLastRun,
  fieldNewFlag,
  fieldPidMismatch,
  rows,
}) {
  if (!rows.length) {
    return {
      table: tableName,
      created_planned: 0,
      updated_planned: 0,
      writes: { created: 0, updated: 0, create_failures: [], update_failures: [] },
    };
  }

  const existingRows = await airtableList(tableName, {
    pageSize: 100,
    "fields[]": [fieldId, fieldPid],
  });

  const existingById = new Map();
  for (const row of existingRows) {
    const idNum = numOrNull(row?.fields?.[fieldId]);
    if (idNum === null) continue;
    if (!existingById.has(idNum)) existingById.set(idNum, row);
  }

  const creates = [];
  const updates = [];

  for (const row of rows) {
    const idNum = numOrNull(row?.[fieldId]);
    const pidNum = numOrNull(row?.[fieldPid]);
    if (idNum === null) continue;
    const existing = existingById.get(idNum);
    if (!existing) {
      creates.push({ fields: pickWritableFields(fieldSet, row) });
      continue;
    }

    const oldPid = numOrNull(existing?.fields?.[fieldPid]);
    const pidMismatch = oldPid !== null && pidNum !== null && oldPid !== pidNum;
    updates.push({
      id: existing.id,
      fields: pickWritableFields(fieldSet, {
        ...row,
        [fieldInactive]: false,
        [fieldRunId]: row[fieldRunId],
        [fieldLastRun]: row[fieldLastRun],
        [fieldNewFlag]: false,
        [fieldPidMismatch]: pidMismatch,
      }),
    });
  }

  const summary = {
    table: tableName,
    created_planned: creates.length,
    updated_planned: updates.length,
    writes: { created: 0, updated: 0, create_failures: [], update_failures: [] },
  };

  if (!DRY_RUN) {
    const createResult = await airtableCreateRecords(tableName, creates);
    const updateResult = await airtablePatchRecords(tableName, updates);
    summary.writes.created = createResult.okRows;
    summary.writes.updated = updateResult.okRows;
    summary.writes.create_failures = createResult.failedRows;
    summary.writes.update_failures = updateResult.failedRows;
  }

  return summary;
}

async function upsertActiveByKey({
  tableName,
  fieldSet,
  fieldKey,
  fieldPid,
  fieldAppSid,
  fieldInactive,
  fieldRunId,
  fieldLastRun,
  fieldNewFlag,
  fieldPidMismatch,
  rows,
  scopePid,
  scopeAppSid,
  scopeByPid = true,
  runId,
  lastRun,
}) {
  const existingRows = await airtableList(tableName, {
    pageSize: 100,
    "fields[]": [fieldKey, fieldPid, fieldAppSid],
  });

  const relevantExisting = existingRows.filter((row) => {
    const appSidNum = numOrNull(row?.fields?.[fieldAppSid]);
    if (appSidNum !== scopeAppSid) return false;
    if (!scopeByPid) return true;
    const pidNum = numOrNull(row?.fields?.[fieldPid]);
    return pidNum === scopePid;
  });

  const existingByKey = new Map();
  for (const row of relevantExisting) {
    const key = normalizeKey(row?.fields?.[fieldKey]);
    if (!key || existingByKey.has(key)) continue;
    existingByKey.set(key, row);
  }

  const keepKeys = new Set();
  const creates = [];
  const updates = [];

  for (const row of rows) {
    const key = normalizeKey(row?.[fieldKey]);
    if (!key) continue;
    keepKeys.add(key);
    const existing = existingByKey.get(key);
    if (!existing) {
      creates.push({ fields: pickWritableFields(fieldSet, row) });
      continue;
    }

    const oldPid = fieldPid ? numOrNull(existing?.fields?.[fieldPid]) : null;
    const currentPid = fieldPid ? numOrNull(row?.[fieldPid]) : null;
    const pidMismatch = fieldPid && oldPid !== null && currentPid !== null && oldPid !== currentPid;
    updates.push({
      id: existing.id,
      fields: pickWritableFields(fieldSet, {
        ...row,
        [fieldInactive]: row[fieldInactive] ?? false,
        [fieldRunId]: row[fieldRunId],
        [fieldLastRun]: row[fieldLastRun],
        [fieldNewFlag]: false,
        [fieldPidMismatch]: pidMismatch,
      }),
    });
  }

  const inactivations = [];
  for (const row of relevantExisting) {
    const key = normalizeKey(row?.fields?.[fieldKey]);
    if (!key || keepKeys.has(key)) continue;
    inactivations.push({
      id: row.id,
      fields: pickWritableFields(fieldSet, {
        [fieldInactive]: true,
        [fieldRunId]: runId,
        [fieldLastRun]: lastRun,
      }),
    });
  }

  const summary = {
    table: tableName,
    created_planned: creates.length,
    updated_planned: updates.length,
    inactivated_planned: inactivations.length,
    writes: {
      created: 0,
      updated: 0,
      inactivated: 0,
      create_failures: [],
      update_failures: [],
      inactivate_failures: [],
    },
  };

  if (!DRY_RUN) {
    const createResult = await airtableCreateRecords(tableName, creates);
    const updateResult = await airtablePatchRecords(tableName, updates);
    const inactivateResult = await airtablePatchRecords(tableName, inactivations);
    summary.writes.created = createResult.okRows;
    summary.writes.updated = updateResult.okRows;
    summary.writes.inactivated = inactivateResult.okRows;
    summary.writes.create_failures = createResult.failedRows;
    summary.writes.update_failures = updateResult.failedRows;
    summary.writes.inactivate_failures = inactivateResult.failedRows;
  }

  return summary;
}

async function upsertActiveGroups({
  tableName,
  fieldSet,
  rows,
  scopeAppSid,
  runId,
  lastRun,
}) {
  if (!fieldSet.size) {
    return {
      table: tableName,
      created_planned: 0,
      updated_planned: 0,
      inactivated_planned: 0,
      writes: {
        created: 0,
        updated: 0,
        inactivated: 0,
        create_failures: [],
        update_failures: [],
        inactivate_failures: [],
      },
      skipped: true,
    };
  }

  const existingRows = await airtableList(tableName, {
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
      fields: pickWritableFields(fieldSet, {
        inactive: true,
        run_id: runId,
        last_run: lastRun,
      }),
    });
  }

  const summary = {
    table: tableName,
    created_planned: creates.length,
    updated_planned: updates.length,
    inactivated_planned: inactivations.length,
    writes: {
      created: 0,
      updated: 0,
      inactivated: 0,
      create_failures: [],
      update_failures: [],
      inactivate_failures: [],
    },
    skipped: false,
  };

  if (DRY_RUN) return summary;

  const createResult = await airtableCreateRecords(tableName, creates);
  const updateResult = await airtablePatchRecords(tableName, updates);
  const inactivateResult = await airtablePatchRecords(tableName, inactivations);

  summary.writes.created = createResult.okRows;
  summary.writes.updated = updateResult.okRows;
  summary.writes.inactivated = inactivateResult.okRows;
  summary.writes.create_failures = createResult.failedRows;
  summary.writes.update_failures = updateResult.failedRows;
  summary.writes.inactivate_failures = inactivateResult.failedRows;

  return summary;
}

function normalizeLinkIds(value) {
  if (!Array.isArray(value)) return [];
  return [...new Set(value.map((item) => strOrNull(item)).filter(Boolean))].sort();
}

function sameLinkIds(left, right) {
  return JSON.stringify(normalizeLinkIds(left)) === JSON.stringify(normalizeLinkIds(right));
}

async function fetchScopedActiveRows(tableName, fieldNames, scopeAppSid) {
  const rows = await airtableList(tableName, {
    pageSize: 100,
    "fields[]": fieldNames,
  });

  return rows.filter((row) => numOrNull(row?.fields?.app_sid) === scopeAppSid);
}

async function syncActiveTableLinks({
  scopeAppSid,
  activeGroupsFieldSet,
  activeClassesFieldSet,
  activeEntriesFieldSet,
  entryClassKeysByEntryKey,
}) {
  const summary = {
    active_groups_to_classes: { planned: 0, updated: 0, failures: [] },
    active_classes_to_groups: { planned: 0, updated: 0, failures: [] },
    active_entries_to_classes: { planned: 0, updated: 0, failures: [] },
    active_classes_to_entries: { planned: 0, updated: 0, failures: [] },
    skipped: false,
  };

  const needsGroupClassLinks = activeGroupsFieldSet.has("active_classes");
  const needsClassGroupLinks = activeClassesFieldSet.has("active_groups");
  const needsEntryClassLinks = activeEntriesFieldSet.has("active_classes");
  const needsClassEntryLinks = activeClassesFieldSet.has("active_entries");

  if (!needsGroupClassLinks && !needsClassGroupLinks && !needsEntryClassLinks && !needsClassEntryLinks) {
    summary.skipped = true;
    return summary;
  }

  const [groupRows, classRows, entryRows] = await Promise.all([
    fetchScopedActiveRows(
      TABLE_ACTIVE_GROUPS,
      ["app_sid", "class_group_id", "inactive", "active_classes"],
      scopeAppSid
    ),
    fetchScopedActiveRows(
      TABLE_ACTIVE_CLASSES,
      ["app_sid", "class_id", "class_group_id", "inactive", "active_groups", "active_entries"],
      scopeAppSid
    ),
    fetchScopedActiveRows(
      TABLE_ACTIVE_ENTRIES,
      ["app_sid", "entry_id", "inactive", "active_classes"],
      scopeAppSid
    ),
  ]);

  const activeGroups = groupRows.filter((row) => !boolValue(row?.fields?.inactive));
  const activeClasses = classRows.filter((row) => !boolValue(row?.fields?.inactive));
  const activeEntries = entryRows.filter((row) => !boolValue(row?.fields?.inactive));

  const groupRecordIdByGroupId = new Map();
  for (const row of activeGroups) {
    const classGroupId = normalizeKey(row?.fields?.class_group_id);
    if (!classGroupId || groupRecordIdByGroupId.has(classGroupId)) continue;
    groupRecordIdByGroupId.set(classGroupId, row.id);
  }

  const classRecordIdByClassId = new Map();
  const classRecordIdsByGroupId = new Map();
  for (const row of activeClasses) {
    const classId = normalizeKey(row?.fields?.class_id);
    const classGroupId = normalizeKey(row?.fields?.class_group_id);
    if (classId && !classRecordIdByClassId.has(classId)) classRecordIdByClassId.set(classId, row.id);
    if (classGroupId) {
      if (!classRecordIdsByGroupId.has(classGroupId)) classRecordIdsByGroupId.set(classGroupId, new Set());
      classRecordIdsByGroupId.get(classGroupId).add(row.id);
    }
  }

  const entryRecordIdByEntryId = new Map();
  const entryRecordIdsByClassId = new Map();
  for (const row of activeEntries) {
    const entryId = normalizeKey(row?.fields?.entry_id);
    if (entryId && !entryRecordIdByEntryId.has(entryId)) entryRecordIdByEntryId.set(entryId, row.id);
  }
  for (const [entryKey, classKeySet] of entryClassKeysByEntryKey.entries()) {
    const entryRecordId = entryRecordIdByEntryId.get(entryKey);
    if (!entryRecordId) continue;
    for (const classKey of classKeySet) {
      if (!classKey) continue;
      if (!entryRecordIdsByClassId.has(classKey)) entryRecordIdsByClassId.set(classKey, new Set());
      entryRecordIdsByClassId.get(classKey).add(entryRecordId);
    }
  }

  const groupUpdates = [];
  if (needsGroupClassLinks) {
    for (const row of activeGroups) {
      const classGroupId = normalizeKey(row?.fields?.class_group_id);
      const desired = [...(classRecordIdsByGroupId.get(classGroupId) || new Set())].sort();
      const current = normalizeLinkIds(row?.fields?.active_classes);
      if (sameLinkIds(current, desired)) continue;
      summary.active_groups_to_classes.planned += 1;
      groupUpdates.push({ id: row.id, fields: { active_classes: desired } });
    }
  }

  const classUpdates = [];
  if (needsClassGroupLinks || needsClassEntryLinks) {
    for (const row of activeClasses) {
      const fields = {};
      if (needsClassGroupLinks) {
        const classGroupId = normalizeKey(row?.fields?.class_group_id);
        const desiredGroupLinks = groupRecordIdByGroupId.get(classGroupId) ? [groupRecordIdByGroupId.get(classGroupId)] : [];
        const currentGroupLinks = normalizeLinkIds(row?.fields?.active_groups);
        if (!sameLinkIds(currentGroupLinks, desiredGroupLinks)) {
          fields.active_groups = desiredGroupLinks;
          summary.active_classes_to_groups.planned += 1;
        }
      }
      if (needsClassEntryLinks) {
        const classId = normalizeKey(row?.fields?.class_id);
        const desiredEntryLinks = [...(entryRecordIdsByClassId.get(classId) || new Set())].sort();
        const currentEntryLinks = normalizeLinkIds(row?.fields?.active_entries);
        if (!sameLinkIds(currentEntryLinks, desiredEntryLinks)) {
          fields.active_entries = desiredEntryLinks;
          summary.active_classes_to_entries.planned += 1;
        }
      }
      if (Object.keys(fields).length) classUpdates.push({ id: row.id, fields });
    }
  }

  const entryUpdates = [];
  if (needsEntryClassLinks) {
    for (const row of activeEntries) {
      const entryId = normalizeKey(row?.fields?.entry_id);
      const desiredClassLinks = [...(entryClassKeysByEntryKey.get(entryId) || new Set())]
        .map((classKey) => classRecordIdByClassId.get(classKey))
        .filter(Boolean)
        .sort();
      const currentClassLinks = normalizeLinkIds(row?.fields?.active_classes);
      if (sameLinkIds(currentClassLinks, desiredClassLinks)) continue;
      summary.active_entries_to_classes.planned += 1;
      entryUpdates.push({ id: row.id, fields: { active_classes: desiredClassLinks } });
    }
  }

  if (DRY_RUN) return summary;

  if (groupUpdates.length) {
    const result = await airtablePatchRecords(TABLE_ACTIVE_GROUPS, groupUpdates);
    summary.active_groups_to_classes.updated = result.okRows;
    summary.active_groups_to_classes.failures = result.failedRows;
  }
  if (classUpdates.length) {
    const result = await airtablePatchRecords(TABLE_ACTIVE_CLASSES, classUpdates);
    summary.active_classes_to_groups.updated = result.okRows;
    summary.active_classes_to_entries.updated = result.okRows;
    summary.active_classes_to_groups.failures = result.failedRows;
    summary.active_classes_to_entries.failures = result.failedRows;
  }
  if (entryUpdates.length) {
    const result = await airtablePatchRecords(TABLE_ACTIVE_ENTRIES, entryUpdates);
    summary.active_entries_to_classes.updated = result.okRows;
    summary.active_entries_to_classes.failures = result.failedRows;
  }

  return summary;
}

async function buildAuxiliaryRowsForTenant({
  payload,
  tenantId,
  activeTenantRecordId,
  wwTrainerRecordId,
  heartbeat,
  showRecordId,
  runId,
  nowIso,
  dateOnly,
  scheduleByClassId,
  activeClassesFieldSet,
  activeEntriesFieldSet,
  activeGroupsFieldSet,
  wwRidersFieldSet,
  wwHorsesFieldSet,
}) {
  const trips = extractPayloadTrips(payload);
  const riderById = new Map();
  const horseById = new Map();
  const classById = new Map();
  const entryById = new Map();

  for (const trip of trips) {
    const riderId = numOrNull(pickFirst(trip?.rider_id, trip?.riderId));
    const riderName = strOrNull(pickFirst(trip?.rider_name, trip?.riderName));
    if (riderId !== null) {
      const existing = riderById.get(riderId) || { rider_id: riderId, rider_name: null };
      if (!existing.rider_name && riderName) existing.rider_name = riderName;
      riderById.set(riderId, existing);
    }

    const horseId = numOrNull(pickFirst(trip?.horse_id, trip?.horseId));
    const horseName = strOrNull(pickFirst(trip?.horse, trip?.Horse));
    const entryId = numOrNull(pickFirst(trip?.entry_id, trip?.entryId));
    const entryNumber = normalizeEntryNumber(pickFirst(trip?.entry_number, trip?.entryNumber, trip?.entry_no, trip?.entryNo, trip?.number));
    const rawClassGroupId = numOrNull(pickFirst(trip?.class_group_id, trip?.classGroupId));
    const rawGroupName = strOrNull(pickFirst(trip?.group_name, trip?.groupName));
    const rawClassGroupSequence = numOrNull(pickFirst(
      trip?.class_group_sequence,
      trip?.classGroupSequence,
      trip?.group_sequence,
      trip?.groupSequence
    ));
    const rawRingNumber = numOrNull(pickFirst(trip?.ring_number, trip?.ringNumber, trip?.ring));
    if (horseId !== null) {
      const existing = horseById.get(horseId) || {
        horse_id: horseId,
        horse: null,
        entry_id: null,
        entry_number: undefined,
      };
      if (!existing.horse && horseName) existing.horse = horseName;
      if (existing.entry_id === null && entryId !== null) existing.entry_id = entryId;
      if (existing.entry_number === undefined && entryNumber !== undefined) existing.entry_number = entryNumber;
      horseById.set(horseId, existing);
    }

    const classId = numOrNull(pickFirst(trip?.class_id, trip?.classId));
    const classNumber = normalizeEntryNumber(pickFirst(trip?.class_number, trip?.classNumber));
    const className = strOrNull(pickFirst(trip?.class_name, trip?.className));
    if (classId !== null) {
      const existing = classById.get(classId) || {
        class_id: classId,
        class_number: undefined,
        class_name: null,
        class_group_id: null,
        group_name: null,
        class_group_sequence: null,
        ring_number: null,
      };
      if (existing.class_number === undefined && classNumber !== undefined) existing.class_number = classNumber;
      if (!existing.class_name && className) existing.class_name = className;
       if (existing.class_group_id === null && rawClassGroupId !== null) existing.class_group_id = rawClassGroupId;
       if (!existing.group_name && rawGroupName) existing.group_name = rawGroupName;
       if (existing.class_group_sequence === null && rawClassGroupSequence !== null) existing.class_group_sequence = rawClassGroupSequence;
       if (existing.ring_number === null && rawRingNumber !== null) existing.ring_number = rawRingNumber;
      classById.set(classId, existing);
    }

    if (entryId !== null) {
      const existing = entryById.get(entryId) || {
        entry_id: entryId,
        entry_number: undefined,
        horse_id: null,
        horse: null,
        rider_id: null,
        rider_name: null,
        class_ids: new Set(),
        class_group_ids: new Set(),
      };
      if (existing.entry_number === undefined && entryNumber !== undefined) existing.entry_number = entryNumber;
      if (existing.horse_id === null && horseId !== null) existing.horse_id = horseId;
      if (!existing.horse && horseName) existing.horse = horseName;
      if (existing.rider_id === null && riderId !== null) existing.rider_id = riderId;
      if (!existing.rider_name && riderName) existing.rider_name = riderName;
      if (classId !== null) existing.class_ids.add(String(classId));
      if (rawClassGroupId !== null) existing.class_group_ids.add(String(rawClassGroupId));
      entryById.set(entryId, existing);
    }
  }

  const commonLinks = {
    shows: linkOne(showRecordId),
    active_tenants: linkOne(activeTenantRecordId),
    ww_trainers: linkOne(wwTrainerRecordId),
  };

  const classEndpointDetailResult = await fetchClassEndpointDetailsById([...classById.values()], heartbeat);
  const classEndpointDetails = classEndpointDetailResult.detailById;

  const riderRows = [...riderById.values()].map((row) => pickWritableFields(wwRidersFieldSet, {
    rider_id: row.rider_id,
    rider_name: row.rider_name || undefined,
    name: row.rider_name || undefined,
    pid: Number(tenantId),
    app_sid: heartbeat.app_show_id,
    app_sql_date: heartbeat.app_sql_date,
    shows: commonLinks.shows,
    active_tenants: commonLinks.active_tenants,
    ww_trainers: commonLinks.ww_trainers,
    inactive: false,
    run_id: runId,
    last_run: dateOnly,
    new_rider: true,
    pid_mismatch: false,
  }));

  const horseRows = [...horseById.values()].map((row) => pickWritableFields(wwHorsesFieldSet, {
    horse_id: row.horse_id,
    horse: row.horse || undefined,
    pid: Number(tenantId),
    trainer_id: Number(tenantId),
    app_sid: heartbeat.app_show_id,
    app_sql_date: heartbeat.app_sql_date,
    shows: commonLinks.shows,
    active_tenants: commonLinks.active_tenants,
    ww_trainers: commonLinks.ww_trainers,
    entry_id: row.entry_id ?? undefined,
    entry_number: row.entry_number,
    inactive: false,
    run_id: runId,
    last_run: dateOnly,
    new_horse: true,
    pid_mismatch: false,
  }));

  const enrichedClassRows = [...classById.values()].map((row) => {
    const schedule = scheduleByClassId.get(String(row.class_id));
    const classDetail = classEndpointDetails.get(String(row.class_id)) || null;
    const resolvedScheduleDate = classDetail?.schedule_date || schedule?.schedule_show_datev2;
    const resolvedEstimatedStart = classDetail?.scheduled_estimated_start_time || schedule?.estimated_start_time;
    const resolvedClassGroupId = classDetail?.class_group_id ?? row.class_group_id ?? schedule?.class_group_id;
    const resolvedRingNumber = classDetail?.ring_number ?? row.ring_number ?? schedule?.ring_number;
    const resolvedGroupName = row.group_name || schedule?.group_name || undefined;
    const resolvedClassGroupSequence = row.class_group_sequence ?? schedule?.class_group_sequence;
    const classMatchesAppDate = resolvedScheduleDate === heartbeat.app_sql_date;
    return {
      key: String(row.class_id),
      app_sid: heartbeat.app_show_id,
      app_sql_date: heartbeat.app_sql_date,
      app_show_idv2: heartbeat.app_show_id,
      app_sql_datev2: heartbeat.app_sql_date,
      app_dow_rawv2: heartbeat.app_dow_raw,
      is_today: classMatchesAppDate,
      " is_today": classMatchesAppDate,
      shows: commonLinks.shows,
      pid: Number(tenantId),
      class_id: row.class_id,
      class_number: classDetail?.class_number ?? row.class_number,
      class_name: classDetail?.class_name || row.class_name || undefined,
      watch_schedule: schedule?.recordId ? linkOne(schedule.recordId) : undefined,
      class_group_id: resolvedClassGroupId,
      group_name: resolvedGroupName,
      class_group_sequence: resolvedClassGroupSequence,
      ring_number: resolvedRingNumber,
      total_trips: classDetail?.total_trips ?? schedule?.total_trips,
      status: classDetail?.status || undefined,
      class_type: classDetail?.class_type ?? schedule?.class_type,
      schedule_sequencetype: classDetail?.schedule_sequencetype ?? schedule?.schedule_sequencetype,
      show_id: classDetail?.show_id ?? heartbeat.app_show_id,
      schedule_date: resolvedScheduleDate || undefined,
      scheduled_date: resolvedScheduleDate || undefined,
      scheduled_estimated_start_time: resolvedEstimatedStart || undefined,
      inactive: false,
      run_id: runId,
      last_run: dateOnly,
      new_class_id: true,
      pid_mismatch: false,
    };
  });

  const classRows = enrichedClassRows.map((row) => pickWritableFields(activeClassesFieldSet, row));

  const groupById = new Map();
  for (const row of enrichedClassRows) {
    const classGroupId = numOrNull(row.class_group_id);
    if (classGroupId === null) continue;

    const key = String(classGroupId);
    const existing = groupById.get(key) || {
      key,
      app_sid: heartbeat.app_show_id,
      app_sql_date: heartbeat.app_sql_date,
      shows: commonLinks.shows,
      class_group_id: classGroupId,
      class_group_sequence: undefined,
      group_name: null,
      ring_number: undefined,
      estimated_start_time: null,
      estimated_end_time: null,
      total: 0,
      inactive: false,
      run_id: runId,
      last_run: dateOnly,
    };

    const totalTrips = numOrNull(row.total_trips);
    existing.class_group_sequence = existing.class_group_sequence ?? numOrNull(row.class_group_sequence);
    existing.group_name = existing.group_name || strOrNull(row.group_name);
    existing.ring_number = existing.ring_number ?? numOrNull(row.ring_number);
    existing.estimated_start_time = minTimeText(existing.estimated_start_time, strOrNull(row.scheduled_estimated_start_time ?? row.estimated_start_time));
    existing.estimated_end_time = maxTimeText(existing.estimated_end_time, strOrNull(row.estimated_end_time));
    existing.total += totalTrips ?? 0;
    groupById.set(key, existing);
  }

  const groupRows = [...groupById.values()].map((row) => pickWritableFields(activeGroupsFieldSet, row));

  const entryRows = [...entryById.values()].map((row) => {
    const singleClassId = row.class_ids.size === 1 ? numOrNull([...row.class_ids][0]) : null;
    const singleClassGroupId = row.class_group_ids.size === 1 ? numOrNull([...row.class_group_ids][0]) : null;

    return pickWritableFields(activeEntriesFieldSet, {
      key: String(row.entry_id),
      app_sid: heartbeat.app_show_id,
      app_sql_date: heartbeat.app_sql_date,
      shows: commonLinks.shows,
      pid: Number(tenantId),
      entry_id: row.entry_id,
      entry_number: row.entry_number,
      horse_id: row.horse_id ?? undefined,
      horse: row.horse || undefined,
      rider_id: row.rider_id ?? undefined,
      rider_name: row.rider_name || undefined,
      class_id: singleClassId ?? undefined,
      class_group_id: singleClassGroupId ?? undefined,
      ww_trainers: commonLinks.ww_trainers,
      inactive: false,
      run_id: runId,
      last_run: dateOnly,
      new_entry: true,
      pid_mismatch: false,
    });
  });

  return {
    tripCount: trips.length,
    riderRows,
    horseRows,
    classRows,
    groupRows,
    entryRows,
    entryClassLinks: [...entryById.values()].map((row) => ({
      entry_key: String(row.entry_id),
      class_keys: [...row.class_ids].map((value) => normalizeKey(value)).filter(Boolean),
    })),
    classEndpointFailures: classEndpointDetailResult.failures,
  };
}

function buildCurrentFields(row, heartbeat, showRecordId, nowIso, dateOnly, currentScopeStatus, watchTripsFieldSet) {
  const fields = {};
  const resolvedScheduleDate = resolveTripScheduleDate(row);
  const resolvedScheduledDate = resolvedScheduleDate;
  const isActiveForScope = resolvedScheduledDate === heartbeat.app_sql_date;
  const maybeSet = (name, value) => {
    if (!watchTripsFieldSet.has(name)) return;
    setIfPresent(fields, name, value);
  };

  maybeSet("heartbeat", [heartbeat.recordId]);
  maybeSet("shows", showRecordId ? [showRecordId] : undefined);
  maybeSet("watch_schedule", row.watch_schedule_record_id ? [row.watch_schedule_record_id] : undefined);
  maybeSet("entryxclasses_uuid", row.entryxclasses_uuid);
  maybeSet("show_id", heartbeat.app_show_id);
  maybeSet("show_date", resolvedScheduleDate || heartbeat.app_sql_date);
  maybeSet("app_show_id", heartbeat.app_show_id);
  maybeSet("app_sql_date", heartbeat.app_sql_date);
  maybeSet("app_sid", heartbeat.app_show_id);
  maybeSet("app_dt", heartbeat.app_sql_date);
  maybeSet("app_time", heartbeat.app_time);
  maybeSet("mode", heartbeat.mode);
  maybeSet("date", heartbeat.app_sql_date);
  maybeSet("dow_raw", heartbeat.app_dow_raw);
  maybeSet("shifted_to_next_day", heartbeat.shifted_to_next_day);
  maybeSet("app_show_idv2", heartbeat.app_show_id);
  maybeSet("app_sql_datev2", heartbeat.app_sql_date);
  maybeSet("app_dow_rawv2", heartbeat.app_dow_raw);
  maybeSet("shifted_to_next_dayv2", heartbeat.shifted_to_next_day);
  maybeSet("scope_run_id", heartbeat.scope_run_id);
  maybeSet("is_current_scope", true);
  maybeSet("scope_status", currentScopeStatus);
  maybeSet("inactive", !isActiveForScope);
  maybeSet("last_seen_at", dateOnly);
  maybeSet("dropped_at", null);
  maybeSet("run_id", heartbeat.scope_run_id);
  maybeSet("run_time", nowIso);
  maybeSet("pid", row.pid);
  maybeSet("entry_id", row.entry_id);
  maybeSet("entry_number", row.entry_number);
  maybeSet("horse", row.horse);
  maybeSet("class_id", row.class_id);
  maybeSet("class_number", row.class_number);
  maybeSet("class_name", row.class_name);
  maybeSet("schedule_sequencetype", row.schedule_sequencetype);
  maybeSet("class_type", row.class_type);
  maybeSet("class_group_id", row.class_group_id);
  maybeSet("group_name", row.group_name);
  maybeSet("class_groupxclasses_id", row.class_groupxclasses_id);
  maybeSet("ring_number", row.ring_number);
  maybeSet("estimated_start_time", row.estimated_start_time);
  maybeSet("estimated_end_time", row.estimated_end_time);
  maybeSet("latest_estimated_start_time", row.estimated_start_time);
  maybeSet("latest_ingested_at", nowIso);
  maybeSet("class_group_sequence", row.class_group_sequence);
  maybeSet("class_endpoint", buildClassesEndpoint(row.class_id, heartbeat.app_show_id, row.class_group_id));
  maybeSet("status", row.status);
  maybeSet("order_of_go", row.order_of_go);
  maybeSet("rider_name", row.rider_name);
  maybeSet("rider_id", row.rider_id);
  maybeSet("placing", row.placing);
  maybeSet("schedule_show_datev2", resolvedScheduleDate);
  maybeSet("scheduled_date", resolvedScheduledDate);
  maybeSet("is_missing", false);

  return fields;
}

function buildDroppedFields(heartbeat, nowIso, dateOnly, droppedScopeStatus, watchTripsFieldSet) {
  const fields = {};
  const maybeSet = (name, value) => {
    if (!watchTripsFieldSet.has(name)) return;
    setIfPresent(fields, name, value);
  };

  maybeSet("heartbeat", []);
  maybeSet("is_current_scope", false);
  maybeSet("scope_status", droppedScopeStatus);
  maybeSet("inactive", true);
  maybeSet("dropped_at", dateOnly);
  maybeSet("run_id", heartbeat.scope_run_id);
  maybeSet("run_time", nowIso);
  maybeSet("last_seen_at", dateOnly);

  return fields;
}

function rowScheduledDateMatchesScope(row, heartbeat) {
  const resolvedScheduledDate = resolveTripScheduleDate(row);
  return resolvedScheduledDate === heartbeat.app_sql_date;
}

async function main() {
  requireEnv("AIRTABLE_TOKEN", AIRTABLE_TOKEN);
  requireEnv("AIRTABLE_BASE_ID", AIRTABLE_BASE_ID);

  const nowIso = new Date().toISOString();
  const dateOnly = nowIso.slice(0, 10);
  const runId = buildNumericRunId(nowIso);

  const heartbeat = await fetchLatestHeartbeat();
  const [
    watchTripsFieldSet,
    scopeStatusChoices,
    showRecordId,
    scheduleRows,
    activeTenantRows,
    wwTrainerRecordIdByPid,
    activeClassesFieldSet,
    activeEntriesFieldSet,
    activeGroupsFieldSet,
    wwRidersFieldSet,
    wwHorsesFieldSet,
  ] = await Promise.all([
    fetchTableFieldSet(TABLE_WATCH_TRIPS),
    fetchScopeStatusChoices(TABLE_WATCH_TRIPS).catch(() => new Set()),
    fetchShowRecordId(heartbeat.app_show_id).catch(() => null),
    fetchWatchScheduleRows(),
    fetchActiveTenantRows(),
    fetchWwTrainerRecordIdByPid().catch(() => new Map()),
    fetchTableFieldSet(TABLE_ACTIVE_CLASSES),
    fetchTableFieldSet(TABLE_ACTIVE_ENTRIES),
    fetchTableFieldSet(TABLE_ACTIVE_GROUPS).catch(() => new Set()),
    fetchTableFieldSet(TABLE_WW_RIDERS),
    fetchTableFieldSet(TABLE_WW_HORSES),
  ]);

  const currentScopeStatus = scopeStatusChoices.has("current") ? "current" : null;
  const droppedScopeStatus = scopeStatusChoices.has("dropped") ? "dropped" : null;
  const scheduleByClassId = buildScheduleMap(scheduleRows);
  const activeTenantMap = new Map();
  for (const row of activeTenantRows) {
    if (!row?.tenant_id || activeTenantMap.has(row.tenant_id)) continue;
    activeTenantMap.set(row.tenant_id, row);
  }
  const activeTenantIds = [...activeTenantMap.keys()];

  if (!activeTenantIds.length) {
    console.log(JSON.stringify({
      ok: true,
      run_status: "NOOP",
      reason: "No active tenant_id values found from active_tenants view",
      app_show_id: heartbeat.app_show_id,
      app_sql_date: heartbeat.app_sql_date,
    }));
    return;
  }

  const peopleFailures = [];
  const tenantSummaries = [];
  const normalizedRows = [];
  const outsideSchedule = [];
  const uniqueRows = new Map();
  const uniqueGroupRows = new Map();
  const uniqueClassRows = new Map();
  const uniqueEntryRows = new Map();
  const entryClassKeysByEntryKey = new Map();

  const preparedTenants = [];
  const softPayloadSamples = [];

  for (const tenantId of activeTenantIds) {
    const tenantRow = activeTenantMap.get(tenantId) || null;
    const endpoint = buildPeopleEndpoint(tenantId, heartbeat);
    let payload = null;
    try {
      payload = await fetchJson(endpoint);
    } catch (error) {
      const softPayload = isSoftPayloadError(error);
      peopleFailures.push({
        tenant_id: tenantId,
        endpoint,
        reason: String(error?.message || error).slice(0, 300),
        soft_payload: softPayload,
      });
      tenantSummaries.push({
        tenant_id: tenantId,
        endpoint,
        status: softPayload ? "soft_payload_blocked" : "fetch_failed",
      });
      if (softPayload) {
        softPayloadSamples.push({
          tenant_id: tenantId,
          endpoint,
          reason: error?.reason || "soft_payload",
          body_length: error?.body_length ?? null,
          content_length: error?.content_length ?? null,
        });
      }
      continue;
    }

    const peopleShowId = extractPeopleShowId(payload);
    if (peopleShowId !== null && peopleShowId !== heartbeat.app_show_id) {
      peopleFailures.push({
        tenant_id: tenantId,
        endpoint,
        reason: `people_show_id_conflict:${peopleShowId}`,
      });
      tenantSummaries.push({
        tenant_id: tenantId,
        endpoint,
        status: "show_id_conflict",
        people_show_id: peopleShowId,
      });
      continue;
    }

    const wwTrainerRecordId = wwTrainerRecordIdByPid.get(String(tenantId)) || null;
    const auxiliaryRows = await buildAuxiliaryRowsForTenant({
      payload,
      tenantId,
      activeTenantRecordId: tenantRow?.recordId || null,
      wwTrainerRecordId,
      heartbeat,
      showRecordId,
      runId,
      nowIso,
      dateOnly,
      scheduleByClassId,
      activeClassesFieldSet,
      activeEntriesFieldSet,
      activeGroupsFieldSet,
      wwRidersFieldSet,
      wwHorsesFieldSet,
    });

    const classSoftFailures = (auxiliaryRows.classEndpointFailures || [])
      .filter((failure) => /soft_payload_/i.test(String(failure?.reason || "")));
    if (classSoftFailures.length) {
      softPayloadSamples.push(...classSoftFailures.slice(0, 5).map((failure) => ({
        tenant_id: tenantId,
        endpoint: failure.endpoint,
        reason: failure.reason,
      })));
      tenantSummaries.push({
        tenant_id: tenantId,
        endpoint,
        status: "soft_payload_blocked",
        class_endpoint_failures: classSoftFailures,
      });
      continue;
    }

    preparedTenants.push({
      tenantId,
      endpoint,
      payload,
      auxiliaryRows,
    });
  }

  if (softPayloadSamples.length) {
    console.log(JSON.stringify({
      ok: false,
      dry_run: DRY_RUN,
      run_status: "SOFT_PAYLOAD_BLOCKED",
      reason: "soft_payload_empty",
      app_show_id: heartbeat.app_show_id,
      app_sql_date: heartbeat.app_sql_date,
      active_tenant_ids: activeTenantIds.length,
      people_failures: peopleFailures,
      tenant_summaries: tenantSummaries,
      soft_payload_samples: softPayloadSamples.slice(0, 10),
      writes_blocked: true,
    }, null, 2));
    process.exitCode = 1;
    return;
  }

  for (const prepared of preparedTenants) {
    const { tenantId, endpoint, payload, auxiliaryRows } = prepared;

    const riderSync = await upsertByNumberId({
      tableName: TABLE_WW_RIDERS,
      fieldSet: wwRidersFieldSet,
      fieldId: "rider_id",
      fieldPid: "pid",
      fieldInactive: "inactive",
      fieldRunId: "run_id",
      fieldLastRun: "last_run",
      fieldNewFlag: "new_rider",
      fieldPidMismatch: "pid_mismatch",
      rows: auxiliaryRows.riderRows,
    });

    const horseSync = await upsertByNumberId({
      tableName: TABLE_WW_HORSES,
      fieldSet: wwHorsesFieldSet,
      fieldId: "horse_id",
      fieldPid: "pid",
      fieldInactive: "inactive",
      fieldRunId: "run_id",
      fieldLastRun: "last_run",
      fieldNewFlag: "new_horse",
      fieldPidMismatch: "pid_mismatch",
      rows: auxiliaryRows.horseRows,
    });

    for (const row of auxiliaryRows.classRows || []) {
      const key = normalizeKey(row?.key);
      if (!key) continue;
      const existing = uniqueClassRows.get(key);
      if (!existing) {
        uniqueClassRows.set(key, { ...row });
        continue;
      }
      existing.app_show_idv2 = existing.app_show_idv2 ?? row.app_show_idv2;
      existing.app_sql_datev2 = existing.app_sql_datev2 ?? row.app_sql_datev2;
      existing.app_dow_rawv2 = existing.app_dow_rawv2 ?? row.app_dow_rawv2;
      existing.is_today = Boolean(existing.is_today) || Boolean(row.is_today);
      existing[" is_today"] = Boolean(existing[" is_today"]) || Boolean(row[" is_today"]);
      existing.class_number = existing.class_number ?? row.class_number;
      existing.class_name = existing.class_name || row.class_name;
      existing.watch_schedule = existing.watch_schedule || row.watch_schedule;
      existing.class_group_id = existing.class_group_id ?? row.class_group_id;
      existing.group_name = existing.group_name || row.group_name;
      existing.class_group_sequence = existing.class_group_sequence ?? row.class_group_sequence;
      existing.ring_number = existing.ring_number ?? row.ring_number;
      existing.total_trips = existing.total_trips ?? row.total_trips;
      existing.status = existing.status || row.status;
      existing.class_type = existing.class_type || row.class_type;
      existing.schedule_sequencetype = existing.schedule_sequencetype || row.schedule_sequencetype;
      existing.show_id = existing.show_id ?? row.show_id;
      existing.schedule_date = existing.schedule_date || row.schedule_date;
      existing.scheduled_date = existing.scheduled_date || row.scheduled_date;
      existing.scheduled_estimated_start_time = existing.scheduled_estimated_start_time || row.scheduled_estimated_start_time;
      existing.inactive = Boolean(existing.inactive) && Boolean(row.inactive);
      if (!sameValue(existing.pid, row.pid)) existing.pid_mismatch = true;
    }

    for (const row of auxiliaryRows.entryRows || []) {
      const key = normalizeKey(row?.key);
      if (!key) continue;
      const existing = uniqueEntryRows.get(key);
      if (!existing) {
        uniqueEntryRows.set(key, { ...row });
        continue;
      }
      existing.entry_number = existing.entry_number ?? row.entry_number;
      existing.horse_id = existing.horse_id ?? row.horse_id;
      existing.horse = existing.horse || row.horse;
      existing.rider_id = existing.rider_id ?? row.rider_id;
      existing.rider_name = existing.rider_name || row.rider_name;
      if (!sameValue(existing.pid, row.pid)) existing.pid_mismatch = true;
    }

    for (const row of auxiliaryRows.entryClassLinks || []) {
      const entryKey = normalizeKey(row?.entry_key);
      if (!entryKey) continue;
      const classKeySet = entryClassKeysByEntryKey.get(entryKey) || new Set();
      for (const classKey of row?.class_keys || []) {
        const normalizedClassKey = normalizeKey(classKey);
        if (normalizedClassKey) classKeySet.add(normalizedClassKey);
      }
      entryClassKeysByEntryKey.set(entryKey, classKeySet);
    }

    for (const row of auxiliaryRows.groupRows || []) {
      const key = normalizeKey(row?.key);
      if (!key) continue;
      const existing = uniqueGroupRows.get(key);
      if (!existing) {
        uniqueGroupRows.set(key, { ...row });
        continue;
      }

      existing.class_group_sequence = existing.class_group_sequence ?? numOrNull(row.class_group_sequence);
      existing.group_name = existing.group_name || strOrNull(row.group_name);
      existing.ring_number = existing.ring_number ?? numOrNull(row.ring_number);
      existing.estimated_start_time = minTimeText(existing.estimated_start_time, strOrNull(row.estimated_start_time));
      existing.estimated_end_time = maxTimeText(existing.estimated_end_time, strOrNull(row.estimated_end_time));
      existing.total = (numOrNull(existing.total) ?? 0) + (numOrNull(row.total) ?? 0);
    }

    const perTenantNormalized = normalizeTripsForScope({
      sourceIds: [tenantId],
      peoplePayloads: new Map([[tenantId, payload]]),
      scheduleByClassId,
    });

    normalizedRows.push(...perTenantNormalized.normalized_rows);
    outsideSchedule.push(...perTenantNormalized.outside_schedule);
    for (const [key, row] of perTenantNormalized.unique_rows_by_key.entries()) {
      if (!uniqueRows.has(key)) uniqueRows.set(key, row);
    }

    tenantSummaries.push({
      tenant_id: tenantId,
      endpoint,
      trip_count: auxiliaryRows.tripCount,
      empty_payload: auxiliaryRows.tripCount === 0,
      watch_trip_rows: perTenantNormalized.unique_row_count,
      outside_schedule_count: perTenantNormalized.outside_schedule.length,
      rider_sync: riderSync,
      horse_sync: horseSync,
      active_class_rows: auxiliaryRows.classRows.length,
      active_entry_rows: auxiliaryRows.entryRows.length,
      class_endpoint_failures: auxiliaryRows.classEndpointFailures,
    });
  }

  const classSync = await upsertActiveByKey({
    tableName: TABLE_ACTIVE_CLASSES,
    fieldSet: activeClassesFieldSet,
    fieldKey: "key",
    fieldPid: "pid",
    fieldAppSid: "app_sid",
    fieldInactive: "inactive",
    fieldRunId: "run_id",
    fieldLastRun: "last_run",
    fieldNewFlag: "new_class_id",
    fieldPidMismatch: "pid_mismatch",
    rows: [...uniqueClassRows.values()],
    scopePid: null,
    scopeAppSid: heartbeat.app_show_id,
    scopeByPid: false,
    runId,
    lastRun: dateOnly,
  });

  const entrySync = await upsertActiveByKey({
    tableName: TABLE_ACTIVE_ENTRIES,
    fieldSet: activeEntriesFieldSet,
    fieldKey: "key",
    fieldPid: "pid",
    fieldAppSid: "app_sid",
    fieldInactive: "inactive",
    fieldRunId: "run_id",
    fieldLastRun: "last_run",
    fieldNewFlag: "new_entry",
    fieldPidMismatch: "pid_mismatch",
    rows: [...uniqueEntryRows.values()],
    scopePid: null,
    scopeAppSid: heartbeat.app_show_id,
    scopeByPid: false,
    runId,
    lastRun: dateOnly,
  });

  const activeGroupSync = await upsertActiveGroups({
    tableName: TABLE_ACTIVE_GROUPS,
    fieldSet: activeGroupsFieldSet,
    rows: [...uniqueGroupRows.values()],
    scopeAppSid: heartbeat.app_show_id,
    runId,
    lastRun: dateOnly,
  });

  const activeLinkSync = await syncActiveTableLinks({
    scopeAppSid: heartbeat.app_show_id,
    activeGroupsFieldSet,
    activeClassesFieldSet,
    activeEntriesFieldSet,
    entryClassKeysByEntryKey,
  });

  const emptyTenantIds = tenantSummaries
    .filter((item) => item.empty_payload)
    .map((item) => item.tenant_id);

  const existingRows = await fetchExistingTripsForShow(heartbeat.app_show_id);
  const heartbeatViewRows = await fetchHeartbeatViewTripRows().catch(() => []);
  const heartbeatViewIdSet = new Set(heartbeatViewRows.map((row) => row.id));

  const groupedExisting = new Map();
  for (const row of existingRows) {
    const key = normalizeKey(row?.fields?.entryxclasses_uuid);
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
  const scopedRows = [...uniqueRows.values()].filter((row) => rowScheduledDateMatchesScope(row, heartbeat));

  for (const row of scopedRows) {
    const key = normalizeKey(row.entryxclasses_uuid);
    if (!key) continue;
    keepKeySet.add(key);
    const existing = existingByKey.get(key);
    const fields = buildCurrentFields(row, heartbeat, showRecordId, nowIso, dateOnly, currentScopeStatus, watchTripsFieldSet);
    if (existing) updateRecords.push({ id: existing.id, fields });
    else createRecords.push({ fields });
  }

  if (!scopedRows.length) {
    const dropUpdates = [];
    for (const row of heartbeatViewRows) {
      dropUpdates.push({
        id: row.id,
        fields: buildDroppedFields(heartbeat, nowIso, dateOnly, droppedScopeStatus, watchTripsFieldSet),
      });
    }

    const emptySummary = {
      ok: true,
      dry_run: DRY_RUN,
      run_status: "NOOP",
      reason: "No people trips matched current watch_schedule class ids",
      app_show_id: heartbeat.app_show_id,
      app_sql_date: heartbeat.app_sql_date,
      active_tenant_ids: activeTenantIds.length,
      watch_schedule_classes: scheduleByClassId.size,
      normalized_rows: normalizedRows.length,
      filtered_out_scheduled_date_mismatch: uniqueRows.size,
      outside_schedule_count: outsideSchedule.length,
      empty_tenant_ids: emptyTenantIds,
      tenant_summaries: tenantSummaries,
      people_failures: peopleFailures,
      drops_planned: dropUpdates.length,
      active_groups: activeGroupSync,
      active_links: activeLinkSync,
      writes: {
        created: 0,
        updated: 0,
        dropped: 0,
        create_failures: [],
        update_failures: [],
        drop_failures: [],
      },
      schedule_date_backfill: {
        planned: 0,
        updated: 0,
        failures: [],
      },
    };

    if (!DRY_RUN) {
      const dropResult = await airtablePatchRecords(TABLE_WATCH_TRIPS, dropUpdates);
      const backfillRows = await fetchTripScheduleBackfillRows(heartbeat.app_show_id);
      const backfillUpdates = buildTripScheduleBackfillUpdates(backfillRows, watchTripsFieldSet);
      const backfillResult = await airtablePatchRecords(TABLE_WATCH_TRIPS, backfillUpdates);
      emptySummary.writes.dropped = dropResult.okRows;
      emptySummary.writes.drop_failures = dropResult.failedRows;
      emptySummary.schedule_date_backfill.planned = backfillUpdates.length;
      emptySummary.schedule_date_backfill.updated = backfillResult.okRows;
      emptySummary.schedule_date_backfill.failures = backfillResult.failedRows;
    } else {
      const backfillRows = await fetchTripScheduleBackfillRows(heartbeat.app_show_id);
      const backfillUpdates = buildTripScheduleBackfillUpdates(backfillRows, watchTripsFieldSet);
      emptySummary.schedule_date_backfill.planned = backfillUpdates.length;
    }

    console.log(JSON.stringify(emptySummary, null, 2));
    return;
  }

  const dropUpdates = [];
  for (const row of heartbeatViewRows) {
    const key = normalizeKey(row?.fields?.entryxclasses_uuid);
    if (!key || keepKeySet.has(key)) continue;
    dropUpdates.push({
      id: row.id,
      fields: buildDroppedFields(heartbeat, nowIso, dateOnly, droppedScopeStatus, watchTripsFieldSet),
    });
  }

  const summary = {
    ok: true,
    dry_run: DRY_RUN,
    app_show_id: heartbeat.app_show_id,
    app_sql_date: heartbeat.app_sql_date,
    app_dow_raw: heartbeat.app_dow_raw,
    shifted_to_next_day: heartbeat.shifted_to_next_day,
    mode: heartbeat.mode,
    active_tenant_ids: activeTenantIds.length,
    watch_schedule_rows: scheduleRows.length,
    watch_schedule_classes: scheduleByClassId.size,
    normalized_rows: normalizedRows.length,
    unique_rows: scopedRows.length,
    filtered_out_scheduled_date_mismatch: uniqueRows.size - scopedRows.length,
    people_failures: peopleFailures,
    empty_tenant_ids: emptyTenantIds,
    tenant_summaries: tenantSummaries,
    outside_schedule_count: outsideSchedule.length,
    active_groups: activeGroupSync,
    active_links: activeLinkSync,
    creates_planned: createRecords.length,
    updates_planned: updateRecords.length,
    drops_planned: dropUpdates.length,
    existing_show_rows: existingRows.length,
    heartbeat_view_rows: heartbeatViewRows.length,
    writes: {
      created: 0,
      updated: 0,
      dropped: 0,
      create_failures: [],
      update_failures: [],
      drop_failures: [],
    },
    schedule_date_backfill: {
      planned: 0,
      updated: 0,
      failures: [],
    },
  };

  const backfillRows = await fetchTripScheduleBackfillRows(heartbeat.app_show_id);
  const backfillUpdates = buildTripScheduleBackfillUpdates(backfillRows, watchTripsFieldSet);
  summary.schedule_date_backfill.planned = backfillUpdates.length;

  if (!DRY_RUN) {
    const createResult = await airtableCreateRecords(TABLE_WATCH_TRIPS, createRecords);
    const updateResult = await airtablePatchRecords(TABLE_WATCH_TRIPS, updateRecords);
    const dropResult = await airtablePatchRecords(TABLE_WATCH_TRIPS, dropUpdates);
    const backfillResult = await airtablePatchRecords(TABLE_WATCH_TRIPS, backfillUpdates);
    summary.writes.created = createResult.okRows;
    summary.writes.updated = updateResult.okRows;
    summary.writes.dropped = dropResult.okRows;
    summary.writes.create_failures = createResult.failedRows;
    summary.writes.update_failures = updateResult.failedRows;
    summary.writes.drop_failures = dropResult.failedRows;
    summary.schedule_date_backfill.updated = backfillResult.okRows;
    summary.schedule_date_backfill.failures = backfillResult.failedRows;
  }

  console.log(JSON.stringify(summary, null, 2));
}

main().catch((error) => {
  const message = String(error?.stack || error?.message || error);
  console.error(JSON.stringify({
    ok: false,
    error: message.slice(0, 4000),
  }));
  process.exitCode = 1;
});
