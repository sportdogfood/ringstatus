const AIRTABLE_TOKEN = process.env.AIRTABLE_TOKEN || "";
const AIRTABLE_BASE_ID = process.env.AIRTABLE_BASE_ID || "";
const CUSTOMER_ID = Number(process.env.CUSTOMER_ID || "15");

const BASE_URL = String(process.env.BASE_URL || "https://broad-tooth-b8ed.gombcg.workers.dev").trim().replace(/\/+$/, "");

const TABLE_HEARTBEAT = process.env.TABLE_HEARTBEAT || "heartbeat";
const TABLE_SHOWS = process.env.TABLE_SHOWS || "shows";
const TABLE_WATCH_SCHEDULE = process.env.TABLE_WATCH_SCHEDULE || "watch_schedule";
const TABLE_WATCH_TRIPS = process.env.TABLE_WATCH_TRIPS || "watch_trips";
const TABLE_ACTIVE_TENANTS = process.env.TABLE_ACTIVE_TENANTS || "active_tenants";
const TABLE_WW_TRAINERS = process.env.TABLE_WW_TRAINERS || "ww_trainers";

const VIEW_WATCH_SCHEDULE = process.env.VIEW_WATCH_SCHEDULE || "heartbeat";
const VIEW_WATCH_TRIPS = process.env.VIEW_WATCH_TRIPS || "heartbeat";
const VIEW_ACTIVE_TENANTS = process.env.VIEW_ACTIVE_TENANTS || "heartbeat";
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

function setIfPresent(target, fieldName, value) {
  if (!fieldName) return;
  if (value === undefined || value === null) return;
  if (typeof value === "string" && value.trim() === "") return;
  target[fieldName] = value;
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
    "fields[]": ["pid", "tenant_active", "heartbeat", "ww_trainers"],
  });

  return rows
    .filter((row) => boolValue(row?.fields?.tenant_active))
    .map((row) => ({
      tenant_pid: normalizePidToken(row?.fields?.pid),
      trainer_links: Array.isArray(row?.fields?.ww_trainers)
        ? row.fields.ww_trainers.map((id) => String(id).trim()).filter(Boolean)
        : [],
    }));
}

async function fetchWwTrainerPidByRecordId() {
  const rows = await airtableList(TABLE_WW_TRAINERS, {
    pageSize: 100,
    "fields[]": ["pid"],
  });

  return new Map(
    rows
      .map((row) => [row.id, normalizePidToken(row?.fields?.pid)])
      .filter(([, pid]) => Boolean(pid))
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

function buildScheduleMap(rows) {
  const byClassId = new Map();

  for (const record of rows) {
    const fields = record?.fields || {};
    const classId = numOrNull(fields.class_id);
    if (classId === null) continue;
    byClassId.set(String(classId), {
      recordId: record.id,
      record_id: strOrNull(fields.record_id) || record.id,
      class_groupxclasses_id: numOrNull(fields.class_groupxclasses_id),
      class_group_id: numOrNull(fields.class_group_id),
      class_id: classId,
      class_number: normalizeEntryNumber(fields.class_number),
      class_name: strOrNull(fields.class_name) || "",
      schedule_sequencetype: strOrNull(fields.schedule_sequencetype) || "",
      class_type: strOrNull(fields.class_type) || "",
      group_name: strOrNull(fields.group_name) || "",
      ring_number: numOrNull(fields.ring_number),
      estimated_start_time: strOrNull(fields.estimated_start_time) || "",
      estimated_end_time: strOrNull(fields.estimated_end_time),
      class_group_sequence: numOrNull(fields.class_group_sequence),
      schedule_show_datev2: toIsoDateOnly(pickFirst(fields.schedule_show_datev2, fields.show_date)),
    });
  }

  return byClassId;
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

function collectTripCandidates(obj, depth = 0, out = []) {
  if (depth > 6) return out;
  if (Array.isArray(obj)) {
    for (const item of obj) collectTripCandidates(item, depth + 1, out);
    return out;
  }
  if (!obj || typeof obj !== "object") return out;

  const hasClass = ("class_id" in obj) || ("classId" in obj);
  const hasHorse = ("horse" in obj) || ("Horse" in obj);
  const hasEntry = ("entry_id" in obj) || ("entryId" in obj) || ("entryxclasses_uuid" in obj);
  if (hasClass && hasEntry && hasHorse) out.push(obj);

  for (const value of Object.values(obj)) collectTripCandidates(value, depth + 1, out);
  return out;
}

function normalizePeopleTripRow(raw, ownerPid) {
  const classId = raw?.class_id ?? raw?.classId ?? null;
  const entryId = raw?.entry_id ?? raw?.entryId ?? null;
  const horse = raw?.horse ?? raw?.Horse ?? "";
  const entryxclassesUuid = raw?.entryxclasses_uuid ?? raw?.entryxclassesUUID ?? raw?.uuid ?? "";
  if (!classId || !entryId || !String(horse || "").trim() || !String(entryxclassesUuid || "").trim()) return null;

  const entryNumber = raw?.entry_number ?? raw?.entryNumber ?? raw?.entry_no ?? raw?.entryNo ?? raw?.number;

  return {
    pid: Number(ownerPid),
    class_id: Number(classId),
    entry_id: Number(entryId),
    entryxclasses_uuid: String(entryxclassesUuid).trim(),
    horse: String(horse).trim(),
    entry_number: normalizeEntryNumber(entryNumber),
    class_name: String(raw?.class_name ?? raw?.className ?? "").trim(),
    class_number: normalizeEntryNumber(raw?.class_number ?? raw?.classNumber),
    rider_name: String(raw?.rider_name ?? raw?.riderName ?? "").trim(),
    rider_id: numOrNull(raw?.rider_id ?? raw?.riderId) ?? undefined,
    placing: numOrNull(raw?.placing) ?? undefined,
  };
}

async function fetchJson(url) {
  const response = await fetchWithRetry(url, {
    method: "GET",
    headers: { Accept: "application/json" },
  });

  const text = await response.text().catch(() => "");
  if (!response.ok) throw new Error(`Fetch failed (${response.status}): ${text.slice(0, 1200)}`);
  try {
    return JSON.parse(text);
  } catch {
    throw new Error(`Response was not valid JSON. First 1200 chars:\n${text.slice(0, 1200)}`);
  }
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

function buildCurrentFields(row, heartbeat, showRecordId, nowIso, dateOnly, currentScopeStatus, watchTripsFieldSet) {
  const fields = {};
  const maybeSet = (name, value) => {
    if (!watchTripsFieldSet.has(name)) return;
    setIfPresent(fields, name, value);
  };

  maybeSet("heartbeat", [heartbeat.recordId]);
  maybeSet("shows", showRecordId ? [showRecordId] : undefined);
  maybeSet("watch_schedule", row.watch_schedule_record_id ? [row.watch_schedule_record_id] : undefined);
  maybeSet("entryxclasses_uuid", row.entryxclasses_uuid);
  maybeSet("show_id", heartbeat.app_show_id);
  maybeSet("show_date", row.schedule_show_datev2 || heartbeat.app_sql_date);
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
  maybeSet("rider_name", row.rider_name);
  maybeSet("rider_id", row.rider_id);
  maybeSet("placing", row.placing);
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
  maybeSet("dropped_at", dateOnly);
  maybeSet("run_id", heartbeat.scope_run_id);
  maybeSet("run_time", nowIso);
  maybeSet("last_seen_at", dateOnly);

  return fields;
}

async function main() {
  requireEnv("AIRTABLE_TOKEN", AIRTABLE_TOKEN);
  requireEnv("AIRTABLE_BASE_ID", AIRTABLE_BASE_ID);

  const nowIso = new Date().toISOString();
  const dateOnly = nowIso.slice(0, 10);

  const heartbeat = await fetchLatestHeartbeat();
  const [watchTripsFieldSet, scopeStatusChoices, showRecordId, scheduleRows, activeTenantRows, wwTrainerPidByRecordId] = await Promise.all([
    fetchTableFieldSet(TABLE_WATCH_TRIPS),
    fetchScopeStatusChoices(TABLE_WATCH_TRIPS).catch(() => new Set()),
    fetchShowRecordId(heartbeat.app_show_id).catch(() => null),
    fetchWatchScheduleRows(),
    fetchActiveTenantRows(),
    fetchWwTrainerPidByRecordId(),
  ]);

  const currentScopeStatus = scopeStatusChoices.has("current") ? "current" : null;
  const droppedScopeStatus = scopeStatusChoices.has("dropped") ? "dropped" : null;

  const scheduleByClassId = buildScheduleMap(scheduleRows);
  if (!scheduleByClassId.size) {
    console.log(JSON.stringify({
      ok: true,
      run_status: "NOOP",
      reason: "No watch_schedule rows found in heartbeat view",
      app_show_id: heartbeat.app_show_id,
      app_sql_date: heartbeat.app_sql_date,
    }));
    return;
  }

  const activeTenantPids = [...new Set(
    activeTenantRows.flatMap((row) => {
      const trainerPids = row.trainer_links
        .map((id) => wwTrainerPidByRecordId.get(id) || "")
        .filter(Boolean);
      if (trainerPids.length) return trainerPids;
      return row.tenant_pid ? [row.tenant_pid] : [];
    })
  )];

  if (!activeTenantPids.length) {
    console.log(JSON.stringify({
      ok: true,
      run_status: "NOOP",
      reason: "No trainer pids found from active_tenants heartbeat view",
      app_show_id: heartbeat.app_show_id,
      app_sql_date: heartbeat.app_sql_date,
    }));
    return;
  }

  const peoplePayloads = new Map();
  const peopleFailures = [];
  await runPool(activeTenantPids, FETCH_CONCURRENCY, async (pid) => {
    const url = `${BASE_URL}/people/${encodeURIComponent(pid)}?pid=${encodeURIComponent(pid)}&show_id=${encodeURIComponent(heartbeat.app_show_id)}&customer_id=${encodeURIComponent(CUSTOMER_ID)}`;
    try {
      const payload = await fetchJson(url);
      peoplePayloads.set(pid, payload);
    } catch (error) {
      peopleFailures.push({ pid, reason: String(error?.message || error).slice(0, 300) });
      peoplePayloads.set(pid, null);
    }
  });

  const normalizedRows = [];
  const outsideSchedule = [];
  for (const pid of activeTenantPids) {
    const payload = peoplePayloads.get(pid);
    const candidates = collectTripCandidates(payload);
    for (const raw of candidates) {
      const trip = normalizePeopleTripRow(raw, pid);
      if (!trip) continue;
      const schedule = scheduleByClassId.get(String(trip.class_id));
      if (!schedule) {
        outsideSchedule.push(`${trip.class_id}|${trip.entryxclasses_uuid}`);
        continue;
      }
      normalizedRows.push({
        record_id: null,
        entryxclasses_uuid: trip.entryxclasses_uuid,
        pid: trip.pid,
        entry_id: trip.entry_id,
        entry_number: trip.entry_number,
        horse: trip.horse,
        class_id: schedule.class_id,
        class_number: schedule.class_number ?? trip.class_number,
        class_name: schedule.class_name || trip.class_name,
        schedule_sequencetype: schedule.schedule_sequencetype,
        class_type: schedule.class_type,
        class_group_id: schedule.class_group_id,
        group_name: schedule.group_name,
        class_groupxclasses_id: schedule.class_groupxclasses_id,
        ring_number: schedule.ring_number,
        estimated_start_time: schedule.estimated_start_time,
        estimated_end_time: schedule.estimated_end_time,
        class_group_sequence: schedule.class_group_sequence,
        schedule_show_datev2: schedule.schedule_show_datev2,
        rider_name: trip.rider_name,
        rider_id: trip.rider_id,
        placing: trip.placing,
        watch_schedule_record_id: schedule.recordId,
      });
    }
  }

  const uniqueRows = new Map();
  for (const row of normalizedRows) {
    const key = normalizeKey(row.entryxclasses_uuid);
    if (!key || uniqueRows.has(key)) continue;
    uniqueRows.set(key, row);
  }

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

  for (const row of uniqueRows.values()) {
    const key = normalizeKey(row.entryxclasses_uuid);
    if (!key) continue;
    keepKeySet.add(key);
    const existing = existingByKey.get(key);
    const fields = buildCurrentFields(row, heartbeat, showRecordId, nowIso, dateOnly, currentScopeStatus, watchTripsFieldSet);
    if (existing) updateRecords.push({ id: existing.id, fields });
    else createRecords.push({ fields });
  }

  if (!uniqueRows.size) {
    console.log(JSON.stringify({
      ok: true,
      dry_run: DRY_RUN,
      run_status: "NOOP",
      reason: "No people trips matched current watch_schedule class ids",
      app_show_id: heartbeat.app_show_id,
      app_sql_date: heartbeat.app_sql_date,
      active_tenant_pids: activeTenantPids.length,
      watch_schedule_classes: scheduleByClassId.size,
      normalized_rows: normalizedRows.length,
      outside_schedule_count: outsideSchedule.length,
      people_failures: peopleFailures,
    }, null, 2));
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
    active_tenant_pids: activeTenantPids.length,
    watch_schedule_rows: scheduleRows.length,
    watch_schedule_classes: scheduleByClassId.size,
    normalized_rows: normalizedRows.length,
    unique_rows: uniqueRows.size,
    people_failures: peopleFailures,
    outside_schedule_count: outsideSchedule.length,
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
  };

  if (!DRY_RUN) {
    const createResult = await airtableCreateRecords(TABLE_WATCH_TRIPS, createRecords);
    const updateResult = await airtablePatchRecords(TABLE_WATCH_TRIPS, updateRecords);
    const dropResult = await airtablePatchRecords(TABLE_WATCH_TRIPS, dropUpdates);
    summary.writes.created = createResult.okRows;
    summary.writes.updated = updateResult.okRows;
    summary.writes.dropped = dropResult.okRows;
    summary.writes.create_failures = createResult.failedRows;
    summary.writes.update_failures = updateResult.failedRows;
    summary.writes.drop_failures = dropResult.failedRows;
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
