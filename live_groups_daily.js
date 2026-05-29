const { fetchTextWithConfiguredTransport } = require("./lib/sgl_fetch_adapter");

const AIRTABLE_TOKEN = process.env.AIRTABLE_TOKEN || "";
const AIRTABLE_BASE_ID = process.env.AIRTABLE_BASE_ID || "";
const TABLE_SHOW = process.env.TABLE_SHOW_TARGET || process.env.TABLE_SHOW || "show";
const VIEW_SHOW_HEARTBEAT = process.env.VIEW_SHOW_HEARTBEAT || "heartbeat";
const TABLE_HEARTBEAT = process.env.TABLE_HEARTBEAT || "heartbeat";
const TABLE_LIVE_GROUPS = process.env.TABLE_LIVE_GROUPS || "live_groups";
const TABLE_WATCH_SCHEDULE = process.env.TABLE_WATCH_SCHEDULE || "watch_schedule";
const TABLE_WATCH_TRIPS = process.env.TABLE_WATCH_TRIPS || "watch_trips";
const TABLE_AUTOMATION_ERRS = process.env.TABLE_AUTOMATION_ERRS || "automation_errs";
const HEARTBEAT_SORT_FIELD = process.env.HEARTBEAT_SORT_FIELD || "hb_at";
const HTTP_TIMEOUT_MS = Number(process.env.HTTP_TIMEOUT_MS || "20000");
const DRY_RUN = String(process.env.DRY_RUN || "0") === "1";

const STATUS_BASE_URL = String(
  process.env.SGL_LIVE_STATUS_BASE_URL ||
  process.env.SGL_DATA_BASE_URL ||
  process.env.SGL_DIRECT_BASE_URL ||
  process.env.SGL_API_BASE_URL ||
  "https://sglapi.wellingtoninternational.com"
).trim().replace(/\/+$/, "");

const LIVE_BASE_URL = String(
  process.env.SGL_LIVE_BASE_URL ||
  "https://sgl.wellingtoninternational.com"
).trim().replace(/\/+$/, "");

const LIVE_GROUPS_WRITABLE_TYPES = new Set([
  "singleLineText",
  "multilineText",
  "number",
  "checkbox",
  "date",
  "dateTime",
  "multipleRecordLinks",
]);

const RUN_AT = new Date().toISOString();
const RUN_ID = Date.now();

function requireEnv(name, value) {
  if (!value) throw new Error(`Missing required env: ${name}`);
}

function isBlank(value) {
  return value === null ||
    value === undefined ||
    (typeof value === "string" && value.trim() === "") ||
    String(value).trim().toLowerCase() === "null" ||
    String(value).trim().toLowerCase() === "nan";
}

function firstValue(value) {
  if (Array.isArray(value)) return value.length ? firstValue(value[0]) : undefined;
  if (value && typeof value === "object" && "name" in value) return value.name;
  return value;
}

function strOrNull(value) {
  const raw = firstValue(value);
  if (isBlank(raw)) return null;
  return String(raw).trim();
}

function numOrNull(value) {
  const text = strOrNull(value);
  if (text === null) return null;
  const num = Number(text);
  return Number.isFinite(num) ? num : null;
}

function boolValue(value) {
  const raw = firstValue(value);
  if (raw === true || raw === 1) return true;
  if (raw === false || raw === 0 || raw === null || raw === undefined) return false;
  const text = String(raw).trim().toLowerCase();
  return ["true", "1", "yes", "checked", "live"].includes(text);
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

function asArray(value) {
  if (Array.isArray(value)) return value;
  if (value === null || value === undefined) return [];
  return [value];
}

function compactStrings(values) {
  return asArray(values)
    .map((value) => strOrNull(value))
    .filter(Boolean);
}

function keyPart(value) {
  return strOrNull(value) || "";
}

function buildLiveGroupsKey({ showId, focusDay, customerId, ringNumber, classGroupId }) {
  return [showId, focusDay, customerId, ringNumber, classGroupId].map(keyPart).join("|");
}

function escapeFormulaString(value) {
  return String(value ?? "").replace(/'/g, "\\'");
}

function showScopeFormula(showId) {
  const sid = Number(showId);
  return `OR({show_id}=${sid},{app_show_idv2}=${sid},{app_show_id}=${sid})`;
}

function scheduleDateScopeFormula(sqlDate) {
  const day = escapeFormulaString(sqlDate);
  return [
    `{scheduled_date}='${day}'`,
    `{app_sql_datev2}='${day}'`,
    `{schedule_show_datev2}='${day}'`,
    `{show_date}='${day}'`,
    `DATETIME_FORMAT({focus_day}, 'YYYY-MM-DD')='${day}'`,
  ].join(",");
}

function tripDateScopeFormula(sqlDate) {
  const day = escapeFormulaString(sqlDate);
  return [
    `{scheduled_date}='${day}'`,
    `{app_sql_datev2}='${day}'`,
    `{app_sql_date}='${day}'`,
    `DATETIME_FORMAT({show_date}, 'YYYY-MM-DD')='${day}'`,
    `DATETIME_FORMAT({schedule_show_datev2}, 'YYYY-MM-DD')='${day}'`,
    `DATETIME_FORMAT({focus_day}, 'YYYY-MM-DD')='${day}'`,
  ].join(",");
}

function recordIsUsable(fields = {}) {
  return !boolValue(fields.archive) &&
    !boolValue(fields.inactive) &&
    !strOrNull(fields.dropped_at);
}

function splitStoredList(value) {
  return String(value || "")
    .split(/[,\n]/)
    .map((item) => item.trim())
    .filter(Boolean);
}

function numberSet(value) {
  return new Set(
    splitStoredList(value)
      .map((item) => Number(item))
      .filter((item) => Number.isFinite(item))
  );
}

function numbersOverlap(set, value) {
  if (!set.size) return true;
  const num = numOrNull(value);
  return num !== null && set.has(num);
}

function airtableHeaders(extra = {}) {
  return {
    Authorization: `Bearer ${AIRTABLE_TOKEN}`,
    "Content-Type": "application/json",
    ...extra,
  };
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

function metaUrl() {
  return `https://api.airtable.com/v0/meta/bases/${encodeURIComponent(AIRTABLE_BASE_ID)}/tables`;
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

async function airtableJson(url, options = {}) {
  const response = await fetchWithTimeout(url, {
    ...options,
    headers: airtableHeaders(options.headers || {}),
  });
  const body = await response.text();
  if (!response.ok) {
    throw new Error(`Airtable request failed ${response.status}: ${body.slice(0, 800)}`);
  }
  return body ? JSON.parse(body) : {};
}

async function airtableList(tableName, params = {}) {
  const records = [];
  let offset = null;
  do {
    const json = await airtableJson(airtableUrl(tableName, { ...params, offset }));
    records.push(...(json.records || []));
    offset = json.offset || null;
  } while (offset);
  return records;
}

async function airtableCreate(tableName, records) {
  if (!records.length) return [];
  if (DRY_RUN) return records.map((_, index) => ({ id: `dry_create_${index}` }));
  const created = [];
  for (let i = 0; i < records.length; i += 10) {
    const json = await airtableJson(airtableUrl(tableName), {
      method: "POST",
      body: JSON.stringify({ records: records.slice(i, i + 10) }),
    });
    created.push(...(json.records || []));
  }
  return created;
}

async function airtableUpdate(tableName, records) {
  if (!records.length) return [];
  if (DRY_RUN) return records.map((record) => ({ id: record.id }));
  const updated = [];
  for (let i = 0; i < records.length; i += 10) {
    const json = await airtableJson(airtableUrl(tableName), {
      method: "PATCH",
      body: JSON.stringify({ records: records.slice(i, i + 10) }),
    });
    updated.push(...(json.records || []));
  }
  return updated;
}

async function tableFieldMap(tableName) {
  const meta = await airtableJson(metaUrl(), { headers: { Authorization: `Bearer ${AIRTABLE_TOKEN}` } });
  const table = (meta.tables || []).find((item) => item.name === tableName);
  if (!table) throw new Error(`Airtable table not found: ${tableName}`);
  return new Map((table.fields || []).map((field) => [field.name, field]));
}

function writableFields(fieldMap) {
  const names = new Set();
  for (const [name, field] of fieldMap.entries()) {
    if (LIVE_GROUPS_WRITABLE_TYPES.has(field.type)) names.add(name);
  }
  return names;
}

function pickWritable(fields, writableSet) {
  const out = {};
  for (const [name, value] of Object.entries(fields)) {
    if (!writableSet.has(name)) continue;
    if (value === undefined) continue;
    out[name] = value;
  }
  return out;
}

async function resolveHeartbeatShow() {
  const envShowRecordId = strOrNull(process.env.HEARTBEAT_TARGET_SHOW_RECORD_ID);
  const envShowId = numOrNull(process.env.HEARTBEAT_TARGET_APP_SHOW_ID);
  const envCustomerId = numOrNull(process.env.HEARTBEAT_TARGET_CUSTOMER_ID || process.env.CUSTOMER_ID);
  const envFocusDay = toIsoDateOnly(process.env.HEARTBEAT_TARGET_SQL_DATES);

  if (envShowRecordId && envShowId !== null && envCustomerId !== null && envFocusDay) {
    return {
      record_id: envShowRecordId,
      show_id: envShowId,
      customer_id: envCustomerId,
      focus_day: envFocusDay,
    };
  }

  const params = {
    view: VIEW_SHOW_HEARTBEAT,
    pageSize: 100,
    "fields[]": ["show_id", "customer_id", "focus_day", "heartbeat"],
  };
  const rows = await airtableList(TABLE_SHOW, params);
  const heartbeatRows = rows.filter((row) => boolValue(row.fields?.heartbeat));
  const scoped = envShowId === null
    ? heartbeatRows
    : heartbeatRows.filter((row) => numOrNull(row.fields?.show_id) === envShowId);

  if (scoped.length !== 1) {
    throw new Error(`Expected exactly one ${TABLE_SHOW}/${VIEW_SHOW_HEARTBEAT} row for live_groups, found ${scoped.length}`);
  }

  const fields = scoped[0].fields || {};
  const showId = numOrNull(fields.show_id);
  const customerId = numOrNull(fields.customer_id);
  const focusDay = toIsoDateOnly(fields.focus_day);
  if (showId === null) throw new Error(`Show ${scoped[0].id} missing show_id`);
  if (customerId === null) throw new Error(`Show ${scoped[0].id} missing customer_id`);
  if (!focusDay) throw new Error(`Show ${scoped[0].id} missing focus_day`);

  return {
    record_id: scoped[0].id,
    show_id: showId,
    customer_id: customerId,
    focus_day: focusDay,
  };
}

async function latestHeartbeatForScope(scope) {
  const rows = await airtableList(TABLE_HEARTBEAT, {
    maxRecords: 25,
    "sort[0][field]": HEARTBEAT_SORT_FIELD,
    "sort[0][direction]": "desc",
    "fields[]": [
      "mode",
      "show_id",
      "app_show_id",
      "app_sql_date",
      "sql_date",
      "focus_day",
      "show",
      "hb_at",
    ],
  });

  return rows.find((row) => {
    const fields = row.fields || {};
    const rowShowId = numOrNull(fields.show_id ?? fields.app_show_id);
    const rowDay = toIsoDateOnly(fields.focus_day ?? fields.app_sql_date ?? fields.sql_date);
    const linkedShow = asArray(fields.show).map(String);
    const showMatches = rowShowId === scope.show_id || linkedShow.includes(scope.record_id);
    const dayMatches = rowDay === scope.focus_day;
    return showMatches && dayMatches;
  }) || rows[0] || null;
}

function normalizeMode(value) {
  const text = strOrNull(value);
  return text ? text.toUpperCase() : "";
}

function statusEndpoint(customerId) {
  return `${STATUS_BASE_URL}/homepage/getLiveClassStatus?customer_id=${encodeURIComponent(customerId)}`;
}

function listAjaxEndpoint() {
  return `${LIVE_BASE_URL}/iphonev2/index.php/esp/liveclassv2/ListAjax?from_wp_api=true`;
}

async function fetchText(url) {
  return fetchTextWithConfiguredTransport(url, async (targetUrl) => {
    const response = await fetchWithTimeout(targetUrl);
    const text = await response.text();
    if (!response.ok) {
      throw new Error(`Fetch failed ${response.status} ${targetUrl}: ${text.slice(0, 500)}`);
    }
    return {
      text,
      response,
      endpoint: targetUrl,
      originalEndpoint: url,
      transport: "node_fetch",
    };
  });
}

function parseJsonText(text, endpoint) {
  try {
    return JSON.parse(text);
  } catch (error) {
    throw new Error(`Invalid JSON from ${endpoint}: ${String(error?.message || error)}`);
  }
}

function statusAllowsLive(payload) {
  if (payload === true) return true;
  if (payload === false || payload === null || payload === undefined) return false;
  if (typeof payload === "number") return payload !== 0;
  if (typeof payload === "string") {
    const text = payload.trim().toLowerCase();
    return ["true", "1", "yes", "live"].includes(text);
  }
  if (typeof payload === "object") {
    for (const key of ["status", "is_live", "live", "success", "ok", "payload"]) {
      if (Object.prototype.hasOwnProperty.call(payload, key)) return statusAllowsLive(payload[key]);
    }
  }
  return false;
}

function normalizePayloadRows(payload, scope) {
  const topRows = Array.isArray(payload) ? payload : [payload];
  const out = [];

  for (const showRow of topRows) {
    const payloadShowId = numOrNull(showRow?.show_id);
    if (payloadShowId !== scope.show_id) continue;

    for (const item of asArray(showRow?.json_data)) {
      const day = toIsoDateOnly(item?.day);
      if (day !== scope.focus_day) continue;

      const classIds = compactStrings(item?.classes);
      const classNumbers = compactStrings(item?.classNumbers);
      const classNames = compactStrings(item?.classNames);
      const classGroupId = numOrNull(item?.class_group_id);
      const ringNumber = numOrNull(item?.ring_number);
      const customerId = scope.customer_id;
      const liveGroupsKey = buildLiveGroupsKey({
        showId: scope.show_id,
        focusDay: scope.focus_day,
        customerId,
        ringNumber,
        classGroupId,
      });
      if (!liveGroupsKey || classGroupId === null || ringNumber === null) continue;

      out.push({
        live_groups_key: liveGroupsKey,
        class_group_id: classGroupId,
        show_id: scope.show_id,
        customer_id: customerId,
        show: [scope.record_id],
        day,
        live_focus_day: scope.focus_day,
        ring_number: ringNumber,
        ring_id: numOrNull(item?.ring_id),
        group_name: strOrNull(item?.group_name),
        classes: JSON.stringify(classIds),
        class_ids: classIds.join(","),
        classNumbers: JSON.stringify(classNumbers),
        class_numbers: classNumbers.join(","),
        classNames: JSON.stringify(classNames),
        class_names: classNames.join("\n"),
        estimated_start_time: strOrNull(item?.estimated_start_time),
        gone: numOrNull(item?.gone),
        total: numOrNull(item?.total ?? item?.trips),
        curr_updated_at: strOrNull(item?.curr_updated_at),
        ingested_at: RUN_AT,
        run_tag: `live_groups_daily|${RUN_ID}|${scope.show_id}|${scope.focus_day}`,
        is_live: boolValue(item?.is_live),
        has_JSON: boolValue(item?.has_JSON),
        is_cuurent_scope: true,
        is_current_scope: true,
        dropped_at: null,
        status: strOrNull(item?.status),
      });
    }
  }

  return out;
}

async function logAutomationEvent({ scope, errorType, message }) {
  const fieldMap = await tableFieldMap(TABLE_AUTOMATION_ERRS);
  const writable = writableFields(fieldMap);
  const fields = pickWritable({
    automation_key: `live_groups_daily|${scope?.show_id || "na"}|${scope?.focus_day || "na"}|${errorType}|${RUN_ID}`,
    automation_name: "live_groups_daily",
    error_type: errorType,
    app_sql_date: scope?.focus_day || null,
    run_id: RUN_ID,
    last_run: RUN_AT.slice(0, 10),
    message,
    app_show_id: scope?.show_id ?? null,
  }, writable);
  await airtableCreate(TABLE_AUTOMATION_ERRS, [{ fields }]);
}

async function fetchWatchScheduleForScope(scope) {
  return airtableList(TABLE_WATCH_SCHEDULE, {
    pageSize: 100,
    filterByFormula: `AND(${showScopeFormula(scope.show_id)},OR(${scheduleDateScopeFormula(scope.focus_day)}))`,
    "fields[]": [
      "show_id",
      "app_show_idv2",
      "app_show_id",
      "scheduled_date",
      "app_sql_datev2",
      "schedule_show_datev2",
      "show_date",
      "focus_day",
      "ring_number",
      "class_group_id",
      "class_number",
      "archive",
      "inactive",
      "dropped_at",
    ],
  });
}

async function fetchWatchTripsForScope(scope) {
  return airtableList(TABLE_WATCH_TRIPS, {
    pageSize: 100,
    filterByFormula: `AND(${showScopeFormula(scope.show_id)},OR(${tripDateScopeFormula(scope.focus_day)}))`,
    "fields[]": [
      "show_id",
      "app_show_idv2",
      "app_show_id",
      "scheduled_date",
      "app_sql_datev2",
      "app_sql_date",
      "show_date",
      "schedule_show_datev2",
      "focus_day",
      "ring_number",
      "class_group_id",
      "class_id",
      "class_number",
      "entry_number",
      "rider_name",
      "horse",
      "archive",
      "inactive",
      "dropped_at",
    ],
  });
}

function matchingWatchScheduleIds(row, watchScheduleRows) {
  const classNumbers = numberSet(row.class_numbers);
  const rowGroupId = numOrNull(row.class_group_id);
  const rowRing = numOrNull(row.ring_number);
  if (rowGroupId === null) return [];

  return watchScheduleRows
    .filter((record) => {
      const fields = record.fields || {};
      if (!recordIsUsable(fields)) return false;
      if (numOrNull(fields.class_group_id) !== rowGroupId) return false;
      const scheduleRing = numOrNull(fields.ring_number);
      if (rowRing !== null && scheduleRing !== null && scheduleRing !== rowRing) return false;
      return numbersOverlap(classNumbers, fields.class_number);
    })
    .map((record) => record.id);
}

function matchingWatchTripIds(row, watchTripRows) {
  const classIds = numberSet(row.class_ids);
  const classNumbers = numberSet(row.class_numbers);
  const rowGroupId = numOrNull(row.class_group_id);
  const rowRing = numOrNull(row.ring_number);
  if (rowGroupId === null) return [];

  return watchTripRows
    .filter((record) => {
      const fields = record.fields || {};
      if (!recordIsUsable(fields)) return false;
      if (numOrNull(fields.class_group_id) !== rowGroupId) return false;
      const tripRing = numOrNull(fields.ring_number);
      if (rowRing !== null && tripRing !== null && tripRing !== rowRing) return false;
      if (classIds.size && numbersOverlap(classIds, fields.class_id)) return true;
      if (classNumbers.size && numbersOverlap(classNumbers, fields.class_number)) return true;
      return !classIds.size && !classNumbers.size;
    })
    .map((record) => record.id);
}

async function attachLiveGroupLinks(rows, scope, writable) {
  if (!rows.length || (!writable.has("watch_schedule") && !writable.has("watch_trips"))) {
    return { watch_schedule_matches: 0, watch_trips_matches: 0 };
  }

  const [watchScheduleRows, watchTripRows] = await Promise.all([
    writable.has("watch_schedule") ? fetchWatchScheduleForScope(scope) : Promise.resolve([]),
    writable.has("watch_trips") ? fetchWatchTripsForScope(scope) : Promise.resolve([]),
  ]);

  let scheduleMatchCount = 0;
  let tripMatchCount = 0;
  for (const row of rows) {
    const scheduleIds = writable.has("watch_schedule")
      ? matchingWatchScheduleIds(row, watchScheduleRows)
      : undefined;
    const tripIds = writable.has("watch_trips")
      ? matchingWatchTripIds(row, watchTripRows)
      : undefined;

    if (scheduleIds) {
      row.watch_schedule = scheduleIds;
      scheduleMatchCount += scheduleIds.length;
    }
    if (tripIds) {
      row.watch_trips = tripIds;
      tripMatchCount += tripIds.length;
    }
  }

  return {
    watch_schedule_matches: scheduleMatchCount,
    watch_trips_matches: tripMatchCount,
  };
}

async function upsertLiveGroups(rows, writable) {
  if (!rows.length) return { created: 0, updated: 0 };

  const showId = rows[0].show_id;
  const day = rows[0].day;
  const customerId = rows[0].customer_id;
  const fetchFields = ["live_groups_key"];
  if (writable.has("is_cuurent_scope")) fetchFields.push("is_cuurent_scope");
  if (writable.has("is_current_scope")) fetchFields.push("is_current_scope");
  if (writable.has("dropped_at")) fetchFields.push("dropped_at");
  const existingRows = await airtableList(TABLE_LIVE_GROUPS, {
    pageSize: 100,
    filterByFormula: `AND({show_id}=${showId},{customer_id}=${customerId},{day}='${day}')`,
    "fields[]": fetchFields,
  });
  const byKey = new Map();
  for (const row of existingRows) {
    const key = strOrNull(row.fields?.live_groups_key);
    if (key) byKey.set(key, row.id);
  }

  const creates = [];
  const updates = [];
  const currentKeys = new Set();
  for (const row of rows) {
    currentKeys.add(row.live_groups_key);
    const fields = pickWritable(row, writable);
    const existingId = byKey.get(row.live_groups_key);
    if (existingId) {
      updates.push({ id: existingId, fields });
    } else {
      creates.push({ fields });
    }
  }

  const droppedUpdates = [];
  for (const row of existingRows) {
    const key = strOrNull(row.fields?.live_groups_key);
    if (!key || currentKeys.has(key)) continue;

    const fields = {};
    if (writable.has("is_cuurent_scope")) fields.is_cuurent_scope = false;
    if (writable.has("is_current_scope")) fields.is_current_scope = false;
    if (writable.has("dropped_at") && !strOrNull(row.fields?.dropped_at)) fields.dropped_at = RUN_AT;
    if (Object.keys(fields).length) droppedUpdates.push({ id: row.id, fields });
  }

  await airtableUpdate(TABLE_LIVE_GROUPS, updates);
  await airtableCreate(TABLE_LIVE_GROUPS, creates);
  await airtableUpdate(TABLE_LIVE_GROUPS, droppedUpdates);
  return { created: creates.length, updated: updates.length, dropped: droppedUpdates.length };
}

async function main() {
  requireEnv("AIRTABLE_TOKEN", AIRTABLE_TOKEN);
  requireEnv("AIRTABLE_BASE_ID", AIRTABLE_BASE_ID);

  const scope = await resolveHeartbeatShow();
  const heartbeat = await latestHeartbeatForScope(scope);
  const mode = normalizeMode(heartbeat?.fields?.mode);
  if (mode !== "DAY") {
    console.log(JSON.stringify({
      ok: true,
      event: "live_groups_skipped",
      reason: "mode_not_day",
      mode,
      show_id: scope.show_id,
      focus_day: scope.focus_day,
      heartbeat_id: heartbeat?.id || null,
    }));
    return;
  }

  const statusUrl = statusEndpoint(scope.customer_id);
  let statusPayload;
  try {
    const statusResult = await fetchText(statusUrl);
    statusPayload = parseJsonText(statusResult.text, statusResult.endpoint);
  } catch (error) {
    await logAutomationEvent({
      scope,
      errorType: "live_groups_status_fetch_failed",
      message: String(error?.stack || error?.message || error).slice(0, 5000),
    });
    throw error;
  }

  if (!statusAllowsLive(statusPayload)) {
    await logAutomationEvent({
      scope,
      errorType: "live_groups_status_false",
      message: `getLiveClassStatus returned false for customer_id=${scope.customer_id}`,
    });
    console.log(JSON.stringify({
      ok: true,
      event: "live_groups_status_false",
      show_id: scope.show_id,
      customer_id: scope.customer_id,
      focus_day: scope.focus_day,
    }));
    return;
  }

  const ajaxUrl = listAjaxEndpoint();
  let ajaxPayload;
  try {
    const ajaxResult = await fetchText(ajaxUrl);
    ajaxPayload = parseJsonText(ajaxResult.text, ajaxResult.endpoint);
  } catch (error) {
    await logAutomationEvent({
      scope,
      errorType: "live_groups_listajax_fetch_failed",
      message: String(error?.stack || error?.message || error).slice(0, 5000),
    });
    throw error;
  }

  const rows = normalizePayloadRows(ajaxPayload, scope);
  if (!rows.length) {
    await logAutomationEvent({
      scope,
      errorType: "live_groups_no_focus_rows",
      message: `ListAjax returned no rows matching show_id=${scope.show_id} day=${scope.focus_day}`,
    });
    console.log(JSON.stringify({
      ok: true,
      event: "live_groups_no_focus_rows",
      show_id: scope.show_id,
      focus_day: scope.focus_day,
    }));
    return;
  }

  const liveFieldMap = await tableFieldMap(TABLE_LIVE_GROUPS);
  const liveWritable = writableFields(liveFieldMap);
  const linkResult = await attachLiveGroupLinks(rows, scope, liveWritable);
  const result = await upsertLiveGroups(rows, liveWritable);

  console.log(JSON.stringify({
    ok: true,
    event: "live_groups_upserted",
    show_id: scope.show_id,
    customer_id: scope.customer_id,
    focus_day: scope.focus_day,
    rows: rows.length,
    created: result.created,
    updated: result.updated,
    dropped: result.dropped,
    watch_schedule_matches: linkResult.watch_schedule_matches,
    watch_trips_matches: linkResult.watch_trips_matches,
    dry_run: DRY_RUN,
  }));
}

main().catch((error) => {
  console.error(JSON.stringify({
    ok: false,
    event: "live_groups_failed",
    error: String(error?.stack || error?.message || error).slice(0, 5000),
  }));
  process.exit(1);
});
