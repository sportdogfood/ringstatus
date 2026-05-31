const { fetchTextWithConfiguredTransport } = require("./lib/sgl_fetch_adapter");

const AIRTABLE_TOKEN = process.env.AIRTABLE_TOKEN || "";
const AIRTABLE_BASE_ID = process.env.AIRTABLE_BASE_ID || "";
const TABLE_SHOW = process.env.TABLE_SHOW_TARGET || process.env.TABLE_SHOW || "show";
const VIEW_SHOW_HEARTBEAT = process.env.VIEW_SHOW_HEARTBEAT || "heartbeat";
const TABLE_HEARTBEAT = process.env.TABLE_HEARTBEAT || "heartbeat";
const TABLE_LIVE_GROUPS = process.env.TABLE_LIVE_GROUPS || "live_groups";
const TABLE_LIVE_GROUP_CHANGES = process.env.TABLE_LIVE_GROUP_CHANGES || "live_group_changes";
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

const LIVE_GROUP_CHANGE_FIELDS = [
  "estimated_start_time",
  "gone",
];

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

function valuesDiffer(oldValue, newValue) {
  return changeValue(oldValue) !== changeValue(newValue);
}

function pairedClassIdForRow(row, watchFields = {}) {
  if (!isBlank(watchFields.class_id)) return undefined;
  const classNumbers = splitStoredList(row.class_numbers);
  const classIds = splitStoredList(row.class_ids);
  if (!classIds.length) return undefined;

  if (classIds.length === 1 && classNumbers.length <= 1) return numOrNull(classIds[0]);

  const watchClassNumber = strOrNull(watchFields.class_number);
  if (!watchClassNumber || classIds.length !== classNumbers.length) return undefined;

  const index = classNumbers.findIndex((value) => value === watchClassNumber);
  return index >= 0 ? numOrNull(classIds[index]) : undefined;
}

function setChangedField(out, writable, existingFields, fieldName, value) {
  if (!writable.has(fieldName) || value === undefined || value === null) return;
  if (!valuesDiffer(existingFields[fieldName], value)) return;
  out[fieldName] = value;
}

function liveGroupWatchPropagationValues(row) {
  return {
    estimated_start_time: row.estimated_start_time,
    status: row.status,
    completed_trips: row.gone,
    total_trips: row.total,
    ring_number: row.ring_number,
    group_name: row.group_name,
  };
}

function buildWatchPropagationFields(row, record, writable, { includeRsCompletedTrips = false } = {}) {
  const existingFields = record.fields || {};
  const out = {};
  const manualTimeOverride = boolValue(existingFields.manual_time_override);
  const propagationValues = liveGroupWatchPropagationValues(row);

  if (!manualTimeOverride) {
    setChangedField(out, writable, existingFields, "estimated_start_time", propagationValues.estimated_start_time);
  }

  setChangedField(out, writable, existingFields, "status", propagationValues.status);
  setChangedField(out, writable, existingFields, "completed_trips", propagationValues.completed_trips);
  setChangedField(out, writable, existingFields, "total_trips", propagationValues.total_trips);
  setChangedField(out, writable, existingFields, "ring_number", propagationValues.ring_number);
  setChangedField(out, writable, existingFields, "group_name", propagationValues.group_name);

  const classId = pairedClassIdForRow(row, existingFields);
  setChangedField(out, writable, existingFields, "class_id", classId);

  if (includeRsCompletedTrips) {
    setChangedField(out, writable, existingFields, "rs_completed_trips", row.gone);
  }

  return out;
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

function changeValue(value) {
  if (isBlank(value)) return "";
  if (typeof value === "number" && Number.isFinite(value)) return String(value);
  if (typeof value === "boolean") return value ? "true" : "false";
  return String(firstValue(value)).trim();
}

function buildLiveGroupChangeRows({ incomingRow, existingRecord, fieldsToWatch }) {
  const existingFields = existingRecord?.fields || {};
  const liveGroupId = existingRecord?.id;
  if (!liveGroupId) return [];

  const changes = [];
  for (const fieldName of fieldsToWatch) {
    const oldValue = changeValue(existingFields[fieldName]);
    const newValue = changeValue(incomingRow[fieldName]);
    if (oldValue === newValue) continue;

    changes.push({
      change_key: [
        incomingRow.live_groups_key,
        fieldName,
        oldValue || "blank",
        newValue || "blank",
        RUN_ID,
      ].join("|"),
      live_groups: [liveGroupId],
      show: incomingRow.show,
      show_id: incomingRow.show_id,
      focus_day: incomingRow.live_focus_day || incomingRow.day,
      is_cuurent_scope: true,
      is_current_scope: true,
      class_group_id: incomingRow.class_group_id,
      group_name: incomingRow.group_name,
      ring_number: incomingRow.ring_number,
      field_changed: fieldName,
      old_value: oldValue,
      new_value: newValue,
      changed_at: RUN_AT,
      run_tag: `live_groups_daily|${RUN_ID}|group_change`,
    });
  }

  return changes;
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
      "class_id",
      "estimated_start_time",
      "status",
      "completed_trips",
      "total_trips",
      "group_name",
      "manual_time_override",
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
      "estimated_start_time",
      "status",
      "completed_trips",
      "total_trips",
      "rs_completed_trips",
      "group_name",
      "manual_time_override",
      "archive",
      "inactive",
      "dropped_at",
    ],
  });
}

function matchingWatchScheduleRecords(row, watchScheduleRows) {
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
    .filter(Boolean);
}

function matchingWatchScheduleIds(row, watchScheduleRows) {
  return matchingWatchScheduleRecords(row, watchScheduleRows).map((record) => record.id);
}

function matchingWatchTripRecords(row, watchTripRows) {
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
    .filter(Boolean);
}

function matchingWatchTripIds(row, watchTripRows) {
  return matchingWatchTripRecords(row, watchTripRows).map((record) => record.id);
}

function addMergedUpdate(updateMap, id, fields) {
  if (!id || !Object.keys(fields).length) return;
  const existing = updateMap.get(id) || {};
  updateMap.set(id, { ...existing, ...fields });
}

async function propagateLiveGroupWatchRows(rows, scope, writable) {
  if (!rows.length) {
    return {
      watch_schedule_matches: 0,
      watch_trips_matches: 0,
      watch_schedule_updates: 0,
      watch_trips_updates: 0,
    };
  }

  const [
    watchScheduleRows,
    watchTripRows,
    watchScheduleFieldMap,
    watchTripFieldMap,
  ] = await Promise.all([
    fetchWatchScheduleForScope(scope),
    fetchWatchTripsForScope(scope),
    tableFieldMap(TABLE_WATCH_SCHEDULE),
    tableFieldMap(TABLE_WATCH_TRIPS),
  ]);
  const watchScheduleWritable = writableFields(watchScheduleFieldMap);
  const watchTripWritable = writableFields(watchTripFieldMap);
  const scheduleUpdates = new Map();
  const tripUpdates = new Map();

  let scheduleMatchCount = 0;
  let tripMatchCount = 0;
  for (const row of rows) {
    const scheduleRecords = matchingWatchScheduleRecords(row, watchScheduleRows);
    const tripRecords = matchingWatchTripRecords(row, watchTripRows);
    const scheduleIds = scheduleRecords.map((record) => record.id);
    const tripIds = tripRecords.map((record) => record.id);

    if (writable.has("watch_schedule")) {
      row.watch_schedule = scheduleIds;
    }
    if (writable.has("watch_trips")) {
      row.watch_trips = tripIds;
    }

    scheduleMatchCount += scheduleIds.length;
    tripMatchCount += tripIds.length;

    for (const record of scheduleRecords) {
      addMergedUpdate(
        scheduleUpdates,
        record.id,
        buildWatchPropagationFields(row, record, watchScheduleWritable)
      );
    }

    for (const record of tripRecords) {
      addMergedUpdate(
        tripUpdates,
        record.id,
        buildWatchPropagationFields(row, record, watchTripWritable, { includeRsCompletedTrips: true })
      );
    }
  }

  await airtableUpdate(
    TABLE_WATCH_SCHEDULE,
    [...scheduleUpdates.entries()].map(([id, fields]) => ({ id, fields }))
  );
  await airtableUpdate(
    TABLE_WATCH_TRIPS,
    [...tripUpdates.entries()].map(([id, fields]) => ({ id, fields }))
  );

  return {
    watch_schedule_matches: scheduleMatchCount,
    watch_trips_matches: tripMatchCount,
    watch_schedule_updates: scheduleUpdates.size,
    watch_trips_updates: tripUpdates.size,
  };
}

async function attachLiveGroupLinks(rows, scope, writable) {
  return propagateLiveGroupWatchRows(rows, scope, writable);
}

async function logLiveGroupChanges(changeRows) {
  const fieldMap = await tableFieldMap(TABLE_LIVE_GROUP_CHANGES);
  const writable = writableFields(fieldMap);
  const records = changeRows
    .map((fields) => ({ fields: pickWritable(fields, writable) }))
    .filter((record) => Object.keys(record.fields).length);
  await airtableCreate(TABLE_LIVE_GROUP_CHANGES, records);
  return records.length;
}

async function clearStaleLiveGroupChangeScope({ showId, focusDay }) {
  const fieldMap = await tableFieldMap(TABLE_LIVE_GROUP_CHANGES);
  const writable = writableFields(fieldMap);
  if (!writable.has("is_cuurent_scope") && !writable.has("is_current_scope")) return 0;

  const fetchFields = ["show_id", "focus_day"];
  if (writable.has("is_cuurent_scope")) fetchFields.push("is_cuurent_scope");
  if (writable.has("is_current_scope")) fetchFields.push("is_current_scope");
  const rows = await airtableList(TABLE_LIVE_GROUP_CHANGES, {
    pageSize: 100,
    filterByFormula: `{show_id}=${Number(showId)}`,
    "fields[]": fetchFields,
  });

  const updates = [];
  for (const row of rows) {
    const rowDay = toIsoDateOnly(row.fields?.focus_day);
    const fields = {};

    if (rowDay === focusDay) {
      if (writable.has("is_cuurent_scope") && !boolValue(row.fields?.is_cuurent_scope)) fields.is_cuurent_scope = true;
      if (writable.has("is_current_scope") && !boolValue(row.fields?.is_current_scope)) fields.is_current_scope = true;
    } else {
      if (writable.has("is_cuurent_scope") && boolValue(row.fields?.is_cuurent_scope)) fields.is_cuurent_scope = false;
      if (writable.has("is_current_scope") && boolValue(row.fields?.is_current_scope)) fields.is_current_scope = false;
    }

    if (Object.keys(fields).length) updates.push({ id: row.id, fields });
  }
  await airtableUpdate(TABLE_LIVE_GROUP_CHANGES, updates);
  return updates.length;
}

async function upsertLiveGroups(rows, writable) {
  if (!rows.length) return { created: 0, updated: 0 };

  const showId = rows[0].show_id;
  const day = rows[0].day;
  const customerId = rows[0].customer_id;
  const fetchFields = ["live_groups_key"];
  for (const fieldName of LIVE_GROUP_CHANGE_FIELDS) fetchFields.push(fieldName);
  if (writable.has("is_cuurent_scope")) fetchFields.push("is_cuurent_scope");
  if (writable.has("is_current_scope")) fetchFields.push("is_current_scope");
  if (writable.has("dropped_at")) fetchFields.push("dropped_at");
  const existingRows = await airtableList(TABLE_LIVE_GROUPS, {
    pageSize: 100,
    filterByFormula: `AND({show_id}=${showId},{customer_id}=${customerId},{day}='${day}')`,
    "fields[]": fetchFields,
  });
  const staleScopeFields = ["live_groups_key", "live_focus_day", "day"];
  if (writable.has("is_cuurent_scope")) staleScopeFields.push("is_cuurent_scope");
  if (writable.has("is_current_scope")) staleScopeFields.push("is_current_scope");
  if (writable.has("dropped_at")) staleScopeFields.push("dropped_at");
  const scopedRowsForShow = await airtableList(TABLE_LIVE_GROUPS, {
    pageSize: 100,
    filterByFormula: `AND({show_id}=${showId},{customer_id}=${customerId})`,
    "fields[]": staleScopeFields,
  });
  const byKey = new Map();
  for (const row of existingRows) {
    const key = strOrNull(row.fields?.live_groups_key);
    if (key) byKey.set(key, row);
  }

  const creates = [];
  const updates = [];
  const changeRows = [];
  const currentKeys = new Set();
  for (const row of rows) {
    currentKeys.add(row.live_groups_key);
    const fields = pickWritable(row, writable);
    const existingRecord = byKey.get(row.live_groups_key);
    if (existingRecord) {
      updates.push({ id: existingRecord.id, fields });
      changeRows.push(...buildLiveGroupChangeRows({
        incomingRow: row,
        existingRecord,
        fieldsToWatch: LIVE_GROUP_CHANGE_FIELDS,
      }));
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
  const seenDropIds = new Set(droppedUpdates.map((row) => row.id));
  for (const row of scopedRowsForShow) {
    if (seenDropIds.has(row.id)) continue;
    const rowDay = toIsoDateOnly(row.fields?.live_focus_day) || toIsoDateOnly(row.fields?.day);
    if (rowDay === day) continue;
    if (strOrNull(row.fields?.dropped_at)) continue;
    if (!boolValue(row.fields?.is_cuurent_scope) && !boolValue(row.fields?.is_current_scope)) continue;

    const fields = {};
    if (writable.has("is_cuurent_scope")) fields.is_cuurent_scope = false;
    if (writable.has("is_current_scope")) fields.is_current_scope = false;
    if (writable.has("dropped_at")) fields.dropped_at = RUN_AT;
    if (Object.keys(fields).length) {
      droppedUpdates.push({ id: row.id, fields });
      seenDropIds.add(row.id);
    }
  }

  await airtableUpdate(TABLE_LIVE_GROUPS, updates);
  await airtableCreate(TABLE_LIVE_GROUPS, creates);
  await airtableUpdate(TABLE_LIVE_GROUPS, droppedUpdates);
  let changesLogged = 0;
  try {
    changesLogged = await logLiveGroupChanges(changeRows);
    await clearStaleLiveGroupChangeScope({ showId, focusDay: day });
  } catch (error) {
    console.warn(JSON.stringify({
      ok: false,
      event: "live_group_changes_failed",
      error: String(error?.message || error).slice(0, 800),
    }));
  }
  return {
    created: creates.length,
    updated: updates.length,
    dropped: droppedUpdates.length,
    changes_logged: changesLogged,
  };
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

  const summary = {
    ok: true,
    event: "live_groups_upserted",
    show_id: scope.show_id,
    customer_id: scope.customer_id,
    focus_day: scope.focus_day,
    rows: rows.length,
    created: result.created,
    updated: result.updated,
    dropped: result.dropped,
    changes_logged: result.changes_logged,
    watch_schedule_matches: linkResult.watch_schedule_matches,
    watch_trips_matches: linkResult.watch_trips_matches,
    watch_schedule_updates: linkResult.watch_schedule_updates,
    watch_trips_updates: linkResult.watch_trips_updates,
    dry_run: DRY_RUN,
  };
  console.log(JSON.stringify(summary));
  return summary;
}

if (require.main === module) {
  main().catch((error) => {
    console.error(JSON.stringify({
      ok: false,
      event: "live_groups_failed",
      error: String(error?.stack || error?.message || error).slice(0, 5000),
    }));
    process.exit(1);
  });
}

module.exports = {
  buildWatchPropagationFields,
  liveGroupWatchPropagationValues,
  main,
  matchingWatchScheduleRecords,
  matchingWatchTripRecords,
  pairedClassIdForRow,
  propagateLiveGroupWatchRows,
};
