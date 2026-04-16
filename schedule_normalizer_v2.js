const fs = require("fs");
const path = require("path");

const AIRTABLE_TOKEN = process.env.AIRTABLE_TOKEN || "";
const AIRTABLE_BASE_ID = process.env.AIRTABLE_BASE_ID || "";
const CUSTOMER_ID = Number(process.env.CUSTOMER_ID || "15");

const TABLE_WATCH_SCHEDULE = process.env.TABLE_WATCH_SCHEDULE || "watch_schedule";
const VIEW_WATCH_SCHEDULE_HEARTBEAT = process.env.VIEW_WATCH_SCHEDULE_HEARTBEAT || "heartbeat";

const HTTP_TIMEOUT_MS = Number(process.env.HTTP_TIMEOUT_MS || "20000");
const AT_RETRY_ATTEMPTS = Number(process.env.AT_RETRY_ATTEMPTS || "3");
const AT_RETRY_BASE_MS = Number(process.env.AT_RETRY_BASE_MS || "400");
const AT_RETRY_MAX_MS = Number(process.env.AT_RETRY_MAX_MS || "2000");

const OUTPUT_PATH = String(process.env.NORMALIZER_OUTPUT_PATH || "").trim();
const OUTPUT_PRETTY = String(process.env.NORMALIZER_OUTPUT_PRETTY || "1") === "1";
const STDOUT_FULL = String(process.env.NORMALIZER_STDOUT_FULL || "0") === "1";
const DEBUG = String(process.env.NORMALIZER_DEBUG || "0") === "1";
const VALID_DOW_RAW = new Set(["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]);

function requireEnv(name, value) {
  if (!value) throw new Error(`Missing required env: ${name}`);
}

function debug(step, extra = {}) {
  if (!DEBUG) return;
  try {
    process.stderr.write(`${JSON.stringify({ step, ...extra })}\n`);
  } catch {}
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

  const us = text.match(/^(\d{2})\/(\d{2})\/(\d{2}|\d{4})$/);
  if (us) {
    const mm = us[1];
    const dd = us[2];
    const yy = us[3].length === 2 ? `20${us[3]}` : us[3];
    return `${yy}-${mm}-${dd}`;
  }

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

function normalizeKey(value) {
  if (isBlank(value)) return "";
  return String(value).trim();
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
  if (!text) throw new Error(`Missing required scope field: ${fieldName}`);
  if (!/^\d{4}-\d{2}-\d{2}$/.test(text)) {
    throw new Error(`Invalid scope ${fieldName}: ${text}`);
  }
  return text;
}

function strictDowRaw(value, fieldName) {
  const text = strOrNull(value);
  if (!text) throw new Error(`Missing required scope field: ${fieldName}`);
  if (!VALID_DOW_RAW.has(text)) {
    throw new Error(`Invalid scope ${fieldName}: ${text}`);
  }
  return text;
}

function uniqueNonBlankValues(rows, selector) {
  const values = [];
  const seen = new Set();
  for (const row of rows) {
    const value = selector(row);
    if (isBlank(value)) continue;
    const text = String(value);
    if (seen.has(text)) continue;
    seen.add(text);
    values.push(value);
  }
  return values;
}

function setIfPresent(target, fieldName, value) {
  if (!fieldName) return;
  if (value === undefined || value === null) return;
  if (typeof value === "string" && value.trim() === "") return;
  target[fieldName] = value;
}

function isObj(value) {
  return value && typeof value === "object" && !Array.isArray(value);
}

function deriveShowDayKey(showId, showDateIso) {
  const dateText = toIsoDateOnly(showDateIso);
  if (!dateText || isBlank(showId)) return undefined;
  return `${showId}-${dateText.replaceAll("-", "")}`;
}

function deriveShowRingKey(showId, ringNumberRaw) {
  const ringNumber = numOrNull(ringNumberRaw);
  if (ringNumber === null || isBlank(showId)) return undefined;
  return `${showId}-${ringNumber}`;
}

function mergePreserveJoinKeys(prev, next) {
  if (!prev) return next;
  const merged = { ...prev, ...next };

  if (prev.show_ring_key && !next.show_ring_key) merged.show_ring_key = prev.show_ring_key;
  if (prev.show_day_key && !next.show_day_key) merged.show_day_key = prev.show_day_key;
  if (prev.ring_number !== undefined && next.ring_number === undefined) merged.ring_number = prev.ring_number;
  if (prev.grouped_class !== undefined && next.grouped_class === undefined) merged.grouped_class = prev.grouped_class;

  return merged;
}

function mergeGroupOntoClass(classRow, groupRow) {
  if (!groupRow) return classRow;
  const merged = { ...groupRow, ...classRow };

  if (groupRow.group_name && !merged.group_name) merged.group_name = groupRow.group_name;
  if (groupRow.class_group_sequence !== undefined && merged.class_group_sequence === undefined) {
    merged.class_group_sequence = groupRow.class_group_sequence;
  }
  if (groupRow.group_has_warmup !== undefined && merged.group_has_warmup === undefined) {
    merged.group_has_warmup = groupRow.group_has_warmup;
  }
  if (groupRow.is_open_card_warmup !== undefined && merged.is_open_card_warmup === undefined) {
    merged.is_open_card_warmup = groupRow.is_open_card_warmup;
  }
  if (groupRow.grouped_class !== undefined && merged.grouped_class === undefined) {
    merged.grouped_class = groupRow.grouped_class;
  }
  if (groupRow.show_day_key && !merged.show_day_key) merged.show_day_key = groupRow.show_day_key;
  if (groupRow.show_ring_key && !merged.show_ring_key) merged.show_ring_key = groupRow.show_ring_key;
  if (groupRow.ring_number !== undefined && merged.ring_number === undefined) merged.ring_number = groupRow.ring_number;
  if (groupRow.show_id !== undefined && merged.show_id === undefined) merged.show_id = groupRow.show_id;
  if (groupRow.show_date && !merged.show_date) merged.show_date = groupRow.show_date;

  return merged;
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

function isViewNotFoundError(error) {
  const message = String(error?.message || error);
  return /VIEW_NAME_NOT_FOUND/i.test(message);
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

function buildScopeKey(appShowId, appSqlDate, appDowRaw, shiftedToNextDay) {
  return [
    isBlank(appShowId) ? "" : String(appShowId),
    strOrNull(appSqlDate) || "",
    strOrNull(appDowRaw) || "",
    shiftedToNextDay ? "1" : "0",
  ].join("|");
}

function summarizeResult(result) {
  return {
    ok: result.ok,
    generated_at: result.generated_at,
    chosen_source: result.chosen_source,
    authoritative: result.authoritative,
    authoritative_empty: result.authoritative_empty,
    row_count: result.row_count,
    existing_scope_row_count: result.existing_scope_row_count,
    added_keys: result.added_keys.length,
    dropped_keys: result.dropped_keys.length,
    unchanged_keys: result.unchanged_keys.length,
    scope: result.scope,
    fetches: result.fetches,
    endpoints: result.endpoints,
  };
}

function buildScheduleEndpoint(appSqlDate, appShowId) {
  if (isBlank(appSqlDate) || isBlank(appShowId)) return null;
  return `https://broad-tooth-b8ed.gombcg.workers.dev/schedule?date=${encodeURIComponent(appSqlDate)}&show_id=${encodeURIComponent(appShowId)}&customer_id=${encodeURIComponent(CUSTOMER_ID)}`;
}

function buildScheduleEmptyEndpoint(appShowId) {
  if (isBlank(appShowId)) return null;
  return `https://broad-tooth-b8ed.gombcg.workers.dev/schedule?date=00/00/00&show_id=${encodeURIComponent(appShowId)}&customer_id=${encodeURIComponent(CUSTOMER_ID)}`;
}

function buildClassesEndpoint(classId, appShowId) {
  if (isBlank(classId) || isBlank(appShowId)) return null;
  return `https://broad-tooth-b8ed.gombcg.workers.dev/classes/${encodeURIComponent(classId)}/?show_id=${encodeURIComponent(appShowId)}&customer_id=${encodeURIComponent(CUSTOMER_ID)}`;
}

async function fetchScheduleHeartbeatRows() {
  try {
    return await airtableList(TABLE_WATCH_SCHEDULE, {
      view: VIEW_WATCH_SCHEDULE_HEARTBEAT,
      pageSize: 100,
      "fields[]": [
        "class_groupxclasses_id",
        "heartbeat_rid",
        "app_show_idv2",
        "app_sql_datev2",
        "app_dow_rawv2",
        "shifted_to_next_dayv2",
      ],
    });
  } catch (error) {
    if (!isViewNotFoundError(error)) throw error;
    return [];
  }
}

function buildScopeFromScheduleHeartbeatRows(rows) {
  if (!rows.length) return null;
  const firstRow = rows[0];

  const appShowIds = uniqueNonBlankValues(rows, (row) => numOrNull(row?.fields?.app_show_idv2));
  const appSqlDates = uniqueNonBlankValues(rows, (row) => strictSqlDate(row?.fields?.app_sql_datev2, "app_sql_datev2"));
  const appDowRaws = uniqueNonBlankValues(rows, (row) => strictDowRaw(row?.fields?.app_dow_rawv2, "app_dow_rawv2"));
  const heartbeatRids = uniqueNonBlankValues(rows, (row) => strOrNull(firstValue(row?.fields?.heartbeat_rid)));
  const shiftedValues = uniqueNonBlankValues(rows, (row) => String(boolValue(row?.fields?.shifted_to_next_dayv2)));

  if (appShowIds.length !== 1) throw new Error(`Inconsistent watch_schedule heartbeat app_show_idv2 values: ${appShowIds.join(",")}`);
  if (appSqlDates.length !== 1) throw new Error(`Inconsistent watch_schedule heartbeat app_sql_datev2 values: ${appSqlDates.join(",")}`);
  if (appDowRaws.length !== 1) throw new Error(`Inconsistent watch_schedule heartbeat app_dow_rawv2 values: ${appDowRaws.join(",")}`);
  if (heartbeatRids.length !== 1) throw new Error(`Inconsistent watch_schedule heartbeat heartbeat_rid values: ${heartbeatRids.join(",")}`);
  if (shiftedValues.length > 1) throw new Error(`Inconsistent watch_schedule heartbeat shifted_to_next_dayv2 values: ${shiftedValues.join(",")}`);

  const appShowId = appShowIds[0];
  const appSqlDate = appSqlDates[0];
  const appDowRaw = appDowRaws[0];
  const heartbeatRecordId = heartbeatRids[0];
  const shiftedToNextDay = shiftedValues.length ? shiftedValues[0] === "true" : false;

  if (appShowId === null) throw new Error("Missing required scope field: app_show_idv2");
  if (!heartbeatRecordId) throw new Error("Missing required scope field: heartbeat_rid");
  const expectedDowRaw = dowName(dayOfWeekUtc(appSqlDate));
  if (expectedDowRaw !== appDowRaw) {
    throw new Error(`Scope app_dow_rawv2 mismatch: expected=${expectedDowRaw} actual=${appDowRaw}`);
  }

  return {
    heartbeat_record_id: heartbeatRecordId,
    heartbeat_rid: heartbeatRecordId,
    hb_at: null,
    app_show_idv2: appShowId,
    app_sql_datev2: appSqlDate,
    app_dow_rawv2: appDowRaw,
    shifted_to_next_dayv2: shiftedToNextDay,
    scope_key: buildScopeKey(appShowId, appSqlDate, appDowRaw, shiftedToNextDay),
    scope_run_id: heartbeatRecordId || firstRow.id,
    heartbeat_time: null,
    heartbeat_show_date: null,
  };
}

function normalizeSchedulePayload(payload, options) {
  const scope = options.scope;
  const source = options.source;
  const generatedAt = options.generatedAt;
  const generatedDate = options.generatedDate;

  const payloadShowId = numOrNull(pickFirst(payload?.show?.show_id, payload?.show_id, payload?.showId, scope.app_show_idv2));
  const payloadShowDate = toIsoDateOnly(pickFirst(payload?.show_date, payload?.showDate, payload?.date));
  const scheduleShowDate = payloadShowDate;
  const scheduleShowDisplayDate = strOrNull(pickFirst(payload?.show_display_date, payload?.showDisplayDate, payload?.display_date));
  const scheduleShowDisplayDateDay = strOrNull(
    pickFirst(payload?.show_display_date_day, payload?.showDisplayDateDay, payload?.show_display_day)
  );

  const groupsById = new Map();
  const classesById = new Map();

  function liftContext(node, ctx) {
    const next = { ...ctx };

    if (isObj(node.show)) next.show_id = pickFirst(next.show_id, node.show.show_id, node.show.showId, node.show.id);
    next.show_id = pickFirst(next.show_id, node.show_id, node.showId, payloadShowId);

    const dateValue = pickFirst(node.show_date, node.showDate, node.date, node.show_day_date, payloadShowDate);
    const iso = toIsoDateOnly(dateValue);
    if (iso) next.show_date = iso;

    if (isObj(node.ring)) {
      next.ring_number = pickFirst(next.ring_number, node.ring.ring_number, node.ring.ringNumber, node.ring.number);
    }
    next.ring_number = pickFirst(next.ring_number, node.ring_number, node.ringNumber, node.ring_no, node.ringNo);

    return next;
  }

  function isClassNode(node) {
    if (!isObj(node)) return false;
    const hasKey = node.class_groupxclasses_id !== undefined || node.classGroupXClassesId !== undefined;
    const hasClassId = node.class_id !== undefined || node.classId !== undefined || node.id !== undefined;
    return Boolean(hasKey && hasClassId);
  }

  function walk(node, ctx) {
    if (Array.isArray(node)) {
      for (const item of node) walk(item, ctx);
      return;
    }
    if (!isObj(node)) return;

    const next = liftContext(node, ctx);

    const showId = numOrNull(pickFirst(next.show_id, payloadShowId));
    const showDate = toIsoDateOnly(pickFirst(next.show_date, payloadShowDate));
    const ringNumber = numOrNull(next.ring_number);
    const showDayKey = deriveShowDayKey(showId, showDate);
    const showRingKey = deriveShowRingKey(showId, ringNumber);

    const classRelated = isObj(node.class_related_data)
      ? node.class_related_data
      : isObj(node.classRelatedData)
      ? node.classRelatedData
      : undefined;

    const classGroupId = numOrNull(pickFirst(node.class_group_id, node.classGroupId));
    if (classGroupId !== null) {
      const groupRow = {
        class_group_id: classGroupId,
        group_name: pickFirst(node.group_name, node.groupName, node.name),
        class_group_sequence: numOrNull(pickFirst(node.class_group_sequence, node.group_sequence, node.groupSequence)),
        show_id: showId,
        show_date: showDate,
        show_day_key: showDayKey,
        show_ring_key: showRingKey,
        ring_number: ringNumber,
        group_has_warmup: pickFirst(node.group_has_warmup, node.groupHasWarmup),
        is_open_card_warmup: pickFirst(node.is_open_card_warmup, node.isOpenCardWarmup),
        grouped_class: pickFirst(classRelated?.grouped_class, classRelated?.groupedClass, node.grouped_class, node.groupedClass),
      };

      const key = normalizeKey(classGroupId);
      groupsById.set(key, mergePreserveJoinKeys(groupsById.get(key), groupRow));
    }

    if (isClassNode(node)) {
      const classGroupXClassesId = numOrNull(pickFirst(node.class_groupxclasses_id, node.classGroupXClassesId));
      if (classGroupXClassesId !== null) {
        const classObj = isObj(node.class) ? node.class : undefined;

        const classRow = {
          class_groupxclasses_id: classGroupXClassesId,
          class_group_id: classGroupId !== null ? classGroupId : numOrNull(pickFirst(node.class_group_id, node.classGroupId)),
          class_id: numOrNull(pickFirst(node.class_id, node.classId, node.id, classObj?.class_id, classObj?.classId)),
          class_number: numOrNull(pickFirst(node.class_number, node.classNumber, node.number, classObj?.number)),
          class_name: strOrNull(pickFirst(node.class_name, node.className, node.name, classObj?.name)),
          class_list: strOrNull(pickFirst(node.class_list, node.classList)),
          class_type: strOrNull(pickFirst(node.class_type, node.classType, classObj?.class_type, classObj?.classType)),
          jumper_table: strOrNull(pickFirst(node.jumper_table, node.jumperTable, classObj?.jumper_table, classObj?.jumperTable)),
          sponsor: strOrNull(pickFirst(node.sponsor, classObj?.sponsor)),
          schedule_sequencetype: strOrNull(
            pickFirst(
              node.schedule_sequencetype,
              node.scheduleSequenceType,
              node.sequencetype,
              node.sequence_type,
              classObj?.schedule_sequencetype,
              classObj?.scheduleSequenceType
            )
          ),
          group_has_warmup: pickFirst(node.group_has_warmup, node.groupHasWarmup),
          is_open_card_warmup: pickFirst(node.is_open_card_warmup, node.isOpenCardWarmup),
          show_id: showId,
          show_date: showDate,
          show_day_key: showDayKey,
          show_ring_key: showRingKey,
          ring_number: ringNumber,
          estimated_start_time: strOrNull(pickFirst(node.estimated_start_time, node.estimatedStartTime)),
          start_time_default: strOrNull(pickFirst(node.start_time_default, node.startTimeDefault)),
          estimated_end_time: strOrNull(pickFirst(node.estimated_end_time, node.estimatedEndTime)),
          grouped_class: pickFirst(classRelated?.grouped_class, classRelated?.groupedClass, node.grouped_class, node.groupedClass),
        };

        const key = normalizeKey(classGroupXClassesId);
        classesById.set(key, mergePreserveJoinKeys(classesById.get(key), classRow));
      }
    }

    for (const value of Object.values(node)) {
      if (typeof value === "string") continue;
      walk(value, next);
    }
  }

  walk(payload, {
    show_id: payloadShowId,
    show_date: payloadShowDate,
  });

  const rows = [];
  for (const [key, classRow] of classesById.entries()) {
    if (!key) continue;
    const groupRow = groupsById.get(normalizeKey(classRow.class_group_id));
    const merged = mergeGroupOntoClass(classRow, groupRow);
    const fields = {};

    setIfPresent(fields, "class_groupxclasses_id", merged.class_groupxclasses_id);
    setIfPresent(fields, "class_group_id", merged.class_group_id);
    setIfPresent(fields, "class_id", merged.class_id);
    setIfPresent(fields, "show_id", merged.show_id);
    setIfPresent(fields, "show_date", merged.show_date);
    setIfPresent(fields, "ring_number", merged.ring_number);
    setIfPresent(fields, "class_group_sequence", merged.class_group_sequence);
    setIfPresent(fields, "group_name", merged.group_name);
    setIfPresent(fields, "class_number", merged.class_number);
    setIfPresent(fields, "class_name", merged.class_name);
    setIfPresent(fields, "class_type", merged.class_type);
    setIfPresent(fields, "schedule_sequencetype", merged.schedule_sequencetype);
    setIfPresent(fields, "estimated_start_time", merged.estimated_start_time || merged.start_time_default);
    setIfPresent(fields, "estimated_end_time", merged.estimated_end_time);
    setIfPresent(fields, "app_show_idv2", scope.app_show_idv2);
    setIfPresent(fields, "app_sql_datev2", scope.app_sql_datev2);
    setIfPresent(fields, "app_dow_rawv2", scope.app_dow_rawv2);
    setIfPresent(fields, "shifted_to_next_dayv2", scope.shifted_to_next_dayv2);
    setIfPresent(fields, "is_current_scope", true);
    setIfPresent(fields, "scope_run_id", scope.scope_run_id);
    setIfPresent(fields, "scope_status", "current");
    setIfPresent(fields, "last_seen_at", generatedDate);
    setIfPresent(fields, "schedule_show_datev2", scheduleShowDate);
    setIfPresent(fields, "schedule_show_display_datev2", scheduleShowDisplayDate);
    setIfPresent(fields, "schedule_show_display_date_dayv2", scheduleShowDisplayDateDay);
    setIfPresent(fields, "latest_ingested_at", generatedAt);

    rows.push({
      key,
      fields,
      refs: {
        heartbeat_record_id: scope.heartbeat_record_id,
        schedule_endpoint: buildScheduleEndpoint(scope.app_sql_datev2, scope.app_show_idv2),
        schedule_empty_endpoint: buildScheduleEmptyEndpoint(scope.app_show_idv2),
        classes_endpointv2: buildClassesEndpoint(merged.class_id, scope.app_show_idv2),
        chosen_source: source,
      },
    });
  }

  return {
    source,
    payload_show_id: payloadShowId,
    payload_show_date: payloadShowDate,
    schedule_show_datev2: scheduleShowDate,
    schedule_show_display_datev2: scheduleShowDisplayDate,
    schedule_show_display_date_dayv2: scheduleShowDisplayDateDay,
    groups_count: groupsById.size,
    classes_count: classesById.size,
    rows,
    keep_keys: rows.map((row) => row.key),
  };
}

async function fetchScheduleVariant(label, url, scope, generatedAt, generatedDate) {
  if (!url) {
    return {
      ok: false,
      source: label,
      url: null,
      error: "missing_url",
      rows: [],
      keep_keys: [],
    };
  }

  try {
    const payload = await fetchJson(url);
    const normalized = normalizeSchedulePayload(payload, {
      scope,
      source: label,
      generatedAt,
      generatedDate,
    });

    return {
      ok: true,
      source: label,
      url,
      ...normalized,
    };
  } catch (error) {
    return {
      ok: false,
      source: label,
      url,
      error: String(error?.message || error),
      rows: [],
      keep_keys: [],
    };
  }
}

function chooseScheduleVariant(datedResult, emptyResult) {
  if (datedResult.ok && datedResult.rows.length > 0) return datedResult;
  if (emptyResult.ok && emptyResult.rows.length > 0) return emptyResult;
  if (datedResult.ok) return datedResult;
  if (emptyResult.ok) return emptyResult;
  throw new Error(`Both schedule fetches failed :: dated=${datedResult.error || "unknown"} :: empty=${emptyResult.error || "unknown"}`);
}

function escapeFormulaString(value) {
  return String(value).replace(/\\/g, "\\\\").replace(/"/g, '\\"');
}

function stableStringify(value) {
  return JSON.stringify(value, null, OUTPUT_PRETTY ? 2 : 0);
}

async function runNormalizer() {
  requireEnv("AIRTABLE_TOKEN", AIRTABLE_TOKEN);
  requireEnv("AIRTABLE_BASE_ID", AIRTABLE_BASE_ID);

  const generatedAt = new Date().toISOString();
  const generatedDate = generatedAt.slice(0, 10);

  debug("fetch_schedule_heartbeat_rows:start");
  const scheduleHeartbeatRows = await fetchScheduleHeartbeatRows();
  debug("fetch_schedule_heartbeat_rows:done", { rows: scheduleHeartbeatRows.length });
  const scheduleHeartbeatScope = buildScopeFromScheduleHeartbeatRows(scheduleHeartbeatRows);

  if (!scheduleHeartbeatScope) {
    throw new Error(`No scoped rows available in ${TABLE_WATCH_SCHEDULE}/${VIEW_WATCH_SCHEDULE_HEARTBEAT}`);
  }

  const heartbeatRecord = null;
  const scope = scheduleHeartbeatScope;
  const existingRows = scheduleHeartbeatRows;

  const datedUrl = buildScheduleEndpoint(scope.app_sql_datev2, scope.app_show_idv2);
  const emptyUrl = buildScheduleEmptyEndpoint(scope.app_show_idv2);

  debug("fetch_variant:start", { variant: "dated_schedule" });
  const datedResult = await fetchScheduleVariant("dated_schedule", datedUrl, scope, generatedAt, generatedDate);
  debug("fetch_variant:done", { variant: "dated_schedule", ok: datedResult.ok, rows: datedResult.rows.length });
  debug("fetch_variant:start", { variant: "empty_ping_schedule" });
  const emptyResult = await fetchScheduleVariant("empty_ping_schedule", emptyUrl, scope, generatedAt, generatedDate);
  debug("fetch_variant:done", { variant: "empty_ping_schedule", ok: emptyResult.ok, rows: emptyResult.rows.length });
  const chosen = chooseScheduleVariant(datedResult, emptyResult);

  const existingKeys = existingRows
    .map((record) => normalizeKey(record?.fields?.class_groupxclasses_id))
    .filter(Boolean);

  const keepKeys = chosen.keep_keys.slice();
  const existingKeySet = new Set(existingKeys);
  const keepKeySet = new Set(keepKeys);
  const addedKeys = keepKeys.filter((key) => !existingKeySet.has(key));
  const droppedKeys = existingKeys.filter((key) => !keepKeySet.has(key));
  const unchangedKeys = keepKeys.filter((key) => existingKeySet.has(key));

  const rowsByKey = {};
  for (const row of chosen.rows) rowsByKey[row.key] = row;

  debug("run_normalizer:return", {
    chosen_source: chosen.source,
    row_count: chosen.rows.length,
    existing_scope_row_count: existingRows.length,
  });
  return {
    ok: true,
    generated_at: generatedAt,
    scope,
    heartbeat: {
      source: "watch_schedule_heartbeat_view",
      record_id: scope.heartbeat_record_id || null,
      heartbeat_id: strOrNull(scope.scope_run_id),
      hb_at: strOrNull(scope.hb_at),
      app_show_id: scope.app_show_idv2,
      app_sql_date: scope.app_sql_datev2,
      app_dow_raw: scope.app_dow_rawv2,
      shifted_to_next_day: scope.shifted_to_next_dayv2,
      show_date: scope.heartbeat_show_date,
      time: scope.heartbeat_time,
    },
    endpoints: {
      dated: datedUrl,
      empty: emptyUrl,
    },
    fetches: {
      dated_schedule: {
        ok: datedResult.ok,
        error: datedResult.error || null,
        rows: datedResult.rows.length,
        schedule_show_datev2: datedResult.schedule_show_datev2 || null,
      },
      empty_ping_schedule: {
        ok: emptyResult.ok,
        error: emptyResult.error || null,
        rows: emptyResult.rows.length,
        schedule_show_datev2: emptyResult.schedule_show_datev2 || null,
      },
    },
    chosen_source: chosen.source,
    authoritative: datedResult.ok || emptyResult.ok,
    authoritative_empty: chosen.rows.length === 0,
    existing_keys: existingKeys,
    keep_keys: keepKeys,
    added_keys: addedKeys,
    dropped_keys: droppedKeys,
    unchanged_keys: unchangedKeys,
    existing_scope_row_count: existingRows.length,
    row_count: chosen.rows.length,
    rows_by_key: rowsByKey,
    rows: chosen.rows,
  };
}

async function main() {
  const result = await runNormalizer();
  const summary = summarizeResult(result);
  const text = stableStringify(result);
  const stdoutText = stableStringify(STDOUT_FULL ? result : summary);

  if (OUTPUT_PATH) {
    const resolved = path.isAbsolute(OUTPUT_PATH)
      ? OUTPUT_PATH
      : path.resolve(__dirname, OUTPUT_PATH);
    fs.mkdirSync(path.dirname(resolved), { recursive: true });
    fs.writeFileSync(resolved, `${text}\n`, "utf8");
  }

  process.stdout.write(`${stdoutText}\n`);
  process.exit(0);
}

if (require.main === module) {
  main().catch((error) => {
    const payload = {
      ok: false,
      error: String(error?.message || error),
    };
    process.stdout.write(`${stableStringify(payload)}\n`);
    process.exit(1);
  });
}

module.exports = {
  chooseScheduleVariant,
  normalizeSchedulePayload,
  runNormalizer,
};
