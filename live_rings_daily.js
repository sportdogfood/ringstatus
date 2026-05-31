const crypto = require("crypto");
const { fetchTextWithConfiguredTransport } = require("./lib/sgl_fetch_adapter");

const AIRTABLE_TOKEN = process.env.AIRTABLE_TOKEN || "";
const AIRTABLE_BASE_ID = process.env.AIRTABLE_BASE_ID || "";
const TABLE_SHOW = process.env.TABLE_SHOW_TARGET || process.env.TABLE_SHOW || "show";
const TABLE_HEARTBEAT = process.env.TABLE_HEARTBEAT || "heartbeat";
const TABLE_LIVE_RINGS = process.env.TABLE_LIVE_RINGS || "live_rings";
const TABLE_LIVE_GROUPS = process.env.TABLE_LIVE_GROUPS || "live_groups";
const VIEW_SHOW_HEARTBEAT = process.env.VIEW_SHOW_HEARTBEAT || "heartbeat";
const HEARTBEAT_SORT_FIELD = process.env.HEARTBEAT_SORT_FIELD || "hb_at";
const HTTP_TIMEOUT_MS = Number(process.env.HTTP_TIMEOUT_MS || "20000");
const DRY_RUN = String(process.env.DRY_RUN || "0") === "1";
const LIVE_SCORE_WIDGET_URL = process.env.LIVE_SCORE_WIDGET_URL ||
  "https://sgl.wellingtoninternational.com/iphone.php/esp/webservice/LiveScoreWidget";

const WRITABLE_TYPES = new Set([
  "singleLineText",
  "multilineText",
  "number",
  "checkbox",
  "date",
  "dateTime",
  "multipleRecordLinks",
]);

function isBlank(value) {
  return value === null ||
    value === undefined ||
    (typeof value === "string" && value.trim() === "") ||
    String(value).trim().toLowerCase() === "null" ||
    String(value).trim().toLowerCase() === "nan";
}

function firstValue(value) {
  if (Array.isArray(value)) {
    for (const item of value) {
      if (!isBlank(item)) return item;
    }
    return undefined;
  }
  return value;
}

function strOrNull(value) {
  const picked = firstValue(value);
  if (isBlank(picked)) return null;
  return String(picked).trim();
}

function numOrNull(value) {
  const picked = firstValue(value);
  if (isBlank(picked)) return null;
  const num = Number(picked);
  return Number.isFinite(num) ? num : null;
}

function toIsoDateOnly(value) {
  const picked = firstValue(value);
  if (isBlank(picked)) return null;
  if (picked instanceof Date) return picked.toISOString().slice(0, 10);
  const text = String(picked).trim();
  if (/^\d{4}-\d{2}-\d{2}$/.test(text)) return text;
  if (/^\d{4}-\d{2}-\d{2}T/.test(text)) return text.slice(0, 10);
  const parsed = Date.parse(text);
  return Number.isFinite(parsed) ? new Date(parsed).toISOString().slice(0, 10) : null;
}

function buildRingKey({ customer_id, customerId, show_id, showId, focus_day, focusDay, ring_number, ringNumber }) {
  const customer = numOrNull(customer_id ?? customerId);
  const show = numOrNull(show_id ?? showId);
  const focus = toIsoDateOnly(focus_day ?? focusDay);
  const ring = numOrNull(ring_number ?? ringNumber);
  if (customer === null || show === null || !focus || ring === null) return "";
  return [customer, show, focus, ring].join("|");
}

function snapshotKeyFromQueryKey(queryKey, asOf) {
  const stamp = strOrNull(asOf);
  if (!queryKey || !stamp) return "";
  return `${queryKey}|${stamp}`;
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
      body: JSON.stringify({ records: records.slice(i, i + 10), typecast: true }),
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
      body: JSON.stringify({ records: records.slice(i, i + 10), typecast: true }),
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
    if (WRITABLE_TYPES.has(field.type)) names.add(name);
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

function stableHash(value) {
  return crypto.createHash("sha1").update(JSON.stringify(value ?? null)).digest("hex");
}

function linkOne(recordId) {
  return recordId ? [recordId] : undefined;
}

function liveGroupLinkKey(showId, focusDay, ringNumber, classGroupId) {
  if (showId === null || !focusDay || ringNumber === null || classGroupId === null) return "";
  return [showId, focusDay, ringNumber, classGroupId].join("|");
}

function pickRingItem(ringData, bucketName) {
  const rows = Array.isArray(ringData?.[bucketName]) ? ringData[bucketName] : [];
  return rows[0] || null;
}

function ringStateFromBuckets(liveItem, nextItem, completedRows) {
  if (liveItem) return "live";
  if (nextItem) return "upcoming";
  if (Array.isArray(completedRows) && completedRows.length) return "completed";
  return "idle";
}

function progressText(item) {
  const gone = numOrNull(item?.gone);
  const total = numOrNull(item?.total);
  if (gone === null || total === null) return null;
  return `${gone}/${total}`;
}

function normalizeLiveRingSnapshots(payload, context = {}) {
  const rows = [];
  const shows = Array.isArray(payload) ? payload : [];
  const customerId = numOrNull(context.customer_id ?? context.customerId);
  const focusDay = toIsoDateOnly(context.focus_day ?? context.focusDay);
  const asOf = strOrNull(context.as_of ?? context.asOf) || new Date().toISOString();
  const liveGroupLinks = context.liveGroupLinks instanceof Map ? context.liveGroupLinks : new Map();

  for (const show of shows) {
    const showId = numOrNull(show?.show_id) ?? numOrNull(context.show_id ?? context.showId);
    if (showId === null || !focusDay) continue;
    const liveData = show?.live_data && typeof show.live_data === "object" ? show.live_data : {};

    for (const [ringNumberText, ringData] of Object.entries(liveData)) {
      const ringNumber = numOrNull(ringData?.ring_number) ?? numOrNull(ringNumberText);
      if (ringNumber === null) continue;

      const liveItem = pickRingItem(ringData, "livenow");
      const nextItem = pickRingItem(ringData, "upcoming");
      const completedRows = Array.isArray(ringData?.completed) ? ringData.completed : [];
      const liveClassGroupId = numOrNull(liveItem?.class_group_id);
      const nextClassGroupId = numOrNull(nextItem?.class_group_id);
      const liveLink = liveGroupLinks.get(liveGroupLinkKey(showId, focusDay, ringNumber, liveClassGroupId));
      const nextLink = liveGroupLinks.get(liveGroupLinkKey(showId, focusDay, ringNumber, nextClassGroupId));
      const ringId = numOrNull(liveItem?.ring_id) ?? numOrNull(nextItem?.ring_id) ?? numOrNull(ringData?.ring_id);
      const ringName = strOrNull(ringData?.name) || strOrNull(liveItem?.ring) || strOrNull(nextItem?.ring);
      const ringKey = buildRingKey({
        customer_id: customerId,
        show_id: showId,
        focus_day: focusDay,
        ring_number: ringNumber,
      });
      const snapshotKey = snapshotKeyFromQueryKey(ringKey, asOf);

      const fields = {
        ring_key: snapshotKey,
        response_ready: true,
        is_latest: true,
        show: linkOne(context.show_record_id ?? context.showRecordId),
        show_id: showId,
        focus_day: focusDay,
        ring_number: ringNumber,
        ring_id: ringId,
        ring_name: ringName,
        is_current_scope: true,
        dropped_at: null,
        as_of: asOf,
        last_seen_at: asOf,
        payload_hash: stableHash(ringData),
        ring_query_key: ringKey,
        live_group: linkOne(liveLink),
        live_class_group_id: liveClassGroupId,
        live_status: strOrNull(liveItem?.status),
        live_start_time: strOrNull(liveItem?.estimated_start_time),
        live_gone: numOrNull(liveItem?.gone),
        live_total: numOrNull(liveItem?.total),
        live_progress: progressText(liveItem),
        next_group: linkOne(nextLink),
        next_class_group_id: nextClassGroupId,
        next_start_time: strOrNull(nextItem?.estimated_start_time),
        ring_state: ringStateFromBuckets(liveItem, nextItem, completedRows),
        snapshot_json: JSON.stringify({
          live: liveItem || null,
          next: nextItem || null,
          completed_count: completedRows.length,
          upcoming_count: Array.isArray(ringData?.upcoming) ? ringData.upcoming.length : 0,
          livenow_count: Array.isArray(ringData?.livenow) ? ringData.livenow.length : 0,
        }),
        snapshot_hash: stableHash({ live: liveItem, next: nextItem, completed_count: completedRows.length }),
      };

      for (const [key, value] of Object.entries(fields)) {
        if (value === undefined || value === null) delete fields[key];
      }
      fields.dropped_at = null;

      rows.push({
        key: snapshotKey,
        query_key: ringKey,
        fields,
        snapshot_hash: fields.snapshot_hash,
      });
    }
  }

  return rows;
}

function normalizeMode(value) {
  const text = strOrNull(value);
  return text ? text.toUpperCase() : "";
}

async function resolveHeartbeatShow() {
  const rows = await airtableList(TABLE_SHOW, {
    view: VIEW_SHOW_HEARTBEAT,
    pageSize: 100,
    "fields[]": ["show_id", "customer_id", "focus_day", "heartbeat"],
  });
  const scoped = rows.filter((row) => {
    const fields = row.fields || {};
    return firstValue(fields.heartbeat) === true || String(firstValue(fields.heartbeat)).toLowerCase() === "true";
  });
  if (scoped.length !== 1) {
    throw new Error(`Expected exactly one ${TABLE_SHOW}/${VIEW_SHOW_HEARTBEAT} row for live_rings, found ${scoped.length}`);
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
    "fields[]": ["mode", "show_id", "app_show_id", "app_sql_date", "sql_date", "focus_day", "show", "hb_at"],
  });
  return rows.find((row) => {
    const fields = row.fields || {};
    const rowShowId = numOrNull(fields.show_id ?? fields.app_show_id);
    const rowDay = toIsoDateOnly(fields.focus_day ?? fields.app_sql_date ?? fields.sql_date);
    const linkedShow = Array.isArray(fields.show) ? fields.show.map(String) : [];
    return rowShowId === scope.show_id && rowDay === scope.focus_day &&
      (!linkedShow.length || linkedShow.includes(scope.record_id));
  }) || null;
}

async function fetchText(url) {
  return fetchTextWithConfiguredTransport(url, async (targetUrl) => {
    const response = await fetchWithTimeout(targetUrl);
    const text = await response.text();
    if (!response.ok) {
      throw new Error(`Fetch failed ${response.status} ${targetUrl}: ${text.slice(0, 500)}`);
    }
    return { text, response, endpoint: targetUrl, originalEndpoint: url, transport: "node_fetch" };
  });
}

function parseJsonText(text, endpoint) {
  try {
    return JSON.parse(text);
  } catch (error) {
    throw new Error(`Invalid JSON from ${endpoint}: ${String(error?.message || error)}`);
  }
}

async function buildLiveGroupLinks(scope) {
  const rows = await airtableList(TABLE_LIVE_GROUPS, {
    pageSize: 100,
    filterByFormula: `AND({show_id}=${Number(scope.show_id)},{customer_id}=${Number(scope.customer_id)},DATETIME_FORMAT({live_focus_day}, 'YYYY-MM-DD')='${scope.focus_day}')`,
    "fields[]": ["class_group_id", "show_id", "live_focus_day", "ring_number"],
  });
  const links = new Map();
  for (const row of rows) {
    const fields = row.fields || {};
    const key = liveGroupLinkKey(
      numOrNull(fields.show_id),
      toIsoDateOnly(fields.live_focus_day),
      numOrNull(fields.ring_number),
      numOrNull(fields.class_group_id)
    );
    if (key) links.set(key, row.id);
  }
  return links;
}

async function writeLiveRingSnapshots(rows, writable) {
  if (!rows.length) return { created: 0, updated: 0, dropped: 0 };
  const showId = rows[0].fields.show_id;
  const focusDay = rows[0].fields.focus_day;
  const currentQueryKeys = new Set(rows.map((row) => row.query_key).filter(Boolean));
  const existingRows = await airtableList(TABLE_LIVE_RINGS, {
    pageSize: 100,
    filterByFormula: `{show_id}=${Number(showId)}`,
    "fields[]": [
      "ring_key",
      "ring_query_key",
      "show_id",
      "focus_day",
      "is_current_scope",
      "is_latest",
      "response_ready",
      "dropped_at",
    ],
  });

  const creates = rows.map((row) => ({ fields: pickWritable(row.fields, writable) }));
  const dropped = [];

  for (const row of existingRows) {
    const queryKey = strOrNull(row.fields?.ring_query_key) || strOrNull(row.fields?.ring_key);
    const rowFocusDay = toIsoDateOnly(row.fields?.focus_day);
    if (!queryKey) continue;
    if (currentQueryKeys.has(queryKey) && rowFocusDay === focusDay) {
      const fields = {};
      if (writable.has("is_latest")) fields.is_latest = false;
      if (writable.has("response_ready")) fields.response_ready = false;
      if (Object.keys(fields).length) dropped.push({ id: row.id, fields });
      continue;
    }
    const fields = {};
    if (writable.has("is_current_scope")) fields.is_current_scope = false;
    if (writable.has("is_latest")) fields.is_latest = false;
    if (writable.has("response_ready")) fields.response_ready = false;
    if (writable.has("dropped_at")) fields.dropped_at = new Date().toISOString().slice(0, 10);
    if (Object.keys(fields).length) dropped.push({ id: row.id, fields });
  }

  await airtableCreate(TABLE_LIVE_RINGS, creates);
  await airtableUpdate(TABLE_LIVE_RINGS, dropped);
  return { created: creates.length, updated: 0, dropped: dropped.length };
}

async function main() {
  if (!AIRTABLE_TOKEN) throw new Error("Missing required env: AIRTABLE_TOKEN");
  if (!AIRTABLE_BASE_ID) throw new Error("Missing required env: AIRTABLE_BASE_ID");

  const scope = await resolveHeartbeatShow();
  const heartbeat = await latestHeartbeatForScope(scope);
  const mode = normalizeMode(heartbeat?.fields?.mode);
  if (mode !== "DAY") {
    const skipped = {
      ok: true,
      event: "live_rings_skipped",
      reason: "mode_not_day",
      mode,
      show_id: scope.show_id,
      focus_day: scope.focus_day,
    };
    console.log(JSON.stringify(skipped));
    return skipped;
  }

  const result = await fetchText(LIVE_SCORE_WIDGET_URL);
  const payload = parseJsonText(result.text, result.endpoint);
  const liveGroupLinks = await buildLiveGroupLinks(scope).catch(() => new Map());
  const rows = normalizeLiveRingSnapshots(payload, {
    customer_id: scope.customer_id,
    show_id: scope.show_id,
    focus_day: scope.focus_day,
    show_record_id: scope.record_id,
    as_of: new Date().toISOString(),
    liveGroupLinks,
  });

  const fieldMap = await tableFieldMap(TABLE_LIVE_RINGS);
  const writable = writableFields(fieldMap);
  const writeResult = await writeLiveRingSnapshots(rows, writable);
  const summary = {
    ok: true,
    event: "live_rings_snapshots_written",
    show_id: scope.show_id,
    customer_id: scope.customer_id,
    focus_day: scope.focus_day,
    rows: rows.length,
    created: writeResult.created,
    updated: writeResult.updated,
    dropped: writeResult.dropped,
    endpoint: result.endpoint,
    dry_run: DRY_RUN,
  };
  console.log(JSON.stringify(summary));
  return summary;
}

module.exports = {
  LIVE_SCORE_WIDGET_URL,
  TABLE_LIVE_RINGS,
  buildRingKey,
  main,
  normalizeLiveRingSnapshots,
  snapshotKeyFromQueryKey,
};

if (require.main === module) {
  main().catch((error) => {
    console.error(JSON.stringify({
      ok: false,
      event: "live_rings_failed",
      error: String(error?.stack || error?.message || error).slice(0, 5000),
    }));
    process.exit(1);
  });
}
