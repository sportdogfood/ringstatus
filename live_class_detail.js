// only keeps linked watch_trips from getLiveClassData; unrelated riders are discarded.
const { fetchTextWithConfiguredTransport } = require("./lib/sgl_fetch_adapter");

const AIRTABLE_TOKEN = process.env.AIRTABLE_TOKEN || "";
const AIRTABLE_BASE_ID = process.env.AIRTABLE_BASE_ID || "";
const TABLE_HEARTBEAT = process.env.TABLE_HEARTBEAT || "heartbeat";
const TABLE_LIVE_GROUPS = process.env.TABLE_LIVE_GROUPS || "live_groups";
const TABLE_WATCH_TRIPS = process.env.TABLE_WATCH_TRIPS || "watch_trips";
const TABLE_LIVE_CLASSES = process.env.TABLE_LIVE_CLASSES || "live_classes";
const VIEW_HAS_JSON = process.env.VIEW_LIVE_GROUPS_HAS_JSON || "has_json";
const VIEW_IS_LIVE = process.env.VIEW_LIVE_GROUPS_IS_LIVE || "is_live";
const HEARTBEAT_SORT_FIELD = process.env.HEARTBEAT_SORT_FIELD || "hb_at";
const HEARTBEAT_CREATED_FIELD = process.env.HEARTBEAT_CREATED_FIELD || "created_time";
const HEARTBEAT_MODE_FIELD = process.env.HEARTBEAT_MODE_FIELD || process.env.FIELD_MODE || "mode";
const DEFAULT_HAS_JSON_SLOTS = "A,C";
const DEFAULT_IS_LIVE_SLOTS = "A,B,C,D";
const DISABLED = String(process.env.LIVE_CLASS_DETAIL_DISABLED || "0") === "1";
const DRY_RUN = String(process.env.DRY_RUN || "0") === "1";
const HTTP_TIMEOUT_MS = Number(process.env.HTTP_TIMEOUT_MS || "20000");
const RUN_AT = new Date().toISOString();
const RUN_ID = Date.now();

const LIVE_BASE_URL = String(
  process.env.SGL_LIVE_BASE_URL ||
  "https://sgl.wellingtoninternational.com"
).trim().replace(/\/+$/, "");

const WRITABLE_TYPES = new Set([
  "singleLineText",
  "multilineText",
  "number",
  "checkbox",
  "date",
  "dateTime",
  "multipleRecordLinks",
]);

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
  return ["true", "1", "yes", "checked"].includes(text);
}

function asArray(value) {
  if (Array.isArray(value)) return value;
  if (value === null || value === undefined) return [];
  return [value];
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

function normalizeName(value) {
  return String(value || "")
    .trim()
    .toUpperCase()
    .replace(/\s+/g, " ");
}

function splitStoredList(value) {
  return String(value || "")
    .split(/[,\n]/)
    .map((item) => item.trim())
    .filter(Boolean);
}

function numberList(value) {
  return splitStoredList(value)
    .map((item) => Number(item))
    .filter((item) => Number.isFinite(item));
}

function parseSlotSet(value, fallback) {
  return new Set(
    String(value || fallback || "")
      .split(",")
      .map((item) => item.trim().toUpperCase())
      .filter(Boolean)
  );
}

function slotFromFields(fields = {}) {
  const active = [
    fields.isA ? "A" : null,
    fields.isB ? "B" : null,
    fields.isC ? "C" : null,
    fields.isD ? "D" : null,
  ].filter(Boolean);
  return active.length === 1 ? active[0] : null;
}

function airtableHeaders(extra = {}) {
  return {
    Authorization: `Bearer ${AIRTABLE_TOKEN}`,
    "Content-Type": "application/json",
    ...extra,
  };
}

function airtableUrl(tableName, params = {}, recordId = null) {
  const encodedTable = encodeURIComponent(tableName);
  const suffix = recordId ? `/${encodeURIComponent(recordId)}` : "";
  const url = new URL(`https://api.airtable.com/v0/${AIRTABLE_BASE_ID}/${encodedTable}${suffix}`);
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

async function airtableRecord(tableName, recordId) {
  return airtableJson(airtableUrl(tableName, {}, recordId));
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

async function airtableCreate(tableName, records) {
  if (!records.length) return [];
  if (DRY_RUN) return records.map((_, index) => ({ id: `dry_log_${index}` }));
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

async function latestHeartbeat() {
  const rows = await airtableList(TABLE_HEARTBEAT, {
    maxRecords: 1,
    "sort[0][field]": HEARTBEAT_SORT_FIELD || HEARTBEAT_CREATED_FIELD,
    "sort[0][direction]": "desc",
    "fields[]": [HEARTBEAT_MODE_FIELD, "isA", "isB", "isC", "isD", HEARTBEAT_SORT_FIELD],
  });
  return rows[0] || null;
}

function selectedViewsForSlot(slot) {
  const views = [];
  if (parseSlotSet(process.env.LIVE_CLASS_DETAIL_HAS_JSON_SLOTS, DEFAULT_HAS_JSON_SLOTS).has(slot)) {
    views.push({ name: VIEW_HAS_JSON, source: "has_json", fields: ["OOG", "Actual_OOG", "Gone"] });
  }
  if (parseSlotSet(process.env.LIVE_CLASS_DETAIL_IS_LIVE_SLOTS, DEFAULT_IS_LIVE_SLOTS).has(slot)) {
    views.push({ name: VIEW_IS_LIVE, source: "is_live", fields: ["Scr", "Pos", "Gone"] });
  }
  return views;
}

async function listLiveGroups(viewName) {
  return airtableList(TABLE_LIVE_GROUPS, {
    view: viewName,
    pageSize: 100,
    "fields[]": [
      "live_groups_key",
      "show_id",
      "live_focus_day",
      "class_group_id",
      "class_ids",
      "class_numbers",
      "watch_trips",
      "has_JSON",
      "is_live",
      "status",
      "is_cuurent_scope",
      "dropped_at",
    ],
  });
}

function classDetailEndpoint(showId, classId) {
  return `${LIVE_BASE_URL}/iphonev2/index.php/esp/liveclassv2/getLiveClassData?show_id=${encodeURIComponent(showId)}&cid=${encodeURIComponent(classId)}&t=${RUN_ID}`;
}

async function fetchText(url) {
  return fetchTextWithConfiguredTransport(url, async (targetUrl) => {
    const response = await fetchWithTimeout(targetUrl);
    const text = await response.text();
    if (!response.ok) throw new Error(`Fetch failed ${response.status} ${targetUrl}: ${text.slice(0, 500)}`);
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

function updateTargetsForSource(source) {
  if (source === "is_live") {
    return [
      { payload: "Scr", canonical: "scr", aliases: ["scr", "scratch_trip"] },
      { payload: "Pos", canonical: "pos", aliases: ["pos", "lastPosition"] },
      { payload: "Gone", canonical: "gone", aliases: ["gone", "gone_in", "rs_gone_in"] },
    ];
  }
  return [
    { payload: "OOG", canonical: "oog", aliases: ["oog", "rider_running_oog", "rs_running_order_of_go"] },
    { payload: "Actual_OOG", canonical: "actual_oog", aliases: ["actual_oog", "actual_order"] },
    { payload: "Gone", canonical: "gone", aliases: ["gone", "gone_in", "rs_gone_in"] },
  ];
}

function firstWritableAlias(aliases, writableSet) {
  return aliases.find((field) => writableSet.has(field)) || null;
}

function valuesDiffer(oldValue, newValue) {
  const oldNum = numOrNull(oldValue);
  const newNum = numOrNull(newValue);
  if (newNum === null) return false;
  if (oldNum === null) return true;
  return oldNum !== newNum;
}

function tripMatchesPayloadRow(trip, payloadRow, classId) {
  const fields = trip.fields || {};
  const tripClassId = numOrNull(fields.class_id);
  if (tripClassId !== null && tripClassId !== Number(classId)) return false;

  const entryNumber = numOrNull(fields.entry_number);
  const payloadEntryNumber = numOrNull(payloadRow.ENo);
  if (entryNumber !== null && payloadEntryNumber !== null && entryNumber === payloadEntryNumber) return true;

  const riderMatch = normalizeName(fields.rider_name || fields.riderName) &&
    normalizeName(fields.rider_name || fields.riderName) === normalizeName(payloadRow.Rid);
  const horseMatch = normalizeName(fields.horse || fields.horse_name || fields.horseName) &&
    normalizeName(fields.horse || fields.horse_name || fields.horseName) === normalizeName(payloadRow.Hor);
  return riderMatch && horseMatch;
}

function buildLogFields({ liveGroup, trip, classId, source, endpoint, payloadRow, canonical, oldValue, newValue }) {
  const tripFields = trip.fields || {};
  const liveFields = liveGroup.fields || {};
  return {
    log_key: [
      "live_class_detail",
      source,
      trip.id,
      classId,
      canonical,
      strOrNull(newValue) || "",
      RUN_ID,
    ].join("|"),
    source_view: source,
    live_groups: [liveGroup.id],
    watch_trips: [trip.id],
    show_id: numOrNull(liveFields.show_id),
    focus_day: toIsoDateOnly(liveFields.live_focus_day),
    class_group_id: numOrNull(liveFields.class_group_id),
    class_id: numOrNull(classId),
    entry_number: numOrNull(tripFields.entry_number ?? payloadRow.ENo),
    rider_name: strOrNull(tripFields.rider_name ?? payloadRow.Rid),
    horse: strOrNull(tripFields.horse ?? payloadRow.Hor),
    payload_row_id: strOrNull(payloadRow.id),
    oog: numOrNull(payloadRow.OOG),
    actual_oog: numOrNull(payloadRow.Actual_OOG),
    gone: numOrNull(payloadRow.Gone),
    scr: numOrNull(payloadRow.Scr),
    pos: numOrNull(payloadRow.Pos),
    field_changed: canonical,
    old_value: oldValue === null || oldValue === undefined ? "" : String(oldValue),
    new_value: newValue === null || newValue === undefined ? "" : String(newValue),
    detail_fetched_at: RUN_AT,
    run_tag: `live_class_detail|${RUN_ID}|${source}`,
    class_detail_endpoint: endpoint,
  };
}

function updateWatchTripsFirst({ liveGroup, trip, payloadRow, classId, source, endpoint, watchWritable, logWritable }) {
  const updates = {};
  const logs = [];

  for (const target of updateTargetsForSource(source)) {
    const field = firstWritableAlias(target.aliases, watchWritable);
    const newValue = numOrNull(payloadRow[target.payload]);
    if (!field || newValue === null) continue;

    const oldValue = trip.fields?.[field];
    if (!valuesDiffer(oldValue, newValue)) continue;
    updates[field] = newValue;

    logs.push({
      fields: pickWritable(buildLogFields({
        liveGroup,
        trip,
        classId,
        source,
        endpoint,
        payloadRow,
        canonical: target.canonical,
        oldValue,
        newValue,
      }), logWritable),
    });
  }

  return { updates, logs };
}

async function fetchLinkedTrips(ids) {
  const out = [];
  for (const id of ids) {
    try {
      out.push(await airtableRecord(TABLE_WATCH_TRIPS, id));
    } catch (error) {
      console.warn(JSON.stringify({ ok: false, event: "linked_trip_fetch_failed", id, error: String(error?.message || error).slice(0, 500) }));
    }
  }
  return out;
}

function tripIsActionable(trip) {
  const fields = trip.fields || {};
  if (boolValue(fields.archive) || boolValue(fields.inactive) || strOrNull(fields.dropped_at)) return false;
  const gone = numOrNull(fields.gone ?? fields.gone_in ?? fields.rs_gone_in);
  const scr = numOrNull(fields.scr ?? fields.scratch_trip);
  return gone !== 1 && scr !== 1;
}

async function processLiveGroup(liveGroup, source, watchWritable, logWritable) {
  const fields = liveGroup.fields || {};
  const linkedTripIds = asArray(fields.watch_trips).map(String).filter(Boolean);
  if (!linkedTripIds.length) {
    return { pings: 0, matched: 0, trip_updates: 0, logs: 0, skipped_no_linked_trips: 1, skipped_no_actionable_trips: 0, skipped_missing_mapping: 0 };
  }

  const linkedTrips = (await fetchLinkedTrips(linkedTripIds)).filter(tripIsActionable);
  if (!linkedTrips.length) {
    return { pings: 0, matched: 0, trip_updates: 0, logs: 0, skipped_no_linked_trips: 0, skipped_no_actionable_trips: 1, skipped_missing_mapping: 0 };
  }

  const showId = numOrNull(fields.show_id);
  const classIds = numberList(fields.class_ids);
  if (showId === null || !classIds.length) {
    return { pings: 0, matched: 0, trip_updates: 0, logs: 0, skipped_no_linked_trips: 0, skipped_no_actionable_trips: 0, skipped_missing_mapping: 1 };
  }

  let pings = 0;
  let matched = 0;
  const tripUpdates = [];
  const logs = [];

  for (const classId of classIds) {
    const endpoint = classDetailEndpoint(showId, classId);
    const result = await fetchText(endpoint);
    const payload = parseJsonText(result.text, result.endpoint);
    pings += 1;

    for (const payloadRow of asArray(payload.rows)) {
      const matchingTrips = linkedTrips.filter((trip) => tripMatchesPayloadRow(trip, payloadRow, classId));
      for (const trip of matchingTrips) {
        matched += 1;
        const change = updateWatchTripsFirst({ liveGroup, trip, payloadRow, classId, source, endpoint, watchWritable, logWritable });
        if (Object.keys(change.updates).length) {
          tripUpdates.push({ id: trip.id, fields: change.updates });
          Object.assign(trip.fields, change.updates);
        }
        logs.push(...change.logs);
      }
    }
  }

  await airtableUpdate(TABLE_WATCH_TRIPS, tripUpdates);
  try {
    await airtableCreate(TABLE_LIVE_CLASSES, logs);
  } catch (error) {
    console.warn(JSON.stringify({ ok: false, event: "live_class_logs_failed", error: String(error?.message || error).slice(0, 800) }));
  }

  return { pings, matched, trip_updates: tripUpdates.length, logs: logs.length, skipped_no_linked_trips: 0, skipped_no_actionable_trips: 0, skipped_missing_mapping: 0 };
}

async function processView(view) {
  const [watchFieldMap, logFieldMap] = await Promise.all([
    tableFieldMap(TABLE_WATCH_TRIPS),
    tableFieldMap(TABLE_LIVE_CLASSES),
  ]);
  const watchWritable = writableFields(watchFieldMap);
  const logWritable = writableFields(logFieldMap);
  const liveGroups = await listLiveGroups(view.name);

  const totals = {
    rows: liveGroups.length,
    pings: 0,
    matched: 0,
    trip_updates: 0,
    logs: 0,
    skipped_no_linked_trips: 0,
    skipped_no_actionable_trips: 0,
    skipped_missing_mapping: 0,
  };
  for (const liveGroup of liveGroups) {
    const result = await processLiveGroup(liveGroup, view.source, watchWritable, logWritable);
    totals.pings += result.pings;
    totals.matched += result.matched;
    totals.trip_updates += result.trip_updates;
    totals.logs += result.logs;
    totals.skipped_no_linked_trips += result.skipped_no_linked_trips || 0;
    totals.skipped_no_actionable_trips += result.skipped_no_actionable_trips || 0;
    totals.skipped_missing_mapping += result.skipped_missing_mapping || 0;
  }
  return totals;
}

async function main() {
  requireEnv("AIRTABLE_TOKEN", AIRTABLE_TOKEN);
  requireEnv("AIRTABLE_BASE_ID", AIRTABLE_BASE_ID);

  if (DISABLED) {
    const summary = { ok: true, event: "live_class_detail_disabled" };
    console.log(JSON.stringify(summary));
    return summary;
  }

  const heartbeat = await latestHeartbeat();
  const fields = heartbeat?.fields || {};
  const mode = String(process.env.ORCH_CURRENT_MODE || fields[HEARTBEAT_MODE_FIELD] || "").toUpperCase();
  const slot = String(process.env.ORCH_CURRENT_SLOT || slotFromFields(fields) || "").toUpperCase() || null;
  if (mode !== "DAY" || !slot) {
    const summary = { ok: true, event: "live_class_detail_skipped", reason: "mode_or_slot", mode, slot };
    console.log(JSON.stringify(summary));
    return summary;
  }

  const selected = selectedViewsForSlot(slot);
  if (!selected.length) {
    const summary = { ok: true, event: "live_class_detail_skipped", reason: "no_due_views", mode, slot };
    console.log(JSON.stringify(summary));
    return summary;
  }

  const results = [];
  for (const view of selected) {
    const totals = await processView(view);
    results.push({ source: view.source, view: view.name, ...totals });
  }

  const summary = {
    ok: true,
    event: "live_class_detail_completed",
    mode,
    slot,
    dry_run: DRY_RUN,
    results,
  };
  console.log(JSON.stringify(summary));
  return summary;
}

if (require.main === module) {
  main().catch((error) => {
    console.error(JSON.stringify({
      ok: false,
      event: "live_class_detail_failed",
      error: String(error?.stack || error?.message || error).slice(0, 5000),
    }));
    process.exit(1);
  });
}

module.exports = {
  main,
  processView,
  processLiveGroup,
  selectedViewsForSlot,
  tripIsActionable,
  tripMatchesPayloadRow,
  updateTargetsForSource,
  updateWatchTripsFirst,
};
