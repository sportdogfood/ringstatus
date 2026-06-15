const BASE_ID = process.env.WEC_AIRTABLE_BASE_ID || "app6XS1RvsPNRT6os";
const AIRTABLE_TOKEN = process.env.AIRTABLE_TOKEN;
const CATALYST_ENDPOINT = process.env.HORSESHOWING_CATALYST_ENDPOINT ||
  "https://horseshowing-700800454.development.catalystserverless.com/server/horseshowing_sync/";

const SHOW_NO = process.env.WEC_SHOW_NO || process.argv[2] || "14906";

const CLASS_TIMES_FIELDS = [
  { name: "show_no", type: "number", options: { precision: 0 } },
  { name: "ring_day_no", type: "number", options: { precision: 0 } },
  { name: "class_no", type: "number", options: { precision: 0 } },
  { name: "class_label", type: "multilineText" },
  { name: "class_time_text", type: "singleLineText" },
  { name: "class_order", type: "number", options: { precision: 0 } },
  { name: "entry_count", type: "number", options: { precision: 0 } },
  { name: "re_type", type: "singleLineText" },
  { name: "oc_id", type: "singleLineText" },
  { name: "live_flag", type: "checkbox", options: { icon: "check", color: "greenBright" } },
  { name: "source_endpoint", type: "singleLineText" },
  { name: "raw_json", type: "multilineText" },
  { name: "count_text", type: "singleLineText" },
  { name: "current_entry_text", type: "multilineText" },
  { name: "current_entry_no", type: "number", options: { precision: 0 } },
  { name: "current_horse", type: "singleLineText" },
  { name: "entries_gone", type: "number", options: { precision: 0 } },
  { name: "entries_to_go", type: "number", options: { precision: 0 } },
  { name: "source_timestamp", type: "number", options: { precision: 0 } },
  { name: "elapsed_seconds", type: "number", options: { precision: 0 } },
  { name: "last_checked_at", type: "singleLineText" },
  { name: "catalyst_row_id", type: "singleLineText" }
];

function requireToken() {
  if (!AIRTABLE_TOKEN) throw new Error("AIRTABLE_TOKEN is required");
}

function clean(value) {
  return String(value ?? "").trim();
}

function num(value) {
  const parsed = Number.parseInt(clean(value), 10);
  return Number.isFinite(parsed) ? parsed : undefined;
}

function bool(value) {
  return value === true || value === "true" || value === "1" || value === 1;
}

function text(value, limit = 90000) {
  const valueText = clean(value);
  return valueText ? valueText.slice(0, limit) : undefined;
}

function isoFromDateText(value) {
  const parsed = new Date(clean(value));
  if (Number.isNaN(parsed.getTime())) return undefined;
  return `${parsed.getUTCFullYear()}-${String(parsed.getUTCMonth() + 1).padStart(2, "0")}-${String(parsed.getUTCDate()).padStart(2, "0")}`;
}

async function airtableFetch(url, options = {}) {
  requireToken();
  const response = await fetch(url, {
    ...options,
    headers: {
      Authorization: `Bearer ${AIRTABLE_TOKEN}`,
      "Content-Type": "application/json",
      ...(options.headers || {})
    }
  });
  const body = await response.text();
  if (!response.ok) throw new Error(`Airtable failed ${response.status}: ${body.slice(0, 1200)}`);
  return body ? JSON.parse(body) : {};
}

async function catalystGet(params) {
  const url = `${CATALYST_ENDPOINT}?${params.toString()}`;
  const response = await fetch(url);
  const body = await response.text();
  if (!response.ok) throw new Error(`Catalyst failed ${response.status}: ${body.slice(0, 1200)}`);
  return body ? JSON.parse(body) : {};
}

async function baseMeta() {
  return airtableFetch(`https://api.airtable.com/v0/meta/bases/${BASE_ID}/tables`);
}

async function createField(tableId, field) {
  return airtableFetch(`https://api.airtable.com/v0/meta/bases/${BASE_ID}/tables/${tableId}/fields`, {
    method: "POST",
    body: JSON.stringify(field)
  });
}

async function ensureClassTimesTable() {
  let meta = await baseMeta();
  let table = (meta.tables || []).find((item) => item.name === "class_times");
  if (!table) {
    await airtableFetch(`https://api.airtable.com/v0/meta/bases/${BASE_ID}/tables`, {
      method: "POST",
      body: JSON.stringify({
        name: "class_times",
        description: "Mirror of Catalyst hs_class_times. One class-time row per class scheduled from update_schedule.",
        fields: [{ name: "class_time_key", type: "singleLineText" }]
      })
    });
    meta = await baseMeta();
    table = (meta.tables || []).find((item) => item.name === "class_times");
  }
  const existing = new Set((table.fields || []).map((field) => field.name));
  for (const field of CLASS_TIMES_FIELDS) {
    if (!existing.has(field.name)) await createField(table.id, field);
  }
}

async function exportCatalystTable(tableKey) {
  const rows = [];
  for (let offset = 0; ; offset += 200) {
    const payload = await catalystGet(new URLSearchParams({
      action: "export-mirror-table",
      show_no: SHOW_NO,
      table: tableKey,
      limit: "200",
      offset: String(offset)
    }));
    rows.push(...(payload.data || []));
    if (!payload.has_more) break;
  }
  const unique = new Map();
  for (const row of rows) {
    const rowId = clean(row.ROWID);
    unique.set(rowId || JSON.stringify(row), row);
  }
  return [...unique.values()];
}

function updateScheduleMirrorRow(row) {
  const showNo = num(row.show_no);
  const days = num(row.ring_day_no);
  const classNo = num(row.class_no);
  return {
    mirror_update_schedule_key: [showNo, days, classNo].join("|"),
    show_no: showNo,
    focus_day: clean(row.focus_day).slice(0, 10) || undefined,
    days,
    ring_no: num(row.ring_no),
    ring_name: text(row.ring_name),
    date_text: text(row.date_text),
    class_no: classNo,
    event_id: num(row.event_id),
    class_payout: text(row.class_payout),
    event_name: text(row.event_name),
    class_name: text(row.class_name),
    time_text: text(row.time_text),
    time: text(row.class_start_time),
    entry_count: num(row.entry_count),
    event_type: num(row.event_type),
    oc_id: num(row.oc_id),
    live_flag: num(row.live_flag),
    source: "update_schedule"
  };
}

function classTimesMirrorRow(row) {
  const showNo = num(row.show_no);
  const ringDayNo = num(row.ring_day_no);
  const classNo = num(row.class_no);
  return {
    class_time_key: [showNo, ringDayNo, classNo].join("|"),
    show_no: showNo,
    ring_day_no: ringDayNo,
    class_no: classNo,
    class_label: text(row.class_label),
    class_time_text: text(row.class_time_text),
    class_order: num(row.class_order),
    entry_count: num(row.entry_count),
    re_type: text(row.re_type),
    oc_id: text(row.oc_id),
    live_flag: bool(row.live_flag),
    source_endpoint: text(row.source_endpoint),
    raw_json: text(row.raw_json),
    count_text: text(row.count_text),
    current_entry_text: text(row.current_entry_text),
    current_entry_no: num(row.current_entry_no),
    current_horse: text(row.current_horse),
    entries_gone: num(row.entries_gone),
    entries_to_go: num(row.entries_to_go),
    source_timestamp: num(row.source_timestamp),
    elapsed_seconds: num(row.elapsed_seconds),
    last_checked_at: text(row.last_checked_at),
    catalyst_row_id: text(row.ROWID)
  };
}

function resultClassMirrorRow(row) {
  return {
    result_class_key: text(row.result_class_key),
    show_no: num(row.show_no),
    focus_day: clean(row.focus_day).slice(0, 10) || undefined,
    class_no: num(row.class_no),
    sect_no: num(row.sect_no),
    class_number: num(row.class_number),
    class_name: text(row.class_name),
    result_entry_count: num(row.result_entry_count),
    has_score: bool(row.has_score),
    has_prize: bool(row.has_prize),
    completed_at: text(row.completed_at),
    source: text(row.source),
    raw_json: text(row.raw_json)
  };
}

function countsMirrorRow(row) {
  const showNo = num(row.show_no);
  const classNo = num(row.class_no);
  return {
    mirror_class_key: text(row.class_key) || [showNo, classNo].join("|"),
    show_no: showNo,
    class_no: classNo,
    class_number: num(row.class_number),
    class_name: text(row.class_name),
    entry_count: num(row.entry_count)
  };
}

function classOogMirrorRow(row, updateByClassNo = new Map()) {
  const key = text(row.class_oog_key);
  const keyParts = key.split("|");
  const showNo = num(row.show_no) || num(keyParts[0]);
  const classNo = num(row.class_no);
  const updateRow = updateByClassNo.get(String(classNo)) || {};
  return {
    mirror_class_oog_key: key,
    show_no: showNo,
    focus_day: clean(updateRow.focus_day || updateRow.iso_date).slice(0, 10) || undefined,
    ring: text(row.ring || updateRow.ring_name),
    ring_no: num(row.ring_no) || num(updateRow.ring_no),
    days: num(row.ring_day_no) || num(updateRow.ring_day_no),
    class_order: num(row.class_order),
    class_no: classNo,
    class_label: text(row.class_label),
    class_number: num(row.class_number),
    class_payout: text(row.class_payout),
    class_name: text(row.class_name),
    entry_order: num(row.entry_order),
    entry_no: num(row.entry_no),
    horse: text(row.horse),
    rider: text(row.rider),
    trainer: text(row.trainer),
    source: text(row.source_endpoint)
  };
}

function getOrdersMirrorRow(row) {
  const showNo = num(row.show_no);
  return {
    get_orders_key_mirror: text(row.get_orders_key),
    show_no: showNo,
    ring_no: num(row.ring_no),
    ring_day_no: num(row.ring_day_no),
    ring_name: text(row.ring_name),
    day_text: text(row.day_text),
    class_no: num(row.class_no),
    class_text: text(row.class_text),
    entry_text: text(row.entry_text),
    total: num(row.total),
    n_to_go: num(row.n_to_go),
    n_gone: num(row.n_gone),
    time_text: text(row.time_text),
    timestamp: text(row.timestamp_value),
    elapsed: text(row.elapsed),
    focus_day: isoFromDateText(row.day_text)
  };
}

function getRingsMirrorRow(row) {
  return {
    get_rings_key_mirror: text(row.get_rings_key),
    show_no: text(row.show_no),
    ring_no: num(row.ring_no),
    ring_day_no: num(row.ring_day_no),
    day_text: text(row.day_text),
    class_no: num(row.class_no),
    class_text: text(row.class_text),
    entry_text: text(row.entry_text),
    total: num(row.total),
    n_to_go: num(row.n_to_go),
    n_gone: num(row.n_gone),
    time_text: text(row.time_text),
    timestamp: text(row.timestamp_value),
    elapsed: text(row.elapsed),
    type: text(row.status_type),
    focus_day: isoFromDateText(row.day_text)
  };
}

async function upsert(tableName, mergeFields, rows) {
  const deduped = new Map();
  for (const row of rows) {
    const key = mergeFields.map((field) => clean(row[field])).join("|");
    if (!key.replace(/\|/g, "")) continue;
    const fields = {};
    for (const [field, value] of Object.entries(row)) {
      if (value !== undefined && value !== null && value !== "") fields[field] = value;
    }
    deduped.set(key, fields);
  }
  const cleanRows = [...deduped.values()];
  for (let index = 0; index < cleanRows.length; index += 10) {
    await airtableFetch(`https://api.airtable.com/v0/${BASE_ID}/${encodeURIComponent(tableName)}`, {
      method: "PATCH",
      body: JSON.stringify({
        performUpsert: { fieldsToMergeOn: mergeFields },
        records: cleanRows.slice(index, index + 10).map((fields) => ({ fields })),
        typecast: true
      })
    });
  }
  return cleanRows.length;
}

async function deleteStale(tableName, keyField, currentKeys, filterFormula) {
  const records = [];
  let offset = "";
  do {
    const url = new URL(`https://api.airtable.com/v0/${BASE_ID}/${encodeURIComponent(tableName)}`);
    url.searchParams.set("pageSize", "100");
    url.searchParams.set("filterByFormula", filterFormula);
    url.searchParams.append("fields[]", keyField);
    if (offset) url.searchParams.set("offset", offset);
    const payload = await airtableFetch(url);
    records.push(...(payload.records || []));
    offset = payload.offset || "";
  } while (offset);
  const stale = records
    .filter((record) => !currentKeys.has(clean(record.fields?.[keyField])))
    .map((record) => record.id);
  for (let index = 0; index < stale.length; index += 10) {
    const url = new URL(`https://api.airtable.com/v0/${BASE_ID}/${encodeURIComponent(tableName)}`);
    for (const id of stale.slice(index, index + 10)) url.searchParams.append("records[]", id);
    await airtableFetch(url, { method: "DELETE" });
  }
  return stale.length;
}

async function countAirtable(tableName, formula) {
  let count = 0;
  let offset = "";
  do {
    const url = new URL(`https://api.airtable.com/v0/${BASE_ID}/${encodeURIComponent(tableName)}`);
    url.searchParams.set("pageSize", "100");
    url.searchParams.set("filterByFormula", formula);
    if (offset) url.searchParams.set("offset", offset);
    const payload = await airtableFetch(url);
    count += (payload.records || []).length;
    offset = payload.offset || "";
  } while (offset);
  return count;
}

async function main() {
  await ensureClassTimesTable();
  const [updateRows, classTimeRows, resultRows, countsRows, classOogRows, getOrdersRows, getRingsRows] = await Promise.all([
    exportCatalystTable("update_schedule"),
    exportCatalystTable("class_times"),
    exportCatalystTable("result_classes"),
    exportCatalystTable("counts"),
    exportCatalystTable("class_oog"),
    exportCatalystTable("get_orders"),
    exportCatalystTable("get_rings")
  ]);
  const updateByClassNo = new Map(
    updateRows
      .filter((row) => clean(row.class_no))
      .map((row) => [clean(row.class_no), row])
  );
  const changed = {
    update_schedule: await upsert("update_schedule", ["mirror_update_schedule_key"], updateRows.map(updateScheduleMirrorRow)),
    class_times: await upsert("class_times", ["class_time_key"], classTimeRows.map(classTimesMirrorRow)),
    result_classes: await upsert("result_classes", ["result_class_key"], resultRows.map(resultClassMirrorRow)),
    counts: await upsert("counts", ["mirror_class_key"], countsRows.map(countsMirrorRow)),
    class_oog: await upsert("class_oog", ["mirror_class_oog_key"], classOogRows.map((row) => classOogMirrorRow(row, updateByClassNo))),
    get_orders: await upsert("get_orders", ["get_orders_key_mirror"], getOrdersRows.map(getOrdersMirrorRow)),
    get_rings: await upsert("get_rings", ["get_rings_key_mirror"], getRingsRows.map(getRingsMirrorRow))
  };
  const deleted_stale = {
    update_schedule: await deleteStale(
      "update_schedule",
      "mirror_update_schedule_key",
      new Set(updateRows.map(updateScheduleMirrorRow).map((row) => clean(row.mirror_update_schedule_key))),
      `{show_no}=${Number(SHOW_NO)}`
    ),
    class_times: await deleteStale(
      "class_times",
      "class_time_key",
      new Set(classTimeRows.map(classTimesMirrorRow).map((row) => clean(row.class_time_key))),
      `{show_no}=${Number(SHOW_NO)}`
    ),
    result_classes: await deleteStale(
      "result_classes",
      "result_class_key",
      new Set(resultRows.map(resultClassMirrorRow).map((row) => clean(row.result_class_key))),
      `{show_no}=${Number(SHOW_NO)}`
    ),
    counts: await deleteStale(
      "counts",
      "mirror_class_key",
      new Set(countsRows.map(countsMirrorRow).map((row) => clean(row.mirror_class_key))),
      `{show_no}=${Number(SHOW_NO)}`
    ),
    class_oog: await deleteStale(
      "class_oog",
      "mirror_class_oog_key",
      new Set(classOogRows.map((row) => classOogMirrorRow(row, updateByClassNo)).map((row) => clean(row.mirror_class_oog_key))),
      `{show_no}=${Number(SHOW_NO)}`
    ),
    get_orders: await deleteStale(
      "get_orders",
      "get_orders_key_mirror",
      new Set(getOrdersRows.map(getOrdersMirrorRow).map((row) => clean(row.get_orders_key_mirror))),
      `{show_no}=${Number(SHOW_NO)}`
    ),
    get_rings: await deleteStale(
      "get_rings",
      "get_rings_key_mirror",
      new Set(getRingsRows.map(getRingsMirrorRow).map((row) => clean(row.get_rings_key_mirror))),
      `{show_no}=${Number(SHOW_NO)}`
    )
  };
  const airtableCounts = {
    update_schedule: await countAirtable("update_schedule", `{show_no}=${Number(SHOW_NO)}`),
    class_times: await countAirtable("class_times", `{show_no}=${Number(SHOW_NO)}`),
    result_classes: await countAirtable("result_classes", `{show_no}=${Number(SHOW_NO)}`),
    counts: await countAirtable("counts", `{show_no}=${Number(SHOW_NO)}`),
    class_oog: await countAirtable("class_oog", `{show_no}=${Number(SHOW_NO)}`),
    get_orders: await countAirtable("get_orders", `{show_no}=${Number(SHOW_NO)}`),
    get_rings: await countAirtable("get_rings", `{show_no}=${Number(SHOW_NO)}`)
  };
  const catalystCounts = {
    update_schedule: updateRows.length,
    class_times: classTimeRows.length,
    result_classes: resultRows.length,
    counts: countsRows.length,
    class_oog: classOogRows.length,
    get_orders: getOrdersRows.length,
    get_rings: getRingsRows.length
  };
  const pass = Object.keys(catalystCounts).every((key) => catalystCounts[key] === airtableCounts[key]);
  console.log(JSON.stringify({ pass, show_no: SHOW_NO, catalystCounts, airtableCounts, changed, deleted_stale }, null, 2));
  if (!pass) process.exitCode = 1;
}

main().catch((error) => {
  console.error(error.stack || error.message || error);
  process.exit(1);
});
