const BASE_ID = process.env.WEC_AIRTABLE_BASE_ID || "app6XS1RvsPNRT6os";
const AIRTABLE_TOKEN = process.env.AIRTABLE_TOKEN;
const CATALYST_ENDPOINT = process.env.HORSESHOWING_CATALYST_ENDPOINT ||
  "https://horseshowing-700800454.development.catalystserverless.com/server/horseshowing_sync/";

const SHOW_NO = process.env.WEC_SHOW_NO || process.argv[2] || "14906";
const TABLE = process.env.WEC_TABLE || process.argv[3] || "class_oog";

const TABLES = {
  update_schedule: {
    airtable: "update_schedule",
    key: "mirror_update_schedule_key",
    catalystKey: "update_schedule_key",
    fields: (row) => ({
      mirror_update_schedule_key: text(row.update_schedule_key),
      show_no: num(row.show_no),
      focus_day: dateText(row.focus_day || row.iso_date),
      days: num(row.ring_day_no),
      ring_no: num(row.ring_no),
      ring_name: text(row.ring_name),
      date_text: text(row.date_text),
      iso_date: dateText(row.iso_date || row.focus_day),
      class_no: num(row.class_no),
      event_id: num(row.event_id),
      event_name: text(row.event_name),
      class_name: text(row.class_name),
      time_text: text(row.time_text),
      time: text(row.class_start_time),
      entry_count: num(row.entry_count),
      event_type: num(row.event_type),
      oc_id: num(row.oc_id),
      live_flag: num(row.live_flag),
      source: "update_schedule"
    })
  },
  result_classes: {
    airtable: "result_classes",
    key: "result_class_key",
    catalystKey: "result_class_key",
    fields: (row) => ({
      result_class_key: text(row.result_class_key),
      show_no: num(row.show_no),
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
    })
  },
  counts: {
    airtable: "counts",
    key: "mirror_class_key",
    catalystKey: "class_key",
    fields: (row) => ({
      mirror_class_key: text(row.class_key) || [num(row.show_no), num(row.class_no)].join("|"),
      show_no: num(row.show_no),
      class_no: num(row.class_no),
      class_number: num(row.class_number),
      class_name: text(row.class_name),
      entry_count: num(row.entry_count)
    })
  },
  class_oog: {
    airtable: "class_oog",
    key: "mirror_class_oog_key",
    catalystKey: "class_oog_key",
    fields: (row) => {
      const key = text(row.class_oog_key);
      const parts = key.split("|");
      return {
        mirror_class_oog_key: key,
        show_no: num(row.show_no) || num(parts[0]),
        ring: text(row.ring),
        ring_no: num(row.ring_no),
        days: num(row.ring_day_no),
        class_order: num(row.class_order),
        class_no: num(row.class_no),
        class_label: text(row.class_label),
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
  },
  class_times: {
    airtable: "class_times",
    key: "class_time_key",
    catalystKey: "class_time_key",
    fields: (row) => ({
      class_time_key: classTimeKey(row),
      show_no: num(row.show_no),
      ring_day_no: num(row.ring_day_no),
      class_no: num(row.class_no),
      class_label: text(row.class_label),
      class_time_text: text(row.class_time_text),
      class_order: num(row.class_order),
      entry_count: num(row.entry_count),
      source_endpoint: text(row.source_endpoint),
      raw_json: text(row.raw_json),
      catalyst_row_id: text(row.ROWID)
    })
  }
};

function clean(value) {
  return String(value ?? "").trim();
}

function text(value, limit = 90000) {
  const valueText = clean(value);
  return valueText ? valueText.slice(0, limit) : undefined;
}

function num(value) {
  const parsed = Number.parseInt(clean(value), 10);
  return Number.isFinite(parsed) ? parsed : undefined;
}

function bool(value) {
  return value === true || value === "true" || value === "1" || value === 1;
}

function dateText(value) {
  const valueText = clean(value).slice(0, 10);
  return valueText || undefined;
}

function eventIdFromRaw(row) {
  const raw = clean(row.raw_json);
  if (!raw) return "";
  try {
    const parsed = JSON.parse(raw);
    return clean(parsed.event_id || parsed.id);
  } catch {
    return "";
  }
}

function classTimeKey(row) {
  return text(row.class_time_key) ||
    [
      clean(row.show_no),
      clean(row.ring_day_no),
      clean(row.class_no),
      eventIdFromRaw(row) || clean(row.ROWID)
    ].join("|");
}

function compact(fields) {
  const out = {};
  for (const [key, value] of Object.entries(fields)) {
    if (value !== undefined && value !== null && value !== "") out[key] = value;
  }
  return out;
}

async function fetchJson(url, options = {}) {
  const response = await fetch(url, options);
  const body = await response.text();
  if (!response.ok) throw new Error(`${response.status}: ${body.slice(0, 1200)}`);
  return body ? JSON.parse(body) : {};
}

async function catalystRows(table) {
  const rows = [];
  for (let offset = 0; ; offset += 200) {
    const url = new URL(CATALYST_ENDPOINT);
    url.searchParams.set("action", "export-mirror-table");
    url.searchParams.set("show_no", SHOW_NO);
    url.searchParams.set("table", table);
    url.searchParams.set("limit", "200");
    url.searchParams.set("offset", String(offset));
    const payload = await fetchJson(url);
    const page = Array.isArray(payload.data) ? payload.data : [];
    rows.push(...page);
    if (!payload.has_more || !page.length) break;
  }
  return rows;
}

async function airtableRecords(tableName, keyField) {
  if (!AIRTABLE_TOKEN) throw new Error("AIRTABLE_TOKEN is required");
  const records = [];
  let offset = "";
  do {
    const url = new URL(`https://api.airtable.com/v0/${BASE_ID}/${encodeURIComponent(tableName)}`);
    url.searchParams.set("pageSize", "100");
    url.searchParams.set("filterByFormula", `{show_no}=${Number(SHOW_NO)}`);
    url.searchParams.append("fields[]", keyField);
    if (offset) url.searchParams.set("offset", offset);
    const payload = await fetchJson(url, {
      headers: { Authorization: `Bearer ${AIRTABLE_TOKEN}` }
    });
    records.push(...(payload.records || []));
    offset = payload.offset || "";
  } while (offset);
  return records;
}

async function upsertRows(tableName, keyField, rows) {
  let changed = 0;
  for (let index = 0; index < rows.length; index += 10) {
    const batch = rows.slice(index, index + 10);
    if (!batch.length) continue;
    await fetchJson(`https://api.airtable.com/v0/${BASE_ID}/${encodeURIComponent(tableName)}`, {
      method: "PATCH",
      headers: {
        Authorization: `Bearer ${AIRTABLE_TOKEN}`,
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        performUpsert: { fieldsToMergeOn: [keyField] },
        records: batch.map((fields) => ({ fields })),
        typecast: true
      })
    });
    changed += batch.length;
  }
  return changed;
}

async function deleteRows(tableName, ids) {
  let deleted = 0;
  for (let index = 0; index < ids.length; index += 10) {
    const batch = ids.slice(index, index + 10);
    if (!batch.length) continue;
    const url = new URL(`https://api.airtable.com/v0/${BASE_ID}/${encodeURIComponent(tableName)}`);
    for (const id of batch) url.searchParams.append("records[]", id);
    await fetchJson(url, {
      method: "DELETE",
      headers: { Authorization: `Bearer ${AIRTABLE_TOKEN}` }
    });
    deleted += batch.length;
  }
  return deleted;
}

async function reconcileOnce() {
  const config = TABLES[TABLE];
  if (!config) throw new Error(`Unsupported table: ${TABLE}`);
  const catalyst = await catalystRows(TABLE);
  const sourceRows = catalyst
    .map((row) => compact(config.fields(row)))
    .filter((row) => clean(row[config.key]));
  const sourceKeys = new Set(sourceRows.map((row) => clean(row[config.key])));
  const airtable = await airtableRecords(config.airtable, config.key);
  const airtableKeys = new Set(airtable.map((record) => clean(record.fields?.[config.key])).filter(Boolean));
  const missingRows = sourceRows.filter((row) => !airtableKeys.has(clean(row[config.key])));
  const staleIds = airtable
    .filter((record) => !sourceKeys.has(clean(record.fields?.[config.key])))
    .map((record) => record.id);
  const upserted = await upsertRows(config.airtable, config.key, missingRows);
  const deleted = await deleteRows(config.airtable, staleIds);
  const after = await airtableRecords(config.airtable, config.key);
  const afterKeys = new Set(after.map((record) => clean(record.fields?.[config.key])).filter(Boolean));
  const missingAfter = [...sourceKeys].filter((key) => !afterKeys.has(key)).length;
  const staleAfter = [...afterKeys].filter((key) => !sourceKeys.has(key)).length;
  return {
    table: TABLE,
    show_no: SHOW_NO,
    catalyst_rows: catalyst.length,
    source_keys: sourceKeys.size,
    airtable_before: airtable.length,
    upserted,
    deleted,
    airtable_after: after.length,
    missing_after: missingAfter,
    stale_after: staleAfter,
    pass: catalyst.length === after.length && sourceKeys.size === afterKeys.size && missingAfter === 0 && staleAfter === 0
  };
}

async function main() {
  const first = await reconcileOnce();
  const second = await reconcileOnce();
  console.log(JSON.stringify({
    ok: first.pass && second.pass && second.deleted === 0,
    first,
    second,
    idempotent: second.deleted === 0 && second.missing_after === 0 && second.stale_after === 0
  }, null, 2));
  if (!first.pass || !second.pass || second.deleted !== 0) process.exit(1);
}

main().catch((error) => {
  console.error(error.stack || error.message);
  process.exit(1);
});
