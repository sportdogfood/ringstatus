const BASE_ID = process.env.WEC_AIRTABLE_BASE_ID || "app6XS1RvsPNRT6os";
const AIRTABLE_TOKEN = process.env.AIRTABLE_TOKEN;

const WAZE_USERS_FIELDS = [
  { name: "cookie_success", type: "checkbox", options: { icon: "check", color: "greenBright" } },
  { name: "cookie_date", type: "dateTime", options: { dateFormat: { name: "iso" }, timeFormat: { name: "24hour" }, timeZone: "America/New_York" } },
  { name: "cookie_expire", type: "dateTime", options: { dateFormat: { name: "iso" }, timeFormat: { name: "24hour" }, timeZone: "America/New_York" } },
  { name: "last_visit", type: "dateTime", options: { dateFormat: { name: "iso" }, timeFormat: { name: "24hour" }, timeZone: "America/New_York" } },
  { name: "geo_lat", type: "number", options: { precision: 7 } },
  { name: "geo_lng", type: "number", options: { precision: 7 } },
  { name: "geo_accuracy_m", type: "number", options: { precision: 1 } },
  { name: "geo_source", type: "singleLineText" },
  { name: "geo_allowed", type: "checkbox", options: { icon: "check", color: "greenBright" } },
  { name: "device_id", type: "singleLineText" },
  { name: "last_session_id", type: "singleLineText" },
  { name: "session_count", type: "number", options: { precision: 0 } },
  { name: "timezone", type: "singleLineText" },
  { name: "user_agent", type: "multilineText" },
  { name: "viewport", type: "singleLineText" },
  { name: "source", type: "singleLineText" },
  { name: "notes", type: "multilineText" }
];

const FOOTPRINT_TABLE = {
  name: "waze_session_footprints",
  description: "WEC Waze-style per-session footprint events for user visit, cookie, geo, ring, class, and entry interaction tracking.",
  primary: { name: "footprint_key", type: "singleLineText" },
  fields: [
    { name: "session_id", type: "singleLineText" },
    { name: "device_id", type: "singleLineText" },
    { name: "waze_name", type: "singleLineText" },
    { name: "event_type", type: "singleSelect", options: { choices: [
      { name: "session_start" },
      { name: "session_heartbeat" },
      { name: "cookie_check" },
      { name: "geo_check" },
      { name: "ring_open" },
      { name: "class_open" },
      { name: "entry_open" },
      { name: "ring_checkin" },
      { name: "comment_submit" },
      { name: "observation_submit" }
    ] } },
    { name: "event_at", type: "dateTime", options: { dateFormat: { name: "iso" }, timeFormat: { name: "24hour" }, timeZone: "America/New_York" } },
    { name: "show_no", type: "number", options: { precision: 0 } },
    { name: "focus_day", type: "date", options: { dateFormat: { name: "iso" } } },
    { name: "ring_no", type: "number", options: { precision: 0 } },
    { name: "class_no", type: "number", options: { precision: 0 } },
    { name: "entry_no", type: "number", options: { precision: 0 } },
    { name: "cookie_success", type: "checkbox", options: { icon: "check", color: "greenBright" } },
    { name: "cookie_date", type: "dateTime", options: { dateFormat: { name: "iso" }, timeFormat: { name: "24hour" }, timeZone: "America/New_York" } },
    { name: "cookie_expire", type: "dateTime", options: { dateFormat: { name: "iso" }, timeFormat: { name: "24hour" }, timeZone: "America/New_York" } },
    { name: "geo_lat", type: "number", options: { precision: 7 } },
    { name: "geo_lng", type: "number", options: { precision: 7 } },
    { name: "geo_accuracy_m", type: "number", options: { precision: 1 } },
    { name: "geo_source", type: "singleLineText" },
    { name: "geo_allowed", type: "checkbox", options: { icon: "check", color: "greenBright" } },
    { name: "page", type: "singleLineText" },
    { name: "path", type: "singleLineText" },
    { name: "timezone", type: "singleLineText" },
    { name: "user_agent", type: "multilineText" },
    { name: "viewport", type: "singleLineText" },
    { name: "source", type: "singleLineText" },
    { name: "notes", type: "multilineText" }
  ]
};

const LINK_TARGETS = [
  { field: "classes", table: "classes" },
  { field: "entries", table: "entries" },
  { field: "shows", table: "shows" },
  { field: "focus_show", table: "focus_show" },
  { field: "rings", table: "rings" },
  { field: "waze_users", table: "waze_users" },
  { field: "wec_sessions", table: "wec_sessions" }
];

function requireToken() {
  if (!AIRTABLE_TOKEN) throw new Error("AIRTABLE_TOKEN is required");
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
  const text = await response.text();
  if (!response.ok) throw new Error(`Airtable failed ${response.status}: ${text}`);
  return text ? JSON.parse(text) : {};
}

async function getMeta() {
  return airtableFetch(`https://api.airtable.com/v0/meta/bases/${BASE_ID}/tables`);
}

function tableByName(meta, name) {
  const table = (meta.tables || []).find((item) => item.name === name);
  if (!table) throw new Error(`Missing Airtable table: ${name}`);
  return table;
}

function hasField(table, fieldName) {
  return (table.fields || []).some((field) => field.name === fieldName);
}

async function createTable(config) {
  return airtableFetch(`https://api.airtable.com/v0/meta/bases/${BASE_ID}/tables`, {
    method: "POST",
    body: JSON.stringify({
      name: config.name,
      description: config.description,
      fields: [config.primary]
    })
  });
}

async function createField(tableId, fieldDef) {
  return airtableFetch(`https://api.airtable.com/v0/meta/bases/${BASE_ID}/tables/${tableId}/fields`, {
    method: "POST",
    body: JSON.stringify(fieldDef)
  });
}

async function listRecords(tableId) {
  const records = [];
  let offset = "";
  do {
    const url = new URL(`https://api.airtable.com/v0/${BASE_ID}/${tableId}`);
    url.searchParams.set("pageSize", "100");
    if (offset) url.searchParams.set("offset", offset);
    const data = await airtableFetch(url.toString());
    records.push(...(data.records || []));
    offset = data.offset || "";
  } while (offset);
  return records;
}

async function createRecord(tableId, fields) {
  return airtableFetch(`https://api.airtable.com/v0/${BASE_ID}/${tableId}`, {
    method: "POST",
    body: JSON.stringify({ fields })
  });
}

async function updateRecord(tableId, recordId, fields) {
  return airtableFetch(`https://api.airtable.com/v0/${BASE_ID}/${tableId}/${recordId}`, {
    method: "PATCH",
    body: JSON.stringify({ fields })
  });
}

async function ensureRecId(table) {
  if (hasField(table, "rec_id")) return "existing";
  await createField(table.id, { name: "rec_id", type: "formula", options: { formula: "RECORD_ID()" } });
  return "created";
}

async function ensureFields(table, fieldDefs) {
  const results = [];
  for (const fieldDef of fieldDefs) {
    if (hasField(table, fieldDef.name)) {
      results.push(`${fieldDef.name}:existing`);
      continue;
    }
    await createField(table.id, fieldDef);
    results.push(`${fieldDef.name}:created`);
  }
  return results;
}

async function ensureLinks(meta, table) {
  const results = [];
  for (const target of LINK_TARGETS) {
    if (table.name === target.table) continue;
    if (hasField(table, target.field)) {
      results.push(`${target.field}:existing`);
      continue;
    }
    const targetTable = tableByName(meta, target.table);
    await createField(table.id, {
      name: target.field,
      type: "multipleRecordLinks",
      options: { linkedTableId: targetTable.id }
    });
    results.push(`${target.field}:created`);
  }
  return results;
}

async function ensureFootprintTable(meta) {
  let table = (meta.tables || []).find((item) => item.name === FOOTPRINT_TABLE.name);
  const created = [];
  if (!table) {
    await createTable(FOOTPRINT_TABLE);
    created.push(FOOTPRINT_TABLE.name);
    meta = await getMeta();
    table = tableByName(meta, FOOTPRINT_TABLE.name);
  }
  const recId = await ensureRecId(table);
  meta = await getMeta();
  table = tableByName(meta, FOOTPRINT_TABLE.name);
  const fields = await ensureFields(table, FOOTPRINT_TABLE.fields);
  meta = await getMeta();
  table = tableByName(meta, FOOTPRINT_TABLE.name);
  const links = await ensureLinks(meta, table);
  return { created, recId, fields, links };
}

async function upsertTableIndex(meta) {
  const indexTable = tableByName(meta, "table_index");
  const records = await listRecords(indexTable.id);
  const existing = records.find((record) => record.fields.table_name === FOOTPRINT_TABLE.name);
  const table = tableByName(meta, FOOTPRINT_TABLE.name);
  const fields = {
    table_name: FOOTPRINT_TABLE.name,
    purpose: FOOTPRINT_TABLE.description,
    cat: "airtable",
    workflow_lane: "Outputs",
    airtable_mirror_table: FOOTPRINT_TABLE.name,
    airtable_mirror_table_id: table.id,
    airtable_table_id: table.id,
    airtable_columns: (table.fields || []).map((field) => field.name).join(", "),
    mirror_status: "airtable_only",
    schema_notes: "WEC Waze session footprint/event history table. waze_users stores latest state; this table stores historical events."
  };
  if (existing) {
    await updateRecord(indexTable.id, existing.id, fields);
    return "updated";
  }
  await createRecord(indexTable.id, fields);
  return "created";
}

async function main() {
  let meta = await getMeta();
  let users = tableByName(meta, "waze_users");
  const userRecId = await ensureRecId(users);
  meta = await getMeta();
  users = tableByName(meta, "waze_users");
  const userFields = await ensureFields(users, WAZE_USERS_FIELDS);
  meta = await getMeta();
  users = tableByName(meta, "waze_users");
  const userLinks = await ensureLinks(meta, users);

  meta = await getMeta();
  const footprint = await ensureFootprintTable(meta);
  meta = await getMeta();
  const tableIndex = await upsertTableIndex(meta);

  console.log(JSON.stringify({
    base: BASE_ID,
    waze_users: { table_id: users.id, rec_id: userRecId, fields: userFields, links: userLinks },
    waze_session_footprints: { table_id: tableByName(meta, FOOTPRINT_TABLE.name).id, ...footprint },
    tableIndex
  }, null, 2));
}

main().catch((error) => {
  console.error(error.message);
  process.exit(1);
});
