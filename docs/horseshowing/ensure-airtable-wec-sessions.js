const BASE_ID = process.env.WEC_AIRTABLE_BASE_ID || "app6XS1RvsPNRT6os";
const AIRTABLE_TOKEN = process.env.AIRTABLE_TOKEN;
const TABLE_NAME = "wec_sessions";

const FIELD_DEFS = [
  { name: "device_id", type: "singleLineText" },
  { name: "show_no", type: "number", options: { precision: 0 } },
  { name: "focus_day", type: "date", options: { dateFormat: { name: "iso" } } },
  {
    name: "started_at",
    type: "dateTime",
    options: { dateFormat: { name: "iso" }, timeFormat: { name: "24hour" }, timeZone: "America/New_York" }
  },
  {
    name: "last_seen_at",
    type: "dateTime",
    options: { dateFormat: { name: "iso" }, timeFormat: { name: "24hour" }, timeZone: "America/New_York" }
  },
  {
    name: "status",
    type: "singleSelect",
    options: { choices: [{ name: "active" }, { name: "inactive" }] }
  },
  { name: "page", type: "singleLineText" },
  { name: "source", type: "singleLineText" }
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

async function createTable() {
  return airtableFetch(`https://api.airtable.com/v0/meta/bases/${BASE_ID}/tables`, {
    method: "POST",
    body: JSON.stringify({
      name: TABLE_NAME,
      description: "Explicit WEC mobile/print user sessions. Start session creates a record; heartbeat updates last_seen_at; stale heartbeat means inactive.",
      fields: [{ name: "session_id", type: "singleLineText" }]
    })
  });
}

async function createField(tableId, fieldDef) {
  return airtableFetch(`https://api.airtable.com/v0/meta/bases/${BASE_ID}/tables/${tableId}/fields`, {
    method: "POST",
    body: JSON.stringify(fieldDef)
  });
}

async function main() {
  let meta = await getMeta();
  let table = (meta.tables || []).find((item) => item.name === TABLE_NAME);
  const created = [];
  const existing = [];

  if (!table) {
    await createTable();
    created.push(TABLE_NAME);
    meta = await getMeta();
    table = (meta.tables || []).find((item) => item.name === TABLE_NAME);
  }

  if (!table) throw new Error(`Unable to resolve ${TABLE_NAME}`);

  const fieldNames = new Set((table.fields || []).map((field) => field.name));
  for (const fieldDef of FIELD_DEFS) {
    if (fieldNames.has(fieldDef.name)) {
      existing.push(fieldDef.name);
      continue;
    }
    await createField(table.id, fieldDef);
    created.push(fieldDef.name);
  }

  console.log(JSON.stringify({
    table: TABLE_NAME,
    table_id: table.id,
    created,
    existing
  }, null, 2));
}

main().catch((error) => {
  console.error(error.message);
  process.exit(1);
});
