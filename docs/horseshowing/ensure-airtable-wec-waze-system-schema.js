const BASE_ID = process.env.WEC_AIRTABLE_BASE_ID || "app6XS1RvsPNRT6os";
const AIRTABLE_TOKEN = process.env.AIRTABLE_TOKEN;

const SYSTEM_TABLES = [
  {
    name: "wec_sessions",
    purpose: "WEC Waze-style browser sessions and user identity state.",
    lane: "Outputs"
  },
  {
    name: "wec_comments",
    purpose: "Master WEC comment audit table for ring, class, and entry comments.",
    lane: "Outputs"
  },
  {
    name: "wec_ring_checkins",
    purpose: "First-hand ring check-in records for WEC Waze-style session context.",
    lane: "Outputs"
  },
  {
    name: "wec_observations",
    purpose: "Structured WEC observation answers from dynamic ring, class, and entry prompts.",
    lane: "Outputs"
  },
  {
    name: "wec_ring_comments",
    purpose: "WEC comments scoped to a ring.",
    lane: "Outputs"
  },
  {
    name: "wec_class_comments",
    purpose: "WEC comments scoped to a class.",
    lane: "Outputs"
  },
  {
    name: "wec_entry_comments",
    purpose: "WEC comments scoped to an entry.",
    lane: "Outputs"
  },
  {
    name: "wec_comment_presets",
    purpose: "Operator-managed prebuilt comments scoped to ring, class, or entry.",
    lane: "Control"
  },
  {
    name: "wec_question_templates",
    purpose: "Operator-managed dynamic question templates scoped to ring, class, or entry.",
    lane: "Control"
  },
  {
    name: "waze_users",
    purpose: "WEC Waze-style user display names and session identity records.",
    lane: "Control"
  }
];

const LINK_TARGETS = [
  { field: "classes", table: "classes" },
  { field: "entries", table: "entries" },
  { field: "shows", table: "shows" },
  { field: "focus_show", table: "focus_show" },
  { field: "rings", table: "rings" },
  { field: "waze_users", table: "waze_users" }
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

function tableByName(meta, name) {
  const table = (meta.tables || []).find((item) => item.name === name);
  if (!table) throw new Error(`Missing Airtable table: ${name}`);
  return table;
}

function hasField(table, fieldName) {
  return (table.fields || []).some((field) => field.name === fieldName);
}

async function ensureRecId(table) {
  if (hasField(table, "rec_id")) return "existing";
  await createField(table.id, {
    name: "rec_id",
    type: "formula",
    options: { formula: "RECORD_ID()" }
  });
  return "created";
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

async function upsertTableIndex(meta) {
  const indexTable = tableByName(meta, "table_index");
  const records = await listRecords(indexTable.id);
  const byName = new Map(records.map((record) => [record.fields.table_name, record]));
  const created = [];
  const updated = [];

  for (const config of SYSTEM_TABLES) {
    const table = tableByName(meta, config.name);
    const fields = {
      table_name: config.name,
      purpose: config.purpose,
      cat: "airtable",
      workflow_lane: config.lane,
      airtable_mirror_table: config.name,
      airtable_mirror_table_id: table.id,
      airtable_table_id: table.id,
      airtable_columns: (table.fields || []).map((field) => field.name).join(", "),
      mirror_status: "airtable_only",
      schema_notes: "WEC comments/Waze system table. rec_id = RECORD_ID(). Links maintained to classes, entries, shows, focus_show, rings, and waze_users where applicable."
    };

    const existing = byName.get(config.name);
    if (existing) {
      await updateRecord(indexTable.id, existing.id, fields);
      updated.push(config.name);
    } else {
      await createRecord(indexTable.id, fields);
      created.push(config.name);
    }
  }

  return { created, updated };
}

async function main() {
  let meta = await getMeta();
  const schemaResults = [];

  for (const config of SYSTEM_TABLES) {
    let table = tableByName(meta, config.name);
    const recId = await ensureRecId(table);
    meta = await getMeta();
    table = tableByName(meta, config.name);
    const links = await ensureLinks(meta, table);
    schemaResults.push({ table: config.name, table_id: table.id, rec_id: recId, links });
    meta = await getMeta();
  }

  const tableIndex = await upsertTableIndex(meta);
  console.log(JSON.stringify({ base: BASE_ID, schemaResults, tableIndex }, null, 2));
}

main().catch((error) => {
  console.error(error.message);
  process.exit(1);
});
