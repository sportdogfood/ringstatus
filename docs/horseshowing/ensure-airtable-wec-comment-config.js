const BASE_ID = process.env.WEC_AIRTABLE_BASE_ID || "app6XS1RvsPNRT6os";
const AIRTABLE_TOKEN = process.env.AIRTABLE_TOKEN;

const TABLES = [
  {
    name: "wec_comment_presets",
    primary: { name: "preset_key", type: "singleLineText" },
    description: "Prebuilt comment options scoped to ring, class, or entry for the WEC comments UI.",
    fields: [
      {
        name: "scope",
        type: "singleSelect",
        options: { choices: [{ name: "ring" }, { name: "class" }, { name: "entry" }] }
      },
      { name: "label", type: "singleLineText" },
      { name: "comment_text", type: "multilineText" },
      { name: "show_no", type: "number", options: { precision: 0 } },
      { name: "focus_day", type: "date", options: { dateFormat: { name: "iso" } } },
      { name: "ring_no", type: "number", options: { precision: 0 } },
      { name: "class_no", type: "number", options: { precision: 0 } },
      { name: "entry_no", type: "number", options: { precision: 0 } },
      {
        name: "status",
        type: "singleSelect",
        options: { choices: [{ name: "active" }, { name: "inactive" }] }
      },
      { name: "sort_order", type: "number", options: { precision: 0 } },
      { name: "source", type: "singleLineText" },
      { name: "notes", type: "multilineText" }
    ]
  },
  {
    name: "wec_question_templates",
    primary: { name: "question_key", type: "singleLineText" },
    description: "Dynamic prompt templates scoped to ring, class, or entry for WEC observation questions.",
    fields: [
      {
        name: "scope",
        type: "singleSelect",
        options: { choices: [{ name: "ring" }, { name: "class" }, { name: "entry" }] }
      },
      { name: "prompt_label", type: "singleLineText" },
      { name: "prompt_text", type: "multilineText" },
      {
        name: "answer_type",
        type: "singleSelect",
        options: { choices: [{ name: "yes_no_unsure" }, { name: "text" }, { name: "choice" }] }
      },
      { name: "choices", type: "multilineText" },
      { name: "show_no", type: "number", options: { precision: 0 } },
      { name: "focus_day", type: "date", options: { dateFormat: { name: "iso" } } },
      { name: "ring_no", type: "number", options: { precision: 0 } },
      { name: "class_no", type: "number", options: { precision: 0 } },
      { name: "entry_no", type: "number", options: { precision: 0 } },
      {
        name: "status",
        type: "singleSelect",
        options: { choices: [{ name: "active" }, { name: "inactive" }] }
      },
      { name: "sort_order", type: "number", options: { precision: 0 } },
      { name: "trigger_context", type: "singleLineText" },
      { name: "source", type: "singleLineText" },
      { name: "notes", type: "multilineText" }
    ]
  }
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

async function ensureTable(config) {
  let meta = await getMeta();
  let table = (meta.tables || []).find((item) => item.name === config.name);
  const created = [];
  const existing = [];

  if (!table) {
    await createTable(config);
    created.push(config.name);
    meta = await getMeta();
    table = (meta.tables || []).find((item) => item.name === config.name);
  }

  if (!table) throw new Error(`Unable to resolve ${config.name}`);

  const fieldNames = new Set((table.fields || []).map((field) => field.name));
  for (const fieldDef of config.fields) {
    if (fieldNames.has(fieldDef.name)) {
      existing.push(fieldDef.name);
      continue;
    }
    await createField(table.id, fieldDef);
    created.push(fieldDef.name);
  }

  return { table: config.name, table_id: table.id, created, existing };
}

async function main() {
  const results = [];
  for (const config of TABLES) {
    results.push(await ensureTable(config));
  }
  console.log(JSON.stringify({ base: BASE_ID, results }, null, 2));
}

main().catch((error) => {
  console.error(error.message);
  process.exit(1);
});
