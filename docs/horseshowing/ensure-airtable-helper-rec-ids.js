const BASE_ID = process.env.WEC_AIRTABLE_BASE_ID || "app6XS1RvsPNRT6os";
const AIRTABLE_TOKEN = process.env.AIRTABLE_TOKEN;

const TABLES = [
  "shows",
  "rings",
  "ring_days",
  "horses",
  "riders",
  "trainers",
  "entries",
  "classes"
];

async function airtableFetch(url, options = {}) {
  if (!AIRTABLE_TOKEN) throw new Error("AIRTABLE_TOKEN is required");
  const response = await fetch(url, {
    ...options,
    headers: {
      Authorization: `Bearer ${AIRTABLE_TOKEN}`,
      "Content-Type": "application/json",
      ...(options.headers || {})
    }
  });
  const text = await response.text();
  if (!response.ok) throw new Error(`Airtable ${response.status}: ${text}`);
  return text ? JSON.parse(text) : {};
}

async function main() {
  const meta = await airtableFetch(`https://api.airtable.com/v0/meta/bases/${BASE_ID}/tables`);
  const created = [];
  const existing = [];
  for (const tableName of TABLES) {
    const table = meta.tables.find((item) => item.name === tableName);
    if (!table) {
      existing.push({ table: tableName, status: "missing_table" });
      continue;
    }
    const hasRecId = table.fields.some((field) => field.name === "rec_id");
    if (hasRecId) {
      existing.push({ table: tableName, status: "exists" });
      continue;
    }
    await airtableFetch(`https://api.airtable.com/v0/meta/bases/${BASE_ID}/tables/${table.id}/fields`, {
      method: "POST",
      body: JSON.stringify({
        name: "rec_id",
        type: "formula",
        options: {
          formula: "RECORD_ID()"
        }
      })
    });
    created.push(tableName);
  }
  console.log(JSON.stringify({ ok: true, created, existing }, null, 2));
}

main().catch((error) => {
  console.error(error.stack || error.message);
  process.exit(1);
});
