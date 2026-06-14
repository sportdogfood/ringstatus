const BASE_ID = process.env.WEC_AIRTABLE_BASE_ID || "app6XS1RvsPNRT6os";
const AIRTABLE_TOKEN = process.env.AIRTABLE_TOKEN;

const TABLE_NAME = "mobile_meta";
const FOCUS_TABLE = "focus_show";

const FIELD_DEFS = [
  { name: "show_no", type: "number", options: { precision: 0 } },
  { name: "focus_day", type: "date", options: { dateFormat: { name: "iso" } } },
  { name: "active", type: "checkbox", options: { icon: "check", color: "greenBright" } },
  { name: "theme_mode", type: "singleLineText" },
  { name: "accent_hex", type: "singleLineText" },
  { name: "ring_header_bg", type: "singleLineText" },
  { name: "team_badge_label", type: "singleLineText" },
  { name: "show_diff_time", type: "checkbox", options: { icon: "check", color: "greenBright" } },
  { name: "show_diff_oog", type: "checkbox", options: { icon: "check", color: "greenBright" } },
  { name: "show_horse_edit", type: "checkbox", options: { icon: "check", color: "greenBright" } },
  { name: "hide_weekday_colors", type: "checkbox", options: { icon: "check", color: "greenBright" } },
  { name: "mobile_max_width", type: "number", options: { precision: 0 } },
  { name: "print_url", type: "singleLineText" },
  { name: "source", type: "singleLineText" },
  { name: "updated_at", type: "singleLineText" }
];

function requireToken() {
  if (!AIRTABLE_TOKEN) {
    throw new Error("AIRTABLE_TOKEN is required");
  }
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
  if (!response.ok) {
    throw new Error(`Airtable failed ${response.status}: ${text}`);
  }
  return text ? JSON.parse(text) : {};
}

async function getBaseMeta() {
  return airtableFetch(`https://api.airtable.com/v0/meta/bases/${BASE_ID}/tables`);
}

async function createTable() {
  return airtableFetch(`https://api.airtable.com/v0/meta/bases/${BASE_ID}/tables`, {
    method: "POST",
    body: JSON.stringify({
      name: TABLE_NAME,
      description: "WEC mobile and print display controls. Airtable is the control surface; runtime keeps code defaults as fallback.",
      fields: [
        {
          name: "mobile_meta_key",
          type: "singleLineText"
        }
      ]
    })
  });
}

async function createField(tableId, fieldDef) {
  try {
    return await airtableFetch(`https://api.airtable.com/v0/meta/bases/${BASE_ID}/tables/${tableId}/fields`, {
      method: "POST",
      body: JSON.stringify(fieldDef)
    });
  } catch (error) {
    if (fieldDef.type === "singleLineText") throw error;
    return airtableFetch(`https://api.airtable.com/v0/meta/bases/${BASE_ID}/tables/${tableId}/fields`, {
      method: "POST",
      body: JSON.stringify({ name: fieldDef.name, type: "singleLineText" })
    });
  }
}

async function airtableGetAll(tableName) {
  const records = [];
  let offset = "";
  do {
    const url = new URL(`https://api.airtable.com/v0/${BASE_ID}/${encodeURIComponent(tableName)}`);
    url.searchParams.set("pageSize", "100");
    if (offset) url.searchParams.set("offset", offset);
    const payload = await airtableFetch(url);
    records.push(...(payload.records || []));
    offset = payload.offset || "";
  } while (offset);
  return records;
}

async function airtableCreate(tableName, fields) {
  return airtableFetch(`https://api.airtable.com/v0/${BASE_ID}/${encodeURIComponent(tableName)}`, {
    method: "POST",
    body: JSON.stringify({ fields, typecast: true })
  });
}

function normalizeDate(value) {
  if (!value) return "";
  return String(value).slice(0, 10);
}

function castField(table, name, value) {
  const field = (table.fields || []).find((item) => item.name === name);
  if (!field) return value;
  if (field.type === "checkbox") return Boolean(value);
  if (field.type === "number") return value === "" ? undefined : Number(value);
  return value == null ? "" : String(value);
}

async function getActiveFocus() {
  const focusRecords = await airtableGetAll(FOCUS_TABLE);
  const active = focusRecords.find((record) => record.fields?.active === true) || focusRecords[0];
  if (!active) throw new Error("No focus_show records found");
  const showNo = active.fields.show_no == null ? "" : String(active.fields.show_no).replace(/\.0$/, "");
  const focusDay = normalizeDate(active.fields.focus_day);
  if (!showNo || !focusDay) throw new Error(`focus_show is missing show_no or focus_day: ${active.id}`);
  return { showNo, focusDay };
}

async function main() {
  let meta = await getBaseMeta();
  let table = (meta.tables || []).find((item) => item.name === TABLE_NAME);
  const created = [];
  const existing = [];

  if (!table) {
    table = await createTable();
    created.push(TABLE_NAME);
    meta = await getBaseMeta();
    table = (meta.tables || []).find((item) => item.name === TABLE_NAME);
  }

  if (!table) throw new Error(`Unable to resolve ${TABLE_NAME}`);

  const currentFieldNames = new Set((table.fields || []).map((field) => field.name));
  for (const fieldDef of FIELD_DEFS) {
    if (currentFieldNames.has(fieldDef.name)) {
      existing.push(fieldDef.name);
      continue;
    }
    await createField(table.id, fieldDef);
    created.push(fieldDef.name);
  }

  meta = await getBaseMeta();
  table = (meta.tables || []).find((item) => item.name === TABLE_NAME);

  const { showNo, focusDay } = await getActiveFocus();
  const mobileMetaKey = `${showNo}|${focusDay}`;
  const mobileMetaRecords = await airtableGetAll(TABLE_NAME);
  const alreadySeeded = mobileMetaRecords.some((record) => record.fields?.mobile_meta_key === mobileMetaKey);

  if (!alreadySeeded) {
    await airtableCreate(TABLE_NAME, {
      mobile_meta_key: mobileMetaKey,
      show_no: castField(table, "show_no", showNo),
      focus_day: castField(table, "focus_day", focusDay),
      active: castField(table, "active", true),
      theme_mode: castField(table, "theme_mode", "bw"),
      accent_hex: castField(table, "accent_hex", "#815374"),
      ring_header_bg: castField(table, "ring_header_bg", "#dcb6d1"),
      team_badge_label: castField(table, "team_badge_label", "CWF"),
      show_diff_time: castField(table, "show_diff_time", true),
      show_diff_oog: castField(table, "show_diff_oog", true),
      show_horse_edit: castField(table, "show_horse_edit", true),
      hide_weekday_colors: castField(table, "hide_weekday_colors", true),
      mobile_max_width: castField(table, "mobile_max_width", 1180),
      print_url: castField(table, "print_url", "https://ringstatus.com/wec-print"),
      source: castField(table, "source", "airtable.mobile_meta"),
      updated_at: castField(table, "updated_at", new Date().toISOString())
    });
  }

  console.log(JSON.stringify({
    ok: true,
    base_id: BASE_ID,
    table: TABLE_NAME,
    created,
    existing,
    seeded_key: mobileMetaKey,
    seed_created: !alreadySeeded
  }, null, 2));
}

main().catch((error) => {
  console.error(error.message);
  process.exit(1);
});
