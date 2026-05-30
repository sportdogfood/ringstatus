const {
  resolveShowHeartbeatAuditScope,
} = require("./lib/heartbeat_scope_audit");

const AIRTABLE_BASE_ID = process.env.AIRTABLE_BASE_ID;
const AIRTABLE_TOKEN = process.env.AIRTABLE_TOKEN;
const TABLE_SHOW = process.env.TABLE_SHOW_TARGET || process.env.TABLE_SHOW || "show";
const VIEW_SHOW_HEARTBEAT = process.env.VIEW_SHOW_HEARTBEAT || "heartbeat";

const FIELDS = [
  "show_id",
  "customer_id",
  "focus_day",
  "focus_day_test",
  "shifted_to_next_day",
  "shifted_to_next_day_test",
  "mode_control",
  "mode_control_test",
  "show_name",
  "start_date",
  "end_date",
  "heartbeat",
];

async function airtableList(tableName, params) {
  if (!AIRTABLE_BASE_ID) throw new Error("AIRTABLE_BASE_ID is required");
  if (!AIRTABLE_TOKEN) throw new Error("AIRTABLE_TOKEN is required");

  const records = [];
  let offset = null;
  do {
    const url = new URL(`https://api.airtable.com/v0/${AIRTABLE_BASE_ID}/${encodeURIComponent(tableName)}`);
    for (const [key, value] of Object.entries(params || {})) {
      if (Array.isArray(value)) {
        for (const item of value) url.searchParams.append(key, item);
      } else if (value !== undefined && value !== null) {
        url.searchParams.set(key, value);
      }
    }
    if (offset) url.searchParams.set("offset", offset);

    const response = await fetch(url, {
      method: "GET",
      headers: { Authorization: `Bearer ${AIRTABLE_TOKEN}` },
    });
    const body = await response.text();
    if (!response.ok) {
      throw new Error(`Airtable read failed (${response.status}): ${body.slice(0, 300)}`);
    }
    const json = JSON.parse(body);
    records.push(...(Array.isArray(json.records) ? json.records : []));
    offset = json.offset || null;
  } while (offset);
  return records;
}

async function main() {
  const rows = await airtableList(TABLE_SHOW, {
    view: VIEW_SHOW_HEARTBEAT,
    pageSize: "100",
    "fields[]": FIELDS,
  });

  const scopes = rows.map(resolveShowHeartbeatAuditScope);
  console.log(JSON.stringify({
    ok: true,
    dry_run: true,
    writes: 0,
    table: TABLE_SHOW,
    view: VIEW_SHOW_HEARTBEAT,
    rows: rows.length,
    scopes,
  }, null, 2));
}

if (require.main === module) {
  main().catch((error) => {
    console.error(JSON.stringify({
      ok: false,
      dry_run: true,
      writes: 0,
      error: String(error?.message || error),
    }, null, 2));
    process.exitCode = 1;
  });
}

module.exports = {
  airtableList,
  main,
};
