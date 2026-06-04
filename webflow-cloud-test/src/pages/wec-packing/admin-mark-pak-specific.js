import { env } from "cloudflare:workers";

export const prerender = false;

const KIT_TABLE_NAMES = new Set([
  "pak_kits",
  "pak_kit_items",
  "horse_packing_kits",
  "horse_kit_changes"
]);

export async function GET() {
  const runtime = { ...(globalThis.process?.env || {}), ...(import.meta.env || {}), ...(env || {}) };
  const token = runtime.AIRTABLE_TOKEN;
  const baseId = runtime.AIRTABLE_BASE_ID || runtime.AIRTABLE_BASE;
  if (!token || !baseId) return json({ ok: false, error: "missing_airtable_runtime" }, 500);

  const headers = {
    Authorization: `Bearer ${token}`,
    "Content-Type": "application/json"
  };

  const schema = await airtableJson(`https://api.airtable.com/v0/meta/bases/${encodeURIComponent(baseId)}/tables`, { headers });
  const tableIndex = schema.tables?.find((table) => table.name === "table_index");
  if (!tableIndex?.id) return json({ ok: false, error: "missing_table_index" }, 404);
  const markerField = tableIndex.fields?.find((field) => field.name === "this_is_pak_specific");
  if (!markerField) return json({ ok: false, error: "missing_this_is_pak_specific" }, 404);

  const records = await listRecords(baseId, tableIndex.id, headers, "pak");
  const targets = records.filter((record) => KIT_TABLE_NAMES.has(String(record.fields?.table_name || "").trim()));
  const updates = targets.map((record) => ({
    id: record.id,
    fields: {
      this_is_pak_specific: "horse_specific"
    }
  }));

  const patched = [];
  for (let index = 0; index < updates.length; index += 10) {
    const batch = updates.slice(index, index + 10);
    if (!batch.length) continue;
    const result = await airtableJson(
      `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(tableIndex.id)}`,
      {
        method: "PATCH",
        headers,
        body: JSON.stringify({ records: batch })
      }
    );
    patched.push(...(result.records || []));
  }

  return json({
    ok: true,
    table: "table_index",
    view: "pak",
    value: "horse_specific",
    patched: patched.map((record) => ({
      id: record.id,
      table_name: record.fields?.table_name,
      this_is_pak_specific: record.fields?.this_is_pak_specific
    }))
  });
}

async function listRecords(baseId, tableId, headers, view) {
  const records = [];
  let offset = "";
  do {
    const url = new URL(`https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(tableId)}`);
    url.searchParams.set("pageSize", "100");
    url.searchParams.set("view", view);
    if (offset) url.searchParams.set("offset", offset);
    const page = await airtableJson(url.toString(), { headers });
    records.push(...(page.records || []));
    offset = page.offset || "";
  } while (offset);
  return records;
}

async function airtableJson(url, options) {
  const response = await fetch(url, options);
  const result = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(`${response.status}: ${JSON.stringify(result)}`);
  return result;
}

function json(data, status = 200) {
  return new Response(JSON.stringify(data, null, 2) + "\n", {
    status,
    headers: {
      "Access-Control-Allow-Origin": "*",
      "Cache-Control": "no-store",
      "Content-Type": "application/json; charset=utf-8"
    }
  });
}
