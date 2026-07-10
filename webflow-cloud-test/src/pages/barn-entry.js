import { env } from "cloudflare:workers";
import html from "../assets/barn-entry/source.html?raw";

const DEFAULT_BASE_ID = "app6XS1RvsPNRT6os";

export const GET = async () => new Response(html, {
  headers: {
    "content-type": "text/html; charset=utf-8",
    "cache-control": "no-store"
  }
});

export const POST = async ({ request }) => {
  try {
    return await submit(request);
  } catch (error) {
    return json({
      ok: false,
      error: "barn_entry_submit_failed",
      detail: error instanceof Error ? error.message : String(error)
    }, 500);
  }
};

async function submit(request) {
  const airtable = airtableConfig(env);
  if (!airtable.ok) return json({ ok: false, error: airtable.error }, 500);

  let payload;
  try {
    payload = await request.json();
  } catch {
    return json({ ok: false, error: "invalid_json" }, 400);
  }

  const rows = Array.isArray(payload?.rows) ? payload.rows : [];
  if (!rows.length) return json({ ok: false, error: "no_rows" }, 400);

  try {
    const schema = await getBaseSchema(airtable);
    const created = [];
    for (const row of rows) {
      const fields = filterAirtableFields(schema, airtable.table, barnEntryFields(payload, row));
      if (!Object.keys(fields).length) return json({ ok: false, error: "no_matching_airtable_fields" }, 500);
      const response = await fetch(airtableUrl(airtable.baseId, airtable.table), {
        method: "POST",
        headers: airtableHeaders(airtable.token),
        body: JSON.stringify({ fields, typecast: true })
      });
      const data = await response.json().catch(() => ({}));
      if (!response.ok) {
        return json({
          ok: false,
          error: "airtable_create_failed",
          status: response.status,
          detail: data
        }, 500);
      }
      created.push(data.id || data.records?.[0]?.id || "");
    }
    return json({ ok: true, row_count: rows.length, created });
  } catch (error) {
    return json({ ok: false, error: error.message || String(error) }, 500);
  }
}

function airtableConfig(env) {
  const token = env.AIRTABLE_TOKEN;
  const baseId = env.AIRTABLE_WEC_BASE_ID || env.WEC_AIRTABLE_BASE_ID || env.AIRTABLE_WEC_SCHEDULES_BASE_ID || env.AIRTABLE_BASE_ID || env.AIRTABLE_BASE || DEFAULT_BASE_ID;
  const table = env.AIRTABLE_BARN_ENTRY_REVIEW_TABLE || env.AIRTABLE_BARN_ENTRY_TABLE || "barn_entry_review";
  if (!token) return { ok: false, error: "missing_airtable_token" };
  if (!baseId) return { ok: false, error: "missing_airtable_base_id" };
  if (!table) return { ok: false, error: "missing_airtable_table" };
  return { ok: true, token, baseId, table };
}

function barnEntryFields(payload, row) {
  return compactFields({
    source: payload.source || "barn_entry_ag_review",
    submitted_at: payload.submitted_at || new Date().toISOString(),
    review_key: row.review_key,
    status: row.status || "pending",
    show_no: numberOrNull(row.show_no),
    focus_day: row.focus_day,
    focus_day_key: row.focus_day_key,
    ring_day_no: numberOrNull(row.ring_day_no),
    ring_no: numberOrNull(row.ring_no),
    ring_name_normalized: row.ring_name_normalized,
    class_number: numberOrNull(row.class_number),
    class_no: numberOrNull(row.class_no),
    class_name: row.class_name,
    display_time: row.display_time,
    entry_no: numberOrNull(row.entry_no),
    barn_name: row.barn_name,
    horse_name: row.horse_name,
    horse_key: row.horse_key,
    matched_source_row: row.matched_source_row,
    payload_json: JSON.stringify(row)
  });
}

function numberOrNull(value) {
  if (value === null || value === undefined || value === "") return null;
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

function compactFields(fields) {
  return Object.fromEntries(Object.entries(fields).filter(([, value]) => value !== undefined && value !== null && value !== ""));
}

async function getBaseSchema(airtable) {
  const response = await fetch(`https://api.airtable.com/v0/meta/bases/${encodeURIComponent(airtable.baseId)}/tables`, {
    headers: { Authorization: `Bearer ${airtable.token}` }
  });
  if (!response.ok) throw new Error(`airtable_schema_failed:${response.status}`);
  return response.json();
}

function filterAirtableFields(schema, tableNameOrId, fields) {
  const table = (schema.tables || []).find((item) => item.id === tableNameOrId || item.name === tableNameOrId);
  if (!table) return fields;
  const allowed = new Set((table.fields || []).map((field) => field.name));
  return Object.fromEntries(Object.entries(fields).filter(([key]) => allowed.has(key)));
}

function airtableUrl(baseId, table) {
  return `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(table)}`;
}

function airtableHeaders(token) {
  return {
    Authorization: `Bearer ${token}`,
    "content-type": "application/json"
  };
}

function json(payload, status = 200) {
  return new Response(JSON.stringify(payload), {
    status,
    headers: {
      "content-type": "application/json; charset=utf-8",
      "cache-control": "no-store"
    }
  });
}
