globalThis.process ??= {};
globalThis.process.env ??= {};
import { env } from "cloudflare:workers";
const config = {
  runtime: "edge"
};
const DEFAULT_TABLE = "ww_horses";
const DEFAULT_VIEW = "8778-tack-horses";
const DEFAULT_LOG_TABLE = "horses_change_log";
const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type,Authorization"
};
const OPTIONS = async () => new Response(null, { status: 204, headers: corsHeaders });
const GET = async () => {
  const airtable = getAirtableConfig();
  if (!airtable.ok) return json({ ok: false, error: airtable.error }, 500);
  try {
    const records = await listAirtableRecords(airtable, airtable.table, airtable.view);
    return json({
      ok: true,
      source: {
        table: airtable.table,
        view: airtable.view
      },
      count: records.length,
      records
    });
  } catch (error) {
    console.error("[8778-tack-horses] load failed", error);
    return json({
      ok: false,
      error: "airtable_load_failed",
      detail: error instanceof Error ? error.message : String(error)
    }, 502);
  }
};
const POST = async ({ request }) => {
  const airtable = getAirtableConfig();
  if (!airtable.ok) return json({ ok: false, error: airtable.error }, 500);
  const payload = await readJson(request);
  const validation = validateChange(payload);
  if (!validation.ok) return json({ ok: false, error: validation.error }, 400);
  try {
    const schema = await getBaseSchema(airtable);
    const updated = await updateHorseRecord(airtable, schema, payload);
    const logged = await createChangeLogRecord(airtable, schema, payload, updated);
    return json({
      ok: true,
      action: "updated_logged",
      updated,
      log: logged
    });
  } catch (error) {
    console.error("[8778-tack-horses] save failed", error);
    return json({
      ok: false,
      error: "airtable_save_failed",
      detail: error instanceof Error ? error.message : String(error)
    }, 502);
  }
};
function getAirtableConfig() {
  const token = env.AIRTABLE_TOKEN;
  const baseId = env.AIRTABLE_BASE_ID || env.AIRTABLE_BASE;
  const table = env.AIRTABLE_WW_HORSES_TABLE || env.AIRTABLE_HORSES_TABLE || DEFAULT_TABLE;
  const view = env.AIRTABLE_WW_HORSES_VIEW || env.AIRTABLE_HORSES_VIEW || DEFAULT_VIEW;
  const logTable = env.AIRTABLE_HORSES_CHANGE_LOG_TABLE || DEFAULT_LOG_TABLE;
  if (!token) return { ok: false, error: "missing_airtable_token" };
  if (!baseId) return { ok: false, error: "missing_airtable_base_id" };
  return { ok: true, token, baseId, table, view, logTable };
}
async function listAirtableRecords(airtable, table, view) {
  const records = [];
  let offset = "";
  do {
    const url = airtableUrl(airtable.baseId, table);
    url.searchParams.set("pageSize", "100");
    if (view) url.searchParams.set("view", view);
    if (offset) url.searchParams.set("offset", offset);
    const response = await fetch(url, { headers: airtableHeaders(airtable.token) });
    const result = await response.json().catch(() => ({}));
    if (!response.ok) {
      throw new Error(`list ${response.status}: ${JSON.stringify(result)}`);
    }
    records.push(...(result.records || []).map((record) => ({
      id: record.id,
      createdTime: record.createdTime,
      fields: record.fields || {}
    })));
    offset = result.offset || "";
  } while (offset);
  return records;
}
async function updateHorseRecord(airtable, schema, payload) {
  const fieldName = String(payload.fieldName || "").trim();
  if (schema?.tables?.[airtable.table] && !schema.tables[airtable.table].has(fieldName)) {
    throw new Error(`field_not_found_in_${airtable.table}: ${fieldName}`);
  }
  const value = airtableFieldValue(fieldName, payload.newValue);
  const response = await fetch(`${airtableUrl(airtable.baseId, airtable.table)}/${encodeURIComponent(payload.horseRecordId)}`, {
    method: "PATCH",
    headers: {
      ...airtableHeaders(airtable.token),
      "Content-Type": "application/json"
    },
    body: JSON.stringify({
      fields: { [fieldName]: value },
      typecast: true
    })
  });
  const result = await response.json().catch(() => ({}));
  if (!response.ok) {
    throw new Error(`update ${response.status}: ${JSON.stringify(result)}`);
  }
  return {
    id: result.id || payload.horseRecordId,
    fieldName,
    value: result.fields?.[fieldName] ?? value,
    action: "updated"
  };
}
async function createChangeLogRecord(airtable, schema, payload, updated) {
  const changedAt = (/* @__PURE__ */ new Date()).toISOString();
  const changeKey = `horse:${payload.horseRecordId || payload.horseKey}:${payload.fieldName}:${Date.now()}`;
  const fields = filterAirtableFields(schema, airtable.logTable, compactFields({
    horse: `${payload.horseName || payload.horseKey || payload.horseRecordId || "horse"} - ${payload.fieldName}`,
    change_key: changeKey,
    horse_record_id: payload.horseRecordId,
    horse_key: payload.horseKey,
    horse_name: payload.horseName,
    field_name: payload.fieldName,
    old_value: stringifyValue(payload.oldValue),
    new_value: stringifyValue(payload.newValue),
    changed_at: changedAt,
    source: payload.source || "8778-tack-horses",
    raw_payload: JSON.stringify({
      ...payload,
      update_action: updated?.action || "",
      update_record_id: updated?.id || ""
    })
  }));
  if (!Object.keys(fields).length) {
    throw new Error(`no_matching_log_fields_in_${airtable.logTable}`);
  }
  const response = await fetch(airtableUrl(airtable.baseId, airtable.logTable), {
    method: "POST",
    headers: {
      ...airtableHeaders(airtable.token),
      "Content-Type": "application/json"
    },
    body: JSON.stringify({ records: [{ fields }], typecast: true })
  });
  const result = await response.json().catch(() => ({}));
  if (!response.ok) {
    throw new Error(`log ${response.status}: ${JSON.stringify(result)}`);
  }
  return {
    id: result.records?.[0]?.id || "",
    changeKey,
    fieldCount: Object.keys(fields).length,
    changedAt
  };
}
async function getBaseSchema(airtable) {
  try {
    const response = await fetch(`https://api.airtable.com/v0/meta/bases/${encodeURIComponent(airtable.baseId)}/tables`, {
      headers: airtableHeaders(airtable.token)
    });
    const result = await response.json().catch(() => ({}));
    if (!response.ok) return null;
    const tables = {};
    for (const table of result.tables || []) {
      tables[table.name] = new Set((table.fields || []).map((field) => field.name));
      tables[table.id] = tables[table.name];
    }
    return { tables };
  } catch {
    return null;
  }
}
function filterAirtableFields(schema, table, fields) {
  const allowed = schema?.tables?.[table];
  if (!allowed) return fields;
  return Object.fromEntries(Object.entries(fields).filter(([field]) => allowed.has(field)));
}
function airtableFieldValue(fieldName, value) {
  if (fieldName === "disciplines" || fieldName === "horse_disciplines") {
    return String(value || "").split(",").map((item) => item.trim()).filter(Boolean);
  }
  if (fieldName === "horse_age" || fieldName === "age" || fieldName === "Age") {
    const number = Number(value);
    return Number.isFinite(number) && String(value).trim() !== "" ? number : value;
  }
  return value;
}
function validateChange(payload) {
  if (!payload || typeof payload !== "object") return { ok: false, error: "invalid_payload" };
  if (!payload.horseRecordId && !payload.horseKey) return { ok: false, error: "missing_horse_identifier" };
  if (!payload.fieldName) return { ok: false, error: "missing_field_name" };
  return { ok: true };
}
async function readJson(request) {
  const text = await request.text();
  if (!text) return {};
  try {
    return JSON.parse(text);
  } catch {
    return {};
  }
}
function airtableUrl(baseId, table) {
  return new URL(`https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(table)}`);
}
function airtableHeaders(token) {
  return { Authorization: `Bearer ${token}` };
}
function compactFields(fields) {
  return Object.fromEntries(Object.entries(fields).filter(([, value]) => value !== void 0 && value !== null && value !== ""));
}
function stringifyValue(value) {
  if (value === void 0 || value === null) return "";
  if (typeof value === "string") return value;
  return JSON.stringify(value);
}
function json(data, status = 200) {
  return new Response(JSON.stringify(data, null, 2) + "\n", {
    status,
    headers: {
      ...corsHeaders,
      "Content-Type": "application/json; charset=utf-8"
    }
  });
}
const _page = /* @__PURE__ */ Object.freeze(/* @__PURE__ */ Object.defineProperty({
  __proto__: null,
  GET,
  OPTIONS,
  POST,
  config
}, Symbol.toStringTag, { value: "Module" }));
const page = () => _page;
export {
  page
};
