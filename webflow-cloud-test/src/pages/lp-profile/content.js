export const config = {
  runtime: "edge"
};

import { env } from "cloudflare:workers";

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type,Authorization"
};

export const OPTIONS = async () => new Response(null, { status: 204, headers: corsHeaders });

export const GET = async () => {
  const airtable = airtableConfig();
  if (!airtable.ok) return json({ ok: false, error: airtable.error }, 500);

  try {
    const records = await listAirtableRecords(airtable);
    return json({
      ok: true,
      source: {
        table: airtable.table
      },
      count: records.length,
      records
    });
  } catch (error) {
    console.error("[lp-profile-content] load failed", error);
    return json({
      ok: false,
      error: "airtable_load_failed",
      detail: error instanceof Error ? error.message : String(error)
    }, 502);
  }
};

export const POST = async ({ request }) => {
  const airtable = airtableConfig();
  if (!airtable.ok) return json({ ok: false, error: airtable.error }, 500);

  const payload = normalizePayload(await readJson(request));
  const validation = validatePayload(payload);
  if (!validation.ok) return json({ ok: false, error: validation.error }, 400);

  try {
    const schema = await getBaseSchema(airtable);
    const saved = await upsertContentRecord(airtable, schema, payload);
    const log = await createChangeLogRecord(airtable, schema, payload, saved);
    return json({
      ok: true,
      recordKey: payload.record_key || payload.recordKey,
      recordType: payload.record_type || payload.recordType,
      record: saved,
      log
    });
  } catch (error) {
    console.error("[lp-profile-content] save failed", error);
    return json({
      ok: false,
      error: "airtable_save_failed",
      detail: error instanceof Error ? error.message : String(error)
    }, 502);
  }
};

function airtableConfig() {
  const token = env.AIRTABLE_TOKEN;
  const baseId = env.AIRTABLE_BASE_ID || env.AIRTABLE_BASE;
  const table = env.AIRTABLE_TABLE_PROFILE_CONTENT;
  const logTable = env.AIRTABLE_TABLE_PROFILE_CHANGE_LOG;

  if (!token) return { ok: false, error: "missing_airtable_token" };
  if (!baseId) return { ok: false, error: "missing_airtable_base_id" };
  if (!table) return { ok: false, error: "missing_profile_content_table" };
  if (!logTable) return { ok: false, error: "missing_profile_change_log_table" };

  return { ok: true, token, baseId, table, logTable };
}

async function listAirtableRecords(airtable) {
  const records = [];
  let offset = "";
  do {
    const url = airtableUrl(airtable.baseId, airtable.table);
    url.searchParams.set("pageSize", "100");
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

async function upsertContentRecord(airtable, schema, payload) {
  const recordKey = payload.recordKey;
  const recordType = payload.recordType;
  const fields = filterAirtableFields(schema, airtable.table, compactFields({
    ...objectValue(payload.data),
    record_key: recordKey,
    record_type: recordType,
    state: payload.recordState,
    record_state: payload.recordState,
    status: payload.status,
    tags: payload.tags,
    payload_json: JSON.stringify(payload.data || {}),
    raw_payload: JSON.stringify(payload.raw || {}),
    metadata_json: JSON.stringify(objectValue(payload.metadata || {})),
    updated_at: new Date().toISOString()
  }));

  const headers = {
    ...airtableHeaders(airtable.token),
    "Content-Type": "application/json"
  };
  const formula = `{record_key} = ${airtableFormulaString(recordKey)}`;
  const lookupUrl = airtableUrl(airtable.baseId, airtable.table);
  lookupUrl.searchParams.set("maxRecords", "1");
  lookupUrl.searchParams.set("filterByFormula", formula);
  const lookup = await fetch(lookupUrl, { headers });
  const lookupJson = await lookup.json().catch(() => ({}));
  if (!lookup.ok) {
    throw new Error(`lookup ${lookup.status}: ${JSON.stringify(lookupJson)}`);
  }

  const existingId = lookupJson.records?.[0]?.id;
  const saveUrl = existingId ? `${airtableUrl(airtable.baseId, airtable.table)}/${encodeURIComponent(existingId)}` : airtableUrl(airtable.baseId, airtable.table);
  const save = await fetch(saveUrl, {
    method: existingId ? "PATCH" : "POST",
    headers,
    body: JSON.stringify(existingId ? { fields, typecast: true } : { records: [{ fields }], typecast: true })
  });
  const saveJson = await save.json().catch(() => ({}));
  if (!save.ok) {
    throw new Error(`save ${save.status}: ${JSON.stringify(saveJson)}`);
  }

  return {
    id: existingId || saveJson.records?.[0]?.id || saveJson.id || "",
    action: existingId ? "updated" : "created",
    fieldCount: Object.keys(fields).length
  };
}

async function createChangeLogRecord(airtable, schema, payload, saved) {
  const changedAt = new Date().toISOString();
  const recordKey = payload.recordKey;
  const recordType = payload.recordType;
  const fields = filterAirtableFields(schema, airtable.logTable, compactFields({
    record_key: `log:${recordKey}:${Date.now()}`,
    source_record_key: recordKey,
    record_type: recordType,
    field_name: payload.field_name || payload.fieldName || "",
    old_value: stringifyValue(payload.old_value ?? payload.oldValue),
    new_value: stringifyValue(payload.new_value ?? payload.newValue),
    change_payload: JSON.stringify(payload.raw || payload),
    payload_json: JSON.stringify(payload.data || {}),
    raw_payload: JSON.stringify(payload.raw || {}),
    changed_at: changedAt,
    source: payload.source || "lp-profile-content",
    page_url: payload.page_url || payload.pageUrl || "",
    save_action: saved?.action || "",
    save_record_id: saved?.id || ""
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
    action: "logged",
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

function validatePayload(payload) {
  if (!payload || typeof payload !== "object") return { ok: false, error: "invalid_payload" };
  if (!payload.recordKey) return { ok: false, error: "missing_record_key" };
  if (!payload.recordType) return { ok: false, error: "missing_record_type" };
  return { ok: true };
}

function normalizePayload(payload) {
  const raw = payload && typeof payload === "object" ? payload : {};
  const data = objectValue(raw.data);
  const fields = objectValue(raw.fields);
  const mergedData = {
    ...fields,
    ...data
  };
  const recordType = String(raw.recordType || raw.record_type || data.record_type || fields.record_type || "").trim();
  const sourceId = String(data.source_id || fields.source_id || raw.source_id || "").trim();
  const recordKey = String(raw.recordKey || raw.record_key || data.record_key || fields.record_key || (recordType && sourceId ? `${recordType}:${sourceId}` : "")).trim();
  const recordState = String(raw.recordState || raw.record_state || data.record_state || data.state || fields.record_state || fields.state || "active").trim() || "active";
  const status = normalizeArray(raw.status || data.status || fields.status);
  const tags = normalizeArray(raw.tags || data.tags || fields.tags);
  return {
    recordKey,
    recordType,
    recordState,
    status,
    tags,
    data: {
      ...mergedData,
      record_key: recordKey,
      record_type: recordType,
      record_state: recordState,
      status,
      tags
    },
    metadata: objectValue(raw.metadata),
    field_name: raw.field_name || raw.fieldName || "",
    fieldName: raw.fieldName || raw.field_name || "",
    old_value: raw.old_value ?? raw.oldValue,
    oldValue: raw.oldValue ?? raw.old_value,
    new_value: raw.new_value ?? raw.newValue,
    newValue: raw.newValue ?? raw.new_value,
    source: raw.source || data.source || "lp-profile-content",
    page_url: raw.page_url || raw.pageUrl || data.page_url || "",
    pageUrl: raw.pageUrl || raw.page_url || data.page_url || "",
    raw
  };
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

function filterAirtableFields(schema, table, fields) {
  const allowed = schema?.tables?.[table];
  if (!allowed) return fields;
  return Object.fromEntries(Object.entries(fields).filter(([field]) => allowed.has(field)));
}

function airtableUrl(baseId, table) {
  return new URL(`https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(table)}`);
}

function airtableHeaders(token) {
  return { Authorization: `Bearer ${token}` };
}

function airtableFormulaString(value) {
  return `"${String(value).replace(/\\/g, "\\\\").replace(/"/g, '\\"')}"`;
}

function compactFields(fields) {
  return Object.fromEntries(Object.entries(fields).filter(([, value]) => (
    value !== undefined &&
    value !== null &&
    value !== "" &&
    (!Array.isArray(value) || value.length > 0)
  )));
}

function normalizeArray(value) {
  if (Array.isArray(value)) return value;
  if (!value) return [];
  return String(value).split(",").map((item) => item.trim()).filter(Boolean);
}

function numericOrOriginal(value) {
  const number = Number(value);
  return Number.isFinite(number) && String(value ?? "").trim() !== "" ? number : value;
}

function objectValue(value) {
  return value && typeof value === "object" && !Array.isArray(value) ? value : {};
}

function stringifyValue(value) {
  if (value === undefined || value === null) return "";
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
