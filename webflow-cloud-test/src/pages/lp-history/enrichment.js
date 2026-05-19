export const config = {
  runtime: "edge"
};

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type,Authorization,X-Edit-Key"
};

const AIRTABLE_FIELDS = new Set([
  "record_key",
  "record_type",
  "payload_json",
  "horse",
  "barn_name",
  "show_name",
  "raw_payload",
  "status",
  "kind",
  "source",
  "competition_type",
  "video",
  "source_id",
  "record_state",
  "class_type",
  "class_sequence",
  "horse_type",
  "horse_disciplines",
  "horse_color",
  "class",
  "competition",
  "horse_gender",
  "horse_age",
  "image_url",
  "video_url",
  "embed_url",
  "thumbnail_url",
  "playlist",
  "group_tags",
  "tags",
  "notes",
  "updated_at"
]);

export const OPTIONS = async () => new Response(null, { status: 204, headers: corsHeaders });

export const GET = async ({ locals }) => {
  const env = getEnv(locals);
  return json({
    ok: true,
    service: "lp-history-enrichment",
    method: "POST",
    env: {
      hasAirtableToken: !!env.AIRTABLE_TOKEN,
      hasAirtableBaseId: !!(env.AIRTABLE_BASE_ID || env.AIRTABLE_BASE),
      table: env.AIRTABLE_TABLE_LP || env.AIRTABLE_TABLE || ""
    }
  });
};

export const POST = async ({ request, locals }) => {
  const payload = await readJson(request);
  const normalized = normalizePayload(payload);
  const validation = validatePayload(normalized);
  if (!validation.ok) return json({ ok: false, error: validation.error }, 400);

  const airtable = airtableConfig(getEnv(locals));
  if (!airtable.ok) return json({ ok: false, error: airtable.error }, 500);

  try {
    const saved = await upsertAirtableRecord(airtable, normalized);
    const log = await createAirtableLogRecord(airtable, normalized, saved);
    return json({
      ok: true,
      recordKey: normalized.recordKey,
      recordType: normalized.recordType,
      record: saved,
      log
    });
  } catch (error) {
    console.error("[lp-history-enrichment] save failed", error);
    return json({
      ok: false,
      error: "airtable_save_failed",
      detail: error instanceof Error ? error.message : String(error)
    }, 502);
  }
};

function getEnv(locals) {
  return locals?.runtime?.env || {};
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

function normalizePayload(payload) {
  const data = payload && typeof payload.data === "object" ? payload.data : {};
  const recordType = String(payload.recordType || data.record_type || "").trim();
  const sourceId = String(data.source_id || "").trim();
  const recordKey = String(payload.recordKey || (recordType && sourceId ? `${recordType}:${sourceId}` : "")).trim();
  return {
    recordKey,
    recordType,
    recordState: String(payload.recordState || data.record_state || "active").trim() || "active",
    status: Array.isArray(payload.status) ? payload.status : normalizeArray(data.status),
    data,
    raw: payload
  };
}

function validatePayload(payload) {
  if (!payload.recordKey) return { ok: false, error: "missing_record_key" };
  if (!["horse", "competition", "class", "video"].includes(payload.recordType)) {
    return { ok: false, error: "invalid_record_type" };
  }
  return { ok: true };
}

function airtableConfig(env) {
  const token = env.AIRTABLE_TOKEN;
  const baseId = env.AIRTABLE_BASE_ID || env.AIRTABLE_BASE;
  const table = env.AIRTABLE_TABLE_LP || env.AIRTABLE_TABLE;
  if (!token) return { ok: false, error: "missing_airtable_token" };
  if (!baseId) return { ok: false, error: "missing_airtable_base_id" };
  if (!table) return { ok: false, error: "missing_airtable_table" };
  return {
    ok: true,
    token,
    baseUrl: `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(table)}`
  };
}

async function upsertAirtableRecord(airtable, payload) {
  const fields = compactFields({
    ...filterFields(payload.data),
    record_key: payload.recordKey,
    record_type: payload.recordType,
    record_state: payload.recordState,
    status: payload.status,
    payload_json: JSON.stringify(payload.data || {}),
    raw_payload: JSON.stringify(payload.raw || {}),
    updated_at: new Date().toISOString()
  });

  const headers = {
    Authorization: `Bearer ${airtable.token}`,
    "Content-Type": "application/json"
  };
  const formula = `{record_key} = ${airtableFormulaString(payload.recordKey)}`;
  const lookupUrl = `${airtable.baseUrl}?maxRecords=1&filterByFormula=${encodeURIComponent(formula)}`;
  const lookup = await fetch(lookupUrl, { headers });
  const lookupJson = await lookup.json().catch(() => ({}));
  if (!lookup.ok) {
    throw new Error(`lookup ${lookup.status}: ${JSON.stringify(lookupJson)}`);
  }

  const existingId = lookupJson.records?.[0]?.id;
  const save = await fetch(existingId ? `${airtable.baseUrl}/${encodeURIComponent(existingId)}` : airtable.baseUrl, {
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
    action: existingId ? "updated" : "created"
  };
}

async function createAirtableLogRecord(airtable, payload, saved) {
  const loggedAt = new Date().toISOString();
  const fields = compactFields({
    ...filterFields(payload.data),
    record_key: `log:${payload.recordKey}:${Date.now()}`,
    record_type: payload.recordType,
    record_state: payload.recordState,
    status: payload.status,
    payload_json: JSON.stringify(payload.data || {}),
    raw_payload: JSON.stringify({
      event: "lp_history_enrichment_save",
      logged_at: loggedAt,
      source_record_key: payload.recordKey,
      source_record_type: payload.recordType,
      save_action: saved?.action || "",
      save_record_id: saved?.id || "",
      payload: payload.raw || {}
    }),
    updated_at: loggedAt
  });

  const response = await fetch(airtable.baseUrl, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${airtable.token}`,
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
    action: "logged"
  };
}

function filterFields(fields) {
  return Object.fromEntries(
    Object.entries(fields || {}).filter(([key]) => AIRTABLE_FIELDS.has(key))
  );
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
  return [String(value)];
}

function airtableFormulaString(value) {
  return `"${String(value).replace(/\\/g, "\\\\").replace(/"/g, '\\"')}"`;
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
