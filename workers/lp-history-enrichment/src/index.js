const JSON_HEADERS = {
  "Content-Type": "application/json; charset=utf-8",
};

const REQUIRED_ENV = [
  ["LP_HISTORY_EDIT_KEY"],
  ["AIRTABLE_API_KEY", "AIRTABLE_TOKEN"],
  ["AIRTABLE_BASE_ID"],
  ["AIRTABLE_TABLE_NAME", "AIRTABLE_TABLE"],
];

const ALLOWED_RECORD_TYPES = new Set(["horse", "competition", "class", "video"]);
const ALLOWED_RECORD_STATES = new Set(["active", "inactive"]);
const ALLOWED_STATUS = new Set(["overview", "favorite", "ignore"]);
const AIRTABLE_FIELDS = [
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
  "updated_at",
];

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    if (request.method === "OPTIONS") {
      return new Response(null, {
        status: 204,
        headers: corsHeaders(request, env),
      });
    }

    if (request.method === "GET" && url.pathname === "/health") {
      return jsonResponse(
        {
          ok: true,
          service: "ringstatus-lp-history-enrichment",
          configured: getMissingEnv(env).length === 0,
          missingEnv: getMissingEnv(env),
        },
        200,
        request,
        env,
      );
    }

    if (request.method === "POST" && url.pathname === "/lp-history/enrichment") {
      return handleEnrichmentWrite(request, env);
    }

    return jsonResponse({ ok: false, error: "not_found" }, 404, request, env);
  },
};

async function handleEnrichmentWrite(request, env) {
  const missingEnv = getMissingEnv(env);
  if (missingEnv.length > 0) {
    return jsonResponse({ ok: false, error: "worker_not_configured", missingEnv }, 500, request, env);
  }

  const providedKey = request.headers.get("X-Edit-Key") || request.headers.get("X-LP-Edit-Key");
  if (!providedKey || !constantTimeEqual(providedKey, env.LP_HISTORY_EDIT_KEY)) {
    return jsonResponse({ ok: false, error: "unauthorized" }, 401, request, env);
  }

  let body;
  try {
    body = await request.json();
  } catch {
    return jsonResponse({ ok: false, error: "invalid_json" }, 400, request, env);
  }

  const normalized = normalizePayload(body);
  if (!normalized.ok) {
    return jsonResponse({ ok: false, error: "invalid_payload", details: normalized.errors }, 400, request, env);
  }

  try {
    const result = await upsertAirtableRecord(normalized.value, env);
    return jsonResponse({ ok: true, record: result }, 200, request, env);
  } catch (error) {
    return jsonResponse(
      { ok: false, error: "airtable_write_failed", message: String(error?.message || error) },
      502,
      request,
      env,
    );
  }
}

function normalizePayload(body) {
  const errors = [];
  const recordType = normalizeString(body.recordType || body.type);
  const recordKey = normalizeKey(body.recordKey || body.record_key || body.key || body.id);
  const recordState = normalizeString(body.recordState || body.record_state || body.state || "active");
  const rawStatus = Array.isArray(body.status) ? body.status : body.status ? [body.status] : [];
  const status = Array.from(new Set(rawStatus.map(normalizeString).filter(Boolean)));
  const data = isPlainObject(body.data) ? body.data : {};

  if (!ALLOWED_RECORD_TYPES.has(recordType)) {
    errors.push("recordType must be horse, competition, class, or video");
  }

  if (!recordKey) {
    errors.push("recordKey is required");
  }

  if (!ALLOWED_RECORD_STATES.has(recordState)) {
    errors.push("recordState must be active or inactive");
  }

  const invalidStatus = status.filter((item) => !ALLOWED_STATUS.has(item));
  if (invalidStatus.length > 0) {
    errors.push(`status contains unsupported value(s): ${invalidStatus.join(", ")}`);
  }

  if (errors.length > 0) {
    return { ok: false, errors };
  }

  const now = new Date().toISOString();
  const passthrough = pickAirtableFields({
    ...data,
    ...body,
  });

  return {
    ok: true,
    value: {
      ...passthrough,
      record_key: recordKey,
      record_type: recordType,
      record_state: recordState,
      status,
      payload_json: JSON.stringify(data),
      raw_payload: JSON.stringify(body),
      updated_at: now,
      source: passthrough.source || "lp-history-edit",
    },
  };
}

function pickAirtableFields(source) {
  const fields = {};

  for (const field of AIRTABLE_FIELDS) {
    if (source[field] !== undefined && source[field] !== null && source[field] !== "") {
      fields[field] = normalizeFieldValue(source[field]);
    }
  }

  return fields;
}

function normalizeFieldValue(value) {
  if (Array.isArray(value)) {
    return value.map((item) => String(item).trim()).filter(Boolean);
  }

  if (isPlainObject(value)) {
    return JSON.stringify(value);
  }

  return value;
}

async function upsertAirtableRecord(fields, env) {
  const existing = await findAirtableRecord(fields.record_key, env);
  const url = existing
    ? airtableUrl(env, `/${encodeURIComponent(existing.id)}`)
    : airtableUrl(env);
  const method = existing ? "PATCH" : "POST";
  const payload = existing ? { fields } : { records: [{ fields }] };

  const response = await fetch(url, {
    method,
    headers: airtableHeaders(env),
    body: JSON.stringify(payload),
  });

  const json = await response.json().catch(() => ({}));
  if (!response.ok) {
    return Promise.reject(new Error(`Airtable ${method} failed: ${response.status} ${JSON.stringify(json)}`));
  }

  if (existing) {
    return { id: json.id, action: "updated" };
  }

  return { id: json.records?.[0]?.id || null, action: "created" };
}

async function findAirtableRecord(recordKey, env) {
  const formula = `{record_key} = ${airtableFormulaString(recordKey)}`;
  const url = `${airtableUrl(env)}?maxRecords=1&filterByFormula=${encodeURIComponent(formula)}`;
  const response = await fetch(url, {
    method: "GET",
    headers: airtableHeaders(env),
  });

  const json = await response.json().catch(() => ({}));
  if (!response.ok) {
    throw new Error(`Airtable lookup failed: ${response.status} ${JSON.stringify(json)}`);
  }

  return json.records?.[0] || null;
}

function airtableUrl(env, suffix = "") {
  return `https://api.airtable.com/v0/${encodeURIComponent(env.AIRTABLE_BASE_ID)}/${encodeURIComponent(getEnvValue(env, "AIRTABLE_TABLE_NAME", "AIRTABLE_TABLE"))}${suffix}`;
}

function airtableHeaders(env) {
  return {
    Authorization: `Bearer ${getEnvValue(env, "AIRTABLE_API_KEY", "AIRTABLE_TOKEN")}`,
    "Content-Type": "application/json",
  };
}

function airtableFormulaString(value) {
  return `"${String(value).replace(/\\/g, "\\\\").replace(/"/g, '\\"')}"`;
}

function getMissingEnv(env) {
  return REQUIRED_ENV.filter((aliases) => !aliases.some((key) => env[key])).map((aliases) => aliases.join(" or "));
}

function getEnvValue(env, ...aliases) {
  const key = aliases.find((alias) => env[alias]);
  return key ? env[key] : "";
}

function jsonResponse(payload, status, request, env) {
  return new Response(JSON.stringify(payload), {
    status,
    headers: {
      ...JSON_HEADERS,
      ...corsHeaders(request, env),
    },
  });
}

function corsHeaders(request, env) {
  const requestOrigin = request.headers.get("Origin") || "";
  const allowedOrigin = env.ALLOWED_ORIGIN || requestOrigin || "*";
  const origin = allowedOrigin === "*" || allowedOrigin === requestOrigin ? allowedOrigin : "null";

  return {
    "Access-Control-Allow-Origin": origin,
    "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type, X-Edit-Key, X-LP-Edit-Key",
    "Access-Control-Max-Age": "86400",
    Vary: "Origin",
  };
}

function normalizeString(value) {
  return String(value || "").trim().toLowerCase();
}

function normalizeKey(value) {
  return String(value || "").trim();
}

function isPlainObject(value) {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}

function constantTimeEqual(a, b) {
  const left = String(a);
  const right = String(b);
  let mismatch = left.length === right.length ? 0 : 1;
  const length = Math.max(left.length, right.length);

  for (let index = 0; index < length; index += 1) {
    const leftCode = left.charCodeAt(index) || 0;
    const rightCode = right.charCodeAt(index) || 0;
    mismatch |= leftCode ^ rightCode;
  }

  return mismatch === 0;
}
