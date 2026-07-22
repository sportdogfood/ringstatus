export const config = { runtime: "edge" };

import { env } from "cloudflare:workers";
import {
  DEFAULT_RS_EDIT_BASE_ID,
  DEFAULT_RS_EDIT_EDITS_TABLE,
  DEFAULT_RS_EDIT_FIELDS_TABLE,
  DEFAULT_RS_EDIT_TARGET_PAGE_ID,
  homeBackgroundFields,
  normalizePageKey,
  safeEditField,
  validateChanges
} from "../../lib/rs-edit.js";
import { HOME_BINDINGS } from "../../lib/rs-edit-home-bindings.js";

export const OPTIONS = async ({ request }) => new Response(null, { status: 204, headers: corsHeaders(allowedOrigin(request)) });

export const POST = async ({ request }) => {
  const origin = allowedOrigin(request);
  if (!origin) return json(request, { ok: false, error: "origin_not_allowed" }, 403);
  const config = airtableConfig();
  if (!config.ok) return json(request, { ok: false, error: config.error }, 500);
  if (!validSaveKey(request)) return json(request, { ok: false, error: "save_key_required" }, 401);

  const payload = await readJson(request);
  const pageKey = normalizePageKey(payload.pageKey);
  if (!pageKey) return json(request, { ok: false, error: "invalid_page_key" }, 400);

  try {
    const registryRecords = await listRecords(config, config.fieldsTable);
    const allowlist = new Map(registryRecords.map((record) => {
      const field = safeEditField(record);
      return [field.fieldKey, field];
    }));
    if (pageKey === "home") {
      for (const field of homeBackgroundFields(HOME_BINDINGS)) allowlist.set(field.fieldKey, field);
    }
    const validation = validateChanges(payload, allowlist);
    if (!validation.ok) return json(request, { ok: false, ...validation }, 400);

    const batchId = crypto.randomUUID();
    const createdAt = new Date().toISOString();
    const edits = [];
    for (const change of validation.changes) {
      if (change.fieldType === "image") return json(request, { ok: false, error: "image_editing_deferred" }, 400);
      const fields = editFields(change, batchId, createdAt, config.targetPageId);
      const edit = await createRecord(config, config.editsTable, fields);
      edits.push({ id: edit.id, fieldKey: change.fieldKey, fieldType: change.fieldType });
    }

    return json(request, { ok: true, action: "content_edits_saved", stopped: true, pageKey, batchId, count: edits.length, edits });
  } catch (error) {
    console.error("[rs-edit] save failed", error);
    return json(request, { ok: false, error: "content_edit_save_failed" }, 502);
  }
};

function editFields(change, batchId, createdAt, targetPageId) {
  const field = change.field;
  const fields = {
    edit_key: `${batchId}:${change.fieldKey}`,
    save_batch_id: batchId,
    source_action: field.contentAction ? [field.contentAction] : undefined,
    field_type: change.fieldType,
    target_page_key: field.pageKey,
    target_webflow_page_id: targetPageId,
    target_element_id: field.elementId,
    source_table: field.pageKey,
    source_record_id: field.sourceRecordId,
    created_at: createdAt
  };
  if (field.page) fields.page = [field.page];
  if (field.mcp) fields.mcp = [field.mcp];
  if (change.fieldType === "text") fields.text_content = change.textContent;
  if (change.fieldType === "color") fields.color_hex = change.colorHex;
  fields.status = "saved";
  return Object.fromEntries(Object.entries(fields).filter(([, value]) => value !== undefined && value !== ""));
}

function airtableConfig() {
  const token = env.AIRTABLE_TOKEN;
  const baseId = env.RS_EDIT_AIRTABLE_BASE_ID || DEFAULT_RS_EDIT_BASE_ID;
  const fieldsTable = env.RS_EDIT_FIELDS_TABLE || DEFAULT_RS_EDIT_FIELDS_TABLE;
  const editsTable = env.RS_EDIT_EDITS_TABLE || DEFAULT_RS_EDIT_EDITS_TABLE;
  const targetPageId = env.RS_EDIT_TARGET_PAGE_ID || DEFAULT_RS_EDIT_TARGET_PAGE_ID;
  if (!token) return { ok: false, error: "missing_airtable_token" };
  return { ok: true, token, baseId, fieldsTable, editsTable, targetPageId };
}

function validSaveKey(request) {
  const required = String(env.RS_EDIT_SAVE_KEY || "");
  const provided = String(request.headers.get("X-RS-Edit-Key") || "");
  return required.length >= 16 && provided === required;
}

async function listRecords(config, table) {
  const records = [];
  let offset = "";
  do {
    const endpoint = airtableUrl(config.baseId, table);
    endpoint.searchParams.set("pageSize", "100");
    if (offset) endpoint.searchParams.set("offset", offset);
    const response = await fetch(endpoint, { headers: authHeaders(config.token) });
    const result = await response.json().catch(() => ({}));
    if (!response.ok) throw new Error(`list ${response.status}`);
    records.push(...(result.records || []));
    offset = result.offset || "";
  } while (offset);
  return records;
}

async function createRecord(config, table, fields) {
  const response = await fetch(airtableUrl(config.baseId, table), {
    method: "POST",
    headers: { ...authHeaders(config.token), "Content-Type": "application/json" },
    body: JSON.stringify({ records: [{ fields }], typecast: true })
  });
  const result = await response.json().catch(() => ({}));
  if (!response.ok || !result.records?.[0]?.id) throw new Error(`create ${response.status}`);
  return result.records[0];
}

async function readJson(request) {
  try {
    return JSON.parse(await request.text() || "{}");
  } catch {
    return {};
  }
}

function airtableUrl(baseId, table) {
  return new URL(`https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(table)}`);
}

function authHeaders(token) {
  return { Authorization: `Bearer ${token}` };
}

function allowedOrigin(request) {
  const origin = request.headers.get("Origin") || "";
  if (!origin) return "same-origin";
  const configured = String(env.RS_EDIT_ALLOWED_ORIGINS || "https://ringstatus.com,https://www.ringstatus.com,https://ringstatus.webflow.io")
    .split(",").map((value) => value.trim()).filter(Boolean);
  return configured.includes(origin) ? origin : "";
}

function json(request, data, status = 200) {
  return new Response(JSON.stringify(data) + "\n", { status, headers: { ...corsHeaders(allowedOrigin(request)), "Content-Type": "application/json; charset=utf-8" } });
}

function corsHeaders(origin) {
  return {
    "Access-Control-Allow-Origin": origin && origin !== "same-origin" ? origin : "https://ringstatus.com",
    "Access-Control-Allow-Methods": "POST,OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type,X-RS-Edit-Key",
    "Vary": "Origin"
  };
}
