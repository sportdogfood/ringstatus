export const config = { runtime: "edge" };

import { env } from "cloudflare:workers";
import {
  DEFAULT_RS_EDIT_BASE_ID,
  DEFAULT_RS_EDIT_FIELDS_TABLE,
  homeBackgroundFields,
  normalizePageKey,
  safeEditField
} from "../../lib/rs-edit.js";
import { HOME_BINDINGS } from "../../lib/rs-edit-home-bindings.js";

export const OPTIONS = async ({ request }) => corsResponse(request, null, 204);

export const GET = async ({ request }) => {
  try {
    const origin = allowedOrigin(request);
    if (!origin) return json(request, { ok: false, error: "origin_not_allowed" }, 403);
    const config = airtableConfig();
    if (!config.ok) return json(request, { ok: false, error: config.error }, 500);
    const pageKey = normalizePageKey(new URL(request.url).searchParams.get("page") || "home");
    if (!pageKey) return json(request, { ok: false, error: "invalid_page_key" }, 400);
    const records = await listRecords(config, config.fieldsTable);
    const fields = records
      .map(safeEditField)
      .filter((field) => field.editable && field.pageKey === pageKey)
      .sort(compareFields);
    if (pageKey === "home") fields.push(...homeBackgroundFields(HOME_BINDINGS));
    return json(request, { ok: true, pageKey, count: fields.length, fields });
  } catch (error) {
    console.error("[rs-edit] content failed", error);
    return json(request, {
      ok: false,
      error: "airtable_load_failed",
      detail: error instanceof Error ? error.message : String(error)
    }, 502);
  }
};

function airtableConfig() {
  const token = env.AIRTABLE_TOKEN;
  const baseId = env.RS_EDIT_AIRTABLE_BASE_ID || DEFAULT_RS_EDIT_BASE_ID;
  const fieldsTable = env.RS_EDIT_FIELDS_TABLE || DEFAULT_RS_EDIT_FIELDS_TABLE;
  if (!token) return { ok: false, error: "missing_airtable_token" };
  return { ok: true, token, baseId, fieldsTable };
}

async function listRecords(config, table) {
  const records = [];
  let offset = "";
  do {
    const endpoint = new URL(`https://api.airtable.com/v0/${encodeURIComponent(config.baseId)}/${encodeURIComponent(table)}`);
    endpoint.searchParams.set("pageSize", "100");
    if (offset) endpoint.searchParams.set("offset", offset);
    const response = await fetch(endpoint, { headers: { Authorization: `Bearer ${config.token}` } });
    const result = await response.json().catch(() => ({}));
    if (!response.ok) throw new Error(`list ${response.status}`);
    records.push(...(result.records || []));
    offset = result.offset || "";
  } while (offset);
  return records;
}

function compareFields(a, b) {
  return (a.mcpSort - b.mcpSort) || (a.mainSort - b.mainSort) ||
    ((a.cardIter || 0) - (b.cardIter || 0)) || ((a.tagIter || 0) - (b.tagIter || 0)) ||
    a.fieldKey.localeCompare(b.fieldKey);
}

function allowedOrigin(request) {
  const origin = request.headers.get("Origin") || "";
  if (!origin) return "same-origin";
  const configured = String(env.RS_EDIT_ALLOWED_ORIGINS || "https://ringstatus.com,https://www.ringstatus.com,https://ringstatus.webflow.io")
    .split(",").map((value) => value.trim()).filter(Boolean);
  return configured.includes(origin) ? origin : "";
}

function corsResponse(request, body, status) {
  const origin = allowedOrigin(request);
  return new Response(body, { status, headers: corsHeaders(origin) });
}

function json(request, data, status = 200) {
  const origin = allowedOrigin(request);
  return new Response(JSON.stringify(data) + "\n", { status, headers: { ...corsHeaders(origin), "Content-Type": "application/json; charset=utf-8" } });
}

function corsHeaders(origin) {
  return {
    "Access-Control-Allow-Origin": origin && origin !== "same-origin" ? origin : "https://ringstatus.com",
    "Access-Control-Allow-Methods": "GET,OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type,X-RS-Edit-Key",
    "Vary": "Origin"
  };
}
