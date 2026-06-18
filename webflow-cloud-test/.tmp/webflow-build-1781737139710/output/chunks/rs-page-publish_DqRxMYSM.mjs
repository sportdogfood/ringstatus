globalThis.process ??= {};
globalThis.process.env ??= {};
import { r as runtimeEnv } from "./wec-plan-modules_BsQGnEh2.mjs";
import { r as renderRsPagePayload } from "./rs-page-render_D-1S8l_o.mjs";
const config = {
  runtime: "edge"
};
const RSCOM_BASE_ID = "appDN3R51ZPmwgMib";
const COMPILED_TABLE = "rs_page_compiled_payloads";
const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type,Authorization,X-RS-Publish-Token"
};
const OPTIONS = async () => new Response(null, { status: 204, headers: corsHeaders });
const GET = async ({ request }) => publish(request);
const POST = async ({ request }) => publish(request);
async function publish(request) {
  const url = new URL(request.url);
  const runtime = runtimeEnv();
  const token = runtime.AIRTABLE_TOKEN;
  const baseId = runtime.AIRTABLE_RSCOM_BASE_ID || runtime.RSCOM_AIRTABLE_BASE_ID || RSCOM_BASE_ID;
  const publishToken = runtime.RS_PAGE_PUBLISH_TOKEN || "";
  const pageKey = clean(url.searchParams.get("pageKey") || "rs_home");
  if (!token) return renderJson({ ok: false, error: "missing_airtable_token" }, 500);
  if (publishToken && getPublishToken(request, url) !== publishToken) {
    return renderJson({ ok: false, error: "unauthorized" }, 401);
  }
  try {
    const rendered = await renderRsPagePayload({ token, baseId, pageKey, refresh: true });
    const html = String(rendered.html || "");
    if (!html) throw new Error("rendered_html_empty");
    const sourceHash = await sha256(html);
    const saved = await upsertCompiledPayload({
      token,
      baseId,
      pageKey,
      html,
      sourceHash
    });
    return renderJson({
      ok: true,
      pageKey,
      table: COMPILED_TABLE,
      recordId: saved.id,
      sourceHash,
      compiledAt: saved.fields?.compiled_at || "",
      htmlLength: html.length
    });
  } catch (error) {
    return renderJson({
      ok: false,
      error: "rs_page_publish_failed",
      detail: error instanceof Error ? error.message : String(error)
    }, 502);
  }
}
async function upsertCompiledPayload({ token, baseId, pageKey, html, sourceHash }) {
  const existing = await findCompiledRecord({ token, baseId, pageKey });
  const fields = {
    page_key: pageKey,
    html,
    source_hash: sourceHash,
    compiled_at: (/* @__PURE__ */ new Date()).toISOString(),
    status: "active",
    notes: "Updated by rs-page-publish."
  };
  const response = await fetch(`https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(COMPILED_TABLE)}`, {
    method: existing ? "PATCH" : "POST",
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json"
    },
    body: JSON.stringify(existing ? { records: [{ id: existing.id, fields }] } : { records: [{ fields }] })
  });
  const payload = await response.json();
  if (!response.ok) throw new Error(`airtable_payload_save_failed:${response.status}:${JSON.stringify(payload)}`);
  return payload.records?.[0];
}
async function findCompiledRecord({ token, baseId, pageKey }) {
  const airtableUrl = new URL(`https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(COMPILED_TABLE)}`);
  airtableUrl.searchParams.set("pageSize", "1");
  airtableUrl.searchParams.set("filterByFormula", `{page_key}='${escapeFormulaString(pageKey)}'`);
  airtableUrl.searchParams.set("sort[0][field]", "compiled_at");
  airtableUrl.searchParams.set("sort[0][direction]", "desc");
  const response = await fetch(airtableUrl, {
    headers: { Authorization: `Bearer ${token}` }
  });
  const payload = await response.json();
  if (!response.ok) throw new Error(`airtable_payload_lookup_failed:${response.status}:${JSON.stringify(payload)}`);
  return payload.records?.[0] || null;
}
function getPublishToken(request, url) {
  return request.headers.get("x-rs-publish-token") || clean(url.searchParams.get("publishToken"));
}
async function sha256(value) {
  const bytes = new TextEncoder().encode(value);
  const digest = await crypto.subtle.digest("SHA-256", bytes);
  return Array.from(new Uint8Array(digest)).map((byte) => byte.toString(16).padStart(2, "0")).join("");
}
function renderJson(payload, status = 200) {
  return new Response(JSON.stringify(payload), {
    status,
    headers: {
      ...corsHeaders,
      "Content-Type": "application/json; charset=utf-8",
      "Cache-Control": "no-store"
    }
  });
}
function clean(value) {
  return String(value || "").trim();
}
function escapeFormulaString(value) {
  return String(value || "").replace(/\\/g, "\\\\").replace(/'/g, "\\'");
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
