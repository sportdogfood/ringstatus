import { RS_PAGE_STATIC_PAYLOAD } from "../lib/rs-page-static-payload.js";
import { runtimeEnv } from "../lib/wec-plan-modules.js";

export const config = {
  runtime: "edge"
};

const RSCOM_BASE_ID = "appDN3R51ZPmwgMib";
const COMPILED_TABLE = "rs_page_compiled_payloads";

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET,OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type,Authorization"
};

export const OPTIONS = async () => new Response(null, { status: 204, headers: corsHeaders });

export const GET = async ({ request }) => {
  const url = new URL(request.url);
  const pageKey = clean(url.searchParams.get("pageKey"));
  const all = clean(url.searchParams.get("all")) === "1";
  const runtime = runtimeEnv();
  const token = runtime.AIRTABLE_TOKEN;
  const baseId = runtime.AIRTABLE_RSCOM_BASE_ID || runtime.RSCOM_AIRTABLE_BASE_ID || RSCOM_BASE_ID;

  if (all) {
    const compiledPages = token ? await readCompiledPages({ token, baseId }).catch(() => ({})) : {};
    return renderJson({
      ok: true,
      generatedAt: new Date().toISOString(),
      pages: {
        ...(RS_PAGE_STATIC_PAYLOAD.pages || {}),
        ...compiledPages
      }
    });
  }

  if (token && pageKey) {
    const compiled = await readCompiledPage({ token, baseId, pageKey }).catch(() => null);
    if (compiled?.html) {
      return renderJson({
        ok: true,
        pageKey,
        generatedAt: compiled.compiledAt || new Date().toISOString(),
        html: compiled.html,
        source: {
          mode: "airtable_compiled_payload",
          baseId,
          table: COMPILED_TABLE,
          recordId: compiled.recordId,
          sourceHash: compiled.sourceHash || ""
        }
      });
    }
  }

  const page = pageKey ? RS_PAGE_STATIC_PAYLOAD.pages?.[pageKey] : null;
  if (!page) {
    return renderJson({ ok: false, error: "unknown_page", pageKey }, 404);
  }

  return renderJson({
    ok: true,
    pageKey,
    generatedAt: RS_PAGE_STATIC_PAYLOAD.generatedAt,
    html: page.html || "",
    source: page.source || null
  });
};

function renderJson(payload, status = 200) {
  return new Response(JSON.stringify(payload), {
    status,
    headers: {
      ...corsHeaders,
      "Content-Type": "application/json; charset=utf-8",
      "Cache-Control": "public, max-age=300, stale-while-revalidate=3600"
    }
  });
}

function clean(value) {
  return String(value || "").trim();
}

async function readCompiledPages({ token, baseId }) {
  const records = await listCompiledRecords({ token, baseId });
  const pages = {};
  records
    .filter((record) => clean(record.fields.status) === "active")
    .filter((record) => clean(record.fields.page_key) && clean(record.fields.html))
    .forEach((record) => {
      const key = clean(record.fields.page_key);
      if (pages[key]) return;
      pages[key] = {
      html: String(record.fields.html || ""),
      source: {
        mode: "airtable_compiled_payload",
        baseId,
        table: COMPILED_TABLE,
        recordId: record.id,
        sourceHash: clean(record.fields.source_hash)
      }
      };
    });
  return pages;
}

async function readCompiledPage({ token, baseId, pageKey }) {
  const formula = `AND({page_key}='${escapeFormulaString(pageKey)}',{status}='active')`;
  const records = await listCompiledRecords({ token, baseId, formula, maxRecords: 1 });
  const record = records[0];
  if (!record) return null;
  return {
    recordId: record.id,
    html: String(record.fields.html || ""),
    compiledAt: clean(record.fields.compiled_at),
    sourceHash: clean(record.fields.source_hash)
  };
}

async function listCompiledRecords({ token, baseId, formula = "", maxRecords = 100 } = {}) {
  const records = [];
  let offset = "";
  do {
    const airtableUrl = new URL(`https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(COMPILED_TABLE)}`);
    airtableUrl.searchParams.set("pageSize", String(Math.min(maxRecords, 100)));
    if (formula) airtableUrl.searchParams.set("filterByFormula", formula);
    airtableUrl.searchParams.set("sort[0][field]", "compiled_at");
    airtableUrl.searchParams.set("sort[0][direction]", "desc");
    if (offset) airtableUrl.searchParams.set("offset", offset);
    const response = await fetch(airtableUrl, {
      headers: { Authorization: `Bearer ${token}` }
    });
    if (!response.ok) throw new Error(`airtable_compiled_payload_failed:${response.status}:${await response.text()}`);
    const payload = await response.json();
    records.push(...(payload.records || []));
    offset = payload.offset || "";
  } while (offset && records.length < maxRecords);
  return records.slice(0, maxRecords);
}

function escapeFormulaString(value) {
  return String(value || "").replace(/\\/g, "\\\\").replace(/'/g, "\\'");
}
