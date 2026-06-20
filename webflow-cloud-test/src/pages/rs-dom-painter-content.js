export const config = {
  runtime: "edge"
};

import {
  corsHeaders,
  json,
  runtimeEnv
} from "../lib/wec-plan-modules.js";

const RSCOM_BASE_ID = "appDN3R51ZPmwgMib";
const CONTENT_TABLE = "rs_content";
const DEFAULT_CONTENT_KEY = "rs_home_section_1_content_1";

export const OPTIONS = async () => new Response(null, { status: 204, headers: corsHeaders });

export const GET = async ({ url }) => {
  const runtime = runtimeEnv();
  const token = runtime.AIRTABLE_TOKEN;
  const baseId = runtime.AIRTABLE_RSCOM_BASE_ID || runtime.RSCOM_AIRTABLE_BASE_ID || RSCOM_BASE_ID;
  const contentKey = clean(url.searchParams.get("contentKey") || DEFAULT_CONTENT_KEY);

  if (!token) return json({ ok: false, error: "missing_airtable_token" }, 500);
  if (!contentKey) return json({ ok: false, error: "missing_content_key" }, 400);

  try {
    const record = await findContentRecord({ token, baseId, contentKey });
    if (!record) return json({ ok: false, error: "content_not_found", contentKey }, 404);

    return json({
      ok: true,
      source: {
        baseId,
        table: CONTENT_TABLE,
        contentKey,
        recordId: record.id
      },
      content: {
        eyebrow: clean(record.fields.eyebrow),
        headline: clean(record.fields.headline),
        body: clean(record.fields.body)
      }
    });
  } catch (error) {
    return json({
      ok: false,
      error: "rs_dom_painter_content_failed",
      detail: error instanceof Error ? error.message : String(error)
    }, 502);
  }
};

async function findContentRecord({ token, baseId, contentKey }) {
  const endpoint = new URL(`https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(CONTENT_TABLE)}`);
  endpoint.searchParams.set("maxRecords", "1");
  endpoint.searchParams.set("filterByFormula", `{content_key} = "${escapeFormulaString(contentKey)}"`);
  ["content_key", "eyebrow", "headline", "body"].forEach((field) => {
    endpoint.searchParams.append("fields[]", field);
  });

  const response = await fetch(endpoint.toString(), {
    headers: { Authorization: `Bearer ${token}` }
  });
  if (!response.ok) {
    throw new Error(`airtable_content_failed:${response.status}:${await response.text()}`);
  }
  const payload = await response.json();
  return (payload.records || [])[0] || null;
}

function escapeFormulaString(value) {
  return String(value ?? "").replace(/\\/g, "\\\\").replace(/"/g, '\\"');
}

function clean(value) {
  if (Array.isArray(value)) return value.map(clean).filter(Boolean).join(", ");
  return String(value ?? "").trim();
}
