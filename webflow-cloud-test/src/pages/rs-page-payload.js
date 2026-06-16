import { RS_PAGE_STATIC_PAYLOAD } from "../lib/rs-page-static-payload.js";

export const config = {
  runtime: "edge"
};

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

  if (all) {
    return renderJson({
      ok: true,
      generatedAt: RS_PAGE_STATIC_PAYLOAD.generatedAt,
      pages: RS_PAGE_STATIC_PAYLOAD.pages || {}
    });
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
