export const config = {
  runtime: "edge"
};

import {
  airtableConfig,
  corsHeaders,
  planPrintHtml
} from "../../../lib/wec-plan-modules.js";

export const OPTIONS = async () => new Response(null, { status: 204, headers: corsHeaders });

export const GET = async ({ request }) => {
  const airtable = airtableConfig();
  if (!airtable.ok) {
    return new Response(`Per Groom print unavailable: ${airtable.error}`, {
      status: 500,
      headers: { ...corsHeaders, "Content-Type": "text/plain; charset=utf-8" }
    });
  }

  try {
    const html = await planPrintHtml(airtable, request.url, "per_groom");
    return new Response(html, {
      status: 200,
      headers: { ...corsHeaders, "Content-Type": "text/html; charset=utf-8" }
    });
  } catch (error) {
    console.error("[wec-plan-per-groom] print failed", error);
    return new Response(`Per Groom print failed: ${error instanceof Error ? error.message : String(error)}`, {
      status: 502,
      headers: { ...corsHeaders, "Content-Type": "text/plain; charset=utf-8" }
    });
  }
};
