globalThis.process ??= {};
globalThis.process.env ??= {};
import { a as airtableConfig, c as corsHeaders, h as horseKitReport, b as horseKitPrintHtml } from "./wec-horse-kits_nKY3K1Ah.mjs";
const config = {
  runtime: "edge"
};
const OPTIONS = async () => new Response(null, { status: 204, headers: corsHeaders });
const GET = async ({ request }) => {
  const airtable = airtableConfig();
  if (!airtable.ok) {
    return new Response(`WEC horse kits print unavailable: ${airtable.error}`, {
      status: 500,
      headers: {
        ...corsHeaders,
        "Content-Type": "text/plain; charset=utf-8"
      }
    });
  }
  try {
    const report = await horseKitReport(airtable, request.url);
    const html = horseKitPrintHtml(report, request.url);
    return new Response(html, {
      status: report.ok ? 200 : 409,
      headers: {
        ...corsHeaders,
        "Content-Type": "text/html; charset=utf-8"
      }
    });
  } catch (error) {
    console.error("[wec-packing] horse kits print failed", error);
    return new Response(`WEC horse kits print failed: ${error instanceof Error ? error.message : String(error)}`, {
      status: 502,
      headers: {
        ...corsHeaders,
        "Content-Type": "text/plain; charset=utf-8"
      }
    });
  }
};
const _page = /* @__PURE__ */ Object.freeze(/* @__PURE__ */ Object.defineProperty({
  __proto__: null,
  GET,
  OPTIONS,
  config
}, Symbol.toStringTag, { value: "Module" }));
const page = () => _page;
export {
  page
};
