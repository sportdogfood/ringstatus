globalThis.process ??= {};
globalThis.process.env ??= {};
import { a as airtableConfig, c as corsHeaders, s as stateReport, p as printReportHtml } from "./wec-packing_DImI9i28.mjs";
const config = {
  runtime: "edge"
};
const OPTIONS = async () => new Response(null, { status: 204, headers: corsHeaders });
const GET = async ({ request }) => {
  const airtable = airtableConfig();
  if (!airtable.ok) {
    return new Response(`WEC print unavailable: ${airtable.error}`, {
      status: 500,
      headers: {
        ...corsHeaders,
        "Content-Type": "text/plain; charset=utf-8"
      }
    });
  }
  try {
    const report = await stateReport(airtable, request.url);
    const html = printReportHtml(report, request.url);
    return new Response(html, {
      status: report.ok ? 200 : 409,
      headers: {
        ...corsHeaders,
        "Content-Type": "text/html; charset=utf-8"
      }
    });
  } catch (error) {
    console.error("[wec-packing] print failed", error);
    return new Response(`WEC print failed: ${error instanceof Error ? error.message : String(error)}`, {
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
