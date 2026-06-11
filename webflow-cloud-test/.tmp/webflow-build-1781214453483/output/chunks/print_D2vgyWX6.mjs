globalThis.process ??= {};
globalThis.process.env ??= {};
import { a as airtableConfig, c as corsHeaders, b as planPrintHtml } from "./wec-plan-modules_hXO0hoAk.mjs";
const config = {
  runtime: "edge"
};
const OPTIONS = async () => new Response(null, { status: 204, headers: corsHeaders });
const GET = async ({ request }) => {
  const airtable = airtableConfig();
  if (!airtable.ok) {
    return new Response(`Per Horse print unavailable: ${airtable.error}`, {
      status: 500,
      headers: { ...corsHeaders, "Content-Type": "text/plain; charset=utf-8" }
    });
  }
  try {
    const html = await planPrintHtml(airtable, request.url, "per_horse");
    return new Response(html, {
      status: 200,
      headers: { ...corsHeaders, "Content-Type": "text/html; charset=utf-8" }
    });
  } catch (error) {
    console.error("[wec-plan-per-horse] print failed", error);
    return new Response(`Per Horse print failed: ${error instanceof Error ? error.message : String(error)}`, {
      status: 502,
      headers: { ...corsHeaders, "Content-Type": "text/plain; charset=utf-8" }
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
