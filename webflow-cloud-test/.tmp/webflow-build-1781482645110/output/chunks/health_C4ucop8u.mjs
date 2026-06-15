globalThis.process ??= {};
globalThis.process.env ??= {};
import { a as airtableConfig, j as json, h as healthReport, c as corsHeaders } from "./wec-packing_STIQVb0i.mjs";
const config = {
  runtime: "edge"
};
const OPTIONS = async () => new Response(null, { status: 204, headers: corsHeaders });
const GET = async () => {
  const airtable = airtableConfig();
  if (!airtable.ok) return json({ ok: false, error: airtable.error }, 500);
  try {
    return json(await healthReport(airtable));
  } catch (error) {
    console.error("[wec-packing] health failed", error);
    return json({
      ok: false,
      error: "wec_health_failed",
      detail: error instanceof Error ? error.message : String(error)
    }, 502);
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
