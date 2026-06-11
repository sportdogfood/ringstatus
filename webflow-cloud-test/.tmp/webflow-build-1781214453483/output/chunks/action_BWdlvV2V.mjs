globalThis.process ??= {};
globalThis.process.env ??= {};
import { c as corsHeaders, a as airtableConfig, j as json, b as actionReport } from "./wec-packing_DImI9i28.mjs";
const config = {
  runtime: "edge"
};
const OPTIONS = async () => new Response(null, { status: 204, headers: corsHeaders });
const POST = async ({ request }) => {
  const airtable = airtableConfig();
  if (!airtable.ok) return json({ ok: false, error: airtable.error }, 500);
  try {
    const payload = await request.json().catch(() => ({}));
    const report = await actionReport(airtable, request.url, payload);
    return json(report, report.ok ? 200 : 400);
  } catch (error) {
    console.error("[wec-packing] action failed", error);
    return json({
      ok: false,
      error: "wec_action_failed",
      detail: error instanceof Error ? error.message : String(error)
    }, 502);
  }
};
const _page = /* @__PURE__ */ Object.freeze(/* @__PURE__ */ Object.defineProperty({
  __proto__: null,
  OPTIONS,
  POST,
  config
}, Symbol.toStringTag, { value: "Module" }));
const page = () => _page;
export {
  page
};
