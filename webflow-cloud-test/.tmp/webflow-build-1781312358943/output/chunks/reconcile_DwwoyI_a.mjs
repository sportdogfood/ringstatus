globalThis.process ??= {};
globalThis.process.env ??= {};
import { a as airtableConfig, j as json, r as reconcileReport, c as corsHeaders } from "./wec-packing_DImI9i28.mjs";
const config = {
  runtime: "edge"
};
const OPTIONS = async () => new Response(null, { status: 204, headers: corsHeaders });
const GET = async ({ request }) => {
  const airtable = airtableConfig();
  if (!airtable.ok) return json({ ok: false, error: airtable.error }, 500);
  try {
    const report = await reconcileReport(airtable, request.url);
    return json(report, report.ok ? 200 : 409);
  } catch (error) {
    console.error("[wec-packing] reconcile failed", error);
    return json({
      ok: false,
      error: "wec_reconcile_failed",
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
