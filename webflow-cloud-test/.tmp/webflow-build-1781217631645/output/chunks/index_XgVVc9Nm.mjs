globalThis.process ??= {};
globalThis.process.env ??= {};
import { a as airtableConfig, j as json, s as stateReport, c as corsHeaders, b as actionReport } from "./wec-packing_DImI9i28.mjs";
const config = {
  runtime: "edge"
};
const OPTIONS = async () => new Response(null, { status: 204, headers: corsHeaders });
const GET = async ({ request }) => {
  const airtable = airtableConfig();
  if (!airtable.ok) return json({ ok: false, error: airtable.error }, 500);
  try {
    const state = await stateReport(airtable, request.url);
    return json(state, state.ok ? 200 : 409);
  } catch (error) {
    console.error("[wec-packing] state failed", error);
    return json({
      ok: false,
      error: "wec_state_failed",
      detail: error instanceof Error ? error.message : String(error)
    }, 502);
  }
};
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
  GET,
  OPTIONS,
  POST,
  config
}, Symbol.toStringTag, { value: "Module" }));
const page = () => _page;
export {
  page
};
