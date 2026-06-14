globalThis.process ??= {};
globalThis.process.env ??= {};
import { a as airtableConfig, j as json, p as planReport, c as corsHeaders, d as planActionReport } from "./wec-plan-modules_BsQGnEh2.mjs";
const config = {
  runtime: "edge"
};
const OPTIONS = async () => new Response(null, { status: 204, headers: corsHeaders });
const GET = async ({ request }) => {
  const airtable = airtableConfig();
  if (!airtable.ok) return json({ ok: false, error: airtable.error }, 500);
  try {
    const report = await planReport(airtable, request.url, "per_horse");
    return json(report, report.ok ? 200 : 409);
  } catch (error) {
    console.error("[wec-plan-per-horse] state failed", error);
    return json({
      ok: false,
      error: "wec_plan_per_horse_failed",
      detail: error instanceof Error ? error.message : String(error)
    }, 502);
  }
};
const POST = async ({ request }) => {
  const airtable = airtableConfig();
  if (!airtable.ok) return json({ ok: false, error: airtable.error }, 500);
  try {
    const payload = await request.json().catch(() => ({}));
    const report = await planActionReport(airtable, request.url, "per_horse", payload);
    return json(report, report.ok ? 200 : 400);
  } catch (error) {
    console.error("[wec-plan-per-horse] action failed", error);
    return json({
      ok: false,
      error: "wec_plan_per_horse_action_failed",
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
