export const config = {
  runtime: "edge"
};

import {
  airtableConfig,
  corsHeaders,
  json,
  planActionReport,
  planReport
} from "../../lib/wec-plan-modules.js";

export const OPTIONS = async () => new Response(null, { status: 204, headers: corsHeaders });

export const GET = async ({ request }) => {
  const airtable = airtableConfig();
  if (!airtable.ok) return json({ ok: false, error: airtable.error }, 500);

  try {
    const report = await planReport(airtable, request.url, "quantity");
    return json(report, report.ok ? 200 : 409);
  } catch (error) {
    console.error("[wec-plan-quantity] state failed", error);
    return json({
      ok: false,
      error: "wec_plan_quantity_failed",
      detail: error instanceof Error ? error.message : String(error)
    }, 502);
  }
};

export const POST = async ({ request }) => {
  const airtable = airtableConfig();
  if (!airtable.ok) return json({ ok: false, error: airtable.error }, 500);

  try {
    const payload = await request.json().catch(() => ({}));
    const report = await planActionReport(airtable, request.url, "quantity", payload);
    return json(report, report.ok ? 200 : 400);
  } catch (error) {
    console.error("[wec-plan-quantity] action failed", error);
    return json({
      ok: false,
      error: "wec_plan_quantity_action_failed",
      detail: error instanceof Error ? error.message : String(error)
    }, 502);
  }
};
