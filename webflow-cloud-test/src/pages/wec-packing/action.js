export const config = {
  runtime: "edge"
};

import { actionReport, airtableConfig, corsHeaders, json } from "../../lib/wec-packing.js";

export const OPTIONS = async () => new Response(null, { status: 204, headers: corsHeaders });

export const POST = async ({ request }) => {
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
