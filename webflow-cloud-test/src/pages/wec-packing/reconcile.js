export const config = {
  runtime: "edge"
};

import { airtableConfig, corsHeaders, json, reconcileReport } from "../../lib/wec-packing.js";

export const OPTIONS = async () => new Response(null, { status: 204, headers: corsHeaders });

export const GET = async ({ request }) => {
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
