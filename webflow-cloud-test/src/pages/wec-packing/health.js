export const config = {
  runtime: "edge"
};

import { airtableConfig, corsHeaders, healthReport, json } from "../../lib/wec-packing.js";

export const OPTIONS = async () => new Response(null, { status: 204, headers: corsHeaders });

export const GET = async () => {
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
