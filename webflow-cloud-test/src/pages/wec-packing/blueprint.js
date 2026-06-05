export const config = {
  runtime: "edge"
};

import {
  airtableConfig,
  blueprintReport,
  corsHeaders,
  json
} from "../../lib/wec-blueprint.js";

export const OPTIONS = async () => new Response(null, { status: 204, headers: corsHeaders });

export const GET = async () => {
  const airtable = airtableConfig();
  if (!airtable.ok) return json({ ok: false, error: airtable.error }, 500);

  try {
    const report = await blueprintReport(airtable);
    return json(report);
  } catch (error) {
    console.error("[wec-blueprint] failed", error);
    return json({
      ok: false,
      error: "wec_blueprint_failed",
      detail: error instanceof Error ? error.message : String(error)
    }, 502);
  }
};
