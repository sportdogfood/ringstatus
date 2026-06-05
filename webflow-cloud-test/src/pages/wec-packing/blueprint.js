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
    return json(await blueprintReport(airtable));
  } catch (error) {
    console.error("[wec-blueprint] report failed", error);
    return json({
      ok: false,
      error: "wec_blueprint_failed",
      detail: error instanceof Error ? error.message : String(error)
    }, 502);
  }
};
