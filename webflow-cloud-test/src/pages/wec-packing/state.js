export const config = {
  runtime: "edge"
};

import { env } from "cloudflare:workers";
import { corsHeaders, getWecAirtableConfig, json, loadWecPackingState } from "../../lib/wec-packing.js";

export const OPTIONS = async () => new Response(null, { status: 204, headers: corsHeaders });

export const GET = async ({ request }) => {
  const airtable = getWecAirtableConfig(env || {});
  if (!airtable.ok) return json({ ok: false, error: airtable.error }, 500, corsHeaders);

  const url = new URL(request.url);
  const options = {
    showId: url.searchParams.get("showId") || "",
    packWaveId: url.searchParams.get("packWaveId") || "",
    groomCount: url.searchParams.get("groomCount") || ""
  };

  try {
    const state = await loadWecPackingState(airtable, options);
    return json(state, 200, corsHeaders);
  } catch (error) {
    console.error("[wec-packing] state load failed", error);
    return json({
      ok: false,
      error: "wec_packing_state_failed",
      detail: error instanceof Error ? error.message : String(error)
    }, 502, corsHeaders);
  }
};
