export const config = {
  runtime: "edge"
};

import { corsHeaders, json } from "../../lib/wec-packing.js";

export const OPTIONS = async () => new Response(null, { status: 204, headers: corsHeaders });

export const POST = async () => json({
  ok: false,
  error: "writes_not_enabled",
  detail: "WEC packing writes are intentionally gated until wec_meta fields_allowed, physical table ids, and event-history targets are finalized."
}, 409, corsHeaders);
