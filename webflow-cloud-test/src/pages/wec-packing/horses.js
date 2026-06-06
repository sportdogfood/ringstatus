export const config = {
  runtime: "edge"
};

import { env } from "cloudflare:workers";

import {
  airtableConfig,
  corsHeaders,
  horseEntityReport,
  json,
  runtimeEnv
} from "../../lib/horse-entity-ui.js";

export const OPTIONS = async () => new Response(null, { status: 204, headers: corsHeaders });

export const GET = async ({ request }) => {
  const airtable = airtableConfig(runtimeEnv(env));
  if (!airtable.ok) return json({ ok: false, error: airtable.error }, 500);

  try {
    return json(await horseEntityReport(airtable, request.url));
  } catch (error) {
    console.error("[wec-packing-horses] load failed", error);
    return json({
      ok: false,
      error: "wec_packing_horses_load_failed",
      detail: error instanceof Error ? error.message : String(error)
    }, 502);
  }
};

export const POST = async ({ request }) => {
  await request.text().catch(() => "");
  return json({
    ok: false,
    error: "horse_entity_writes_not_enabled_in_p2"
  }, 405);
};
