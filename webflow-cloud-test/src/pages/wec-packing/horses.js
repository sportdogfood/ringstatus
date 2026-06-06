export const config = {
  runtime: "edge"
};

import { env } from "cloudflare:workers";

import {
  airtableConfig,
  corsHeaders,
  horseEntityActionReport,
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
  const airtable = airtableConfig(runtimeEnv(env));
  if (!airtable.ok) return json({ ok: false, error: airtable.error }, 500);

  try {
    const payload = await request.json().catch(() => ({}));
    const report = await horseEntityActionReport(airtable, request.url, payload);
    return json(report, report.ok ? 200 : 400);
  } catch (error) {
    console.error("[wec-packing-horses] action failed", error);
    const detail = error instanceof Error ? error.message : String(error);
    const status = isHorseActionValidationError(detail) ? 400 : 502;
    return json({
      ok: false,
      error: "wec_packing_horses_action_failed",
      detail
    }, status);
  }
};

function isHorseActionValidationError(detail) {
  return detail.startsWith("missing_")
    || detail.startsWith("invalid_")
    || detail.startsWith("no_allowed_")
    || detail.endsWith("_not_found");
}
