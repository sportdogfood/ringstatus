export const config = {
  runtime: "edge"
};

import {
  airtableConfig,
  corsHeaders,
  horseKitLaneActionReport,
  horseKitLaneActionReportV2,
  horseKitLaneReport,
  horseKitLaneReportV2,
  json
} from "../../lib/wec-packing.js";

export const OPTIONS = async () => new Response(null, { status: 204, headers: corsHeaders });

export const GET = async ({ request }) => {
  const airtable = airtableConfig();
  if (!airtable.ok) return json({ ok: false, error: airtable.error }, 500);

  try {
    const url = new URL(request.url);
    const report = url.searchParams.get("v") === "2"
      ? await horseKitLaneReportV2(airtable, request.url)
      : await horseKitLaneReport(airtable, request.url);
    return json(report, report.ok ? 200 : 409);
  } catch (error) {
    console.error("[wec-packing] horse kits lane failed", error);
    return json({
      ok: false,
      error: "wec_horse_kits_lane_failed",
      detail: error instanceof Error ? error.message : String(error)
    }, 502);
  }
};

export const POST = async ({ request }) => {
  const airtable = airtableConfig();
  if (!airtable.ok) return json({ ok: false, error: airtable.error }, 500);

  try {
    const payload = await request.json().catch(() => ({}));
    const url = new URL(request.url);
    const report = url.searchParams.get("v") === "2"
      ? await horseKitLaneActionReportV2(airtable, request.url, payload)
      : await horseKitLaneActionReport(airtable, request.url, payload);
    return json(report, report.ok ? 200 : 400);
  } catch (error) {
    console.error("[wec-packing] horse kits lane action failed", error);
    return json({
      ok: false,
      error: "wec_horse_kits_lane_action_failed",
      detail: error instanceof Error ? error.message : String(error)
    }, 502);
  }
};
