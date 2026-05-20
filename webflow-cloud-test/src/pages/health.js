export const config = {
  runtime: "edge"
};

import { env } from "cloudflare:workers";

export const GET = async () => {
  return json({
    ok: true,
    service: "webflow-cloud-test",
    enrichmentEndpoint: "/test/lp-history/enrichment",
    horsesEndpoint: "/test/8778-tack-horses/horses",
    hpsEndpoint: "/test/hps/horses",
    env: {
      hasAirtableToken: !!env.AIRTABLE_TOKEN,
      hasAirtableBaseId: !!(env.AIRTABLE_BASE_ID || env.AIRTABLE_BASE),
      table: env.AIRTABLE_TABLE_LP || env.AIRTABLE_TABLE || "",
      horsesTable: env.AIRTABLE_WW_HORSES_TABLE || env.AIRTABLE_HORSES_TABLE || "ww_horses",
      horsesView: env.AIRTABLE_WW_HORSES_VIEW || env.AIRTABLE_HORSES_VIEW || "8778-tack-horses",
      horsesChangeLog: env.AIRTABLE_HORSES_CHANGE_LOG_TABLE || "horses_change_log",
      hpsHorsesTable: env.AIRTABLE_HPS_HORSES_TABLE || env.AIRTABLE_WW_HORSES_TABLE || env.AIRTABLE_HORSES_TABLE || "ww_horses",
      hpsViewPrefix: env.AIRTABLE_HPS_VIEW_PREFIX || "hps_",
      hpsChangeLog: env.AIRTABLE_HPS_CHANGE_LOG_TABLE || "hp_cls",
      hpsActiveTenantsTable: env.AIRTABLE_HPS_ACTIVE_TENANTS_TABLE || "active_tenants",
      hpsActiveTenantsView: env.AIRTABLE_HPS_ACTIVE_TENANTS_VIEW || "active_tenants"
    }
  });
};

function json(data, status = 200) {
  return new Response(JSON.stringify(data, null, 2) + "\n", {
    status,
    headers: {
      "Content-Type": "application/json; charset=utf-8"
    }
  });
}
