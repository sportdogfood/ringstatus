export const config = {
  runtime: "edge"
};

import { env } from "cloudflare:workers";

export const GET = async () => {
  return json({
    ok: true,
    service: "webflow-cloud-test",
    enrichmentEndpoint: "/test/lp-history/enrichment",
    profileContentEndpoint: "/test/lp-profile/content",
    personalizedSectionEndpoint: "/test/personalized-section/content",
    horsesEndpoint: "/test/8778-tack-horses/horses",
    hpsEndpoint: "/test/hps/horses",
    wecPackingHealthEndpoint: "/test/wec-packing/health",
    wecPackingStateEndpoint: "/test/wec-packing/state",
    env: {
      hasAirtableToken: !!env.AIRTABLE_TOKEN,
      hasAirtableBaseId: !!(env.AIRTABLE_BASE_ID || env.AIRTABLE_BASE),
      table: env.AIRTABLE_TABLE_LP || env.AIRTABLE_TABLE || "",
      profileContentTable: env.AIRTABLE_TABLE_PROFILE_CONTENT || "",
      profileChangeLogTable: env.AIRTABLE_TABLE_PROFILE_CHANGE_LOG || "",
      horsesTable: env.AIRTABLE_WW_HORSES_TABLE || env.AIRTABLE_HORSES_TABLE || "ww_horses",
      horsesView: env.AIRTABLE_WW_HORSES_VIEW || env.AIRTABLE_HORSES_VIEW || "8778-tack-horses",
      horsesChangeLog: env.AIRTABLE_HORSES_CHANGE_LOG_TABLE || "horses_change_log",
      hpsHorsesTable: env.AIRTABLE_HPS_HORSES_TABLE || env.AIRTABLE_WW_HORSES_TABLE || env.AIRTABLE_HORSES_TABLE || "ww_horses",
      hpsViewPrefix: env.AIRTABLE_HPS_VIEW_PREFIX || "hps_",
      hpsChangeLog: env.AIRTABLE_HPS_CHANGE_LOG_TABLE || "hp_cls",
      hpsActiveTenantsTable: env.AIRTABLE_HPS_ACTIVE_TENANTS_TABLE || "active_tenants",
      hpsActiveTenantsView: env.AIRTABLE_HPS_ACTIVE_TENANTS_VIEW || "active_tenants",
      wecPackWavesTable: env.AIRTABLE_WEC_PACK_WAVES_TABLE || "",
      wecPackingItemsTable: env.AIRTABLE_WEC_PACKING_ITEMS_TABLE || "",
      wecPackingItemHorsesTable: env.AIRTABLE_WEC_PACKING_ITEM_HORSES_TABLE || "",
      wecPackingEventsTable: env.AIRTABLE_WEC_PACKING_EVENTS_TABLE || ""
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
