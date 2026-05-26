export const config = {
  runtime: "edge"
};

import { env } from "cloudflare:workers";
import { corsHeaders, getWecAirtableConfig, json, listAirtableRecords } from "../../lib/wec-packing.js";

export const OPTIONS = async () => new Response(null, { status: 204, headers: corsHeaders });

export const GET = async () => {
  const runtimeEnv = env || {};
  const airtable = getWecAirtableConfig(runtimeEnv);
  const response = {
    ok: airtable.ok,
    service: "wec-packing",
    endpoints: {
      state: "/test/wec-packing/state",
      action: "/test/wec-packing/action"
    },
    env: {
      hasAirtableToken: !!runtimeEnv.AIRTABLE_TOKEN,
      hasAirtableBaseId: !!(runtimeEnv.AIRTABLE_BASE_ID || runtimeEnv.AIRTABLE_BASE),
      metaTable: runtimeEnv.AIRTABLE_WEC_META_TABLE || "tbllJywsOstkqT5yZ"
    }
  };

  if (!airtable.ok) return json({ ...response, error: airtable.error }, 500, corsHeaders);

  try {
    const meta = await listAirtableRecords(airtable, airtable.metaTable);
    return json({
      ...response,
      meta: {
        count: meta.length,
        tables: meta.map((record) => record.fields?.table_name || record.fields?.meta).filter(Boolean)
      }
    }, 200, corsHeaders);
  } catch (error) {
    return json({
      ...response,
      ok: false,
      error: "wec_meta_load_failed",
      detail: error instanceof Error ? error.message : String(error)
    }, 502, corsHeaders);
  }
};
