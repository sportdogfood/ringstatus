export const config = {
  runtime: "edge"
};

import { env } from "cloudflare:workers";

export const GET = async () => {
  return json({
    ok: true,
    service: "webflow-cloud-test",
    enrichmentEndpoint: "/test/lp-history/enrichment",
    env: {
      hasAirtableToken: !!env.AIRTABLE_TOKEN,
      hasAirtableBaseId: !!(env.AIRTABLE_BASE_ID || env.AIRTABLE_BASE),
      table: env.AIRTABLE_TABLE_LP || env.AIRTABLE_TABLE || ""
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
