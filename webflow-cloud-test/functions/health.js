const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type,Authorization"
};

export async function onRequest({ env }) {
  return json({
    ok: true,
    service: "webflow-cloud-test",
    enrichmentEndpoint: "/lp-history/enrichment",
    mode: env.AIRTABLE_WEBHOOK_URL ? "forward" : "local-echo"
  });
}

function json(data, status = 200) {
  return new Response(JSON.stringify(data, null, 2) + "\n", {
    status,
    headers: {
      ...corsHeaders,
      "Content-Type": "application/json; charset=utf-8"
    }
  });
}

