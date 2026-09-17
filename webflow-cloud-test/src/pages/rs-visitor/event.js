export const config = {
  runtime: "edge"
};

import { env } from "cloudflare:workers";
import { VisitorEventError, recordVisitorEvent } from "../../lib/rs-visitor-event.js";

const corsHeaders = {
  "Access-Control-Allow-Origin": "https://ringstatus.com",
  "Access-Control-Allow-Methods": "POST,OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type"
};

export const OPTIONS = async () => new Response(null, {
  status: 204,
  headers: corsHeaders
});

export const POST = async ({ request }) => {
  try {
    const payload = await request.json().catch(() => ({}));
    const result = await recordVisitorEvent({ env, request, payload });
    return json(result, 201);
  } catch (error) {
    if (error instanceof VisitorEventError) {
      if (error.status >= 500) console.error("[rs-visitor] event failed", error.code);
      return json({ ok: false, error: error.code }, error.status);
    }
    console.error("[rs-visitor] unexpected event failure");
    return json({ ok: false, error: "visitor_event_failed" }, 502);
  }
};

function json(body, status) {
  return new Response(JSON.stringify(body), {
    status,
    headers: {
      ...corsHeaders,
      "Content-Type": "application/json; charset=utf-8",
      "Cache-Control": "no-store"
    }
  });
}
