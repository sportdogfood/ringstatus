export const config = {
  runtime: "edge"
};

import { env } from "cloudflare:workers";
import {
  RecognitionIdentityError,
  performRecognitionAction
} from "../../lib/rs-recognition-identity.js";

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "POST,OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type,Authorization"
};

export const OPTIONS = async () => new Response(null, {
  status: 204,
  headers: corsHeaders
});

export const POST = async ({ request }) => {
  try {
    const payload = await request.json().catch(() => ({}));
    const result = await performRecognitionAction({ env, request, payload });
    return json(result);
  } catch (error) {
    if (error instanceof RecognitionIdentityError) {
      if (error.status >= 500) console.error("[rs-recognition] identity action failed", error);
      return json({ ok: false, error: error.code }, error.status);
    }
    console.error("[rs-recognition] unexpected identity action failure", error);
    return json({ ok: false, error: "identity_action_failed" }, 502);
  }
};

function json(body, status = 200) {
  return new Response(JSON.stringify(body, null, 2), {
    status,
    headers: {
      ...corsHeaders,
      "Content-Type": "application/json; charset=utf-8"
    }
  });
}
