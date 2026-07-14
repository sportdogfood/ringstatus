export const config = { runtime: "edge" };

import { env } from "cloudflare:workers";
import { RecognitionActionError, runRecognitionAction } from "../../lib/rs-recognition-action.js";

const cors = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "POST,OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type,Authorization"
};

export const OPTIONS = async () => new Response(null, { status: 204, headers: cors });

export const POST = async ({ request }) => {
  try {
    const payload = await request.json().catch(() => ({}));
    return json(await runRecognitionAction({ env, request, payload }));
  } catch (error) {
    if (error instanceof RecognitionActionError) {
      if (error.status >= 500) console.error("[rs-recognition] action failed", error);
      return json({ ok: false, error: error.code }, error.status);
    }
    console.error("[rs-recognition] unexpected action failure", error);
    return json({ ok: false, error: "action_failed" }, 502);
  }
};

function json(body, status = 200) {
  return new Response(JSON.stringify(body), { status, headers: { ...cors, "Content-Type": "application/json; charset=utf-8" } });
}
