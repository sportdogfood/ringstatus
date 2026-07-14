import assert from "node:assert/strict";
import test from "node:test";

import { env } from "cloudflare:workers";
import { OPTIONS, POST } from "../src/pages/rs-recognition/action.js";

Object.assign(env, {
  AIRTABLE_TOKEN: "pat_test",
  AIRTABLE_BASE_ID: "app_test"
});

test("action route supports browser preflight", async () => {
  const response = await OPTIONS();
  assert.equal(response.status, 204);
  assert.match(response.headers.get("Access-Control-Allow-Methods"), /POST/);
});

test("action route rejects unsupported actions", async () => {
  const response = await POST({ request: new Request("https://ringstatus.webflow.io/test/rs-recognition/action", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ action: "other", session_uid: "session_001", session_event_uid: "event_001" })
  }) });
  assert.equal(response.status, 400);
  assert.deepEqual(await response.json(), { ok: false, error: "unsupported_action" });
});
