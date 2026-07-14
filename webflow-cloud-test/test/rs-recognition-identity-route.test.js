import assert from "node:assert/strict";
import test from "node:test";

import { env } from "cloudflare:workers";
import { OPTIONS, POST } from "../src/pages/rs-recognition/identity.js";

Object.assign(env, {
  AIRTABLE_TOKEN: "pat_test",
  AIRTABLE_BASE_ID: "app_test",
  AIRTABLE_RS_PEOPLE_TEST_TABLE: "rs_people_test",
  AIRTABLE_RS_DEVICES_TEST_TABLE: "rs_devices_test",
  AIRTABLE_RS_PHONE_ALIASES_TEST_TABLE: "rs_phone_aliases_test",
  AIRTABLE_RS_RECOGNITION_SESSIONS_TEST_TABLE: "rs_recognition_sessions_test"
});

test("identity route supports browser preflight", async () => {
  const response = await OPTIONS();
  assert.equal(response.status, 204);
  assert.match(response.headers.get("Access-Control-Allow-Methods"), /POST/);
});

test("identity route rejects an unknown action", async () => {
  const response = await POST({
    request: new Request("https://ringstatus.webflow.io/test/rs-recognition/identity", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        action: "invented_action",
        session_uid: "session_route_001",
        session_event_uid: "event_route_001"
      })
    })
  });
  const body = await response.json();

  assert.equal(response.status, 400);
  assert.deepEqual(body, { ok: false, error: "unsupported_action" });
});
