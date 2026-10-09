import { installControlDatabase } from '../test-support/recognize-control-db.mjs';
import assert from "node:assert/strict";
import test from "node:test";

import { env } from "cloudflare:workers";
import { OPTIONS, POST } from "../src/pages/rs-recognition/action.js";

Object.assign(env, {
  AIRTABLE_TOKEN: "pat_test",
  AIRTABLE_BASE_ID: "app_wrong_barn_entry",
  AIRTABLE_RS_RECOGNITION_BASE_ID: "app9kOZdIaGyKk5uG"
});

test("action route supports browser preflight", async () => {
  const response = await OPTIONS();
  assert.equal(response.status, 204);
  assert.match(response.headers.get("Access-Control-Allow-Methods"), /POST/);
  assert.match(response.headers.get("Access-Control-Allow-Headers"), /X-RS-Audit-Outcome/);
});

for (const native of [false, true]) {
  test(`audit failure preserves ${native ? "explicit native pending status" : "existing caller error behavior"}`, async () => {
    const savedFetch = globalThis.fetch;
    let writes = 0;
    globalThis.fetch = async (url, options = {}) => {
      if (options.method === "PATCH") { writes++; return Response.json({ records: [{ id: "recSynthetic00001" }] }); }
      if (options.method === "POST") return Response.json({ error: "synthetic-audit-failure" }, { status: 502 });
      return Response.json({ records: String(url).includes("tblfkRSJAEMzuzApR") ? [{ id: "recSynthetic00001" }] : [] });
    };
    try {
      const response = await POST({ request: new Request("https://example.invalid/rs-recognition/action", {
        method: "POST", headers: { "Content-Type": "application/json", ...(native ? { "X-RS-Audit-Outcome": "report" } : {}) },
        body: JSON.stringify({ action: "retire_device", device_token: "synthetic-device", session_uid: "synthetic-session", session_event_uid: "synthetic-event" })
      }) });
      const body = await response.json();
      assert.equal(writes, 1);
      assert.equal(response.status, native ? 200 : 502);
      if (native) { assert.equal(body.retired, true); assert.equal(body.audit_status, "pending"); }
      else assert.equal(body.error, "action_failed");
    } finally { globalThis.fetch = savedFetch; }
  });
}

test("action route rejects unsupported actions", async () => {
  const response = await POST({ request: new Request("https://ringstatus.webflow.io/test/rs-recognition/action", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ action: "other", session_uid: "session_001", session_event_uid: "event_001" })
  }) });
  assert.equal(response.status, 400);
  assert.deepEqual(await response.json(), { ok: false, error: "unsupported_action" });
});

installControlDatabase(env);
