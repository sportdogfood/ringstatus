import assert from "node:assert/strict";
import test from "node:test";

import { env } from "cloudflare:workers";
import { OPTIONS, POST } from "../src/pages/rs-recognition/session.js";

Object.assign(env, {
  AIRTABLE_TOKEN: "pat_test",
  AIRTABLE_BASE_ID: "app_test",
  AIRTABLE_RS_RECOGNITION_SESSIONS_TEST_TABLE: "rs_recognition_sessions_test",
  RS_RECOGNITION_SIGNAL_SECRET: "recognition-test-secret"
});

function payload(overrides = {}) {
  return {
    session_event_uid: "session_event_route_001",
    session_uid: "session_route_001",
    event_type: "recognition",
    event_result: "success",
    idempotency_key: "recognition:session_event_route_001",
    ...overrides
  };
}

function request(body = payload()) {
  return new Request("https://ringstatus.webflow.io/test/rs-recognition/session", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Origin: "https://ringstatus.com"
    },
    body: JSON.stringify(body)
  });
}

test("OPTIONS allows the browser POST request", async () => {
  const response = await OPTIONS();
  assert.equal(response.status, 204);
  assert.match(response.headers.get("Access-Control-Allow-Methods"), /POST/);
});

test("POST creates a session event and returns 201", async () => {
  let call = 0;
  globalThis.fetch = async (_url, options = {}) => {
    call += 1;
    if (!options.method || options.method === "GET") return Response.json({ records: [] });
    return Response.json({ records: [{ id: "recSessionRoute01" }] });
  };

  const response = await POST({ request: request() });
  const body = await response.json();

  assert.equal(response.status, 201);
  assert.equal(body.ok, true);
  assert.equal(body.duplicate, false);
  assert.equal(body.record_id, "recSessionRoute01");
  assert.equal(call, 2);
});

test("POST returns 200 for an idempotent duplicate", async () => {
  globalThis.fetch = async () => Response.json({
    records: [{
      id: "recExistingRoute",
      fields: {
        session_event_uid: "session_event_route_001",
        session_uid: "session_route_001"
      }
    }]
  });

  const response = await POST({ request: request() });
  const body = await response.json();

  assert.equal(response.status, 200);
  assert.equal(body.ok, true);
  assert.equal(body.duplicate, true);
  assert.equal(body.record_id, "recExistingRoute");
});

test("POST returns 400 for an incomplete event", async () => {
  globalThis.fetch = async () => {
    throw new Error("Airtable must not be called");
  };

  const response = await POST({ request: request(payload({ idempotency_key: "" })) });
  const body = await response.json();

  assert.equal(response.status, 400);
  assert.equal(body.ok, false);
  assert.equal(body.error, "missing_idempotency_key");
});

test("POST returns 502 when Airtable rejects the event", async () => {
  let call = 0;
  const logged = [];
  const originalConsoleError = console.error;
  console.error = (...args) => logged.push(args);
  globalThis.fetch = async () => {
    call += 1;
    if (call === 1) return Response.json({ records: [] });
    return Response.json({ error: { type: "INVALID_REQUEST" } }, { status: 422 });
  };

  try {
    const response = await POST({ request: request() });
    const body = await response.json();

    assert.equal(response.status, 502);
    assert.equal(body.ok, false);
    assert.equal(body.error, "session_event_create_failed");
    assert.equal(logged.length, 1);
  } finally {
    console.error = originalConsoleError;
  }
});
