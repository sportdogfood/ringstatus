import assert from "node:assert/strict";
import test from "node:test";

import {
  RecognitionSessionError,
  recordRecognitionSession
} from "../src/lib/rs-recognition-session.js";

const env = {
  AIRTABLE_TOKEN: "pat_test",
  AIRTABLE_BASE_ID: "app_test",
  AIRTABLE_RS_RECOGNITION_SESSIONS_TEST_TABLE: "rs_recognition_sessions_test",
  RS_RECOGNITION_SIGNAL_SECRET: "recognition-test-secret"
};

function requestWithSignals() {
  const request = new Request("https://ringstatus.webflow.io/test/rs-recognition/session", {
    method: "POST",
    headers: {
      "CF-Connecting-IP": "203.0.113.42",
      "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 18_0 like Mac OS X) AppleWebKit/605.1.15 Version/18.0 Mobile/15E148 Safari/604.1",
      "Accept-Language": "en-US,en;q=0.9"
    }
  });
  Object.defineProperty(request, "cf", {
    value: {
      country: "US",
      region: "New York",
      city: "Ocala",
      timezone: "America/New_York",
      asn: 64500,
      colo: "MIA"
    }
  });
  return request;
}

function payload(overrides = {}) {
  return {
    session_event_uid: "session_event_test_001",
    session_uid: "session_test_001",
    event_type: "recognition",
    event_result: "success",
    person_record_id: "recxMolAW8UhI3Hph",
    device_record_id: "rec0OtWNkYWs7iGgk",
    phone_alias_record_id: "recH9O5Ahxn5kqHWl",
    matched_by: "device_token",
    recognition_status: "known_device",
    idempotency_key: "recognition:session_event_test_001",
    client_timezone: "America/New_York",
    viewport_width: 390,
    page_path: "/",
    referrer: "https://google.com/search?q=ringstatus",
    detail: { source: "recognition_test" },
    ...overrides
  };
}

test("creates one pending session event without storing raw IP or user agent", async () => {
  const calls = [];
  const fetchImpl = async (url, options = {}) => {
    calls.push({ url: String(url), options });
    if (!options.method || options.method === "GET") {
      return Response.json({ records: [] });
    }
    return Response.json({ records: [{ id: "recSession001" }] }, { status: 200 });
  };

  const result = await recordRecognitionSession({
    env,
    fetchImpl,
    request: requestWithSignals(),
    payload: payload()
  });

  assert.deepEqual(result, {
    ok: true,
    duplicate: false,
    record_id: "recSession001",
    session_event_uid: "session_event_test_001",
    session_uid: "session_test_001"
  });
  assert.equal(calls.length, 2);
  assert.match(calls[0].url, /rs_recognition_sessions_test/);
  assert.match(calls[0].url, /filterByFormula=/);
  assert.equal(calls[1].options.method, "POST");

  const body = JSON.parse(calls[1].options.body);
  const fields = body.records[0].fields;
  assert.equal(fields.automation_status, "Pending");
  assert.equal(fields.automation_attempt_count, 0);
  assert.deepEqual(fields.person, ["recxMolAW8UhI3Hph"]);
  assert.deepEqual(fields.device, ["rec0OtWNkYWs7iGgk"]);
  assert.deepEqual(fields.phone_alias, ["recH9O5Ahxn5kqHWl"]);
  assert.equal(fields.country_code, "US");
  assert.equal(fields.region, "New York");
  assert.equal(fields.city, "Ocala");
  assert.equal(fields.asn, "64500");
  assert.equal(fields.edge_colo, "MIA");
  assert.equal(fields.browser_family, "Safari");
  assert.equal(fields.os_family, "iOS");
  assert.equal(fields.device_class, "Mobile");
  assert.equal(fields.viewport_bucket, "Mobile");
  assert.equal(fields.language, "en-US");
  assert.equal(fields.referrer_host, "google.com");
  assert.equal(fields.signal_version, 1);
  assert.match(fields.ip_hash, /^[a-f0-9]{64}$/);
  assert.match(fields.network_hash, /^[a-f0-9]{64}$/);
  assert.match(fields.user_agent_hash, /^[a-f0-9]{64}$/);
  assert.notEqual(fields.ip_hash, fields.network_hash);
  assert.equal(JSON.parse(fields.event_detail).source, "recognition_test");
  assert.doesNotMatch(JSON.stringify(body), /203\.0\.113\.42/);
  assert.doesNotMatch(JSON.stringify(body), /Mozilla\/5\.0/);
});

test("returns an existing event for a repeated idempotency key", async () => {
  const calls = [];
  const fetchImpl = async (url, options = {}) => {
    calls.push({ url: String(url), options });
    return Response.json({
      records: [{
        id: "recExisting",
        fields: {
          session_event_uid: "session_event_test_001",
          session_uid: "session_test_001"
        }
      }]
    });
  };

  const result = await recordRecognitionSession({
    env,
    fetchImpl,
    request: requestWithSignals(),
    payload: payload()
  });

  assert.deepEqual(result, {
    ok: true,
    duplicate: true,
    record_id: "recExisting",
    session_event_uid: "session_event_test_001",
    session_uid: "session_test_001"
  });
  assert.equal(calls.length, 1);
});

test("rejects incomplete events before calling Airtable", async () => {
  let called = false;

  await assert.rejects(
    recordRecognitionSession({
      env,
      fetchImpl: async () => {
        called = true;
        return Response.json({});
      },
      request: requestWithSignals(),
      payload: payload({ idempotency_key: "" })
    }),
    (error) => {
      assert.ok(error instanceof RecognitionSessionError);
      assert.equal(error.code, "missing_idempotency_key");
      assert.equal(error.status, 400);
      return true;
    }
  );

  assert.equal(called, false);
});

test("reports Airtable creation failures as upstream errors", async () => {
  let call = 0;
  const fetchImpl = async () => {
    call += 1;
    if (call === 1) return Response.json({ records: [] });
    return Response.json({ error: { type: "INVALID_REQUEST" } }, { status: 422 });
  };

  await assert.rejects(
    recordRecognitionSession({
      env,
      fetchImpl,
      request: requestWithSignals(),
      payload: payload()
    }),
    (error) => {
      assert.ok(error instanceof RecognitionSessionError);
      assert.equal(error.code, "session_event_create_failed");
      assert.equal(error.status, 502);
      return true;
    }
  );
});
