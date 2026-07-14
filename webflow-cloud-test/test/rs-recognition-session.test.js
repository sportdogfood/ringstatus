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

test("creates one queued session event without storing raw IP or user agent", async () => {
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
  assert.equal("typecast" in body, false);
  assert.equal(fields.event_type, "start");
  assert.equal(fields.event_result, "matched");
  assert.equal(fields.matched_by, "external");
  assert.equal(fields.recognition_status, "confirmed");
  assert.equal(fields.automation_status, "queued");
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
  assert.equal(fields.device_class, "mobile");
  assert.equal(fields.viewport_bucket, "small");
  assert.equal(fields.language, "en-US");
  assert.equal(fields.referrer_host, "google.com");
  assert.equal(fields.signal_version, 1);
  assert.match(fields.ip_hash, /^[a-f0-9]{64}$/);
  assert.match(fields.network_hash, /^[a-f0-9]{64}$/);
  assert.match(fields.user_agent_hash, /^[a-f0-9]{64}$/);
  assert.notEqual(fields.ip_hash, fields.network_hash);
  const detail = JSON.parse(fields.event_detail);
  assert.equal(detail.source, "recognition_test");
  assert.equal(detail.event_type_raw, "recognition");
  assert.equal(detail.event_result_raw, "success");
  assert.equal(detail.matched_by_raw, "device_token");
  assert.equal(detail.recognition_status_raw, "known_device");
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

test("maps every recognition event into the live Airtable select contract", async () => {
  const cases = [
    ["recognition", "not_matched", "none", "unknown_device", "start", "not_matched", "unknown", "rejected"],
    ["new", "success", "manual", "confirmed", "success", "matched", "manual", "confirmed"],
    ["save", "success", "manual", "confirmed", "update", "matched", "manual", "confirmed"],
    ["login", "not_matched", "phone", "rejected", "start", "not_matched", "manual", "rejected"],
    ["recovery", "matched", "manual", "pending", "other", "matched", "manual", "pending"],
    ["visit", "success", "device_token", "confirmed", "success", "matched", "external", "confirmed"],
    ["device_retired", "success", "device_token", "rejected", "update", "matched", "external", "rejected"]
  ];

  for (const [eventType, eventResult, matchedBy, recognitionStatus, storedType, storedResult, storedMatch, storedStatus] of cases) {
    let written;
    const fetchImpl = async (url, options = {}) => {
      if (!options.method || options.method === "GET") return Response.json({ records: [] });
      written = JSON.parse(options.body).records[0].fields;
      return Response.json({ records: [{ id: "recSession001" }] });
    };
    await recordRecognitionSession({
      env,
      fetchImpl,
      request: requestWithSignals(),
      payload: payload({
        session_event_uid: `event_${eventType}`,
        idempotency_key: `case:${eventType}`,
        event_type: eventType,
        event_result: eventResult,
        matched_by: matchedBy,
        recognition_status: recognitionStatus
      })
    });
    assert.deepEqual(
      [written.event_type, written.event_result, written.matched_by, written.recognition_status],
      [storedType, storedResult, storedMatch, storedStatus]
    );
  }
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
