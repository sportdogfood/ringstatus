import assert from "node:assert/strict";
import test from "node:test";

import {
  ALLOWED_VISITOR_PATHS,
  VisitorEventError,
  sha256Hex,
  recordVisitorEvent
} from "../src/lib/rs-visitor-event.js";

const env = {
  AIRTABLE_TOKEN: "pat_test",
  AIRTABLE_BASE_ID: "app_wrong_base",
  AIRTABLE_RS_RECOGNITION_BASE_ID: "apptdhhNzduxm5gjn",
  AIRTABLE_RS_VISITOR_EVENTS_TABLE: "tbldR3ymyJxYRHtdD"
};

function requestWithSignals() {
  const request = new Request("https://ringstatus.com/test/rs-visitor/event", {
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

function payload(pagePath = "/") {
  return {
    event_uid: `visitor_event_${pagePath.replace(/\W/g, "") || "home"}`,
    page_path: pagePath,
    referrer: "https://google.com/search?q=ringstatus",
    client_timezone: "America/New_York",
    viewport_width: 390,
    utm_source: "newsletter",
    utm_medium: "email",
    utm_campaign: "fall-show"
  };
}

test("accepts exactly the six approved page paths", async () => {
  assert.deepEqual([...ALLOWED_VISITOR_PATHS].sort(), ["/", "/lainey", "/lc", "/ld", "/lh", "/lm"]);

  for (const path of ALLOWED_VISITOR_PATHS) {
    let written;
    const result = await recordVisitorEvent({
      env,
      request: requestWithSignals(),
      payload: payload(path),
      fetchImpl: async (url, options) => {
        written = { url: String(url), body: JSON.parse(options.body) };
        return Response.json({ records: [{ id: "recVisitorEvent" }] });
      }
    });
    assert.equal(result.ok, true);
    assert.equal(written.body.records[0].fields.page_path, path);
  }
});

test("rejects every unsupported page path before Airtable is called", async () => {
  for (const path of ["/other", "/lainey/", "/LM", "https://ringstatus.com/"]) {
    let called = false;
    await assert.rejects(
      recordVisitorEvent({
        env,
        request: requestWithSignals(),
        payload: payload(path),
        fetchImpl: async () => {
          called = true;
          return Response.json({});
        }
      }),
      (error) => error instanceof VisitorEventError && error.code === "unsupported_page_path" && error.status === 400
    );
    assert.equal(called, false);
  }
});

test("writes privacy-safe server-derived signals to the configured visitor table", async () => {
  let call;
  const result = await recordVisitorEvent({
    env,
    request: requestWithSignals(),
    payload: payload("/lainey"),
    fetchImpl: async (url, options) => {
      call = { url: String(url), options };
      return Response.json({ records: [{ id: "recVisitorEvent" }] }, { status: 200 });
    }
  });

  assert.equal(result.record_id, "recVisitorEvent");
  assert.match(call.url, /apptdhhNzduxm5gjn/);
  assert.match(call.url, /tbldR3ymyJxYRHtdD/);
  assert.doesNotMatch(call.url, /app_wrong_base/);

  const body = JSON.parse(call.options.body);
  const fields = body.records[0].fields;
  assert.equal(fields.event_uid, "visitor_event_lainey");
  assert.equal(fields.page_path, "/lainey");
  assert.equal(fields.referrer_host, "google.com");
  assert.equal(fields.country_code, "US");
  assert.equal(fields.region, "New York");
  assert.equal(fields.city, "Ocala");
  assert.equal(fields.timezone, "America/New_York");
  assert.equal(fields.asn, "64500");
  assert.equal(fields.edge_colo, "MIA");
  assert.equal(fields.browser_family, "Safari");
  assert.equal(fields.os_family, "iOS");
  assert.equal(fields.device_class, "mobile");
  assert.equal(fields.language, "en-US");
  assert.equal(fields.client_timezone, "America/New_York");
  assert.equal(fields.viewport_bucket, "small");
  assert.equal(fields.utm_source, "newsletter");
  assert.equal(fields.signal_version, 1);
  assert.match(fields.event_at, /^\d{4}-\d{2}-\d{2}T/);
  assert.match(fields.ip_hash, /^[a-f0-9]{64}$/);
  assert.match(fields.network_hash, /^[a-f0-9]{64}$/);
  assert.match(fields.user_agent_hash, /^[a-f0-9]{64}$/);
  assert.match(fields.environment_hash, /^[a-f0-9]{64}$/);
  assert.doesNotMatch(JSON.stringify(body), /203\.0\.113\.42/);
  assert.doesNotMatch(JSON.stringify(body), /Mozilla\/5\.0/);
  for (const forbidden of ["visitor_uid", "session_uid", "returning_status", "matched_by"]) {
    assert.equal(forbidden in fields, false, `${forbidden} must not be written`);
  }
});

test("anonymous SHA-256 hashes are deterministic without a secret", async () => {
  const first = await sha256Hex("ip:203.0.113.42");
  const second = await sha256Hex("ip:203.0.113.42");
  const otherValue = await sha256Hex("ip:203.0.113.43");
  assert.equal(first, second);
  assert.notEqual(first, otherValue);
  assert.match(first, /^[a-f0-9]{64}$/);
});

test("uses the first valid forwarded address when CF-Connecting-IP is unavailable", async () => {
  const request = new Request("https://ringstatus.com/test/rs-visitor/event", {
    method: "POST",
    headers: {
      "X-Forwarded-For": "203.0.113.42, 198.51.100.10",
      "User-Agent": "Mozilla/5.0",
      "Accept-Language": "en-US"
    }
  });
  let fields;

  await recordVisitorEvent({
    env,
    request,
    payload: payload("/lc"),
    fetchImpl: async (_url, options) => {
      fields = JSON.parse(options.body).records[0].fields;
      return Response.json({ records: [{ id: "recVisitorEvent" }] });
    }
  });

  assert.equal(fields.ip_hash, await sha256Hex("ip:203.0.113.42"));
  assert.equal(fields.network_hash, await sha256Hex("network:203.0.113.0/24"));
  assert.doesNotMatch(JSON.stringify(fields), /203\.0\.113\.42|198\.51\.100\.10/);
});

test("reports Airtable failures without exposing upstream details or secrets", async () => {
  await assert.rejects(
    recordVisitorEvent({
      env,
      request: requestWithSignals(),
      payload: payload(),
      fetchImpl: async () => Response.json({ error: { message: "secret upstream detail" } }, { status: 422 })
    }),
    (error) => {
      assert.ok(error instanceof VisitorEventError);
      assert.equal(error.code, "visitor_event_create_failed");
      assert.equal(error.status, 502);
      assert.doesNotMatch(error.message, /secret upstream detail|pat_test/);
      return true;
    }
  );
});
