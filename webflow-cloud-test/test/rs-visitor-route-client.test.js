import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import vm from "node:vm";

import { env } from "cloudflare:workers";
import { GET } from "../src/pages/rs-visitor/client.js.ts";
import { POST } from "../src/pages/rs-visitor/event.js";

Object.assign(env, {
  AIRTABLE_TOKEN: "pat_test",
  AIRTABLE_RS_RECOGNITION_BASE_ID: "apptdhhNzduxm5gjn",
  AIRTABLE_RS_VISITOR_EVENTS_TABLE: "tbldR3ymyJxYRHtdD",
  RS_RECOGNITION_SIGNAL_SECRET: "visitor-test-secret"
});

test("hosted client route serves JavaScript", async () => {
  const response = GET();
  assert.equal(response.status, 200);
  assert.match(response.headers.get("content-type"), /^text\/javascript/);
  assert.match(await response.text(), /rs-visitor\/event/);
});

test("client attempts exactly one POST per document execution and uses no persistent browser state", async () => {
  const source = readFileSync(new URL("../src/assets/rs-visitor/client.js", import.meta.url), "utf8");
  assert.doesNotMatch(source, /cookie|localStorage|sessionStorage/);

  const calls = [];
  const context = {
    window: {
      location: { pathname: "/lm", search: "?utm_source=test&utm_medium=email&utm_campaign=launch" },
      innerWidth: 1280,
      __rsVisitorEventSent: undefined
    },
    document: { referrer: "https://example.com/path" },
    navigator: { language: "en-US" },
    Intl,
    URLSearchParams,
    crypto,
    fetch: async (...args) => {
      calls.push(args);
      return new Response(null, { status: 201 });
    }
  };
  context.globalThis = context;

  vm.runInNewContext(source, context);
  await new Promise((resolve) => setTimeout(resolve, 0));
  vm.runInNewContext(source, context);
  await new Promise((resolve) => setTimeout(resolve, 0));

  assert.equal(calls.length, 1);
  assert.equal(calls[0][0], "https://ringstatus.com/test/rs-visitor/event");
  assert.equal(calls[0][1].method, "POST");
  assert.equal(calls[0][1].keepalive, true);
  const body = JSON.parse(calls[0][1].body);
  assert.deepEqual(Object.keys(body).sort(), [
    "client_timezone", "event_uid", "page_path", "referrer", "utm_campaign", "utm_medium", "utm_source", "viewport_width"
  ]);
  assert.equal(body.page_path, "/lm");
});

test("POST returns a controlled error when Airtable rejects the event", async () => {
  const originalFetch = globalThis.fetch;
  const originalConsoleError = console.error;
  console.error = () => {};
  globalThis.fetch = async () => Response.json({ error: { message: "private upstream failure" } }, { status: 422 });
  try {
    const request = new Request("https://ringstatus.com/test/rs-visitor/event", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ event_uid: "visitor_route_001", page_path: "/" })
    });
    const response = await POST({ request });
    const body = await response.json();
    assert.equal(response.status, 502);
    assert.deepEqual(body, { ok: false, error: "visitor_event_create_failed" });
    assert.doesNotMatch(JSON.stringify(body), /private upstream failure|pat_test|visitor-test-secret/);
  } finally {
    globalThis.fetch = originalFetch;
    console.error = originalConsoleError;
  }
});
