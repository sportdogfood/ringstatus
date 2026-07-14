import assert from "node:assert/strict";
import test from "node:test";

import {
  RecognitionActionError,
  runRecognitionAction
} from "../src/lib/rs-recognition-action.js";

const env = {
  AIRTABLE_TOKEN: "pat_test",
  AIRTABLE_BASE_ID: "app_test",
  AIRTABLE_RS_PEOPLE_TEST_TABLE: "rs_people_test",
  AIRTABLE_RS_DEVICES_TEST_TABLE: "rs_devices_test",
  AIRTABLE_RS_PHONE_ALIASES_TEST_TABLE: "rs_phone_aliases_test"
};

function payload(action, values = {}) {
  return {
    action,
    session_uid: "session_action_001",
    session_event_uid: `event_${action}_001`,
    device_token: "device_token_001",
    ...values
  };
}

function sequencedFetch(items, calls) {
  return async (url, options = {}) => {
    calls.push({
      url: String(url),
      method: options.method || "GET",
      body: options.body ? JSON.parse(options.body) : null
    });
    const item = items.shift();
    if (!item) throw new Error(`Unexpected request: ${options.method || "GET"} ${url}`);
    return Response.json(item.body, { status: item.status || 200 });
  };
}

test("create_profile creates one person, phone alias, device, and session event", async () => {
  const calls = [];
  const events = [];
  const fetchImpl = sequencedFetch([
    { body: { records: [] } },
    { body: { records: [] } },
    { body: { records: [{ id: "recPersonCreate01" }] } },
    { body: { records: [] } },
    { body: { records: [{ id: "recAliasCreate001" }] } },
    { body: { records: [] } },
    { body: { records: [{ id: "recDeviceCreate01" }] } }
  ], calls);

  const result = await runRecognitionAction({
    env,
    fetchImpl,
    request: new Request("https://ringstatus.webflow.io/test/rs-recognition/action"),
    recordSession: async (input) => events.push(input.payload),
    payload: payload("create_profile", {
      user: "Lainey",
      first: "Lainey",
      last: "Posa",
      sms: "6318752160",
      email: "lainey@example.com"
    })
  });

  assert.equal(result.ok, true);
  assert.equal(result.person_record_id, "recPersonCreate01");
  assert.equal(result.device_record_id, "recDeviceCreate01");
  const personCreate = calls.find((call) => call.method === "POST" && call.url.includes("rs_people_test"));
  assert.equal(personCreate.body.records[0].fields.person_name, "Lainey");
  assert.equal(personCreate.body.records[0].fields.primary_phone_e164, "+16318752160");
  assert.equal(events[0].event_type, "new");
  assert.equal(events[0].person_record_id, "recPersonCreate01");
});

test("update_profile updates the person and attaches the current device", async () => {
  const calls = [];
  const events = [];
  const fetchImpl = sequencedFetch([
    { body: { records: [] } },
    { body: { records: [] } },
    { body: { records: [{ id: "recPersonExisting" }] } },
    { body: { records: [] } },
    { body: { records: [{ id: "recAliasUpdate001" }] } },
    { body: { records: [] } },
    { body: { records: [{ id: "recDeviceUpdate01" }] } }
  ], calls);

  const result = await runRecognitionAction({
    env,
    fetchImpl,
    request: new Request("https://ringstatus.webflow.io/test/rs-recognition/action"),
    recordSession: async (input) => events.push(input.payload),
    payload: payload("update_profile", {
      person_record_id: "recPersonExisting",
      person_uid: "63187",
      user: "Lainey",
      first: "Lainey",
      last: "Posa",
      sms: "+16318752160",
      email: "lainey@example.com"
    })
  });

  assert.equal(result.person_uid, "63187");
  assert.ok(calls.some((call) => call.method === "PATCH" && call.url.includes("rs_people_test")));
  assert.equal(events[0].event_type, "save");
});

test("phone_login matches a person and persists another device", async () => {
  const events = [];
  const fetchImpl = sequencedFetch([
    { body: { records: [{ id: "recPhonePerson001", fields: { person_uid: "63187", person_name: "Lainey", primary_phone_e164: "+16318752160", status: "Active", access_level: "member" } }] } },
    { body: { records: [] } },
    { body: { records: [{ id: "recPhoneDevice001" }] } }
  ], []);

  const result = await runRecognitionAction({
    env,
    fetchImpl,
    request: new Request("https://ringstatus.webflow.io/test/rs-recognition/action"),
    recordSession: async (input) => events.push(input.payload),
    payload: payload("phone_login", { sms: "6318752160" })
  });

  assert.equal(result.recognized, true);
  assert.equal(result.person_uid, "63187");
  assert.equal(events[0].event_type, "login");
  assert.equal(events[0].event_result, "matched");
});

test("recovery always returns the same response and links a match only in the session event", async () => {
  const events = [];
  const fetchImpl = sequencedFetch([
    { body: { records: [{ id: "recRecoveryMatch1", fields: { person_uid: "63187" } }] } }
  ], []);

  const result = await runRecognitionAction({
    env,
    fetchImpl,
    request: new Request("https://ringstatus.webflow.io/test/rs-recognition/action"),
    recordSession: async (input) => events.push(input.payload),
    payload: payload("recovery", { first: "Lainey", last: "Posa", email: "submitted@example.com" })
  });

  assert.deepEqual(result, { ok: true, accepted: true, return_to: "/" });
  assert.equal(events[0].person_record_id, "recRecoveryMatch1");
  assert.deepEqual(events[0].detail, { automation_action: "send_member_link", return_to: "/" });
  assert.doesNotMatch(JSON.stringify(events[0]), /submitted@example\.com/);
});

test("retire_device retires only the supplied device token", async () => {
  const calls = [];
  const events = [];
  const fetchImpl = sequencedFetch([
    { body: { records: [{ id: "recDeviceRetire01", fields: { device_uid: "device_001" } }] } },
    { body: { records: [{ id: "recDeviceRetire01" }] } }
  ], calls);

  const result = await runRecognitionAction({
    env,
    fetchImpl,
    request: new Request("https://ringstatus.webflow.io/test/rs-recognition/action"),
    recordSession: async (input) => events.push(input.payload),
    payload: payload("retire_device")
  });

  assert.equal(result.retired, true);
  assert.equal(calls[1].body.records[0].fields.status, "Retired");
  assert.equal(events[0].event_type, "device_retired");
});

test("profile actions require user and sms before Airtable is called", async () => {
  let called = false;
  await assert.rejects(
    runRecognitionAction({
      env,
      fetchImpl: async () => { called = true; },
      recordSession: async () => {},
      request: new Request("https://ringstatus.webflow.io/test/rs-recognition/action"),
      payload: payload("create_profile", { user: "", sms: "" })
    }),
    (error) => error instanceof RecognitionActionError && error.code === "missing_user"
  );
  assert.equal(called, false);
});
