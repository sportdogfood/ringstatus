import assert from "node:assert/strict";
import test from "node:test";

import {
  RecognitionActionError,
  runRecognitionAction
} from "../src/lib/rs-recognition-action.js";

const env = {
  AIRTABLE_TOKEN: "pat_test",
  AIRTABLE_BASE_ID: "app_wrong_barn_entry",
  AIRTABLE_RS_RECOGNITION_BASE_ID: "app_test",
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
  assert.match(personCreate.url, /app_test/);
  assert.doesNotMatch(personCreate.url, /app_wrong_barn_entry/);
  assert.equal("typecast" in personCreate.body, false);
  assert.equal(personCreate.body.records[0].fields.person_name, "Lainey");
  assert.equal(personCreate.body.records[0].fields.primary_phone_e164, "+16318752160");
  assert.equal(personCreate.body.records[0].fields.member_pin, "2160");
  assert.equal(result.member_pin, "2160");
  assert.equal(events[0].event_type, "new");
  assert.ok(events[0].detail.changed_fields.includes("member_pin"));
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
      pin: "4826",
      email: "lainey@example.com"
    })
  });

  assert.equal(result.person_uid, "63187");
  const personUpdate = calls.find((call) => call.method === "PATCH" && call.url.includes("rs_people_test"));
  assert.equal(personUpdate.body.records[0].fields.member_pin, "4826");
  assert.equal(events[0].event_type, "save");
  assert.ok(events[0].detail.changed_fields.includes("member_pin"));
});

test("phone_login normalizes a formatted phone and persists another device", async () => {
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
    payload: payload("phone_login", { sms: "(631) 875-2160" })
  });

  assert.equal(result.recognized, true);
  assert.equal(result.person_uid, "63187");
  assert.equal(events[0].event_type, "login");
  assert.equal(events[0].event_result, "matched");
});

test("phone_login accepts a four digit member PIN", async () => {
  const calls = [];
  const events = [];
  const fetchImpl = sequencedFetch([
    { body: { records: [{ id: "recPinPerson0001", fields: { person_uid: "63187", person_name: "Lainey", primary_phone_e164: "+16318752160", member_pin: "4826", status: "Active", access_level: "member" } }] } },
    { body: { records: [] } },
    { body: { records: [{ id: "recPinDevice0001" }] } }
  ], calls);

  const result = await runRecognitionAction({
    env,
    fetchImpl,
    request: new Request("https://ringstatus.webflow.io/test/rs-recognition/action"),
    recordSession: async (input) => events.push(input.payload),
    payload: payload("phone_login", { sms: "4826" })
  });

  assert.equal(result.recognized, true);
  assert.equal(result.person_uid, "63187");
  assert.match(calls[0].url, /member_pin/);
  assert.equal(events[0].matched_by, "pin");
});

test("phone_login refuses Guest access without creating a device", async () => {
  const calls = [];
  const events = [];
  const fetchImpl = sequencedFetch([
    { body: { records: [{ id: "recGuestPerson01", fields: { person_uid: "guest_001", status: "Active", access_level: "Guest" } }] } }
  ], calls);

  const result = await runRecognitionAction({
    env,
    fetchImpl,
    request: new Request("https://ringstatus.webflow.io/test/rs-recognition/action"),
    recordSession: async (input) => events.push(input.payload),
    payload: payload("phone_login", { sms: "(631) 875-2160" })
  });

  assert.equal(result.recognized, false);
  assert.equal(calls.length, 1);
  assert.equal(events[0].recognition_status, "rejected");
});

test("recovery always returns the same response and links a match only in the session event", async () => {
  const events = [];
  const fetchImpl = sequencedFetch([
    { body: { records: [{ id: "recRecoveryMatch1", fields: { person_uid: "63187", status: "Active", access_level: "member" } }] } }
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
