import assert from "node:assert/strict";
import test from "node:test";

import {
  RecognitionIdentityError,
  performRecognitionAction
} from "../src/lib/rs-recognition-identity.js";

const env = {
  AIRTABLE_TOKEN: "pat_test",
  AIRTABLE_BASE_ID: "app_test",
  AIRTABLE_RS_PEOPLE_TEST_TABLE: "rs_people_test",
  AIRTABLE_RS_DEVICES_TEST_TABLE: "rs_devices_test",
  AIRTABLE_RS_PHONE_ALIASES_TEST_TABLE: "rs_phone_aliases_test"
};

function basePayload(action, overrides = {}) {
  return {
    action,
    session_uid: "session_identity_001",
    session_event_uid: `event_${action}_001`,
    device_token: "device_token_browser_001",
    ...overrides
  };
}

function request() {
  return new Request("https://ringstatus.webflow.io/test/rs-recognition/identity", {
    method: "POST",
    headers: { "Content-Type": "application/json" }
  });
}

function response(body, status = 200) {
  return Response.json(body, { status });
}

test("create_profile creates the person, phone alias, and device before logging new", async () => {
  const calls = [];
  const events = [];
  const fetchImpl = async (url, options = {}) => {
    const call = { url: String(url), method: options.method || "GET", body: options.body ? JSON.parse(options.body) : null };
    calls.push(call);
    if (call.method === "GET") return response({ records: [] });
    if (call.url.includes("rs_people_test")) return response({ records: [{ id: "recPersonCreate01", fields: call.body.records[0].fields }] });
    if (call.url.includes("rs_phone_aliases_test")) return response({ records: [{ id: "recAliasCreate001", fields: call.body.records[0].fields }] });
    if (call.url.includes("rs_devices_test")) return response({ records: [{ id: "recDeviceCreate01", fields: call.body.records[0].fields }] });
    throw new Error(`Unexpected call ${call.method} ${call.url}`);
  };

  const result = await performRecognitionAction({
    env,
    request: request(),
    fetchImpl,
    recordSession: async (input) => events.push(input.payload),
    payload: basePayload("create_profile", {
      user: "Lainey",
      first: "Lainey",
      last: "Posa",
      sms: "(631) 875-2160",
      email: "lainey@example.com"
    })
  });

  assert.equal(result.ok, true);
  assert.equal(result.recognized, true);
  assert.match(result.person_uid, /^person_/);
  assert.equal(result.person_record_id, "recPersonCreate01");
  assert.equal(result.phone_alias_record_id, "recAliasCreate001");
  assert.equal(result.device_record_id, "recDeviceCreate01");
  const personCreate = calls.find((call) => call.method === "POST" && call.url.includes("rs_people_test"));
  assert.deepEqual(personCreate.body.records[0].fields, {
    person_uid: result.person_uid,
    person_name: "Lainey",
    first_name: "Lainey",
    last_name: "Posa",
    primary_phone_e164: "+16318752160",
    email: "lainey@example.com",
    status: "Active",
    access_level: "member"
  });
  assert.equal(events.length, 1);
  assert.equal(events[0].event_type, "new");
  assert.equal(events[0].event_result, "success");
  assert.equal(events[0].person_record_id, "recPersonCreate01");
  assert.deepEqual(events[0].detail.changed_fields, ["person_name", "first_name", "last_name", "primary_phone_e164", "email"]);
});

test("update_profile updates the existing person and attaches another device", async () => {
  const calls = [];
  const events = [];
  const fetchImpl = async (url, options = {}) => {
    const call = { url: String(url), method: options.method || "GET", body: options.body ? JSON.parse(options.body) : null };
    calls.push(call);
    if (call.method === "PATCH" && call.url.includes("rs_people_test")) return response({ records: [{ id: "recPersonExisting", fields: call.body.records[0].fields }] });
    if (call.method === "GET") return response({ records: [] });
    if (call.url.includes("rs_phone_aliases_test")) return response({ records: [{ id: "recAliasUpdate001", fields: call.body.records[0].fields }] });
    if (call.url.includes("rs_devices_test")) return response({ records: [{ id: "recDeviceUpdate01", fields: call.body.records[0].fields }] });
    throw new Error(`Unexpected call ${call.method} ${call.url}`);
  };

  const result = await performRecognitionAction({
    env,
    request: request(),
    fetchImpl,
    recordSession: async (input) => events.push(input.payload),
    payload: basePayload("update_profile", {
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
  assert.equal(result.device_record_id, "recDeviceUpdate01");
  assert.equal(events[0].event_type, "save");
  assert.equal(events[0].event_result, "success");
  assert.ok(calls.some((call) => call.method === "PATCH" && call.url.includes("rs_people_test")));
});

test("update_profile rejects a phone owned by another person before changing the profile", async () => {
  const calls = [];
  const fetchImpl = async (url, options = {}) => {
    calls.push({ url: String(url), method: options.method || "GET" });
    return response({ records: [{ id: "recOtherPerson01", fields: { status: "Active" } }] });
  };

  await assert.rejects(
    performRecognitionAction({
      env,
      request: request(),
      fetchImpl,
      recordSession: async () => {},
      payload: basePayload("update_profile", {
        person_record_id: "recPersonExisting",
        person_uid: "63187",
        user: "Lainey",
        sms: "+16318752160"
      })
    }),
    (error) => error instanceof RecognitionIdentityError && error.code === "phone_already_registered"
  );

  assert.equal(calls.some((call) => call.method === "PATCH"), false);
});

test("phone_login recognizes a primary phone and persists the current device", async () => {
  const events = [];
  const fetchImpl = async (url, options = {}) => {
    const text = String(url);
    if (text.includes("rs_people_test") && (!options.method || options.method === "GET")) {
      return response({ records: [{
        id: "recPhonePerson001",
        fields: {
          person_uid: "63187",
          person_name: "Lainey",
          first_name: "Lainey",
          last_name: "Posa",
          primary_phone_e164: "+16318752160",
          email: "lainey@example.com",
          status: "Active",
          access_level: "member"
        }
      }] });
    }
    if (text.includes("rs_devices_test") && (!options.method || options.method === "GET")) return response({ records: [] });
    if (text.includes("rs_devices_test") && options.method === "POST") return response({ records: [{ id: "recPhoneDevice001" }] });
    throw new Error(`Unexpected call ${options.method || "GET"} ${text}`);
  };

  const result = await performRecognitionAction({
    env,
    request: request(),
    fetchImpl,
    recordSession: async (input) => events.push(input.payload),
    payload: basePayload("phone_login", { sms: "6318752160" })
  });

  assert.equal(result.recognized, true);
  assert.equal(result.person_uid, "63187");
  assert.equal(result.device_record_id, "recPhoneDevice001");
  assert.equal(events[0].event_type, "login");
  assert.equal(events[0].event_result, "matched");
  assert.equal(events[0].matched_by, "phone");
});

test("phone_login returns a generic miss and records not_matched", async () => {
  const events = [];
  const fetchImpl = async () => response({ records: [] });

  const result = await performRecognitionAction({
    env,
    request: request(),
    fetchImpl,
    recordSession: async (input) => events.push(input.payload),
    payload: basePayload("phone_login", { sms: "5555555555" })
  });

  assert.deepEqual(result, { ok: true, recognized: false });
  assert.equal(events[0].event_type, "login");
  assert.equal(events[0].event_result, "not_matched");
  assert.equal(events[0].person_record_id, "");
});

test("recovery returns the same accepted response while linking a matched person for automation", async () => {
  const events = [];
  const fetchImpl = async () => response({ records: [{
    id: "recRecoveryMatch1",
    fields: { person_uid: "63187", person_name: "Lainey", email: "on-file@example.com" }
  }] });

  const result = await performRecognitionAction({
    env,
    request: request(),
    fetchImpl,
    recordSession: async (input) => events.push(input.payload),
    payload: basePayload("recovery", {
      first: "Lainey",
      last: "Posa",
      email: "submitted@example.com"
    })
  });

  assert.deepEqual(result, { ok: true, accepted: true, return_to: "/" });
  assert.equal(events[0].event_type, "recovery");
  assert.equal(events[0].event_result, "matched");
  assert.equal(events[0].person_record_id, "recRecoveryMatch1");
  assert.deepEqual(events[0].detail, { automation_action: "send_member_link", return_to: "/" });
  assert.doesNotMatch(JSON.stringify(events[0]), /submitted@example\.com/);
});

test("retire_device retires only the supplied token and logs device_retired", async () => {
  const events = [];
  let patchedFields;
  const fetchImpl = async (_url, options = {}) => {
    if (!options.method || options.method === "GET") {
      return response({ records: [{ id: "recDeviceRetire01", fields: { device_uid: "device_existing_001" } }] });
    }
    patchedFields = JSON.parse(options.body).records[0].fields;
    return response({ records: [{ id: "recDeviceRetire01", fields: patchedFields }] });
  };

  const result = await performRecognitionAction({
    env,
    request: request(),
    fetchImpl,
    recordSession: async (input) => events.push(input.payload),
    payload: basePayload("retire_device")
  });

  assert.equal(result.retired, true);
  assert.equal(result.device_record_id, "recDeviceRetire01");
  assert.equal(patchedFields.status, "Retired");
  assert.equal(events[0].event_type, "device_retired");
});

test("profile actions require both user and sms before any Airtable call", async () => {
  let called = false;
  await assert.rejects(
    performRecognitionAction({
      env,
      request: request(),
      fetchImpl: async () => { called = true; },
      recordSession: async () => {},
      payload: basePayload("create_profile", { user: "", sms: "" })
    }),
    (error) => error instanceof RecognitionIdentityError && error.code === "missing_user"
  );
  assert.equal(called, false);
});
