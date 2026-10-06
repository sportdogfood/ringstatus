import assert from "node:assert/strict";
import test from "node:test";
import { createInputRecognition } from "../src/lib/rs-inputs-recognition.js";

const env = { RS_INPUTS_BASE_ID: "app" + "1".repeat(14), AIRTABLE_TOKEN: "test-only-token", RS_INPUTS_WRITE_MODE: "isolated-trial" };
const person = { id: "rec" + "p".repeat(14), fields: { person_uid: "person_alex", person_name: "Alex Morgan", first_name: "Alex", last_name: "Morgan", primary_phone_e164: "+12025550148", member_pin: "0148", email: "alex@example.com", status: "Active", access_level: "member" } };
const device = { id: "rec" + "d".repeat(14), fields: { device_token: "device_one", status: "Active", person: [person.id] } };
const alias = { id: "rec" + "a".repeat(14), fields: { person: [person.id] } };
const list = (record) => ({ records: record ? [record] : [] });
const request = new Request("https://example.test/test/inputs/api/recognition");
const payload = (action, values = {}) => ({ action, values, device_token: "device_one", requestId: "request_123" });
const errorCode = (code, status) => (error) => error.code === code && error.status === status;
function fixture(responses, overrides = {}, principalPersonUid = person.fields.person_uid) {
  const calls = [];
  const fetchImpl = async (url, options = {}) => {
    calls.push({ url: String(url), method: options.method || "GET", body: options.body ? JSON.parse(options.body) : null });
    assert.ok(responses.length, `Unexpected request ${url}`);
    const response = responses.shift();
    return response instanceof Response ? response : Response.json(response);
  };
  const configuredEnv = { ...env, ...overrides };
  const api = createInputRecognition({ env: configuredEnv, fetchImpl, principalPersonUid });
  const expectedBase = configuredEnv.RS_INPUTS_RECOGNITION_BASE_ID?.trim() || configuredEnv.RS_INPUTS_BASE_ID;
  return { api, calls, done() { assert.equal(responses.length, 0); for (const call of calls) assert.equal(new URL(call.url).pathname.split("/")[2], expectedBase); } };
}

test("configuration fails closed without an explicit clean base and token", () => {
  const fetchImpl = () => assert.fail("No network allowed");
  for (const [override, code] of [
    [{ RS_INPUTS_BASE_ID: "" }, "missing_rs_inputs_base_id"],
    [{ RS_INPUTS_BASE_ID: "appZahVgD156cMAe3" }, "unsafe_rs_inputs_base_id"],
    [{ RS_INPUTS_BASE_ID: "apptdhhNzduxm5gjn" }, "unsafe_rs_inputs_base_id"],
    [{ RS_INPUTS_RECOGNITION_BASE_ID: "appZahVgD156cMAe3" }, "unsafe_rs_inputs_base_id"],
    [{ RS_INPUTS_BASE_ID: "apptdhhNzduxm5gjn", RS_INPUTS_RECOGNITION_BASE_ID: " " }, "unsafe_rs_inputs_base_id"],
    [{ RS_INPUTS_RECOGNITION_BASE_ID: "invalid" }, "invalid_rs_inputs_base_id"],
    [{ RS_INPUTS_BASE_ID: "app_invalid" }, "invalid_rs_inputs_base_id"],
    [{ AIRTABLE_TOKEN: "" }, "missing_airtable_token"]
  ]) assert.throws(() => createInputRecognition({ env: { ...env, ...override }, fetchImpl }), errorCode(code, 503));
});

test("passive lookup is read-only and ignores old base/table overrides", async () => {
  const f = fixture([list(device), person], { AIRTABLE_RS_RECOGNITION_BASE_ID: "apptdhhNzduxm5gjn", AIRTABLE_RS_PEOPLE_TEST_TABLE: "wrong_table" });
  assert.deepEqual(await f.api.lookup("device_one"), { ok: true, recognized: true, device: "active", profile: { person_uid: "person_alex", person_name: "Alex Morgan", first_name: "Alex", last_name: "Morgan", sms: "+12025550148", pin: "0148", email: "alex@example.com" } });
  assert.ok(f.calls.every((call) => call.method === "GET"));
  assert.equal(new URL(f.calls[0].url).searchParams.get("maxRecords"), "2");
  assert.ok(f.calls[1].url.includes("rs_people_test"));
  f.done();
});

test("missing, unknown, and retired device do not return a profile or write", async () => {
  const f = fixture([list(), list({ ...device, fields: { ...device.fields, status: "Retired" } })]);
  assert.deepEqual(await f.api.lookup(""), { ok: true, recognized: false, device: "unknown", profile: null });
  assert.equal(f.calls.length, 0);
  assert.equal((await f.api.lookup("absent")).device, "unknown");
  assert.deepEqual(await f.api.lookup("retired"), { ok: true, recognized: false, device: "retired", profile: null });
  assert.ok(f.calls.every((call) => call.method === "GET"));
  f.done();
});

test("ambiguous devices and ambiguous person links fail instead of selecting first", async () => {
  const f = fixture([{ records: [device, device] }, list({ ...device, fields: { ...device.fields, person: [person.id, "recOther000000001"] } })]);
  await assert.rejects(f.api.lookup("duplicate"), errorCode("ambiguous_device", 409));
  await assert.rejects(f.api.lookup("two_people"), errorCode("ambiguous_device_person", 409));
  f.done();
});

test("inactive people and disallowed access remain unrecognized", async () => {
  for (const fields of [{ status: "Retired" }, { access_level: "guest" }]) {
    const f = fixture([list(device), { ...person, fields: { ...person.fields, ...fields } }]);
    assert.equal((await f.api.lookup("device_one")).recognized, false);
    f.done();
  }
});

test("write mode and recovery guards prevent every upstream request", async () => {
  const closed = fixture([], { RS_INPUTS_WRITE_MODE: "production" });
  await assert.rejects(closed.api.action(payload("confirm_device"), request), errorCode("recognition_writes_disabled", 503));
  const f = fixture([]);
  await assert.rejects(f.api.action(payload("recovery", { email: "alex@example.com" }), request), errorCode("recovery_delivery_unconfigured", 503));
  await assert.rejects(f.api.action({ ...payload("phone_login"), requestId: "" }, request), errorCode("invalid_request_id", 400));
  assert.equal(f.calls.length + closed.calls.length, 0);
});

test("confirm reuses existing action and audit with server-hydrated ownership", async () => {
  const f = fixture([list(device), person, list(device), person, list(device), list(device), list(), list({ id: "recSession00000001" }), list(device), person]);
  const answer = await f.api.action(payload("confirm_device", { person_uid: "forged", person_record_id: "recOther000000001" }), request);
  assert.equal(answer.profile.person_uid, "person_alex");
  const patch = f.calls.find((call) => call.method === "PATCH");
  assert.deepEqual(patch.body.records[0].fields.person, [person.id]);
  const audit = f.calls.find((call) => call.method === "POST" && call.url.endsWith("rs_recognition_sessions_test"));
  assert.equal(audit.body.records[0].fields.idempotency_key, "confirm_device:inputs_request_123");
  assert.deepEqual(audit.body.records[0].fields.person, [person.id]);
  f.done();
});

test("update maps frontend profile fields through existing normalization and audit", async () => {
  const updated = { ...person, fields: { ...person.fields, person_name: "Alex New", first_name: "Alexandra", last_name: "New", member_pin: "4826", email: "new@example.com" } };
  const f = fixture([list(device), person, list(person), list(alias), list(device), person, list(person), list(updated), list(alias), list(alias), list(device), list(device), list(), list({ id: "recSession00000001" }), list(device), updated]);
  const answer = await f.api.action(payload("update_profile", { person_name: "Alex New", first_name: "Alexandra", last_name: "New", sms: "202-555-0148", pin: "4826", email: "NEW@EXAMPLE.COM", person_uid: "forged" }), request);
  assert.equal(answer.profile.pin, "4826");
  const patch = f.calls.find((call) => call.method === "PATCH" && call.url.endsWith("rs_people_test"));
  assert.deepEqual(patch.body.records[0], { id: person.id, fields: { person_name: "Alex New", first_name: "Alexandra", last_name: "New", primary_phone_e164: "+12025550148", member_pin: "4826", email: "new@example.com" } });
  f.done();
});

test("phone_login maps identifier and returns the confirmed server profile", async () => {
  const f = fixture([list(), list(person), list(), list(person), list(), list(device), list(), list({ id: "recSession00000001" }), list(device), person]);
  assert.equal((await f.api.action(payload("phone_login", { identifier: "202-555-0148" }), request)).recognized, true);
  assert.match(new URL(f.calls[1].url).searchParams.get("filterByFormula"), /\+12025550148/);
  f.done();
});

test("unrecognized profile update and duplicate-token action never mutate", async () => {
  const f = fixture([list(), { records: [device, device] }]);
  await assert.rejects(f.api.action(payload("update_profile"), request), errorCode("unauthorized_device", 403));
  await assert.rejects(f.api.action(payload("retire_device"), request), errorCode("ambiguous_device", 409));
  assert.ok(f.calls.every((call) => call.method === "GET"));
  f.done();
});

test("retire returns no profile after the existing action and audit succeed", async () => {
  const f = fixture([list(device), person, list(device), list(device), list(), list({ id: "recSession00000001" })]);
  assert.deepEqual(await f.api.action(payload("retire_device"), request), { ok: true, recognized: false, profile: null, device: "retired" });
  assert.equal(f.calls.find((call) => call.method === "PATCH").body.records[0].fields.status, "Retired");
  f.done();
});

test("audit failure after an action write is surfaced and never reported as success", async () => {
  const f = fixture([list(device), person, list(device), list(device), list(), Response.json({ secretDetail: "must not escape" }, { status: 502 })]);
  await assert.rejects(f.api.action(payload("retire_device"), request), (error) => {
    assert.equal(error.message, "session_event_create_failed");
    return error.code === "session_event_create_failed" && error.status === 502;
  });
  // Existing actions are nontransactional: failure does not undo the device write.
  assert.equal(f.calls.filter((call) => call.method === "PATCH").length, 1);
  f.done();
});

test("every action requires an independent verified principal before any lookup or write", async () => {
  const f = fixture([], {}, "");
  for (const action of ["create_profile", "update_profile", "phone_login", "confirm_device", "retire_device", "recovery"]) {
    await assert.rejects(f.api.action(payload(action), request), errorCode("verified_profile_required", 503));
  }
  assert.equal(f.calls.length, 0);
});

test("registration remains unavailable rather than granting authority from a new profile", async () => {
  const f = fixture([]);
  await assert.rejects(f.api.action(payload("create_profile"), request), errorCode("profile_creation_unconfigured", 503));
  assert.equal(f.calls.length, 0);
});

test("verified actor cannot update, confirm, retire, or rebind another person's device", async () => {
  for (const action of ["update_profile", "confirm_device", "retire_device", "phone_login"]) {
    const f = fixture([list(device), person], {}, "person_someone_else");
    await assert.rejects(f.api.action(payload(action, { identifier: "2025550148" }), request), errorCode("verified_profile_required", 403));
    assert.ok(f.calls.every((call) => call.method === "GET"));
    f.done();
  }
});

test("knowing someone else's phone or PIN does not authorize device binding", async () => {
  for (const identifier of ["2025550148", "0148"]) {
    const f = fixture(identifier.length === 4 ? [list(), list(person)] : [list(), list(person), list()], {}, "person_someone_else");
    await assert.rejects(f.api.action(payload("phone_login", { identifier }), request), errorCode("verified_profile_required", 403));
    assert.ok(f.calls.every((call) => call.method === "GET"));
    f.done();
  }
});

test("duplicate people, duplicate aliases, conflicting aliases and ambiguous PINs fail before writes", async () => {
  const otherAlias = { ...alias, fields: { person: ["rec" + "z".repeat(14)] } };
  for (const { responses, code, identifier } of [
    { responses: [list(), { records: [person, person] }], code: "ambiguous_phone" },
    { responses: [list(), list(person), { records: [alias, alias] }], code: "ambiguous_phone_alias" },
    { responses: [list(), list(person), list(otherAlias)], code: "conflicting_phone_alias" },
    { responses: [list(), { records: [person, person] }], code: "ambiguous_pin", identifier: "0148" }
  ]) {
    const f = fixture(responses);
    await assert.rejects(f.api.action(payload("phone_login", { identifier: identifier || "2025550148" }), request), errorCode(code, 409));
    assert.ok(f.calls.every((call) => call.method === "GET"));
    f.done();
  }
});

test("profile edit checks phone and alias ambiguity before changing the person", async () => {
  for (const { candidates, code } of [
    { candidates: [{ records: [person, person] }], code: "ambiguous_phone" },
    { candidates: [list(person), { records: [alias, alias] }], code: "ambiguous_phone_alias" },
    { candidates: [list({ ...person, id: "rec" + "z".repeat(14) }), list()], code: "phone_already_registered" }
  ]) {
    const f = fixture([list(device), person, ...candidates]);
    await assert.rejects(f.api.action(payload("update_profile", { sms: "2025550148" }), request), errorCode(code, 409));
    assert.ok(f.calls.every((call) => call.method === "GET"));
    f.done();
  }
});

test("login cannot rebind an existing retired device", async () => {
  const f = fixture([list({ ...device, fields: { ...device.fields, status: "Retired" } })]);
  await assert.rejects(f.api.action(payload("phone_login", { identifier: "2025550148" }), request), errorCode("unauthorized_device", 403));
  assert.ok(f.calls.every((call) => call.method === "GET"));
  f.done();
});

test("lookup never exposes a different person's profile to the trusted actor", async () => {
  const f = fixture([list(device), person], {}, "person_someone_else");
  await assert.rejects(f.api.lookup("device_one"), errorCode("verified_profile_required", 403));
  assert.ok(f.calls.every((call) => call.method === "GET"));
  f.done();
});

test("lookup with a token requires a trusted principal before reading profile data", async () => {
  const f = fixture([], {}, "");
  await assert.rejects(f.api.lookup("device_one"), errorCode("verified_profile_required", 503));
  assert.deepEqual(await f.api.lookup(""), { ok: true, recognized: false, profile: null, device: "unknown" });
  assert.equal(f.calls.length, 0);
});

test("same-owner retirement retry repairs the audit after the device write committed", async () => {
  const retired = { ...device, fields: { ...device.fields, status: "Retired" } };
  const f = fixture([
    list(device), person, list(device), list(retired), list(), Response.json({}, { status: 502 }),
    list(retired), person, list(retired), list(retired), list(), list({ id: "recSession00000001" })
  ]);
  await assert.rejects(f.api.action(payload("retire_device"), request), errorCode("session_event_create_failed", 502));
  assert.deepEqual(await f.api.action(payload("retire_device"), request), { ok: true, recognized: false, profile: null, device: "retired" });
  const deviceWrites = f.calls.filter((call) => call.method === "PATCH");
  assert.equal(deviceWrites.length, 2);
  assert.ok(deviceWrites.every((call) => call.body.records[0].id === device.id && call.body.records[0].fields.status === "Retired"));
  const auditWrites = f.calls.filter((call) => call.method === "POST");
  assert.equal(auditWrites.length, 2);
  assert.ok(auditWrites.every((call) => call.body.records[0].fields.idempotency_key === "retire_device:inputs_request_123"));
  f.done();
});

test("retired-device retry still rejects a different trusted principal", async () => {
  const retired = { ...device, fields: { ...device.fields, status: "Retired" } };
  const f = fixture([list(retired), person], {}, "person_someone_else");
  await assert.rejects(f.api.action(payload("retire_device"), request), errorCode("verified_profile_required", 403));
  assert.ok(f.calls.every((call) => call.method === "GET"));
  f.done();
});

test("explicit existing recognition binding permits read-only lookup before input base setup", async () => {
  const f = fixture([list(device), person], {
    RS_INPUTS_BASE_ID: "",
    RS_INPUTS_RECOGNITION_BASE_ID: "apptdhhNzduxm5gjn"
  });
  assert.equal((await f.api.lookup("device_one")).profile.person_uid, person.fields.person_uid);
  assert.ok(f.calls.every((call) => call.method === "GET"));
  f.done();
});

test("split recognition binding routes reused action and audit only to selected recognition base", async () => {
  const f = fixture([list(device), person, list(device), person, list(device), list(device), list(), list({ id: "recSession00000001" }), list(device), person], {
    RS_INPUTS_RECOGNITION_BASE_ID: "apptdhhNzduxm5gjn",
    AIRTABLE_RS_RECOGNITION_BASE_ID: "appZahVgD156cMAe3",
    AIRTABLE_RS_DEVICES_TEST_TABLE: "wrong_devices",
    AIRTABLE_RS_PEOPLE_TEST_TABLE: "wrong_people",
    AIRTABLE_RS_RECOGNITION_SESSIONS_TEST_TABLE: "wrong_sessions"
  });
  assert.equal((await f.api.action(payload("confirm_device"), request)).recognized, true);
  assert.ok(f.calls.every((call) => !call.url.includes("wrong_") && !call.url.includes(env.RS_INPUTS_BASE_ID) && !call.url.includes("appZahVgD156cMAe3")));
  assert.equal(f.calls.find((call) => call.method === "POST").url.split("/").at(-1), "rs_recognition_sessions_test");
  f.done();
});

test("explicit existing recognition binding preserves principal and trial-write guards", async () => {
  const settings = { RS_INPUTS_RECOGNITION_BASE_ID: "apptdhhNzduxm5gjn" };
  const untrusted = fixture([], settings, "");
  await assert.rejects(untrusted.api.lookup("device_one"), errorCode("verified_profile_required", 503));
  const writesDisabled = fixture([], { ...settings, RS_INPUTS_WRITE_MODE: "" });
  await assert.rejects(writesDisabled.api.action(payload("confirm_device"), request), errorCode("recognition_writes_disabled", 503));
  const ambiguous = fixture([{ records: [device, device] }], settings);
  await assert.rejects(ambiguous.api.lookup("device_one"), errorCode("ambiguous_device", 409));
  untrusted.done();
  writesDisabled.done();
  ambiguous.done();
});
