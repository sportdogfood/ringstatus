import assert from "node:assert/strict";
import test from "node:test";
import { recognitionConfig, RECOGNIZE_BASE_ID, RECOGNIZE_TABLES } from "../src/lib/rs-recognition-config.js";
import { runRecognitionAction } from "../src/lib/rs-recognition-action.js";
import { recordRecognitionSession } from "../src/lib/rs-recognition-session.js";
import { env as routeEnv } from "cloudflare:workers";
import { GET } from "../src/pages/rs-recognition/device.js";

const env = { AIRTABLE_TOKEN: "synthetic-token", AIRTABLE_RS_RECOGNITION_BASE_ID: RECOGNIZE_BASE_ID };
const request = new Request("https://example.invalid/rs-recognition/action");
const payload = { action: "retire_device", device_token: "synthetic-device", session_uid: "synthetic-session", session_event_uid: "synthetic-event" };

test("existing explicit recognition binding is supported without a general-base fallback", () => {
  const configured = { AIRTABLE_TOKEN: env.AIRTABLE_TOKEN, RS_INPUTS_RECOGNITION_BASE_ID: RECOGNIZE_BASE_ID };
  assert.equal(recognitionConfig(configured).baseId, RECOGNIZE_BASE_ID);
  assert.throws(() => recognitionConfig({ ...configured, AIRTABLE_RS_RECOGNITION_BASE_ID: "apptdhhNzduxm5gjn" }));
  assert.throws(() => recognitionConfig({ AIRTABLE_TOKEN: env.AIRTABLE_TOKEN, AIRTABLE_BASE_ID: RECOGNIZE_BASE_ID, RS_INPUTS_BASE_ID: RECOGNIZE_BASE_ID }));
  assert.throws(() => recognitionConfig({ ...configured, RS_INPUTS_RECOGNITION_BASE_ID: "apptdhhNzduxm5gjn" }));
});

test("deployed trusted invited-principal contract is preserved without a browser identity override", async () => {
  const personId = "recSynthetic00001";
  const trustedUid = "synthetic-invited";
  let writes = 0;
  const fetchImpl = async (url, options = {}) => {
    if (options.method === "POST" || options.method === "PATCH") {
      writes++; return Response.json({ records: [{ id: "recSynthetic00002" }] });
    }
    if (new URL(url).pathname.endsWith(personId)) return Response.json({ id: personId, fields: { person_uid: trustedUid, status: "Active", access_level: "Guest", input_access: "invited" } });
    return Response.json({ records: [] });
  };
  const input = { ...payload, action: "confirm_device", person_record_id: personId, person_uid: trustedUid, verifiedInputPersonUid: trustedUid };
  await assert.rejects(runRecognitionAction({ env, fetchImpl, request, payload: input, recordSession: async () => {} }), error => error.code === "unauthorized_device");
  assert.equal(writes, 0);
  const response = await runRecognitionAction({ env, fetchImpl, request, payload: input, verifiedInputPersonUid: trustedUid, recordSession: async () => {} });
  assert.equal(response.confirmed, true);
  assert.equal(writes, 1);
});

test("trusted principal cannot bypass revoked or inactive invited access", async () => {
  for (const fields of [{ status: "Inactive", input_access: "invited" }, { status: "Active", input_access: "revoked" }]) {
    let writes = 0;
    const personId = "recSynthetic00001", personUid = "synthetic-invited";
    const fetchImpl = async (url, options = {}) => {
      if (options.method) writes++;
      return new URL(url).pathname.endsWith(personId)
        ? Response.json({ id: personId, fields: { ...fields, person_uid: personUid, access_level: "Guest" } })
        : Response.json({ records: [] });
    };
    await assert.rejects(runRecognitionAction({ env, fetchImpl, request, verifiedInputPersonUid: personUid, recordSession: async () => {}, payload: { ...payload, action: "confirm_device", person_record_id: personId, person_uid: personUid } }), error => error.code === "unauthorized_person");
    assert.equal(writes, 0);
  }
});

test("every recognition entry point rejects absent, old and unrelated bases before any fetch", async () => {
  for (const base of [undefined, "apptdhhNzduxm5gjn", "app_wrong"]) {
    const bad = { ...env, AIRTABLE_RS_RECOGNITION_BASE_ID: base };
    let calls = 0;
    const fetchImpl = async () => { calls++; throw new Error("must not fetch"); };
    await assert.rejects(runRecognitionAction({ env: bad, payload, request, fetchImpl }));
    await assert.rejects(recordRecognitionSession({ env: bad, payload: {}, request, fetchImpl }));
    Object.assign(routeEnv, bad);
    const saved = globalThis.fetch;
    globalThis.fetch = fetchImpl;
    try {
      const response = await GET({ request: new Request("https://example.invalid/device?device_token=synthetic") });
      assert.equal(response.status, 500);
    } finally { globalThis.fetch = saved; }
    assert.equal(calls, 0);
  }
});

test("configuration pins all table IDs and rejects arbitrary table override", () => {
  const config = recognitionConfig(env);
  for (const key of Object.keys(RECOGNIZE_TABLES)) assert.equal(config[key], RECOGNIZE_TABLES[key]);
  for (const variable of ["AIRTABLE_RS_PEOPLE_TEST_TABLE", "AIRTABLE_RS_DEVICES_TEST_TABLE", "AIRTABLE_RS_PHONE_ALIASES_TEST_TABLE", "AIRTABLE_RS_RECOGNITION_SESSIONS_TEST_TABLE"]) {
    assert.throws(() => recognitionConfig({ ...env, [variable]: "tblOther" }), /recognition_table_mismatch/);
  }
});

test("same-runtime concurrent replay performs one effective retirement and one log", async () => {
  let writes = 0, logs = 0;
  const fetchImpl = async (_url, options = {}) => {
    if (options.method === "PATCH") { writes++; return Response.json({ records: [{ id: "recSynthetic00001" }] }); }
    return Response.json({ records: [{ id: "recSynthetic00001", fields: { status: "Active" } }] });
  };
  const args = { env, payload, request, fetchImpl, recordSession: async () => { logs++; } };
  const results = await Promise.all([runRecognitionAction(args), runRecognitionAction(args), runRecognitionAction(args)]);
  assert.equal(writes, 1); assert.equal(logs, 1);
  assert.ok(results.every(result => result.retired && result.audit_status === "recorded"));
});

test("log-write failure preserves outcome and retries only the log in the same runtime", async () => {
  let writes = 0, logs = 0;
  const fetchImpl = async (_url, options = {}) => {
    if (options.method === "PATCH") { writes++; return Response.json({ records: [{ id: "recSynthetic00001" }] }); }
    return Response.json({ records: [{ id: "recSynthetic00001" }] });
  };
  const args = { env, payload, request, fetchImpl, recordSession: async () => { if (++logs === 1) throw new Error("synthetic-log-failure"); } };
  const first = await runRecognitionAction(args);
  assert.equal(first.retired, true); assert.equal(first.audit_status, "pending");
  const retry = await runRecognitionAction(args);
  assert.equal(retry.audit_status, "recorded"); assert.equal(writes, 1); assert.equal(logs, 2);
});

test("request ID cannot be reused with different action input", async () => {
  const fetchImpl = async () => Response.json({ records: [] });
  const args = { env, payload, request, fetchImpl, recordSession: async () => {} };
  await runRecognitionAction(args);
  await assert.rejects(runRecognitionAction({ ...args, payload: { ...payload, device_token: "another-device" } }), /request_id_conflict/);
});

test("uncertain upstream mutation is not repeated within the same runtime", async () => {
  let writes = 0;
  const fetchImpl = async (_url, options = {}) => {
    if (options.method === "PATCH") { writes++; return Response.json({}, { status: 502 }); }
    return Response.json({ records: [{ id: "recSynthetic00001" }] });
  };
  const args = { env, payload, request, fetchImpl, recordSession: async () => {} };
  await assert.rejects(runRecognitionAction(args), error => error.code === "airtable_request_failed");
  await assert.rejects(runRecognitionAction(args), /action_outcome_unknown/);
  assert.equal(writes, 1);
});

test("caller-supplied credentials and raw payloads are excluded from session detail", async () => {
  let written;
  const fetchImpl = async (_url, options = {}) => {
    if (!options.method) return Response.json({ records: [] });
    written = JSON.parse(options.body).records[0].fields;
    return Response.json({ records: [{ id: "recSynthetic00001" }] });
  };
  await recordRecognitionSession({ env, request, fetchImpl, payload: {
    session_uid: "session", session_event_uid: "event", idempotency_key: "key", event_type: "visit", event_result: "success",
    detail: { member_pin: "secret-pin", device_token: "secret-device", raw_payload: { password: "secret-password" }, source: "native-recognize" }
  } });
  assert.doesNotMatch(JSON.stringify(written), /secret-pin|secret-device|secret-password/);
  assert.match(written.event_detail, /native-recognize/);
});

test("blank edit PIN omits the credential from the PATCH and leaves its stored value intact", async () => {
  let personPatch;
  const personId = "recSynthetic00001", deviceId = "recSynthetic00002";
  const fetchImpl = async (url, options = {}) => {
    const path = new URL(url).pathname;
    if (options.method === "PATCH" || options.method === "POST") {
      const data = JSON.parse(options.body).records[0];
      if (path.endsWith(RECOGNIZE_TABLES.people)) personPatch = data.fields;
      return Response.json({ records: [{ id: data.id || "recSynthetic00003", fields: data.fields }] });
    }
    if (path.endsWith(RECOGNIZE_TABLES.devices)) return Response.json({ records: [{ id: deviceId, fields: { person: [personId], status: "Active" } }] });
    if (path.endsWith(personId)) return Response.json({ id: personId, fields: { person_uid: "synthetic-person", status: "Active", access_level: "member", member_pin: "4826" } });
    return Response.json({ records: [] });
  };
  const result = await runRecognitionAction({ env, request, fetchImpl, recordSession: async () => {}, payload: {
    ...payload, action: "update_profile", person_record_id: personId, person_uid: "synthetic-person", user: "Synthetic", sms: "2025550199", pin: ""
  } });
  assert.equal(result.ok, true); assert.equal("member_pin" in personPatch, false);
  assert.equal("member_pin" in result, false);
});

test("ambiguous device refuses retirement before any mutation", async () => {
  let writes = 0;
  const fetchImpl = async (_url, options = {}) => {
    if (options.method) writes++;
    return Response.json({ records: [{ id: "recSynthetic00001" }, { id: "recSynthetic00002" }] });
  };
  await assert.rejects(runRecognitionAction({ env, request, fetchImpl, payload, recordSession: async () => {} }), error => error.code === "ambiguous_device");
  assert.equal(writes, 0);
});

test("a conflicting phone alias is rejected before primary profile mutation", async () => {
  let writes = 0;
  const fetchImpl = async (url, options = {}) => {
    if (options.method) writes++;
    if (new URL(url).pathname.endsWith(RECOGNIZE_TABLES.people)) return Response.json({ records: [{ id: "recSynthetic00001" }] });
    return Response.json({ records: [{ id: "recSynthetic00003", fields: { person: ["recSynthetic00002"], status: "Active" } }] });
  };
  await assert.rejects(runRecognitionAction({ env, request, fetchImpl, recordSession: async () => {}, payload: { ...payload, action: "phone_login", sms: "2025550199" } }), error => error.code === "ambiguous_phone");
  assert.equal(writes, 0);
});

test("partial profile mutation followed by alias conflict retains its uncertain retry marker", async () => {
  let reads = 0, writes = 0;
  const fetchImpl = async (_url, options = {}) => {
    if (options.method) { writes++; return Response.json({ records: [{ id: "recSynthetic00001" }] }); }
    if (++reads < 3) return Response.json({ records: [] });
    return Response.json({ records: [{ id: "recSynthetic00003", fields: { person: ["recSynthetic00002"], status: "Active" } }] });
  };
  const args = { env, request, fetchImpl, recordSession: async () => {}, payload: { ...payload, action: "create_profile", user: "Synthetic", sms: "2025550199" } };
  await assert.rejects(runRecognitionAction(args), error => error.code === "phone_already_registered");
  await assert.rejects(runRecognitionAction(args), error => error.code === "action_outcome_unknown");
  assert.equal(writes, 1);
});

test("a completed request cache does not permanently stop the runtime after 256 actions", async () => {
  const fetchImpl = async () => Response.json({ records: [] });
  for (let i = 0; i < 258; i++) {
    const result = await runRecognitionAction({ env, request, fetchImpl, recordSession: async () => {}, payload: { ...payload, session_event_uid: "synthetic-" + i } });
    assert.equal(result.retired, true);
  }
});

test("zero-write validation failures do not exhaust action capacity", async () => {
  let calls = 0;
  const fetchImpl = async () => { calls++; return Response.json({ records: [] }); };
  for (let i = 0; i < 258; i++) {
    await assert.rejects(runRecognitionAction({ env, request, fetchImpl, recordSession: async () => {}, payload: { ...payload, action: "create_profile", user: "", session_event_uid: "invalid-" + i } }), error => error.code === "missing_user");
  }
  assert.equal(calls, 0);
  const result = await runRecognitionAction({ env, request, fetchImpl, recordSession: async () => {}, payload });
  assert.equal(result.retired, true);
});
