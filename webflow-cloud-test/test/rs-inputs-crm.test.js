import assert from "node:assert/strict";
import test from "node:test";
import { createCrmInputStore } from "../src/lib/rs-inputs-crm.js";
import { loadSchema, main, provision } from "../scripts/rs-inputs-storage.mjs";
import { runRecognitionAction } from "../src/lib/rs-recognition-action.js";

const mappings = { barns: { module: "RS_Trial_Barns", fields: { entity_uid: "Entity_UID", name: "Name", barn_uid: "Barn_UID", revision: "Revision" } } };
const org = { org: [{ zgid: "941333935", id: "different_crm_id", company_name: "Ringstatus" }] };
function fixture(sequence) {
  const calls = [];
  const fetchImpl = async (url, options = {}) => {
    calls.push({ url: String(url), method: options.method || "GET", headers: options.headers, body: options.body && JSON.parse(options.body) });
    assert.ok(sequence.length, `Unexpected request ${url}`);
    const next = sequence.shift();
    if (next instanceof Error) throw next;
    return Response.json(next.body ?? next, { status: next.status || 200 });
  };
  return { calls, fetchImpl };
}
function metadata({ unique = { case_sensitive: true } } = {}) {
  return [org, { modules: [{ api_name: "RS_Trial_Barns", api_supported: true }] }, { fields: Object.values(mappings.barns.fields).map(api_name => ({ api_name, ...(api_name === "Entity_UID" ? { unique } : {}) })) }];
}
const success = { data: [{ status: "success", code: "SUCCESS", details: { id: "901", Modified_Time: "2026-10-05T16:00:00-04:00" } }] };

test("CRM wrong org stops before metadata and writes", async () => {
  const f = fixture([{ org: [{ zgid: "681603578" }] }]);
  const store = createCrmInputStore({ token: "fake", mappings, ...f });
  await assert.rejects(store.create("barns", { entity_uid: "barn_test", name: "Test" }), { code: "wrong_crm_organization" });
  assert.equal(f.calls.length, 1);
  assert.equal(f.calls[0].method, "GET");
});

test("CRM refuses commerce modules, unsupported types, and missing mappings", async () => {
  assert.throws(() => createCrmInputStore({ token: "fake", mappings: { barns: { ...mappings.barns, module: "Accounts" } } }), { code: "invalid_crm_mapping" });
  assert.throws(() => createCrmInputStore({ token: "fake", mappings: { horses: mappings.barns } }), { code: "invalid_crm_mapping" });
  const store = createCrmInputStore({ token: "fake", mappings, fetchImpl: () => assert.fail("no request expected") });
  await assert.rejects(store.create("locations", { entity_uid: "x", name: "x" }), { code: "unconfigured_crm_entity" });
});

test("CRM rejects nonexistent mapped fields and nonunique canonical ID before writes", async () => {
  const missing = fixture([org, metadata()[1], { fields: [] }]);
  await assert.rejects(createCrmInputStore({ token: "fake", mappings, ...missing }).preflight(), { code: "crm_field_unavailable" });
  const f = fixture(metadata({ unique: null }));
  await assert.rejects(createCrmInputStore({ token: "fake", mappings, ...f }).create("barns", { entity_uid: "b", name: "Test" }), { code: "crm_identity_not_unique" });
  assert.ok(f.calls.every(c => c.method === "GET"));
});

test("CRM create uses exact supplied mapping and disables trigger workflows", async () => {
  const f = fixture([...metadata(), success]);
  const result = await createCrmInputStore({ token: "fake", mappings, ...f }).create("barns", { entity_uid: "barn_1", name: "Test barn", revision: 1 });
  assert.equal(result.record_id, "901");
  assert.deepEqual(f.calls.at(-1).body.data, [{ Entity_UID: "barn_1", Name: "Test barn", Revision: 1 }]);
  assert.deepEqual(f.calls.at(-1).body.trigger, []);
  assert.deepEqual(f.calls.at(-1).body.skip_feature_execution, [{ name: "cadences" }]);
});

test("CRM stale update supplies If-Unmodified-Since and reports conflict", async () => {
  const f = fixture([...metadata(), { data: [{ id: "901", Entity_UID: "barn_1", Name: "Original" }] }, { status: 412, body: { code: "ALREADY_MODIFIED" } }]);
  const store = createCrmInputStore({ token: "fake", mappings, ...f });
  const time = "2026-10-05T15:00:00-04:00";
  await assert.rejects(store.update("barns", "901", { name: "Changed" }, { modifiedTime: time }), { code: "stale_revision" });
  assert.equal(f.calls.at(-1).headers["If-Unmodified-Since"], time);
  assert.equal(f.calls.at(-1).method, "PUT");
});

test("CRM rejects missing revision and immutable ID before network", async () => {
  const store = createCrmInputStore({ token: "fake", mappings, fetchImpl: () => assert.fail("no request expected") });
  await assert.rejects(store.update("barns", "901", { name: "Changed" }), { code: "missing_modified_time" });
  await assert.rejects(store.update("barns", "901", { entity_uid: "other" }), { code: "immutable_entity_identity" });
});

test("CRM write timeout remains unknown and is never retried", async () => {
  const f = fixture([...metadata(), new Error("timeout")]);
  await assert.rejects(createCrmInputStore({ token: "fake", mappings, ...f }).create("barns", { entity_uid: "b", name: "Test" }), { code: "crm_write_outcome_unknown" });
  assert.equal(f.calls.filter(c => c.method === "POST").length, 1);
});

test("CRM HTTP 207 record error cannot be mistaken for saved", async () => {
  const f = fixture([...metadata(), { status: 207, body: { data: [{ status: "error", code: "DUPLICATE_DATA" }] } }]);
  await assert.rejects(createCrmInputStore({ token: "fake", mappings, ...f }).create("barns", { entity_uid: "b", name: "Test" }), { code: "duplicate_entity" });
});

test("CRM list follows pagination and returns mapped IDs", async () => {
  const f = fixture([...metadata(), { data: [{ id: "901", Entity_UID: "b", Name: "One" }], info: { more_records: true } }, { data: [{ id: "902", Entity_UID: "c", Name: "Two" }], info: { more_records: false } }]);
  const rows = await createCrmInputStore({ token: "fake", mappings, ...f }).list("barns");
  assert.deepEqual(rows.map(r => r.entity_uid), ["b", "c"]);
  assert.match(f.calls.at(-1).url, /page=2$/);
});

test("CRM malformed list response is not silently treated as no matches", async () => {
  const f = fixture([...metadata(), { message: "invalid provider result" }]);
  await assert.rejects(createCrmInputStore({ token: "fake", mappings, ...f }).list("barns"), { code: "invalid_crm_response" });
});

test("provision defaults to dry-run and never accesses network", async () => {
  const result = await provision({ workspaceId: "wspTest", token: "fake", fetchImpl: () => assert.fail("dry run made a request") });
  assert.equal(result.mode, "dry-run");
  assert.equal(result.tables.length, 10);
  for (const table of result.tables.filter(t => /^rs_input_/.test(t.name) && t.name !== "rs_input_events")) {
    assert.equal(table.fields[0].name, "entity_uid");
    assert.ok(table.fields.some(f => f.name === "owner_uid"));
    assert.ok(table.fields.some(f => f.name === "request_hash"));
  }
});

test("CLI requires explicit credential variable/workspace and has no existing base mode", async () => {
  await assert.rejects(main(["--base", "appZahVgD156cMAe3"], {}), /Unsupported/);
  await assert.rejects(main(["--workspace", "wspTest"], {}), /token-env/);
  await assert.rejects(main(["--workspace", "wspTest", "--token-env", "TEST_PAT"], {}), /explicit token/);
  const result = await main(["--workspace", "wspTest", "--token-env", "TEST_PAT"], { TEST_PAT: "not-real" });
  assert.equal(result.mode, "dry-run");
  assert.ok(!JSON.stringify(result).includes("not-real"));
});

test("provision refuses protected returned base and reports partial link failure", async () => {
  const protectedFetch = fixture([{ id: "appZahVgD156cMAe3", tables: [] }]);
  await assert.rejects(provision({ workspaceId: "wspTest", token: "fake", apply: true, ...protectedFetch }), /protected/);
  assert.equal(protectedFetch.calls.length, 1);
  const schema = await loadSchema();
  const f = fixture([{ id: "appFreshTrial", tables: schema.tables.map((t, i) => ({ name: t.name, id: `tbl${i}` })) }, { status: 403, body: {} }]);
  await assert.rejects(provision({ workspaceId: "wspTest", token: "fake", apply: true, ...f }), error => error.partial?.baseId === "appFreshTrial" && error.partial.completedLinks.length === 0);
  assert.equal(f.calls.length, 2);
});

test("provision mock completes link fields only inside fresh returned base", async () => {
  const schema = await loadSchema();
  const f = fixture([{ id: "appFreshTrial", tables: schema.tables.map((t, i) => ({ name: t.name, id: `tbl${i}` })) }, ...schema.linkFields.map(() => ({ id: "fldNew" }))]);
  const result = await provision({ workspaceId: "wspTest", token: "fake", apply: true, ...f });
  assert.equal(result.completedLinks.length, 5);
  assert.ok(f.calls.slice(1).every(c => c.url.startsWith("https://api.airtable.com/v0/meta/bases/appFreshTrial/")));
});

test("recognition create action + session event fits scaffold fields and link types", async () => {
  const schema = await loadSchema();
  const fields = new Map(schema.tables.map(t => [t.name, new Set(t.fields.map(f => f.name))]));
  for (const link of schema.linkFields) fields.get(link.table).add(link.name);
  let sequence = 0;
  const writes = [];
  const fetchImpl = async (value, options = {}) => {
    const url = new URL(value);
    const table = decodeURIComponent(url.pathname.split("/")[3]);
    assert.ok(fields.has(table), `Unknown table ${table}`);
    if (!options.method || options.method === "GET") return Response.json({ records: [] });
    const body = JSON.parse(options.body);
    for (const row of body.records) for (const name of Object.keys(row.fields)) assert.ok(fields.get(table).has(name), `Missing ${table}.${name}`);
    writes.push({ table, body });
    return Response.json({ records: body.records.map(row => ({ id: `rec${String(++sequence).padStart(14, "0")}`, ...row })) });
  };
  const result = await runRecognitionAction({ env: { AIRTABLE_TOKEN: "fake", AIRTABLE_RS_RECOGNITION_BASE_ID: "appFreshTrial" }, fetchImpl, payload: { action: "create_profile", session_uid: "session_trial", session_event_uid: "event_trial", device_token: "device_trial", first: "Trial", last: "Person", user: "Trial Person", sms: "2025550123", pin: "0123", email: "trial@example.test" }, request: new Request("https://example.test/recognize") });
  assert.equal(result.ok, true);
  assert.deepEqual(writes.map(w => w.table), ["rs_people_test", "rs_phone_aliases_test", "rs_devices_test", "rs_recognition_sessions_test"]);
});
