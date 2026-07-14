import { recordRecognitionSession } from "./rs-recognition-session.js";

const ACTIONS = new Set(["create_profile", "update_profile", "phone_login", "recovery", "confirm_device", "retire_device"]);
const DEFAULT_RECOGNITION_BASE_ID = "apptdhhNzduxm5gjn";

export class RecognitionActionError extends Error {
  constructor(code, status, detail = "") {
    super(detail || code);
    this.name = "RecognitionActionError";
    this.code = code;
    this.status = status;
  }
}

export async function runRecognitionAction({ env, fetchImpl = fetch, payload, request, recordSession = recordRecognitionSession }) {
  const config = getConfig(env);
  const input = normalizeInput(payload);
  let result;

  if (input.action === "create_profile") result = await createProfile(config, input, fetchImpl);
  else if (input.action === "update_profile") result = await updateProfile(config, input, fetchImpl);
  else if (input.action === "phone_login") result = await phoneLogin(config, input, fetchImpl);
  else if (input.action === "recovery") result = await recovery(config, input, fetchImpl);
  else if (input.action === "confirm_device") result = await confirmDevice(config, input, fetchImpl);
  else result = await retireDevice(config, input, fetchImpl);

  await recordSession({ env, fetchImpl, request, payload: sessionPayload(input, result) });
  return result.response;
}

async function createProfile(config, input, fetchImpl) {
  const profile = profileInput(input);
  if (await findPersonByPhone(config, profile.sms, fetchImpl)) throw new RecognitionActionError("phone_already_registered", 409);
  const personUid = makeUid("person");
  const person = await createRecord(config, config.people, {
    person_uid: personUid,
    person_name: profile.user,
    first_name: profile.first,
    last_name: profile.last,
    primary_phone_e164: profile.sms,
    member_pin: profile.pin,
    email: profile.email,
    status: "Active",
    access_level: "member"
  }, fetchImpl);
  const alias = await upsertAlias(config, profile.sms, person.id, fetchImpl);
  const device = await upsertDevice(config, input.device_token, person.id, fetchImpl);
  return actionResult({
    person, alias, device, event_type: "new", event_result: "success", matched_by: "manual", recognition_status: "confirmed",
    detail: { changed_fields: ["person_name", "first_name", "last_name", "primary_phone_e164", "member_pin", "email"] },
    response: { ok: true, recognized: true, person_record_id: person.id, person_uid: personUid, person_name: profile.user, first_name: profile.first, last_name: profile.last, primary_phone_e164: profile.sms, member_pin: profile.pin, email: profile.email, device_record_id: device.id }
  });
}

async function updateProfile(config, input, fetchImpl) {
  const profile = profileInput(input);
  const personId = recordId(input.person_record_id, "missing_person_record_id");
  const personUid = required(input.person_uid, "missing_person_uid");
  const owner = await findPersonByPhone(config, profile.sms, fetchImpl);
  if (owner && owner.id !== personId) throw new RecognitionActionError("phone_already_registered", 409);
  const person = await updateRecord(config, config.people, personId, {
    person_name: profile.user,
    first_name: profile.first,
    last_name: profile.last,
    primary_phone_e164: profile.sms,
    member_pin: profile.pin,
    email: profile.email
  }, fetchImpl);
  const alias = await upsertAlias(config, profile.sms, personId, fetchImpl);
  const device = await upsertDevice(config, input.device_token, personId, fetchImpl);
  return actionResult({
    person, alias, device, event_type: "save", event_result: "success", matched_by: "manual", recognition_status: "confirmed",
    detail: { changed_fields: ["person_name", "first_name", "last_name", "primary_phone_e164", "member_pin", "email"] },
    response: { ok: true, recognized: true, person_record_id: personId, person_uid: personUid, person_name: profile.user, first_name: profile.first, last_name: profile.last, primary_phone_e164: profile.sms, member_pin: profile.pin, email: profile.email, device_record_id: device.id }
  });
}

async function phoneLogin(config, input, fetchImpl) {
  const identifier = required(input.sms, "missing_sms");
  const digits = identifier.replace(/\D/g, "");
  const matchedBy = digits.length === 4 ? "pin" : "phone";
  const person = matchedBy === "pin"
    ? await findPersonByPin(config, digits, fetchImpl)
    : await findPersonByPhone(config, phoneNumber(identifier), fetchImpl);
  if (!person || !isActive(person.fields?.status) || !isMemberAccess(person.fields?.access_level)) {
    return actionResult({ event_type: "login", event_result: "not_matched", matched_by: matchedBy, recognition_status: "rejected", detail: { source: "members_gate" }, response: { ok: true, recognized: false } });
  }
  const device = await upsertDevice(config, input.device_token, person.id, fetchImpl);
  return actionResult({
    person, device, event_type: "login", event_result: "matched", matched_by: matchedBy, recognition_status: "confirmed", detail: { source: "members_gate" },
    response: { ok: true, recognized: true, ...publicPerson(person), device_record_id: device.id }
  });
}

function findPersonByPin(config, pin, fetchImpl) {
  return listFirst(config, config.people, `OR({member_pin} = '${escapeFormula(pin)}',RIGHT({primary_phone_e164},4) = '${escapeFormula(pin)}')`, fetchImpl);
}

async function recovery(config, input, fetchImpl) {
  const first = clean(input.first);
  const last = clean(input.last);
  const email = emailAddress(input.email);
  if (!(first && last) && !email) throw new RecognitionActionError("missing_recovery_identity", 400);
  const clauses = [];
  if (email) clauses.push(`LOWER({email}) = '${escapeFormula(email)}'`);
  if (first && last) clauses.push(`AND(LOWER({first_name}) = '${escapeFormula(first.toLowerCase())}',LOWER({last_name}) = '${escapeFormula(last.toLowerCase())}')`);
  const match = await listFirst(config, config.people, clauses.length === 1 ? clauses[0] : `OR(${clauses.join(",")})`, fetchImpl);
  const person = match && isActive(match.fields?.status) && isMemberAccess(match.fields?.access_level) ? match : null;
  return actionResult({
    person, event_type: "recovery", event_result: person ? "matched" : "not_matched", matched_by: "manual", recognition_status: "pending",
    detail: person ? { automation_action: "send_member_link", return_to: "/" } : { automation_action: "none", return_to: "/" },
    response: { ok: true, accepted: true, return_to: "/" }
  });
}

async function confirmDevice(config, input, fetchImpl) {
  const personId = recordId(input.person_record_id, "missing_person_record_id");
  const personUid = required(input.person_uid, "missing_person_uid");
  const device = await upsertDevice(config, input.device_token, personId, fetchImpl);
  return actionResult({ person: { id: personId }, device, event_type: "visit", event_result: "success", matched_by: "device_token", recognition_status: "confirmed", detail: { source: "card_close" }, response: { ok: true, confirmed: true, person_uid: personUid, device_record_id: device.id } });
}

async function retireDevice(config, input, fetchImpl) {
  const token = required(input.device_token, "missing_device_token");
  const device = await findDevice(config, token, fetchImpl);
  const updated = device ? await updateRecord(config, config.devices, device.id, { status: "Retired", last_seen_at: new Date().toISOString() }, fetchImpl) : null;
  return actionResult({ device: updated, event_type: "device_retired", event_result: "success", matched_by: "device_token", recognition_status: "rejected", detail: { source: "not_you" }, response: { ok: true, retired: true, device_record_id: device?.id || "" } });
}

function actionResult(result) { return result; }

function sessionPayload(input, result) {
  return {
    session_event_uid: input.session_event_uid,
    session_uid: input.session_uid,
    event_type: result.event_type,
    event_result: result.event_result,
    idempotency_key: `${input.action}:${input.session_event_uid}`,
    person_record_id: result.person?.id || "",
    device_record_id: result.device?.id || "",
    phone_alias_record_id: result.alias?.id || "",
    matched_by: result.matched_by,
    recognition_status: result.recognition_status,
    client_timezone: clean(input.client_timezone),
    viewport_width: Number.isFinite(Number(input.viewport_width)) ? Number(input.viewport_width) : null,
    page_path: clean(input.page_path),
    referrer: clean(input.referrer),
    detail: result.detail
  };
}

async function findPersonByPhone(config, phone, fetchImpl) {
  const digits = phone.replace(/^\+/, "");
  const match = `OR({primary_phone_e164} = '${escapeFormula(phone)}',{primary_phone_e164} = '${escapeFormula(digits)}')`;
  const direct = await listFirst(config, config.people, match, fetchImpl);
  if (direct) return direct;
  const alias = await listFirst(config, config.aliases, `OR({alias_phone_e164} = '${escapeFormula(phone)}',{alias_phone_e164} = '${escapeFormula(digits)}')`, fetchImpl);
  const personId = firstLink(alias?.fields?.person);
  return personId ? getRecord(config, config.people, personId, fetchImpl) : null;
}

async function upsertAlias(config, phone, personId, fetchImpl) {
  const digits = phone.replace(/^\+/, "");
  const existing = await listFirst(config, config.aliases, `OR({alias_phone_e164} = '${escapeFormula(phone)}',{alias_phone_e164} = '${escapeFormula(digits)}')`, fetchImpl);
  const fields = { alias_phone_e164: phone, person: [personId], alias_type: "Mobile", status: "Active" };
  if (existing) {
    const owner = firstLink(existing.fields?.person);
    if (owner && owner !== personId) throw new RecognitionActionError("phone_already_registered", 409);
    return updateRecord(config, config.aliases, existing.id, fields, fetchImpl);
  }
  return createRecord(config, config.aliases, { alias_uid: makeUid("phone_alias"), ...fields }, fetchImpl);
}

async function upsertDevice(config, tokenValue, personId, fetchImpl) {
  const token = required(tokenValue, "missing_device_token");
  const existing = await findDevice(config, token, fetchImpl);
  const fields = { device_uid: clean(existing?.fields?.device_uid) || makeUid("device"), device_token: token, person: [personId], status: "Active", last_seen_at: new Date().toISOString(), recognition_source: "Web" };
  return existing ? updateRecord(config, config.devices, existing.id, fields, fetchImpl) : createRecord(config, config.devices, fields, fetchImpl);
}

function findDevice(config, token, fetchImpl) {
  return listFirst(config, config.devices, `{device_token} = '${escapeFormula(token)}'`, fetchImpl);
}

async function listFirst(config, table, formula, fetchImpl) {
  const url = tableUrl(config.baseId, table);
  url.searchParams.set("maxRecords", "1");
  url.searchParams.set("filterByFormula", formula);
  const result = await airtableFetch(url, { headers: headers(config.token) }, fetchImpl);
  return result.records?.[0] || null;
}

function getRecord(config, table, id, fetchImpl) {
  return airtableFetch(`${tableUrl(config.baseId, table)}/${encodeURIComponent(id)}`, { headers: headers(config.token) }, fetchImpl);
}

async function createRecord(config, table, fields, fetchImpl) {
  const result = await airtableFetch(tableUrl(config.baseId, table), { method: "POST", headers: headers(config.token), body: JSON.stringify({ records: [{ fields }] }) }, fetchImpl);
  if (!result.records?.[0]?.id) throw new RecognitionActionError("airtable_create_failed", 502);
  return result.records[0];
}

async function updateRecord(config, table, id, fields, fetchImpl) {
  const result = await airtableFetch(tableUrl(config.baseId, table), { method: "PATCH", headers: headers(config.token), body: JSON.stringify({ records: [{ id, fields }] }) }, fetchImpl);
  if (!result.records?.[0]?.id) throw new RecognitionActionError("airtable_update_failed", 502);
  return result.records[0];
}

async function airtableFetch(url, options, fetchImpl) {
  const response = await fetchImpl(url, options);
  const result = await response.json().catch(() => ({}));
  if (!response.ok) throw new RecognitionActionError("airtable_request_failed", 502, JSON.stringify(result));
  return result;
}

function normalizeInput(value) {
  const input = value && typeof value === "object" ? { ...value } : {};
  input.action = required(input.action, "missing_action");
  if (!ACTIONS.has(input.action)) throw new RecognitionActionError("unsupported_action", 400);
  input.session_uid = required(input.session_uid, "missing_session_uid");
  input.session_event_uid = required(input.session_event_uid, "missing_session_event_uid");
  return input;
}

function profileInput(input) {
  const user = required(input.user, "missing_user");
  const sms = phoneNumber(required(input.sms, "missing_sms"));
  return {
    user,
    first: clean(input.first),
    last: clean(input.last),
    sms,
    pin: memberPin(input.pin, sms),
    email: emailAddress(input.email)
  };
}

function publicPerson(person) {
  const f = person.fields || {};
  return { person_record_id: person.id, person_uid: clean(f.person_uid), person_name: clean(f.person_name), first_name: clean(f.first_name), last_name: clean(f.last_name), primary_phone_e164: clean(f.primary_phone_e164), member_pin: clean(f.member_pin) || clean(f.primary_phone_e164).slice(-4), email: clean(f.email), access_level: selectName(f.access_level) };
}

function getConfig(env) {
  const token = clean(env?.AIRTABLE_TOKEN);
  const baseId = clean(env?.AIRTABLE_RS_RECOGNITION_BASE_ID) || DEFAULT_RECOGNITION_BASE_ID;
  if (!token) throw new RecognitionActionError("missing_airtable_token", 500);
  if (!baseId) throw new RecognitionActionError("missing_airtable_base_id", 500);
  return { token, baseId, people: clean(env?.AIRTABLE_RS_PEOPLE_TEST_TABLE) || "rs_people_test", devices: clean(env?.AIRTABLE_RS_DEVICES_TEST_TABLE) || "rs_devices_test", aliases: clean(env?.AIRTABLE_RS_PHONE_ALIASES_TEST_TABLE) || "rs_phone_aliases_test" };
}

function emailAddress(value) {
  const email = clean(value).toLowerCase();
  if (!email) return "";
  if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) throw new RecognitionActionError("invalid_email", 400);
  return email;
}
function phoneNumber(value) {
  const digits = clean(value).replace(/\D/g, "");
  const normalized = digits.length === 10 ? `1${digits}` : digits;
  if (!/^1\d{10}$/.test(normalized)) throw new RecognitionActionError("invalid_sms", 400);
  return `+${normalized}`;
}
function memberPin(value, phone) {
  const raw = clean(value);
  if (!raw) return phone.slice(-4);
  const digits = raw.replace(/\D/g, "");
  if (!/^\d{4}$/.test(digits)) throw new RecognitionActionError("invalid_pin", 400);
  return digits;
}
function recordId(value, code) {
  const id = required(value, code);
  if (!/^rec[A-Za-z0-9]{14}$/.test(id)) throw new RecognitionActionError("invalid_record_id", 400);
  return id;
}
function required(value, code) { const text = clean(value); if (!text) throw new RecognitionActionError(code, 400); return text.slice(0, 255); }
function clean(value) { return value === undefined || value === null ? "" : String(value).trim(); }
function selectName(value) { return typeof value === "string" ? value : clean(value?.name); }
function isActive(value) { return ["active", "test"].includes(selectName(value).toLowerCase()); }
function isMemberAccess(value) { return ["admin", "user", "member"].includes(selectName(value).toLowerCase()); }
function firstLink(value) { if (!Array.isArray(value) || !value.length) return ""; return typeof value[0] === "string" ? value[0] : clean(value[0]?.id); }
function tableUrl(baseId, table) { return new URL(`https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(table)}`); }
function headers(token) { return { Authorization: `Bearer ${token}`, "Content-Type": "application/json" }; }
function escapeFormula(value) { return String(value).replace(/\\/g, "\\\\").replace(/'/g, "\\'"); }
function makeUid(prefix) { const id = typeof crypto.randomUUID === "function" ? crypto.randomUUID().replace(/-/g, "") : `${Date.now().toString(36)}${Math.random().toString(36).slice(2)}`; return `${prefix}_${id}`; }
