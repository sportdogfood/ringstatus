import { runRecognitionAction } from "./rs-recognition-action.js";

const INFRASTRUCTURE_BASE = "appZahVgD156cMAe3";
const EXISTING_RECOGNITION_BASE = "apptdhhNzduxm5gjn";
const ACTIONS = new Set(["create_profile", "update_profile", "phone_login", "confirm_device", "retire_device", "recovery"]);
const clean = (value) => value == null ? "" : String(value).trim();
const choice = (value) => clean(typeof value === "object" ? value?.name : value).toLowerCase();
const active = (value) => ["active", "test"].includes(choice(value));
const formulaValue = (value) => value.replace(/\\/g, "\\\\").replace(/'/g, "\\'");

export class InputRecognitionError extends Error {
  constructor(code, status) {
    super(code);
    this.name = "InputRecognitionError";
    this.code = code;
    this.status = status;
  }
}

function fail(code, status = 400) { throw new InputRecognitionError(code, status); }
function bounded(value, code, max = 255) {
  if (typeof value !== "string" || !value.trim() || value.trim().length > max) fail(code);
  return value.trim();
}
function result(profile = null, device = "unknown") {
  return { ok: true, recognized: !!profile, profile, device };
}
function profileOf(person) {
  const fields = person.fields || {};
  return {
    person_uid: clean(fields.person_uid), person_name: clean(fields.person_name),
    first_name: clean(fields.first_name), last_name: clean(fields.last_name),
    sms: clean(fields.primary_phone_e164),
    pin: clean(fields.member_pin) || clean(fields.primary_phone_e164).slice(-4),
    email: clean(fields.email)
  };
}

// This adapter supplies recognition context only. The route must independently
// establish a trusted actor before exposing lookup data or accepting mutations.
export function createInputRecognition({ env, fetchImpl = fetch, principalPersonUid, verifiedInputAccess = false }) {
  const recognitionBaseId = clean(env?.RS_INPUTS_RECOGNITION_BASE_ID);
  const baseId = recognitionBaseId || clean(env?.RS_INPUTS_BASE_ID);
  if (!baseId) fail("missing_rs_inputs_base_id", 503);
  // Existing recognition tables are an explicitly selected interim source;
  // they must never become the implicit operational-input storage fallback.
  if (baseId === INFRASTRUCTURE_BASE || (!recognitionBaseId && baseId === EXISTING_RECOGNITION_BASE)) fail("unsafe_rs_inputs_base_id", 503);
  if (!/^app[A-Za-z0-9]{14}$/.test(baseId)) fail("invalid_rs_inputs_base_id", 503);
  const token = clean(env?.AIRTABLE_TOKEN);
  if (!token) fail("missing_airtable_token", 503);
  const principal = clean(principalPersonUid);
  // Deliberately do not inherit the old recognition base or table overrides.
  const tables = {
    people: "rs_people_test", devices: "rs_devices_test",
    aliases: baseId === "app9kOZdIaGyKk5uG" ? "tblgDWKi0Bb6OcoqS" : "rs_phone_aliases_test", sessions: "rs_recognition_sessions_test"
  };
  const actionEnv = {
    RS_RECOGNITION_CONTROL_DB: env.RS_RECOGNITION_CONTROL_DB,
    AIRTABLE_TOKEN: token,
    AIRTABLE_RS_RECOGNITION_BASE_ID: baseId,
    AIRTABLE_RS_PEOPLE_TEST_TABLE: tables.people,
    AIRTABLE_RS_DEVICES_TEST_TABLE: tables.devices,
    AIRTABLE_RS_PHONE_ALIASES_TEST_TABLE: tables.aliases,
    AIRTABLE_RS_RECOGNITION_SESSIONS_TEST_TABLE: tables.sessions,
    RS_RECOGNITION_SIGNAL_SECRET: clean(env?.RS_RECOGNITION_SIGNAL_SECRET)
  };
  const tableUrl = (table) => `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(table)}`;
  async function read(url) {
    const response = await fetchImpl(url, { headers: { Authorization: `Bearer ${token}` } });
    const body = await response.json().catch(() => null);
    if (!response.ok) fail("recognition_lookup_failed", 502);
    if (!body || typeof body !== "object") fail("invalid_recognition_response", 502);
    return body;
  }
  async function unique(table, formula, ambiguityCode) {
    const url = new URL(tableUrl(table));
    url.searchParams.set("maxRecords", "2");
    url.searchParams.set("filterByFormula", formula);
    const body = await read(url);
    if (!Array.isArray(body.records)) fail("invalid_recognition_response", 502);
    if (body.records.length > 1 || body.offset) fail(ambiguityCode, 409);
    return body.records[0] || null;
  }
  const eligible = person => active(person.fields?.status) && (verifiedInputAccess ? ["invited", "approved"].includes(person.fields?.input_access) : ["admin", "user", "member"].includes(choice(person.fields?.access_level)));
  async function invitedPerson() {
    const person = await unique(tables.people, `{person_uid} = '${formulaValue(principal)}'`, "ambiguous_person");
    assertPrincipal(person);
    return person;
  }
  function assertPrincipal(person) {
    if (!person || clean(person.fields?.person_uid) !== principal) fail("verified_profile_required", 403);
    if (!eligible(person)) fail("unauthorized_person", 403);
  }
  function normalizedPhone(value) {
    const digits = bounded(value, "missing_sms").replace(/\D/g, "");
    const normalized = digits.length === 10 ? `1${digits}` : digits;
    if (!/^1\d{10}$/.test(normalized)) fail("invalid_sms");
    return `+${normalized}`;
  }
  async function phoneCandidate(value) {
    const phone = normalizedPhone(value);
    const digits = phone.slice(1);
    const direct = await unique(tables.people, `OR({primary_phone_e164} = '${formulaValue(phone)}',{primary_phone_e164} = '${formulaValue(digits)}')`, "ambiguous_phone");
    // Check aliases even when a direct person exists: the old action would
    // otherwise use the first person and overlook a conflicting alias owner.
    const alias = await unique(tables.aliases, `OR({alias_phone_e164} = '${formulaValue(phone)}',{alias_phone_e164} = '${formulaValue(digits)}')`, "ambiguous_phone_alias");
    if (!alias) return direct;
    const links = alias.fields?.person;
    if (!Array.isArray(links) || links.length !== 1) fail("ambiguous_phone_alias", 409);
    const aliasPersonId = clean(typeof links[0] === "string" ? links[0] : links[0]?.id);
    if (!/^rec[A-Za-z0-9]{14}$/.test(aliasPersonId)) fail("invalid_recognition_response", 502);
    if (direct && direct.id !== aliasPersonId) fail("conflicting_phone_alias", 409);
    if (direct) return direct;
    const person = await read(`${tableUrl(tables.people)}/${aliasPersonId}`);
    if (person.id !== aliasPersonId || !person.fields) fail("invalid_recognition_response", 502);
    return person;
  }
  async function loginCandidate(value) {
    const identifier = bounded(value, "missing_sms");
    const digits = identifier.replace(/\D/g, "");
    return digits.length === 4
      ? unique(tables.people, `OR({member_pin} = '${formulaValue(digits)}',RIGHT({primary_phone_e164},4) = '${formulaValue(digits)}')`, "ambiguous_pin")
      : phoneCandidate(identifier);
  }
  async function resolve(deviceToken, { includeRetiredOwner = false } = {}) {
    if (deviceToken == null || deviceToken === "") return { response: result(), person: null };
    const value = bounded(deviceToken, "invalid_device_token");
    const url = new URL(tableUrl(tables.devices));
    url.searchParams.set("maxRecords", "2");
    url.searchParams.set("filterByFormula", `{device_token} = '${formulaValue(value)}'`);
    const body = await read(url);
    if (!Array.isArray(body.records)) fail("invalid_recognition_response", 502);
    if (body.records.length > 1 || body.offset) fail("ambiguous_device", 409);
    const device = body.records[0];
    if (!device) return { response: result(), person: null };
    const retired = choice(device.fields?.status) === "retired";
    if (!active(device.fields?.status) && !(retired && includeRetiredOwner)) return { response: result(null, retired ? "retired" : "unknown"), person: null, existingDevice: true };
    const links = device.fields?.person;
    if (!Array.isArray(links) || !links.length) return { response: result(), person: null, existingDevice: true };
    if (links.length > 1) fail("ambiguous_device_person", 409);
    const personId = clean(typeof links[0] === "string" ? links[0] : links[0]?.id);
    if (!/^rec[A-Za-z0-9]{14}$/.test(personId)) fail("invalid_recognition_response", 502);
    const person = await read(`${tableUrl(tables.people)}/${personId}`);
    if (person.id !== personId || !person.fields) fail("invalid_recognition_response", 502);
    if (!eligible(person)) return { response: result(), person: null, existingDevice: true };
    if (!clean(person.fields.person_uid)) fail("invalid_recognition_response", 502);
    return { response: retired ? result(null, "retired") : result(profileOf(person), "active"), person, existingDevice: true, deviceRecordId: device.id };
  }
  return {
    // Server-only lookup for managed phone verification; never return it publicly.
    async verificationCandidate(value) {
      const person = await phoneCandidate(value);
      return person && eligible(person) ? { personUid: person.fields.person_uid, phone: normalizedPhone(value) } : null;
    },
    // Recognition context for the public launcher, never an Inputs actor or grant.
    async recognizeDevice(deviceToken) {
      const resolved = await resolve(deviceToken);
      return { ...resolved.response, personRecordId: resolved.person?.id || '', deviceRecordId: resolved.deviceRecordId || '' };
    },
    async lookup(deviceToken) {
      if (deviceToken != null && deviceToken !== "" && !principal) fail("verified_profile_required", 503);
      const resolved = await resolve(deviceToken);
      if (resolved.person) assertPrincipal(resolved.person);
      if (verifiedInputAccess && !resolved.existingDevice && !resolved.person) return { ok: true, recognized: false, device: "unknown", profile: profileOf(await invitedPerson()) };
      return resolved.response;
    },
    async action(payload, request) {
      if (clean(env?.RS_INPUTS_WRITE_MODE) !== "isolated-trial") fail("recognition_writes_disabled", 503);
      if (!principal) fail("verified_profile_required", 503);
      const action = clean(payload?.action);
      if (!ACTIONS.has(action)) fail("unsupported_action");
      if (action === "recovery") fail("recovery_delivery_unconfigured", 503);
      if (action === "create_profile") fail("profile_creation_unconfigured", 503);
      const deviceToken = bounded(payload?.device_token, "missing_device_token");
      const requestId = bounded(payload?.requestId, "invalid_request_id", 128);
      const values = payload?.values;
      if (!values || typeof values !== "object" || Array.isArray(values) || Object.values(values).some((value) => typeof value !== "string")) fail("invalid_recognition_values");
      // Preflight all actions so a duplicate device token never silently picks
      // the first device in the reused legacy action implementation.
      // Retirement may have committed before its audit failed. On that one
      // action, retain the retired device's owner internally to authorize retry.
      const current = await resolve(deviceToken, { includeRetiredOwner: action === "retire_device" });
      if (verifiedInputAccess && action === "confirm_device" && !current.existingDevice && !current.person) current.person = await invitedPerson();
      if (["update_profile", "confirm_device", "retire_device"].includes(action)) {
        if (!current.person) fail("unauthorized_device", 403);
        assertPrincipal(current.person);
      }
      if (action === "phone_login") {
        // A recognized device belonging to someone else cannot be rebound.
        if (current.existingDevice && !current.person) fail("unauthorized_device", 403);
        if (current.person) assertPrincipal(current.person);
        const candidate = await loginCandidate(values.identifier ?? values.login ?? values.sms);
        if (!candidate) return result();
        assertPrincipal(candidate);
      }
      if (action === "update_profile") {
        const owner = await phoneCandidate(values.sms);
        if (owner && owner.id !== current.person.id) fail("phone_already_registered", 409);
      }
      const input = {
        action, device_token: deviceToken,
        session_uid: `inputs_${requestId}`, session_event_uid: `inputs_${requestId}`,
        user: values.person_name ?? values.name ?? values.user,
        first: values.first_name ?? values.first, last: values.last_name ?? values.last,
        sms: action === "phone_login" ? (values.identifier ?? values.login ?? values.sms) : values.sms,
        pin: values.pin, email: values.email,
        page_path: request ? new URL(request.url).pathname : "",
        // Client-supplied person IDs never select the record being changed.
        person_record_id: current.person?.id,
        person_uid: current.person?.fields?.person_uid
      };
      try {
        const actionResult = await runRecognitionAction({ env: actionEnv, fetchImpl, payload: input, request, ...(verifiedInputAccess ? { verifiedInputPersonUid: principal } : {}) });
        if (action === "retire_device") return result(null, "retired");
        if (action === "phone_login" && !actionResult.recognized) return result();
        return (await resolve(deviceToken)).response;
      } catch (error) {
        // Do not return raw upstream Airtable payloads through the new API.
        if (error?.code && Number.isInteger(error.status)) throw new InputRecognitionError(error.code, error.status);
        throw new InputRecognitionError("recognition_action_failed", 502);
      }
    }
  };
}
