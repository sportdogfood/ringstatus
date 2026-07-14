import { recordRecognitionSession } from "./rs-recognition-session.js";

const DEFAULT_PEOPLE_TABLE = "rs_people_test";
const DEFAULT_DEVICES_TABLE = "rs_devices_test";
const DEFAULT_ALIASES_TABLE = "rs_phone_aliases_test";
const ALLOWED_ACTIONS = new Set([
  "create_profile",
  "update_profile",
  "phone_login",
  "recovery",
  "confirm_device",
  "retire_device"
]);

export class RecognitionIdentityError extends Error {
  constructor(code, status, detail = "") {
    super(detail || code);
    this.name = "RecognitionIdentityError";
    this.code = code;
    this.status = status;
  }
}

export async function performRecognitionAction({
  env,
  fetchImpl = fetch,
  payload,
  request,
  recordSession = recordRecognitionSession
}) {
  const config = airtableConfig(env);
  const input = normalizeInput(payload);
  let outcome;

  if (input.action === "create_profile") {
    outcome = await createProfile({ config, fetchImpl, input });
  } else if (input.action === "update_profile") {
    outcome = await updateProfile({ config, fetchImpl, input });
  } else if (input.action === "phone_login") {
    outcome = await phoneLogin({ config, fetchImpl, input });
  } else if (input.action === "recovery") {
    outcome = await recoverProfile({ config, fetchImpl, input });
  } else if (input.action === "confirm_device") {
    outcome = await confirmDevice({ config, fetchImpl, input });
  } else {
    outcome = await retireDevice({ config, fetchImpl, input });
  }

  await recordSession({
    env,
    fetchImpl,
    request,
    payload: sessionPayload(input, outcome)
  });

  return outcome.response;
}

async function createProfile({ config, fetchImpl, input }) {
  const profile = requiredProfile(input);
  const existing = await findPersonByPhone(config, profile.sms, fetchImpl);
  if (existing) throw new RecognitionIdentityError("phone_already_registered", 409);

  const personUid = uid("person");
  const person = await createRecord(config, config.peopleTable, {
    person_uid: personUid,
    person_name: profile.user,
    first_name: profile.first,
    last_name: profile.last,
    primary_phone_e164: profile.sms,
    email: profile.email,
    status: "Active",
    access_level: "member"
  }, fetchImpl);
  const alias = await upsertPhoneAlias(config, profile.sms, person.id, fetchImpl);
  const device = await upsertDevice(config, input.device_token, person.id, fetchImpl);

  return {
    person,
    alias,
    device,
    event_type: "new",
    event_result: "success",
    matched_by: "manual",
    recognition_status: "confirmed",
    detail: {
      changed_fields: ["person_name", "first_name", "last_name", "primary_phone_e164", "email"]
    },
    response: {
      ok: true,
      recognized: true,
      person_record_id: person.id,
      person_uid: personUid,
      person_name: profile.user,
      first_name: profile.first,
      last_name: profile.last,
      primary_phone_e164: profile.sms,
      email: profile.email,
      phone_alias_record_id: alias.id,
      device_record_id: device.id
    }
  };
}

async function updateProfile({ config, fetchImpl, input }) {
  const profile = requiredProfile(input);
  const personRecordId = requiredRecordId(input.person_record_id, "missing_person_record_id");
  const personUid = required(input.person_uid, "missing_person_uid");
  const phoneOwner = await findPersonByPhone(config, profile.sms, fetchImpl);
  if (phoneOwner && phoneOwner.id !== personRecordId) {
    throw new RecognitionIdentityError("phone_already_registered", 409);
  }
  const person = await updateRecord(config, config.peopleTable, personRecordId, {
    person_name: profile.user,
    first_name: profile.first,
    last_name: profile.last,
    primary_phone_e164: profile.sms,
    email: profile.email
  }, fetchImpl);
  const alias = await upsertPhoneAlias(config, profile.sms, personRecordId, fetchImpl);
  const device = await upsertDevice(config, input.device_token, personRecordId, fetchImpl);

  return {
    person,
    alias,
    device,
    event_type: "save",
    event_result: "success",
    matched_by: "manual",
    recognition_status: "confirmed",
    detail: {
      changed_fields: ["person_name", "first_name", "last_name", "primary_phone_e164", "email"]
    },
    response: {
      ok: true,
      recognized: true,
      person_record_id: personRecordId,
      person_uid: personUid,
      person_name: profile.user,
      first_name: profile.first,
      last_name: profile.last,
      primary_phone_e164: profile.sms,
      email: profile.email,
      phone_alias_record_id: alias.id,
      device_record_id: device.id
    }
  };
}

async function phoneLogin({ config, fetchImpl, input }) {
  const phone = normalizePhone(required(input.sms, "missing_sms"));
  if (!phone) throw new RecognitionIdentityError("invalid_sms", 400);
  const person = await findPersonByPhone(config, phone, fetchImpl);

  if (!person || !isActive(person.fields?.status)) {
    return {
      event_type: "login",
      event_result: "not_matched",
      matched_by: "phone",
      recognition_status: "rejected",
      detail: { source: "members_gate" },
      response: { ok: true, recognized: false }
    };
  }

  const device = await upsertDevice(config, input.device_token, person.id, fetchImpl);
  const publicPerson = personResponse(person);
  return {
    person,
    device,
    event_type: "login",
    event_result: "matched",
    matched_by: "phone",
    recognition_status: "confirmed",
    detail: { source: "members_gate" },
    response: {
      ok: true,
      recognized: true,
      ...publicPerson,
      device_record_id: device.id
    }
  };
}

async function recoverProfile({ config, fetchImpl, input }) {
  const first = clean(input.first);
  const last = clean(input.last);
  const email = normalizeEmail(input.email);
  if (!(first && last) && !email) {
    throw new RecognitionIdentityError("missing_recovery_identity", 400);
  }
  const person = await findPersonForRecovery(config, { first, last, email }, fetchImpl);

  return {
    person,
    event_type: "recovery",
    event_result: person ? "matched" : "not_matched",
    matched_by: "manual",
    recognition_status: "pending",
    detail: person
      ? { automation_action: "send_member_link", return_to: "/" }
      : { automation_action: "none", return_to: "/" },
    response: { ok: true, accepted: true, return_to: "/" }
  };
}

async function confirmDevice({ config, fetchImpl, input }) {
  const personRecordId = requiredRecordId(input.person_record_id, "missing_person_record_id");
  const personUid = required(input.person_uid, "missing_person_uid");
  const device = await upsertDevice(config, input.device_token, personRecordId, fetchImpl);
  return {
    person: { id: personRecordId },
    device,
    event_type: "visit",
    event_result: "success",
    matched_by: "device_token",
    recognition_status: "confirmed",
    detail: { source: clean(input.source) || "confirmed_identity" },
    response: {
      ok: true,
      confirmed: true,
      person_record_id: personRecordId,
      person_uid: personUid,
      device_record_id: device.id
    }
  };
}

async function retireDevice({ config, fetchImpl, input }) {
  const token = required(input.device_token, "missing_device_token");
  const device = await findDeviceByToken(config, token, fetchImpl);
  if (!device) {
    return {
      event_type: "device_retired",
      event_result: "success",
      matched_by: "device_token",
      recognition_status: "rejected",
      detail: { source: "not_you", already_absent: true },
      response: { ok: true, retired: true, device_record_id: "" }
    };
  }
  const updated = await updateRecord(config, config.devicesTable, device.id, {
    status: "Retired",
    last_seen_at: new Date().toISOString()
  }, fetchImpl);
  return {
    device: updated,
    event_type: "device_retired",
    event_result: "success",
    matched_by: "device_token",
    recognition_status: "rejected",
    detail: { source: "not_you" },
    response: { ok: true, retired: true, device_record_id: device.id }
  };
}

function sessionPayload(input, outcome) {
  return {
    session_event_uid: input.session_event_uid,
    session_uid: input.session_uid,
    event_type: outcome.event_type,
    event_result: outcome.event_result,
    idempotency_key: `${input.action}:${input.session_event_uid}`,
    event_at: new Date().toISOString(),
    person_record_id: outcome.person?.id || "",
    device_record_id: outcome.device?.id || "",
    phone_alias_record_id: outcome.alias?.id || "",
    matched_by: outcome.matched_by,
    recognition_status: outcome.recognition_status,
    client_timezone: input.client_timezone,
    viewport_width: input.viewport_width,
    page_path: input.page_path,
    referrer: input.referrer,
    detail: outcome.detail
  };
}

async function findPersonByPhone(config, phone, fetchImpl) {
  const digits = phone.replace(/^\+/, "");
  const formula = `OR({primary_phone_e164} = '${escapeAirtableString(phone)}', {primary_phone_e164} = '${escapeAirtableString(digits)}')`;
  const direct = await listFirst(config, config.peopleTable, formula, fetchImpl);
  if (direct) return direct;

  const aliasFormula = `OR({alias_phone_e164} = '${escapeAirtableString(phone)}', {alias_phone_e164} = '${escapeAirtableString(digits)}')`;
  const alias = await listFirst(config, config.aliasesTable, aliasFormula, fetchImpl);
  const personId = firstLinkedRecordId(alias?.fields?.person);
  return personId ? getRecord(config, config.peopleTable, personId, fetchImpl) : null;
}

async function findPersonForRecovery(config, identity, fetchImpl) {
  const parts = [];
  if (identity.email) parts.push(`LOWER({email}) = '${escapeAirtableString(identity.email.toLowerCase())}'`);
  if (identity.first && identity.last) {
    parts.push(`AND(LOWER({first_name}) = '${escapeAirtableString(identity.first.toLowerCase())}', LOWER({last_name}) = '${escapeAirtableString(identity.last.toLowerCase())}')`);
  }
  return listFirst(config, config.peopleTable, parts.length === 1 ? parts[0] : `OR(${parts.join(",")})`, fetchImpl);
}

async function upsertPhoneAlias(config, phone, personRecordId, fetchImpl) {
  const digits = phone.replace(/^\+/, "");
  const formula = `OR({alias_phone_e164} = '${escapeAirtableString(phone)}', {alias_phone_e164} = '${escapeAirtableString(digits)}')`;
  const existing = await listFirst(config, config.aliasesTable, formula, fetchImpl);
  if (existing) {
    const existingPerson = firstLinkedRecordId(existing.fields?.person);
    if (existingPerson && existingPerson !== personRecordId) {
      throw new RecognitionIdentityError("phone_already_registered", 409);
    }
    return updateRecord(config, config.aliasesTable, existing.id, {
      alias_phone_e164: phone,
      person: [personRecordId],
      alias_type: "Mobile",
      status: "Active"
    }, fetchImpl);
  }
  return createRecord(config, config.aliasesTable, {
    alias_uid: uid("phone_alias"),
    alias_phone_e164: phone,
    person: [personRecordId],
    alias_type: "Mobile",
    status: "Active"
  }, fetchImpl);
}

async function upsertDevice(config, deviceToken, personRecordId, fetchImpl) {
  const token = required(deviceToken, "missing_device_token");
  const existing = await findDeviceByToken(config, token, fetchImpl);
  const fields = {
    device_uid: clean(existing?.fields?.device_uid) || uid("device"),
    device_token: token,
    person: [personRecordId],
    status: "Active",
    last_seen_at: new Date().toISOString(),
    recognition_source: "Web"
  };
  return existing
    ? updateRecord(config, config.devicesTable, existing.id, fields, fetchImpl)
    : createRecord(config, config.devicesTable, fields, fetchImpl);
}

function findDeviceByToken(config, token, fetchImpl) {
  const formula = `{device_token} = '${escapeAirtableString(token)}'`;
  return listFirst(config, config.devicesTable, formula, fetchImpl);
}

async function listFirst(config, table, formula, fetchImpl) {
  const url = airtableUrl(config.baseId, table);
  url.searchParams.set("maxRecords", "1");
  url.searchParams.set("filterByFormula", formula);
  const result = await airtableRequest(url, { headers: airtableHeaders(config.token) }, fetchImpl);
  return result.records?.[0] || null;
}

async function getRecord(config, table, recordId, fetchImpl) {
  const url = `${airtableUrl(config.baseId, table)}/${encodeURIComponent(recordId)}`;
  return airtableRequest(url, { headers: airtableHeaders(config.token) }, fetchImpl);
}

async function createRecord(config, table, fields, fetchImpl) {
  const result = await airtableRequest(airtableUrl(config.baseId, table), {
    method: "POST",
    headers: airtableHeaders(config.token),
    body: JSON.stringify({ records: [{ fields }], typecast: true })
  }, fetchImpl);
  const record = result.records?.[0];
  if (!record?.id) throw new RecognitionIdentityError("airtable_record_create_failed", 502);
  return record;
}

async function updateRecord(config, table, recordId, fields, fetchImpl) {
  const result = await airtableRequest(airtableUrl(config.baseId, table), {
    method: "PATCH",
    headers: airtableHeaders(config.token),
    body: JSON.stringify({ records: [{ id: recordId, fields }], typecast: true })
  }, fetchImpl);
  const record = result.records?.[0];
  if (!record?.id) throw new RecognitionIdentityError("airtable_record_update_failed", 502);
  return record;
}

async function airtableRequest(url, options, fetchImpl) {
  const response = await fetchImpl(url, options);
  const result = await response.json().catch(() => ({}));
  if (!response.ok) {
    throw new RecognitionIdentityError(
      "airtable_request_failed",
      502,
      `Airtable ${options.method || "GET"} ${response.status}: ${JSON.stringify(result)}`
    );
  }
  return result;
}

function normalizeInput(payload) {
  const input = payload && typeof payload === "object" ? { ...payload } : {};
  input.action = required(input.action, "missing_action");
  if (!ALLOWED_ACTIONS.has(input.action)) throw new RecognitionIdentityError("unsupported_action", 400);
  input.session_uid = required(input.session_uid, "missing_session_uid");
  input.session_event_uid = required(input.session_event_uid, "missing_session_event_uid");
  return input;
}

function requiredProfile(input) {
  const user = required(input.user, "missing_user");
  const smsText = required(input.sms, "missing_sms");
  const sms = normalizePhone(smsText);
  if (!sms) throw new RecognitionIdentityError("invalid_sms", 400);
  const email = normalizeEmail(input.email);
  if (clean(input.email) && !email) throw new RecognitionIdentityError("invalid_email", 400);
  return {
    user,
    first: clean(input.first),
    last: clean(input.last),
    sms,
    email
  };
}

function personResponse(person) {
  const fields = person.fields || {};
  return {
    person_record_id: person.id,
    person_uid: clean(fields.person_uid),
    person_name: clean(fields.person_name),
    first_name: clean(fields.first_name),
    last_name: clean(fields.last_name),
    primary_phone_e164: clean(fields.primary_phone_e164),
    email: clean(fields.email),
    person_status: selectName(fields.status),
    access_level: selectName(fields.access_level)
  };
}

function airtableConfig(env) {
  const token = clean(env?.AIRTABLE_TOKEN);
  const baseId = clean(env?.AIRTABLE_BASE_ID || env?.AIRTABLE_BASE);
  if (!token) throw new RecognitionIdentityError("missing_airtable_token", 500);
  if (!baseId) throw new RecognitionIdentityError("missing_airtable_base_id", 500);
  return {
    token,
    baseId,
    peopleTable: clean(env?.AIRTABLE_RS_PEOPLE_TEST_TABLE) || DEFAULT_PEOPLE_TABLE,
    devicesTable: clean(env?.AIRTABLE_RS_DEVICES_TEST_TABLE) || DEFAULT_DEVICES_TABLE,
    aliasesTable: clean(env?.AIRTABLE_RS_PHONE_ALIASES_TEST_TABLE) || DEFAULT_ALIASES_TABLE
  };
}

function airtableUrl(baseId, table) {
  return new URL(`https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(table)}`);
}

function airtableHeaders(token) {
  return { Authorization: `Bearer ${token}`, "Content-Type": "application/json" };
}

function normalizePhone(value) {
  const digits = clean(value).replace(/\D/g, "");
  const e164 = digits.length === 10 ? `1${digits}` : digits;
  return /^1\d{10}$/.test(e164) ? `+${e164}` : "";
}

function normalizeEmail(value) {
  const email = clean(value).toLowerCase();
  if (!email) return "";
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email) ? email : "";
}

function required(value, code) {
  const text = clean(value);
  if (!text) throw new RecognitionIdentityError(code, 400);
  return text.slice(0, 255);
}

function requiredRecordId(value, code) {
  const text = required(value, code);
  if (!/^rec[A-Za-z0-9]{14}$/.test(text)) throw new RecognitionIdentityError("invalid_record_id", 400);
  return text;
}

function firstLinkedRecordId(value) {
  if (!Array.isArray(value) || !value.length) return "";
  const first = value[0];
  return typeof first === "string" ? first : clean(first?.id);
}

function selectName(value) {
  if (!value) return "";
  if (typeof value === "string") return value;
  return clean(value.name);
}

function isActive(value) {
  return ["active", "test"].includes(selectName(value).toLowerCase());
}

function clean(value) {
  return value === undefined || value === null ? "" : String(value).trim();
}

function uid(prefix) {
  const value = typeof crypto.randomUUID === "function"
    ? crypto.randomUUID().replace(/-/g, "")
    : `${Date.now().toString(36)}${Math.random().toString(36).slice(2)}`;
  return `${prefix}_${value}`;
}

function escapeAirtableString(value) {
  return String(value).replace(/\\/g, "\\\\").replace(/'/g, "\\'");
}
