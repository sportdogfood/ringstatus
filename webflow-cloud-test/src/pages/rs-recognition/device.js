export const config = {
  runtime: "edge"
};

import { env } from "cloudflare:workers";

const DEFAULT_DEVICES_TABLE = "rs_devices_test";
const DEFAULT_PEOPLE_TABLE = "rs_people_test";
const DEFAULT_RECOGNITION_BASE_ID = "apptdhhNzduxm5gjn";
const ALLOWED_DEVICE_STATUSES = new Set(["active", "test"]);
const ALLOWED_PERSON_STATUSES = new Set(["active", "test"]);
const ALLOWED_ACCESS_LEVELS = new Set(["admin", "user", "member"]);

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET,OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type,Authorization"
};

export const OPTIONS = async () => new Response(null, { status: 204, headers: corsHeaders });

export const GET = async ({ request }) => {
  const airtable = getAirtableConfig();
  if (!airtable.ok) return json({ ok: false, recognized: false, error: airtable.error }, 500);

  const url = new URL(request.url);
  const deviceToken = String(url.searchParams.get("device_token") || "").trim();

  if (!deviceToken) {
    return json({ ok: false, recognized: false, error: "missing_device_token" }, 400);
  }

  try {
    const device = await findDeviceByToken(airtable, deviceToken);

    if (!device) {
      return json({
        ok: true,
        recognized: false,
        recognition_status: "unknown_device",
        matched_by: "none"
      });
    }

    const deviceStatus = selectName(device.fields?.status);
    const normalizedDeviceStatus = deviceStatus.toLowerCase();
    if (!ALLOWED_DEVICE_STATUSES.has(normalizedDeviceStatus)) {
      return json({
        ok: true,
        recognized: false,
        recognition_status: "device_not_active",
        matched_by: "device_token",
        device_record_id: device.id,
        device_uid: device.fields?.device_uid || "",
        device_status: deviceStatus || ""
      });
    }

    const personRecordId = firstLinkedRecordId(device.fields?.person);
    if (!personRecordId) {
      return json({
        ok: true,
        recognized: false,
        recognition_status: "device_found_no_person",
        matched_by: "device_token",
        device_record_id: device.id,
        device_uid: device.fields?.device_uid || "",
        device_status: deviceStatus || ""
      });
    }

    const person = await getAirtableRecord(airtable, airtable.peopleTable, personRecordId);
    const personStatus = selectName(person.fields?.status);
    const accessLevel = selectName(person.fields?.access_level);

    if (!ALLOWED_PERSON_STATUSES.has(personStatus.toLowerCase())) {
      return json({
        ok: true,
        recognized: false,
        recognition_status: "person_not_active",
        matched_by: "device_token",
        device_record_id: device.id,
        device_uid: device.fields?.device_uid || "",
        device_status: deviceStatus || "",
        person_record_id: person.id,
        person_status: personStatus || ""
      });
    }

    if (!ALLOWED_ACCESS_LEVELS.has(accessLevel.toLowerCase())) {
      return json({
        ok: true,
        recognized: false,
        recognition_status: "access_not_allowed",
        matched_by: "device_token",
        device_record_id: device.id,
        device_uid: device.fields?.device_uid || "",
        device_status: deviceStatus || "",
        person_record_id: person.id,
        person_status: personStatus || "",
        access_level: accessLevel || ""
      });
    }

    return json({
      ok: true,
      recognized: true,
      recognition_status: "known_device",
      matched_by: "device_token",
      device_record_id: device.id,
      device_uid: device.fields?.device_uid || "",
      device_status: deviceStatus || "",
      person_record_id: person.id,
      person_name: person.fields?.person_name || "",
      person_uid: person.fields?.person_uid || "",
      first_name: person.fields?.first_name || "",
      last_name: person.fields?.last_name || "",
      primary_phone_e164: person.fields?.primary_phone_e164 || "",
      member_pin: person.fields?.member_pin || String(person.fields?.primary_phone_e164 || "").slice(-4),
      email: person.fields?.email || "",
      person_status: personStatus || "",
      access_level: accessLevel || ""
    });
  } catch (error) {
    console.error("[rs-recognition] device lookup failed", error);
    return json({
      ok: false,
      recognized: false,
      error: "device_lookup_failed",
      detail: error instanceof Error ? error.message : String(error)
    }, 502);
  }
};

function getAirtableConfig() {
  const token = env.AIRTABLE_TOKEN;
  const baseId = env.AIRTABLE_RS_RECOGNITION_BASE_ID || DEFAULT_RECOGNITION_BASE_ID;
  const devicesTable = env.AIRTABLE_RS_DEVICES_TEST_TABLE || DEFAULT_DEVICES_TABLE;
  const peopleTable = env.AIRTABLE_RS_PEOPLE_TEST_TABLE || DEFAULT_PEOPLE_TABLE;

  if (!token) return { ok: false, error: "missing_airtable_token" };
  if (!baseId) return { ok: false, error: "missing_airtable_base_id" };

  return {
    ok: true,
    token,
    baseId,
    devicesTable,
    peopleTable
  };
}

async function findDeviceByToken(airtable, deviceToken) {
  const formula = `{device_token} = '${escapeAirtableString(deviceToken)}'`;
  const url = airtableUrl(airtable.baseId, airtable.devicesTable);
  url.searchParams.set("maxRecords", "1");
  url.searchParams.set("filterByFormula", formula);

  const response = await fetch(url, { headers: airtableHeaders(airtable.token) });
  const result = await response.json().catch(() => ({}));

  if (!response.ok) {
    throw new Error(`device list ${response.status}: ${JSON.stringify(result)}`);
  }

  return result.records?.[0] || null;
}

async function getAirtableRecord(airtable, table, recordId) {
  const response = await fetch(`${airtableUrl(airtable.baseId, table)}/${encodeURIComponent(recordId)}`, {
    headers: airtableHeaders(airtable.token)
  });
  const result = await response.json().catch(() => ({}));

  if (!response.ok) {
    throw new Error(`record get ${table}/${recordId} ${response.status}: ${JSON.stringify(result)}`);
  }

  return result;
}

function airtableUrl(baseId, table) {
  return new URL(`https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(table)}`);
}

function airtableHeaders(token) {
  return {
    Authorization: `Bearer ${token}`
  };
}

function firstLinkedRecordId(value) {
  if (!Array.isArray(value) || !value.length) return "";
  const first = value[0];
  if (typeof first === "string") return first;
  if (first && typeof first === "object") return first.id || "";
  return "";
}

function selectName(value) {
  if (!value) return "";
  if (typeof value === "string") return value;
  if (value && typeof value === "object" && typeof value.name === "string") return value.name;
  return String(value);
}

function escapeAirtableString(value) {
  return String(value).replace(/\\/g, "\\\\").replace(/'/g, "\\'");
}

function json(body, status = 200) {
  return new Response(JSON.stringify(body, null, 2), {
    status,
    headers: {
      ...corsHeaders,
      "Content-Type": "application/json; charset=utf-8"
    }
  });
}
