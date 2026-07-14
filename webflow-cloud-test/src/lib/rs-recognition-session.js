const DEFAULT_SESSIONS_TABLE = "rs_recognition_sessions_test";
const SIGNAL_VERSION = 1;

export class RecognitionSessionError extends Error {
  constructor(code, status, detail = "") {
    super(detail || code);
    this.name = "RecognitionSessionError";
    this.code = code;
    this.status = status;
  }
}

export async function recordRecognitionSession({
  env,
  fetchImpl = fetch,
  request,
  payload
}) {
  const config = airtableConfig(env);
  const event = normalizeEvent(payload);
  const tableUrl = airtableUrl(config.baseId, config.sessionsTable);
  const existing = await findByIdempotencyKey({
    config,
    event,
    fetchImpl,
    tableUrl
  });

  if (existing) {
    return {
      ok: true,
      duplicate: true,
      record_id: existing.id,
      session_event_uid: existing.fields?.session_event_uid || event.session_event_uid,
      session_uid: existing.fields?.session_uid || event.session_uid
    };
  }

  const fields = await buildAirtableFields({
    event,
    request,
    signalSecret: config.signalSecret
  });
  const response = await fetchImpl(tableUrl, {
    method: "POST",
    headers: airtableHeaders(config.token),
    body: JSON.stringify({
      records: [{ fields }],
      typecast: true
    })
  });
  const result = await response.json().catch(() => ({}));

  if (!response.ok || !result.records?.[0]?.id) {
    throw new RecognitionSessionError(
      "session_event_create_failed",
      502,
      `Airtable create ${response.status}: ${JSON.stringify(result)}`
    );
  }

  return {
    ok: true,
    duplicate: false,
    record_id: result.records[0].id,
    session_event_uid: event.session_event_uid,
    session_uid: event.session_uid
  };
}

function airtableConfig(env) {
  const token = clean(env?.AIRTABLE_TOKEN);
  const baseId = clean(env?.AIRTABLE_BASE_ID || env?.AIRTABLE_BASE);
  const sessionsTable = clean(env?.AIRTABLE_RS_RECOGNITION_SESSIONS_TEST_TABLE) || DEFAULT_SESSIONS_TABLE;
  const signalSecret = clean(env?.RS_RECOGNITION_SIGNAL_SECRET);

  if (!token) throw new RecognitionSessionError("missing_airtable_token", 500);
  if (!baseId) throw new RecognitionSessionError("missing_airtable_base_id", 500);

  return { token, baseId, sessionsTable, signalSecret };
}

function normalizeEvent(payload) {
  const input = payload && typeof payload === "object" ? payload : {};
  const event = {
    session_event_uid: required(input.session_event_uid, "missing_session_event_uid"),
    session_uid: required(input.session_uid, "missing_session_uid"),
    event_type: required(input.event_type, "missing_event_type"),
    event_result: required(input.event_result, "missing_event_result"),
    idempotency_key: required(input.idempotency_key, "missing_idempotency_key"),
    event_at: validDate(input.event_at) || new Date().toISOString(),
    person_record_id: recordId(input.person_record_id),
    device_record_id: recordId(input.device_record_id),
    phone_alias_record_id: recordId(input.phone_alias_record_id),
    matched_by: clean(input.matched_by),
    recognition_status: clean(input.recognition_status),
    client_timezone: clean(input.client_timezone),
    viewport_width: finiteNumber(input.viewport_width),
    page_path: clean(input.page_path),
    referrer: clean(input.referrer),
    detail: input.detail
  };

  return event;
}

async function findByIdempotencyKey({ config, event, fetchImpl, tableUrl }) {
  const url = new URL(tableUrl);
  url.searchParams.set("maxRecords", "1");
  url.searchParams.set(
    "filterByFormula",
    `{idempotency_key} = '${escapeAirtableString(event.idempotency_key)}'`
  );

  const response = await fetchImpl(url, {
    headers: airtableHeaders(config.token)
  });
  const result = await response.json().catch(() => ({}));

  if (!response.ok) {
    throw new RecognitionSessionError(
      "session_idempotency_lookup_failed",
      502,
      `Airtable lookup ${response.status}: ${JSON.stringify(result)}`
    );
  }

  return result.records?.[0] || null;
}

async function buildAirtableFields({ event, request, signalSecret }) {
  const cf = request?.cf || {};
  const ip = clean(request?.headers?.get("CF-Connecting-IP"));
  const userAgent = clean(request?.headers?.get("User-Agent"));
  const network = networkPrefix(ip);
  const agent = classifyUserAgent(userAgent);
  const fields = {
    session_event_uid: event.session_event_uid,
    session_uid: event.session_uid,
    event_type: event.event_type,
    event_result: event.event_result,
    event_at: event.event_at,
    idempotency_key: event.idempotency_key,
    automation_status: "Pending",
    automation_attempt_count: 0,
    signal_version: SIGNAL_VERSION
  };

  add(fields, "person", linkValue(event.person_record_id));
  add(fields, "device", linkValue(event.device_record_id));
  add(fields, "phone_alias", linkValue(event.phone_alias_record_id));
  add(fields, "matched_by", event.matched_by);
  add(fields, "recognition_status", event.recognition_status);
  add(fields, "country_code", clean(cf.country));
  add(fields, "region", clean(cf.region));
  add(fields, "city", clean(cf.city));
  add(fields, "timezone", clean(cf.timezone));
  add(fields, "asn", clean(cf.asn));
  add(fields, "edge_colo", clean(cf.colo));
  add(fields, "browser_family", agent.browser);
  add(fields, "os_family", agent.os);
  add(fields, "device_class", agent.device);
  add(fields, "language", primaryLanguage(request?.headers?.get("Accept-Language")));
  add(fields, "client_timezone", event.client_timezone);
  add(fields, "viewport_bucket", viewportBucket(event.viewport_width, agent.device));
  add(fields, "page_path", event.page_path);
  add(fields, "referrer_host", referrerHost(event.referrer));
  add(fields, "event_detail", eventDetail(event.detail));

  if (signalSecret) {
    add(fields, "ip_hash", ip ? await hmacHex(signalSecret, `ip:${ip}`) : "");
    add(fields, "network_hash", network ? await hmacHex(signalSecret, `network:${network}`) : "");
    add(fields, "user_agent_hash", userAgent ? await hmacHex(signalSecret, `ua:${userAgent}`) : "");
  }

  return fields;
}

function airtableUrl(baseId, table) {
  return new URL(`https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(table)}`);
}

function airtableHeaders(token) {
  return {
    Authorization: `Bearer ${token}`,
    "Content-Type": "application/json"
  };
}

async function hmacHex(secret, value) {
  const encoder = new TextEncoder();
  const key = await crypto.subtle.importKey(
    "raw",
    encoder.encode(secret),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"]
  );
  const signature = await crypto.subtle.sign("HMAC", key, encoder.encode(value));
  return [...new Uint8Array(signature)].map((byte) => byte.toString(16).padStart(2, "0")).join("");
}

function networkPrefix(ip) {
  if (!ip) return "";
  const ipv4 = ip.split(".");
  if (ipv4.length === 4 && ipv4.every((part) => /^\d{1,3}$/.test(part) && Number(part) <= 255)) {
    return `${ipv4[0]}.${ipv4[1]}.${ipv4[2]}.0/24`;
  }

  const expanded = expandIpv6(ip);
  return expanded ? `${expanded.slice(0, 3).join(":")}::/48` : "";
}

function expandIpv6(ip) {
  if (!ip.includes(":")) return null;
  const [headText, tailText = ""] = ip.split("::");
  const head = headText ? headText.split(":") : [];
  const tail = tailText ? tailText.split(":") : [];
  const fill = ip.includes("::") ? 8 - head.length - tail.length : 0;
  const parts = [...head, ...Array(Math.max(fill, 0)).fill("0"), ...tail];
  if (parts.length !== 8 || parts.some((part) => !/^[a-f\d]{0,4}$/i.test(part))) return null;
  return parts.map((part) => (part || "0").padStart(4, "0").toLowerCase());
}

function classifyUserAgent(userAgent) {
  const ua = userAgent || "";
  const browser = /Edg\//.test(ua)
    ? "Edge"
    : /Chrome\//.test(ua)
      ? "Chrome"
      : /Firefox\//.test(ua)
        ? "Firefox"
        : /Safari\//.test(ua) && /Version\//.test(ua)
          ? "Safari"
          : "Unknown";
  const os = /iPhone|iPad|iPod/.test(ua)
    ? "iOS"
    : /Android/.test(ua)
      ? "Android"
      : /Windows/.test(ua)
        ? "Windows"
        : /Mac OS X/.test(ua)
          ? "macOS"
          : /Linux/.test(ua)
            ? "Linux"
            : "Unknown";
  const device = /iPad|Tablet/.test(ua)
    ? "Tablet"
    : /Mobile|iPhone|iPod|Android/.test(ua)
      ? "Mobile"
      : "Desktop";

  return { browser, os, device };
}

function viewportBucket(width, fallback) {
  if (width === null) return fallback || "Desktop";
  if (width <= 480) return "Mobile";
  if (width <= 1024) return "Tablet";
  return "Desktop";
}

function primaryLanguage(value) {
  return clean(value).split(",")[0].split(";")[0];
}

function referrerHost(value) {
  if (!value) return "";
  try {
    return new URL(value).hostname;
  } catch {
    return "";
  }
}

function eventDetail(value) {
  if (value === undefined || value === null || value === "") return "";
  const text = typeof value === "string" ? value : JSON.stringify(value);
  return text.slice(0, 10000);
}

function validDate(value) {
  const text = clean(value);
  if (!text) return "";
  const date = new Date(text);
  return Number.isNaN(date.getTime()) ? "" : date.toISOString();
}

function required(value, code) {
  const text = clean(value);
  if (!text) throw new RecognitionSessionError(code, 400);
  return text.slice(0, 255);
}

function recordId(value) {
  const text = clean(value);
  return /^rec[A-Za-z0-9]{14}$/.test(text) ? text : "";
}

function linkValue(value) {
  return value ? [value] : null;
}

function finiteNumber(value) {
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

function clean(value) {
  return value === undefined || value === null ? "" : String(value).trim();
}

function add(target, field, value) {
  if (value !== "" && value !== null && value !== undefined) target[field] = value;
}

function escapeAirtableString(value) {
  return String(value).replace(/\\/g, "\\\\").replace(/'/g, "\\'");
}
