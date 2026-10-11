import { recognitionConfig } from "./rs-recognition-config.js";
import { claimOnce, completeClaim, requireClaimDatabase } from './rs-recognition-claims.js';


const SIGNAL_VERSION = 1;
const EVENT_TYPE_CHOICES = new Map([
  ["start", "start"], ["success", "success"], ["failure", "failure"], ["update", "update"], ["other", "other"],
  ["recognition", "start"], ["new", "success"], ["save", "update"], ["login", "start"],
  ["recovery", "other"], ["visit", "success"], ["device_retired", "update"]
]);
const EVENT_RESULT_CHOICES = new Map([
  ["matched", "matched"], ["not_matched", "not_matched"], ["pending", "pending"], ["error", "error"], ["unknown", "unknown"],
  ["success", "matched"], ["failure", "error"], ["rejected", "not_matched"]
]);
const MATCHED_BY_CHOICES = new Map([
  ["ai", "ai"], ["manual", "manual"], ["external", "external"], ["unknown", "unknown"],
  ["device_token", "external"], ["phone", "manual"], ["pin", "manual"], ["none", "unknown"]
]);
const RECOGNITION_STATUS_CHOICES = new Map([
  ["pending", "pending"], ["confirmed", "confirmed"], ["rejected", "rejected"], ["flagged", "flagged"],
  ["known_device", "confirmed"], ["unknown_device", "rejected"], ["device_not_active", "rejected"],
  ["device_found_no_person", "rejected"], ["person_not_active", "rejected"], ["access_not_allowed", "rejected"]
]);

export class RecognitionSessionError extends Error {
  constructor(code, status, detail = "") {
    super(detail || code);
    this.name = "RecognitionSessionError";
    this.code = code;
    this.status = status;
  }
}

// Separate mutable summary namespace; immutable Footprints claims below stay
// unchanged. A claimed uncertain write is reconciled by reading, never replayed.
export async function recordRecognitionSummary({ env, fetchImpl = fetch, sessionUid,
  tokenFingerprint, personRecordId = '', deviceRecordId = '', recognized = false,
  retired = false, now = () => Date.now() }) {
  if (typeof sessionUid !== 'string' || !/^[A-Za-z0-9_-]{1,128}$/.test(sessionUid)) throw new RecognitionSessionError('invalid_session_uid', 400);
  if (!/^[a-f0-9]{64}$/.test(tokenFingerprint || '')) throw new RecognitionSessionError('invalid_session_binding', 400);
  if (recognized && (!recordId(personRecordId) || !recordId(deviceRecordId))) throw new RecognitionSessionError('invalid_session_binding', 503);
  const config = recognitionConfig(env, RecognitionSessionError);
  const db = requireClaimDatabase(env);
  const bytes = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(JSON.stringify(['recognition-summary', [config.baseId, sessionUid]])));
  const key = [...new Uint8Array(bytes)].map(b => b.toString(16).padStart(2, '0')).join('');
  const tableUrl = airtableUrl(config.baseId, config.summaryTable);
  const fail = code => { throw new RecognitionSessionError(code, 503); };
  const conflict = code => { throw new RecognitionSessionError(code, 409); };
  async function readSummary() {
    const url = new URL(tableUrl);
    url.searchParams.set('maxRecords', '2');
    url.searchParams.set('filterByFormula', `{session_uid} = '${escapeAirtableString(sessionUid)}'`);
    const response = await fetchImpl(url, { headers: airtableHeaders(config.token) });
    const body = await response.json().catch(() => null);
    if (!response.ok || !Array.isArray(body?.records)) fail('session_summary_lookup_failed');
    if (body.records.length > 1 || body.offset) conflict('session_summary_ambiguous');
    return body.records[0] || null;
  }
  function matches(record, fields) {
    return record && ['session_uid', 'started_at', 'last_activity_at', 'status'].every(k => record.fields?.[k] === fields[k]) &&
      JSON.stringify(record.fields?.person || []) === JSON.stringify(fields.person || []) &&
      JSON.stringify(record.fields?.device || []) === JSON.stringify(fields.device || []);
  }
  function sessionOf(fields) {
    return { session_uid: sessionUid, person_record_id: fields.person[0], device_record_id: fields.device[0],
      started_at: fields.started_at, last_activity_at: fields.last_activity_at, status: fields.status };
  }
  async function finish(expected, result) {
    const updated = await db.prepare("UPDATE recognize_claims SET state = 'complete', result_json = ? WHERE claim_key = ? AND state = 'claimed' AND result_json = ?")
      .bind(JSON.stringify(result), key, JSON.stringify(expected)).run();
    if (updated.meta.changes !== 1) fail('session_summary_busy');
    return result;
  }
  let row = await db.prepare('SELECT state, result_json FROM recognize_claims WHERE claim_key = ?').bind(key).first();
  let context = row?.result_json ? JSON.parse(row.result_json) : null;
  if (context && context.token_fingerprint !== tokenFingerprint) conflict('session_identity_conflict');
  if (context && recognized && (context.fields.person[0] !== personRecordId || context.fields.device[0] !== deviceRecordId)) conflict('session_identity_conflict');
  if (row?.state === 'claimed') {
    if (!context?.pending) fail('session_summary_write_uncertain');
    const existing = await readSummary();
    if (!matches(existing, context.fields)) fail('session_summary_write_uncertain');
    context = await finish(context, { ...context, pending: false, record_id: existing.id });
    // Reconciliation confirms only the previous outcome. A denial must still
    // close a previously active summary, never return it as active permission.
    if (recognized && context.fields.status !== 'active') conflict('session_closed');
    if (recognized || context.fields.status !== 'active') return sessionOf(context.fields);
    row = { state: 'complete', result_json: JSON.stringify(context) };
  }
  if (!row && !recognized) return null;
  if (context && context.fields.status !== 'active') {
    if (!recognized) return sessionOf(context.fields);
    conflict('session_closed');
  }
  let won;
  if (!row) {
    const inserted = await db.prepare("INSERT INTO recognize_claims (claim_key, state) VALUES (?, 'claimed') ON CONFLICT(claim_key) DO NOTHING").bind(key).run();
    won = inserted.meta.changes === 1;
  } else {
    const updated = await db.prepare("UPDATE recognize_claims SET state = 'claimed' WHERE claim_key = ? AND state = 'complete' AND result_json = ?").bind(key, row.result_json).run();
    won = updated.meta.changes === 1;
  }
  if (!won) fail('session_summary_busy');
  let existing;
  try {
    existing = await readSummary();
  } catch (error) {
    // No provider write has started. Release only the claim just acquired;
    // uncertain POST/PATCH claims retain their pending reconciliation context.
    if (context) await finish(context, context);
    else {
      const released = await db.prepare("DELETE FROM recognize_claims WHERE claim_key = ? AND state = 'claimed' AND result_json IS NULL")
        .bind(key).run();
      if (released.meta.changes !== 1) fail('session_summary_busy');
    }
    throw error;
  }
  if (existing) {
    if (!context) conflict('session_identity_conflict');
    const expectedPerson = context?.fields.person[0] || personRecordId;
    const expectedDevice = context?.fields.device[0] || deviceRecordId;
    if (existing.fields?.session_uid !== sessionUid || JSON.stringify(existing.fields?.person || []) !== JSON.stringify([expectedPerson]) ||
        JSON.stringify(existing.fields?.device || []) !== JSON.stringify([expectedDevice]) || (context && existing.id !== context.record_id)) {
      // Known validation failure before any external write can release ownership.
      if (context) await finish(context, context);
      conflict('session_identity_conflict');
    }
    if (existing.fields.started_at !== context.fields.started_at || !validDate(existing.fields.last_activity_at)) {
      await finish(context, context);
      fail('session_summary_invalid');
    }
    if (existing.fields.status !== 'active') {
      if (context) await finish(context, { ...context, fields: existing.fields });
      conflict('session_closed');
    }
  } else if (context) fail('session_summary_write_uncertain');
  const priorTime = Math.max(Date.parse(existing?.fields?.last_activity_at || '') || 0, Date.parse(context?.fields.last_activity_at || '') || 0);
  const timestamp = new Date(Math.max(Number(now()), priorTime + 1)).toISOString();
  const fields = { session_uid: sessionUid, person: [context?.fields.person[0] || personRecordId],
    device: [context?.fields.device[0] || deviceRecordId], started_at: existing?.fields.started_at || timestamp,
    last_activity_at: timestamp, status: recognized ? 'active' : retired ? 'revoked' : 'ended' };
  if (!validDate(fields.started_at)) fail('session_summary_invalid');
  const pending = { token_fingerprint: tokenFingerprint, record_id: existing?.id || null, fields, pending: true };
  const prepared = await db.prepare("UPDATE recognize_claims SET result_json = ? WHERE claim_key = ? AND state = 'claimed' AND result_json IS ?")
    .bind(JSON.stringify(pending), key, context ? JSON.stringify(context) : null).run();
  if (prepared.meta.changes !== 1) fail('session_summary_busy');
  let response;
  try {
    response = await fetchImpl(tableUrl, { method: existing ? 'PATCH' : 'POST', headers: airtableHeaders(config.token),
      body: JSON.stringify({ records: [{ ...(existing ? { id: existing.id } : {}), fields }] }) });
  } catch { fail('session_summary_write_uncertain'); }
  const body = await response.json().catch(() => ({}));
  if (!response.ok || !body.records?.[0]?.id) fail('session_summary_write_uncertain');
  await finish(pending, { ...pending, record_id: body.records[0].id, pending: false });
  return sessionOf(fields);
}

export async function recordRecognitionSession({
  env,
  fetchImpl = fetch,
  geoFetchImpl = fetch,
  request,
  payload
}) {
  const config = airtableConfig(env);
  const event = normalizeEvent(payload);
  const fields = await buildAirtableFields({ event, request, signalSecret: config.signalSecret, geoFetchImpl });
  const tableUrl = airtableUrl(config.baseId, config.sessionsTable);
  const claim = await claimOnce(env, 'recognition-event', [config.baseId, event.idempotency_key]);
  if (claim.result) return { ...claim.result, duplicate: true };
  const existing = await findByIdempotencyKey({
    config,
    event,
    fetchImpl,
    tableUrl
  });

  if (existing) {
    return completeClaim(env, claim, {
      ok: true,
      duplicate: true,
      record_id: existing.id,
      session_event_uid: existing.fields?.session_event_uid || event.session_event_uid,
      session_uid: existing.fields?.session_uid || event.session_uid
    });
  }
  if (!claim.won) throw new RecognitionSessionError('session_event_write_uncertain', 503);
  let response;
  try { response = await fetchImpl(tableUrl, {
    method: "POST",
    headers: airtableHeaders(config.token),
    body: JSON.stringify({
      records: [{ fields }]
    })
  }); } catch { throw new RecognitionSessionError('session_event_write_uncertain', 503); }
  const result = await response.json().catch(() => ({}));

  if (!response.ok || !result.records?.[0]?.id) {
    throw new RecognitionSessionError(
      "session_event_create_failed",
      502
    );
  }

  return completeClaim(env, claim, {
    ok: true,
    duplicate: false,
    record_id: result.records[0].id,
    session_event_uid: event.session_event_uid,
    session_uid: event.session_uid
  });
}

function airtableConfig(env) {
  const config = recognitionConfig(env, RecognitionSessionError);
  return { ...config, sessionsTable: config.sessions, signalSecret: clean(env?.RS_RECOGNITION_SIGNAL_SECRET) };
}

function normalizeEvent(payload) {
  const input = payload && typeof payload === "object" ? payload : {};
  const raw = {
    event_type: required(input.event_type, "missing_event_type"),
    event_result: required(input.event_result, "missing_event_result"),
    matched_by: clean(input.matched_by),
    recognition_status: clean(input.recognition_status)
  };
  const event = {
    session_event_uid: required(input.session_event_uid, "missing_session_event_uid"),
    session_uid: required(input.session_uid, "missing_session_uid"),
    event_type: choice(EVENT_TYPE_CHOICES, raw.event_type, "other"),
    event_result: choice(EVENT_RESULT_CHOICES, raw.event_result, "unknown"),
    idempotency_key: required(input.idempotency_key, "missing_idempotency_key"),
    event_at: validDate(input.event_at) || new Date().toISOString(),
    person_record_id: recordId(input.person_record_id),
    device_record_id: recordId(input.device_record_id),
    phone_alias_record_id: recordId(input.phone_alias_record_id),
    matched_by: choice(MATCHED_BY_CHOICES, raw.matched_by, "unknown"),
    recognition_status: choice(RECOGNITION_STATUS_CHOICES, raw.recognition_status, "pending"),
    client_timezone: clean(input.client_timezone),
    viewport_width: finiteNumber(input.viewport_width),
    page_path: clean(input.page_path),
    referrer: clean(input.referrer),
    detail: enrichedDetail(input.detail, raw)
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

async function buildAirtableFields({ event, request, signalSecret, geoFetchImpl }) {
  const cf = request?.cf || {};
  const ip = clientIp(request);
  const geo = await geoSignals({ cf, ip, geoFetchImpl });
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
    automation_status: "queued",
    automation_attempt_count: 0,
    signal_version: SIGNAL_VERSION
  };

  add(fields, "person", linkValue(event.person_record_id));
  add(fields, "device", linkValue(event.device_record_id));
  add(fields, "phone_alias", linkValue(event.phone_alias_record_id));
  add(fields, "matched_by", event.matched_by);
  add(fields, "recognition_status", event.recognition_status);
  add(fields, "country_code", clean(geo.country));
  add(fields, "region", clean(geo.region));
  add(fields, "city", clean(geo.city));
  add(fields, "timezone", clean(geo.timezone));
  add(fields, "asn", clean(geo.asn));
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
    ? "tablet"
    : /Mobile|iPhone|iPod|Android/.test(ua)
      ? "mobile"
      : "desktop";

  return { browser, os, device };
}

function viewportBucket(width, fallback) {
  if (width === null) return fallback === "mobile" ? "small" : fallback === "tablet" ? "medium" : "large";
  if (width <= 480) return "small";
  if (width <= 1024) return "medium";
  if (width <= 1440) return "large";
  return "extra_large";
}

function choice(choices, value, fallback) {
  return choices.get(clean(value).toLowerCase()) || fallback;
}

function enrichedDetail(value, raw) {
  const detail = value && typeof value === "object" && !Array.isArray(value)
    ? Object.fromEntries(Object.entries(value).filter(([key]) => ["changed_fields", "source", "automation_action", "return_to", "error_code", "correlation_id"].includes(key)).map(([key, item]) => [key, key === "changed_fields" ? (Array.isArray(item) ? item.filter(x => ["person_name", "first_name", "last_name", "primary_phone_e164", "member_pin", "email"].includes(x)) : []) : clean(item).slice(0, 255)]))
    : {};
  return {
    ...detail,
    event_type_raw: raw.event_type,
    event_result_raw: raw.event_result,
    matched_by_raw: raw.matched_by,
    recognition_status_raw: raw.recognition_status
  };
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

// Reused unchanged from rs-visitor-event.js; approximate footprint signals only.
async function geoSignals({ cf, ip, geoFetchImpl }) {
  const cloudflare = {
    country: clean(cf.country),
    region: clean(cf.region),
    city: clean(cf.city),
    timezone: clean(cf.timezone),
    asn: clean(cf.asn)
  };
  if (!ip || (cloudflare.country && cloudflare.region && cloudflare.city)) return cloudflare;

  try {
    const response = await geoFetchImpl(
      `https://get.geojs.io/v1/ip/geo/${encodeURIComponent(ip)}.json`,
      { signal: AbortSignal.timeout(1500) }
    );
    if (!response.ok) return cloudflare;
    const result = await response.json();
    return {
      country: cloudflare.country || clean(result.country_code),
      region: cloudflare.region || clean(result.region),
      city: cloudflare.city || clean(result.city),
      timezone: cloudflare.timezone || clean(result.timezone),
      asn: cloudflare.asn || clean(result.asn)
    };
  } catch {
    return cloudflare;
  }
}

function clientIp(request) {
  const candidates = [
    clean(request?.headers?.get("CF-Connecting-IP")),
    ...clean(request?.headers?.get("X-Forwarded-For")).split(",").map((value) => value.trim())
  ];
  return candidates.find((value) => value && networkPrefix(value)) || "";
}
