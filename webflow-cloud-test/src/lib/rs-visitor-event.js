const DEFAULT_RECOGNITION_BASE_ID = "apptdhhNzduxm5gjn";
const DEFAULT_VISITOR_EVENTS_TABLE = "tbldR3ymyJxYRHtdD";
const SIGNAL_VERSION = 1;
const CLIENT_FIELDS = new Set([
  "event_uid",
  "page_path",
  "referrer",
  "client_timezone",
  "viewport_width",
  "utm_source",
  "utm_medium",
  "utm_campaign"
]);

export const ALLOWED_VISITOR_PATHS = new Set(["/", "/lainey", "/lm", "/ld", "/lh", "/lc"]);

export class VisitorEventError extends Error {
  constructor(code, status) {
    super(code);
    this.name = "VisitorEventError";
    this.code = code;
    this.status = status;
  }
}

export async function recordVisitorEvent({ env, fetchImpl = fetch, geoFetchImpl = fetch, request, payload }) {
  const config = airtableConfig(env);
  const event = normalizeEvent(payload);
  const fields = await buildAirtableFields({
    event,
    request,
    geoFetchImpl
  });
  const response = await fetchImpl(airtableUrl(config.baseId, config.table), {
    method: "POST",
    headers: {
      Authorization: `Bearer ${config.token}`,
      "Content-Type": "application/json"
    },
    body: JSON.stringify({ records: [{ fields }] })
  });
  const result = await response.json().catch(() => ({}));

  if (!response.ok || !result.records?.[0]?.id) {
    throw new VisitorEventError("visitor_event_create_failed", 502);
  }

  return {
    ok: true,
    record_id: result.records[0].id,
    event_uid: event.event_uid
  };
}

function airtableConfig(env) {
  const token = clean(env?.AIRTABLE_TOKEN);
  const baseId = clean(env?.AIRTABLE_RS_RECOGNITION_BASE_ID) || DEFAULT_RECOGNITION_BASE_ID;
  const table = clean(env?.AIRTABLE_RS_VISITOR_EVENTS_TABLE) || DEFAULT_VISITOR_EVENTS_TABLE;

  if (!token) throw new VisitorEventError("missing_airtable_token", 500);
  return { token, baseId, table };
}

function normalizeEvent(payload) {
  const input = payload && typeof payload === "object" && !Array.isArray(payload) ? payload : {};
  if (Object.keys(input).some((key) => !CLIENT_FIELDS.has(key))) {
    throw new VisitorEventError("unsupported_client_field", 400);
  }

  const eventUid = clean(input.event_uid);
  const pagePath = clean(input.page_path);
  if (!eventUid || eventUid.length > 255) throw new VisitorEventError("invalid_event_uid", 400);
  if (!ALLOWED_VISITOR_PATHS.has(pagePath)) throw new VisitorEventError("unsupported_page_path", 400);

  return {
    event_uid: eventUid,
    page_path: pagePath,
    referrer: limited(input.referrer, 2048),
    client_timezone: limited(input.client_timezone, 255),
    viewport_width: finiteNumber(input.viewport_width),
    utm_source: limited(input.utm_source, 255),
    utm_medium: limited(input.utm_medium, 255),
    utm_campaign: limited(input.utm_campaign, 255)
  };
}

async function buildAirtableFields({ event, request, geoFetchImpl }) {
  const cf = request?.cf || {};
  const ip = clientIp(request);
  const geo = await geoSignals({ cf, ip, geoFetchImpl });
  const userAgent = clean(request?.headers?.get("User-Agent"));
  const network = networkPrefix(ip);
  const agent = classifyUserAgent(userAgent);
  const language = primaryLanguage(request?.headers?.get("Accept-Language"));
  const viewport = viewportBucket(event.viewport_width, agent.device);
  const groupingSignals = [
    network,
    clean(geo.country).toUpperCase(),
    clean(geo.region).toLowerCase(),
    clean(geo.city).toLowerCase(),
    clean(geo.timezone),
    clean(geo.asn),
    agent.browser,
    agent.os,
    agent.device,
    language.toLowerCase(),
    event.client_timezone,
    viewport
  ].join("|");
  const fields = {
    event_uid: event.event_uid,
    event_at: new Date().toISOString(),
    page_path: event.page_path,
    signal_version: SIGNAL_VERSION,
    ip_hash: ip ? await sha256Hex(`ip:${ip}`) : undefined,
    network_hash: network ? await sha256Hex(`network:${network}`) : undefined,
    user_agent_hash: userAgent ? await sha256Hex(`ua:${userAgent}`) : undefined,
    environment_hash: await sha256Hex(`environment:${groupingSignals}`)
  };

  add(fields, "referrer_host", referrerHost(event.referrer));
  add(fields, "country_code", clean(geo.country).toUpperCase());
  add(fields, "region", clean(geo.region));
  add(fields, "city", clean(geo.city));
  add(fields, "timezone", clean(geo.timezone));
  add(fields, "asn", clean(geo.asn));
  add(fields, "edge_colo", clean(cf.colo));
  add(fields, "browser_family", agent.browser);
  add(fields, "os_family", agent.os);
  add(fields, "device_class", agent.device);
  add(fields, "language", language);
  add(fields, "client_timezone", event.client_timezone);
  add(fields, "viewport_bucket", viewport);
  add(fields, "utm_source", event.utm_source);
  add(fields, "utm_medium", event.utm_medium);
  add(fields, "utm_campaign", event.utm_campaign);

  for (const key of Object.keys(fields)) {
    if (fields[key] === undefined) delete fields[key];
  }
  return fields;
}

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

function airtableUrl(baseId, table) {
  return `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(table)}`;
}

export async function sha256Hex(value) {
  const encoder = new TextEncoder();
  const digest = await crypto.subtle.digest("SHA-256", encoder.encode(value));
  return [...new Uint8Array(digest)].map((byte) => byte.toString(16).padStart(2, "0")).join("");
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
  const pieces = ip.split("::");
  if (pieces.length > 2) return null;
  const head = pieces[0] ? pieces[0].split(":") : [];
  const tail = pieces[1] ? pieces[1].split(":") : [];
  const fill = pieces.length === 2 ? 8 - head.length - tail.length : 0;
  const parts = [...head, ...Array(Math.max(fill, 0)).fill("0"), ...tail];
  if (parts.length !== 8 || parts.some((part) => !/^[a-f\d]{1,4}$/i.test(part))) return null;
  return parts.map((part) => part.padStart(4, "0").toLowerCase());
}

function classifyUserAgent(userAgent) {
  const ua = userAgent || "";
  const browser = /Edg\//.test(ua) ? "Edge"
    : /Chrome\//.test(ua) ? "Chrome"
      : /Firefox\//.test(ua) ? "Firefox"
        : /Safari\//.test(ua) && /Version\//.test(ua) ? "Safari" : "Unknown";
  const os = /iPhone|iPad|iPod/.test(ua) ? "iOS"
    : /Android/.test(ua) ? "Android"
      : /Windows/.test(ua) ? "Windows"
        : /Mac OS X/.test(ua) ? "macOS"
          : /Linux/.test(ua) ? "Linux" : "Unknown";
  const device = /iPad|Tablet/.test(ua) ? "tablet"
    : /Mobile|iPhone|iPod|Android/.test(ua) ? "mobile" : "desktop";
  return { browser, os, device };
}

function viewportBucket(width, fallback) {
  if (width === null) return fallback === "mobile" ? "small" : fallback === "tablet" ? "medium" : "large";
  if (width <= 480) return "small";
  if (width <= 1024) return "medium";
  if (width <= 1440) return "large";
  return "extra_large";
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

function finiteNumber(value) {
  if (value === "" || value === undefined || value === null) return null;
  const number = Number(value);
  return Number.isFinite(number) && number >= 0 ? number : null;
}

function limited(value, max) {
  return clean(value).slice(0, max);
}

function clean(value) {
  return value === undefined || value === null ? "" : String(value).trim();
}

function add(target, field, value) {
  if (value !== "" && value !== null && value !== undefined) target[field] = value;
}
