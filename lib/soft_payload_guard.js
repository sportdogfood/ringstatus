class SoftPayloadError extends Error {
  constructor(message, details = {}) {
    super(message);
    this.name = "SoftPayloadError";
    this.code = "SOFT_PAYLOAD";
    Object.assign(this, details);
  }
}

function byteLength(text) {
  return Buffer.byteLength(String(text ?? ""), "utf8");
}

function contentLengthFromResponse(response) {
  const raw = response?.headers?.get?.("content-length") ?? response?.headers?.get?.("Content-Length");
  const value = Number(raw);
  return Number.isFinite(value) ? value : null;
}

function isPlainObject(value) {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function isEmptyPlainObject(value) {
  return isPlainObject(value) && Object.keys(value).length === 0;
}

function normalizeKeys(keys) {
  return (Array.isArray(keys) ? keys : [])
    .map((key) => String(key || "").trim())
    .filter(Boolean);
}

function positiveInteger(value) {
  const number = Number(value);
  if (!Number.isFinite(number) || number <= 0) return null;
  return Math.floor(number);
}

function positiveIntegerEnv(name) {
  return positiveInteger(process.env[name]);
}

function endpointPath(endpoint) {
  const text = String(endpoint || "");
  try {
    return new URL(text).pathname.toLowerCase();
  } catch {
    return text.toLowerCase();
  }
}

function softPayloadEndpointKind({ endpoint, lane } = {}) {
  const path = endpointPath(endpoint);
  const laneText = String(lane || "").toLowerCase();

  if (path.includes("/schedule")) return "schedule";
  if (path.includes("/classes/")) return "class";
  if (path.includes("/classsignup")) return "classsignup";
  if (path.includes("/entries/")) return "entry";
  if (path.includes("/people/")) return "people";
  if (path.includes("/ring")) return "ring";

  if (laneText.includes("schedule")) return "schedule";
  if (laneText.includes("trips")) return "people";
  if (laneText.includes("tagger")) return "ring";

  return "default";
}

function defaultSoftPayloadMinBodyLength(kind) {
  switch (kind) {
    case "schedule":
      return 5000;
    case "classsignup":
      return 1000;
    case "people":
      return 1000;
    case "class":
      return 500;
    case "entry":
      return 128;
    case "ring":
      return 128;
    default:
      return 64;
  }
}

function softPayloadMinBodyLengthForEndpoint({ endpoint, lane, minBodyLength } = {}) {
  const explicit = positiveInteger(minBodyLength);
  if (explicit !== null) return explicit;

  const kind = softPayloadEndpointKind({ endpoint, lane });
  const specificEnvName = `SOFT_PAYLOAD_MIN_BODY_LENGTH_${kind.toUpperCase()}`;
  const specificEnv = positiveIntegerEnv(specificEnvName);
  if (specificEnv !== null) return specificEnv;

  const defaultValue = defaultSoftPayloadMinBodyLength(kind);
  const globalEnv = positiveIntegerEnv("SOFT_PAYLOAD_MIN_BODY_LENGTH");
  return Math.max(defaultValue, globalEnv || 0);
}

function inspectSoftPayload({
  payload,
  text,
  response,
  endpoint,
  lane,
  expectedTopLevelKeys = [],
  expectedPredicate = null,
  minBodyLength,
  normalContentLengthHint = 61010,
} = {}) {
  const body_length = byteLength(text);
  const content_length = contentLengthFromResponse(response);
  const min_body_length = softPayloadMinBodyLengthForEndpoint({ endpoint, lane, minBodyLength });
  const keys = isPlainObject(payload) ? Object.keys(payload) : [];
  const expectedKeys = normalizeKeys(expectedTopLevelKeys);
  const hasExpectedKey = expectedKeys.length
    ? expectedKeys.some((key) => Object.prototype.hasOwnProperty.call(payload || {}, key))
    : true;
  const predicateOk = typeof expectedPredicate === "function"
    ? Boolean(expectedPredicate(payload))
    : true;
  const smallBody = body_length > 0 && body_length < min_body_length;
  const smallContentLength = content_length !== null && content_length > 0 && content_length < min_body_length;

  if (isEmptyPlainObject(payload)) {
    return {
      ok: false,
      reason: "soft_payload_empty",
      body_length,
      content_length,
      min_body_length,
      normal_content_length_hint: normalContentLengthHint,
      payload_keys: keys,
    };
  }

  if (smallBody || smallContentLength) {
    return {
      ok: false,
      reason: "soft_payload_too_small",
      body_length,
      content_length,
      min_body_length,
      normal_content_length_hint: normalContentLengthHint,
      payload_keys: keys,
    };
  }

  if (expectedKeys.length && !hasExpectedKey) {
    return {
      ok: false,
      reason: "soft_payload_missing_expected_keys",
      body_length,
      content_length,
      min_body_length,
      normal_content_length_hint: normalContentLengthHint,
      expected_top_level_keys: expectedKeys,
      payload_keys: keys,
    };
  }

  if (!predicateOk) {
    return {
      ok: false,
      reason: "soft_payload_missing_expected_shape",
      body_length,
      content_length,
      min_body_length,
      normal_content_length_hint: normalContentLengthHint,
      payload_keys: keys,
    };
  }

  return {
    ok: true,
    reason: null,
    body_length,
    content_length,
    min_body_length,
    normal_content_length_hint: normalContentLengthHint,
    payload_keys: keys,
  };
}

function assertValidPayload(options = {}) {
  const result = inspectSoftPayload(options);
  if (result.ok) return result;

  throw new SoftPayloadError(result.reason, {
    ...result,
    lane: options.lane || null,
    endpoint: options.endpoint || options.url || null,
    http_status: options.response?.status ?? null,
  });
}

function isSoftPayloadError(error) {
  return error instanceof SoftPayloadError ||
    error?.code === "SOFT_PAYLOAD" ||
    /^soft_payload_/i.test(String(error?.reason || error?.message || ""));
}

function softPayloadLogFields(error) {
  return {
    reason: error?.reason || "soft_payload",
    endpoint: error?.endpoint || null,
    lane: error?.lane || null,
    http_status: error?.http_status ?? null,
    body_length: error?.body_length ?? null,
    content_length: error?.content_length ?? null,
    min_body_length: error?.min_body_length ?? null,
    normal_content_length_hint: error?.normal_content_length_hint ?? 61010,
    payload_keys: error?.payload_keys || [],
  };
}

module.exports = {
  SoftPayloadError,
  assertValidPayload,
  inspectSoftPayload,
  isSoftPayloadError,
  softPayloadMinBodyLengthForEndpoint,
  softPayloadLogFields,
};
