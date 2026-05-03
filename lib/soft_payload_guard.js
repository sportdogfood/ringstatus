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

function inspectSoftPayload({
  payload,
  text,
  response,
  expectedTopLevelKeys = [],
  expectedPredicate = null,
  minBodyLength = 0,
  normalContentLengthHint = 61010,
} = {}) {
  const body_length = byteLength(text);
  const content_length = contentLengthFromResponse(response);
  const keys = isPlainObject(payload) ? Object.keys(payload) : [];
  const expectedKeys = normalizeKeys(expectedTopLevelKeys);
  const hasExpectedKey = expectedKeys.length
    ? expectedKeys.some((key) => Object.prototype.hasOwnProperty.call(payload || {}, key))
    : true;
  const predicateOk = typeof expectedPredicate === "function"
    ? Boolean(expectedPredicate(payload))
    : true;
  const smallBody = body_length > 0 && body_length <= Math.max(2, Number(minBodyLength) || 0);
  const smallContentLength = content_length !== null && content_length <= Math.max(2, Number(minBodyLength) || 0);

  if (isEmptyPlainObject(payload)) {
    return {
      ok: false,
      reason: "soft_payload_empty",
      body_length,
      content_length,
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
      normal_content_length_hint: normalContentLengthHint,
      payload_keys: keys,
    };
  }

  return {
    ok: true,
    reason: null,
    body_length,
    content_length,
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
    normal_content_length_hint: error?.normal_content_length_hint ?? 61010,
    payload_keys: error?.payload_keys || [],
  };
}

module.exports = {
  SoftPayloadError,
  assertValidPayload,
  inspectSoftPayload,
  isSoftPayloadError,
  softPayloadLogFields,
};
