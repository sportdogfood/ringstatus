const crypto = require("crypto");

function isBlank(value) {
  return value === null ||
    value === undefined ||
    (typeof value === "string" && value.trim() === "") ||
    String(value).trim().toLowerCase() === "null" ||
    String(value).trim().toLowerCase() === "nan";
}

function firstNonBlank(value) {
  if (Array.isArray(value)) {
    for (const item of value) {
      if (!isBlank(item)) return item;
    }
    return undefined;
  }
  return value;
}

function deriveSglTokenPrefix(token) {
  try {
    const decoded = Buffer.from(token, "base64").toString("ascii");
    const match = decoded.match(/^[A-Za-z0-9]+/);
    return match ? match[0].slice(0, 16) : "";
  } catch {
    return "";
  }
}

function buildSglTokenFields(rawValue) {
  const raw = firstNonBlank(rawValue);
  if (isBlank(raw)) return {};

  const token = String(raw).trim();
  return {
    sgl_token_raw: token,
    sgl_token_prefix: deriveSglTokenPrefix(token),
    sgl_token_length: token.length,
    sgl_token_hash: crypto.createHash("sha256").update(token).digest("hex"),
  };
}

module.exports = {
  buildSglTokenFields,
  deriveSglTokenPrefix,
};
