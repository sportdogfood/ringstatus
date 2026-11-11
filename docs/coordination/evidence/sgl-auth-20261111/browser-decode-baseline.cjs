"use strict";
// Reuses the existing Worker decoder unchanged; no proxy calls or credential storage.
const SGL_BUNDLE_SALT = "5874a2ec29e1d30258abe7a00220fd5d";

async function decodeSglPayload(body, authorization, salt, keepBlobs = false) {
  const payload = JSON.parse(body);
  if (!payload || typeof payload !== "object" || Array.isArray(payload)) throw Error("Invalid payload");
  // A token refresh requires a fresh authenticated response, not a guessed key.
  if (payload._t) throw Error("Upstream token refresh required");
  const keys = new Map();
  async function visit(value) {
    if (!value || typeof value !== "object") return;
    if (Array.isArray(value)) {
      for (const row of value) await visit(row);
      return;
    }
    if (Object.hasOwn(value, "_")) {
      if (!authorization || typeof value._ !== "string") throw Error("Missing authentication or blob");
      const bytes = Uint8Array.from(atob(value._), c => c.charCodeAt(0));
      if (bytes.length < 36) throw Error("Invalid blob length");
      const minute = new TextDecoder().decode(bytes.slice(0, 8));
      if (!/^\d{8}$/.test(minute)) throw Error("Invalid blob minute");
      if (!keys.has(minute)) {
        const material = new TextEncoder().encode(salt + "|" + authorization + "|" + minute);
        const digest = await crypto.subtle.digest("SHA-256", material);
        keys.set(minute, await crypto.subtle.importKey("raw", digest, {name:"AES-GCM"}, false, ["decrypt"]));
      }
      const plaintext = await crypto.subtle.decrypt({name:"AES-GCM", iv:bytes.slice(8,20)}, keys.get(minute), bytes.slice(20));
      const fields = JSON.parse(new TextDecoder("utf-8", {fatal:true}).decode(plaintext));
      if (!fields || typeof fields !== "object" || Array.isArray(fields)) throw Error("Invalid decoded fields");
      for (const [key, field] of Object.entries(fields)) {
        if (["__proto__", "constructor", "prototype", "_", "_t"].includes(key)) throw Error("Invalid decoded key");
        value[key] = field;
      }
      if (!keepBlobs) delete value._;
    }
    for (const child of Object.values(value)) await visit(child);
  }
  await visit(payload);
  if (Object.keys(payload).length === 0) throw Error("Empty source payload");
  return JSON.stringify(payload);
}
module.exports={decodeSglPayload,SGL_BUNDLE_SALT};
