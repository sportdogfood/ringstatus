import { accessHash } from './rs-inputs-access.js';
import { requireClaimDatabase } from './rs-recognition-claims.js';

const fail = (code, status = 503) => { const error = new Error(code); error.code = code; error.status = status; throw error; };
const keyFor = id => accessHash('recognition-otp:' + id);
export function randomRecognitionCode() {
  const number = new Uint32Array(1);
  do { crypto.getRandomValues(number); } while (number[0] >= 4294000000);
  return String(number[0] % 1000000).padStart(6, '0');
}
export function createOtpStore(env, now = () => Date.now()) {
  const db = requireClaimDatabase(env);
  const secret = env.RS_INPUTS_SESSION_SECRET;
  if (!/^[a-f0-9]{64}$/i.test(secret || '')) fail('otp_secret_missing');
  async function digest(value) {
    const key = await crypto.subtle.importKey('raw', new TextEncoder().encode(secret), { name: 'HMAC', hash: 'SHA-256' }, false, ['sign']);
    return Array.from(new Uint8Array(await crypto.subtle.sign('HMAC', key, new TextEncoder().encode('recognition-otp:' + value))), b => b.toString(16).padStart(2, '0')).join('');
  }
  return {
    async start(id, phone) {
      const subject = await digest(phone);
      // Three sends per ten-minute window; the counter is shared across Workers.
      const rateKey = await keyFor('rate:' + subject + ':' + Math.floor(now() / 600000));
      const rate = await db.prepare(`INSERT INTO recognize_claims(claim_key,state,result_json) VALUES (?,'complete','{"count":1}')
        ON CONFLICT(claim_key) DO UPDATE SET result_json=json_set(result_json,'$.count',json_extract(result_json,'$.count')+1)
        WHERE json_extract(result_json,'$.count') < 3 RETURNING result_json`).bind(rateKey).first();
      if (!rate) fail('otp_rate_limited', 429);
      await db.prepare("INSERT INTO recognize_claims(claim_key,state,result_json) VALUES (?,'complete',?)")
        .bind(await keyFor(id), JSON.stringify({ subject, queuedAt: now(), attempts: 0, used: 0 })).run();
    },
    async prepare(id, phone) {
      const code = randomRecognitionCode(), subject = await digest(phone), hash = await digest(id + ':' + phone + ':' + code);
      const saved = await db.prepare(`UPDATE recognize_claims SET result_json=json_set(result_json,'$.hash',?,'$.expires',?)
        WHERE claim_key=? AND json_extract(result_json,'$.subject')=? AND json_extract(result_json,'$.hash') IS NULL
        AND json_extract(result_json,'$.queuedAt') > ? RETURNING claim_key`)
        .bind(hash, now() + 600000, await keyFor(id), subject, now() - 600000).first();
      if (!saved) fail('otp_request_expired', 409);
      return code;
    },
    async consume(id, phone, code) {
      const hash = await digest(id + ':' + phone + ':' + code), subject = await digest(phone);
      // Check, increment and consume in one SQLite statement: concurrent callers
      // cannot both use a code, or exceed five guesses across separate Workers.
      const row = await db.prepare(`UPDATE recognize_claims SET result_json=json_set(result_json,
        '$.attempts',json_extract(result_json,'$.attempts')+1,
        '$.used',CASE WHEN json_extract(result_json,'$.hash')=? THEN 1 ELSE 0 END)
        WHERE claim_key=? AND json_extract(result_json,'$.subject')=? AND json_extract(result_json,'$.used')=0
        AND json_extract(result_json,'$.attempts')<5 AND json_extract(result_json,'$.expires')>?
        RETURNING result_json`).bind(hash, await keyFor(id), subject, now()).first();
      if (!row) return { verified: false, expired: true };
      return { verified: JSON.parse(row.result_json).used === 1 };
    }
  };
}
