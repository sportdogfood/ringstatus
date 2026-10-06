// Operator-only issuance/reissue/revocation. Never imported by public routes.
// Existing canonical person required: review/create that record before granting access.
import { writeFile } from 'node:fs/promises';
import { pathToFileURL } from 'node:url';
import { createAccessStore, randomAccessToken, accessHash } from '../src/lib/rs-inputs-access.js';
export async function issueAccess({ env, personUid, decision, fetchImpl = fetch, now = Date.now() }) {
  if (!['invited', 'approved', 'revoked'].includes(decision)) throw new Error('Explicit invited, approved or revoked decision required');
  const store = createAccessStore({ env, fetchImpl });
  const person = await store.byUid(personUid);
  if (!person || (decision !== 'revoked' && !['active', 'test'].includes(String(person.fields.status).toLowerCase()))) throw new Error('A unique existing active person is required; no implicit creation or membership grant');
  const token = decision === 'revoked' ? null : randomAccessToken();
  const version = crypto.randomUUID().replaceAll('-', '');
  const expires = token ? new Date(now + 24 * 60 * 60 * 1000).toISOString() : null;
  const values = { input_access: decision, input_invite_hash: token ? await accessHash(token) : '', input_invite_expires_at: expires, input_session_version: version };
  const saved = await store.update(person.id, values);
  if (!saved || saved.id !== person.id || saved.fields.input_session_version !== version || saved.fields.input_access !== decision || (saved.fields.input_invite_hash || '') !== values.input_invite_hash) throw new Error('Grant outcome unconfirmed; inspect person and reissue, do not send a link');
  return { personUid, token, expires, decision };
}
async function main() {
  const [decision, personUid, outputPath] = process.argv.slice(2);
  if (!outputPath || !process.env.RS_INPUTS_ONBOARDING_URL) throw new Error('Usage: node scripts/rs-inputs-access.mjs invited|approved|revoked PERSON_UID NEW_PRIVATE_OUTPUT_PATH; configure RS_INPUTS_ONBOARDING_URL');
  const url = new URL(process.env.RS_INPUTS_ONBOARDING_URL);
  if (url.protocol !== 'https:' || url.username || url.password || url.search || url.hash || !url.pathname.endsWith('/onboarding')) throw new Error('Explicit HTTPS onboarding URL required');
  // Reserve a private new file before changing any grant; never print bearer links.
  await writeFile(outputPath, 'Issuance pending; no usable link.\n', { flag: 'wx', mode: 0o600 });
  const result = await issueAccess({ env: process.env, personUid, decision });
  if (result.token) url.hash = `invite=${result.token}`;
  await writeFile(outputPath, JSON.stringify({ ...result, token: undefined, ...(result.token ? { url: url.href } : {}) }, null, 2) + '\n', { mode: 0o600 });
  console.info(JSON.stringify({ event: 'rs_input_access_operator', personUid, decision, outcome: 'saved', outputPath }));
}
if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) main().catch(() => { console.error('Access operation failed. Inspect the target person before reissuing. No link was sent.'); process.exitCode = 1; });
