// Durable one-winner claims. SQLite coordinates callers; it does not make
// external Airtable writes transactional. An uncertain claim is never released.
function fail(code) { const error = new Error(code); error.code = code; error.status = 503; throw error; }
export function requireClaimDatabase(env) {
  const db = env?.RS_RECOGNITION_CONTROL_DB;
  if (!db || typeof db.prepare !== 'function') fail('recognition_control_database_required');
  return db;
}
export async function claimOnce(env, namespace, identity) {
  const db = requireClaimDatabase(env);
  const bytes = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(JSON.stringify([namespace, identity])));
  const key = [...new Uint8Array(bytes)].map(b => b.toString(16).padStart(2, '0')).join('');
  try {
    const inserted = await db.prepare("INSERT INTO recognize_claims (claim_key, state) VALUES (?, 'claimed') ON CONFLICT(claim_key) DO NOTHING").bind(key).run();
    const row = await db.prepare('SELECT state, result_json FROM recognize_claims WHERE claim_key = ?').bind(key).first();
    if (!row) fail('recognition_claim_unavailable');
    return { key, won: inserted.meta.changes === 1, result: row.state === 'complete' ? JSON.parse(row.result_json) : null };
  } catch { fail('recognition_claim_unavailable'); }
}
export async function completeClaim(env, claim, result) {
  try {
    await requireClaimDatabase(env).prepare("UPDATE recognize_claims SET state = 'complete', result_json = ? WHERE claim_key = ? AND state = 'claimed'").bind(JSON.stringify(result), claim.key).run();
  } catch { fail('recognition_claim_unavailable'); }
  return result;
}
