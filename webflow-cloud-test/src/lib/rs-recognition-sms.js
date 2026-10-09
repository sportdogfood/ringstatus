import { recognitionConfig } from './rs-recognition-config.js';
import { createAccessStore, randomAccessToken, accessHash } from './rs-inputs-access.js';
import { claimOnce, completeClaim, requireClaimDatabase } from './rs-recognition-claims.js';

export const SMS_TABLES = Object.freeze({ requests: 'tblxC4SYtzYhJNcJS', events: 'tblF1Hdqi3yoKTZPV' });
const runtimes = new WeakMap();
const eligible = p => p && ['active', 'test'].includes(String(p.fields?.status).toLowerCase()) && ['invited', 'approved'].includes(p.fields?.input_access);
const clean = v => typeof v === 'string' ? v.trim() : '';
const escape = v => v.replace(/\\/g, '\\\\').replace(/'/g, "\\'");
const fail = (code, status = 409) => { const e = new Error(code); e.code = code; e.status = status; throw e; };
const recordId = v => { if (!/^rec[A-Za-z0-9]{14}$/.test(v || '')) fail('invalid_sms_request', 400); return v; };

export function createRecoverySms({ env, fetchImpl = fetch, now = () => Date.now() }) {
  const config = recognitionConfig(env);
  if (env.RS_INPUTS_BASE_ID && env.RS_INPUTS_BASE_ID !== config.baseId) fail('recognition_base_mismatch', 503);
  const bindings = { ...env, RS_INPUTS_BASE_ID: config.baseId, RS_INPUTS_RECOGNITION_BASE_ID: config.baseId };
  if (!runtimes.has(fetchImpl)) runtimes.set(fetchImpl, { tail: Promise.resolve(), uncertain: new Set() });
  const runtime = runtimes.get(fetchImpl);
  const enabled = () => { if (env.RS_RECOGNITION_SMS_QUEUE_MODE !== 'ready-gated') fail('sms_recovery_unconfigured', 503); requireClaimDatabase(env); };
  const mode = () => { const v = env.RS_RECOGNITION_SMS_RECORD_MODE; if (!['test', 'live'].includes(v)) fail('sms_record_mode_required', 503); return v; };
  const phone = p => { const v = clean(p.fields?.primary_phone_e164); if (!/^\+1\d{10}$/.test(v)) fail('sms_recipient_unavailable', 409); if (mode() === 'test' && v !== env.RS_RECOGNITION_SMS_TEST_RECIPIENT) fail('sms_test_recipient_denied', 403); return v; };
  const url = table => new URL(`https://api.airtable.com/v0/${config.baseId}/${table}`);
  async function call(table, method = 'GET', fields, id, formula) {
    const target = url(table); if (id && method === 'GET') target.pathname += '/' + recordId(id);
    if (formula) { target.searchParams.set('filterByFormula', formula); target.searchParams.set('maxRecords', '2'); }
    let response; try { response = await fetchImpl(target, { method, headers: { Authorization: `Bearer ${config.token}`, 'Content-Type': 'application/json' }, ...(fields ? { body: JSON.stringify({ records: [{ ...(id ? { id: recordId(id) } : {}), fields }] }) } : {}) }); } catch { fail(method === 'GET' ? 'sms_read_failed' : 'sms_write_outcome_unknown', 503); }
    const data = await response.json().catch(() => null);
    if (!response.ok || !data) fail(method === 'GET' ? 'sms_read_failed' : 'sms_write_outcome_unknown', 503);
    if (method !== 'GET') { if (!data.records?.[0]?.id) fail('sms_write_outcome_unknown', 503); return data.records[0]; }
    if (id) return data;
    if (!Array.isArray(data.records) || data.records.length > 1 || data.offset) fail('ambiguous_sms_request');
    return data.records[0] || null;
  }
  async function serial(work) { const prior = runtime.tail; let release; runtime.tail = new Promise(r => { release = r; }); await prior; try { return await work(); } finally { release(); } }
  async function event(row, type, providerSid, error) {
    const uid = `${row.fields.request_uid}:${type}`;
    const claim = await claimOnce(env, 'sms-event', [config.baseId, uid]);
    if (claim.result) return;
    const existing = await call(SMS_TABLES.events, 'GET', null, null, `{event_uid} = '${escape(uid)}'`);
    if (existing) return completeClaim(env, claim, { record_id: existing.id });
    if (!claim.won) fail('sms_write_outcome_unknown', 503);
    const saved = await call(SMS_TABLES.events, 'POST', { event_uid: uid, request: [row.id], occurred_at: new Date(now()).toISOString(), event_type: type,
      source: type === 'requested' ? 'application' : 'automation', ...(providerSid ? { provider_message_sid: providerSid } : {}), ...(error ? { error_code: error } : {}) });
    await completeClaim(env, claim, { record_id: saved.id });
  }
  async function request(payload) {
    enabled(); mode();
    const uid = clean(payload.session_event_uid); if (!/^[A-Za-z0-9_-]{16,128}$/.test(uid)) fail('invalid_recovery_request', 400);
    const clauses = [];
    if (clean(payload.email)) clauses.push(`LOWER({email}) = '${escape(clean(payload.email).toLowerCase())}'`);
    if (clean(payload.first) && clean(payload.last)) clauses.push(`AND(LOWER({first_name}) = '${escape(clean(payload.first).toLowerCase())}',LOWER({last_name}) = '${escape(clean(payload.last).toLowerCase())}')`);
    if (!clauses.length) fail('missing_recovery_identity', 400);
    const reply = { ok: true, accepted: true, request_id: uid, delivery_status: 'unconfirmed' };
    return serial(async () => {
      const person = await call(config.people, 'GET', null, null, clauses.length === 1 ? clauses[0] : `AND(${clauses.join(',')})`);
      if (!eligible(person)) return reply;
      try { phone(person); } catch (e) { if (['sms_recipient_unavailable', 'sms_test_recipient_denied'].includes(e.code)) return reply; throw e; }
      const claim = await claimOnce(env, 'sms-request', [config.baseId, uid]);
      let row = await call(SMS_TABLES.requests, 'GET', null, null, `{request_uid} = '${escape(uid)}'`);
      if (row && (row.fields.person_uid?.length !== 1 || row.fields.person_uid[0] !== person.id)) fail('recovery_request_conflict');
      if (!row) {
        if (!claim.won) fail('sms_write_outcome_unknown', 503);
        row = await call(SMS_TABLES.requests, 'POST', { request_uid: uid, person_uid: [person.id], status: 'draft', record_mode: mode(), requested_at: new Date(now()).toISOString() });
      }
      await event(row, 'requested');
      if (row.fields.status === 'draft') await call(SMS_TABLES.requests, 'PATCH', { status: 'ready' }, row.id);
      await completeClaim(env, claim, { record_id: row.id });
      return reply;
    });
  }
  async function prepare(id) {
    enabled(); return serial(async () => {
      const row = await call(SMS_TABLES.requests, 'GET', null, recordId(id));
      if (row.fields?.status !== 'ready') fail('sms_attempt_already_started');
      if (row.fields.record_mode !== mode() || !Number.isFinite(Date.parse(row.fields.requested_at)) || now() - Date.parse(row.fields.requested_at) > 15 * 60000) fail('sms_request_expired');
      if (row.fields.person_uid?.length !== 1) fail('ambiguous_sms_recipient');
      const person = await call(config.people, 'GET', null, recordId(row.fields.person_uid[0]));
      if (!eligible(person)) fail('sms_access_denied', 403);
      const to = phone(person);
      const lookup = row.fields['primary_phone_e164 (from person_uid)'];
      if (!Array.isArray(lookup) || lookup.length !== 1 || lookup[0] !== to) fail('sms_phone_lookup_mismatch');
      const target = new URL(env.RS_INPUTS_ONBOARDING_URL || 'https://invalid.invalid');
      if (target.protocol !== 'https:' || target.username || target.password || target.search || target.hash || !target.pathname.endsWith('/onboarding')) fail('sms_onboarding_url_required', 503);
      // Retain the configured site origin; recovery belongs to its native page.
      target.pathname = '/rs-recognize';
      const claim = await claimOnce(env, 'sms-prepare', [config.baseId, row.id]);
      if (!claim.won) fail('sms_attempt_already_started');
      await call(SMS_TABLES.requests, 'PATCH', { status: 'processing' }, row.id);
      await event(row, 'attempt_started');
      // Reuse existing invitation primitives, retain the current access decision.
      // No operator CLI is imported or exposed as a public grant endpoint.
      const store = createAccessStore({ env: bindings, fetchImpl });
      const current = await store.byUid(person.fields.person_uid);
      if (!current || current.id !== person.id || !eligible(current) || phone(current) !== to) fail('sms_access_denied', 403);
      const version = current.fields.input_session_version;
      if (!/^[a-f0-9]{32}$/.test(version || '')) fail('sms_access_not_provisioned', 403);
      const token = randomAccessToken(), hash = await accessHash(token);
      const expires = new Date(now() + 24 * 3600000).toISOString();
      // Recovery is not an operator grant/revoke decision. An anonymous request
      // must never rotate the session version and log out an existing session.
      const saved = await store.update(person.id, { input_invite_hash: hash, input_invite_expires_at: expires });
      if (!saved || saved.id !== person.id || !eligible(saved) || phone(saved) !== to || saved.fields.input_invite_hash !== hash || saved.fields.input_session_version !== version) fail('sms_invitation_outcome_unknown', 503);
      target.hash = `invite=${token}`;
      // Sensitive body is returned only to the authenticated automation handoff.
      // It is never stored in the ordinary queue/history or returned publicly.
      return { ok: true, request_record_id: row.id, request_id: row.fields.request_uid, to, body: `Open your RingStatus invitation: ${target.href}` };
    });
  }
  async function outcome(payload) {
    enabled(); return serial(async () => {
      const row = await call(SMS_TABLES.requests, 'GET', null, recordId(payload.request_record_id));
      if (row.fields.record_mode !== mode()) fail('sms_outcome_scope_denied', 403);
      const type = payload.outcome;
      if (!['submitted', 'failed', 'unknown'].includes(type)) fail('unverified_sms_outcome', 400);
      const sid = clean(payload.provider_message_sid);
      if ((sid || type === 'submitted') && !/^SM[0-9a-fA-F]{32}$/.test(sid)) fail('sms_provider_reference_required', 400);
      if (!['processing', type].includes(row.fields.status)) fail('sms_outcome_conflict');
      const error = clean(payload.error_code); if (error && !/^[A-Za-z0-9_-]{1,80}$/.test(error)) fail('invalid_sms_error', 400);
      if (row.fields.provider_message_sid && row.fields.provider_message_sid !== sid) fail('sms_provider_reference_conflict');
      await call(SMS_TABLES.requests, 'PATCH', { status: type, ...(sid ? { provider_message_sid: sid } : {}), ...(error ? { error_code: error } : {}) }, row.id);
      await event(row, type, sid, error);
      return { ok: true, request_id: row.fields.request_uid, status: type, delivered: false };
    });
  }
  return { request, prepare, outcome };
}

export function requireSmsAutomation(request, env) {
  const secret = env.RS_RECOGNITION_AUTOMATION_SECRET;
  if (typeof secret !== 'string' || secret.length < 32) fail('sms_automation_unconfigured', 503);
  const actual = request.headers.get('Authorization') || '', expected = `Bearer ${secret}`;
  let mismatch = actual.length ^ expected.length;
  for (let i = 0; i < expected.length; i++) mismatch |= (actual.charCodeAt(i) || 0) ^ expected.charCodeAt(i);
  if (mismatch) fail('sms_automation_denied', 403);
}
