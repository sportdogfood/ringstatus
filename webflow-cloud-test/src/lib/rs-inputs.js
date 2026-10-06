// Shared input rules. Authentication is supplied by the server, never by a device token.
export const INPUT_KINDS = ['barn', 'users', 'riders', 'horses', 'locations'];
export const collection = kind => kind === 'barn' ? 'barns' : kind;
export class InputError extends Error {
  constructor(code, status = 400) { super(code); this.code = code; this.status = status; }
}
export const fail = (code, status) => { throw new InputError(code, status); };
export async function digest(value) {
  const bytes = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(value));
  return Array.from(new Uint8Array(bytes), n => n.toString(16).padStart(2, '0')).join('');
}
const text = value => typeof value === 'string' ? value.trim() : '';
const can = (actor, permission) => actor.permissions?.includes(permission);
const owns = (actor, barn) => barn && (barn.ownerUid === actor.id || actor.barnIds?.includes(barn.id));
const publicRecord = ({ ownerUid, requestUid, requestHash, ...record }) => record;
export function requireActor(actor) {
  if (!actor || typeof actor.id !== 'string' || !actor.id.trim()) fail('authentication_required', 401);
}
export async function readInputState(store, actor, selectedBarn = '') {
  requireActor(actor);
  if (!can(actor, 'inputs:read')) fail('permission_denied', 403);
  const barns = (await store.list('barn')).filter(barn => owns(actor, barn));
  const ids = new Set(barns.map(barn => barn.id));
  const state = { version: 1, barns: barns.map(publicRecord), selectedBarn: ids.has(selectedBarn) ? selectedBarn : barns[0]?.id || '' };
  for (const kind of INPUT_KINDS.slice(1)) state[kind] = (await store.list(kind)).filter(record => ids.has(record.barnId)).map(publicRecord);
  return state;
}
function normalizeDraft(draft) {
  if (!draft || typeof draft !== 'object' || Array.isArray(draft)) fail('invalid_draft');
  const out = {};
  for (const field of ['id', 'name', 'email', 'userId', 'riderId', 'locationId', 'address']) {
    out[field] = text(draft[field]);
    if (out[field].length > (field === 'address' ? 1000 : 200)) fail('field_too_long');
  }
  if (!out.name) fail('name_required');
  if (out.email && !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(out.email)) fail('invalid_email');
  return out;
}
async function authorizedBarn(store, actor, id) {
  const barn = (await store.list('barn')).find(item => item.id === id);
  if (!owns(actor, barn)) fail('barn_not_found', 404);
  return barn;
}
export async function saveInputRecord({ store, actor, payload, profileLink = false }) {
  requireActor(actor);
  if (!can(actor, 'inputs:read') || !can(actor, 'inputs:write')) fail('permission_denied', 403);
  if (!/^[\w-]{8,128}$/.test(payload.requestId || '')) fail('invalid_request_id');
  let kind = profileLink ? 'users' : payload.kind;
  if (!INPUT_KINDS.includes(kind)) fail('invalid_kind');
  let draft = payload.draft;
  let barnId = text(payload.barnId);
  let expectedRevision = payload.expectedRevision;
  // Read the principal's verified profile; the client cannot nominate another identity.
  if (profileLink) {
    const profile = actor.profile;
    if (!profile?.personUid || !profile.name) fail('verified_profile_required', 403);
    await authorizedBarn(store, actor, barnId);
    const matches = (await store.list('users')).filter(item => item.barnId === barnId && item.recognitionPersonId === profile.personUid);
    if (matches.length > 1) fail('ambiguous_profile_link', 409);
    const existing = matches[0];
    draft = { id: existing?.id, name: profile.name, email: profile.email || '' };
    expectedRevision = existing?.revision;
  }
  const input = normalizeDraft(draft);
  if (kind === 'barn' && !input.id && !can(actor, 'barns:create')) fail('permission_denied', 403);
  if (kind === 'barn' && input.id) barnId = input.id;
  if (kind !== 'barn' || input.id) await authorizedBarn(store, actor, barnId);
  // Replay identity excludes server-derived profile changes and revision on profile-link.
  const fingerprint = await digest(JSON.stringify(profileLink ? { action: 'profile-link', barnId, personUid: actor.profile.personUid } : { kind, barnId, input, expectedRevision }));
  const eventId = await digest(`${actor.id}|${payload.requestId}`);
  const replay = await store.event(eventId);
  if (replay) {
    if (replay.inputHash !== fingerprint) fail('request_id_reused', 409);
    await authorizedBarn(store, actor, replay.record.barnId);
    return { ok: true, record: replay.record, state: await readInputState(store, actor, replay.record.barnId) };
  }
  const records = await store.list(kind);
  const id = input.id || `rs_${(await digest(`${eventId}|${kind}`)).slice(0, 32)}`;
  if (kind === 'barn') barnId = id;
  const current = records.find(record => record.id === id);
  if (input.id && (!current || current.barnId !== barnId)) fail('record_not_found', 404);
  if (current?.requestUid === eventId) {
    if (current.requestHash !== fingerprint) fail('request_id_reused', 409);
    await authorizedBarn(store, actor, current.barnId);
    // A retry after an uncertain audit outcome repairs the audit without reapplying the edit.
    await writeAudit(store, { eventId, actor, kind, barnId, fingerprint, requestId: payload.requestId, record: publicRecord(current) });
    return { ok: true, record: publicRecord(current), state: await readInputState(store, actor, barnId) };
  }
  if (current && (!Number.isInteger(expectedRevision) || expectedRevision !== current.revision)) fail('record_changed', 409);
  if (records.some(record => record.id !== id && (kind === 'barn' ? owns(actor, record) : record.barnId === barnId) && record.name.toLocaleLowerCase('en-US') === input.name.toLocaleLowerCase('en-US'))) fail('duplicate_name', 409);
  const record = { id, barnId, name: input.name, revision: (current?.revision || 0) + 1, ownerUid: current?.ownerUid || actor.id, requestUid: eventId, requestHash: fingerprint };
  if (kind === 'users') {
    record.email = input.email;
    record.recognitionPersonId = profileLink ? actor.profile.personUid : current?.recognitionPersonId || '';
  }
  if (kind === 'locations') record.address = input.address;
  const relationships = kind === 'riders' ? [['userId', 'users']] : kind === 'horses' ? [['riderId', 'riders'], ['locationId', 'locations']] : [];
  for (const [field, relatedKind] of relationships) {
    record[field] = input[field];
    if (record[field] && !(await store.list(relatedKind)).some(other => other.id === record[field] && other.barnId === barnId)) fail('invalid_relationship');
  }
  // The provider must qualify its own concurrency guarantees. Airtable is trial-only.
  await store.put(kind, record, { expectedRevision: current?.revision });
  await writeAudit(store, { eventId, actor, kind, barnId, fingerprint, requestId: payload.requestId, record: publicRecord(record) });
  return { ok: true, record: publicRecord(record), state: await readInputState(store, actor, barnId) };
}
async function writeAudit(store, { eventId, actor, kind, barnId, fingerprint, requestId, record }) {
  try {
    await store.appendEvent({ eventId, actorId: actor.id, kind, barnId, inputHash: fingerprint, requestId, record, occurredAt: new Date().toISOString(), action: record.revision === 1 ? 'create' : 'update' });
  } catch { fail('write_outcome_unknown', 503); }
}
export async function handleInputRequest({ request, actor, store, recognition, log = () => {} }) {
  const traceId = crypto.randomUUID();
  const operation = new URL(request.url).pathname.split('/').filter(Boolean).at(-1);
  const headers = { 'Content-Type': 'application/json', 'Cache-Control': 'no-store', 'Vary': 'Cookie', 'X-Content-Type-Options': 'nosniff', 'X-Request-Id': traceId };
  let requestId;
  const json = (body, status = 200) => {
    // IDs and outcomes only: no names, phone/PIN, device tokens or draft bodies.
    try { log({ event: 'rs_input_request', traceId, requestId, operation, method: request.method, actorId: actor?.id, status, code: body.error || 'ok', entityId: body.record?.id }); } catch { /* logging must not change a committed outcome */ }
    return new Response(JSON.stringify(body), { status, headers });
  };
  try {
    requireActor(actor);
    const url = new URL(request.url);
    if (request.method === 'GET' && operation === 'state') return json({ ok: true, state: await readInputState(store, actor, url.searchParams.get('barn_id') || '') });
    if (request.method === 'GET' && operation === 'recognition') return json(await recognition.lookup(url.searchParams.get('device_token') || ''));
    if (request.method !== 'POST') return json({ ok: false, error: 'method_not_allowed' }, 405);
    if (request.headers.get('Origin') !== url.origin) fail('origin_denied', 403);
    if (!(request.headers.get('Content-Type') || '').startsWith('application/json')) fail('json_required', 415);
    const raw = await request.text();
    if (new TextEncoder().encode(raw).length > 16384) fail('request_too_large', 413);
    let payload;
    try { payload = JSON.parse(raw); } catch { fail('invalid_json'); }
    if (!payload || typeof payload !== 'object' || Array.isArray(payload)) fail('invalid_payload');
    if (/^[\w-]{8,128}$/.test(payload.requestId || '')) requestId = payload.requestId;
    if (operation === 'recognition') return json(await recognition.action(payload, request));
    if (!['record', 'profile-link'].includes(operation)) fail('not_found', 404);
    return json(await saveInputRecord({ store, actor, payload, profileLink: operation === 'profile-link' }));
  } catch (error) {
    const status = Number.isInteger(error.status) ? error.status : 502;
    // Never echo upstream response bodies or credentials to the browser/logs.
    return json({ ok: false, error: error.code || 'storage_unavailable' }, status);
  }
}
