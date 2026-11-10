import { env } from 'cloudflare:workers';
import { claimOnce, completeClaim, requireClaimDatabase } from '../../lib/rs-recognition-claims.js';

const TACKAPP = 'appPLyqAX4IctCOeq';
const ADDITIONS = 'tblYG2yUEJO5Mmser'; // tack-horse-additions
const ROSTER = 'tblmi0Ozfd0axbWto'; // tack-all
const DIRECTORY = 'tbl0CrISBsZM6J4UC'; // w-horses, read only
const TENANT = 8778;
const WRITE_ORIGIN = 'https://ringstatus.com';
const TACK_ORIGIN = 'https://tack.ringstatus.com';
const writeOrigins = new Set([WRITE_ORIGIN, TACK_ORIGIN]);
const additionFields = ['addition_key', 'tenant_id', 'source', 'horse_key', 'show_name', 'barn_name', 'color', 'gender', 'added_by', 'directory_record_id'];
const rosterFields = ['horse_key', 'show_name', 'barn_name', 'barn_active', 'source', 'addition_key'];
const colors = new Set(['Grey', 'Chestnut', 'Bay', 'Palomino', 'Liverchestnut', 'Black', 'Paint']);
const genders = new Set(['Mare', 'Gelding']);

const datasets = new Map([
  ['lists', {
    table: 'tblRGvWvkLYV7RKqK', // tack_lists
    view: 'viw3WXE5krtboYmxr', // 8778-tack-lists
    fields: ['list_id', 'sort', 'key', 'label', 'type', 'inNav', 'inSummary', 'inShare'],
    key: 'key',
    label: 'label',
  }],
]);

const appOrigins = new Set([
  'https://tack.ringstatus.com',
  'https://ringstatus.com',
  'https://www.ringstatus.com',
  'https://ringstatus.webflow.io',
]);

export const GET = async ({ request, params }) => {
  const headers = new Headers({ 'Cache-Control': 'no-store', Vary: 'Origin' });
  const origin = request.headers.get('Origin');
  if (appOrigins.has(origin)) headers.set('Access-Control-Allow-Origin', origin);

  const dataset = datasets.get(params.dataset);
  if (!dataset && !['horses', 'horse-search'].includes(params.dataset)) {
    return Response.json({ error: 'dataset_not_found' }, { status: 404, headers });
  }

  const base = env.AIRTABLE_BASE_ID || env.AIRTABLE_BASE;
  const token = env.AIRTABLE_TOKEN;
  if (!base || !token) {
    return Response.json({ error: 'airtable_not_configured' }, { status: 500, headers });
  }

  try {
    const rows = params.dataset === 'lists'
      ? await readDataset(base, token, dataset)
      : await readHorseData(params.dataset, env, fetch);
    return Response.json(rows, { headers });
  } catch (error) {
    console.error('[tackapp]', params.dataset, error.message);
    return Response.json({ error: 'airtable_load_failed' }, { status: 502, headers });
  }
};

async function readDataset(base, token, { table, view, fields, key, label }) {
  const records = await listRecords({ base, token, table, view, fields });
  const seen = new Set();
  const rows = records.map(({fields: values}) => {
    const id = String(values[key] ?? '').trim();
    if (!id || !String(values[label] ?? '').trim() || seen.has(id)) {
      throw new Error('Airtable returned a missing or duplicate catalog entry');
    }
    seen.add(id);
    return Object.fromEntries(fields.map(field => [field, values[field] ?? null]));
  });
  if (!rows.length) throw new Error('Airtable returned an empty catalog');
  return rows;
}

function tableUrl(base, table, recordId = '') {
  return new URL(`https://api.airtable.com/v0/${encodeURIComponent(base)}/${table}${recordId ? `/${recordId}` : ''}`);
}

async function airtable(fetchImpl, url, token, options = {}) {
  const response = await fetchImpl(url, {
    ...options, redirect: 'manual', signal: AbortSignal.timeout(10000),
    headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
  });
  if (!response.ok) {
    const error = new Error(`Airtable returned ${response.status}`);
    error.providerStatus = response.status;
    throw error;
  }
  return response.json();
}

async function listRecords({ base, token, table, view, fields, formula, paced = false }, fetchImpl = fetch) {
  const url = tableUrl(base, table);
  if (view) url.searchParams.set('view', view);
  if (formula) url.searchParams.set('filterByFormula', formula);
  url.searchParams.set('pageSize', '100');
  for (const field of fields) url.searchParams.append('fields[]', field);
  const rows = [];
  const seen = new Set();
  do {
    const { records, offset } = await airtable(fetchImpl, url, token);
    if (!Array.isArray(records) || seen.has(offset)) throw new Error('Invalid Airtable page');
    rows.push(...records);
    if (!offset) break;
    seen.add(offset);
    url.searchParams.set('offset', offset);
    if (paced) await new Promise(resolve => setTimeout(resolve, 225));
  } while (true);
  return rows;
}

const clean = value => typeof value === 'string' ? value.trim() : '';
const validHorseId = value => Number.isSafeInteger(value) && value > 0;
const validRecordId = value => typeof value === 'string' && /^rec[A-Za-z0-9]{14}$/.test(value);
const validUuid = value => typeof value === 'string' && /^[a-f0-9]{8}-[a-f0-9]{4}-4[a-f0-9]{3}-[89ab][a-f0-9]{3}-[a-f0-9]{12}$/i.test(value);
const tackToken = bindings => bindings.AIRTABLE_TACKAPP_TOKEN || bindings.AIRTABLE_TOKEN;
const additions = (bindings, fetchImpl, filter = '') => listRecords({
  base: TACKAPP, token: tackToken(bindings), table: ADDITIONS, fields: additionFields,
  formula: filter ? `AND({tenant_id}=${TENANT},${filter})` : `{tenant_id}=${TENANT}`,
}, fetchImpl);

function toHorse(fields) {
  const horseId = String(fields.horse_key ?? '');
  const horseName = clean(fields.barn_name) || clean(fields.show_name);
  if (!horseId || !horseName) throw new Error('Invalid TackLists addition');
  return { horseId: /^\d+$/.test(horseId) ? Number(horseId) : horseId, horseName, barnName: horseName, barnActive: fields.barn_active === true };
}

const readRoster = (bindings, fetchImpl) => listRecords({
  base: TACKAPP, token: tackToken(bindings), table: ROSTER, fields: rosterFields,
}, fetchImpl);

function isRosterHorse(roster, horseId, name) {
  return roster.some(({ fields }) => fields.horse_key === String(horseId) ||
    (fields.horse_key?.startsWith('hps_') &&
      [fields.show_name, fields.barn_name].some(label => clean(label).toLowerCase() === name.toLowerCase())));
}

async function readHorseData(dataset, bindings, fetchImpl) {
  const roster = await readRoster(bindings, fetchImpl);
  if (!roster.length || new Set(roster.map(row => row.fields.horse_key)).size !== roster.length) {
    throw new Error('TackLists roster is empty or has duplicate keys');
  }
  if (dataset === 'horses') return roster.map(({ fields }) => ({
    ...toHorse(fields), ...(fields.source === 'Default' ? {} : { tackAddition: true }),
  }));

  const rows = await listRecords({ base: TACKAPP, token: tackToken(bindings), table: DIRECTORY,
    fields: ['horse-id', 'horse-name'], paced: true }, fetchImpl);
  const matches = [];
  const seen = new Set();
  for (const { id, fields } of rows) {
    const horseId = fields['horse-id'];
    const name = clean(fields['horse-name']);
    if (!validRecordId(id) || !validHorseId(horseId) || !name || seen.has(horseId) || isRosterHorse(roster, horseId, name)) continue;
    seen.add(horseId);
    matches.push({ recordId: id, horseId, name });
  }
  return matches.sort((a, b) => a.name.localeCompare(b.name));
}

async function upsertTackRow(bindings, fetchImpl, table, key, fields) {
  const result = await airtable(fetchImpl, tableUrl(TACKAPP, table), tackToken(bindings), {
    method: 'PATCH', body: JSON.stringify({ performUpsert: { fieldsToMergeOn: [key] }, records: [{ fields }] }),
  });
  const row = result.records?.[0];
  if (result.records?.length !== 1 || !validRecordId(row?.id) || row.fields?.[key] !== fields[key]) {
    throw new Error('TackLists save could not be confirmed');
  }
  return row;
}

async function publishAddition(bindings, fetchImpl, { fields }) {
  const { horse_key, show_name, barn_name, source, addition_key, color, gender, directory_record_id } = fields;
  await upsertTackRow(bindings, fetchImpl, ROSTER, 'horse_key', {
    horse_key, show_name, barn_name, source, addition_key,
    ...(color ? { color } : {}), ...(gender ? { gender } : {}),
    ...(directory_record_id ? { source_record_id: directory_record_id } : {}),
  });
}

const reply = (status, body) => Response.json(body, { status, headers: { 'Cache-Control': 'no-store' } });
const failure = (status, error) => reply(status, { error });

function allowTackOrigin(response, request) {
  response.headers.append('Vary', 'Origin');
  if (request.headers.get('Origin') === TACK_ORIGIN && new URL(request.url).origin === WRITE_ORIGIN) {
    response.headers.set('Access-Control-Allow-Origin', TACK_ORIGIN);
  }
  return response;
}

export const OPTIONS = ({ request, params }) => {
  if (params.dataset !== 'horses' || new URL(request.url).origin !== WRITE_ORIGIN ||
      request.headers.get('Origin') !== TACK_ORIGIN ||
      request.headers.get('Access-Control-Request-Method') !== 'POST') {
    return failure(403, 'origin_denied');
  }
  return allowTackOrigin(new Response(null, { status: 204, headers: {
    'Access-Control-Allow-Methods': 'POST',
    'Access-Control-Allow-Headers': 'Content-Type',
  } }), request);
};

export const POST = async ({ request, params }) => params.dataset === 'horses'
  ? allowTackOrigin(await handleTackHorseAdd(request, env, fetch), request)
  : failure(405, 'method_not_allowed');

async function reserveAddition(db) {
  // Bound writes to the shared roster even when browsers start new sessions.
  const hour = Math.floor(Date.now() / 3600000);
  return db.prepare(`INSERT INTO recognize_claims (claim_key, state, result_json)
    VALUES ('tackapp-addition-budget-v1', 'complete', json_object('hour', ?1, 'count', 1))
    ON CONFLICT (claim_key) DO UPDATE SET result_json = json_object('hour', ?1, 'count',
      CASE WHEN json_extract(result_json, '$.hour') = ?1 THEN json_extract(result_json, '$.count') + 1 ELSE 1 END)
    WHERE json_extract(result_json, '$.hour') <> ?1 OR json_extract(result_json, '$.count') < 60
    RETURNING claim_key`).bind(hour).first();
}

export async function handleTackHorseAdd(request, bindings, fetchImpl) {
  if (new URL(request.url).origin !== WRITE_ORIGIN || !writeOrigins.has(request.headers.get('Origin'))) {
    return failure(403, 'origin_denied');
  }
  if (!request.headers.get('Content-Type')?.toLowerCase().startsWith('application/json')) {
    return failure(415, 'invalid_content_type');
  }
  try {
    const bytes = new Uint8Array(2048);
    const reader = request.body?.getReader();
    if (!reader) return failure(400, 'invalid_request');
    let length = 0;
    for (;;) {
      const { value, done } = await reader.read();
      if (done) break;
      if (length + value.length > bytes.length) { await reader.cancel(); return failure(413, 'request_too_large'); }
      bytes.set(value, length);
      length += value.length;
    }
    let body;
    try { body = JSON.parse(new TextDecoder().decode(bytes.subarray(0, length))); }
    catch { return failure(400, 'invalid_request'); }
    if (!validUuid(body?.session_id)) return failure(400, 'invalid_session');

    const fields = { tenant_id: TENANT, added_by: `tack-session:${body.session_id.toLowerCase()}`, source: body.source };
    if (body?.source === 'Directory' && validRecordId(body.directory_record_id)) {
      const record = await airtable(fetchImpl, tableUrl(TACKAPP, DIRECTORY, body.directory_record_id), tackToken(bindings));
      const id = record.fields?.['horse-id'];
      const show = clean(record.fields?.['horse-name']);
      if (record.id !== body.directory_record_id || !validHorseId(id) || !show) return failure(404, 'horse_not_found');
      Object.assign(fields, { addition_key: `${TENANT}:directory:${id}`, horse_key: String(id),
        directory_record_id: record.id, show_name: show, barn_name: clean(record.fields['horse-nickname']) || show });
    } else if (body?.source === 'Manual' && validUuid(body.request_id)) {
      const show = clean(body.show_name), barn = clean(body.barn_name);
      if (!show || show.length > 150 || !barn || barn.length > 150 || !colors.has(body.color) || !genders.has(body.gender)) {
        return failure(400, 'invalid_horse_details');
      }
      const requestId = body.request_id.toLowerCase();
      Object.assign(fields, { addition_key: `${TENANT}:manual:${requestId}`, horse_key: `tack_${requestId}`,
        show_name: show, barn_name: barn, color: body.color, gender: body.gender });
    } else return failure(400, 'invalid_request');

    const lookup = () => additions(bindings, fetchImpl, `{addition_key}='${fields.addition_key}'`);
    const replay = async rows => {
      if (rows.length !== 1) throw new Error('Unexpected TackLists addition count');
      const existing = rows[0].fields;
      if (fields.source === 'Manual' && !Object.entries(fields).every(([key, value]) => existing[key] === value)) {
        return failure(409, 'request_conflict');
      }
      await publishAddition(bindings, fetchImpl, rows[0]);
      return reply(200, { horse: toHorse(existing), additionId: rows[0].id });
    };
    const existing = await lookup();
    if (existing.length) return replay(existing);
    if (fields.source === 'Directory') {
      const roster = await readRoster(bindings, fetchImpl);
      if (isRosterHorse(roster, fields.horse_key, fields.show_name)) return failure(409, 'horse_already_in_picker');
    }

    // One writer owns the log entry and its attribution. A retry checks Airtable before any new write.
    const claim = await claimOnce(bindings, 'tackapp-base-horse-addition', fields.addition_key);
    if (!claim.won) {
      const confirmed = await lookup();
      return confirmed.length ? replay(confirmed) : failure(503, 'write_outcome_unknown');
    }
    const db = requireClaimDatabase(bindings);
    const releaseClaim = () => db.prepare("DELETE FROM recognize_claims WHERE claim_key = ? AND state = 'claimed' AND result_json IS NULL").bind(claim.key).run();
    let reserved;
    try { reserved = await reserveAddition(db); }
    catch (error) { await releaseClaim(); throw error; }
    if (!reserved) {
      await releaseClaim();
      return failure(429, 'addition_limit_reached');
    }
    let saved;
    try {
      saved = await upsertTackRow(bindings, fetchImpl, ADDITIONS, 'addition_key', { ...fields, added_at: new Date().toISOString() });
      if (!Object.entries(fields).every(([key, value]) => saved.fields?.[key] === value)) {
        return failure(503, 'write_outcome_unknown');
      }
    } catch (error) {
      if (error.providerStatus >= 400 && error.providerStatus < 500 && error.providerStatus !== 408) {
        await releaseClaim();
      }
      return failure(503, 'write_outcome_unknown');
    }
    await publishAddition(bindings, fetchImpl, saved);
    try { await completeClaim(bindings, claim, { additionId: saved.id }); }
    catch { console.error('[tackapp] addition confirmed but claim completion failed'); }
    return reply(201, { horse: toHorse(saved.fields), additionId: saved.id });
  } catch (error) {
    if (error.providerStatus === 404) return failure(404, 'horse_not_found');
    return failure(503, 'storage_unavailable');
  }
}
