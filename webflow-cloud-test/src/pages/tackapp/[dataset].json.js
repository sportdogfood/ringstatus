import { env } from 'cloudflare:workers';
import { iamActor } from '../../lib/rs-inputs-iam-policy.js';
import { claimOnce, completeClaim } from '../../lib/rs-recognition-claims.js';

const RINGSTATUS = 'apptdhhNzduxm5gjn';
const ADDITIONS = 'tblVkaUDMLmyDn0ft';
const DIRECTORY_BASE = 'appETrjio9Yj9Pv17';
const DIRECTORY = 'tbl8uYnqzysdiBqiu';
const CASTLEWOOD = 'recZ9MyqRplHD9AY8';
const TENANT = 8778;
const WRITE_ORIGIN = 'https://ringstatus.com';
const TACK_ORIGIN = 'https://tack.ringstatus.com';
const writeOrigins = new Set([WRITE_ORIGIN, TACK_ORIGIN]);
const directoryFields = ['horse-id', 'horse-name', 'horse-nickname'];
const additionFields = ['addition_key', 'tenant_id', 'source', 'horse_key', 'show_name', 'barn_name', 'color', 'gender', 'added_by', 'directory_record_id'];
const colors = new Set(['Grey', 'Chestnut', 'Bay', 'Palomino', 'Liverchestnut', 'Black', 'Paint']);
const genders = new Set(['Mare', 'Gelding']);

const datasets = new Map([
  ['horses', {
    table: 'tblliyUZ1ZS88Kfvl', // ww_horses
    view: 'viwaB98wQ0MZcWkXC', // 8778-tacklists-full-roster
    fields: ['horseId', 'horseName', 'barnName', 'barnActive'],
    key: 'horseId',
    label: 'horseName',
  }],
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
  if (!dataset && params.dataset !== 'horse-search') return Response.json({ error: 'dataset_not_found' }, { status: 404, headers });

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
const directoryToken = bindings => bindings.AIRTABLE_TACK_DIRECTORY_TOKEN || bindings.AIRTABLE_TOKEN;
const additions = (bindings, fetchImpl, filter = '') => listRecords({
  base: RINGSTATUS, token: bindings.AIRTABLE_TOKEN, table: ADDITIONS, fields: additionFields,
  formula: filter ? `AND({tenant_id}=${TENANT},${filter})` : `{tenant_id}=${TENANT}`,
}, fetchImpl);

function toHorse(fields) {
  const horseId = String(fields.horse_key ?? '');
  const horseName = clean(fields.barn_name) || clean(fields.show_name);
  if (!horseId || !horseName) throw new Error('Invalid TackLists addition');
  return { horseId: /^\d+$/.test(horseId) ? Number(horseId) : horseId, horseName, barnName: horseName, barnActive: false };
}

async function readHorseData(dataset, bindings, fetchImpl) {
  const [defaults, added] = await Promise.all([
    readDataset(bindings.AIRTABLE_BASE_ID || bindings.AIRTABLE_BASE, bindings.AIRTABLE_TOKEN, datasets.get('horses')),
    additions(bindings, fetchImpl),
  ]);
  const catalog = new Map(defaults.map(horse => [String(horse.horseId), horse]));
  for (const { fields } of added) {
    const horse = toHorse(fields);
    const id = String(horse.horseId);
    if (!catalog.has(id)) catalog.set(id, horse);
  }
  if (dataset === 'horses') return [...catalog.values()];

  const rows = await listRecords({ base: DIRECTORY_BASE, token: directoryToken(bindings), table: DIRECTORY,
    fields: ['horse-id', 'horse-name'], paced: true }, fetchImpl);
  const matches = [];
  for (const { id, fields } of rows) {
    const horseId = fields['horse-id'];
    const name = clean(fields['horse-name']);
    if (!validRecordId(id) || !validHorseId(horseId) || !name || catalog.has(String(horseId))) continue;
    catalog.set(String(horseId), null);
    matches.push({ recordId: id, name });
  }
  return matches.sort((a, b) => a.name.localeCompare(b.name));
}

const reply = (status, body) => Response.json(body, { status, headers: { 'Cache-Control': 'no-store' } });
const failure = (status, error) => reply(status, { error });

function allowTackCredentials(response, request) {
  response.headers.append('Vary', 'Origin');
  if (request.headers.get('Origin') === TACK_ORIGIN && new URL(request.url).origin === WRITE_ORIGIN) {
    response.headers.set('Access-Control-Allow-Origin', TACK_ORIGIN);
    response.headers.set('Access-Control-Allow-Credentials', 'true');
  }
  return response;
}

export const OPTIONS = ({ request, params }) => {
  if (params.dataset !== 'horses' || new URL(request.url).origin !== WRITE_ORIGIN ||
      request.headers.get('Origin') !== TACK_ORIGIN ||
      request.headers.get('Access-Control-Request-Method') !== 'POST') {
    return failure(403, 'origin_denied');
  }
  return allowTackCredentials(new Response(null, { status: 204, headers: {
    'Access-Control-Allow-Methods': 'POST',
    'Access-Control-Allow-Headers': 'Content-Type',
  } }), request);
};

export const POST = async ({ request, params }) => params.dataset === 'horses'
  ? allowTackCredentials(await handleTackHorseAdd(request, env, fetch), request)
  : failure(405, 'method_not_allowed');

export async function handleTackHorseAdd(request, bindings, fetchImpl, authorize = iamActor) {
  if (new URL(request.url).origin !== WRITE_ORIGIN || !writeOrigins.has(request.headers.get('Origin'))) {
    return failure(403, 'origin_denied');
  }
  if (!request.headers.get('Content-Type')?.toLowerCase().startsWith('application/json')) {
    return failure(415, 'invalid_content_type');
  }
  try {
    const actor = await authorize(request, bindings);
    if (!actor?.id || !actor.permissions?.includes('inputs:write') || !actor.barnIds?.includes(CASTLEWOOD)) {
      return failure(403, 'permission_denied');
    }
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

    const fields = { tenant_id: TENANT, added_by: actor.id, source: body?.source };
    if (body?.source === 'Directory' && validRecordId(body.directory_record_id)) {
      const record = await airtable(fetchImpl, tableUrl(DIRECTORY_BASE, DIRECTORY, body.directory_record_id), directoryToken(bindings));
      const id = record.fields?.['horse-id'];
      const show = clean(record.fields?.['horse-name']);
      if (record.id !== body.directory_record_id || !validHorseId(id) || !show) return failure(404, 'horse_not_found');
      Object.assign(fields, { addition_key: `${TENANT}:directory:${id}`, horse_key: String(id),
        directory_record_id: record.id, show_name: show, barn_name: clean(record.fields['horse-nickname']) || show });
    } else if (body?.source === 'Manual' && typeof body.request_id === 'string' && /^[a-f0-9]{8}-[a-f0-9]{4}-4[a-f0-9]{3}-[89ab][a-f0-9]{3}-[a-f0-9]{12}$/i.test(body.request_id)) {
      const show = clean(body.show_name), barn = clean(body.barn_name);
      if (!show || show.length > 150 || !barn || barn.length > 150 || !colors.has(body.color) || !genders.has(body.gender)) {
        return failure(400, 'invalid_horse_details');
      }
      const requestId = body.request_id.toLowerCase();
      Object.assign(fields, { addition_key: `${TENANT}:manual:${requestId}`, horse_key: `tack_${requestId}`,
        show_name: show, barn_name: barn, color: body.color, gender: body.gender });
    } else return failure(400, 'invalid_request');

    const lookup = () => additions(bindings, fetchImpl, `{addition_key}='${fields.addition_key}'`);
    const replay = rows => {
      if (rows.length !== 1) throw new Error('Unexpected TackLists addition count');
      const existing = rows[0].fields;
      if (fields.source === 'Manual' && !Object.entries(fields).every(([key, value]) => existing[key] === value)) {
        return failure(409, 'request_conflict');
      }
      return reply(200, { horse: toHorse(existing), additionId: rows[0].id });
    };
    const existing = await lookup();
    if (existing.length) return replay(existing);
    if (fields.source === 'Directory') {
      const defaults = await listRecords({ base: RINGSTATUS, token: bindings.AIRTABLE_TOKEN,
        table: datasets.get('horses').table, view: datasets.get('horses').view,
        fields: ['horseId'], formula: `{horseId}=${Number(fields.horse_key)}` }, fetchImpl);
      if (defaults.some(row => String(row.fields.horseId) === fields.horse_key)) return failure(409, 'horse_already_in_picker');
    }

    // One writer owns the log entry and its attribution. A retry checks Airtable before any new write.
    const claim = await claimOnce(bindings, 'tackapp-horse-addition', fields.addition_key);
    if (!claim.won) {
      const confirmed = await lookup();
      return confirmed.length ? replay(confirmed) : failure(503, 'write_outcome_unknown');
    }
    let saved;
    try {
      const result = await airtable(fetchImpl, tableUrl(RINGSTATUS, ADDITIONS), bindings.AIRTABLE_TOKEN, {
        method: 'PATCH', body: JSON.stringify({ performUpsert: { fieldsToMergeOn: ['addition_key'] }, records: [{ fields }] }),
      });
      saved = result.records?.[0];
      if (result.records?.length !== 1 || !validRecordId(saved?.id) || !Object.entries(fields).every(([key, value]) => saved.fields?.[key] === value)) {
        return failure(503, 'write_outcome_unknown');
      }
    } catch (error) {
      if (error.providerStatus >= 400 && error.providerStatus < 500 && error.providerStatus !== 408) {
        await bindings.RS_RECOGNITION_CONTROL_DB.prepare("DELETE FROM recognize_claims WHERE claim_key = ? AND state = 'claimed' AND result_json IS NULL").bind(claim.key).run();
      }
      return failure(503, 'write_outcome_unknown');
    }
    try { await completeClaim(bindings, claim, { additionId: saved.id }); }
    catch { console.error('[tackapp] addition confirmed but claim completion failed'); }
    return reply(201, { horse: toHorse(saved.fields), additionId: saved.id });
  } catch (error) {
    if (error.code === 'authentication_required') return failure(401, 'authentication_required');
    if (error.providerStatus === 404) return failure(404, 'horse_not_found');
    return failure(503, 'storage_unavailable');
  }
}
