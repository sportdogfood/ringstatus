import { env } from 'cloudflare:workers';

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
  if (!dataset) return Response.json({ error: 'dataset_not_found' }, { status: 404, headers });

  const base = env.AIRTABLE_BASE_ID || env.AIRTABLE_BASE;
  const token = env.AIRTABLE_TOKEN;
  if (!base || !token) {
    return Response.json({ error: 'airtable_not_configured' }, { status: 500, headers });
  }

  try {
    const rows = await readDataset(base, token, dataset);
    return Response.json(rows, { headers });
  } catch (error) {
    console.error('[tackapp]', params.dataset, error.message);
    return Response.json({ error: 'airtable_load_failed' }, { status: 502, headers });
  }
};

async function readDataset(base, token, { table, view, fields, key, label }) {
  const url = new URL(`https://api.airtable.com/v0/${encodeURIComponent(base)}/${table}`);
  url.searchParams.set('view', view);
  url.searchParams.set('pageSize', '100');
  for (const field of fields) url.searchParams.append('fields[]', field);

  const rows = [];
  const seen = new Set();
  do {
    const response = await fetch(url, { headers: { Authorization: `Bearer ${token}` } });
    if (!response.ok) throw new Error(`Airtable returned ${response.status}`);
    const { records, offset } = await response.json();
    for (const { fields: values } of records) {
      const id = String(values[key] ?? '').trim();
      if (!id || !String(values[label] ?? '').trim() || seen.has(id)) {
        throw new Error('Airtable returned a missing or duplicate catalog entry');
      }
      seen.add(id);
      rows.push(Object.fromEntries(fields.map(field => [field, values[field] ?? null])));
    }
    if (!offset) break;
    url.searchParams.set('offset', offset);
  } while (true);

  if (!rows.length) throw new Error('Airtable returned an empty catalog');
  return rows;
}
