import fs from 'node:fs/promises';
import path from 'node:path';
import {fileURLToPath} from 'node:url';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');

export async function collectDay1({fetchImpl = fetch, token = process.env.AIRTABLE_TOKEN,
  output = path.join(root, 'runtime/schedule-json/day1')} = {}) {
  const started = new Date().toISOString();
  let result = {started, day: 1};
  async function write(file, value) {
    await fs.mkdir(path.dirname(file), {recursive: true});
    await fs.writeFile(file + '.tmp', JSON.stringify(value, null, 2) + '\n', 'utf8');
    await fs.rename(file + '.tmp', file);
  }
  try {
    if (!token) throw Error('Existing AIRTABLE_TOKEN is unavailable');
    const schema = JSON.parse(await fs.readFile(path.join(root, 'schema.json'), 'utf8')).tables;
    async function records(name, formula) {
      const url = new URL('https://api.airtable.com/v0/appdl16HJ5ETm8N69/' + schema[name].id);
      url.searchParams.set('filterByFormula', formula);
      url.searchParams.set('returnFieldsByFieldId', 'true');
      url.searchParams.set('maxRecords', '2');
      const response = await fetchImpl(url, {headers: {Authorization: 'Bearer ' + token}, signal: AbortSignal.timeout(120000)});
      if (!response.ok) throw Error('Day 1 selection HTTP ' + response.status);
      return (await response.json()).records;
    }
    const fields = name => Object.fromEntries(schema[name].fields.map(f => [f.name, f.id]));
    const sf = fields('w-shows'), df = fields('w-days');
    const shows = await records('w-shows', '{' + sf.is_default + '}=1');
    if (shows.length !== 1) throw Error('Default show did not resolve to one record');
    const show = shows[0].fields, key = show[sf['show-key']];
    if (!/^\d+\|\d+$/.test(key)) throw Error('Invalid saved show key');
    const days = await records('w-days', 'AND({' + df.show + '}="' + key + '",{' + df['day-priority'] + '}=1)');
    if (!days.length) result = {...result, ok: true, state: 'Skipped', reason: 'No Day 1 in the selected show'};
    else {
      if (days.length !== 1) throw Error('Day 1 did not resolve to one date');
      const date = days[0].fields[df.date].slice(0, 10);
      const url = new URL('https://wef-schedules-decoded.gombcg.workers.dev/schedule');
      url.search = new URLSearchParams({customer_id: show[sf.customer_id], show_id: show[sf.show_id], date}).toString();
      result = {...result, date, endpoint: url.href};
      const response = await fetchImpl(url, {headers: {'User-Agent': 'Mozilla/5.0', Accept: 'application/json'}, signal: AbortSignal.timeout(120000)});
      if (!response.ok) throw Error('Schedule HTTP ' + response.status);
      const payload = await response.json();
      if (String(payload.show?.show_id) !== String(show[sf.show_id]) || payload.show_date !== date || !Array.isArray(payload.rings)) throw Error('Schedule response has missing or mismatched show/date/rings');
      const file = path.join(output, ...key.split('|'), date + '.json');
      await write(file, payload);
      result = {...result, ok: true, state: 'Saved', http: response.status, rings: payload.rings.length, file};
    }
  } catch (error) {
    result = {...result, ok: false, state: 'Failed', error: error.message};
  }
  result.completed = new Date().toISOString();
  await write(path.join(output, 'last-run.json'), result);
  return result;
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  collectDay1().then(result => {console.log(JSON.stringify(result)); process.exitCode = result.ok ? 0 : 1;})
    .catch(error => {console.error(error.message); process.exitCode = 1;});
}
