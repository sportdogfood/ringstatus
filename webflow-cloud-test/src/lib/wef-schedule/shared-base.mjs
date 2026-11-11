export const APP_BASE = 'appqWC58FLtmHcUAW';

const tables = {
  shows: ['tblYkodPn0xSLnXac', 'show-key show_id customer_id facility_id show_name is_default'],
  days: ['tblzRTVjHbR8HvMP1', 'show-day-key show date day-priority'],
  schedules: ['tbljK5BImDJnQcNB4', 'schedule-key w-shows w-days ring group class kind source-position name start-time record-status last-success source-json'],
  classes: ['tblL2BwNM2Z8sdGDQ', 'class-key w-shows w-days ring group class-id class-number class-name status entry-count record-status last-success trips source-json'],
  trips: ['tbl9dATHFaF8z0x0I', 'trip-key w-shows w-days class entry horse rider trainer trip-id source-uuid record-status order-of-go actual-order estimated-go-time scratch gone-in last-success source-json'],
  rings: ['tblst0MKROc3puhm5', 'ring-key ring-id ring-number ring-nickname name active'],
  horses: ['tblgxiK0U8fZ1y4Sg', 'horse-id horse-name horse-nickname'],
  riders: ['tblnCTfF3CeBYwnWl', 'rider-id rider-name rider-nickname rider-fallback'],
  trainers: ['tblUkA2i9W6QYZqz5', 'trainer-id trainer-name trainer-nickname trainer-fallback'],
  liveRings: ['tblgUj7Q2RPJ0Tr89', 'ring-key customer-id show-id date ring-id ring-number current-group next-group completed-groups scope-status last-seen source-updated-at source-json'],
  liveGroups: ['tblDkL5dV1jwKS1XE', 'group-key customer-id date class-group-id ring status is-live estimated-start-time scope-status last-seen source-updated-at source-json'],
  liveClasses: ['tblhWQL0K4TtGJe7m', 'class-key customer-id show-id date class-id ring detail-available current-trip-id scope-status last-seen source-updated-at source-json'],
  liveEntries: ['tblpMWHRn7k5Kb58M', 'live-entry-key customer-id show-id date class trip-id entry-id horse-id rider-id trainer-id order-of-go actual-order scratch scope-status participation-link-status participation-verified-at last-seen source-updated-at source-json'],
  feed: ['tblzsgMl5UNsPMk5W', 'show-day-key show-key date day-priority generated-at source-updated-at row-count payload-json'],
  runs: ['tblWhGshVZuUlKHbO', 'run-id started completed result show-id date summary errors ring-count group-count class-count entry-count'],
  errors: ['tbl0q9iniaffVoXAa', 'error-key run-id occurred-at show-id date message'],
  heartbeat: ['tblXeSnesC31P8P5S', 'heartbeat-key checked-at show-id date result summary errors run-id started completed']
};
const writeKey = {feed:'show-day-key', runs:'run-id', errors:'error-key', heartbeat:'heartbeat-key'};
const quoted = value => JSON.stringify(String(value));
export const equals = (field, value) => `{${field}}=${typeof value === 'number' ? value : quoted(value)}`;
export const prefix = (field, value) => `LEFT({${field}},${value.length})=${quoted(value)}`;
export const recordId = value => /^rec[A-Za-z0-9]{14}$/.test(value ?? '');

export function sharedBase({token, fetchImpl = fetch, pause = ms => new Promise(r => setTimeout(r, ms)), writable = false}) {
  let last = 0;
  async function call(url, init) {
    if (!token) throw Error('missing_schedule_app_credentials');
    await pause(Math.max(0, 225 - (Date.now() - last)));
    last = Date.now();
    const response = await fetchImpl(url, {
      headers: {Authorization: `Bearer ${token}`, 'Content-Type':'application/json'},
      cache:'no-store', redirect:'manual', signal:AbortSignal.timeout(15000), ...init
    });
    if (!response.ok) throw Error(`schedule_app_airtable_${response.status}`);
    const body = await response.json();
    if (!Array.isArray(body.records)) throw Error('invalid_shared_response');
    return body;
  }
  const urlFor = name => new URL(`https://api.airtable.com/v0/${APP_BASE}/${tables[name][0]}`);
  return {
    async read(name, filter) {
      const url = urlFor(name);
      for (const field of tables[name][1].split(' ')) url.searchParams.append('fields[]', field);
      if (filter) url.searchParams.set('filterByFormula', filter);
      const rows = [], seen = new Set(), offsets = new Set();
      for (;;) {
        const page = await call(url);
        for (const row of page.records) {
          if (!recordId(row.id) || !row.fields || seen.has(row.id)) throw Error('invalid_shared_record');
          seen.add(row.id);
          rows.push({...row.fields, id:row.id});
        }
        if (!page.offset) return rows;
        if (offsets.has(page.offset)) throw Error('repeated_shared_page');
        offsets.add(page.offset);
        url.searchParams.set('offset', page.offset);
      }
    },
    async upsert(name, fields) {
      if (!writable || !Object.hasOwn(writeKey, name)) throw Error('schedule_app_write_forbidden');
      const key = writeKey[name];
      const allowed = tables[name][1].split(' ');
      if (!fields[key] || Object.keys(fields).some(field => !allowed.includes(field))) throw Error('invalid_feed_write');
      const saved = await call(urlFor(name), {method:'PATCH', body:JSON.stringify({
        performUpsert:{fieldsToMergeOn:[key]}, records:[{fields}]
      })});
      if (saved.records.length !== 1 || !recordId(saved.records[0].id)) throw Error('invalid_feed_write_result');
      const rows = await this.read(name, equals(key, fields[key]));
      if (rows.length !== 1 || rows[0].id !== saved.records[0].id || Object.entries(fields).some(([k,v]) =>
        k.endsWith('-at') || ['started','completed'].includes(k) ? Date.parse(rows[0][k]) !== Date.parse(v) : (rows[0][k] ?? null) !== v
      )) throw Error('schedule_app_write_readback');
      return rows[0];
    }
  };
}
