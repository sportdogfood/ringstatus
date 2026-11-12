export const APP_BASE = 'appqWC58FLtmHcUAW';

const rowFields = 'schedule-key show date kind ring ring-number source-position class-number class-name group start-time sequence type status source-schedule-id source-class-key stage-status entry-count trip-count horses riders trainers record-json rs-lever rs-flag rs-priority schedule_sequencetype class_type source-record-status';
const tables = {
  shows: ['tblYkodPn0xSLnXac', 'show-key show_id customer_id facility_id show_name is_default'],
  days: ['tblzRTVjHbR8HvMP1', 'show-day-key show date day-priority'],
  schedules: ['tbljK5BImDJnQcNB4', 'schedule-key w-shows w-days ring group class kind source-position name start-time record-status last-success source-json rs-lever rs-flag rs-priority schedule_sequencetype class_type'],
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
  conflicts: ['tbl2e132BoSMYolXr', ['conflict-key','detected-at','run-id','lane','date','record-key','reason','details-json','admin-status','admin-notes','Record','Problem 1','Problem 2','Problem 3']],
  feed: ['tblzsgMl5UNsPMk5W', 'show-day-key show-key date day-priority generated-at source-updated-at row-count payload-json'],
  dataRows: ['tblKh1xEt1p809qnt', rowFields],
  day0Rows: ['tblHoADAZbp6u52Bo', rowFields+' source-feed baseline-record base-version diff-checked-at diff-applied live-state-json'],
  runs: ['tblWhGshVZuUlKHbO', 'run-id started completed result show-id date summary errors ring-count group-count class-count entry-count'],
  errors: ['tbl0q9iniaffVoXAa', 'error-key run-id occurred-at show-id date message'],
  heartbeat: ['tblXeSnesC31P8P5S', 'heartbeat-key checked-at show-id date result summary errors run-id started completed']
};
const writeKey = {conflicts:'conflict-key',dataRows:'schedule-key',day0Rows:'schedule-key',feed:'show-day-key', runs:'run-id', errors:'error-key', heartbeat:'heartbeat-key'};
const fieldsFor = name => Array.isArray(tables[name][1]) ? tables[name][1] : tables[name][1].split(' ');
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
      for (const field of fieldsFor(name)) url.searchParams.append('fields[]', field);
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
    async upsertRows(name, records) {
      if (!writable || !['dataRows','day0Rows'].includes(name) || !records.length || records.length>10) throw Error('invalid_readable_batch');
      const key=writeKey[name], allowed=fieldsFor(name);
      if(records.some(fields=>!fields[key] || Object.keys(fields).some(k=>!allowed.includes(k))) || new Set(records.map(r=>r[key])).size!==records.length) throw Error('invalid_readable_write');
      const saved=await call(urlFor(name),{method:'PATCH',body:JSON.stringify({performUpsert:{fieldsToMergeOn:[key]},records:records.map(fields=>({fields}))})});
      if(saved.records.length!==records.length || saved.records.some(r=>!recordId(r.id))) throw Error('invalid_readable_write_result');
      const rows=await this.read(name,'OR('+records.map(r=>equals(key,r[key])).join(',')+')');
      const normalize=v=>Array.isArray(v)?(v.length?v.map(x=>typeof x==='string'?x:x?.id):null):v??null;
      for(const fields of records){const found=rows.filter(r=>r[key]===fields[key]);if(found.length!==1 || !saved.records.some(r=>r.id===found[0].id) || Object.entries(fields).some(([k,v])=>(typeof v==='boolean'?Boolean(found[0][k])!==v:JSON.stringify(normalize(found[0][k]))!==JSON.stringify(normalize(v))))) throw Error('readable_write_readback');}
      return rows;
    },
    async upsert(name, fields) {
      if (!writable || !Object.hasOwn(writeKey, name)) throw Error('schedule_app_write_forbidden');
      const key = writeKey[name];
      const allowed = fieldsFor(name);
      if (!fields[key] || Object.keys(fields).some(field => !allowed.includes(field))) throw Error('invalid_feed_write');
      const saved = await call(urlFor(name), {method:'PATCH', body:JSON.stringify({
        performUpsert:{fieldsToMergeOn:[key]}, records:[{fields}]
      })});
      if (saved.records.length !== 1 || !recordId(saved.records[0].id)) throw Error('invalid_feed_write_result');
      const rows = await this.read(name, equals(key, fields[key]));
      if (rows.length !== 1 || rows[0].id !== saved.records[0].id || Object.entries(fields).some(([k,v]) =>
        k.endsWith('-at') || ['started','completed'].includes(k) ? Date.parse(rows[0][k]) !== Date.parse(v) :
        name==='conflicts' && k==='Record' && typeof rows[0][k]==='string' ? rows[0][k].replace(/\r?\n$/,'') !== v :
        (rows[0][k] ?? null) !== v
      )) throw Error('schedule_app_write_readback');
      return rows[0];
    }
  };
}
