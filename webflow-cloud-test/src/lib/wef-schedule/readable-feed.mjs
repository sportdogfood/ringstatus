import {sharedScope} from './shared-projection.mjs';
import {readScheduleFeed} from './shared-feed.mjs';
import {prefix} from './shared-base.mjs';
import {applyDay0Diff} from '../../../public/wef-schedule-v2/day0-diff.js';

const normalize = value => Array.isArray(value) ? (value.length?value.map(v=>typeof v==='string'?v:v?.id):null) : value??null;
const same = (a,b) => typeof b==='boolean' ? Boolean(a)===b : JSON.stringify(normalize(a))===JSON.stringify(normalize(b));
const json = value => value ? JSON.stringify(value) : null;
const keyed = (rows,key) => {
 const map=new Map();
 for(const row of rows){if(!row[key]||map.has(row[key]))throw Error('duplicate_readable_identity');map.set(row[key],row);}
 return map;
};

export function readableFields(row, dataset, source, stage) {
 if(!source || source['schedule-key']!==row.key || source.id!==row.scheduleId)throw Error('readable_source_identity');
 const ring=dataset.ringDetails[row.ring];if(!ring)throw Error('readable_ring_missing');
 const entries=row.entries;
 const fields={
  'schedule-key':row.key,show:dataset.metadata.show,date:row.date,kind:row.kind,ring:ring.name,
  'ring-number':ring.number,'source-position':row.position,'class-number':row.id||null,
  'class-name':row.name||null,group:row.group||null,'start-time':row.time||null,
  sequence:row.sequence||null,type:row.type||null,status:row.status||null,
  'source-schedule-id':row.scheduleId,'source-class-key':row.classId||null,'stage-status':stage,
  'entry-count':row.entryCount??null,'trip-count':row.trips??null,
  horses:entries.map(e=>e.name||'').join('\n')||null,
  riders:entries.map(e=>e.rider||'').join('\n')||null,
  trainers:entries.map(e=>e.trainer||'').join('\n')||null,
  'record-json':JSON.stringify(row),'source-record-status':source['record-status']||null,
  ...Object.fromEntries(['rs-lever','rs-flag','rs-priority','schedule_sequencetype','class_type'].map(k=>[k,source[k]??null]))
 };
 if(Object.values(fields).some(v=>typeof v==='string'&&v.length>100000))throw Error('readable_field_too_large');
 return fields;
}

// Uses the same saved Data/verified-Live packages as the existing reader.
// It writes only derived rows; publication columns remain Airtable formulas.
export async function refreshReadableSchedule(api,lane,now,progress=[]) {
 const scope=await sharedScope(api), showKey=scope.show['show-key'];
 const full=await readScheduleFeed(api,scope.show.id,'full');
 const diff=lane==='day0'?await readScheduleFeed(api,scope.show.id,'day0'):null;
 const dataset=diff?applyDay0Diff(full,diff):full;
 const name=lane==='day0'?'day0Rows':'dataRows';
 const [sources,existing]=await Promise.all([
  api.read('schedules',prefix('schedule-key',lane==='day0'?scope.day0['show-day-key']+'|':showKey+'|')),
  api.read(name)
 ]);
 const sourceByKey=keyed(sources,'schedule-key'), old=keyed(existing,'schedule-key');
 const pending=new Set(full.metadata.pendingDays?.map(d=>d.date)||[]);
 // A valid freshly built Day0 diff can advance while the full lane awaits its next slot.
 if(diff)pending.delete(diff.date);
 const targets=lane==='day0'?await api.read('dataRows',prefix('schedule-key',scope.day0['show-day-key']+'|')):[];
 const baseline=keyed(targets,'schedule-key');
 const feeds=lane==='day0'?await api.read('feed',prefix('show-day-key',scope.day0['show-day-key'])):[];
 if(lane==='day0'&&(feeds.length!==1||feeds[0]['show-day-key']!==scope.day0['show-day-key']))throw Error('readable_day0_feed_missing');
 const touched=diff?new Set([...diff.rows.upsert.map(r=>r.key),...diff.entries.map(r=>r.row)]):null;
 const ringEdits=diff?new Set([...diff.rings.upsert.map(r=>r.key),...diff.rings.remove]):null;
 const desired=keyed(dataset.records,'key'),updates=[];
 for(const row of dataset.records){
  // A pending day retains the last useful saved values and publication flags.
  if(pending.has(row.date)&&old.has(row.key))continue;
  const fields=readableFields(row,dataset,sourceByKey.get(row.key),lane==='day0'?'Current Day-0':'Current Data');
  if(diff)Object.assign(fields,{
   'source-feed':[feeds[0].id],'baseline-record':baseline.has(row.key)?[baseline.get(row.key).id]:[],
   'base-version':diff.baseVersion,'diff-checked-at':diff.checkedAt,
   'diff-applied':touched.has(row.key)||ringEdits.has(String(row.ring)),
   'live-state-json':json(row.live)
  });
  const previous=old.get(row.key);
  if(!previous)updates.push(fields);
  else{const patch=Object.fromEntries(Object.entries(fields).filter(([k,v])=>k==='schedule-key'||!same(previous[k],v)));if(Object.keys(patch).length>1)updates.push(patch);}
 }
 let retired=0;
 for(const previous of existing){
  if(desired.has(previous['schedule-key']))continue;
  if(lane==='full'&&pending.has(previous.date)&&previous['schedule-key'].startsWith(showKey+'|'))continue;
  const stage=(lane==='day0'&&!previous['schedule-key'].startsWith(scope.day0['show-day-key']+'|'))||!previous['schedule-key'].startsWith(showKey+'|')?'Outside current scope':'Removed from current source';
  if(previous['stage-status']!==stage){updates.push({'schedule-key':previous['schedule-key'],'stage-status':stage});retired++;}
 }
 const current=await sharedScope(api);
 if(current.show.id!==scope.show.id || JSON.stringify(current.days.map(d=>[d.id,d['day-priority']]))!==JSON.stringify(scope.days.map(d=>[d.id,d['day-priority']])))throw Error('readable_scope_changed');
 let written=0;
 for(let i=0;i<updates.length;i+=10){
  const batch=updates.slice(i,i+10);await api.upsertRows(name,batch);written+=batch.length;
  progress.push(...batch.map(r=>name+'|'+r['schedule-key']));
 }
 return {table:name,records:dataset.records.length,upserts:written,unchanged:dataset.records.length-(updates.length-retired),retired,pendingDays:[...pending]};
}
