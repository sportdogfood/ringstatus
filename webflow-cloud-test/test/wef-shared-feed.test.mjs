import {refreshReadableSchedule} from '../src/lib/wef-schedule/readable-feed.mjs';
import test from 'node:test';
import assert from 'node:assert/strict';
import { randomBytes } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import { APP_BASE, sharedBase } from '../src/lib/wef-schedule/shared-base.mjs';
import { buildSharedDay, sharedScope } from '../src/lib/wef-schedule/shared-projection.mjs';
import { readScheduleFeed, refreshScheduleFeed } from '../src/lib/wef-schedule/shared-feed.mjs';
import { runScheduleRefresh } from '../src/lib/wef-schedule/refresh-run.mjs';
import { runDueFeeds } from '../src/lib/wef-schedule/cadence.mjs';
import {conflictReviewFields} from '../src/lib/wef-schedule/conflict-review.mjs';
import {publicationStatus,publishedSchedule} from '../public/wef-schedule-v2/publication.js';
import { applyDay0Diff, makeDay0Diff } from '../public/wef-schedule-v2/day0-diff.js';
import { createDeviceData, validDataset } from '../public/wef-schedule-v2/device-data.js';
import { encodePayload, decodePayload } from '../src/lib/wef-schedule/shared-payload.mjs';

const now = '2026-11-12T17:00:00.000Z', date = '2026-11-12', other = '2026-11-13';
const showId = 'rec00000000000001', showKey = '15|200000078';
const dayKey = d => showKey+'|'+d;
const ringKey = d => dayKey(d)+'|1';
const classKey = ringKey(date)+'|99', uuid = 'ABC', tripKey = classKey+'|12|'+uuid;
const scheduleKey = ringKey(date)+'|25';
const rawTrip = {entry_id:12,entryxclasses_uuid:uuid,trip_id:101,number:44,horse_id:55,rider_id:66,trainer_id:77,order_of_go:2};

function fixture() {
  return {
    shows:[{id:showId,is_default:true,'show-key':showKey,show_id:200000078,customer_id:15,facility_id:500,show_name:'Test show'}],
    days:[date,other].map((d,i) => ({id:'rec'+String(i+2).padStart(14,'0'),show:showKey,date:d,'show-day-key':dayKey(d),'day-priority':i})),
    schedules:[
      {id:'schedule',kind:'class','schedule-key':scheduleKey,'source-position':0,'w-shows':showKey,'w-days':dayKey(date),ring:ringKey(date),
        group:ringKey(date)+'|11',class:classKey,name:'Jumper','start-time':'09:00:00','record-status':'Active','last-success':now,
        'source-json':{class_id:99,class_groupxclasses_id:25,class_group_id:11,group_name:'Jumper',class_type:'Jumpers',total_trips:1}},
      {id:'note',kind:'break','schedule-key':ringKey(other)+'|position-0','source-position':0,'w-shows':showKey,'w-days':dayKey(other),
        ring:ringKey(other),group:ringKey(other)+'|22',name:'Schooling','start-time':'10:00:00','record-status':'Active','last-success':now,
        'source-json':{show_id:200000078,customer_id:15,day:other,ring:1,ring_id:51,ring_name:'International',class_group_id:22,group_name:'Schooling'}}
    ],
    classes:[{id:'class','class-key':classKey,'w-shows':showKey,'w-days':dayKey(date),ring:ringKey(date),'class-id':99,
      'class-number':'808','class-name':'Jumper','entry-count':1,status:'Not Started',trips:['trip'],
      'source-json':{show_id:200000078,class:{class_id:99,schedule_ring_id:51,order_of_go_set:1},trips:[rawTrip]}}],
    trips:[{id:'trip','trip-key':tripKey,class:classKey,entry:'200000078|12',horse:'55',rider:'66',trainer:'77',
      'trip-id':'101','source-uuid':uuid,'record-status':'active','order-of-go':2,'source-json':rawTrip}],
    rings:[{id:'master','ring-key':'15|500|id:51','ring-id':51,'ring-number':1,'ring-nickname':'Intl',name:'International',active:true}],
    horses:[{id:'horse','horse-id':55,'horse-name':'Horse'}],
    riders:[{id:'rider','rider-id':66,'rider-name':'Rider'}],
    trainers:[{id:'trainer','trainer-id':77,'trainer-name':'Trainer'}],
    feed:[],runs:[],errors:[],heartbeat:[],conflicts:[],liveRings:[],liveGroups:[],liveClasses:[],liveEntries:[]
  };
}

function filter(rows, formula='') {
  const pre = /^LEFT\(\{([^}]+)\},\d+\)=(.+)$/.exec(formula);
  if (pre) return rows.filter(row => String(row[pre[1]] ?? '').startsWith(JSON.parse(pre[2])));
  const terms = [...formula.matchAll(/\{([^}]+)\}=("[^"]*"|\d+)/g)];
  if (!terms.length) return rows;
  return rows.filter(row => terms.some(([,key,value]) => (key === 'is_default' ? Number(row[key]) : row[key]) === JSON.parse(value)));
}

const mergeKeys = {conflicts:'conflict-key',dataRows:'schedule-key',day0Rows:'schedule-key',feed:'show-day-key',runs:'run-id',heartbeat:'heartbeat-key',errors:'error-key'};
function memory(data=fixture()) {
  data.dataRows??=[];data.day0Rows??=[];
  const reads=[], writes=[];
  return {
    data,reads,writes,
    async upsertRows(name,rows){return Promise.all(rows.map(row=>this.upsert(name,row)));},
    async read(name,formula) {reads.push(name);return structuredClone(filter(data[name],formula));},
    async upsert(name,fields) {
      const key = mergeKeys[name], prior = data[name].find(row => row[key] === fields[key]);
      const row = prior ?? {id:name+'-'+data[name].length};
      Object.assign(row,structuredClone(fields));
      if (!prior) data[name].push(row);
      writes.push({name,fields});
      return structuredClone(row);
    }
  };
}
function live(data) {
  const scope = {'show-id':200000078,'customer-id':15,date,'source-updated-at':'2026-11-12T16:59:00Z'};
  data.liveRings = [{...scope,id:'liveRing','ring-key':'live-ring','ring-id':51,'ring-number':1,'current-group':['liveGroup']}];
  data.liveGroups = [{...scope,id:'liveGroup','class-group-id':11,ring:'live-ring',status:'Underway','is-live':true,'estimated-start-time':'09:15:00'}];
  data.liveClasses = [{...scope,id:'liveClass','class-id':99,ring:['liveRing'],'detail-available':true,'current-trip-id':101}];
  data.liveEntries = [{...scope,id:'liveTrip',class:['liveClass'],'trip-id':101,'entry-id':12,'horse-id':55,'rider-id':66,'trainer-id':77,
    'participation-link-status':'current','participation-verified-at':'2026-11-12T16:59:00Z','order-of-go':1,'actual-order':3,scratch:false}];
}

test('day rollover accepts signed priorities and retains past schedule days in the full feed',async () => {
  const api=memory();
  api.data.days[0]['day-priority']=-1;
  api.data.days[1]['day-priority']=0;
  const scope=await sharedScope(api);
  assert.equal(scope.day0.date,other);
  assert.deepEqual(scope.days.map(d=>d['day-priority']),[-1,0]);
  await refreshScheduleFeed(api,'full',now);
  const full=await readScheduleFeed(api,showId,'full');
  assert.deepEqual(full.days.map(d=>d.date),[date,other]);
  assert.equal(full.records.some(r=>r.key===scheduleKey),true);
  await refreshScheduleFeed(api,'day0',now);
  assert.equal((await readScheduleFeed(api,showId,'day0')).date,other);
  assert.deepEqual((await refreshScheduleFeed(api,'full',now)).upserts,[]);
});

test('signed priorities still reject malformed values and ambiguous Day 0',async () => {
  for(const priority of [undefined,null,'-1',-0.5,Number.NaN,Number.MAX_SAFE_INTEGER+1]) {
    const api=memory();api.data.days[1]['day-priority']=priority;
    await assert.rejects(sharedScope(api),/invalid_shared_days/);
    assert.deepEqual(api.writes,[]);
  }
  for(const priorities of [[-2,-1],[0,0]]) {
    const api=memory();api.data.days.forEach((d,i)=>d['day-priority']=priorities[i]);
    await assert.rejects(sharedScope(api),/invalid_shared_day0/);
    assert.deepEqual(api.writes,[]);
  }
});

test('full upsert is Data-only, reuses date keys and preserves an unchanged Day 0 diff',async () => {
  const api = memory();live(api.data);
  assert.equal((await refreshScheduleFeed(api,'full',now)).upserts.length,2);
  assert.equal(api.reads.some(n => n.startsWith('live')),false);
  const full = await readScheduleFeed(api,showId,'full');
  assert.equal(validDataset(full,showId,'full'),true);
  assert.equal(full.records[0].entries[0].order,2);
  const first = api.data.feed.map(r => r.id);
  assert.equal((await refreshScheduleFeed(api,'day0',now)).changed,true);
  const diff = await readScheduleFeed(api,showId,'day0');
  assert.equal(diff.records,undefined);
  assert.deepEqual(diff.entries[0].upsert.map(e => e.key),[tripKey]);
  assert.equal(diff.rows.upsert[0].entries,undefined);
  assert.equal(applyDay0Diff(full,diff).records[0].entries[0].order,1);
  assert.equal(applyDay0Diff(full,diff).records[0].status,'upc');
  assert.equal(applyDay0Diff(full,diff).records[0].live.currentTripKey,tripKey);
  assert.equal((await refreshScheduleFeed(api,'day0','2026-11-12T17:01:00Z')).changed,false);
  api.data.schedules[1].name = 'Updated note';
  assert.equal((await refreshScheduleFeed(api,'full','2026-11-12T17:02:00Z')).upserts.length,1);
  assert.deepEqual(api.data.feed.map(r => r.id),first);
  assert.deepEqual(await readScheduleFeed(api,showId,'day0'),diff);
  assert.equal((await readScheduleFeed(api,showId,'full')).records[1].name,'Updated note');
});

test('full feed keeps all snapshot trips when normalized trip records are missing; Day 0 remains strict',async()=>{
  const api=memory();
  const first={...rawTrip,estimated_go_time:'09:03:00',gone_in:1};
  const second={...rawTrip,entryxclasses_uuid:'DEF',trip_id:102,order_of_go:3,estimated_go_time:'09:07:00',scratch_trip:1};
  api.data.classes[0]['source-json'].trips=[first,second];
  api.data.classes[0]['entry-count']=2;
  api.data.schedules[0]['source-json'].total_trips=2;
  api.data.trips=[];
  await refreshScheduleFeed(api,'full',now);
  const full=await readScheduleFeed(api,showId,'full'), entries=full.records[0].entries;
  assert.equal(entries.length,2);
  assert.deepEqual(entries.map(e=>e.key),[tripKey,classKey+'|12|DEF']);
  assert.deepEqual(entries.map(e=>e.name),['Horse','Horse']);
  assert.deepEqual(entries.map(e=>e.go),['9:03A','9:07A']);
  assert.equal(entries[0].goneIn,1);assert.equal(entries[1].status,'scratch');
  assert.deepEqual((await refreshScheduleFeed(api,'full',now)).upserts,[]);
  await assert.rejects(refreshScheduleFeed(api,'day0',now),/incomplete_shared_class_trips/);
});

test('full snapshot content remains authoritative while optional native IDs are retained',async()=>{
  const api=memory();
  api.data.trips[0]['source-json']={...rawTrip,horse_id:999,order_of_go:99};
  await refreshScheduleFeed(api,'full',now);
  const entry=(await readScheduleFeed(api,showId,'full')).records[0].entries[0];
  assert.equal(entry.tripId,'trip');assert.equal(entry.order,2);assert.equal(entry.name,'Horse');
});

test('duplicate snapshot trip identities reject the full feed before writing',async()=>{
  const api=memory();
  api.data.classes[0]['source-json'].trips.push({...rawTrip});
  await assert.rejects(refreshScheduleFeed(api,'full',now),/invalid_shared_trip_identity/);
  assert.deepEqual(api.writes,[]);
});

test('full feed includes November 15 at priority 4 once shared rows arrive',async () => {
  const api = memory();
  const sunday = '2026-11-15';
  api.data.days.push({id:'rec00000000000004',show:showKey,date:sunday,'show-day-key':dayKey(sunday),'day-priority':4});
  const pendingRun=await refreshScheduleFeed(api,'full',now);
  assert.equal(pendingRun.pendingDays[0].date,sunday);
  const pendingFull=await readScheduleFeed(api,showId,'full');
  assert.equal(pendingFull.days.find(d=>d.date===sunday).pending,true);
  assert.equal(pendingFull.metadata.pendingDays[0].sourceRowCount,0);
  const ids=api.data.feed.map(r=>r.id);
  const schedule=api.data.schedules[0], cls=api.data.classes[0], trip=api.data.trips[0];
  const sundayClass=classKey.replace(date,sunday);
  api.data.schedules.push({...schedule,id:'sunday-schedule','schedule-key':scheduleKey.replace(date,sunday),
    'w-days':dayKey(sunday),ring:ringKey(sunday),group:schedule.group.replace(date,sunday),class:sundayClass});
  api.data.classes.push({id:'sunday-class','class-key':sundayClass,'w-shows':showKey,'w-days':dayKey(sunday),
    ring:ringKey(sunday),'class-id':99,'record-status':'Active'});
  assert.equal((await refreshScheduleFeed(api,'full',now)).pendingDays[0].sourceRowCount,1);
  Object.assign(api.data.classes.at(-1),{...cls,id:'sunday-class','class-key':sundayClass,
    'w-days':dayKey(sunday),ring:ringKey(sunday),trips:['sunday-trip']});
  api.data.trips.push({...trip,id:'sunday-trip','trip-key':tripKey.replace(date,sunday),class:sundayClass});
  assert.deepEqual((await refreshScheduleFeed(api,'full',now)).upserts,[dayKey(sunday)]);
  assert.deepEqual(api.data.feed.map(r=>r.id),ids);
  assert.deepEqual((await refreshScheduleFeed(api,'full',now)).upserts,[]);
  const full = await readScheduleFeed(api,showId,'full');
  assert.equal(validDataset(full,showId,'full'),true);
  assert.deepEqual(full.days.map(day => day.date),[date,other,sunday]);
  assert.equal(full.records.length,3);
  assert.equal(full.records.find(r=>r.date===sunday).entries[0].key,tripKey.replace(date,sunday));
  assert.deepEqual(full.metadata.pendingDays,[]);
  const diff = await readScheduleFeed(api,showId,'day0');
  assert.equal(diff.date,date);
  assert.equal(diff.records,undefined);
});

test('missing, stale, ambiguous or wrong-scope Live clears the prior overlay without changing Data',async () => {
  for (const reject of [
    data => data.liveEntries[0]['source-updated-at']='2026-11-12T16:40:00Z',
    data => data.liveEntries.push({...data.liveEntries[0],id:'duplicate'}),
    data => data.liveEntries[0]['horse-id']=999,
    data => data.liveEntries[0]['participation-link-status']='historical',
    data => {for(const k of ['liveRings','liveGroups','liveClasses','liveEntries'])data[k]=[];}
  ]) {
    const api = memory();live(api.data);
    await refreshScheduleFeed(api,'full',now);
    const full = await readScheduleFeed(api,showId,'full');
    await refreshScheduleFeed(api,'day0',now);
    reject(api.data);
    await refreshScheduleFeed(api,'day0','2026-11-12T17:01:00Z');
    const next = await readScheduleFeed(api,showId,'day0');
    assert.equal(next.entries.length,0);
    assert.equal(applyDay0Diff(full,next).records[0].entries[0].order,2);
  }
});

test('scope rollover and changed base reject an old diff; browser fetches the matching full package',async () => {
  const api = memory();live(api.data);
  await refreshScheduleFeed(api,'full',now);
  await refreshScheduleFeed(api,'day0',now);
  const oldFull = await readScheduleFeed(api,showId,'full'), oldDiff = await readScheduleFeed(api,showId,'day0');
  api.data.schedules[0]['start-time']='09:30:00';
  await refreshScheduleFeed(api,'full','2026-11-12T17:01:00Z');
  await refreshScheduleFeed(api,'day0','2026-11-12T17:01:00Z');
  const newer = await readScheduleFeed(api,showId,'full');
  assert.throws(() => applyDay0Diff(newer,oldDiff),/version_mismatch/);
  const calls=[], cache=new Map([['full',oldFull]]);
  const device=createDeviceData({showId,endpoint:'https://example.test/feed',onData:()=>{},
    storage:{read:async(s,l)=>cache.get(l),commit:async(s,l,p)=>(cache.set(l,p),p)},
    fetchImpl:async url => {const lane=url.searchParams.get('lane');calls.push(lane);return Response.json(await readScheduleFeed(api,showId,lane));}});
  const stop=device.start();
  try {
    await stop.ready;
    assert.deepEqual(calls,['day0','full']);
    assert.equal(device.current.records[0].time,'9:30A');
    assert.equal(device.current.records[0].entries[0].order,1);
    assert.deepEqual(device.current.records[1],oldFull.records[1]);
  } finally {stop();}
  api.data.days[0]['day-priority']=999;
  api.data.days[1]['day-priority']=0;
  await refreshScheduleFeed(api,'full','2026-11-13T17:00:00Z');
  const moved = await readScheduleFeed(api,showId,'full');
  assert.equal(moved.metadata.defaultDate,other);
  assert.deepEqual(moved.days.map(d => d.date),[date,other]);
  assert.equal(moved.records[0].entries[0].order,2);
  assert.throws(() => applyDay0Diff(moved,oldDiff),/version_mismatch/);
  assert.equal((await readScheduleFeed(api,showId,'day0')).entries.length,0);
});

test('incomplete input keeps confirmed records and queues admin review without stopping the next lane',async () => {
  const api=memory();
  await refreshScheduleFeed(api,'full',now);
  const saved=structuredClone(api.data.feed), records=(await readScheduleFeed(api,showId,'full')).records;
  const missing=api.data.schedules.shift();
  const failed=await runScheduleRefresh(api,{lane:'full',slot:now,clock:()=>now});
  assert.equal(failed.log.result,'Success');
  assert.equal(api.data.conflicts.some(r=>r.reason.startsWith('unconfirmed_empty_day:')),true);
  assert.deepEqual((await readScheduleFeed(api,showId,'full')).records,records);
  assert.equal(api.data.errors.length,0);
  assert.equal(api.data.heartbeat[0].result,'Success');
  api.data.schedules.unshift(missing);
  const next=await runScheduleRefresh(api,{lane:'day0',slot:now,clock:()=>now});
  assert.equal(next.log.result,'Success');
  assert.equal(JSON.parse(next.log.summary).changed,false);
  assert.deepEqual((await readScheduleFeed(api,showId,'full')).records,records);
  await assert.rejects(runScheduleRefresh(api,{lane:'day0',slot:now,clock:()=>now}),/already_recorded/);
});

test('participant conflicts use per-trip snapshot identity, queue review once and preserve admin edits',async()=>{
  const api=memory();await refreshScheduleFeed(api,'full',now);
  api.data.trips[0].rider='88, 66';
  const first=await runScheduleRefresh(api,{lane:'day0',slot:now,clock:()=>now});
  assert.equal(first.log.result,'Success');assert.equal(api.data.conflicts.length,1);
  assert.equal(api.data.conflicts[0].reason,'shared_participant_conflict');
  const effective=applyDay0Diff(await readScheduleFeed(api,showId,'full'),await readScheduleFeed(api,showId,'day0'));
  assert.equal(effective.records[0].entries[0].riderId,'rider');
  api.data.conflicts[0]['admin-status']='Reviewing';api.data.conflicts[0]['admin-notes']='Admin owns this';
  const before=api.writes.filter(r=>r.name==='conflicts').length;
  const repeat=await runScheduleRefresh(api,{lane:'day0',slot:'2026-11-12T17:03:00Z',clock:()=>now});
  assert.equal(repeat.log.result,'Success');assert.equal(api.writes.filter(r=>r.name==='conflicts').length,before);
  assert.equal(api.data.conflicts[0]['admin-notes'],'Admin owns this');
});

test('class conflicts keep confirmed day data while unrelated dates update',async()=>{
  const api=memory();await refreshScheduleFeed(api,'full',now);
  const original=(await readScheduleFeed(api,showId,'full')).records[0];
  api.data.classes[0]['entry-count']=null;
  api.data.schedules[1].name='Updated unaffected date';
  const run=await runScheduleRefresh(api,{lane:'full',slot:now,clock:()=>now});
  assert.equal(run.log.result,'Success');
  assert.equal(api.data.conflicts.some(r=>r['record-key']===classKey&&r.reason==='invalid_shared_class'),true);
  const full=await readScheduleFeed(api,showId,'full');
  assert.deepEqual(full.records[0],{...original,detailsMissing:true});assert.equal(full.records[1].name,'Updated unaffected date');
  assert.equal(api.data.dataRows.some(r=>r['class-name']==='Updated unaffected date'),true);
});

test('November 15 schedule publishes with missing class details and publication exclusions stay separate',async()=>{
  const api=memory(),sunday='2026-11-15';
  api.data.days.push({id:'rec00000000000009',show:showKey,date:sunday,'show-day-key':dayKey(sunday),'day-priority':3});
  const cases=[['200031576','Flat',9,'Under Saddle/Flat'],['200031558','Schooling pony',999,'Over Fences'],['200031999','First trip',1,'Over Fences']];
  for(const [id,flag,priority,sequence]of cases){
    const key=ringKey(sunday)+'|'+id;
    api.data.classes.push({...api.data.classes[0],id:'class-'+id,'class-key':key,'class-id':Number(id),'w-days':dayKey(sunday),ring:ringKey(sunday),'entry-count':undefined,'source-json':undefined,trips:[]});
    api.data.schedules.push({...api.data.schedules[0],id:'schedule-'+id,'schedule-key':key+'0',class:key,ring:ringKey(sunday),'w-days':dayKey(sunday),group:ringKey(sunday)+'|11','source-position':Number(id),
      'rs-flag':flag,'rs-priority':priority,'rs-lever':'lever1',schedule_sequencetype:sequence,class_type:'Hunters',
      'source-json':{class_id:Number(id),class_groupxclasses_id:Number(id+'0'),class_group_id:11,group_name:'Known schedule',class_type:'Hunters',schedule_sequencetype:sequence,total_trips:1}});
  }
  const source=JSON.stringify([api.data.schedules,api.data.classes]);
  const run=await runScheduleRefresh(api,{lane:'full',slot:now,clock:()=>now});
  assert.equal(run.log.result,'Success');assert.deepEqual(JSON.parse(run.log.summary).pendingDays,[]);
  const full=await readScheduleFeed(api,showId,'full'),rows=full.records.filter(r=>r.date===sunday);
  assert.equal(rows.length,3);assert.equal(rows.every(r=>r.detailsMissing&&r.entryCount===null&&r.entries.length===0),true);
  assert.deepEqual(rows.map(r=>r.publication),['Optional: classes','Trip detail only','Visible']);
  assert.equal(publishedSchedule(rows).length,1);
  assert.equal(publishedSchedule(rows,{showOptionalClasses:true}).length,2);
  assert.equal(api.data.dataRows.filter(r=>r.date===sunday).length,3);
  assert.equal(api.data.conflicts.filter(r=>r.reason==='invalid_shared_class').length,3);
  assert.equal(JSON.stringify([api.data.schedules,api.data.classes]),source);
  const unchanged=await runScheduleRefresh(api,{lane:'full',slot:'2026-11-12T17:15:00Z',clock:()=>now});
  assert.equal(unchanged.log.result,'Success');assert.equal(JSON.parse(unchanged.log.summary).readable.upserts,0);
});

test('publication rules match approved precedence without a blanket priority999 exclusion',()=>{
 const base={kind:'class','record-status':'Active','rs-lever':'lever1'};
 for(const [extra,count,expected]of [
  [{'rs-flag':'No entries'},null,'Hidden'],[{},0,'Hidden'],[{kind:'break'},null,'Optional: breaks'],
  [{'rs-priority':2},1,'Trip detail only'],[{'rs-flag':'Model'},1,'Trip detail only'],
  [{'rs-flag':'Classic',class_type:'Hunters'},1,'Trip detail only'],
  [{'rs-flag':'Classic',class_type:'Jumpers'},1,'Visible'],[{'rs-priority':999},1,'Visible'],
  [{'rs-flag':'First trip','rs-priority':1},null,'Visible'],
  [{'rs-flag':'Warmup'},1,'Optional: classes'],[{'record-status':'Dropped','rs-flag':'Warmup'},1,'Hidden']
 ])assert.equal(publicationStatus({...base,...extra},count),expected);
});

test('scheduled data conflicts do not stop either lane or rewrite synced sources',async()=>{
  const api=memory();await refreshScheduleFeed(api,'full',now);
  api.data.trips[0].rider='88, 66';
  const original=JSON.stringify([api.data.shows,api.data.days,api.data.schedules,api.data.classes,api.data.trips]);
  const state={},first=await runDueFeeds({api,state,save:async()=>{},now:new Date(now)});
  assert.equal(first.stopped,null);assert.deepEqual(first.outcomes.map(r=>r.log.result),['Success','Success']);
  const reviewWrites=api.writes.filter(r=>r.name==='conflicts').length;
  const next=await runDueFeeds({api,state,save:async()=>{},now:new Date('2026-11-12T17:03:00Z')});
  assert.equal(next.stopped,null);assert.equal(next.outcomes.length,1);
  assert.equal(api.writes.filter(r=>r.name==='conflicts').length,reviewWrites);
  assert.equal(JSON.stringify([api.data.shows,api.data.days,api.data.schedules,api.data.classes,api.data.trips]),original);
});

test('review fields show a named direct class link and only observed short problems',async()=>{
  const api={read:async()=>[{id:'recDSq8H2HQ8IlnjV','class-number':'526','class-name':'Medal [Flat]'}]};
  const fields=await conflictReviewFields(api,{recordKey:classKey,reason:'invalid_shared_class',
    details:{source:'wef-data.classes',classId:99,snapshotShowId:null,snapshotClassId:null,entryCount:null}});
  assert.equal(fields.Record,'[Class — 526 — Medal \\[Flat\\]](https://airtable.com/appqWC58FLtmHcUAW/tblL2BwNM2Z8sdGDQ/recDSq8H2HQ8IlnjV)');
  assert.equal(fields['Problem 1'],'Class details are empty.');
  assert.equal(fields['Problem 2'],'Entry count is missing.');assert.equal(fields['Problem 3'],null);
});

test('review trip link points to the trip and explains the two rider values',async()=>{
  const api={read:async()=>[{id:'rec00000000000055','source-json':JSON.stringify({number:811})}]};
  const fields=await conflictReviewFields(api,{recordKey:tripKey,reason:'shared_participant_conflict',details:{source:'wef-data.trips',field:'rider',linked:'45891, 19389',snapshot:19389}});
  assert.equal(fields.Record,'[Trip — entry 811](https://airtable.com/appqWC58FLtmHcUAW/tbl9dATHFaF8z0x0I/rec00000000000055)');
  assert.equal(fields['Problem 1'],'Rider field lists multiple riders.');
  assert.equal(fields['Problem 2'],'Trip details identify rider 19389.');assert.equal(fields['Problem 3'],null);
});

test('normal runner fills older review display fields once and preserves admin links and notes',async()=>{
  const api=memory();api.data.conflicts.push({'conflict-key':'existing','record-key':classKey,reason:'invalid_shared_class',
    'details-json':JSON.stringify({source:'wef-data.classes',entryCount:null}),'admin-status':'Reviewing','admin-notes':'Keep this','wef-data.classes':['existing-link']});
  const run=await runScheduleRefresh(api,{lane:'full',slot:now,clock:()=>now});
  assert.equal(run.log.result,'Success');assert.ok(api.data.conflicts[0].Record);
  assert.equal(api.data.conflicts[0]['admin-status'],'Reviewing');assert.equal(api.data.conflicts[0]['admin-notes'],'Keep this');
  assert.deepEqual(api.data.conflicts[0]['wef-data.classes'],['existing-link']);
  const count=api.writes.filter(w=>w.name==='conflicts').length;
  await runScheduleRefresh(api,{lane:'full',slot:'2026-11-12T17:15:00Z',clock:()=>now});
  assert.equal(api.writes.filter(w=>w.name==='conflicts').length,count);
});

test('lossless compression carries an oversized full day and Day 0 stays a diff', async () => {
  const api=memory();
  const label='Jumper 🐴 é '.repeat(12000);
  api.data.schedules[1].name=label;
  await refreshScheduleFeed(api,'full',now);
  const compressed=api.data.feed.find(r=>r.date===other);
  assert.equal(JSON.parse(compressed['payload-json']).encoding,'gzip-base64-v1');
  assert.ok(compressed['payload-json'].length<=100000);
  const before=decodePayload(compressed['payload-json']);
  assert.equal(before.records[0].name,label);
  assert.deepEqual(decodePayload(encodePayload(before)),before);
  const full=await readScheduleFeed(api,showId,'full');
  assert.equal(full.records[1].name,label);
  assert.equal(full.records[0].entries.length,1);
  assert.deepEqual((await refreshScheduleFeed(api,'full',now)).upserts,[]);
  const day0Saved=structuredClone(decodePayload(api.data.feed.find(r=>r.date===date)['payload-json']));
  api.data.schedules[0].name=label;
  assert.equal((await refreshScheduleFeed(api,'day0',now)).changed,true);
  const diff=await readScheduleFeed(api,showId,'day0');
  assert.equal(diff.records,undefined);
  assert.equal(diff.rows.upsert[0].name,label);
  assert.equal(applyDay0Diff(full,diff).records[0].entries.length,1);
  const {day0Diff,...unchangedBase}=decodePayload(api.data.feed.find(r=>r.date===date)['payload-json']);
  assert.deepEqual(unchangedBase,day0Saved);
  assert.equal(day0Diff.rows.upsert[0].name,label);
  assert.equal((await refreshScheduleFeed(api,'day0',now)).changed,false);
});

test('a value still over the field limit after compression fails before writing', async () => {
  const api=memory();
  api.data.schedules[1].name=randomBytes(160000).toString('base64');
  await assert.rejects(refreshScheduleFeed(api,'full',now),/schedule_feed_payload_too_large/);
  assert.deepEqual(api.writes,[]);
});

test('diffs contain only affected rows and trips, including removals', () => {
  const day={id:'day',date,rings:[51],ringDetails:{51:{name:'Intl',number:1}}};
  const full={metadata:{showId,defaultDate:date,dayVersions:{[date]:'base'}},days:[day],ringDetails:day.ringDetails,
    records:[{key:'a',date,ring:51,entries:[{key:'t1',order:1},{key:'t2',order:2}]},{key:'b',date,ring:51,entries:[]}]};
  const {ringDetails,...dayHeader}=day;
  const base={...full,days:[dayHeader],metadata:{...full.metadata,dataVersion:'base'}};
  const after={...base,records:[{...base.records[0],entries:[{key:'t1',order:3}]},{key:'c',date,ring:51,entries:[]}]};
  const diff={...makeDay0Diff(base,after),checkedAt:now};
  assert.deepEqual(diff.rows.upsert.map(row=>row.key),['c']);
  assert.deepEqual(diff.rows.remove,['b']);
  assert.deepEqual(diff.entries[0].remove,['t2']);
  assert.deepEqual(applyDay0Diff(full,diff).records,after.records);
});

test('Airtable adapter uses only app base and rejects writes to shared tables',async () => {
  const calls=[];
  const api=sharedBase({token:'fixture',writable:true,pause:async()=>{},fetchImpl:async (url,init)=>{
    calls.push([url.pathname,init.method]);return Response.json({records:[]});}});
  await api.read('liveEntries');
  await assert.rejects(api.upsert('trips',{}),/write_forbidden/);
  assert.deepEqual(calls,[[`/v0/${APP_BASE}/tblpMWHRn7k5Kb58M`,undefined]]);
});

const testEnv = await import('cloudflare:workers').then(m => m.env,() => null);
test('Astro routes serve the saved full feed and only a diff for Day 0', {skip:!testEnv}, async t => {
  const api=memory();live(api.data);
  api.data.schedules[0].name='Large class 🐴 '.repeat(12000);
  await refreshScheduleFeed(api,'full',now);
  await refreshScheduleFeed(api,'day0',now);
  testEnv.AIRTABLE_TOKEN='fixture';
  const tables={tblYkodPn0xSLnXac:'shows',tblzRTVjHbR8HvMP1:'days',tblzsgMl5UNsPMk5W:'feed'};
  t.mock.method(globalThis,'fetch',async (url,options) => {
    const [,,base,table]=url.pathname.split('/');
    assert.equal(base,APP_BASE);
    assert.ok(tables[table]);
    assert.equal(options.method,undefined);
    return Response.json({records:filter(api.data[tables[table]],url.searchParams.get('filterByFormula') ?? '')
      .map(({id,...fields},i) => ({id:/^rec[A-Za-z0-9]{14}$/.test(id) ? id : 'rec'+String(i+2).padStart(14,'0'),fields}))});
  });
  const {GET:scopeGet}=await import('../src/pages/wef-schedule/scope.js');
  const {GET:dataGet}=await import('../src/pages/wef-schedule/data.js');
  const scope=await (await scopeGet()).json();
  assert.equal(scope.showId,showId);
  assert.equal(scope.defaultDate,date);
  const get=lane=>dataGet({request:new Request('https://example.test/test/wef-schedule/data?show='+showId+'&lane='+lane)});
  const fullResponse=await get('full'), day0Response=await get('day0');
  assert.equal(fullResponse.status,200);
  assert.equal(day0Response.status,200);
  const full=await fullResponse.json(),diff=await day0Response.json();
  assert.equal(validDataset(full,showId,'full'),true);
  assert.equal(diff.records,undefined);
  assert.equal(full.records[0].name,api.data.schedules[0].name);
  assert.equal(JSON.parse(api.data.feed[0]['payload-json']).encoding,'gzip-base64-v1');
  assert.equal(applyDay0Diff(full,diff).records[0].entries[0].order,1);
  api.data.feed.pop();
  assert.equal((await get('full')).status,503);
});

test('older shared capture projects both dates but malformed coverage still blocks publication', {skip:!process.env.WEF_SHARED_FIXTURE}, async () => {
  const {captured,coverage}=JSON.parse(await readFile(process.env.WEF_SHARED_FIXTURE,'utf8'));
  const api=memory(captured), scope=await sharedScope(api);
  for (const [d,rowCount,tripCount] of [['2026-11-11',14,0],['2026-11-12',23,46]]) {
    const day=scope.days.find(day=>day.date===d);
    const {dataset}=await buildSharedDay(api,scope,day,'2026-11-10T17:20:00Z');
    assert.equal(validDataset(dataset,scope.show.id,'day0'),true);
    assert.equal(dataset.records.length,rowCount);
    assert.equal(dataset.records.flatMap(r=>r.entries).length,tripCount);
  }
  api.data.schedules=coverage;
  await assert.rejects(refreshScheduleFeed(api,'full',now),/invalid_shared_break/);
  assert.deepEqual(api.writes,[]);
});

test('pending source retains the last good day and does not rewrite unchanged markers',async()=>{
  const api=memory();
  await refreshScheduleFeed(api,'full',now);
  const before=await readScheduleFeed(api,showId,'full');
  const ids=api.data.feed.map(r=>r.id);
  const missing=api.data.schedules.pop();
  assert.equal((await refreshScheduleFeed(api,'full',now)).pendingDays[0].date,other);
  const pending=await readScheduleFeed(api,showId,'full');
  assert.deepEqual(pending.records,before.records);
  assert.equal(pending.metadata.pendingDays[0].hasSavedData,true);
  assert.deepEqual((await refreshScheduleFeed(api,'full',now)).upserts,[]);
  assert.equal((await readScheduleFeed(api,showId,'day0')).records,undefined);
  api.data.schedules.push(missing);
  assert.deepEqual((await refreshScheduleFeed(api,'full',now)).upserts,[dayKey(other)]);
  assert.deepEqual(api.data.feed.map(r=>r.id),ids);
  assert.deepEqual((await readScheduleFeed(api,showId,'full')).metadata.pendingDays,[]);
});


test('readable full rows preserve originals and skip unchanged writes, including flag-only changes',async()=>{
 const api=memory(), source=structuredClone(api.data.schedules);
 await refreshScheduleFeed(api,'full',now);
 assert.equal((await refreshReadableSchedule(api,'full',now)).upserts,2);
 const row=api.data.dataRows.find(r=>r['schedule-key']===scheduleKey);
 const first=row.id, record=row['record-json'];
 assert.equal(JSON.parse(record).entries[0].key,tripKey);
 assert.equal((await refreshReadableSchedule(api,'full',now)).upserts,0);
 api.data.schedules[0]['rs-flag']='No entries';
 assert.equal((await refreshReadableSchedule(api,'full',now)).upserts,1);
 assert.equal(row['rs-flag'],'No entries');assert.equal(row.id,first);assert.equal(row['record-json'],record);
 assert.equal(Object.keys(row).some(k=>k.startsWith('publication-')||k==='publish-default'),false);
 assert.deepEqual(api.data.schedules.map(({['rs-flag']:ignored,...r})=>r),source);
});

test('readable Day0 carries accepted Live, skips repeats and clears obsolete live fields',async()=>{
 const api=memory();live(api.data);
 await refreshScheduleFeed(api,'full',now);await refreshReadableSchedule(api,'full',now);
 await refreshScheduleFeed(api,'day0',now);
 assert.equal((await refreshReadableSchedule(api,'day0',now)).upserts,1);
 const row=api.data.day0Rows[0];assert.equal(JSON.parse(row['record-json']).entries[0].order,1);
 assert.equal(row['diff-applied'],true);assert.equal(JSON.parse(row['live-state-json']).currentTripKey,tripKey);
 assert.deepEqual(row['baseline-record'],[api.data.dataRows[0].id]);
 assert.equal((await refreshReadableSchedule(api,'day0',now)).upserts,0);
 for(const k of ['liveRings','liveGroups','liveClasses','liveEntries'])api.data[k]=[];
 await refreshScheduleFeed(api,'day0','2026-11-12T17:01:00Z');
 assert.equal((await refreshReadableSchedule(api,'day0',now)).upserts,1);
 assert.equal(row['live-state-json'],null);assert.equal(row['diff-applied'],false);
 assert.equal(JSON.parse(row['record-json']).entries[0].order,2);
 assert.equal((await refreshReadableSchedule(api,'day0',now)).upserts,0);
});

test('Day0 rollover retains and hides prior-day rows without deleting them',async()=>{
 const api=memory();await refreshScheduleFeed(api,'full',now);await refreshReadableSchedule(api,'full',now);
 await refreshReadableSchedule(api,'day0',now);const old=structuredClone(api.data.day0Rows[0]);
 api.data.days[0]['day-priority']=1;api.data.days[1]['day-priority']=0;
 await refreshScheduleFeed(api,'day0',now);const result=await refreshReadableSchedule(api,'day0',now);
 assert.equal(result.records,1);assert.equal(result.retired,1);assert.equal(api.data.day0Rows.length,2);
 assert.equal(api.data.day0Rows[0]['stage-status'],'Outside current scope');
 assert.equal(api.data.day0Rows[0]['record-json'],old['record-json']);
 assert.equal((await refreshReadableSchedule(api,'day0',now)).upserts,0);
});

test('pending full days retain their prior readable records and metadata',async()=>{
 const api=memory();await refreshScheduleFeed(api,'full',now);await refreshReadableSchedule(api,'full',now);
 const before=structuredClone(api.data.dataRows);api.data.schedules.pop();
 await refreshScheduleFeed(api,'full',now);
 assert.equal((await refreshReadableSchedule(api,'full',now)).upserts,0);
 assert.deepEqual(api.data.dataRows,before);
});

test('confirmed removal retires only the derived occurrence and repeat does not rewrite it',async()=>{
 const api=memory(), clone=structuredClone(api.data.schedules[0]);clone.id='second';clone['schedule-key']=ringKey(date)+'|26';clone['source-position']=1;clone['source-json'].class_groupxclasses_id=26;api.data.schedules.push(clone);
 await refreshScheduleFeed(api,'full',now);await refreshReadableSchedule(api,'full',now);
 api.data.schedules.pop();await refreshScheduleFeed(api,'full',now);
 assert.equal((await refreshReadableSchedule(api,'full',now)).retired,1);
 assert.equal(api.data.dataRows.length,3);
 const retired=api.data.dataRows.find(r=>r['schedule-key']===clone['schedule-key']);
 assert.equal(retired['stage-status'],'Removed from current source');assert.ok(retired['record-json']);
 assert.equal((await refreshReadableSchedule(api,'full',now)).upserts,0);
});

test('derived failure logs Error and stage even after Data save succeeds',async()=>{
 const api=memory();api.upsertRows=async()=>{throw Error('readable_write_readback');};
 const result=await runScheduleRefresh(api,{lane:'full',slot:now,clock:()=>now});
 assert.equal(result.log.result,'Error');assert.equal(result.log.errors,'readable_write_readback');
 assert.equal(JSON.parse(result.log.summary).stage,'readable');assert.equal(api.data.errors.length,1);
 assert.equal(api.data.heartbeat[0].result,'Error');assert.equal(api.data.feed.length,2);
});

test('invalid readable source identity fails before any derived write',async()=>{
 const api=memory();await refreshScheduleFeed(api,'full',now);api.data.schedules[0].id='changed-native-id';
 await assert.rejects(refreshReadableSchedule(api,'full',now),/readable_source_identity/);
 assert.equal(api.data.dataRows.length,0);
});

test('identity conflict still rejects incompatible entries with a specific error',async()=>{
 const api=memory();api.data.classes[0]['source-json'].trips.push({...rawTrip,entryxclasses_uuid:'SECOND',number:999});
 await assert.rejects(refreshScheduleFeed(api,'full',now),/conflicting_shared_entry_identity/);
 assert.deepEqual(api.writes,[]);
});

test('derived batch adapter normalizes false and empty links and forbids synced writes',async()=>{
 let saved=[],calls=0;const api=sharedBase({token:'fixture',writable:true,pause:async()=>{},fetchImpl:async(url,init)=>{
  calls++;if(init.method==='PATCH'){saved=JSON.parse(init.body).records.map((r,i)=>({id:'rec'+String(i+100).padStart(14,'0'),fields:r.fields}));return Response.json({records:saved});}
  return Response.json({records:saved.map(r=>({id:r.id,fields:Object.fromEntries(Object.entries(r.fields).filter(([,v])=>v!==false&&!(Array.isArray(v)&&!v.length)))}))});
 }});
 await api.upsertRows('day0Rows',[{'schedule-key':'key','diff-applied':false,'baseline-record':[],'rs-priority':0}]);
 assert.equal(calls,2);
 await assert.rejects(api.upsertRows('schedules',[{'schedule-key':'key'}]),/invalid_readable_batch/);
 await assert.rejects(api.upsertRows('day0Rows',[{'schedule-key':'key','publish-default':'Yes'}]),/invalid_readable_write/);
 assert.equal(calls,2);
});

test('review link readback accepts Airtable terminal newline but rejects changed links and problem text',async()=>{
  let stored,change='newline';
  const fields={'conflict-key':'review',Record:'[Class 526](https://airtable.com/appqWC58FLtmHcUAW/tblL2BwNM2Z8sdGDQ/recDSq8H2HQ8IlnjV)','Problem 1':'Class details are empty.'};
  const api=sharedBase({token:'fixture',writable:true,pause:async()=>{},fetchImpl:async(url,init)=>{
    if(init.method==='PATCH'){stored=JSON.parse(init.body).records[0].fields;return Response.json({records:[{id:'rec00000000000055',fields:stored}]});}
    const returned={...stored,Record:stored.Record+'\n'};
    if(change==='link')returned.Record=returned.Record.replace('recDSq8H2HQ8IlnjV','rec00000000000066');
    if(change==='problem')returned['Problem 1']+='\n';
    return Response.json({records:[{id:'rec00000000000055',fields:returned}]});
  }});
  await api.upsert('conflicts',fields);
  change='link';await assert.rejects(api.upsert('conflicts',fields),/schedule_app_write_readback/);
  change='problem';await assert.rejects(api.upsert('conflicts',fields),/schedule_app_write_readback/);
});


test('group-name conflicts publish every occurrence, retain its own name and keep both lanes running',async()=>{
 const api=memory();await refreshScheduleFeed(api,'full',now);
 api.data.schedules[0].name='Current scheduled class';
 api.data.schedules.push({...structuredClone(api.data.schedules[0]),id:'rec00000000000010',
  'schedule-key':ringKey(date)+'|26','source-position':1,name:'Second occurrence',
  'source-json':{...api.data.schedules[0]['source-json'],class_groupxclasses_id:26,group_name:'Jumper & Young Jumper'}});
 const source=JSON.stringify([api.data.schedules,api.data.classes,api.data.trips]);
 const run=await runScheduleRefresh(api,{lane:'full',slot:now,clock:()=>now});
 assert.equal(run.log.result,'Success');assert.deepEqual(JSON.parse(run.log.summary).pendingDays,[]);
 const full=await readScheduleFeed(api,showId,'full'),rows=full.records.filter(r=>r.date===date);
 assert.deepEqual(rows.map(r=>r.name),['Current scheduled class','Second occurrence']);
 assert.deepEqual(rows.map(r=>r.group),['Jumper','Jumper & Young Jumper']);
 assert.equal(rows.every(r=>r.entries.length===1),true);
 const conflict=api.data.conflicts.find(r=>r.reason==='conflicting_shared_group_identity');
 assert.ok(conflict.Record.includes('/tbljK5BImDJnQcNB4/rec00000000000010'));
 assert.equal(conflict['Problem 1'],'The same group has different names.');
 assert.equal((await runScheduleRefresh(api,{lane:'day0',slot:now,clock:()=>now})).log.result,'Success');
 const count=api.writes.length;
 const repeat=await runScheduleRefresh(api,{lane:'full',slot:'2026-11-12T17:15:00Z',clock:()=>now});
 assert.equal(repeat.log.result,'Success');
 assert.equal(api.writes.slice(count).some(w=>['feed','dataRows','conflicts'].includes(w.name)),false);
 assert.equal(JSON.stringify([api.data.schedules,api.data.classes,api.data.trips]),source);
});

test('missing class trip arrays never hold back current schedule fields or derived rows',async()=>{
 const api=memory();await refreshScheduleFeed(api,'full',now);
 api.data.classes[0]['source-json'].trips=undefined;
 api.data.schedules[0].name='New class name';api.data.schedules[0]['start-time']='10:30:00';
 const source=JSON.stringify([api.data.schedules,api.data.classes,api.data.trips]);
 const run=await runScheduleRefresh(api,{lane:'full',slot:now,clock:()=>now});
 assert.equal(run.log.result,'Success');assert.deepEqual(JSON.parse(run.log.summary).pendingDays,[]);
 const row=(await readScheduleFeed(api,showId,'full')).records[0];
 assert.equal(row.name,'New class name');assert.equal(row.time,'10:30A');assert.equal(row.entries.length,1);
 assert.equal(api.data.dataRows.find(r=>r['schedule-key']===scheduleKey)['class-name'],'New class name');
 assert.equal(api.data.conflicts.some(r=>r.reason==='incomplete_shared_class_trips'),true);
 assert.equal(JSON.stringify([api.data.schedules,api.data.classes,api.data.trips]),source);
});

test('Day0 applies available changes without removing confirmed trips missing normalized details',async()=>{
 const api=memory(),second={...rawTrip,entryxclasses_uuid:'DEF',trip_id:102,order_of_go:3};
 api.data.classes[0]['source-json'].trips.push(second);api.data.classes[0]['entry-count']=2;api.data.schedules[0]['source-json'].total_trips=2;
 await refreshScheduleFeed(api,'full',now);
 api.data.trips[0]['gone-in']=1;api.data.schedules[0].name='Current schedule';
 const run=await runScheduleRefresh(api,{lane:'day0',slot:now,clock:()=>now});assert.equal(run.log.result,'Success');
 const full=await readScheduleFeed(api,showId,'full'),diff=await readScheduleFeed(api,showId,'day0'),row=applyDay0Diff(full,diff).records[0];
 assert.equal(row.name,'Current schedule');assert.equal(row.entries.length,2);assert.equal(row.entries.find(e=>e.key===tripKey).goneIn,1);
 assert.deepEqual(diff.rows.remove,[]);assert.equal(diff.entries.every(e=>e.remove.length===0),true);
 assert.equal(api.data.day0Rows.find(r=>r['schedule-key']===scheduleKey)['trip-count'],2);
 const count=api.writes.length;
 assert.equal((await runScheduleRefresh(api,{lane:'day0',slot:'2026-11-12T17:03:00Z',clock:()=>now})).log.result,'Success');
 assert.equal(api.writes.slice(count).some(w=>['feed','day0Rows','conflicts'].includes(w.name)),false);
});

test('unreadable class or trip details isolate the concern instead of freezing schedule publication',async()=>{
 for(const table of ['classes','trips']){
  const api=memory();await refreshScheduleFeed(api,'full',now);
  api.data[table][0]['source-json']='{not json';api.data.schedules[0].name='Updated despite bad details';
  const lane=table==='classes'?'full':'day0',run=await runScheduleRefresh(api,{lane,slot:now,clock:()=>now});
  assert.equal(run.log.result,'Success');
  const full=await readScheduleFeed(api,showId,'full'),data=lane==='full'?full:applyDay0Diff(full,await readScheduleFeed(api,showId,'day0'));
  assert.equal(data.records[0].name,'Updated despite bad details');assert.equal(data.records[0].entries.length,1);
  assert.equal(api.data.conflicts.some(r=>r.reason==='invalid_shared_json'),true);
 }
});

test('a bad trip does not discard its valid class peers or their new fields',async()=>{
 const api=memory(),second={...rawTrip,entryxclasses_uuid:'DEF',trip_id:102,order_of_go:3};
 api.data.classes[0]['source-json'].trips.push(second);api.data.classes[0]['entry-count']=2;
 api.data.classes[0].trips.push('second');
 api.data.trips.push({...structuredClone(api.data.trips[0]),id:'second','trip-key':classKey+'|12|DEF','trip-id':102,'source-uuid':'DEF','source-json':second});
 await refreshScheduleFeed(api,'full',now);
 api.data.trips[0]['gone-in']=1;api.data.trips[1]['source-json']='{broken';
 const run=await runScheduleRefresh(api,{lane:'day0',slot:now,clock:()=>now});assert.equal(run.log.result,'Success');
 const full=await readScheduleFeed(api,showId,'full'),row=applyDay0Diff(full,await readScheduleFeed(api,showId,'day0')).records[0];
 assert.equal(row.entries.length,2);assert.equal(row.entries.find(e=>e.key===tripKey).goneIn,1);
 assert.equal(api.data.conflicts.some(r=>r.reason==='invalid_shared_json'),true);
});


test('malformed snapshot trip items keep good trips and schedule rows, including repeat publication',async()=>{
 const api=memory();api.data.classes[0]['source-json'].trips.push(null);api.data.schedules[0].name='Available schedule';
 const run=await runScheduleRefresh(api,{lane:'full',slot:now,clock:()=>now});
 assert.equal(run.log.result,'Success');assert.deepEqual(JSON.parse(run.log.summary).pendingDays,[]);
 const row=(await readScheduleFeed(api,showId,'full')).records[0];
 assert.equal(row.name,'Available schedule');assert.equal(row.entries.length,1);
 assert.equal(api.data.conflicts.length>0,true);
 assert.equal((await runScheduleRefresh(api,{lane:'full',slot:'2026-11-12T17:15:00Z',clock:()=>now})).log.result,'Success');
 assert.equal((await readScheduleFeed(api,showId,'full')).records[0].entries.length,1);
 const fast=await runScheduleRefresh(api,{lane:'day0',slot:now,clock:()=>now});
 assert.equal(fast.log.result,'Success');
 assert.equal((await readScheduleFeed(api,showId,'day0')).rows.remove.length,0);
});

test('a fresh Day0 diff updates readable rows despite a legacy pending full-day marker',async()=>{
 const api=memory();await runScheduleRefresh(api,{lane:'full',slot:now,clock:()=>now});
 const saved=api.data.feed.find(r=>r.date===date),data=decodePayload(saved['payload-json']);
 data.pending={date,reason:'conflicting_shared_group_identity',sourceRowCount:1};saved['payload-json']=encodePayload(data);
 api.data.day0Rows.push({...structuredClone(api.data.dataRows[0]),id:'existing-fast-row','class-name':'Stale derived name'});
 await runScheduleRefresh(api,{lane:'day0',slot:now,clock:()=>now});
 assert.equal(api.data.day0Rows.find(r=>r['schedule-key']===scheduleKey)['class-name'],'Jumper');
 api.data.schedules[0].name='Fast-lane current schedule';
 const run=await runScheduleRefresh(api,{lane:'day0',slot:'2026-11-12T17:03:00Z',clock:()=>now});
 assert.equal(run.log.result,'Success');
 assert.equal(api.data.day0Rows.find(r=>r['schedule-key']===scheduleKey)['class-name'],'Fast-lane current schedule');
 assert.equal((await readScheduleFeed(api,showId,'full')).metadata.pendingDays.length,0);
});

test('an older source clock queues review but cannot hold back current scheduled fields',async()=>{
 const api=memory();await refreshScheduleFeed(api,'full',now);
 api.data.schedules[0].name='Current shared values';api.data.schedules[0]['last-success']='2026-11-11T09:00:00Z';
 const run=await runScheduleRefresh(api,{lane:'full',slot:now,clock:()=>now});
 assert.equal(run.log.result,'Success');assert.equal(api.data.conflicts.some(r=>r.reason==='older_shared_dataset'),true);
 assert.equal((await readScheduleFeed(api,showId,'full')).records[0].name,'Current shared values');
});

test('repeat conflict logging reads existing concerns once and preserves admin decisions',async()=>{
 const api=memory();
 for(let i=1;i<=12;i++)api.data.schedules.push({...structuredClone(api.data.schedules[0]),id:'rec'+String(i+100).padStart(14,'0'),
  'schedule-key':ringKey(date)+'|'+(25+i),'source-position':i,
  'source-json':{...api.data.schedules[0]['source-json'],class_groupxclasses_id:25+i,group_name:'Different group '+i}});
 assert.equal((await runScheduleRefresh(api,{lane:'full',slot:now,clock:()=>now})).log.result,'Success');
 assert.equal(api.data.conflicts.length,12);
 api.data.conflicts[0]['admin-status']='Reviewed';api.data.conflicts[0]['admin-notes']='Keep for later';
 const before=JSON.stringify(api.data.conflicts),read=api.reads.length,write=api.writes.length;
 assert.equal((await runScheduleRefresh(api,{lane:'full',slot:'2026-11-12T17:15:00Z',clock:()=>now})).log.result,'Success');
 assert.equal(api.reads.slice(read).filter(n=>n==='conflicts').length,1);
 assert.equal(api.writes.slice(write).some(w=>w.name==='conflicts'),false);
 assert.equal(JSON.stringify(api.data.conflicts),before);
});
