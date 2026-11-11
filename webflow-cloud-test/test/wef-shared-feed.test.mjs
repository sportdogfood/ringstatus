import test from 'node:test';
import assert from 'node:assert/strict';
import { randomBytes } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import { APP_BASE, sharedBase } from '../src/lib/wef-schedule/shared-base.mjs';
import { buildSharedDay, sharedScope } from '../src/lib/wef-schedule/shared-projection.mjs';
import { readScheduleFeed, refreshScheduleFeed } from '../src/lib/wef-schedule/shared-feed.mjs';
import { runScheduleRefresh } from '../src/lib/wef-schedule/refresh-run.mjs';
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
    feed:[],runs:[],errors:[],heartbeat:[],liveRings:[],liveGroups:[],liveClasses:[],liveEntries:[]
  };
}

function filter(rows, formula='') {
  const pre = /^LEFT\(\{([^}]+)\},\d+\)=(.+)$/.exec(formula);
  if (pre) return rows.filter(row => String(row[pre[1]] ?? '').startsWith(JSON.parse(pre[2])));
  const terms = [...formula.matchAll(/\{([^}]+)\}=("[^"]*"|\d+)/g)];
  if (!terms.length) return rows;
  return rows.filter(row => terms.some(([,key,value]) => (key === 'is_default' ? Number(row[key]) : row[key]) === JSON.parse(value)));
}

const mergeKeys = {feed:'show-day-key',runs:'run-id',heartbeat:'heartbeat-key',errors:'error-key'};
function memory(data=fixture()) {
  const reads=[], writes=[];
  return {
    data,reads,writes,
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

test('incomplete full input preserves the feed and logs error; normal no-change logs success',async () => {
  const api=memory();
  await refreshScheduleFeed(api,'full',now);
  const saved=structuredClone(api.data.feed);
  const missing=api.data.schedules.shift();
  const failed=await runScheduleRefresh(api,{lane:'full',slot:now,clock:()=>now});
  assert.equal(failed.log.result,'Error');
  assert.match(failed.log.errors,/unconfirmed_empty_day/);
  assert.deepEqual(api.data.feed,saved);
  assert.equal(api.data.errors[0]['run-id'],failed.log['run-id']);
  assert.equal(api.data.heartbeat[0].result,'Error');
  api.data.schedules.unshift(missing);
  const next=await runScheduleRefresh(api,{lane:'day0',slot:now,clock:()=>now});
  assert.equal(next.log.result,'Success');
  assert.equal(JSON.parse(next.log.summary).changed,false);
  assert.deepEqual(api.data.feed,saved);
  await assert.rejects(runScheduleRefresh(api,{lane:'day0',slot:now,clock:()=>now}),/already_recorded/);
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
