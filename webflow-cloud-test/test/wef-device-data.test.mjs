import test from 'node:test';
import assert from 'node:assert/strict';
import {createDeviceData,filterTrainer,trainerOptions,CWF_TEAM,teamRollups,REFRESH_MS,FULL_REFRESH_MS,validDataset} from '../public/wef-schedule-v2/device-data.js';
const showId='recxI7H0RojcE39BP';
const sample=(lane='full',stamp='2026-11-04T12:00:00Z',name='Nick')=>({metadata:{showId,defaultDate:'2026-10-30',capturedAt:stamp,refreshedAt:stamp,package:{lane,complete:true,startedAt:stamp,dates:['2026-10-30']}},days:[{date:'2026-10-30',rings:[51]}],ringDetails:{51:{name:'Intl',number:1}},records:[{key:'row',date:'2026-10-30',ring:51,entryCount:5,trips:6,entries:[{key:'trip1',entryId:'same',trainerId:'t1',trainer:name},{key:'trip2',entryId:'same',trainerId:'t1',trainer:name},{key:'trip3',trainerId:'t2',trainer:'Other'}]}]});
const store=()=>{const values=new Map();return {read:async(s,l)=>values.get(l),commit:async(s,l,d)=>{values.set(l,d);return d;}};};
const flush=()=>new Promise(resolve=>setImmediate(resolve));
test('day-0 first, full second, independent 3/15 minute cadence',async t=>{
 t.mock.timers.enable({apis:['setInterval']});const calls=[];let holdFull=null;
 const device=createDeviceData({showId,endpoint:'https://example.test/data',storage:store(),onData:()=>{},fetchImpl:async url=>{const lane=url.searchParams.get('lane');calls.push(lane);if(holdFull&&lane==='full')await holdFull;return Response.json(sample(lane));}});
 const stop=device.start();await stop.ready;assert.deepEqual(calls,['day0','full']);
 t.mock.timers.tick(REFRESH_MS);await flush();assert.deepEqual(calls,['day0','full','day0']);
 let release;holdFull=new Promise(r=>release=r);const full=device.refresh('full');await flush();
 t.mock.timers.tick(REFRESH_MS);await flush();assert.equal(calls.at(-1),'day0');release();await full;
 assert.equal(FULL_REFRESH_MS,900000);stop();const count=calls.length;t.mock.timers.tick(FULL_REFRESH_MS);await flush();assert.equal(calls.length,count);
});
test('unchanged refresh does not render; full cannot overwrite the fast lane',async()=>{
 let version=0;const renders=[];
 const device=createDeviceData({showId,endpoint:'https://example.test/data',storage:store(),onData:d=>renders.push(d),fetchImpl:async url=>Response.json(sample(url.searchParams.get('lane'),'2026-11-04T12:00:0'+version+'Z',url.searchParams.get('lane')==='day0'?'Fast':'Old full'))});
 const stop=device.start();await stop.ready;const initial=renders.length;version++;await device.refresh('day0');await device.refresh('full');assert.equal(renders.length,initial);assert.equal(device.current.records[0].entries[0].trainer,'Fast');stop();
});
test('incomplete, wrong-show and duplicate packages retain device data',async()=>{
 let invalid=null;const device=createDeviceData({showId,endpoint:'https://example.test/data',storage:store(),onData:()=>{},fetchImpl:async url=>Response.json(invalid||sample(url.searchParams.get('lane')))});
 const stop=device.start();await stop.ready;const old=device.current.records[0];
 for(const mutate of [d=>d.metadata.package.complete=false,d=>d.metadata.showId='other',d=>d.records.push(d.records[0])]){invalid=sample('day0');mutate(invalid);assert.equal(validDataset(invalid,showId,'day0'),false);await device.refresh('day0');assert.equal(device.current.records[0],old);}stop();
});

test('a valid current empty package clears older rows on the device and in its saved copy',async()=>{
 const saved=store();let empty=false;const renders=[];
 const device=createDeviceData({showId,endpoint:'https://example.test/data',storage:saved,onData:d=>renders.push(d),fetchImpl:async url=>{
  const data=sample(url.searchParams.get('lane'),empty?'2026-11-04T12:03:00Z':'2026-11-04T12:00:00Z');
  if(empty)data.records=[];return Response.json(data);
 }});
 const stop=device.start();await stop.ready;assert.equal(device.current.records.length,1);
 empty=true;await device.refresh('day0');assert.equal(device.current.records.length,0);
 assert.equal((await saved.read(showId,'day0')).records.length,0);
 assert.equal(renders.at(-1).records.length,0);stop();
});
test('trainer filter keeps both trips, totals and day eligibility',()=>{
 const data=sample(),result=filterTrainer(data.records,'t1');assert.deepEqual(result[0].entries.map(e=>e.key),['trip1','trip2']);assert.equal(result[0].entryCount,5);assert.equal(result[0].trips,6);assert.deepEqual(trainerOptions(data.records,'2026-11-01'),[]);
});

test('CWF rollups preserve the entire schedule and source totals, including classes without team trips',()=>{
 const data=sample();data.records[0].entries[0].trainerId=CWF_TEAM.trainerId;data.records[0].entries[1].trainerId=CWF_TEAM.trainerId;
 data.records.push({...data.records[0],key:'other-class',entries:[{key:'other-trip',trainerId:'other'}]},{...data.records[0],key:'no-trips',entries:[]});
 const before=structuredClone(data.records),result=teamRollups(data.records,'cwf');
 assert.deepEqual(result.map(r=>r.key),before.map(r=>r.key));
 assert.deepEqual(result.map(r=>[r.entryCount,r.trips]),before.map(r=>[r.entryCount,r.trips]));
 assert.deepEqual(result.map(r=>r.entries.map(e=>e.key)),[['trip1','trip2'],[],[]]);
 assert.deepEqual(data.records,before);assert.equal(teamRollups(data.records,''),data.records);
});

test('team scope is independent of trainer scope and is reapplied to refreshed trips',()=>{
 const rows=sample().records;rows[0].entries[0].trainerId=CWF_TEAM.trainerId;
 assert.equal(teamRollups(filterTrainer(rows,'t2'),'cwf').length,1);
 assert.equal(teamRollups(filterTrainer(rows,'t2'),'cwf')[0].entries.length,0);
 assert.equal(teamRollups(rows,'cwf')[0].entries.length,1);
 const refreshed=structuredClone(rows);refreshed[0].entries.push({...refreshed[0].entries[0],key:'new-trip'});
 assert.equal(teamRollups(refreshed,'cwf')[0].entries.length,2);
 assert.equal(teamRollups([{...rows[0],date:'2026-11-15',entries:[]}],'cwf').length,1);
});
test('full refresh fires at fifteen minutes while fast lane fires five times',async t=>{
 t.mock.timers.enable({apis:['setInterval']});const calls=[];
 const device=createDeviceData({showId,endpoint:'https://example.test/data',storage:store(),onData:()=>{},fetchImpl:async url=>{const lane=url.searchParams.get('lane');calls.push(lane);return Response.json(sample(lane));}});
 const stop=device.start();await stop.ready;
 for(let n=0;n<5;n++){t.mock.timers.tick(180000);await flush();}
 assert.equal(calls.filter(l=>l==='day0').length,6);assert.equal(calls.filter(l=>l==='full').length,2);stop();
});
test('saved packages display before network; each failed lane keeps confirmed data',async()=>{
 const saved=store();await saved.commit(showId,'full',sample());await saved.commit(showId,'day0',sample('day0'));
 const seen=[],errors=[];let release;
 const device=createDeviceData({showId,endpoint:'https://example.test/data',storage:saved,onData:d=>seen.push(d),onError:(e,d,l)=>errors.push(l),fetchImpl:async()=>{await new Promise(r=>release=r);return new Response('',{status:503});}});
 const stop=device.start();await flush();assert.equal(seen.length,1);release();await flush();release();await stop.ready;
 assert.deepEqual(errors,['day0','full']);assert.equal(device.current.records[0].entries.length,3);stop();
});
