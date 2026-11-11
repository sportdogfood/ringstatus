import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { sharedScope, buildSharedDay } from '../src/lib/wef-schedule/shared-projection.mjs';
import { refreshScheduleFeed, readScheduleFeed } from '../src/lib/wef-schedule/shared-feed.mjs';
import { validDataset } from '../public/wef-schedule-v2/device-data.js';
import { encodePayload, decodePayload } from '../src/lib/wef-schedule/shared-payload.mjs';

const folder=process.env.WEF_SHARED_CAPTURE;
test('current shared capture publishes November 11–14 locally and marks November 15 pending without losing source data', {skip:!folder}, async()=>{
  const data={};
  for(const name of ['shows','days','schedules','classes','trips','rings','horses','riders','trainers','feed'])data[name]=JSON.parse(await readFile(join(folder,name+'.json'),'utf8'));
  const reads=[],writes=[],expected=[];
  const originalSource=JSON.stringify(Object.fromEntries(Object.entries(data).filter(([k])=>k!=='feed')));
  const api={async read(name,formula=''){
    reads.push(name);
    const pre=/^LEFT\(\{([^}]+)\},\d+\)=(.+)$/.exec(formula);
    if(pre)return structuredClone(data[name].filter(r=>String(r[pre[1]]??'').startsWith(JSON.parse(pre[2]))));
    const terms=[...formula.matchAll(/\{([^}]+)\}=("[^"]*"|\d+)/g)];
    return structuredClone(data[name].filter(r=>!terms.length||terms.some(([,k,v])=>(k==='is_default'?Number(r[k]):r[k])===JSON.parse(v))));
  },async upsert(name,fields){
    assert.equal(name,'feed');
    let row=data.feed.find(r=>r['show-day-key']===fields['show-day-key']);
    if(!row){row={id:'feed-'+data.feed.length};data.feed.push(row);}
    Object.assign(row,structuredClone(fields));writes.push(fields['show-day-key']);return structuredClone(row);
  }};
  const scope=await sharedScope(api),report=[];
  for(const day of scope.days){
    if(day.date==='2026-11-15')continue;
    const {dataset}=await buildSharedDay(api,scope,day,'2026-11-11T13:14:46.750Z',undefined,{snapshots:true});
    const stored=encodePayload(dataset);
    expected.push(...dataset.records);
    assert.deepEqual(decodePayload(stored),dataset);
    assert.ok(stored.length<=100000);
    report.push({date:day.date,rows:dataset.records.length,trips:dataset.records.flatMap(r=>r.entries).length,
      originalCharacters:JSON.stringify(dataset).length,storedCharacters:stored.length});
  }
  assert.equal(report.length,4);
  const result=await refreshScheduleFeed(api,'full','2026-11-11T13:14:46.750Z');
  assert.deepEqual(result.pendingDays,[{date:'2026-11-15',reason:'invalid_shared_class',sourceRowCount:155}]);
  const full=await readScheduleFeed(api,scope.show.id,'full');
  assert.equal(validDataset(full,scope.show.id,'full'),true);
  assert.deepEqual(full.records,expected);
  assert.equal(full.records.length,366);
  assert.equal(full.days.find(d=>d.date==='2026-11-15').pending,true);
  assert.deepEqual((await refreshScheduleFeed(api,'full','2026-11-11T13:14:46.750Z')).upserts,[]);
  assert.equal(JSON.stringify(Object.fromEntries(Object.entries(data).filter(([k])=>k!=='feed'))),originalSource);
  assert.equal(reads.some(n=>n.startsWith('live')),false);
  await writeFile(join(folder,'pending-full-preview.json'),JSON.stringify(full)+'\n');
  const diff=await readScheduleFeed(api,scope.show.id,'day0');
  assert.equal(diff.records,undefined);
  await writeFile(join(folder,'pending-day0-preview.json'),JSON.stringify(diff)+'\n');
  await writeFile(join(folder,'pending-verification.json'),JSON.stringify({report,pendingDays:full.metadata.pendingDays,rows:full.records.length,roundTrip:'all fields identical',sourceUnchanged:true},null,2)+'\n');
});
