const test=require('node:test'),assert=require('node:assert/strict');
const {run}=require('../../wef-timing/data-runner');
const {TIMING,SHOWS,DATA_TABLES:T,CLASS_TIMING,CLASS_ANSWERS,SYNC_GROUPS,SYNC_RINGS}=require('../../wef-timing/api');
const {adapt}=require('../../wef-timing/class-adapter');
test('Live uses TODAY while canonical selection is tomorrow; outside show range stays inactive',()=>{
 const fields={'show-key':'15|76',customer_id:15,show_id:76,is_default:true,start_date:'2026-10-29',end_date:'2026-11-01',selected_date:'2026-10-31',available_dates:'2026-10-29, 2026-10-30, 2026-10-31, 2026-11-01',last_checked:'2026-10-30T22:49:00Z',authority_checked_at:'2026-10-30T23:04:00Z'};
 const scope=require('../../wef-live/model').scope([{id:'canonical',fields}],'2026-10-30T23:09:00Z');assert.equal(scope.date,'2026-10-30');assert.equal(scope.active,true);assert.equal(fields.selected_date,'2026-10-31');
 fields.authority_checked_at='2026-11-02T13:00:00Z';assert.equal(require('../../wef-live/model').scope([{id:'canonical',fields}],'2026-11-02T13:04:00Z').active,false);
});
test('Timing normal function calculates TODAY twice with tomorrow canonical fence and no future-day purge',async()=>{
 const now='2026-10-30T23:09:00Z',day='2026-10-30',selected='2026-10-31';
 const show={id:'canonical',fields:{'show-key':'15|76',show_id:76,customer_id:15,is_default:true,start_date:'2026-10-29',end_date:'2026-11-01',selected_date:selected,last_checked:'2026-10-29T20:00:00Z',authority_checked_at:'2026-10-30T23:04:00Z'}};
 const context={'show-key':'15|76','show-id':76,'customer-id':15,'start-date':'2026-10-29','end-date':'2026-11-01','selected-date':selected};
 const data={source_lane:'data_live',context,proof:{'run-key':'head','show-key':'15|76','state-sha256':'a'.repeat(64)},inventories:{[day]:{observed_at:'2026-10-30T23:04:00Z',inventory_complete:true,refs:[{base:'appdl16HJ5ETm8N69',table:'collections',record:'retained',key:'c',sha256:'b'.repeat(64)}],groups:[{identity:{customer:15,show:76,date:day,ring:6,group:19},rows:[{'stage-key':'15|76|2026-10-30|6|occ','class-id':30,'start-time':'19:00:00','end-time':'19:33:00','total-trips':11,type:'class'}],class_keys:['76|2026-10-30|30']}]}}};
 const db={[T.answers]:[],[T.checkpoint]:[],[CLASS_ANSWERS]:[]},filters=[];let seq=0,classCalls=0;
 const api={clockVerified:true,list:async(base,table,formula)=>{
  if(base===SHOWS)return[structuredClone(show)];
  if(table===SYNC_GROUPS||table===SYNC_RINGS){filters.push(formula);return[];}
  const rows=structuredClone(db[table]);assert.ok(rows,table);if(!formula)return rows;const ids=[...formula.matchAll(/RECORD_ID\(\)='([^']+)'/g)].map(m=>m[1]);return rows.filter(r=>ids.includes(r.id));
 },upsert:async(table,key,fields)=>{for(const f of fields){let r=db[table].find(r=>r.fields[key]===f[key]);if(!r){r={id:'rec'+String(++seq).padStart(14,'0'),fields:{}};db[table].push(r);}r.fields=structuredClone(f);}return{records:fields.map(f=>structuredClone(db[table].find(r=>r.fields[key]===f[key])))};},removeSnapshots:async()=>{throw Error('must not purge');}};
 for(let n=0;n<2;n++){
 const r=await run({api,classApi:api,clock:()=>now,runId:'fixture-'+n,readPointer:c=>{assert.equal(c['selected-date'],selected);return{head:'head'};},readData:async c=>{assert.equal(c['selected-date'],selected);return structuredClone(data);},readLive:async()=>({classes:new Map(),issues:[]}),prepareClassBatch:args=>{classCalls++;assert.equal(args.data.context['selected-date'],selected);assert.deepEqual(Object.keys(args.data.inventories),[day]);const outputs=adapt({data:args.data,now,paceRows:[]});assert.equal(outputs.length,1);return{outputs,writes:[],unchanged:1};}});
 assert.equal(r.ok,true,JSON.stringify(r));assert.equal(r.counts.evaluated,1);assert.equal(r.counts.classes_evaluated,1);assert.equal(r.counts.purged,0);
 }
 assert.equal(classCalls,2);assert.ok(filters.length);assert(filters.every(f=>f.includes("{date}='2026-10-30'")));assert(db[T.answers].every(r=>r.fields.date===day));
});

test('upcoming canonical show cannot retire outgoing show TODAY records',async()=>{
 const now='2026-11-02T03:04:00Z',fields={'show-key':'15|77',customer_id:15,show_id:77,is_default:true,start_date:'2026-11-05',end_date:'2026-11-08',selected_date:'2026-11-05',available_dates:'2026-11-05, 2026-11-06, 2026-11-07, 2026-11-08',last_checked:now,authority_checked_at:now};
 const scope=require('../../wef-live/model').scope([{id:'new',fields}],now);assert.equal(scope.active,false);assert.equal(scope.date,'2026-11-05');
 const outgoing={id:'outgoing',fields:{'show-id':76,date:'2026-11-01'}};let retired=0,archived=0;
 const api={list:async table=>table==='live-entries'?[outgoing]:[],upsert:async()=>{throw Error('No catalogs expected');},retire:async()=>{retired++;}};
 const report={archives:[],run_id:'fixture'};
 await require('../../wef-live/retirement').retireOld({api,scope,report,now,archive:async()=>{archived++;throw Error('No archive expected');}});
 assert.equal(retired,0);assert.equal(archived,0);assert.deepEqual(report.retired,[]);
});

test('active TODAY collection preserves tomorrow-selected retirement guard for other-show TODAY rows',async()=>{
 const now='2026-10-30T23:04:00Z',fields={'show-key':'15|76',customer_id:15,show_id:76,is_default:true,start_date:'2026-10-29',end_date:'2026-11-01',selected_date:'2026-10-31',available_dates:'2026-10-30, 2026-10-31',last_checked:now,authority_checked_at:now};
 const scope=require('../../wef-live/model').scope([{id:'canonical',fields}],now);assert.equal(scope.active,true);assert.equal(scope.date,'2026-10-30');assert.equal(scope.selected_date,'2026-10-31');
 const api={list:async table=>table==='live-entries'?[{id:'other',fields:{'show-id':75,date:'2026-10-30'}}]:[],upsert:async()=>{throw Error('No catalogs expected');},retire:async()=>{throw Error('No retirement expected');}};
 const report={archives:[],run_id:'fixture'};
 await require('../../wef-live/retirement').retireOld({api,scope,report,now,archive:async()=>{throw Error('No archive expected');}});assert.deepEqual(report.retired,[]);
});

test('missed calendar wake does not widen Live retirement cutoff while TODAY observations continue',async()=>{
 const now='2026-10-31T13:04:00Z',fields={'show-key':'15|76',customer_id:15,show_id:76,is_default:true,start_date:'2026-10-29',end_date:'2026-11-01',selected_date:'2026-10-30',available_dates:'2026-10-30, 2026-10-31',last_checked:now,authority_checked_at:now};
 const scope=require('../../wef-live/model').scope([{id:'canonical',fields}],now);assert.equal(scope.active,true);assert.equal(scope.date,'2026-10-31');
 const api={list:async table=>table==='live-entries'?[{id:'yesterday',fields:{'show-id':76,date:'2026-10-30'}}]:[],upsert:async()=>{throw Error('No catalogs expected');},retire:async()=>{throw Error('No retirement expected');}};
 const report={archives:[],run_id:'fixture'};await require('../../wef-live/retirement').retireOld({api,scope,report,now,archive:async()=>{throw Error('No archive expected');}});assert.deepEqual(report.retired,[]);
});
