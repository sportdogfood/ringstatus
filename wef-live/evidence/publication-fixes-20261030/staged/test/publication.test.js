'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const {publish,observations,plans,fresh}=require('../publication');const {FIELD,transport}=require('../publication-api');
const NOW='2026-10-29T15:00:00Z';const scoped={customer:15,show:76,date:'2026-10-29',active:true};
function fixture(){
 const common={'show-id':76,'customer-id':15,date:scoped.date,'last-seen':NOW,'source-updated-at':'raw-unzoned'};
 const ring={...common,'ring-key':'76|2026-10-29|44','ring-id':44,'ring-number':3,'source-json':JSON.stringify({livenow:[{class_group_id:20,day:scoped.date}],upcoming:[],completed:[]})};
 const group={...common,'group-key':'76|2026-10-29|20','class-group-id':20,_ring:ring['ring-key'],'source-json':JSON.stringify({status:'Live',is_live:true,total:5,gone:0,jo_total:2,jo_gone:0,r2_total:0,r2_gone:0,group_sequence:1})};
 const cls={...common,'class-key':'76|2026-10-29|30','class-id':30,_ring:ring['ring-key'],_groups:[group['group-key']],'source-json':JSON.stringify({ID:30,recs:1,gone_trips:'',jo_gone_trips:'',r2_gone_trips:'',curr:400})};
 const identity={trip_id:400,entry_id:200,horse_id:1,rider_id:2,trainer_id:3,entryxclasses_uuid:'uuid'};
 const entry={...common,'live-entry-key':'76|2026-10-29|30|400','trip-id':400,'entry-id':200,'horse-id':1,'rider-id':2,'trainer-id':3,_class:cls['class-key'],'source-json':JSON.stringify({identity,live:{id:400,OOG:0,Actual_OOG:1,Gone:0,Scr:0,Pos:2}})};
 const input={scope:scoped,rings:[ring],groups:[group],classes:[cls],details:[{row:cls,observedAt:NOW,entries:[entry]}],sourceArchives:['source-groups','source-widget','class-30'].map(kind=>({kind,file:'source.gz',sha256:'proof'}))};
 const rec=(id,fields)=>({id,fields});const rk='15|76|2026-10-29|3',ck=rk+'|30';
 const db={schedules:{rings:[rec('recSR',{customer_id:15,show_id:76,ring_date:scoped.date,ring_id:44,ring_number:3,'ring-key':rk,notes:'protected'})],schedules:[rec('recSS',{'stage-key':'76|2026-10-29|20_100','show-id':76,date:scoped.date,ring:3,'group-id':20,'class-id':30,status:'active',rings:['recSR'],'row-json':'protected','total-trips':90})]},
 data:{rings:[rec('recDR',{'ring-key':rk,'ring-id':44,'ring-number':3,status:'acquisition'})],groups:[rec('recDG',{'group-key':rk+'|20','group-id':20,ring:['recDR']})],classes:[rec('recDC',{'class-key':ck,'class-id':30,ring:['recDR'],status:'acquisition','entry-count':90})],schedules:[rec('recDS',{'schedule-key':rk+'|100',class:['recDC'],group:['recDG'],ring:['recDR']})],trips:[rec('recDT',{'trip-key':ck+'|200|uuid','trip-id':'400','source-uuid':'uuid',class:['recDC'],horse:['horse-local'],rider:['rider-local'],trainer:['trainer-local'],'record-status':'active','last-success':NOW,'source-json':JSON.stringify(identity)})]}};
 const audits=[],writes=[];const state={data:{schedules:{pending:{},cursor:0},data:{pending:{},cursor:0}},save(){}};
 const api={list:async(l,t)=>structuredClone(db[l][t]),get:async(l,t,id)=>structuredClone(db[l][t].find(r=>r.id===id)),patch:async(l,t,id,v)=>{assert(audits.some(a=>a.kind==='publication-'+l+'-intent'));assert(Object.keys(state.data[l].pending).length);writes.push({l,t,id,v});db[l][t].find(r=>r.id===id).fields[FIELD]=v;}};
 const archive=async(kind,value)=>{audits.push({kind,value:structuredClone(value)});return{kind,file:'archive.gz',sha256:'hash'};};
 return{input,db,audits,writes,state,api,args:{input,api,state,archive,clock:()=>NOW,authority:async()=>{},budgetMs:100000}};
}
test('seven existing targets receive only observed JSON, all original facts and links preserved',async()=>{
 const f=fixture(),before=structuredClone(f.db);const r=await publish(f.args);assert.deepEqual(r.errors,[]);assert.equal(f.writes.length,7);
 for(const [l,tables]of Object.entries(f.db))for(const [t,rows]of Object.entries(tables))for(const row of rows){const fields={...row.fields};const envelope=JSON.parse(fields[FIELD]);delete fields[FIELD];assert.deepEqual(fields,before[l][t].find(r=>r.id===row.id).fields);assert.equal(envelope.format,'wef-live-observation-v1');assert.equal(envelope.destination.lane,l);}
 const s=JSON.parse(f.db.schedules.schedules[0].fields[FIELD]);assert.equal(s.components.class.values.gone,0);assert.deepEqual(Object.keys(s.components.group.values),['status','isLive']);
 await publish(f.args);assert.equal(f.writes.length,7,'Same observation must not rewrite');
});
test('fresh detail required independently of refreshed group/class discovery',async()=>{
 const f=fixture();f.input.details[0].observedAt='2026-10-29T14:53:59Z';const r=await publish(f.args);assert.equal(r.errors.length,0);assert.equal(f.writes.length,4);assert(!f.db.data.classes[0].fields[FIELD]);assert(!f.db.data.trips[0].fields[FIELD]);
});
test('future/missing timestamps and absent values do not become current zeros',()=>{
 assert(fresh('2026-10-29T14:54:00Z',NOW));assert(!fresh('2026-10-29T14:53:59Z',NOW));assert(!fresh('2026-10-29T15:01:01Z',NOW));assert(!fresh(null,NOW));const f=fixture();delete f.input.details[0].entries[0]['source-updated-at'];f.input.details[0].row['source-json']='{"curr":0}';const o=observations(f.input,{file:'a',sha256:'h'});assert.equal(o.classes[0].component.values.gone,null);assert.equal(o.classes[0].component.values.currentTripId,0);
});
test('empty/outside TODAY performs no destination inventory or writes',async()=>{
 const f=fixture();for(const input of [undefined,{...f.input,scope:{...scoped,date:'2026-10-30'}}]){const r=await publish({...f.args,input,api:{...f.api,list:async()=>{throw Error('Unexpected read')}}});assert.equal(r.errors.length,0);}assert.equal(f.writes.length,0);
});
test('ambiguous class occurrences never duplicate class counts',async()=>{
 const f=fixture();f.db.schedules.schedules.push({...structuredClone(f.db.schedules.schedules[0]),id:'recSS2',fields:{...f.db.schedules.schedules[0].fields,'stage-key':'76|2026-10-29|20_101'}});f.db.data.schedules.push({...structuredClone(f.db.data.schedules[0]),id:'recDS2'});const r=await publish(f.args);assert(r.errors.some(e=>e.message.includes('2 matches')));for(const row of f.db.schedules.schedules)assert(!JSON.parse(row.fields[FIELD]).components.class);for(const row of f.db.data.schedules)assert(!row.fields[FIELD]);assert(f.db.data.classes[0].fields[FIELD]);
});
test('one unavailable lane cannot stop the other or change source success',async()=>{
 const f=fixture(),list=f.api.list;f.api.list=async(l,t)=>{if(l==='schedules')throw Error('lane unavailable');return list(l,t);};const r=await publish(f.args);assert.equal(r.lanes.schedules.status,'Partial');assert.equal(r.lanes.data.status,'Success');assert.equal(f.writes.length,5);
});
test('unknown PATCH retains durable intent, next invocation reads back before any retry',async()=>{
 const f=fixture(),patch=f.api.patch;let fail=true;f.api.patch=async(...args)=>{await patch(...args);if(args[0]==='schedules'&&fail){fail=false;throw Error('lost response')}};
 const r=await publish(f.args);assert.equal(r.lanes.schedules.status,'Partial');assert.equal(Object.keys(f.state.data.schedules.pending).length,1);const count=f.writes.length;await publish(f.args);assert.equal(f.writes.length,count+1);assert(f.audits.some(a=>a.kind==='publication-schedules-reconcile'&&a.value.outcome==='Verified'));assert.equal(Object.keys(f.state.data.schedules.pending).length,0);
});
test('intent archive failure prevents PATCH and preserves destination values',async()=>{
 const f=fixture(),archive=f.args.archive;f.args.archive=async(k,v)=>{if(k.endsWith('-intent'))throw Error('disk failure');return archive(k,v);};const r=await publish(f.args);assert(r.errors.length);assert.equal(f.writes.length,0);
});
test('rider conflict without same stable UUID is withheld, parent/native links untouched',async()=>{
 const f=fixture(),trip=f.db.data.trips[0],raw=JSON.parse(trip.fields['source-json']);raw.rider_id=99;trip.fields['source-json']=JSON.stringify(raw);trip.fields['source-uuid']='other';const r=await publish(f.args);assert(r.errors.some(e=>e.message.includes('UUID conflict')));assert(!trip.fields[FIELD]);assert.deepEqual(trip.fields.rider,['rider-local']);
});
test('new collected unchanged observation may advance time; retry may not',async()=>{
 const f=fixture();await publish(f.args);f.args.clock=()=> '2026-10-29T15:03:00Z';await publish(f.args);assert.equal(f.writes.length,7);f.input.details[0].observedAt='2026-10-29T15:03:00Z';await publish(f.args);assert.equal(f.writes.length,11);
});
test('publication transport only writes one field by existing record ID',async()=>{
 const calls=[];const api=transport({pause:async()=>{},request:async(u,o)=>{calls.push({u,o});return{ok:true,json:async()=>({records:[]})}}});await api.patch('data','trips','recABC','{}');assert.deepEqual(JSON.parse(calls[0].o.body),{records:[{id:'recABC',fields:{[FIELD]:'{}'}}]});await assert.rejects(()=>api.patch('data','entries','recABC','{}'),/Forbidden/);await assert.rejects(()=>api.patch('schedules','schedules','','{}'),/Invalid/);assert.equal(calls.length,1);
});

test('multiple source groups with only one destination cannot attribute class counters',async()=>{
 const f=fixture();f.input.classes[0]._groups.push('76|2026-10-29|21');const r=await publish(f.args);assert(r.errors.some(e=>e.message.includes('Ambiguous source group')));assert(!JSON.parse(f.db.schedules.schedules[0].fields[FIELD]).components.class);assert(!f.db.data.schedules[0].fields[FIELD]);assert(f.db.data.classes[0].fields[FIELD]);
});
test('exact trip suffix and raw UUID must agree',async()=>{
 for(const change of [f=>f['trip-key']+='wrong',f=>f['source-uuid']='',f=>{const s=JSON.parse(f['source-json']);delete s.entryxclasses_uuid;f['source-json']=JSON.stringify(s);}]){const f=fixture();change(f.db.data.trips[0].fields);await publish(f.args);assert(!f.db.data.trips[0].fields[FIELD]);}
});
test('unknown write archives outcome immediately while preserving pending',async()=>{
 const f=fixture();f.api.patch=async()=>{throw Error('timeout')};await publish(f.args);assert(f.audits.some(a=>a.kind.endsWith('-outcome')&&a.value.outcome==='Unknown'));assert(Object.keys(f.state.data.data.pending).length);
});
test('detail flag preserves missing versus explicit false',()=>{
 const f=fixture();let o=observations(f.input,{file:'a',sha256:'h'});assert.equal(o.groups[0].component.values.hasJson,null);const s=JSON.parse(f.input.groups[0]['source-json']);s.has_JSON=false;f.input.groups[0]['source-json']=JSON.stringify(s);o=observations(f.input,{file:'a',sha256:'h'});assert.equal(o.groups[0].component.values.hasJson,false);
});
test('expired lane budget prevents reconciliation inventory and writes',async()=>{
 const f=fixture();let reads=0;f.api.get=f.api.list=async()=>{reads++;throw Error('unexpected')};const r=await publish({...f.args,budgetMs:0});assert.equal(reads,0);assert.equal(f.writes.length,0);assert(r.errors.every(e=>e.message.includes('budget')));
});
test('transport bounds pagination by lane deadline',async()=>{
 let reads=0;const limit={deadline:Date.now()+1000};const api=transport({pause:async()=>{},request:async()=>{reads++;limit.deadline=Date.now()-1;return{ok:true,json:async()=>({records:[],offset:'next'})}}});await assert.rejects(()=>api.list('data','rings',scoped,limit),/budget/);assert.equal(reads,1);
});

test('intent archive delay cannot publish expired observations',async()=>{
 const f=fixture(),archive=f.args.archive;let at='2026-10-29T15:05:59Z';f.args.clock=()=>at;f.args.archive=async(k,v)=>{if(k.endsWith('-intent'))at='2026-10-29T15:06:01Z';return archive(k,v);};await publish(f.args);assert.equal(f.writes.length,0);
});
test('transport rechecks freshness after pacing and before PATCH dispatch',async()=>{
 let calls=0;const api=transport({pause:async()=>{},request:async()=>{calls++;return{ok:true,json:async()=>({})}}});await assert.rejects(()=>api.patch('data','trips','recABC','{}',{beforeDispatch(){throw Error('expired')}}),/expired/);assert.equal(calls,0);
});


test('saved source has_JSON casing preserves true false and missing without alias guessing',()=>{
 const evidence=require('./publication-source-evidence.json');assert(Object.hasOwn(evidence.group,'has_JSON'));
 for(const source of [evidence.group,{has_JSON:true},{has_JSON:false},{},{has_json:true}]){
  const f=fixture();f.input.groups[0]['source-json']=JSON.stringify(source);
  assert.equal(observations(f.input,{file:'a',sha256:'h'}).groups[0].component.values.hasJson,source.has_JSON??null);
 }
});

test('uncertain early exit counts all untouched plans and retains readback-before-retry',async()=>{
 const f=fixture(),patch=f.api.patch;let once=true;
 f.api.patch=async(...args)=>{await patch(...args);if(args[0]==='data'&&once){once=false;throw Error('lost response');}};
 const first=await publish(f.args),s=first.lanes.data;
 assert.equal(s.planned,5);assert.equal(s.attempted,1);assert.equal(s.patched,0);assert.equal(s.uncertain,1);assert.equal(s.deferred,4);
 assert.equal(f.state.data.data.cursor,1);const count=f.writes.length;
 const next=await publish(f.args);assert.equal(next.lanes.data.uncertain,0);assert.equal(f.writes.length,count+4);
 assert(f.audits.some(a=>a.kind==='publication-data-reconcile'&&a.value.outcome==='Verified'));
});

test('safe pre-intent failure counts deferred and patch cap counts untouched',async()=>{
 const f=fixture(),get=f.api.get;
 f.api.get=async(l,t,id)=>{if(l==='data'&&t==='classes')throw Error('read failed');return get(l,t,id);};
 const r=await publish({...f.args,maxPatches:1});
 assert.equal(r.lanes.data.planned,5);assert.equal(r.lanes.data.patched,1);assert.equal(r.lanes.data.deferred,4);assert.equal(r.lanes.data.uncertain,0);
});

test('producer metadata changes before and during PATCH remain audited and preserved',async()=>{
 const evidence=require('./publication-source-evidence.json'),f=fixture(),get=f.api.get,patch=f.api.patch;
 f.api.get=async(l,t,id)=>{const row=f.db[l][t].find(x=>x.id===id);row.fields['last-success']='2026-10-29T15:01:00Z';for(const key of evidence.concurrentFields)row.fields[key]='independent-before';return get(l,t,id);};
 f.api.patch=async(l,t,id,v)=>{await patch(l,t,id,v);const row=f.db[l][t].find(x=>x.id===id);row.fields['acquisition-counter']=123;row.fields.notes='independent-after';};
 const r=await publish(f.args);assert.deepEqual(r.errors,[]);assert.equal(f.writes.length,7);
 assert.equal(r.lanes.data.status,'Success');assert.equal(r.lanes.schedules.status,'Success');
 for(const lane of Object.values(f.db))for(const rows of Object.values(lane))for(const row of rows){assert.equal(row.fields.notes,'independent-after');assert.equal(row.fields['acquisition-counter'],123);}
 assert(f.audits.some(a=>a.kind.endsWith('-outcome')&&a.value.readback.fields['acquisition-counter']===123));
});

test('pre-PATCH identity native-link overlay and trip source changes still withhold',async()=>{
 const cases=[
  ['schedules','schedules',f=>f['class-id']=999],
  ['schedules','schedules',f=>f.rings=['recOther']],
  ['data','trips',f=>{const src=JSON.parse(f['source-json']);src.rider_id=99;f['source-json']=JSON.stringify(src);}],
  ['data','trips',f=>f.horse=['recOther']],
  ['data','groups',f=>f['group-key']+='wrong'],
  ['data','classes',f=>f[FIELD]='unexpected']
 ];
 for(const [lane,table,change]of cases){const f=fixture(),get=f.api.get;f.api.get=async(l,t,id)=>{if(l===lane&&t===table)change(f.db[l][t].find(x=>x.id===id).fields);return get(l,t,id);};
  const r=await publish(f.args);assert(!f.writes.some(w=>w.l===lane&&w.t===table));assert(r.lanes[lane].issues.some(x=>/changed/.test(x)));assert(r.lanes[lane].deferred>0);
 }
});

test('post-PATCH changed identity or native link preserves pending audit and stops lane',async()=>{
 for(const change of [f=>f['class-id']=999,f=>f.ring=['recOther'],f=>f.newLinks=['recNew']]){
  const f=fixture(),patch=f.api.patch;f.api.patch=async(l,t,id,v)=>{await patch(l,t,id,v);if(l==='data')change(f.db[l][t].find(r=>r.id===id).fields);};
  const r=await publish(f.args);assert.equal(r.lanes.data.uncertain,1);assert.equal(r.lanes.data.deferred,4);assert.equal(r.lanes.data.status,'Partial');assert.equal(r.lanes.schedules.status,'Success');
 }
});

test('trip source cosmetic updates allowed but timestamp supporting a contradiction is protected',async()=>{
 const f=fixture(),get=f.api.get;f.api.get=async(l,t,id)=>{if(l==='data'&&t==='trips'){const row=f.db[l][t][0],src=JSON.parse(row.fields['source-json']);src.token='new';row.fields['source-json']=JSON.stringify(src);}return get(l,t,id);};
 assert.equal((await publish(f.args)).lanes.data.status,'Success');
 const g=fixture(),src=JSON.parse(g.db.data.trips[0].fields['source-json']);src.rider_id=99;g.db.data.trips[0].fields['source-json']=JSON.stringify(src);g.db.data.trips[0].fields['last-success']='2026-10-29T14:59:00Z';const get2=g.api.get;
 g.api.get=async(l,t,id)=>{if(l==='data'&&t==='trips')g.db.data.trips[0].fields['last-success']='2026-10-29T15:01:00Z';return get2(l,t,id);};
 const r=await publish(g.args);assert(!g.writes.some(w=>w.t==='trips'));assert(r.lanes.data.deferred>0);
});
