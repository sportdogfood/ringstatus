'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const {adapt,localInstant}=require('./class-adapter');
const {evaluate,hash}=require('./pace');
const at='2026-10-29T14:00:00Z',now='2026-10-29T14:05:00Z',day='2026-10-29';
function fixture(){
 const identity={customer:15,show:76,date:day,ring:2,group:3};
 const row={'class-id':4,'stage-key':'15|76|2026-10-29|2|123','start-time':'10:00:00','end-time':'10:20:00','total-trips':10,type:'class'};
 const inv={observed_at:at,inventory_complete:true,refs:[{base:'appdl16HJ5ETm8N69',table:'collections',key:'part1',sha256:'a'.repeat(64),record:'rec1'},
 {base:'appdl16HJ5ETm8N69',table:'collections',key:'part2',sha256:'b'.repeat(64),archive:{path:'evidence',sha256:'c'.repeat(64)}}],
 groups:[{identity,rows:[row]}]};
 return{lane:'data_live',now,paceRows:[],data:{source_lane:'data_live',context:{'customer-id':15,'show-id':76,'show-key':'15|76','start-date':day,'end-date':day,'selected-date':day},
 proof:{'show-key':'15|76','run-key':'run1','state-sha256':'d'.repeat(64)},inventories:{[day]:inv}}};
}
function paced(i){
 const inv=i.data.inventories[day],g=inv.groups[0];
 const refs=[{base:'appCaSmL819KK7UFJ',table:'shows',record:'show1',observed_at:at},...inv.refs.map(r=>({...r,observed_at:at}))];
 const result=evaluate({identity:g.identity,rows:g.rows,observedAt:at,inputRefs:refs,now,sourceLane:'data_live'});
 i.paceRows=[{id:'pace1',fields:result.fields}];return i;
}
function live(i){
 const id={...i.data.inventories[day].groups[0].identity,class:4};
 i.liveClasses=new Map([['76|2026-10-29|2|3|4',{identity:id,source_lane:'data_live',observed_at:at,valid_until:'2026-10-29T14:10:00Z',
 ref:{source_key:'76|2026-10-29|4',revision:'livehash'},counts:{scope:'class',total:12,completed:4,completion_scope:'unknown'}}]]);
 return i;
}
test('local times reject both DST gaps and ambiguity and invalid calendar dates',()=>{
 assert.equal(localInstant(day,'10:00:00'),'2026-10-29T14:00:00.000Z');
 for(const [d,t] of [['2026-11-01','01:30:00'],['2026-03-08','02:30:00'],['2026-02-30','10:00:00']])assert.equal(localInstant(d,t),null);
});
test('Data only fallback retains all native/archive parts and authority',()=>{
 const i=fixture(),[r]=adapt(i);
 assert.equal(r.payload.start_source,'data');assert.equal(r.payload.total_trips,10);
 assert.equal(r.payload.pace_minutes_per_trip,3);assert.equal(r.payload.completed_trips,null);
 const ref=r.payload.input_refs.find(r=>r.role==='data_evidence');
 assert.deepEqual(ref.parts,i.data.inventories[day].refs);assert.deepEqual(ref.authority,i.data.context);
 assert.equal(ref.parts[1].record,undefined);
});
test('existing producer pace hash and precise unrounded value are consumed',()=>{
 const [r]=adapt(paced(fixture()));assert.equal(r.payload.pace_minutes_per_trip,2);assert.equal(r.flags.default_pace,false);
});
test('tampered pace identity, fields, hash, members, provenance or original freshness defaults',()=>{
 for(const mutate of [
  f=>f['content-hash']='e'.repeat(64), f=>f['pace-minutes-per-trip']=99,
  f=>{const p=JSON.parse(f['payload-json']);p.identity.customer=9;f['payload-json']=JSON.stringify(p);},
  f=>{const p=JSON.parse(f['payload-json']);p.schedule_members[0]['stage-key']='other';f['payload-json']=JSON.stringify(p);},
  f=>{const refs=JSON.parse(f['input-refs-json']);refs[0].observed_at='2026-10-29T13:00:00Z';f['input-refs-json']=JSON.stringify(refs);},
  f=>f['evaluated-at']='2026-10-29T14:06:00Z',
  f=>f['source-lane']='schedules_live']){
  const i=paced(fixture());mutate(i.paceRows[0].fields);const [r]=adapt(i);
  assert.equal(r.flags.default_pace,true);assert(r.payload.reasons.some(x=>x.startsWith('ring_pace_')));
 }
});
test('newer invalid pace does not revive older good snapshot',()=>{
 const i=paced(fixture()),bad=structuredClone(i.paceRows[0]);bad.fields['evaluated-at']=now;bad.fields['content-hash']='e'.repeat(64);i.paceRows.push(bad);
 assert.equal(adapt(i)[0].flags.default_pace,true);
});
test('Live identity preserved and class counts preferred only for unique occurrence',()=>{
 const i=live(fixture());const [r]=adapt(i);assert.equal(r.payload.total_trips,12);assert.equal(r.payload.count_source,'live');
 const group=i.data.inventories[day].groups[0];group.rows.push({...group.rows[0],'stage-key':'15|76|2026-10-29|2|124'});
 const answers=adapt(i);assert.equal(answers.length,2);assert.notEqual(answers[0].fields['scope-key'],answers[1].fields['scope-key']);
 for(const r of answers){assert.equal(r.payload.total_trips,null);assert(r.payload.reasons.includes('live_occurrence_ambiguous'));}
});
test('wrong Live customer and stale Live do not contaminate Data fallback',()=>{
 for(const change of [v=>v.identity.customer=7,v=>v.observed_at='2026-10-29T13:00:00Z']){
  const i=live(fixture());change([...i.liveClasses.values()][0]);assert.equal(adapt(i)[0].payload.total_trips,10);
 }
});
test('multi-class group never repeats group counts',()=>{
 const i=fixture(),g=i.data.inventories[day].groups[0];g.rows.push({...g.rows[0],'stage-key':'other','class-id':5});
 const results=adapt(i);assert.equal(results.length,2);for(const r of results)assert.equal(r.payload.total_trips,null);
});
test('no admission cap: all 501 exact occurrences are represented',()=>{
 const i=fixture(),inv=i.data.inventories[day],g=inv.groups[0];
 inv.groups=Array.from({length:501},(_,n)=>({identity:{...g.identity,group:n+1},rows:[{...g.rows[0],'class-id':n+1,'stage-key':'occ-'+n}]}));
 assert.equal(adapt(i).length,501);
});
test('coverage, proof, exact occurrence, duplicates and forbidden Schedules fail explicitly',()=>{
 for(const change of [i=>delete i.data.inventories[day],i=>i.data.proof['show-key']='other',
 i=>i.data.inventories[day].inventory_complete=false,
 i=>i.data.inventories[day].groups[0].rows[0]['stage-key']='x|position-0',
 i=>i.data.inventories[day].groups[0].rows.push({...i.data.inventories[day].groups[0].rows[0]}),
 i=>i.schedules={},i=>i.lane='schedules_live']){
  const i=fixture();change(i);assert.throws(()=>adapt(i));
 }
});
test('stale Data stays enumerated but unavailable; missing data never becomes completed',()=>{
 const i=fixture();i.now='2026-10-29T14:30:00Z';const [r]=adapt(i);
 assert.equal(r.fields.availability,'unavailable');assert.equal(r.flags.completion_candidate,false);
});
test('verified empty selected inventory is explicit zero coverage, not missing coverage',()=>{
 const i=fixture();i.data.inventories[day].groups=[];assert.deepEqual(adapt(i),[]);
});

test('observed ring pace carries original endpoints and is consumed without identity invention',()=>{
 const i=fixture(),inv=i.data.inventories[day],g=inv.groups[0];
 const ref=at=>({base:'appgzosUcVqA4vZdf',table:'collections',record:'live1',fingerprint:'e'.repeat(64),observed_at:at});
 const previous={at:'2026-10-29T14:01:00Z',total:10,gone:1,membership:'same',ref:ref('2026-10-29T14:01:00Z')};
 const current={at:'2026-10-29T14:04:00Z',total:10,gone:3,membership:'same',ref:ref('2026-10-29T14:04:00Z')};
 const inputRefs=[{base:'appCaSmL819KK7UFJ',table:'shows',record:'show1',observed_at:at},
 ...inv.refs.map(r=>({...r,observed_at:at})),current.ref];
 i.paceRows=[{id:'observed',fields:evaluate({identity:g.identity,rows:g.rows,observedAt:at,inputRefs,live:current,previous,now,sourceLane:'data_live'}).fields}];
 assert.equal(adapt(i)[0].payload.pace_minutes_per_trip,1.5);
});
test('stale supporting evidence is rejected even if its content hash is internally consistent',()=>{
 const i=paced(fixture()),f=i.paceRows[0].fields,p=JSON.parse(f['payload-json']),refs=JSON.parse(f['input-refs-json']);
 refs[0].observed_at='2026-10-29T13:00:00Z';f['input-refs-json']=JSON.stringify(refs);
 f['content-hash']=hash({policy:'ring-pace-v1',payload:p,inputRefs:refs,observedAt:at});
 f['answer-key']=f['scope-key']+'|'+f['content-hash'];
 const [r]=adapt(i);assert.equal(r.flags.default_pace,true);assert(r.payload.reasons.includes('ring_pace_supporting_freshness'));
});

test('unrelated Data head advances preserve material hash while receipts retain exact validation proof',()=>{
 const i=fixture(),first=adapt(i)[0];
 i.data.proof['run-key']='unrelated-next-run';i.data.proof['state-sha256']='f'.repeat(64);
 const next=adapt(i)[0];
 assert.equal(next.fields['content-hash'],first.fields['content-hash']);
 assert.equal(next.payload.input_refs.find(r=>r.role==='data_evidence').producer_proof['run-key'],'unrelated-next-run');
 assert.notDeepEqual(next.payload.input_refs,first.payload.input_refs);
 i.data.inventories[day].refs[0].sha256='e'.repeat(64);
 const changed=adapt(i)[0];assert.notEqual(changed.fields['content-hash'],next.fields['content-hash']);
});

test('pace supporting Shows authority survives overnight without relaxing Data observation age',()=>{
 const i=fixture(),inv=i.data.inventories[day],g=inv.groups[0];
 const build=showAt=>{
  const refs=[{base:'appCaSmL819KK7UFJ',table:'shows',record:'show1',observed_at:showAt},...inv.refs.map(r=>({...r,observed_at:at}))];
  const result=evaluate({identity:g.identity,rows:g.rows,observedAt:at,inputRefs:refs,now,sourceLane:'data_live'});
  i.paceRows=[{id:'pace1',fields:result.fields}];return adapt(i)[0];
 };
 assert.equal(build('2026-10-28T23:45:00Z').payload.pace_minutes_per_trip,2);
 assert.equal(build('2026-10-27T23:45:00Z').flags.default_pace,true);
});
