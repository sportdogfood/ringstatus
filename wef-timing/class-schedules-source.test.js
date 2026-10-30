"use strict";
const test=require('node:test'),assert=require('node:assert/strict');
const {loadSchedulesSource}=require('./class-schedules-source');
const {scheduleFingerprint}=require('./runner');
const {SCHEDULES}=require('./api');
function fixture(size=1) {
  const raw=Array.from({length:size},(_,n)=>({key:`g${n}_c`,fields:{show_id:76,customer_id:15,show_date:'2026-10-29',ring_number:6,
    class_group_id:19+n,class_id:30+n,class_groupxclasses_id:100+n,estimated_start_time:'08:06:00',estimated_end_time:'08:39:00',
    total_trips:11,completed_trips:2,class_type:'Jumpers',status:'Upcoming',sgl_token_raw:'DO_NOT_EXPOSE'}}));
  const shows=[{id:'show',fields:{'show-key':'15|76',show_id:76,customer_id:15,is_default:true,start_date:'2026-10-29',end_date:'2026-11-01',selected_date:'2026-10-29',last_checked:'2026-10-29T14:24:00Z'}}];
  const db={shows,rings:[{id:'ring',fields:{show_id:76,ring_date:'2026-10-29',ring_number:6}}]};
  function refresh(){
    db.schedules=raw.map((r,n)=>({id:'row'+n,fields:{'stage-key':`76|2026-10-29|${r.key}`,'dataset-id':'dataset',status:r.inactive?'dropped':'active',
      'show-id':76,date:'2026-10-29',ring:r.fields.ring_number,'group-id':r.fields.class_group_id,'class-id':r.fields.class_id,
      'start-time':r.fields.estimated_start_time,'end-time':r.fields.estimated_end_time,'total-trips':r.fields.total_trips,
      'completed-trips':r.fields.completed_trips,type:r.fields.class_type,'row-json':JSON.stringify(r)}}));
    db['show-collection']=[{id:'collection',fields:{'show-id':76,date:'2026-10-29',status:'complete',completed:'2026-10-29T14:24:00Z',
      'dataset-id':'dataset','row-count':raw.length,fingerprint:scheduleFingerprint(raw)}}];
  }
  refresh();const calls=[];
  return {raw,db,calls,refresh,clock:()=> '2026-10-29T14:25:00Z',readArchive:()=>structuredClone(raw),
    api:{list:async(base,table)=>{assert.equal(base,SCHEDULES);assert(['shows','rings','schedules','show-collection'].includes(table));calls.push(table);return structuredClone(db[table]);}}};
}
test('returns exact source scope, validated singleton counts, original times and no secrets',async()=>{
  const f=fixture(),r=await loadSchedulesSource(f),row=r.rows[0];
  assert.equal(row.stageKey,'76|2026-10-29|g0_c');assert.deepEqual(row.identity,{customer:15,show:76,date:'2026-10-29',ring:6,group:19,class:30});
  assert.equal(row.completedTrips,2);assert.equal(row.totalTrips,11);assert.equal(row.classStatus,null);assert.equal(row.sourceStatus,'Upcoming');
  assert.equal(row.observedAt,'2026-10-29T14:24:00Z');assert.equal(r.dates['2026-10-29'].validUntil,'2026-10-29T14:54:00.000Z');
  assert(!JSON.stringify(r).includes('DO_NOT_EXPOSE'));assert.equal(f.calls.filter(x=>x==='schedules').length,1);
});
test('reads all rows without a 500/80 row cap',async()=>{const r=await loadSchedulesSource(fixture(501));assert.equal(r.rows.length,501);});
test('no Live dependency; empty complete date remains usable evidence',async()=>{
  const f=fixture(0),r=await loadSchedulesSource(f);assert.equal(r.rows.length,0);assert.equal(r.dates['2026-10-29'].reason,null);
});
test('stale, future and incomplete date scopes never expose usable rows',async()=>{
  for(const patch of [{completed:'2026-10-29T13:55:00Z'},{completed:'2026-10-29T14:26:00Z'},{status:'partial'}]){
    const f=fixture();Object.assign(f.db['show-collection'][0].fields,patch);const r=await loadSchedulesSource(f);assert.equal(r.rows.length,0);assert(r.dates['2026-10-29'].reason);
  }
});
test('completed projection, frozen archive, dataset and retention conflicts reject the date',async()=>{
  for(const change of [f=>f.db.schedules[0].fields['completed-trips']=9,f=>f.readArchive=()=>[],
    f=>f.db.schedules[0].fields['dataset-id']='wrong',f=>f.db.schedules[0].fields.status='dropped']){
    const f=fixture();change(f);const r=await loadSchedulesSource(f);assert.equal(r.rows.length,0);assert.equal(r.dates['2026-10-29'].reason,'schedule_consistency_unproven');
  }
});
test('parallel classes retain exact rows but do not duplicate group counters as class counts',async()=>{
  const f=fixture(2);f.raw[1].fields.class_group_id=19;f.refresh();const r=await loadSchedulesSource(f);
  assert.equal(r.rows.length,2);for(const row of r.rows){assert.equal(row.completedTrips,null);assert.equal(row.totalTrips,null);assert.equal(row.sourceCompletedTrips,2);assert(row.reasons.includes('class_count_semantics_unproven'));}
});
test('dropped rows excluded and blocks cannot supply class counts',async()=>{
  const f=fixture(3);f.raw[0].inactive=true;f.raw[1].fields.schedule_break=true;f.raw[2].fields.class_type='Schooling';f.refresh();
  const r=await loadSchedulesSource(f);assert.equal(r.rows.length,2);assert(r.rows.every(x=>x.isBlock&&x.completedTrips===null&&x.totalTrips===null));
});
test('invalid completed counts are withheld even when projection agrees',async()=>{
  const f=fixture();f.raw[0].fields.completed_trips=12;f.refresh();const r=await loadSchedulesSource(f);
  assert.equal(r.rows[0].completedTrips,null);assert.equal(r.rows[0].sourceCompletedTrips,12);
});
test('ring identity ambiguity fails closed',async()=>{
  const f=fixture();f.db.rings.push({...f.db.rings[0],id:'other-ring'});const r=await loadSchedulesSource(f);
  assert.equal(r.rows.length,0);assert.equal(r.dates['2026-10-29'].detail,'ring_reference_unresolved');
});
test('show, collection and ring changes after reading invalidate the whole adapter result',async()=>{
  for(const table of ['shows','show-collection','rings']){
    const f=fixture(),list=f.api.list;let n=0;f.api.list=async(base,t)=>{const rows=await list(base,t);if(t===table&&++n===2)rows[0].fields.changed='changed';return rows;};
    await assert.rejects(loadSchedulesSource(f),e=>e.code==='SOURCE_CHANGED');
  }
});
test('duplicate stage keys and wrong default identity are rejected',async()=>{
  const f=fixture();f.db.schedules.push(f.db.schedules[0]);await assert.rejects(loadSchedulesSource(f),/Duplicate/);
  const g=fixture();g.db.shows[0].fields['show-key']='99|76';await assert.rejects(loadSchedulesSource(g),/identity/);
});
test('freshness is checked again after archive work',async()=>{
  const f=fixture();let now='2026-10-29T14:25:00Z';f.clock=()=>now;
  f.db.shows[0].fields.last_checked='2026-10-29T14:30:00Z';now='2026-10-29T14:31:00Z';
  f.readArchive=()=>{now='2026-10-29T14:54:00Z';return structuredClone(f.raw);};
  const r=await loadSchedulesSource(f);assert.equal(r.rows.length,0);assert.equal(r.dates['2026-10-29'].reason,'stale_observation');
});
