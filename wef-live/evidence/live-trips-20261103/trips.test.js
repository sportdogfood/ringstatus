'use strict';
globalThis.fetch=async()=>{throw Error('Offline network forbidden');};
const {test}=require('node:test'),assert=require('node:assert/strict');
const {project}=require('./candidate/live-trips');
const s={show:77,customer:15,date:'2026-11-05'},c={'class-id':30,'class-key':'77|2026-11-05|30',_ring:'77|2026-11-05|6'};
const raw={trip_id:400,entry_id:100,entryxclasses_uuid:'uuid1',horse_id:1,rider_id:2,trainer_id:3,owner_id:4,horse:'Horse',rider_name:'Rider',trainer:'Trainer',Owner:'Owner',order_of_go:12,actual_order:11,placing:2,score1:0,time_one:'70.52',faults_one:0,scratch_trip:0,gone_in:0};
const decoded=trips=>({class:{class_id:30,show_id:77},class_related_data:{date:s.date,results_verified:0,status:'Open'},trips});
test('one decoded class trip has source identity, four entities and its own result without UI detail',()=>{
 const r=project(decoded([raw]),null,s,c,'now');assert.equal(r.rows.length,1);assert.equal(r.results.length,1);const t=r.rows[0],result=r.results[0];
 assert.equal(t['trip-key'],'15|77|2026-11-05|6|30|100|uuid1');assert.equal(t['entry-key'],'77|100');assert.deepEqual([t['horse-id'],t['rider-id'],t['trainer-id'],t['owner-id']],[1,2,3,4]);assert.equal(result['result-key'],t['trip-key']);assert.equal(result.score1,'0');assert.equal(result['time-one'],'70.52');assert.equal(result['faults-one'],'0');assert.equal(result['results-verified'],'0');
});
test('separate source UUIDs preserve trips while sharing one show entry',()=>{
 const r=project(decoded([raw,{...raw,trip_id:401,entryxclasses_uuid:'uuid2',rider_id:5}]),null,s,c,'now');assert.equal(new Set(r.rows.map(x=>x['trip-key'])).size,2);assert.equal(new Set(r.rows.map(x=>x['entry-key'])).size,1);assert.equal(r.rows[1]['rider-id'],5);
});
test('source trip-ID fallback works, missing owner ID stays absent and errors do not stop other trips',()=>{
 const errors=[];const r=project(decoded([{...raw,entry_id:null},{...raw,entryxclasses_uuid:null,owner_id:null}]),null,s,c,'now',(...x)=>errors.push(x));assert.equal(r.rows.length,1);assert(r.rows[0]['trip-key'].endsWith('|400'));assert(!('owner-id' in r.rows[0]));assert.equal(r.rows[0]['owner-name'],'Owner');assert.equal(errors.length,1);
});
test('explicit empty trips succeeds; other show/date does not become day0',()=>{
 assert.equal(project(decoded([]),null,s,c,'now').rows.length,0);assert.throws(()=>project({...decoded([raw]),class_related_data:{date:'2026-11-06'}},null,s,c,'now'),/scope mismatch/);
});
test('matching live observation updates zero/false facts without suppressing other decoded trips',()=>{
 const live={ID:30,recs:1,rows:[{id:400,class_id:30,OOG:0,Actual_OOG:0,Scr:false,Gone:0,ActualPos:0}]};const r=project(decoded([raw,{...raw,trip_id:401,entryxclasses_uuid:'uuid2'}]),live,s,c,'now');assert.equal(r.rows.length,2);assert.equal(r.rows[0]['order-of-go'],0);assert.equal(r.rows[0].scratch,false);assert.equal(r.results[0].position,'0');assert.equal(r.rows[1]['order-of-go'],12);
});
test('ambiguous live rows are logged, decoded source trip still collected',()=>{
 const issues=[];const live={ID:30,recs:2,rows:[{id:400,class_id:30},{id:400,class_id:30}]};assert.equal(project(decoded([raw]),live,s,c,'now',x=>issues.push(x)).rows.length,1);assert.equal(issues.length,1);
});
