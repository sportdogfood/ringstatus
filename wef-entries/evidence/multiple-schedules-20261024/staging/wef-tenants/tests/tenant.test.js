"use strict";
const test=require('node:test'),assert=require('node:assert/strict');
const {validateTrainers,validatePeople,normalizeEntries,mergeEntries,validateShow}=require('../model');
const show={show_id:75,customer_id:15,facility_id:516,start_date:'2026-10-22',end_date:'2026-10-25',selected_date:'2026-10-22',is_default:true,last_checked:'2026-10-20T17:00:00Z'};
const main={id:'main',fields:{'trainer-id':8778,role:'main',slot:1,active:true}};
const trainer=(id,slot)=>({id:String(id),fields:{'trainer-id':id,role:'additional',slot,active:true}});
test('one main plus five distinct additional trainers; disabled slots do not run',()=>{
 assert.equal(validateTrainers([main,...[2,3,4,5,6].map(n=>trainer(n,n)),{fields:{active:false}}]).length,6);
 for(const rows of [[main,main],[trainer(2,2)],[main,...[2,3,4,5,6,7].map(n=>trainer(n,n))],[main,trainer(8778,2)],[main,trainer(2,1)]])assert.throws(()=>validateTrainers(rows));
});
test('small but identified and explicit empty payload is valid; missing, wrong or inconsistent data fails',()=>{
 assert.equal(validatePeople({people:{people_id:8778},show_id:75,trips:[],total_trips:0},8778,75).length,0);
 for(const p of [{},{people:{people_id:8778}},{people:{people_id:2},trips:[]},{people:{people_id:8778},show_id:74,trips:[]},{people:{people_id:8778},trips:[],total_trips:1}])assert.throws(()=>validatePeople(p,8778,75));
});
test('source-selected date is required and stale references rejected',()=>{
 assert.equal(validateShow([show],'2026-10-20T17:05:00Z').selected_date,'2026-10-22');
 for(const rows of [[{...show,selected_date:null}],[{...show,selected_date:'2026-10-20'}],[show,show],[{...show,last_checked:'2026-10-19T17:00:00Z'}]])assert.throws(()=>validateShow(rows,'2026-10-20T17:05:00Z'));
});
const schedule=(date,ring=1)=>({id:'sch'+date,fields:{'show-id':75,date,'class-id':123,ring,'stage-key':'stage'+date,'class-name':'Class'}});
test('entries retain all show dates and cross-base schedule keys',()=>{
 const trips=['2026-10-22','2026-10-23'].map(show_date=>({entry_id:22,horse_id:33,rider_id:44,class_id:123,show_date}));
 const rows=normalizeEntries(trips,main,show,[schedule('2026-10-22'),schedule('2026-10-23')]);
 assert.equal(rows.length,2);assert.equal(new Set(rows.map(r=>r['entry-key'])).size,2);assert.equal(rows[1].date,'2026-10-23');
 assert.match(rows[0]['schedule-link'],/appUq5Qz9kARqFija/);
});
test('unmatched or ambiguous schedules block inferred date assignment',()=>{
 const t=[{entry_id:22,horse_id:33,rider_id:44,class_id:123}];
 assert.throws(()=>normalizeEntries(t,main,show,[]));
 assert.throws(()=>normalizeEntries(t,main,show,[schedule('2026-10-22'),schedule('2026-10-22',2)]));
 assert.throws(()=>normalizeEntries(t,main,show,[schedule('2026-10-22'),schedule('2026-10-23')]));
 assert.throws(()=>normalizeEntries([{...t[0],show_id:74}],main,show,[schedule('2026-10-22')]));
});
test('same entry across trainers deduplicates while keeping both sources',()=>{
 const t=[{entry_id:22,horse_id:33,rider_id:44,class_id:123}];
 const a=normalizeEntries(t,main,show,[schedule('2026-10-22')]);
 const b=normalizeEntries(t,trainer(19676,2),show,[schedule('2026-10-22')]);
 const result=mergeEntries([...a,...b]);assert.equal(result.length,1);assert.equal(result[0]['trainer-ids'],'8778,19676');
 assert.throws(()=>mergeEntries([...a,{...b[0],'horse-id':99}]));
});
