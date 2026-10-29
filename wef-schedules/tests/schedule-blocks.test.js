'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const fs=require('node:fs'),os=require('node:os'),path=require('node:path');
const {normalizeSchedulePayload,buildScheduleMachineKey}=require('../normalizer');
const {enrichTimes}=require('../schedule-times');
const {collectStagedSchedules}=require('../store');
const {currentStore}=require('../current-store');
const {fixture,NOW}=require('./fixtures/schedule-collector');
global.fetch=async()=>{throw Error('Network forbidden in offline block tests');};
const scope={app_show_idv2:74,app_sql_datev2:'2026-10-15',customer_id:15};
const group=(id,name,extra={})=>({class_group_id:id,group_name:name,schedule_break:1,class_list:'',estimated_start_time:'09:00:00',total_trips:0,completed_trips:0,...extra});
const payload=()=>({show_id:74,show_date:'2026-10-15',rings:[{ring_number:2,classes:[group(8,'Back Number Schooling'),group(9,'Drag Break'),{class_group_id:10,number:11,class_id:12,class_name:'Class A',total_trips:3}]}]});
const normalize=p=>normalizeSchedulePayload(p,{scope,source:'dated_schedule',generatedAt:NOW,generatedDate:NOW.slice(0,10)});

test('explicit schooling/break blocks coexist with unchanged class identities and source counts',()=>{
 const r=normalize(payload());assert.equal(r.rows.length,3);assert.equal(r.classes_count,1);assert.equal(r.schedule_blocks_count,2);
 const school=r.rows.find(x=>x.key==='block:8'),br=r.rows.find(x=>x.key==='block:9');
 assert.equal(school.fields.class_type,'schooling');assert.equal(br.fields.class_type,'break');
 assert.equal(school.fields.total_trips,0);assert.equal(school.fields.class_id,undefined);assert.equal(school.fields.class_number,undefined);
 assert.equal(school.refs.classes_endpointv2,null);assert.equal(school.fields.ring_number,2);assert.equal(school.fields.group_name,'Back Number Schooling');
 assert.equal(buildScheduleMachineKey(school.fields),'block:8');
 assert.equal(r.rows.find(x=>x.fields.class_id===12).key,'10_11');
});

test('ordinary groups and class-bearing groups do not create synthetic duplicate blocks',()=>{
 const p=payload();p.rings[0].classes=[group(8,'not a block',{schedule_break:0}),group(9,'Class group',{class_id:90,class_number:91}),group(10,'Missing IDs',{class_group_id:null})];
 const r=normalize(p);assert.equal(r.rows.length,1);assert.equal(r.rows[0].key,'9_91');assert.equal(r.schedule_blocks_count,0);
 assert.equal(buildScheduleMachineKey({class_group_id:9,class_number:91,schedule_break:true}),'9_91');
});

test('schooling-only days retain source times without requesting class HTML or warning',async()=>{
 const r=normalize(payload()).rows.filter(x=>x.fields.schedule_break);
 const result=await enrichTimes({rows:r,showId:74,date:'2026-10-15',capture:async()=>{throw Error('Class browser must not launch');}});
 assert.equal(result.summary.error,null);assert.equal(result.summary.unmatched,0);assert.equal(result.rows[0].fields.estimated_start_time,'09:00:00');
 const prior=[{...r[0],fields:{...r[0].fields,manual_time_override:true,estimated_start_time:'10:00:00'}}];
 const manual=await enrichTimes({rows:r,previous:prior,showId:74,date:'2026-10-15'});
 assert.equal(manual.rows[0].fields.estimated_start_time,'10:00:00');
});

test('recurring collection adds blocks in place, preserves linked classes and repeats without duplicates',async t=>{
 const historyRoot=fs.mkdtempSync(path.join(os.tmpdir(),'schedule-blocks-'));t.after(()=>fs.rmSync(historyRoot,{recursive:true,force:true}));
 const db={schedules:[],collections:[]};let next=0;
 const api={airtableList:async table=>structuredClone(db[table]),airtableCreateRecords:async(table,records)=>{for(const r of records)db[table].push({id:'rec'+(++next),fields:structuredClone(r.fields)});return {failedRows:[]};},airtablePatchRecords:async(table,records)=>{for(const r of records)Object.assign(db[table].find(x=>x.id===r.id).fields,r.fields);return {failedRows:[]};}};
 const store=currentStore({api,collections:'collections',historyRoot,removeRecords:async()=>{throw Error('No cleanup expected');}});
 const shows=[{fields:{show_id:74,customer_id:15,focus_day:'2026-10-15',start_date:'2026-10-15',end_date:'2026-10-15'}}];
 let blocks=false;const f=fixture({fetch:()=>{const p=payload();if(!blocks)p.rings[0].classes=p.rings[0].classes.filter(x=>x.class_id);return p;}});
 const run=()=>collectStagedSchedules({store,io:f.io,shows,now:NOW,clock:()=>NOW});
 assert.equal((await run()).ok,true);const original=db.schedules[0];original.fields.entries=['recEntry'];original.fields['owner-note']='keep';
 blocks=true;const first=await run();assert.equal(first.ok,true,JSON.stringify(first));assert.equal(db.schedules.length,3);assert.equal(first.days[0].schedule_blocks_count,2);
 assert.deepEqual(original.fields.entries,['recEntry']);assert.equal(original.fields['owner-note'],'keep');assert.equal(original.fields['stage-key'],'74|2026-10-15|10_11');
 const ids=db.schedules.map(x=>x.id);const repeat=await run();assert.equal(repeat.ok,true);assert.equal(repeat.days[0].unchanged,true);assert.deepEqual(db.schedules.map(x=>x.id),ids);
 assert.equal(JSON.parse(db.schedules.find(x=>x.fields.type==='schooling').fields['row-json']).fields.schedule_break,true);
 assert.equal(JSON.parse(db.collections[0].fields.message).schedule_blocks,2);
});

test('truly empty day message explicitly covers classes, schooling sessions and breaks',async()=>{
 const f=fixture({fetch:()=>({show_id:74,show_date:'2026-10-15',rings:[]})}),saved=new Map();let record;
 const store={begin:async fields=>(record={fields}),latest:async()=>null,writeRows:async(id,s,d,rows)=>saved.set(id,rows),loadRows:async id=>saved.get(id),finish:async(r,fields)=>Object.assign(r.fields,fields)};
 const result=await collectStagedSchedules({store,io:f.io,shows:[{fields:{show_id:74,customer_id:15,focus_day:'2026-10-15',start_date:'2026-10-15',end_date:'2026-10-15'}}],now:NOW});
 assert.equal(result.ok,true);assert.match(result.days[0].warnings[0].message,/no classes, schooling sessions or breaks/i);
});
