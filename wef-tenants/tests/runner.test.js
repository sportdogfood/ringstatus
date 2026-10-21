"use strict";
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),os=require('node:os'),path=require('node:path'),zlib=require('node:zlib');
const {runTenant}=require('../collector');const {archiveFile}=require('../archive');
const config=require('../settings');
const now='2026-10-20T17:00:00Z';
function fixture({failure=false}={}){
 const data={};let serial=0;const key=(base,t)=>base+'|'+t;
 function seed(t,fields,base=config.BASE_ID){data[key(base,t)]=fields.map(fields=>({id:'rec'+(++serial),fields}));}
 seed('tenant-details',[{'tenant-key':'CWF','tenant-id':8778,active:true,'barn-name':'home'}]);
 seed('trainers',[{'trainer-key':'8778','trainer-id':8778,role:'main',slot:1,active:true},...failure?[{'trainer-key':'19676','trainer-id':19676,role:'additional',slot:2,active:true}]:[]]);
 seed(config.SHARED_SHOWS_TABLE,[{'show-key':'15|75',show_id:75,customer_id:15,facility_id:516,show_name:'Test show',start_date:'2026-10-22',end_date:'2026-10-25',selected_date:'2026-10-22',available_dates:'2026-10-22, 2026-10-23, 2026-10-24, 2026-10-25',is_default:true,last_checked:now}],config.SHARED_BASE_ID);
 seed('rings-helper',[{'ring-key':'15|516|id:10',customer_id:15,facility_id:516,ring_id:10,ring_number:1,ring_name:'Ring 1','ring-walk':8,'ring-walk-is-example':true,'walk-tenant':['cwf']}],config.SHARED_BASE_ID);
 seed('tenants',[{'tenant-key':'CWF'}],config.SHARED_BASE_ID);data[key(config.SHARED_BASE_ID,'tenants')][0].id='cwf';
 seed('rings',['22','23','24','25'].map(d=>({'ring-key':'15|75|2026-10-'+d+'|1',show_id:75,customer_id:15,facility_id:516,ring_date:'2026-10-'+d,ring_id:10,ring_number:1,ring_name:'Ring 1',is_selected_date:d==='22'})),config.SHARED_BASE_ID);
 seed('schedules',[],config.SHARED_BASE_ID);
 const calls=[];const api={
  async list(t,base=config.BASE_ID){return structuredClone(data[key(base,t)]||[]);},
  async upsert(t,k,fields,base=config.BASE_ID){calls.push({t,base,fields:structuredClone(fields)});const rows=data[key(base,t)]||(data[key(base,t)]=[]);return fields.map(f=>{let r=rows.find(r=>r.fields[k]===f[k]);if(!r){r={id:'rec'+(++serial),fields:{}};rows.push(r);}Object.assign(r.fields,structuredClone(f));return structuredClone(r);});},
  async remove(t,ids){calls.push({remove:t,ids});data[key(config.BASE_ID,t)]=(data[key(config.BASE_ID,t)]||[]).filter(r=>!ids.includes(r.id));}
  ,async call(base,t,method,body){assert.equal(method,'PATCH');const rows=data[key(base,t)]||[];for(const r of body.records)Object.assign(rows.find(old=>old.id===r.id).fields,r.fields);return{};}
 };
 const archives=[];return{api,calls,data,seed,archives,options:{api,standingSource:async()=>({tenant:{id:'legacyTenant',fields:{tenant_id:8778}},horses:[],riders:[]}),clock:()=>now,archive:async(name,value,scope)=>{const event={kind:name,outcome:'Created',file:name+'.gz',sha256:'verified',records:Array.isArray(value)?value.length:1,shows:scope?.shows||[],dates:scope?.dates||[]};archives.push({event,value:structuredClone(value)});return event;},fetchPeople:async id=>{if(id===19676)throw Error('Source unavailable');return{people:{people_id:id},show_id:75,trips:[],total_trips:0};}}};
}
test('normal empty run covers four days; repeat does not duplicate references or collections',async()=>{
 const f=fixture();const a=await runTenant(f.options);assert.equal(a.ok,true);assert.equal(a.trainers[0].result,'No entries');assert.equal(a.days.length,4);assert.equal(a.rows,0);
 const first=(await f.api.list('collections')).map(r=>r.id);const b=await runTenant(f.options);assert.equal(b.ok,true);assert.deepEqual((await f.api.list('collections')).map(r=>r.id),first);assert.equal((await f.api.list('run-logs')).length,2);assert.equal((await f.api.list('rings')).length,4);
 assert.equal((await f.api.list('ring-walks'))[0].fields.minutes,8);assert.equal((await f.api.list('ring-walks'))[0].fields['is-example'],true);
 assert.equal(f.calls.filter(c=>c.base===config.SHARED_BASE_ID&&c.t!=='wef-tenant-index').length,0);
 assert.ok(f.archives.length>=10);
});
test('standing helpers populate with zero current-show entries and survive repeat source failure',async()=>{
 const f=fixture();const standingSource=async()=>({tenant:{id:'legacyTenant',fields:{tenant_id:8778}},horses:[{id:'legacyHorse',fields:{horse_id:11,horse:'Horse',barn_name:'Pony',pid:8778}}],riders:[{id:'legacyRider',fields:{rider_id:22,name:'Rider Name',properFirst:'Rider',properLast:'Name',pid:8778}}]});
 const a=await runTenant({...f.options,standingSource});assert.equal(a.ok,true);assert.equal(a.rows,0);assert.equal(a.roster.horses,1);assert.equal(a.roster.riders,1);const h=(await f.api.list('horses'))[0];assert.equal(h.fields['nick-name'],'Pony');
 const b=await runTenant({...f.options,standingSource,fetchPeople:async()=>{throw Error('People unavailable');}});assert.equal(b.ok,false);assert.equal((await f.api.list('horses'))[0].id,h.id);assert.equal((await f.api.list('riders')).length,1);
});
test('standing helpers populate even when shared-show discovery is unavailable',async()=>{
 const f=fixture();f.seed(config.SHARED_SHOWS_TABLE,[],config.SHARED_BASE_ID);const standingSource=async()=>({tenant:{id:'tenant',fields:{tenant_id:8778}},horses:[{id:'legacyHorse',fields:{horse_id:11,horse:'Horse',pid:8778}}],riders:[]});const r=await runTenant({...f.options,standingSource});assert.equal(r.ok,false);assert.equal((await f.api.list('horses')).length,1);assert.equal((await f.api.list('entries')).length,0);
});
test('actual helper writes omit computed fields and nonempty entries preserve deliberately blank helper names',async()=>{
 const f=fixture();f.seed('horses',[{'horse-key':'11','horse-id':11,'horse-name':'',name:'','source-base':'apptdhhNzduxm5gjn','source-record':'legacyHorse','lookup-text':'old formula text'}]);f.seed('schedules',[{'stage-key':'75|date|123','show-id':75,date:'2026-10-22','class-id':123,ring:1}],config.SHARED_BASE_ID);
 const standingSource=async()=>({tenant:{id:'tenant',fields:{tenant_id:8778}},horses:[{id:'legacyHorse',fields:{horse_id:11,horse:'Source name',pid:8778}}],riders:[]});
 const fetchPeople=async()=>({people:{people_id:8778},trips:[{entry_id:1,horse_id:11,rider_id:22,class_id:123,show_date:'2026-10-22',horse:'Entry name'}]});const r=await runTenant({...f.options,standingSource,fetchPeople});assert.equal(r.ok,true);const h=(await f.api.list('horses'))[0];assert.equal(h.fields.name,'');assert.equal(h.fields['horse-name'],'');assert.equal(f.calls.filter(c=>c.t==='horses').some(c=>c.fields.some(row=>'lookup-text' in row)),false);
});
test('one trainer failure is logged independently and preserves that trainer entries',async()=>{
 const f=fixture({failure:true});f.seed('entries',[{'entry-key':'75|2026-10-22|1|2',date:'2026-10-22','show-id':75,'trainer-ids':'19676',status:'active'}]);
 const r=await runTenant(f.options);assert.equal(r.ok,false);assert.equal(r.trainers.length,2);assert.equal(r.trainers[0].result,'No entries');assert.equal(r.trainers[1].result,'Failed');assert.equal((await f.api.list('entries'))[0].fields.status,'active');assert.equal((await f.api.list('run-logs'))[0].fields.result,'Failed');assert.match((await f.api.list('run-logs'))[0].fields.errors,/Source unavailable/);
});
test('archive failure blocks dataset changes and old-show deletion',async()=>{
 const f=fixture();f.seed('entries',[{'entry-key':'74|old','show-id':74,status:'active'}]);const r=await runTenant({...f.options,archive:async()=>{throw Error('disk failure');}});assert.equal(r.ok,false);assert.equal((await f.api.list('entries')).length,1);assert.equal(f.calls.some(c=>c.remove),false);
});
test('late failure leaves heartbeat and tenant index terminal Failed',async()=>{
 const f=fixture();const up=f.api.upsert;f.api.upsert=async(t,...args)=>{if(t==='entries')throw Error('entry readback failed');return up(t,...args);};
 f.seed('entries',[{'entry-key':'75|2026-10-22|1|2',date:'2026-10-22','show-id':75,'trainer-ids':'8778',status:'active'}]);
 const r=await runTenant(f.options);assert.equal(r.ok,false);assert.equal((await f.api.list('heartbeat'))[0].fields.result,'Failed');assert.equal((await f.api.list('wef-tenant-index',config.SHARED_BASE_ID))[0].fields['last-result'],'Failed');
});
test('nonempty trainer datasets link all days, dedupe shared entries and preserve disabled roster',async()=>{
 const f=fixture();f.seed('trainers',[{'trainer-key':'8778','trainer-id':8778,role:'main',slot:1,active:true},{'trainer-key':'19676','trainer-id':19676,role:'additional',slot:2,active:true}]);
 f.seed('schedules',['22','23'].map(d=>({'stage-key':'75|2026-10-'+d+'|123','show-id':75,date:'2026-10-'+d,'class-id':123,ring:1})),config.SHARED_BASE_ID);
 f.seed('horses',[{'horse-key':'33','horse-id':33,'horse-name':'Horse',disabled:true,'trainer-ids':'8778'}]);
 const fetchPeople=async id=>({people:{people_id:id},show_id:75,trips:['22','23'].map(d=>({entry_id:22,horse_id:33,rider_id:id===8778?44:55,class_id:123,show_date:'2026-10-'+d,horse:'Horse',rider_name:'Rider'}))});
 // Different rider identities for the same entry must fail rather than merge.
 const bad=await runTenant({...f.options,fetchPeople});assert.equal(bad.ok,false);
 const g=fixture();g.seed('schedules',['22','23'].map(d=>({'stage-key':'75|2026-10-'+d+'|123','show-id':75,date:'2026-10-'+d,'class-id':123,ring:1})),config.SHARED_BASE_ID);g.seed('horses',[{'horse-key':'33','horse-id':33,'horse-name':'Horse',disabled:true}]);
 const r=await runTenant({...g.options,fetchPeople});assert.equal(r.ok,true);assert.equal(r.rows,2);const entries=await g.api.list('entries');assert.equal(entries[0].fields.horses.length,1);assert.equal(entries[0].fields.riders.length,1);assert.equal(entries[0].fields.rings.length,1);assert.equal((await g.api.list('horses'))[0].fields.disabled,true);
});
test('log retention archives the exact finished row before reusing its ID',async()=>{
 const f=fixture();f.seed('run-logs',[{'run-id':'old',started:'2026-01-01',completed:'2026-01-01',result:'Success'}]);const before=(await f.api.list('run-logs'))[0];const r=await runTenant({...f.options,logLimit:1});assert.equal(r.ok,true);const rows=await f.api.list('run-logs');assert.equal(rows.length,1);assert.equal(rows[0].id,before.id);assert.equal(f.archives[0].value[0].fields['run-id'],'old');
});
test('show rollover archives exact old records before removing them',async()=>{
 const f=fixture();f.seed('entries',[{'entry-key':'74|old','show-id':74,status:'active'}]);const r=await runTenant(f.options);assert.equal(r.ok,true);const a=f.archives.find(a=>a.event.kind.startsWith('retired-entries'));assert.equal(a.value[0].fields['entry-key'],'74|old');assert.equal((await f.api.list('entries')).length,0);
});
test('archive is compressed, read back, content addressed and collision protected',async()=>{
 const root=fs.mkdtempSync(path.join(os.tmpdir(),'wef-tenant-test-'));try{const a=await archiveFile(root,'dataset',[],{});const b=await archiveFile(root,'dataset',[],{});assert.equal(a.file,b.file);assert.equal(b.outcome,'Reused');assert.deepEqual(JSON.parse(zlib.gunzipSync(fs.readFileSync(a.file))),[]);fs.writeFileSync(a.file,'bad');await assert.rejects(archiveFile(root,'dataset',[],{}));}finally{fs.rmSync(root,{recursive:true,force:true});}
});
module.exports={fixture};

test('rider collision logs exact conflict before either standing table writes or show source calls',async()=>{
 const f=fixture();let calls=0;
 const standingSource=async()=>({tenant:{id:'tenant',fields:{tenant_id:8778}},horses:[{id:'h',fields:{horse_id:11,horse:'Horse',pid:8778}}],riders:[{id:'r1',fields:{rider_id:21,name:'Same Rider',pid:8778}},{id:'r2',fields:{rider_id:22,name:' SAME  RIDER ',pid:8778}}]});
 const result=await runTenant({...f.options,standingSource,fetchPeople:async()=>{calls++;throw Error('Must not fetch');}});
 assert.equal(result.ok,false);assert.equal(calls,0);assert.equal(f.calls.some(c=>['horses','riders','entries'].includes(c.t)),false);
 const log=(await f.api.list('run-logs'))[0].fields;assert.equal(log.result,'Failed');assert.match(log.errors,/Duplicate riders.rider-name/);assert.match(log.errors,/21 and 22/);
});
