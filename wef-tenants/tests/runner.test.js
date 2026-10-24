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
 seed(config.SYNC_RINGS_TABLE,['22','23','24','25'].map(d=>({'ring-key':'15|75|2026-10-'+d+'|1',show_id:75,customer_id:15,facility_id:516,ring_date:'2026-10-'+d,ring_id:10,ring_number:1,ring_name:'Ring 1',is_selected_date:d==='22'})));
 seed(config.SYNC_SHOWS_TABLE,data[key(config.SHARED_BASE_ID,config.SHARED_SHOWS_TABLE)].map(r=>({...r.fields})));
 seed(config.SYNC_SCHEDULES_TABLE,[]);
 const calls=[];const api={
  async list(t,base=config.BASE_ID){
   const rows=structuredClone(data[key(base,t)]||[]);if(base!==config.BASE_ID)return rows;
   const shows=data[key(base,config.SYNC_SHOWS_TABLE)]||[],rings=data[key(base,config.SYNC_RINGS_TABLE)]||[],schedules=data[key(base,config.SYNC_SCHEDULES_TABLE)]||[];
   for(const r of rows){if(t===config.SYNC_SHOWS_TABLE){r.fields.rings=rings.filter(x=>x.fields.show_id===r.fields.show_id).map(x=>x.id);r.fields.schedules=schedules.filter(x=>x.fields['show-id']===r.fields.show_id).map(x=>x.id);}if(t===config.SYNC_RINGS_TABLE){r.fields['show-details']=shows.filter(x=>x.fields.show_id===r.fields.show_id).map(x=>x.id);r.fields.schedules=schedules.filter(x=>x.fields['show-id']===r.fields.show_id&&x.fields.date===r.fields.ring_date&&Number(x.fields.ring)===Number(r.fields.ring_number)).map(x=>x.id);}if(t===config.SYNC_SCHEDULES_TABLE){r.fields['show-details']=shows.filter(x=>x.fields.show_id===r.fields['show-id']).map(x=>x.id);r.fields.rings=rings.filter(x=>x.fields.show_id===r.fields['show-id']&&x.fields.ring_date===r.fields.date&&Number(x.fields.ring_number)===Number(r.fields.ring)).map(x=>x.id);}}return rows;
  },
  async upsert(t,k,fields,base=config.BASE_ID){calls.push({t,base,fields:structuredClone(fields)});const rows=data[key(base,t)]||(data[key(base,t)]=[]);return fields.map(f=>{let r=rows.find(r=>r.fields[k]===f[k]);if(!r){r={id:'rec'+(++serial),fields:{}};rows.push(r);}Object.assign(r.fields,structuredClone(f));return structuredClone(r);});},
  async remove(t,ids){calls.push({remove:t,ids});data[key(config.BASE_ID,t)]=(data[key(config.BASE_ID,t)]||[]).filter(r=>!ids.includes(r.id));}
  ,async call(base,t,method,body){assert.equal(method,'PATCH');const rows=data[key(base,t)]||[];for(const r of body.records)Object.assign(rows.find(old=>old.id===r.id).fields,r.fields);return{};}
 };
 const archives=[];return{api,calls,data,seed,archives,options:{api,standingSource:async()=>({tenant:{id:'legacyTenant',fields:{tenant_id:8778}},horses:[],riders:[]}),clock:()=>now,archive:async(name,value,scope)=>{const event={kind:name,outcome:'Created',file:name+'.gz',sha256:'verified',records:Array.isArray(value)?value.length:1,shows:scope?.shows||[],dates:scope?.dates||[]};archives.push({event,value:structuredClone(value)});return event;},fetchPeople:async id=>{if(id===19676)throw Error('Source unavailable');return{people:{people_id:id},show_id:75,trips:[],total_trips:0};}}};
}
test('normal empty run covers four days; repeat does not duplicate references or collections',async()=>{
 const f=fixture();const a=await runTenant(f.options);assert.equal(a.ok,true);assert.equal(a.trainers[0].result,'No entries');assert.equal(a.days.length,4);assert.equal(a.rows,0);
 const first=(await f.api.list('collections')).map(r=>r.id);const b=await runTenant(f.options);assert.equal(b.ok,true);assert.deepEqual((await f.api.list('collections')).map(r=>r.id),first);assert.equal((await f.api.list('run-logs')).length,2);assert.equal((await f.api.list(config.SYNC_RINGS_TABLE)).length,4);assert.equal(f.calls.some(c=>['show-details','rings',config.SYNC_SHOWS_TABLE,config.SYNC_RINGS_TABLE,config.SYNC_SCHEDULES_TABLE].includes(c.t)),false);
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
 const f=fixture();f.seed(config.SYNC_SHOWS_TABLE,[]);const standingSource=async()=>({tenant:{id:'tenant',fields:{tenant_id:8778}},horses:[{id:'legacyHorse',fields:{horse_id:11,horse:'Horse',pid:8778}}],riders:[]});const r=await runTenant({...f.options,standingSource});assert.equal(r.ok,false);assert.equal((await f.api.list('horses')).length,1);assert.equal((await f.api.list('entries')).length,0);
});
test('actual helper writes omit computed fields and nonempty entries preserve deliberately blank helper names',async()=>{
 const f=fixture();f.seed('horses',[{'horse-key':'11','horse-id':11,'horse-name':'',name:'','source-base':'apptdhhNzduxm5gjn','source-record':'legacyHorse','lookup-text':'old formula text'}]);f.seed(config.SYNC_SCHEDULES_TABLE,[{'stage-key':'75|date|123','show-id':75,date:'2026-10-22','class-id':123,ring:1}]);
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
 f.seed(config.SYNC_SCHEDULES_TABLE,['22','23'].map(d=>({'stage-key':'75|2026-10-'+d+'|123','show-id':75,date:'2026-10-'+d,'class-id':123,ring:1})));
 f.seed('horses',[{'horse-key':'33','horse-id':33,'horse-name':'Horse',disabled:true,'trainer-ids':'8778'}]);
 const fetchPeople=async id=>({people:{people_id:id},show_id:75,trips:['22','23'].map(d=>({entry_id:22,horse_id:33,rider_id:id===8778?44:55,class_id:123,show_date:'2026-10-'+d,horse:'Horse',rider_name:'Rider'}))});
 // Different rider identities for the same entry must fail rather than merge.
 const bad=await runTenant({...f.options,fetchPeople});assert.equal(bad.ok,false);
 const g=fixture();g.seed(config.SYNC_SCHEDULES_TABLE,['22','23'].map(d=>({'stage-key':'75|2026-10-'+d+'|123','show-id':75,date:'2026-10-'+d,'class-id':123,ring:1})));g.seed('horses',[{'horse-key':'33','horse-id':33,'horse-name':'Horse',disabled:true}]);
 const r=await runTenant({...g.options,fetchPeople});assert.equal(r.ok,true);assert.equal(r.rows,2);const entries=await g.api.list('entries');assert.equal(entries[0].fields.horses.length,1);assert.equal(entries[0].fields.riders.length,1);assert.equal(entries[0].fields['synced-rings'].length,1);assert.equal((await g.api.list('horses'))[0].fields.disabled,true);
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

function consumerFixture({empty=false}={}){
 const f=fixture(),sid=75,date='2026-10-22';
 f.seed(config.SYNC_SCHEDULES_TABLE,[{'stage-key':'75|2026-10-22|123','show-id':sid,date,'class-id':123,ring:1}]);
 const fields={'entry-key':'75|2026-10-22|1|123','show-id':sid,date,'entry-id':1,'class-id':123,'horse-id':33,'horse-name':'Horse','rider-id':44,'rider-name':'Rider','trainer-ids':'8778','class-name':'','ring-number':1,status:'active','schedule-key':'75|2026-10-22|123','last-seen':now};
 f.seed('entries',empty?[]:[fields],config.ENTRIES_BASE_ID);f.seed(config.SYNC_ENTRIES_TABLE,empty?[]:[fields]);
 const source=f.data[config.ENTRIES_BASE_ID+'|entries'];
 f.seed('collections',['22','23','24','25'].map(d=>({'collection-id':`CWF|75|2026-10-${d}|trainer-feed|8778`,status:'complete',completed:now,'row-count':!empty&&d==='22'?1:0,entries:!empty&&d==='22'?[source[0].id]:[]})),config.ENTRIES_BASE_ID);
 f.seed('heartbeat',[{'heartbeat-key':'CWF','show-id':75,result:'Success','checked-at':now}],config.ENTRIES_BASE_ID);
 f.options.entrySourceMode='synced';f.options.fetchPeople=async()=>{throw Error('Duplicate people fetch forbidden');};return f;
}
test('synced consumption preserves old entries and helpers, links new entries, and makes zero source fetches on repeat',async()=>{
 const f=consumerFixture();f.seed('entries',[{'entry-key':'old|74','show-id':74,horses:['oldHorse'],riders:['oldRider'],status:'active'}]);
 f.seed('horses',[{'horse-key':'33','horse-id':33,name:'Owner horse','horse-name':'Owner horse','nick-name':'Barn horse',disabled:true}]);
 f.seed('riders',[{'rider-key':'44','rider-id':44,name:'Owner rider','rider-name':'Owner rider','nick-name':'Barn rider',disabled:true,horses:['priorHorse']}]);
 const before=await f.api.list('entries'),horse=await f.api.list('horses'),rider=await f.api.list('riders');
 for(let i=0;i<2;i++){const result=await runTenant(f.options);assert.equal(result.ok,true,result.errors.join('\n'));assert.equal(result.people_fetches,0);assert.equal(result.rows,1);assert.equal(result.entry_sync.links_verified,1);assert.equal(result.entry_sync.links_updated,i===0?1:0);assert.deepEqual(await f.api.list('entries'),before);
  const synced=(await f.api.list(config.SYNC_ENTRIES_TABLE))[0];assert.deepEqual(synced.fields.horses,[horse[0].id]);assert.deepEqual(synced.fields.riders,[rider[0].id]);for(const name of ['shows','synced-rings','schedules','trainers'])assert.equal(synced.fields[name].length,1);
  for(const [table,old]of [['horses',horse],['riders',rider]]){const current=(await f.api.list(table))[0];for(const key of ['name','nick-name','disabled'])assert.deepEqual(current.fields[key],old[0].fields[key]);}
  assert.ok((await f.api.list('riders'))[0].fields.horses.includes('priorHorse'));assert.match((await f.api.list('heartbeat'))[0].fields.summary,/people endpoint fetches: 0/);
 }
 assert.equal(f.calls.some(c=>c.t==='entries'||c.remove==='entries'||c.base===config.ENTRIES_BASE_ID),false);
});
test('synced zero-entry collections succeed without requiring a horse or rider',async()=>{const f=consumerFixture({empty:true});const result=await runTenant(f.options);assert.equal(result.ok,true,result.errors.join('\n'));assert.equal(result.rows,0);assert.equal(result.people_fetches,0);assert.equal(result.days.length,4);assert.equal((await f.api.list('collections')).length,4);});

test('normal synced runner keeps inactive participation and helpers while updating only the tenant slice',async()=>{
 const f=consumerFixture();f.seed('horses',[{'horse-key':'33','horse-id':33,'horse-name':'Horse',disabled:true,'nick-name':'Retained'}]);
 for(let i=0;i<2;i++){const result=await runTenant({...f.options,tenantSlicesEnabled:true});assert.equal(result.ok,true,result.errors.join('\n'));assert.equal(result.rows,1);assert.equal(result.people_fetches,0);assert.equal(result.tenant_slices.active,0);assert.equal(result.tenant_slices.hidden_inactive,1);const row=(await f.api.list(config.SYNC_ENTRIES_TABLE))[0];assert.equal(row.fields.status,'active');assert.equal(row.fields['active-team'],false);assert.equal(row.fields.horses.length,1);assert.equal((await f.api.list('horses'))[0].fields['nick-name'],'Retained');assert.match((await f.api.list('heartbeat'))[0].fields.summary,/0 active team; 1 hidden/);}
 const horse=(await f.api.list('horses'))[0];await f.api.upsert('horses','horse-key',[{'horse-key':horse.fields['horse-key'],disabled:false}]);const restored=await runTenant({...f.options,tenantSlicesEnabled:true});assert.equal(restored.ok,true,restored.errors.join('\n'));assert.equal(restored.tenant_slices.active,1);assert.equal((await f.api.list(config.SYNC_ENTRIES_TABLE))[0].fields['active-team'],true);
});
test('enabled slices accept empty normal collections without manufacturing private or team records',async()=>{const f=consumerFixture({empty:true});const result=await runTenant({...f.options,tenantSlicesEnabled:true});assert.equal(result.ok,true,result.errors.join('\n'));assert.equal(result.tenant_slices.active,0);assert.equal((await f.api.list(config.SYNC_ENTRIES_TABLE)).length,0);});
test('incomplete, stale, failed or private-contaminated sync stops consumption without source fallback or old entry edits',async()=>{
 for(const kind of ['missing','stale','failed','private','count','duplicate','link']){const f=consumerFixture();f.seed('entries',[{'entry-key':'untouched','show-id':74}]);
  if(kind==='missing')f.seed(config.SYNC_ENTRIES_TABLE,[]);
  if(kind==='stale')f.data[config.BASE_ID+'|'+config.SYNC_ENTRIES_TABLE][0].fields['last-seen']='2026-10-19T00:00:00Z';
  if(kind==='failed')f.data[config.ENTRIES_BASE_ID+'|heartbeat'][0].fields.result='Failed';
  if(kind==='private'){const row=structuredClone(f.data[config.BASE_ID+'|'+config.SYNC_ENTRIES_TABLE][0]);row.id='private';row.fields['entry-key']='private';row.fields['trainer-ids']='';f.data[config.BASE_ID+'|'+config.SYNC_ENTRIES_TABLE].push(row);}
  if(kind==='count')f.data[config.ENTRIES_BASE_ID+'|collections'][0].fields['row-count']=2;
  if(kind==='duplicate')f.data[config.BASE_ID+'|'+config.SYNC_ENTRIES_TABLE].push(structuredClone(f.data[config.BASE_ID+'|'+config.SYNC_ENTRIES_TABLE][0]));
  if(kind==='link')f.seed(config.SYNC_SCHEDULES_TABLE,[]);
  const result=await runTenant(f.options);assert.equal(result.ok,false,kind);assert.equal(result.people_fetches,0,kind);assert.equal((await f.api.list('entries'))[0].fields['entry-key'],'untouched');assert.equal(f.calls.some(c=>c.remove||c.t==='entries'),false);
 }
});
test('private-only central participation does not enter CWF helpers or synced team',async()=>{const f=consumerFixture({empty:true});f.seed('entries',[{'entry-key':'private','show-id':75,'trainer-ids':'','horse-id':999,'rider-id':888,status:'active'}],config.ENTRIES_BASE_ID);const result=await runTenant(f.options);assert.equal(result.ok,true,result.errors.join('\n'));assert.equal(result.rows,0);assert.equal((await f.api.list('horses')).length,0);});
test('shadow validates scheduled legacy entries before cutover while still making legacy requests',async()=>{const f=consumerFixture();const fetchPeople=async()=>({people:{people_id:8778},trips:[{entry_id:1,horse_id:33,rider_id:44,class_id:123,show_date:'2026-10-22',horse:'Horse',rider_name:'Rider'}]});const result=await runTenant({...f.options,entrySourceMode:'shadow',fetchPeople});assert.equal(result.ok,true,result.errors.join('\n'));assert.equal(result.entry_sync.shadow_match,true);assert.equal(result.people_fetches,1);assert.equal((await f.api.list('entries')).length,1);});

test('read-only scheduled verifier and repeat comparison accept synced links without legacy projections',async()=>{
 const f=fixture();f.seed(config.SYNC_SCHEDULES_TABLE,[{'stage-key':'75|2026-10-22|123','show-id':75,date:'2026-10-22','class-id':123,ring:1}]);
 const temp=fs.mkdtempSync(path.join(os.tmpdir(),'wef-synced-verify-'));
 try{
  const options={...f.options,archive:(kind,value,scope)=>archiveFile(temp,kind,value,scope),fetchPeople:async()=>({people:{people_id:8778},trips:[{entry_id:1,horse_id:33,rider_id:44,class_id:123,show_date:'2026-10-22',horse:'Horse',rider_name:'Rider'}]})};
  const first=await runTenant(options);assert.equal(first.ok,true);const {verify}=require('../verify'),{compare}=require('../compare');
  const a=await verify({api:f.api,receipt:first,now:()=>now});assert.equal(a.counts['synced-schedules'],1);assert.equal(a.records['show-details'],undefined);
  const second=await runTenant(options);assert.equal(second.ok,true);const b=await verify({api:f.api,receipt:second,now:()=>now});assert.equal(compare(a,b).ok,true);
  const changed=structuredClone(b);changed.records.entries[0].fields.riders=['wrong'];assert.throws(()=>compare(a,changed),/entries changed/);
  const entries=await f.api.list('entries');await f.api.upsert('entries','entry-key',[{'entry-key':entries[0].fields['entry-key'],schedules:['wrong']}]);await assert.rejects(verify({api:f.api,receipt:second,now:()=>now}),/Broken active entry/);
 }finally{fs.rmSync(temp,{recursive:true,force:true});}
});

test('entries and collections use local synced references while legacy reference links remain untouched',async()=>{
 const f=fixture();f.seed(config.SYNC_SCHEDULES_TABLE,[{'stage-key':'75|2026-10-22|123','show-id':75,date:'2026-10-22','class-id':123,ring:1}]);
 f.seed('entries',[{'entry-key':'75|2026-10-22|1|123','entry-id':1,'horse-id':33,'rider-id':44,'class-id':123,'show-id':75,date:'2026-10-22','ring-number':1,'trainer-ids':'8778',status:'active','show-details':['oldShow'],rings:['oldRing']}]);
 const before=(await f.api.list('entries'))[0],result=await runTenant({...f.options,fetchPeople:async()=>({people:{people_id:8778},trips:[{entry_id:1,horse_id:33,rider_id:44,class_id:123,show_date:'2026-10-22',horse:'Horse',rider_name:'Rider'}]})});
 assert.equal(result.ok,true);const entry=(await f.api.list('entries'))[0],show=(await f.api.list(config.SYNC_SHOWS_TABLE))[0],ring=(await f.api.list(config.SYNC_RINGS_TABLE))[0],schedule=(await f.api.list(config.SYNC_SCHEDULES_TABLE))[0];assert.equal(entry.id,before.id);assert.deepEqual(entry.fields.shows,[show.id]);assert.deepEqual(entry.fields['synced-rings'],[ring.id]);assert.deepEqual(entry.fields.schedules,[schedule.id]);assert.deepEqual(entry.fields['show-details'],['oldShow']);assert.deepEqual(entry.fields.rings,['oldRing']);assert.equal(entry.fields['schedule-link'],`https://airtable.com/${config.BASE_ID}/${config.SYNC_SCHEDULES_TABLE}/${schedule.id}`);
 for(const r of await f.api.list('collections'))assert.deepEqual(r.fields.shows,[show.id]);assert.equal(f.calls.some(c=>['show-details','rings',config.SYNC_SHOWS_TABLE,config.SYNC_RINGS_TABLE,config.SYNC_SCHEDULES_TABLE].includes(c.t)),false);
});

test('incomplete sync prevents people collection and entry replacement or old-show retirement',async()=>{
 const f=fixture();f.seed('entries',[{'entry-key':'74|old','show-id':74,status:'active'}]);const list=f.api.list;f.api.list=async(t,...args)=>{const rows=await list(t,...args);if(t===config.SYNC_SHOWS_TABLE)rows[0].fields.schedules=['not-yet-synced'];return rows;};let fetches=0;const result=await runTenant({...f.options,fetchPeople:async()=>{fetches++;throw Error('Must not fetch during partial sync');}});assert.equal(result.ok,false);assert.equal(fetches,0);assert.equal((await f.api.list('entries')).length,1);assert.equal(f.calls.some(c=>c.remove||c.t==='entries'),false);
});

test('direct rider horse links collect distinct horses and preserve previous associations and helpers on repeat',async()=>{
 const f=fixture();f.seed(config.SYNC_SCHEDULES_TABLE,[{'stage-key':'75|2026-10-22|123','show-id':75,date:'2026-10-22','class-id':123,ring:1}]);
 f.seed('horses',[{'horse-key':'35','horse-id':35,'horse-name':'Prior Horse',name:'Prior Horse'}]);const priorHorse=(await f.api.list('horses'))[0];
 f.seed('riders',[{'rider-key':'44','rider-id':44,'rider-name':'Owner Name',name:'Owner Name','nick-name':'Barn Name',notes:'Keep notes',disabled:true,'source-record':'retainedRider',horses:[priorHorse.id]}]);const before=(await f.api.list('riders'))[0];
 const trips=[33,34,33].map((horse_id,i)=>({entry_id:100+i,horse_id,rider_id:44,class_id:123,show_date:'2026-10-22',horse:'Horse '+horse_id,rider_name:'Source Name'}));
 const fetchPeople=async()=>({people:{people_id:8778},trips});
 for(let i=0;i<2;i++){
  const result=await runTenant({...f.options,fetchPeople});assert.equal(result.ok,true);const rider=(await f.api.list('riders'))[0],horses=await f.api.list('horses');assert.equal(rider.id,before.id);
  assert.deepEqual(rider.fields.horses,[priorHorse.id,horses.find(h=>h.fields['horse-id']===33).id,horses.find(h=>h.fields['horse-id']===34).id]);
  for(const k of ['rider-name','name','nick-name','notes','disabled'])assert.deepEqual(rider.fields[k],before.fields[k]);
  for(const entry of await f.api.list('entries')){assert.deepEqual(entry.fields.riders,[rider.id]);assert.ok(rider.fields.horses.includes(entry.fields.horses[0]));}
 }
 const prior=(await f.api.list('riders'))[0].fields.horses;const empty=await runTenant({...f.options,fetchPeople:async()=>({people:{people_id:8778},trips:[]})});assert.equal(empty.ok,true);assert.deepEqual((await f.api.list('riders'))[0].fields.horses,prior);
});

test('failed entry save does not publish direct rider horse links',async()=>{
 const f=fixture();f.seed(config.SYNC_SCHEDULES_TABLE,[{'stage-key':'75|2026-10-22|123','show-id':75,date:'2026-10-22','class-id':123,ring:1}]);
 const upsert=f.api.upsert;f.api.upsert=async(t,...args)=>{if(t==='entries')throw Error('Entry save failed');return upsert(t,...args);};
 const result=await runTenant({...f.options,fetchPeople:async()=>({people:{people_id:8778},trips:[{entry_id:1,horse_id:33,rider_id:44,class_id:123,show_date:'2026-10-22',horse:'Horse',rider_name:'Rider'}]})});assert.equal(result.ok,false);assert.equal(f.calls.some(c=>c.t==='riders'&&c.fields.some(r=>Object.hasOwn(r,'horses'))),false);
});

test('all active trainers create rider helpers and link entries without changing existing helpers on repeat',async()=>{
 const f=fixture();f.seed('trainers',[{'trainer-key':'8778','trainer-id':8778,role:'main',slot:1,active:true},{'trainer-key':'50121','trainer-id':50121,role:'additional',slot:2,active:true},{'trainer-key':'80970','trainer-id':80970,role:'additional',slot:3,active:true},{'trainer-key':'99999','trainer-id':99999,role:'additional',slot:4,active:false}]);
 f.seed(config.SYNC_SCHEDULES_TABLE,[{'stage-key':'75|2026-10-22|123','show-id':75,date:'2026-10-22','class-id':123,ring:1}]);
 f.seed('riders',[{'rider-key':'44','rider-id':44,'rider-name':'Owner display',name:'Owner display','nick-name':'Barn rider',notes:'Keep this',disabled:true,'trainer-id':8778,'trainer-ids':'8778','source-record':'existing-source','source-base':'apptdhhNzduxm5gjn'},{'rider-key':'88','rider-id':88,'rider-name':'No current entries',name:'No current entries'}]);
 const original=await f.api.list('riders');const fetched=[];
 const fetchPeople=async id=>{fetched.push(id);return{people:{people_id:id},trips:[{entry_id:id,horse_id:33,rider_id:id===8778?44:id,class_id:123,show_date:'2026-10-22',horse:'Horse',rider_name:id===8778?'Source display':id===50121?'ISABELA DE SOUSA':'TAYLOR COWLES'}]};};
 let riderIds,entryIds;
 for(let i=0;i<2;i++){
  const result=await runTenant({...f.options,fetchPeople});assert.equal(result.ok,true);assert.equal(result.rows,3);
  const riders=await f.api.list('riders'),entries=await f.api.list('entries');assert.equal(riders.length,4);
  for(const old of original){const current=riders.find(r=>r.id===old.id);assert.ok(current);for(const key of ['rider-key','rider-id','rider-name','name','nick-name','notes','disabled'])assert.deepEqual(current.fields[key],old.fields[key]);}
  for(const entry of entries){const rider=riders.find(r=>r.fields['rider-id']===entry.fields['rider-id']);assert.deepEqual(entry.fields.riders,[rider.id]);if(entry.fields['trainer-ids']!=='8778'){assert.equal(rider.fields['tenant-id'],8778);assert.deepEqual(rider.fields.trainers,entry.fields.trainers);}}
  if(i===0){riderIds=riders.map(r=>r.id);entryIds=entries.map(r=>r.id);}else{assert.deepEqual(riders.map(r=>r.id),riderIds);assert.deepEqual(entries.map(r=>r.id),entryIds);}
 }
 assert.deepEqual(fetched,[8778,50121,80970,8778,50121,80970]);
});

test('a rider shared by active trainers gets one helper with both associations and entry links',async()=>{
 const f=fixture();f.seed('trainers',[{'trainer-key':'8778','trainer-id':8778,role:'main',slot:1,active:true},{'trainer-key':'50121','trainer-id':50121,role:'additional',slot:2,active:true}]);
 f.seed(config.SYNC_SCHEDULES_TABLE,[{'stage-key':'75|2026-10-22|123','show-id':75,date:'2026-10-22','class-id':123,ring:1}]);
 const fetchPeople=async id=>({people:{people_id:id},trips:[{entry_id:id,horse_id:33,rider_id:44,class_id:123,show_date:'2026-10-22',horse:'Horse',rider_name:'One Rider'}]});
 const result=await runTenant({...f.options,fetchPeople});assert.equal(result.ok,true);const riders=await f.api.list('riders');assert.equal(riders.length,1);assert.equal(riders[0].fields['trainer-ids'],'50121,8778');assert.equal(riders[0].fields.trainers.length,2);for(const entry of await f.api.list('entries'))assert.deepEqual(entry.fields.riders,[riders[0].id]);
});

test('an additional trainer rider name collision is rejected before helper or entry creation',async()=>{
 const f=fixture();f.seed('trainers',[{'trainer-key':'8778','trainer-id':8778,role:'main',slot:1,active:true},{'trainer-key':'50121','trainer-id':50121,role:'additional',slot:2,active:true}]);f.seed('riders',[{'rider-key':'44','rider-id':44,'rider-name':'Same Rider',name:'Same Rider'}]);
 f.seed(config.SYNC_SCHEDULES_TABLE,[{'stage-key':'75|2026-10-22|123','show-id':75,date:'2026-10-22','class-id':123,ring:1}]);
 const {assertProspectiveRoster}=require('../identity-policy'),upsert=f.api.upsert;f.api.upsert=async(table,key,rows,...args)=>{if(table==='riders')assertProspectiveRoster(table,key,await f.api.list(table),rows);return upsert(table,key,rows,...args);};
 const fetchPeople=async id=>({people:{people_id:id},trips:id===8778?[]:[{entry_id:2,horse_id:33,rider_id:55,class_id:123,show_date:'2026-10-22',horse:'Horse',rider_name:' SAME  RIDER '}]});
 const result=await runTenant({...f.options,fetchPeople});assert.equal(result.ok,false);assert.match(result.errors.join('\n'),/Duplicate riders.rider-name/);assert.equal((await f.api.list('riders')).length,1);assert.equal((await f.api.list('entries')).length,0);
});

test('validated people names populate each trainer on empty runs and retain record identities on repeat',async()=>{
 const f=fixture();f.seed('trainers',[{'trainer-key':'8778','trainer-id':8778,'trainer-name':'ALAN KOROTKIN',role:'main',slot:1,active:true},{'trainer-key':'80970','trainer-id':80970,role:'additional',slot:3,active:true}]);
 const before=await f.api.list('trainers');
 const fetchPeople=async id=>({people:{people_id:id,name:id===80970?'  TAYLOR COWLES  ':'ALAN KOROTKIN'},show_id:75,trips:[],total_trips:0});
 for(let run=0;run<2;run++){
  const result=await runTenant({...f.options,fetchPeople});assert.equal(result.ok,true);assert.equal(result.rows,0);
  const trainers=await f.api.list('trainers');assert.deepEqual(trainers.map(t=>t.id),before.map(t=>t.id));
  assert.equal(trainers[1].fields['trainer-name'],'TAYLOR COWLES');assert.equal(trainers[1].fields.slot,3);assert.equal(trainers[1].fields.active,true);assert.equal(trainers[1].fields.role,'additional');
  assert.equal(trainers[0].fields['trainer-name'],'ALAN KOROTKIN');
 }
});

test('missing or unusable people names preserve existing trainer names',async()=>{
 for(const name of [undefined,null,'','   ',12,{}]){
  const f=fixture();f.seed('trainers',[{'trainer-key':'8778','trainer-id':8778,'trainer-name':'ALAN KOROTKIN',role:'main',slot:1,active:true}]);
  const result=await runTenant({...f.options,fetchPeople:async()=>({people:{people_id:8778,name},trips:[]})});
  assert.equal(result.ok,true);assert.equal((await f.api.list('trainers'))[0].fields['trainer-name'],'ALAN KOROTKIN');
 }
});

test('invalid identity or failed archive never overwrites trainer name',async()=>{
 for(const failure of ['identity','archive']){
  const f=fixture();f.seed('trainers',[{'trainer-key':'8778','trainer-id':8778,'trainer-name':'ALAN KOROTKIN',role:'main',slot:1,active:true}]);
  const archive=f.options.archive;
  const result=await runTenant({...f.options,fetchPeople:async()=>({people:{people_id:failure==='identity'?80970:8778,name:'WRONG NAME'},trips:[]}),archive:async(kind,...args)=>{if(failure==='archive'&&kind==='people-8778')throw Error('Archive unavailable');return archive(kind,...args);}});
  assert.equal(result.ok,false);assert.equal((await f.api.list('trainers'))[0].fields['trainer-name'],'ALAN KOROTKIN');
 }
});

test('rider collision logs exact conflict before either standing table writes or show source calls',async()=>{
 const f=fixture();let calls=0;
 const standingSource=async()=>({tenant:{id:'tenant',fields:{tenant_id:8778}},horses:[{id:'h',fields:{horse_id:11,horse:'Horse',pid:8778}}],riders:[{id:'r1',fields:{rider_id:21,name:'Same Rider',pid:8778}},{id:'r2',fields:{rider_id:22,name:' SAME  RIDER ',pid:8778}}]});
 const result=await runTenant({...f.options,standingSource,fetchPeople:async()=>{calls++;throw Error('Must not fetch');}});
 assert.equal(result.ok,false);assert.equal(calls,0);assert.equal(f.calls.some(c=>['horses','riders','entries'].includes(c.t)),false);
 const log=(await f.api.list('run-logs'))[0].fields;assert.equal(log.result,'Failed');assert.match(log.errors,/Duplicate riders.rider-name/);assert.match(log.errors,/21 and 22/);
});

test('CWF recurring plural consumption keeps local schedules/rings and inactive helpers on two fixture runs',async()=>{
 const f=consumerFixture(),a='75|2026-10-22|123',b='75|2026-10-22|456';
 f.seed(config.SYNC_SCHEDULES_TABLE,[a,b].map((key,i)=>({'stage-key':key,'show-id':75,date:'2026-10-22','class-id':123,ring:i+1,status:'active'})));
 const rings=f.data[config.BASE_ID+'|'+config.SYNC_RINGS_TABLE];rings.push({id:'secondRing',fields:{...rings[0].fields,'ring-key':'ring2',ring_id:11,ring_number:2,ring_name:'Ring 2'}});
 f.data[config.SHARED_BASE_ID+'|rings-helper'].push({id:'secondHelper',fields:{'ring-key':'15|516|id:11',customer_id:15,facility_id:516,ring_id:11,ring_number:2,ring_name:'Ring 2'}});
 for(const [base,t]of [[config.BASE_ID,config.SYNC_ENTRIES_TABLE],[config.ENTRIES_BASE_ID,'entries']])Object.assign(f.data[base+'|'+t][0].fields,{'schedule-key':'','schedule-keys':JSON.stringify([a,b]),'observation-status':'successful','ring-number':null});
 f.seed('horses',[{'horse-key':'33','horse-id':33,'horse-name':'Owner horse','nick-name':'Keep',disabled:true}]);
 for(let i=0;i<2;i++){const r=await runTenant({...f.options,tenantSlicesEnabled:true});assert.equal(r.ok,true,r.errors.join('; '));assert.equal(r.people_fetches,0);assert.equal(r.tenant_slices.hidden_inactive,1);const e=(await f.api.list(config.SYNC_ENTRIES_TABLE))[0];assert.deepEqual(e.fields.schedules,(await f.api.list(config.SYNC_SCHEDULES_TABLE)).map(r=>r.id));assert.equal(e.fields['synced-rings'].length,2);assert.equal((await f.api.list('horses'))[0].fields['nick-name'],'Keep');}
});
