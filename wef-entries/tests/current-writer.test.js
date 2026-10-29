"use strict";
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const {runEntries}=require('../collector'),settings=require('../settings'),{airtable}=require('../airtable');
function fixture(){
 const f=JSON.parse(fs.readFileSync(path.resolve(__dirname,'fixtures/narrow-matcher/partial.json')));
 const date='2026-10-30',now=date+'T12:00:00Z',data=new Map(),writes=[],reads=[],archives=[];let serial=0;
 const mapKey=(b,t)=>b+'|'+t;
 const seed=(table,rows,base=settings.BASE_ID)=>data.set(mapKey(base,table),rows.map(r=>({id:r.id||'rec'+(++serial),fields:structuredClone(r.fields||r)})));
 const api={list:async(t,b=settings.BASE_ID)=>{reads.push([b,t]);return structuredClone(data.get(mapKey(b,t))||[]);},upsert:async(t,k,fields)=>{writes.push(t);const rows=data.get(mapKey(settings.BASE_ID,t))||[];for(const value of fields){let r=rows.find(r=>r.fields[k]===value[k]);if(!r){r={id:'rec'+(++serial),fields:{}};rows.push(r);}Object.assign(r.fields,structuredClone(value));}data.set(mapKey(settings.BASE_ID,t),rows);return structuredClone(fields.map(value=>rows.find(r=>r.fields[k]===value[k])));},call:async()=>{throw Error('Unexpected direct mutation');}};
 const show={'show-key':'15|200000076',show_id:200000076,customer_id:15,start_date:date,end_date:date,selected_date:date,is_default:true,last_checked:now};
 seed(settings.SHARED_SHOWS_TABLE,[show],settings.SHOWS_BASE_ID);seed('schedules',f.schedules,settings.SHARED_BASE_ID);seed(settings.SYNC_TABLE,f.schedules);seed('show-collection',[{'show-id':show.show_id,date,status:'complete',completed:now}],settings.SHARED_BASE_ID);
 const input={head:'accepted-fixture',attempts:[],classes:[{scopeKey:'15|200000076|2026-10-30|3|200030671',payload:f.payload,observedAt:now,sourceEvidence:{revision:'accepted-fixture',collections:[{key:'part',sha256:'fixture'}]},inventory:{revision:'accepted-fixture',complete:true,customerId:15,showId:show.show_id,groups:f.groups},producerWork:{'last-success':now,'next-due':date+'T12:30:00Z','failure-count':0}}]};
 const options={api,clock:()=>now,readInput:async()=>structuredClone(input),archive:async(kind,value,scope)=>{const event={at:now,kind,outcome:'Created',records:Array.isArray(value)?value.length:1,file:'fixture/'+kind,sha256:'fixture',...scope};archives.push({event,value:structuredClone(value)});return event;}};
 return{api,seed,reads,writes,archives,input,options,now};
}
test('independent writer reads no tenant or source and persists ordinary links despite group uncertainty',async()=>{
 const f=fixture(),r=await runEntries(f.options);assert.equal(r.ok,true);assert.equal(r.result,'Partial');assert.equal(r.rows,1);assert.equal(r.accepted_rows,1);
 assert(f.reads.every(([b])=>[settings.BASE_ID,settings.SHOWS_BASE_ID,settings.LIVE_BASE_ID].includes(b)));
 const row=(await f.api.list('entries'))[0].fields;assert.equal(row['schedule-link-basis'],'class-date');assert.equal(row['group-link-status'],'unresolved');assert(row['source-trainer-ids']);assert.equal(row['trainer-ids'],undefined);
 assert.equal((await f.api.list('heartbeat'))[0].fields['heartbeat-key'],'wef-entries');assert(f.archives.some(a=>a.event.kind==='entries-before'));assert(f.archives.some(a=>a.event.kind==='retained-class-dataset'));
});
test('repeat preserves IDs, native links, helper fields and source observation time',async()=>{
 const f=fixture();await runEntries(f.options);const first=(await f.api.list('entries'))[0];await f.api.upsert('entries','entry-key',[{'entry-key':first.fields['entry-key'],nickname:'keep',horses:['helperHorse']}]);
 await runEntries(f.options);const after=await f.api.list('entries');assert.equal(after.length,1);assert.equal(after[0].id,first.id);assert.deepEqual(after[0].fields.schedules,first.fields.schedules);assert.equal(after[0].fields.nickname,'keep');assert.deepEqual(after[0].fields.horses,['helperHorse']);assert.equal(after[0].fields['last-seen'],f.now);
});
test('initial empty scope does not bulk-drop feed-only rows',async()=>{
 const f=fixture();f.seed('entries',[{'entry-key':'old-feed-only','show-id':200000076,date:'2026-10-30','class-id':200030671,status:'active','last-seen':'2026-10-30T11:00:00Z',schedules:['prior']}]);f.input.classes[0].payload.trips=[];f.input.classes[0].payload.total_entry_trips=0;
 const r=await runEntries(f.options);assert.equal(r.ok,true);assert.equal((await f.api.list('entries'))[0].fields.status,'active');assert.equal((await f.api.list('collections'))[0].fields['row-count'],0);
});
test('retirement is disabled even for later complete empty scope; links retained',async()=>{
 const f=fixture();await runEntries(f.options);const first=(await f.api.list('entries'))[0];f.input.classes[0].observedAt='2026-10-30T12:01:00Z';f.input.classes[0].producerWork['last-success']=f.input.classes[0].observedAt;f.input.classes[0].payload.trips=[];f.input.classes[0].payload.total_entry_trips=0;
 const r=await runEntries({...f.options,clock:()=> '2026-10-30T12:02:00Z'});assert.equal(r.ok,true);const after=(await f.api.list('entries'))[0];assert.equal(after.id,first.id);assert.equal(after.fields.status,'active');assert.deepEqual(after.fields.schedules,first.fields.schedules);assert.equal(after.fields['last-seen'],first.fields['last-seen']);
});
test('malformed scope preserves rows and membership; other scopes continue',async()=>{
 const f=fixture();await runEntries(f.options);const prior=(await f.api.list('entries'))[0],collection=(await f.api.list('collections'))[0];delete f.input.classes[0].payload.trips;
 const r=await runEntries(f.options);assert.equal(r.ok,false);assert.deepEqual((await f.api.list('entries'))[0],prior);assert.deepEqual((await f.api.list('collections'))[0].fields.entries,collection.fields.entries);
});
test('overdue input neither refreshes prior observation nor drops records',async()=>{
 const f=fixture();await runEntries(f.options);const before=(await f.api.list('entries'))[0];f.input.classes[0].producerWork['next-due']='2026-10-30T11:59:00Z';f.input.classes[0].payload.trips=[];f.input.classes[0].payload.total_entry_trips=0;
 const r=await runEntries(f.options);assert.equal(r.ok,true);assert.deepEqual((await f.api.list('entries'))[0],before);
});
test('newer existing observations never regress and scope membership stays linked',async()=>{
 const f=fixture();await runEntries(f.options);const before=(await f.api.list('entries'))[0];await f.api.upsert('entries','entry-key',[{'entry-key':before.fields['entry-key'],'last-seen':'2026-10-30T12:10:00Z','rider-name':'newer'}]);
 await runEntries(f.options);const after=(await f.api.list('entries'))[0];assert.equal(after.fields['rider-name'],'newer');assert.equal(after.fields['last-seen'],'2026-10-30T12:10:00Z');assert.deepEqual((await f.api.list('collections'))[0].fields.entries,[before.id]);
});
test('no shared schedules still processes Data participation and preserves prior native links as history',async()=>{
 const f=fixture();await runEntries(f.options);const prior=(await f.api.list('entries'))[0];f.seed(settings.SYNC_TABLE,[]);let reads=0;const result=await runEntries({...f.options,readInput:async()=>{reads++;return structuredClone(f.input);}});const after=(await f.api.list('entries'))[0];assert.equal(after.id,prior.id);assert.deepEqual(after.fields.schedules,prior.fields.schedules);assert.equal(after.fields['schedule-link-status'],'unresolved');assert.equal(after.fields['last-seen'],prior.fields['last-seen']);assert.equal(result.ok,true);assert.equal(result.rows,1);assert.equal(reads,1);
});
test('archive failure prevents participation writes',async()=>{
 const f=fixture();const r=await runEntries({...f.options,archive:async()=>{throw Error('Archive unavailable');}});assert.equal(r.ok,false);assert.equal(f.writes.includes('entries'),false);
});
test('Data access is GET-only and old tenant targets are forbidden',async()=>{
 let calls=0;const api=airtable({pause:async()=>{},request:async()=>{calls++;return{ok:true,json:async()=>({records:[]})};}});
 await api.list('runs',settings.DATA_BASE_ID,'{status}="Verified completed revision"');assert.equal(calls,1);
 await assert.rejects(api.call(settings.DATA_BASE_ID,'runs','PATCH',{}),/forbidden/);await assert.rejects(api.list('trainers','appxKPBtdl2Rdq7CS'),/Unapproved/);assert.equal(calls,1);
});
test('missing canonical show logs failure using null absent date and preserves business rows',async()=>{
 const f=fixture();f.seed(settings.SHARED_SHOWS_TABLE,[],settings.SHOWS_BASE_ID);
 const r=await runEntries(f.options);assert.equal(r.ok,false);assert.equal((await f.api.list('heartbeat'))[0].fields['selected-date'],null);assert.equal(f.writes.includes('entries'),false);
});
test('directory-only revision flags missing shared evidence and preserves records',async()=>{
 const f=fixture();f.input.classes=[];const r=await runEntries(f.options);
 assert.equal(r.ok,true);assert.equal(r.result,'Partial');assert(r.link_issues.some(x=>x.kind==='missing-class-evidence'));
 assert.equal(f.writes.includes('entries'),false);assert.equal(f.writes.includes('collections'),false);
});
test('applicable unfinished attempts persist on participation and scope provenance',async()=>{
 const f=fixture();f.input.attempts=[{key:'newer',status:'Writing',reservedClass:{'class-keys':[f.input.classes[0].scopeKey]}}];
 const r=await runEntries(f.options);assert.equal(r.ok,true);
 for(const [table,field] of [['entries','source-json'],['collections','message']]){
  const evidence=JSON.parse((await f.api.list(table))[0].fields[field]);
  assert.deepEqual(evidence.latestApplicableAttempts,f.input.attempts);assert.equal(evidence.currentness,'not-asserted');
 }
});
test('Airtable timestamp normalization verifies and repeats without rewriting source time',async()=>{
 const rows=[];let mutations=0;const api=airtable({pause:async()=>{},request:async(url,options)=>{
  if(options.method!=='GET'){mutations++;for(const record of JSON.parse(options.body).records){const fields={...record.fields};fields['last-seen']=new Date(fields['last-seen']).toISOString();rows.push({id:'recTime',fields});}}
  return{ok:true,json:async()=>({records:structuredClone(rows)})};
 }});
 const original='2026-10-29T14:11:18.894144+00:00',fields=[{'entry-key':'test','last-seen':original,'source-json':JSON.stringify({sourceAsOf:original})}];
 await api.upsert('entries','entry-key',fields);await api.upsert('entries','entry-key',fields);
 assert.equal(mutations,1);assert.equal(JSON.parse(rows[0].fields['source-json']).sourceAsOf,original);
});
test('ambiguous schedule is Conflict, preserves prior links and repeats while other scopes continue',async()=>{
 const f=fixture();await runEntries(f.options);const prior=(await f.api.list('entries'))[0];
 const schedules=await f.api.list('schedules',settings.SHARED_BASE_ID);
 const duplicate=structuredClone(schedules[0]);duplicate.id='duplicateSchedule';duplicate.fields['stage-key']+='-other';
 const healthy=structuredClone(schedules[0]);healthy.id='healthySchedule';healthy.fields['class-id']=200039999;healthy.fields['stage-key']='healthy-key';delete healthy.fields['row-json'];
 f.seed('schedules',[...schedules,duplicate,healthy],settings.SHARED_BASE_ID);f.seed(settings.SYNC_TABLE,[...schedules,duplicate,healthy]);
 const next=structuredClone(f.input.classes[0]);next.scopeKey+='-healthy';next.payload.class.class_id=200039999;for(const trip of next.payload.trips)if(trip.class_id!=null)trip.class_id=200039999;f.input.classes.push(next);
 for(let repeat=0;repeat<2;repeat++){
  const r=await runEntries(f.options);assert.equal(r.ok,true);assert.equal(r.result,'Partial');assert.equal(r.errors.length,0);
  const rows=await f.api.list('entries'),conflict=rows.find(x=>x.id===prior.id);assert.equal(rows.length,2);
  assert.equal(conflict.fields['schedule-link-status'],'Conflict');assert.equal(conflict.fields['schedule-link-basis'],'');
  assert.match(conflict.fields['link-issues'],/schedule: Ambiguous class\/date schedule/);
  for(const key of ['last-seen','schedules','schedule-key','schedule-keys','horse-id','rider-id'])assert.deepEqual(conflict.fields[key],prior.fields[key]);
  assert.equal(rows.find(x=>x.fields['class-id']===200039999).fields['schedule-link-status'],'linked');
 }
});
test('Data-only participation survives absent shared schedule coverage; external schedules are never read',async()=>{
 const f=fixture();const extra=structuredClone(f.input.classes[0]);extra.scopeKey+='outside';extra.payload.class.class_id=200039999;f.input.classes.push(extra);
 const list=f.api.list;f.api.list=async(t,b)=>{if(b===settings.SHARED_BASE_ID)throw Error('External schedules forbidden');return list(t,b);};
 const r=await runEntries(f.options);assert.equal(r.ok,true);assert.equal(r.rows,2);assert.equal(r.out_of_scope_classes,0);
 const rows=await f.api.list('entries');assert.equal(rows.length,2);assert(f.reads.every(([b])=>b!==settings.SHARED_BASE_ID));
 const source=JSON.parse(rows.find(r=>r.fields['class-id']===200039999).fields['source-json']);assert.equal(source.laneAssociations.schedules.status,'unresolved');assert.equal(source.format,'wef-entry-link-v1');assert(source.sourceEvidence);assert(source.trips);
});
test('unknown failed Data attempt does not gate accepted links or claim exact-class failure',async()=>{
 const f=fixture();f.input.attempts=[{key:'failed-inventory',status:'Failed',phase:'Writing',reservedClass:null}];
 const r=await runEntries(f.options);assert.equal(r.ok,true);const row=(await f.api.list('entries'))[0].fields;
 assert.equal(row['schedule-link-status'],'linked');assert.match(row['link-issues'],/roster/);assert.doesNotMatch(row['link-issues'],/inventory unavailable/);
 assert.deepEqual(JSON.parse(row['source-json']).latestApplicableAttempts,[]);assert.deepEqual(r.producer_attempts,f.input.attempts);
});

test('separate occurrence evidence never mixes Data IDs into native Schedules links',async()=>{
 const f=fixture(),item=f.input.classes[0],p=item.payload;
 f.input.schedules=[{key:'UNCHANGED-DATA-KEY',recordId:'recForeignData',customerId:15,showId:p.show_id,date:p.class_related_data.date.slice(0,10),classId:p.class.class_id,ringNumber:p.class_related_data.ring,kind:'class',sourceAsOf:f.now}];
 await runEntries(f.options);const first=(await f.api.list('entries'))[0],source=JSON.parse(first.fields['source-json']);
 assert.equal(source.laneAssociations.data.key,'UNCHANGED-DATA-KEY');assert.equal(source.laneAssociations.data.status,'linked');assert.equal(source.laneAssociations.schedules.status,'linked');assert(!first.fields.schedules.includes('recForeignData'));
 f.input.schedules.push({...f.input.schedules[0],key:'OTHER-UNCHANGED-KEY',recordId:'recOtherData'});
 await runEntries(f.options);const repeated=(await f.api.list('entries'))[0],after=JSON.parse(repeated.fields['source-json']);assert.equal(repeated.id,first.id);assert.deepEqual(repeated.fields.schedules,first.fields.schedules);assert.equal(after.laneAssociations.data.status,'Conflict');assert.equal(after.laneAssociations.schedules.status,'linked');assert.equal(repeated.fields['last-seen'],first.fields['last-seen']);
});
test('repeat preserves additional existing source-json properties',async()=>{
 const f=fixture();await runEntries(f.options);const first=(await f.api.list('entries'))[0],source=JSON.parse(first.fields['source-json']);source.existingOwnerEvidence={keep:'exact'};source.priorLink={keys:'historical-proof',observedAt:'original'};
 await f.api.upsert('entries','entry-key',[{'entry-key':first.fields['entry-key'],'source-json':JSON.stringify(source)}]);await runEntries(f.options);
 const after=(await f.api.list('entries'))[0];assert.deepEqual(JSON.parse(after.fields['source-json']).existingOwnerEvidence,{keep:'exact'});assert.equal(after.id,first.id);assert.deepEqual(JSON.parse(after.fields['source-json']).priorLink,source.priorLink);
});

test('TODAY enrichment bypasses unavailable multi-day Data without refreshing its time or links',async()=>{
 const f=fixture();await runEntries(f.options);const prior=(await f.api.list('entries'))[0],fields=prior.fields,trip=JSON.parse(fields['source-json']).trips[0];
 const observed={entryKey:fields['entry-key'],entryId:fields['entry-id'],classId:fields['class-id'],horseId:fields['horse-id'],riderId:fields['rider-id'],trainerId:trip.trainer_id,tripId:Number(trip.trip_id),tripUuid:trip.entryxclasses_uuid,sourceAsOf:'2026-10-30T12:01:00Z'};
 const result=await runEntries({...f.options,readInput:async()=>{throw Error('Multi-day Data unavailable');},readLive:async()=>({status:'verified',evidence:new Map([[fields['entry-key'],[observed]]])})});
 assert.equal(result.ok,false);assert.equal(result.live.enriched,1);const after=(await f.api.list('entries'))[0];assert.equal(after.id,prior.id);assert.equal(after.fields['last-seen'],fields['last-seen']);assert.deepEqual(after.fields.schedules,fields.schedules);const source=JSON.parse(after.fields['source-json']);assert.equal(source.sourceAsOf,JSON.parse(fields['source-json']).sourceAsOf);assert.equal(source.liveEvidence.observations[0].sourceAsOf,observed.sourceAsOf);
});

test('full-show archive references stay complete without exceeding the existing log field',async()=>{
 const f=fixture(),first=structuredClone(f.input.classes[0]);
 for(let i=1;i<400;i++){
  const item=structuredClone(first);item.scopeKey+='-'+i;item.payload.class.class_id=200040000+i;
  for(const trip of item.payload.trips)if(trip.class_id!=null)trip.class_id=item.payload.class.class_id;
  f.input.classes.push(item);
 }
 const upsert=f.api.upsert;
 f.api.upsert=async(t,k,values)=>{if(t==='run-logs')for(const value of values)if((value.archives||'').length>100000)throw Error('Log field too long');return upsert(t,k,values);};
 const r=await runEntries({...f.options,archive:async(kind,value,scope)=>{const event=await f.options.archive(kind,value,scope);event.file='existing-runtime-history/'.repeat(15)+kind;return event;}});
 assert.equal(r.ok,true);assert.equal(r.rows,400);const log=(await f.api.list('run-logs'))[0].fields;assert(log.archives.length<100000);assert.match(log.archives,/Full archive index/);
 const index=f.archives.find(a=>a.event.kind==='run-archive-index');assert(index);assert.equal(index.value.run_id,r.run_id);assert.equal(index.value.archives.filter(a=>a.kind==='retained-class-dataset').length,400);assert.equal(log['archive-count'],r.archives.length);
});
