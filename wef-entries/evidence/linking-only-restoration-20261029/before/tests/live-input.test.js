'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),os=require('node:os'),zlib=require('node:zlib'),crypto=require('node:crypto');
const {verifyClass,readLive,forParticipation}=require('../live-input'),settings=require('../settings');
function fixture(t){
 const root=fs.mkdtempSync(path.join(os.tmpdir(),'entries-live-test-'));t.after(()=>fs.rmSync(root,{recursive:true,force:true}));
 const date='2026-10-29',now=date+'T15:00:00Z',show={customer_id:15,show_id:76,start_date:date,end_date:'2026-11-01'};
 const member={'live-entry-key':`76|${date}|7|10`,'entry-key':`76|${date}|20|7`,'show-id':76,'customer-id':15,date,'scope-status':'current','last-seen':now,'source-updated-at':'original-source-value','entry-id':20,'horse-id':30,'rider-id':40,'trip-id':10,'trainer-id':50,_class:`76|${date}|7`,'source-json':JSON.stringify({identity:{trip_id:10,entry_id:20,horse_id:30,rider_id:40,entryxclasses_uuid:'original-uuid'}})};
 const body=Buffer.from(JSON.stringify([member])),fingerprint=crypto.createHash('sha256').update(body).digest('hex'),file=path.join(root,fingerprint+'.json.gz');fs.writeFileSync(file,zlib.gzipSync(body));
 const collection={id:'recCollection',fields:{'collection-id':`76|${date}|class|7`,'show-id':76,date,'source-kind':'class','source-id':'7',status:'complete','row-count':1,fingerprint,'archive-file':file}};
 const cls={id:'recClass',fields:{'class-key':member._class,'class-id':7,'show-id':76,'customer-id':15,date,'scope-status':'current','last-seen':now,total:1,collection:['recCollection']}};
 const { _class,...clean}=member,entries=[{id:'recLive',fields:{...clean,class:['recClass'],collection:['recCollection'],entry:['circularBinder'],'participation-link-status':'linked'}}];
 const row={'entry-key':member['entry-key'],'entry-id':20,'class-id':7,'horse-id':30,'rider-id':40,'source-trips':[{entry_id:20,horse_id:30,rider_id:40,trainer_id:50,trip_id:10,entryxclasses_uuid:'original-uuid'}]};
 return{root,now,show,member,collection,cls,entries,row};
}
test('verified original normalized membership enriches exact participation, never binder or group',t=>{
 const f=fixture(t),values=verifyClass(f),evidence=forParticipation(f.row,values);assert.equal(evidence.status,'verified');assert.equal(values[0].recordId,'recLive');assert.equal(values[0].sourceAsOf,f.now);assert.equal(values[0].groupAssociation.status,'unresolved');assert.equal(values[0].entry,undefined);
 assert.equal(forParticipation({...f.row,'source-trips':[{...f.row['source-trips'][0],trip_id:99}]},values),null);
 assert.equal(forParticipation({...f.row,'horse-id':99},values),null);
});
test('stale, wrong scope, incomplete, mutated and archive tamper are isolated',t=>{
 for(const kind of ['stale','show','customer','class','count','collection','member','path','hash']){
  const f=fixture(t);
  if(kind==='stale')f.now='2026-10-29T16:00:00Z';
  if(kind==='show')f.cls.fields['show-id']=99;
  if(kind==='customer')f.entries[0].fields['customer-id']=99;
  if(kind==='class')f.cls.fields['class-key']='76|2026-10-29|99';
  if(kind==='count')f.collection.fields['row-count']=0;
  if(kind==='collection')f.collection.fields.status='error';
  if(kind==='member')f.entries[0].fields['rider-id']=99;
  if(kind==='path')f.root=path.join(f.root,'not-parent');
  if(kind==='hash')fs.writeFileSync(f.collection.fields['archive-file'],zlib.gzipSync(Buffer.from('[]')));
  assert.throws(()=>verifyClass(f),undefined,kind);
 }
});
test('optional Live failure and empty class never block accepted Data',async t=>{
 const f=fixture(t);const result=await readLive({list:async()=>{throw Error('Live unavailable');}},f.show,f.now,{root:f.root});assert.equal(result.status,'skipped');assert.equal(result.evidence.size,0);
});
test('read fences ignore reverse binder changes but reject source races',async t=>{
 for(const race of [false,true]){
  const f=fixture(t);let reads=0;
  const api={list:async table=>{reads++;const rows=structuredClone(table===settings.LIVE_ENTRIES?f.entries:table===settings.LIVE_CLASSES?[f.cls]:[f.collection]);if(reads>3&&table===settings.LIVE_ENTRIES){rows[0].fields.entry=['changedReverse'];if(race)rows[0].fields['horse-id']=999;}return rows;}};
  const result=await readLive(api,f.show,f.now,{root:f.root});assert.equal(result.status,race?'skipped':'verified');assert.equal(result.evidence.size,race?0:1);
 }
});
test('a newly added class member between reads invalidates enrichment',async t=>{
 const f=fixture(t);let calls=0;
 const api={list:async table=>{calls++;const rows=structuredClone(table===settings.LIVE_ENTRIES?f.entries:table===settings.LIVE_CLASSES?[f.cls]:[f.collection]);if(calls>3&&table===settings.LIVE_ENTRIES)rows.push({id:'newMember',fields:{...f.entries[0].fields,'live-entry-key':'new'}});return rows;}};
 const result=await readLive(api,f.show,f.now,{root:f.root});assert.equal(result.status,'skipped');assert.equal(result.evidence.size,0);
});
test('only the three Live tables are readable and remain read-only',async()=>{
 let calls=0;const api=require('../airtable').airtable({pause:async()=>{},request:async()=>{calls++;return{ok:true,json:async()=>({records:[]})};}});
 for(const table of [settings.LIVE_ENTRIES,settings.LIVE_CLASSES,settings.LIVE_COLLECTIONS]){await api.list(table,settings.LIVE_BASE_ID);await assert.rejects(api.call(settings.LIVE_BASE_ID,table,'PATCH',{}),/forbidden/);}
 await assert.rejects(api.list('live-groups',settings.LIVE_BASE_ID),/Unapproved/);assert.equal(calls,3);
});
