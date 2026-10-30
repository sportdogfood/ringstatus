'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const {readLiveClasses}=require('./class-live-source');
const {digest}=require('./pace');
const at='2026-10-29T14:00:00Z',now='2026-10-29T14:05:00Z',date='2026-10-29',classKey='76|2026-10-29|4';
function fixture(){
 const common={'show-id':76,'customer-id':15,date,'scope-status':'current','last-seen':at};
 const rawGroup={ring_number:2,jo_total:0,r2_total:0,jo_gone:0,r2_gone:0};
 const group={...common,'group-key':'76|2026-10-29|3','class-group-id':3,'source-json':JSON.stringify(rawGroup),_classes:[classKey]};
 const rows=[1,2].map(id=>({...common,'trip-id':id,_class:classKey,gone:1,
 'source-json':JSON.stringify({live:{id,class_id:4,Gone:1},identity:{trip_id:id}})}));
 const archives={group:[group],class:rows};
 const collections=['group','class'].map(kind=>({id:kind+'Collection',fields:{'show-id':76,date,
 status:'complete','source-kind':kind==='group'?'groups':'class','source-id':kind==='group'?'76':'4',
 completed:at,'row-count':archives[kind].length,fingerprint:digest(JSON.stringify(archives[kind])),'archive-file':kind}}));
 const f={...common,'class-id':4,'class-key':classKey,'detail-available':true,groups:['group1'],collection:['classCollection'],
 total:2,gone:2,'source-json':JSON.stringify({gone_trips:'1,2',jo_gone_trips:'',r2_gone_trips:''})};
 const db={'live-classes':[{id:'class1',fields:f}],'live-groups':[{id:'group1',fields:{...group,collection:['groupCollection']}}],collections};
 let calls={};let change=null;
 return{db,archives,setChange(fn){change=fn;},args:{customer:15,show:76,now,
 api:{async list(base,table){assert.equal(base,'appgzosUcVqA4vZdf');calls[table]=(calls[table]||0)+1;
 const rows=structuredClone(db[table]);if(calls[table]>1&&change)change(table,rows);return rows;}},
 readArchive:async file=>structuredClone(archives[file])}};
}
test('populated Live fixture returns exact source class/group identity and archived class counts',async()=>{
 const x=fixture(),r=await readLiveClasses(x.args);assert.deepEqual(r.issues,[]);
 const v=r.classes.get('76|2026-10-29|2|3|4');
 assert.deepEqual(v.identity,{customer:15,show:76,date,ring:2,group:3,class:4});
 assert.equal(v.identity.occurrence,undefined);assert.equal(v.counts.completed,2);assert.equal(v.counts.total,2);
 assert.equal(v.ref.parts.length,2);assert.equal(v.completion,undefined);
});
test('wrong legacy link name does not silently bind a class',async()=>{
 const x=fixture();x.db['live-classes'][0].fields['live-groups']=['group1'];delete x.db['live-classes'][0].fields.groups;
 const r=await readLiveClasses(x.args);assert.equal(r.classes.size,0);assert.equal(r.issues[0].reason,'ambiguous_group');
});
test('raw header cannot fabricate completed trips against frozen trip evidence',async()=>{
 const x=fixture();x.db['live-classes'][0].fields['source-json']=JSON.stringify({gone_trips:'1,1'});
 const r=await readLiveClasses(x.args);assert.equal(r.classes.size,0);assert.equal(r.issues[0].reason,'class_counts');
});
test('missing gone list stays unknown rather than zero',async()=>{
 const x=fixture();x.db['live-classes'][0].fields['source-json']='{}';
 const r=await readLiveClasses(x.args);assert.equal(r.classes.size,1);assert.equal([...r.classes.values()][0].counts.completed,null);
});
test('archive tampering is rejected even with injected reader',async()=>{
 const x=fixture();x.archives.class[0].gone=0;
 const r=await readLiveClasses(x.args);assert.equal(r.classes.size,0);assert.equal(r.issues[0].reason,'archive_coverage_or_hash');
});
test('group projection must match frozen archive, not current mutable source-json alone',async()=>{
 const x=fixture();x.db['live-groups'][0].fields['source-json']=JSON.stringify({ring_number:5});
 const r=await readLiveClasses(x.args);assert.equal(r.classes.size,0);assert.equal(r.issues[0].reason,'group_projection');
});
test('fresh class cannot borrow stale group evidence',async()=>{
 const x=fixture();x.db['live-groups'][0].fields['last-seen']='2026-10-29T13:50:00Z';
 const r=await readLiveClasses(x.args);assert.equal(r.classes.size,0);
});
test('known additional rounds remain phase counts, never whole-class confirmation',async()=>{
 const x=fixture();const raw=JSON.parse(x.archives.group[0]['source-json']);raw.jo_total=2;
 x.archives.group[0]['source-json']=JSON.stringify(raw);
 x.db['live-groups'][0].fields['source-json']=JSON.stringify(raw);
 x.db.collections[0].fields.fingerprint=digest(JSON.stringify(x.archives.group));
 const r=await readLiveClasses(x.args);assert.equal([...r.classes.values()][0].counts.completion_scope,'phase');
});
test('duplicate class identity invalidates all duplicates',async()=>{
 const x=fixture();x.db['live-classes'].push({...structuredClone(x.db['live-classes'][0]),id:'other'});
 const r=await readLiveClasses(x.args);assert.equal(r.classes.size,0);assert.equal(r.issues.length,2);
});
test('class, group and collection mutations during reads each reject the snapshot',async()=>{
 for(const changed of ['live-classes','live-groups','collections']){
  const x=fixture();x.setChange((table,rows)=>{if(table===changed)rows[0].fields.changed=true;});
  await assert.rejects(readLiveClasses(x.args),/changed during read/);
 }
});
test('empty Live is a valid missing-Live input; unavailable source read fails explicitly',async()=>{
 const x=fixture();x.db['live-classes']=[];assert.equal((await readLiveClasses(x.args)).classes.size,0);
 x.args.api.list=async()=>{throw Error('read failure');};await assert.rejects(readLiveClasses(x.args),/read failure/);
});
