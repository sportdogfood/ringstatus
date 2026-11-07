'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),os=require('node:os'),path=require('node:path'),vm=require('node:vm'),{EventEmitter}=require('node:events');
const logger=require('../../attempt-log'),{pruneDelivered:prune}=require('./candidate/cleanup-local-logs');
function fixture(t){
 const root=fs.mkdtempSync(path.join(os.tmpdir(),'wef-cleanup-proof-'));t.after(()=>fs.rmSync(root,{recursive:true,force:true}));
 const config={base:'appFixture',workflow:'wef-data',tables:{'run-logs':'runs','err-logs':'errors',heartbeat:'heart'}},db={runs:[],errors:[],heart:[]};
 const request=async(method,table,{key,primary,id,fields})=>{if(method==='GET')return db[table].filter(r=>r.fields[primary]===key);const row=method==='POST'?{id:'rec'+Object.values(db).flat().length,fields}:db[table].find(r=>r.id===id);row.fields=fields;if(method==='POST')db[table].push(row);return row;};
 const add=(id,n,status='Succeeded',error=true)=>{logger.begin(root,config,id,`2026-11-06T12:0${n}:00.000Z`);if(status!=='Started')logger.finish(root,id,{status,stage:'Collection','run-key':'run-'+id,counts:{saved:10},...(error?{error:{message:'fixture '+id}}:{})},`2026-11-06T12:0${n}:01.000Z`);const a=logger.readAttempt(root,id);delete a['owner-pid'];logger.atomic(path.join(root,'run-logs',id+'.json'),a);};
 const flush=()=>logger.flush(root,config,request,{milliseconds:100000,operations:1000});
 const exists=(table,id)=>fs.existsSync(path.join(root,table,id+'.json'));
 return {root,config,db,add,flush,exists};
}
test('exact delivered families removed; heartbeat and Airtable history unchanged; repeat is empty',async t=>{
 const f=fixture(t);f.add('old',1);f.add('latest',2);assert.equal((await f.flush()).ok,true);const before=JSON.stringify(f.db);assert.deepEqual(prune(f.root,f.config),{runs:1,errors:1,acks:2});assert(!f.exists('run-logs','old'));assert(f.exists('run-logs','latest'));assert.equal((await f.flush()).operations,0);assert.equal(JSON.stringify(f.db),before);assert.deepEqual(prune(f.root,f.config),{runs:0,errors:0,acks:0});
});
test('missing error acknowledgment preserves entire parent family byte for byte',async t=>{
 const f=fixture(t);f.add('pending',1);f.add('latest',2);await f.flush();fs.unlinkSync(path.join(f.root,'acks','err-logs-pending-0001.json'));const before=fs.readFileSync(path.join(f.root,'run-logs','pending.json'),'utf8');prune(f.root,f.config);assert(f.exists('err-logs','pending-0001'));assert.equal(fs.readFileSync(path.join(f.root,'run-logs','pending.json'),'utf8'),before);
});
test('changed run, changed error and malformed/missing acknowledgment are retained',async t=>{
 const f=fixture(t);for(const [i,id] of ['runChange','errorChange','badAck','noAck','latest'].entries())f.add(id,i);await f.flush();
 const a=logger.readAttempt(f.root,'runChange');a.reason='new undelivered value';logger.atomic(path.join(f.root,'run-logs','runChange.json'),a);
 const ep=path.join(f.root,'err-logs','errorChange-0001.json'),e=JSON.parse(fs.readFileSync(ep));e.message='undelivered';logger.atomic(ep,e);
 fs.writeFileSync(path.join(f.root,'acks','run-logs-badAck.json'),'{');fs.unlinkSync(path.join(f.root,'acks','run-logs-noAck.json'));
 assert.deepEqual(prune(f.root,f.config),{runs:0,errors:0,acks:0});for(const id of ['runChange','errorChange','badAck','noAck'])assert(f.exists('run-logs',id));
});
test('newest success and newest error remain when newest attempt is different',async t=>{
 const f=fixture(t);f.add('success',1,'Succeeded',false);f.add('error',2,'Failed');f.add('latest',3,'Skipped',false);await f.flush();assert.deepEqual(prune(f.root,f.config),{runs:0,errors:0,acks:0});const before=JSON.stringify(f.db.heart);await f.flush();assert.equal(JSON.stringify(f.db.heart),before);
});
test('active, started and other-workflow logs are never pruned',async t=>{
 const f=fixture(t);f.add('active',1);f.add('started',2,'Started',false);f.add('latest',3);await f.flush();const a=logger.readAttempt(f.root,'active');a['owner-pid']=process.pid;logger.atomic(path.join(f.root,'run-logs','active.json'),a);assert.equal(prune(f.root,f.config).runs,0);delete a['owner-pid'];logger.atomic(path.join(f.root,'run-logs','active.json'),a);assert.equal(prune(f.root,{...f.config,workflow:'wef-data-linking'}).runs,0);
});
test('error deletion failure leaves parent and pending logs readable',async t=>{
 const f=fixture(t);f.add('old',1);f.add('latest',2);await f.flush();const unlink=fs.unlinkSync;try{fs.unlinkSync=file=>{if(file.endsWith('old-0001.json'))throw Error('fixture denied');return unlink(file);};assert.throws(()=>prune(f.root,f.config),/fixture denied/);}finally{fs.unlinkSync=unlink;}assert(f.exists('run-logs','old'));assert.equal((await f.flush()).ok,true);
});
