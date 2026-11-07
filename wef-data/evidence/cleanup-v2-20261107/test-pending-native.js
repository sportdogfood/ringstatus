const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),os=require('node:os'),path=require('node:path');
const log=require('../../attempt-log'),helper=require('./candidate/cleanup-local-logs'),{airtable}=require('./candidate/cleanup-airtable'),schema=require('./candidate/schema.json');
test('older pending local family protects its actual native Airtable parent and all errors',async t=>{
 const root=fs.mkdtempSync(path.join(os.tmpdir(),'v2-pending-parent-'));t.after(()=>fs.rmSync(root,{recursive:true,force:true}));const config={workflow:'wef-data',base:'appFixture',tables:{'run-logs':'runs','err-logs':'errors',heartbeat:'heart'}};const db={runs:[],errors:[],heart:[]};
 const request=async(method,table,{primary,key,id,fields})=>{if(method==='GET')return db[table].filter(r=>r.fields[primary]===key);const r=method==='POST'?{id:'rec'+Object.values(db).flat().length,fields}:db[table].find(r=>r.id===id);r.fields=fields;if(method==='POST')db[table].push(r);return r;};
 const local=path.join(root,'wef-data');for(const [i,id]of ['older','delivered','latest'].entries()){log.begin(local,config,id,'2026-11-07T00:0'+i+':00Z');log.finish(local,id,{status:'Succeeded'},'2026-11-07T00:0'+i+':01Z');}
 assert.equal((await log.flush(local,config,request,{operations:100})).ok,true);
 log.recordError(local,'older',{message:'not delivered'},'Logging');
 const pending=helper.pending(root,config.base);assert.deepEqual(pending.attempts,['older']);assert.deepEqual(pending.errors,['older-0001']);
 const older=db.runs.find(r=>r.fields['attempt-id']==='older'),delivered=db.runs.find(r=>r.fields['attempt-id']==='delivered'),latest=db.runs.find(r=>r.fields['attempt-id']==='latest');
 const nativeErrors=[{id:'recOldError',fields:{'error-id':'previous-error',run:[older.id],'occurred-at':'2026-11-07T00:00:00Z'}}];
 const api=airtable({localPending:()=>pending,pause:async()=>{},request:async url=>({ok:true,json:async()=>({records:url.includes((schema.tables['run-logs']?.id||'run-logs'))?db.runs:url.includes((schema.tables['err-logs']?.id||'err-logs'))?nativeErrors:[{id:'recHeartbeat',fields:{'last-run':[latest.id]}}]})})});
 await api.prepare();assert(api.protectedIds.has(older.id));assert(api.protectedIds.has('recOldError'));assert(api.protectedIds.has(latest.id));assert(!api.protectedIds.has(delivered.id));
 await log.flush(local,config,request,{operations:100});assert.deepEqual(helper.pending(root,config.base),{attempts:[],errors:[]});
});
