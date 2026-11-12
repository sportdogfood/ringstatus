'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const fs=require('node:fs'),os=require('node:os'),path=require('node:path');
const logger=require(process.env.WEF_LOG_REVIEW_MODULE||'./candidate/attempt-log.js');
function fixture(){
 const root=fs.mkdtempSync(path.join(os.tmpdir(),'wef-log-cap-'));
 const config={base:'appFixture',workflow:'wef-data',tables:{'run-logs':'tblRuns','err-logs':'tblErrors',heartbeat:'tblHeart'}};
 const db={tblRuns:[],tblErrors:[],tblHeart:[]};let calls=0;
 const request=async(method,table,{primary,key,fields,id})=>{
  calls++;
  if(method==='GET')return db[table].filter(r=>r.fields[primary]===key);
  if(method==='POST'){const row={id:'rec'+Object.values(db).flat().length,fields};db[table].push(row);return row;}
  const row=db[table].find(r=>r.id===id);Object.assign(row.fields,fields);return row;
 };
 return {root,config,db,request,get calls(){return calls;},close(){fs.rmSync(root,{recursive:true,force:true});}};
}
test('all 51 collection errors deliver once beyond the former 30-call limit',async()=>{
 const f=fixture();try{
  logger.begin(f.root,f.config,'current');
  for(let i=0;i<51;i++)logger.recordError(f.root,'current',{category:'Collection',target:'class-'+i,message:'Source HTTP 502'},'Collection');
  logger.finish(f.root,'current',{status:'Partial'});
  const first=await logger.flush(f.root,f.config,f.request);
  assert.equal(first.ok,true);assert.equal(first.operations,159);
  assert.equal(f.db.tblErrors.length,51);
  assert(f.db.tblErrors.every(r=>r.fields.run[0]===f.db.tblRuns[0].id));
  const repeated=await logger.flush(f.root,f.config,f.request);
  assert.equal(repeated.ok,true);assert.equal(repeated.operations,0);
  assert.equal(f.db.tblErrors.length,51);
 }finally{f.close();}
});
test('elapsed delivery time is not a synthetic logging failure; real failures remain pending',async()=>{
 const f=fixture(),originalNow=Date.now;let clock=originalNow(),fail=true;
 try{
  logger.begin(f.root,f.config,'current');logger.finish(f.root,'current',{status:'Succeeded'});
  Date.now=()=>clock;
  const request=async(...args)=>{clock+=21000;if(fail&&f.calls===1)throw Error('actual connection reset');return f.request(...args);};
  const failed=await logger.flush(f.root,f.config,request);
  assert.equal(failed.ok,false);assert.equal(failed.error,'actual connection reset');
  assert.equal(fs.existsSync(path.join(f.root,'acks/run-logs-current.json')),false);
  fail=false;
  const recovered=await logger.flush(f.root,f.config,request);
  assert.equal(recovered.ok,true);assert.equal(f.db.tblRuns.length,1);
  assert.equal(f.db.tblRuns[0].fields.status,'Succeeded');
 }finally{Date.now=originalNow;f.close();}
});
