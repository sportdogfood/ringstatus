'use strict';
const fs=require('node:fs'),path=require('node:path'),{spawn}=require('node:child_process');
const log=require('../wef-data/attempt-log');
const deliveryLog=require('./logging');
const {acquireLock}=require('../wef-schedules/collect');
const schema=require('./schema.json');
const root=path.join(__dirname,'runtime/logging');
const config={base:schema.base,workflow:'wef-people',tables:Object.fromEntries(['run-logs','err-logs','heartbeat'].map(n=>[n,schema.tables[n].id]))};
async function scheduled(){
 const id=process.env.WEF_PEOPLE_ATTEMPT;
 if(!id)throw Error('Durable scheduled attempt required');
 let release;
 try { release=acquireLock(path.join(__dirname,'runtime/collector.lock')); }
 catch(error){if(!error.message.includes('already running'))throw error;log.finish(root,id,{status:'Skipped',stage:'OverlapGuard',reason:'Existing invocation holds lock'});await deliveryLog.deliver(root,config);return;}
 const stop=path.join(__dirname,'runtime/stopped.json');
 try{
  const current=log.readAttempt(root,id);current['child-pid']=process.pid;log.atomic(path.join(root,'run-logs',id+'.json'),current);
  log.recoverInterrupted(root,id);
  const logs=fs.readdirSync(path.join(root,'run-logs')).filter(n=>n.endsWith('.json')).map(n=>JSON.parse(fs.readFileSync(path.join(root,'run-logs',n),'utf8')));
  if(logs.some(r=>r.id!==id && ['Failed','Interrupted'].includes(r.status)) && !fs.existsSync(stop))log.atomic(stop,{at:new Date().toISOString(),reason:'Prior failed or interrupted invocation requires authorization'});
  if(fs.existsSync(stop)){
   log.finish(root,id,{status:'Skipped',stage:'Stopped',reason:'Prior failure; owner authorization required'});await deliveryLog.deliver(root,config);return;
  }
  const result=await new Promise(resolve=>{
   const child=spawn(path.join(__dirname,'../wef-data/.venv/Scripts/python.exe'),['-B',path.join(__dirname,'join.py')],{cwd:__dirname,env:{...process.env,PYTHONDONTWRITEBYTECODE:'1'},windowsHide:true,stdio:['ignore','pipe','pipe']});
   let out='';child.stdout.on('data',b=>{out+=b});child.stderr.on('data',()=>{});
   child.on('error',()=>resolve({status:'Failed',stage:'Process',error:{category:'Process',message:'People process could not start'}}));
   child.on('close',code=>{try{const value=JSON.parse(out);if(code!==0 && value.status!=='Failed')throw Error();resolve(value)}catch{resolve({status:'Failed',stage:'Process',error:{category:'Process',message:'People returned no valid terminal receipt'}})}});
  });
  for(const issue of deliveryLog.grouped(result.issues||[]))log.recordError(root,id,issue,'Join');
  delete result.issues;
  if(result.status==='Failed')log.atomic(stop,{at:new Date().toISOString(),...result});
  log.finish(root,id,result);
  const delivery=await deliveryLog.deliver(root,config);
  if(!delivery.ok)log.recordError(root,id,{category:'Logging',message:delivery.error},'LogDelivery');
  log.atomic(path.join(__dirname,'runtime/last-run.json'),{at:new Date().toISOString(),result,delivery});
  if(result.status==='Failed')process.exitCode=1;
 }finally{release();}
}
if(require.main===module)scheduled().catch(error=>{
 log.atomic(path.join(__dirname,'runtime/stopped.json'),{at:new Date().toISOString(),reason:log.safe(error.message)});
 console.error(log.safe(error.message));process.exitCode=1;
});
module.exports={scheduled};
