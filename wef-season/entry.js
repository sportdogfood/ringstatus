"use strict";
const fs=require('node:fs'),path=require('node:path'),{spawn}=require('node:child_process');
const logging=require('../wef-data/attempt-log');
const {acquireLock}=require('../wef-schedules/collect');
const config=JSON.parse(fs.readFileSync(path.join(__dirname,'../wef-data/logging-schema.json'),'utf8'));
async function scheduled(){
 const attempts=JSON.parse(process.env.WEF_SEASON_ATTEMPTS||'{}');if(Object.keys(attempts).length!==4)throw Error('Four durable Season starts required');
 let release;
 try{release=acquireLock(path.join(__dirname,'runtime/collector.lock'));}
 catch(error){for(const [workflow,id] of Object.entries(attempts)){const root=path.join(__dirname,'runtime/logging',workflow);logging.finish(root,id,{status:'Skipped',stage:'OverlapGuard',reason:'Existing Season invocation still holds the lock'});await logging.deliver(root,config.workflows[workflow]);}return;}
 try{
 for(const [workflow,id] of Object.entries(attempts)){const root=path.join(__dirname,'runtime/logging',workflow);const row=logging.readAttempt(root,id);row['child-pid']=process.pid;logging.atomic(path.join(root,'run-logs',id+'.json'),row);}
 for(const [workflow,id] of Object.entries(attempts)){const root=path.join(__dirname,'runtime/logging',workflow);logging.recoverInterrupted(root,id);const result=await logging.deliver(root,config.workflows[workflow]);if(!result.ok)logging.recordError(root,id,{category:'Logging',message:result.error},'LogDelivery');}
 const result=await new Promise(resolve=>{const child=spawn(path.join(__dirname,'../wef-data/.venv/Scripts/python.exe'),['-B',path.join(__dirname,'season.py')],{cwd:__dirname,env:{...process.env,PYTHONDONTWRITEBYTECODE:'1'},windowsHide:true,stdio:['ignore','pipe','pipe']});let out='';child.stdout.on('data',b=>{out+=b;if(out.length>100000)child.kill()});child.stderr.on('data',()=>{});child.on('error',()=>resolve({error:'Season process could not start'}));child.on('close',code=>{try{resolve(JSON.parse(out))}catch{resolve({error:'Season process returned no valid receipt',exit:code})}})});
 for(const [workflow,id] of Object.entries(attempts)){const entity=workflow.replace('wef-season-',''),root=path.join(__dirname,'runtime/logging',workflow);const outcome=result[entity]||{status:'Failed',stage:'Process',error:{category:'Process',message:result.error||'Missing Season result'}};if(outcome.status==='Failed'){const stop=path.join(__dirname,'runtime',entity,'stopped.json');if(!fs.existsSync(stop))logging.atomic(stop,{at:new Date().toISOString(),...outcome});}logging.finish(root,id,outcome);const delivered=await logging.deliver(root,config.workflows[workflow]);if(!delivered.ok)logging.recordError(root,id,{category:'Logging',message:delivered.error},'LogDelivery');}
 const file=path.join(__dirname,'runtime/last-run.json');logging.atomic(file,{completed:new Date().toISOString(),results:result});
 }finally{release();}
}
if(require.main===module)scheduled().catch(e=>{console.error(logging.safe(e.message));process.exitCode=1});
module.exports={scheduled};
