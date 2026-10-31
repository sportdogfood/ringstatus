"use strict";
const fs=require('node:fs'),path=require('node:path'),{spawn}=require('node:child_process');
const {acquireLock,atomicReport}=require('../wef-schedules/collect');
async function collect(runtime){
 let release;
 try{
  release=acquireLock(path.join(runtime,'collector.lock'));
  const result=await new Promise((resolve,reject)=>{
   const child=spawn(path.join(__dirname,'.venv','Scripts','python.exe'),['-B',path.join(__dirname,'scheduled.py')],
    {cwd:__dirname,env:{...process.env,PYTHONDONTWRITEBYTECODE:'1'},windowsHide:true,stdio:['ignore','pipe','pipe']});
   let output='',error='';child.stdout.on('data',value=>output+=value);child.stderr.on('data',value=>error+=value);
   child.on('error',reject);child.on('close',code=>resolve({code,output,error}));
  });
  const report=JSON.parse(result.output.trim());
  if(result.code!==0){report.ok=false;report.state='Partial';report['item-errors']=[...(report['item-errors']||[]),{key:'Collector',error:result.error||'Collector process failed'}];}
  return report;
 }catch(error){return {ok:false,state:'Partial',counts:{saved:0},'item-errors':[{key:'Collector',error:error.message}]};}
 finally{if(release)release();}
}
async function scheduled(){
 const runtime=path.join(__dirname,'runtime');fs.mkdirSync(runtime,{recursive:true});
 const attempt=process.env.WEF_LOG_ATTEMPT_ID;
 let logging,root,config;
 if(attempt){
  logging=require('./attempt-log');root=path.join(runtime,'logging/wef-data');
  config=JSON.parse(fs.readFileSync(path.join(__dirname,'logging-schema.json'),'utf8')).workflows['wef-data'];
  const row=logging.readAttempt(root,attempt);row['child-pid']=process.pid;logging.atomic(path.join(root,'run-logs',attempt+'.json'),row);
 }
 const report=await collect(runtime);
 await atomicReport(path.join(runtime,'last-run.json'),report);console.log(JSON.stringify(report));
 if(attempt){
  const errors=report['item-errors']||[];
  for(const error of errors)logging.recordError(root,attempt,{category:'Collection',operation:'Collect',target:error.key,message:error.error,'write-outcome':'Unknown'},'Collection');
  logging.finish(root,attempt,{status:errors.length?'Partial':'Succeeded',stage:'Collection','show-key':report['show-key'],counts:report.counts});
  const delivered=await logging.deliver(root,config);
  if(!delivered.ok)logging.recordError(root,attempt,{category:'Logging',message:delivered.error,'write-outcome':'Unknown'},'LogDelivery');
 }
}
if(require.main===module)scheduled().catch(error=>console.error(error.message));
module.exports={scheduled};
