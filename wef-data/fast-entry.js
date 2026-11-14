'use strict';
const fs=require('node:fs'),path=require('node:path'),{spawn}=require('node:child_process'),log=require('./attempt-log');
async function requestReadyTask(task){
 const requested=new Date().toISOString();
 try{return await new Promise((resolve,reject)=>{
  const child=spawn(path.join(process.env.SystemRoot||'C:\\Windows','System32','schtasks.exe'),['/Run','/TN',task],{windowsHide:true,stdio:['ignore','pipe','pipe']});let out='',err='';
  child.stdout.on('data',v=>out+=v);child.stderr.on('data',v=>err+=v);child.on('error',reject);
  child.on('close',code=>resolve({task,requested,completed:new Date().toISOString(),ok:code===0,...(code===0?{}:{error:(err||out||'Task start failed').trim(),code})}));
 });}catch(e){return {task,requested,completed:new Date().toISOString(),ok:false,error:e.message};}
}
async function main({linking=false}={}){
 const args=process.argv.slice(2),saving=args.includes('--saving'),index=args.indexOf('--day'),day=index<0?1:Number(args[index+1]);
 const config=JSON.parse(fs.readFileSync(path.join(__dirname,'logging-schema.json'),'utf8')).workflows['wef-data'];
 if(linking)config.workflow='wef-data-linking';else if(saving)config.workflow='wef-data-save';
 const label=linking?'fast-linker':saving?'fast-save':'day'+day,root=path.join(__dirname,'runtime/fast',label,'logging'),attempt=log.begin(root,config);let report;
 try{report=await new Promise((resolve,reject)=>{
  const child=spawn(path.join(__dirname,'.venv/Scripts/python.exe'),['-B',path.join(__dirname,linking?'fast_linker.py':'fast-day.py'),...(linking?[]:saving?['--saving']:['--day',String(day)])],{cwd:__dirname,env:{...process.env,WEF_LOG_ATTEMPT_ID:attempt.id},windowsHide:true,stdio:['ignore','pipe','pipe']});let out='',err='';child.stdout.on('data',v=>out+=v);child.stderr.on('data',v=>err+=v);child.on('error',reject);child.on('close',()=>{try{resolve(JSON.parse(out))}catch{reject(Error(err||'No collection report'))}});
 });}catch(e){report={ok:false,counts:{},'item-errors':[{key:label,error:e.message}]};}
 for(const e of report['item-errors']||[])log.recordError(root,attempt.id,{category:linking?'Linking':saving?'Saving':'Collection',target:e.key,message:e.error},label);
 log.finish(root,attempt.id,{status:report.ok?(['Skipped','Deferred'].includes(report.state)?report.state:'Succeeded'):'Partial',stage:label,'show-key':report['show-key'],'run-key':report['run-key']?.split('|')[0],counts:report.counts,reason:report.reason});
 let readyTask;
 const nextTask=!saving&&!linking&&report.state==='Collected'&&report.manifest?'wef-data-save':null;
 if(nextTask){readyTask=await requestReadyTask(nextTask);if(!readyTask.ok)log.recordError(root,attempt.id,{category:'Trigger',target:nextTask,message:readyTask.error},'ReadyTask');}
 let delivery={ok:true,deferred:true};
 if(saving){
  for(let n=0;n<5;n++){const sourceRoot=path.join(__dirname,'runtime/fast','day'+n,'logging');if(!fs.existsSync(sourceRoot))continue;const sourceConfig={...config,workflow:'wef-data'};const sent=await log.deliver(sourceRoot,sourceConfig);if(!sent.ok){delivery=sent;break;}}
 }
 if((saving||linking)&&delivery.ok)delivery=await log.deliver(root,config);if(!delivery.ok)log.recordError(root,attempt.id,{category:'Logging',message:delivery.error},'LogDelivery');
 const result={report,delivery,...(readyTask?{'ready-task':readyTask}:{})};log.atomic(path.join(__dirname,'runtime/fast',label+'-last-run.json'),result);console.log(JSON.stringify(result));return result;
}
if(require.main===module)main().catch(e=>{console.error(e.message);process.exitCode=1;});
module.exports={main,requestReadyTask};
