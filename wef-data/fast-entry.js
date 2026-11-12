'use strict';
const fs=require('node:fs'),path=require('node:path'),{spawn}=require('node:child_process'),log=require('./attempt-log');
async function main({linking=false}={}){
 const args=process.argv.slice(2),index=args.indexOf('--day'),day=index<0?1:Number(args[index+1]);
 const config=JSON.parse(fs.readFileSync(path.join(__dirname,'logging-schema.json'),'utf8')).workflows['wef-data'];
 if(linking)config.workflow='wef-data-linking';
 const label=linking?'fast-linker':'day'+day,root=path.join(__dirname,'runtime/fast',label,'logging'),attempt=log.begin(root,config);let report;
 try{report=await new Promise((resolve,reject)=>{
  const child=spawn(path.join(__dirname,'.venv/Scripts/python.exe'),['-B',path.join(__dirname,linking?'fast_linker.py':'fast-day.py'),...(linking?[]:['--day',String(day)])],{cwd:__dirname,env:{...process.env,WEF_LOG_ATTEMPT_ID:attempt.id},windowsHide:true,stdio:['ignore','pipe','pipe']});let out='',err='';child.stdout.on('data',v=>out+=v);child.stderr.on('data',v=>err+=v);child.on('error',reject);child.on('close',()=>{try{resolve(JSON.parse(out))}catch{reject(Error(err||'No collection report'))}});
 });}catch(e){report={ok:false,counts:{},'item-errors':[{key:label,error:e.message}]};}
 for(const e of report['item-errors']||[])log.recordError(root,attempt.id,{category:linking?'Linking':'Collection',target:e.key,message:e.error},label);
 log.finish(root,attempt.id,{status:report.ok?(report.state==='Skipped'?'Skipped':'Succeeded'):'Partial',stage:label,'show-key':report['show-key'],'run-key':report['run-key']?.split('|')[0],counts:report.counts});
 const delivery=await log.deliver(root,config);if(!delivery.ok)log.recordError(root,attempt.id,{category:'Logging',message:delivery.error},'LogDelivery');
 const result={report,delivery};log.atomic(path.join(__dirname,'runtime/fast',label+'-last-run.json'),result);console.log(JSON.stringify(result));return result;
}
if(require.main===module)main().catch(e=>{console.error(e.message);process.exitCode=1;});
module.exports={main};
