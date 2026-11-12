"use strict";
const fs=require('node:fs'),path=require('node:path'),{spawn,execFile}=require('node:child_process'),log=require('./attempt-log');
const python=path.join(__dirname,'.venv/Scripts/python.exe');
async function publishGroup(report){
 const repo=path.resolve(__dirname,'../../ringstatus-data');
 const parts=report['show-key'].split('|'),folder='wef-data/shows/'+parts[0]+'/'+parts[1]+'/day0-order-of-go/'+report.day;
 const git=args=>new Promise((resolve,reject)=>execFile('git',['-C',repo,...args],{windowsHide:true,env:{...process.env,GIT_TERMINAL_PROMPT:'0'}},(error,stdout,stderr)=>error?reject(Error(log.safe(stderr||error.message))):resolve(stdout)));
 const changed=await git(['status','--porcelain','--',folder]);
 if(changed.trim()){
  await git(['add','--',folder]);await git(['commit','-m','Save Day 0 grouped order of go '+report.day,'--',folder]);
 }
 return {ok:true,changed:Boolean(changed.trim()),commit:(await git(['rev-parse','HEAD'])).trim(),pushed:false};
}
async function main(){
 const config=JSON.parse(fs.readFileSync(path.join(__dirname,'logging-schema.json'),'utf8')).workflows['wef-data'];
 const args=process.argv.slice(2),rerun=args.includes('--rerun'),index=args.indexOf('--day'),day=index<0?1:Number(args[index+1]);
 const label=rerun?'day-reruns':'day'+day,stage=rerun?'Trouble':'Day'+day;
 const root=path.join(__dirname,'runtime',label,'logging'),attempt=log.begin(root,config);let report;
 const pythonArgs=['-B',path.join(__dirname,'day1.py'),...(rerun?['--rerun']:['--day',String(day)])];
 try{report=await new Promise((resolve,reject)=>{
  const child=spawn(python,pythonArgs,{cwd:__dirname,env:{...process.env,WEF_LOG_ATTEMPT_ID:attempt.id},windowsHide:true,stdio:['ignore','pipe','pipe']});
  let out='',err='';child.stdout.on('data',v=>out+=v);child.stderr.on('data',v=>err+=v);child.on('error',reject);
  child.on('close',code=>{try{resolve(JSON.parse(out));}catch{reject(Error(err||'Collector returned no report; exit '+code));}});
 });}
 catch(error){report={ok:false,counts:{},'item-errors':[{key:stage,error:error.message}]};}
 const dayReports=rerun?(report.counts?.days||[]):[report];
 for(const item of dayReports)if(item.counts?.['day-number']===0&&item.day){
  try{report['git-archive']=await publishGroup(item);}
  catch(error){report.ok=false;report['git-archive']={ok:false,error:error.message};report['item-errors']=[...(report['item-errors']||[]),{key:'Git order-of-go archive',error:error.message}];}
 }
 for(const error of report['item-errors']||[])log.recordError(root,attempt.id,{category:'Collection',target:error.key,message:error.error},stage);
 log.finish(root,attempt.id,{status:report.ok?(report.state==='Skipped'?'Skipped':'Succeeded'):'Partial',stage,'show-key':report['show-key'],'run-key':report['run-key']?.split('|')[0],counts:report.counts});
 const delivery=await log.deliver(root,config);
 if(!delivery.ok)log.recordError(root,attempt.id,{category:'Logging',message:delivery.error},'LogDelivery');
 const result={report,delivery};log.atomic(path.join(__dirname,'runtime',label+'-last-run.json'),result);console.log(JSON.stringify(result));
}
if(require.main===module)main().catch(error=>{console.error(error.message);process.exitCode=1;});
module.exports={main,publishGroup};
