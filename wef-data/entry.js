"use strict";
const fs=require('node:fs'),path=require('node:path'),{spawn}=require('node:child_process');
async function atomicReport(filename, report) {

  fs.mkdirSync(path.dirname(filename),{recursive:true});

  const tmp=filename+"."+process.pid+".tmp";

  try {

    fs.writeFileSync(tmp,JSON.stringify(report,null,2));

    // Windows can briefly hold the destination open. Retry replacement only;

    // never delete the last complete receipt or repeat business writes.

    for(let attempt=0;;attempt++) {

      try { fs.renameSync(tmp,filename);break; }

      catch(error) {

        if(!["EPERM","EACCES","EBUSY"].includes(error.code)||attempt===4) throw error;

        await new Promise(resolve=>setTimeout(resolve,25*(attempt+1)));

      }

    }

  }

  finally { if(fs.existsSync(tmp)) fs.unlinkSync(tmp); }

}
function read(file){return JSON.parse(fs.readFileSync(file,'utf8'));}
function collectionKeys(runtime){
 const state=read(path.join(runtime,'day-cadence.json'));
 let scope,show;
 try{const latest=read(path.join(runtime,'last-run.json'));scope=latest.counts?.['day-priorities'];show=latest['show-key'];}catch{}
 if(scope&&!Object.values(scope).includes(0))scope=undefined;
 let zero;
 if(scope&&show){const day=Object.keys(scope).find(day=>scope[day]===0);if(day)zero=[show+'|'+day,state[show+'|'+day]||{attempted:0}];}
 zero||=Object.entries(state).filter(([,r])=>r.priority===0).sort((a,b)=>b[1].attempted-a[1].attempted)[0];
 if(!zero)return {keys:[],zero:null};
 show=zero[0].split('|').slice(0,-1).join('|');
 const days=scope?Object.entries(scope).filter(([,priority])=>[0,1,2,3,4].includes(priority)).map(([day,priority])=>
  [show+'|'+day,{...(state[show+'|'+day]||{attempted:0}),priority}]):
  Object.entries(state).filter(([k,r])=>k.startsWith(show+'|')&&k.split('|').at(-1)>=zero[0].split('|').at(-1)&&[0,1,2,3,4].includes(r.priority));
 const keys=days.filter(([,r])=>r['run-key'])
  .map(([k,r])=>[k,r['run-key']]).sort((a,b)=>a[0].localeCompare(b[0]));
 return {keys,zero,days};
}
function due(runtime,linking=false,now=new Date()){
 try{
  const {keys,zero,days}=collectionKeys(runtime);
  if(!zero)return !linking;
  const today=new Intl.DateTimeFormat('en-CA',{timeZone:'America/New_York',year:'numeric',month:'2-digit',day:'2-digit'}).format(now);
  const day=zero[0].split('|').at(-1),interval=today===day?180:3600;
  if(!linking){
   const future=days.filter(([,r])=>r.priority>0).sort((a,b)=>a[1].priority-b[1].priority);
   return today===day||now.getTime()/1000-Number(zero[1].attempted)>=3600||future.some(([,r],i)=>i===0&&(r['force-next']===true||now.getTime()/1000-Number(r.attempted)>=1800));
  }
  if(!keys.length)return false;
  const previous=read(path.join(runtime,'linking','last-run.json'));
  if(previous.ok&&JSON.stringify(previous.counts?.['collection-run-keys'])===JSON.stringify(keys))return false;
  if(!previous.counts?.['collection-run-keys'])return true;
  const attempt=String(previous['run-key']||'').replace(/^linking-/,'');
  const started=read(path.join(runtime,'logging','wef-data-linking','run-logs',attempt+'.json')).started;
  return today===day||now.getTime()-Date.parse(started)>=interval*1000;
 }catch{return true;}
}
async function collect(runtime,linking=false){
 try{
  const result=await new Promise((resolve,reject)=>{
   const child=spawn(path.join(__dirname,'.venv','Scripts','python.exe'),['-B',path.join(__dirname,linking?'linking-scheduled.py':'scheduled.py')],
    {cwd:__dirname,env:{...process.env,PYTHONDONTWRITEBYTECODE:'1'},windowsHide:true,stdio:['ignore','pipe','pipe']});
   let output='',error='';child.stdout.on('data',value=>output+=value);child.stderr.on('data',value=>error+=value);
   child.on('error',reject);child.on('close',code=>resolve({code,output,error}));
  });
  const report=JSON.parse(result.output.trim());
  if(result.code!==0){report.ok=false;report.state='Partial';report['item-errors']=[...(report['item-errors']||[]),{key:'Collector',error:result.error||'Collector process failed'}];}
  return report;
 }catch(error){return {ok:false,state:'Partial',counts:{saved:0},'item-errors':[{key:'Collector',error:error.message}]};}
}
async function scheduled(){
 const linking=(process.argv||[]).includes('--linking'),workflow=linking?'wef-data-linking':'wef-data',stage=linking?'Linking':'Collection';
 const baseRuntime=path.join(__dirname,'runtime'),runtime=linking?path.join(baseRuntime,'linking'):baseRuntime;fs.mkdirSync(runtime,{recursive:true});
 const attempt=process.env.WEF_LOG_ATTEMPT_ID;
 let logging,root,config;
 if(attempt){
  logging=require('./attempt-log');root=path.join(baseRuntime,'logging',workflow);
  config=JSON.parse(fs.readFileSync(path.join(__dirname,'logging-schema.json'),'utf8')).workflows[workflow];
  const row=logging.readAttempt(root,attempt);row['child-pid']=process.pid;logging.atomic(path.join(root,'run-logs',attempt+'.json'),row);
 }
 let inputs;
 if(linking){try{inputs=collectionKeys(baseRuntime).keys;}catch{}}
 const report=await collect(runtime,linking);
 if(linking&&attempt&&!report['run-key'])report['run-key']='linking-'+attempt;
 if(linking&&inputs)report.counts={...report.counts,'collection-run-keys':inputs};
 if(!linking && attempt){
  try{require('./collection-alerts').check(runtime,report,attempt,logging);}
  catch(error){logging.recordError(root,attempt,{category:'Notification',message:error.message},'Alert');}
 }
 for(const day of report.counts?.days || []) delete day['confirmed-classes'];
 await atomicReport(path.join(runtime,'last-run.json'),report);console.log(JSON.stringify(report));
 if(attempt){
  const errors=report['item-errors']||[];
  for(const error of errors)logging.recordError(root,attempt,{category:stage,operation:linking?'Link':'Collect',target:error.key,message:error.error,'write-outcome':'Unknown'},stage);
  logging.finish(root,attempt,{status:errors.some(error=>!error.nonblocking)?'Partial':'Succeeded',stage,'show-key':report['show-key'],'run-key':report['run-key'],counts:report.counts,reason:errors.length?errors[0].error:undefined});
  const delivered=await logging.deliver(root,config);
  if(!delivered.ok)logging.recordError(root,attempt,{category:'Logging',message:delivered.error,'write-outcome':'Unknown'},'LogDelivery');
 }
}
if(require.main===module){
 if(process.argv.includes('--due'))process.exitCode=due(path.join(__dirname,'runtime'),process.argv.includes('--linking'))?0:3;
 else scheduled().catch(error=>console.error(error.message));
}
module.exports={scheduled,due,collectionKeys};
