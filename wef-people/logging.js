'use strict';
// Adapt the existing attempt-log transport; retain its durable outbox/readback.
const fs=require('node:fs'),path=require('node:path');
const log=require('../wef-data/attempt-log');
function grouped(issues){
 const groups=new Map();
 for(const issue of issues){const key=JSON.stringify([issue.category,issue.message]);if(!groups.has(key))groups.set(key,{...issue,targets:[]});groups.get(key).targets.push(issue.target);}
 const output=[];
 for(const issue of groups.values()){
  let targets=[];
  for(const target of issue.targets){
   if(JSON.stringify(targets.concat(target)).length>1100 && targets.length){output.push({...issue,target:'Shared source records',message:issue.message+'; source-context='+JSON.stringify(targets)});targets=[];}
   targets.push(target);
  }
  if(targets.length)output.push({...issue,target:'Shared source records',message:issue.message+'; source-context='+JSON.stringify(targets)});
 }
 return output;
}
async function deliver(root,config){
 const token=process.env.AIRTABLE_TOKEN;
 if(!token)return {ok:false,error:'Logging token unavailable'};
 let last=0;
 const request=async(method,table,options)=>{
  const delay=250-(Date.now()-last);if(delay>0)await new Promise(resolve=>setTimeout(resolve,delay));last=Date.now();
  if(config.base!=='app6uHgUYCCKTVlJE'||!Object.values(config.tables).includes(table))throw Error('Logging scope mismatch');
  const query=method==='GET'?'?'+new URLSearchParams({filterByFormula:'{'+options.primary+'}='+JSON.stringify(options.key),pageSize:'100'}):'';
  const body=method==='GET'?undefined:JSON.stringify({records:[{...(method==='PATCH'?{id:options.id}:{}),fields:options.fields}]});
  const r=await fetch('https://api.airtable.com/v0/'+config.base+'/'+table+query,{method,headers:{Authorization:'Bearer '+token,'Content-Type':'application/json'},body,signal:AbortSignal.timeout(5000)});
  if(!r.ok)throw Error('Logging Airtable '+method+' HTTP '+r.status);
  const data=await r.json();if(method==='GET'&&data.offset)throw Error('Ambiguous logging identity');
  return method==='GET'?data.records:data.records[0];
 };
 const count=['run-logs','err-logs'].reduce((n,t)=>n+(fs.existsSync(path.join(root,t))?fs.readdirSync(path.join(root,t)).filter(f=>f.endsWith('.json')).length:0),0);
 // Size delivery to the actual durable outbox; never cap source coverage.
 return log.flush(root,config,request,{operations:3*(count+1),milliseconds:Math.max(20000,16000*(count+1))});
}
module.exports={grouped,deliver};
