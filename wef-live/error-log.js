"use strict";
const fs=require('node:fs'),path=require('node:path'),settings=require('./settings');
// One row per new/changed condition. Full affected-record details stay in run receipts.
async function report({api,runtime,lane,rows}){
 const file=path.join(runtime,'errors-'+lane+'.json');
 const signature=f=>JSON.stringify([f['show-id']??null,f.date??'',f.message]);
 const current=new Map(rows.map(f=>[signature(f),f]));
 let previous=[];
 try{previous=JSON.parse(fs.readFileSync(file,'utf8'));}catch(e){if(e.code!=='ENOENT')throw e;}
 const seen=new Set(previous),fresh=[...current].filter(([key])=>!seen.has(key));
 const checkpoint=keys=>{fs.mkdirSync(runtime,{recursive:true});const tmp=file+'.'+process.pid+'.tmp';fs.writeFileSync(tmp,JSON.stringify([...keys]));fs.renameSync(tmp,file);};
 for(let i=0;i<fresh.length;i+=10){
  const batch=fresh.slice(i,i+10);
  // Record the attempt first: a lost response must not cause duplicate creates.
  // Any uncertain outcome is reported by the caller in its existing local receipt.
  for(const [key]of batch)seen.add(key);checkpoint(seen);
  const saved=await api.call(settings.BASE_ID,'errs-log','POST',{records:batch.map(([,fields])=>({fields}))});
  if(saved.records?.length!==batch.length)throw Error('Incomplete error log response');
 }
 checkpoint(current.keys());
 return {logged:fresh.length,suppressed:rows.length-fresh.length};
}
module.exports={report};
