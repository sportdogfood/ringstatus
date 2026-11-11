"use strict";
const fs=require('node:fs'),path=require('node:path'),crypto=require('node:crypto'),{spawn}=require('node:child_process');
const TARGET='01a169bc-e58a-7de0-94c6-5d3931fcf745';
function failures(reports){
 const rows=Array.isArray(reports)?reports:[reports];
 return rows.flatMap(r=>{
  if(!r)return[];
  const errors=[...(r.stage_errors||[]).map(e=>e.message),...(r.logging_errors||[])];
  // Linker no-match checks and routine date warnings stay in existing error logs.
  if(r.lane!=='linker')errors.push(...(r.issues||[]).map(e=>e.message).filter(s=>!/^Warning:/.test(s)));
  if(r.cleanup?.error)errors.push('Cleanup: '+r.cleanup.error);
  else if(['Error','Blocked','Failed'].includes(r.cleanup?.result))errors.push('Cleanup: '+r.cleanup.result);
  if(r.ok===false&&!errors.length)errors.push('Run failed without error detail');
  return [...new Set(errors)].map(message=>({lane:r.lane||'scheduled',run_id:r.run_id||null,show:r.scope?.show,date:r.scope?.date,message}));
 });
}
function deliver(prompt,{spawnProcess=spawn,cli=path.join(process.env.LOCALAPPDATA||'','Programs/OpenAI/Codex/bin/codex.exe')}={}){
 return new Promise((resolve,reject)=>{
  const child=spawnProcess(cli,['exec','--sandbox','read-only','resume',TARGET,'-','--json'],{windowsHide:true,stdio:['pipe','pipe','pipe'],env:Object.fromEntries(Object.entries(process.env).filter(([k])=>!/TOKEN|COOKIE|API_KEY|PASSWORD|SECRET/i.test(k)))});
  let buffer='',ack=false,ended=false;
  const timer=setTimeout(()=>{child.kill();finish(Error('Records delivery timed out'));},60000);
  function finish(error){if(ended)return;ended=true;clearTimeout(timer);error?reject(error):resolve();}
  child.stdout.on('data',part=>{buffer+=part.toString();let n;while((n=buffer.indexOf('\n'))>=0){const line=buffer.slice(0,n);buffer=buffer.slice(n+1);try{const e=JSON.parse(line);if(e.type==='item.completed'&&e.item?.type==='agent_message'&&e.item.text?.trim()==='WEF_LIVE_ALERT_RECEIVED')ack=true;}catch{}}});
  child.stderr.on('data',()=>{});child.on('error',finish);child.stdin.on('error',finish);
  child.on('close',code=>finish(code===0&&ack?null:Error('Records delivery exited '+code+(ack?'':' without acknowledgement'))));
  child.stdin.end(prompt);
 });
}
async function notify(reports,{runtime=require('./settings').RUNTIME,send=deliver,clock=()=>new Date().toISOString()}={}){
 fs.mkdirSync(runtime,{recursive:true});
 const connection=path.join(runtime,'records-alert-connection.json'),first=!fs.existsSync(connection),errors=failures(reports);
 if(!first&&!errors.length)return{result:'Not needed'};
 const row={alert_id:crypto.randomUUID(),target:TARGET,started:clock(),kind:first?'Connection check':'Failure alert',errors,result:'Started'};
 const file=path.join(runtime,'records-alert-'+row.alert_id+'.json');
 const save=()=>{fs.writeFileSync(file,JSON.stringify(row,null,2));if(first)fs.writeFileSync(connection,JSON.stringify(row,null,2));};save();
 const message='Scheduled WEF Live '+row.kind+'. '+(first?'This is a delivery connection check, not a fabricated collection failure. ':'')+'\n'+errors.map(e=>JSON.stringify(e)).join('\n')+'\nAcknowledge receipt only with exactly WEF_LIVE_ALERT_RECEIVED. No tools, edits, source calls, collector runs, retries, repairs or other actions.';
 try{await send(message);row.result='Delivered';}catch(e){row.result='Failed';row.error=e.message;}
 row.completed=clock();save();return row;
}
module.exports={notify,deliver,failures,TARGET};
