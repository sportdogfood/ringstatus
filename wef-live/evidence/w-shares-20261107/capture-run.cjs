'use strict';
// Saved receipts and Airtable GET only. Does not import a business runner.
const fs=require('node:fs'),path=require('node:path'),crypto=require('node:crypto');
const label=process.argv[2];if(!/^[a-z0-9-]+$/.test(label||''))throw Error('Unique evidence label required');
const file=path.join(__dirname,label+'.json');if(fs.existsSync(file))throw Error('Preserve prior evidence');
const root=path.resolve(__dirname,'../..'),runtime=path.join(root,'runtime');
const read=n=>JSON.parse(fs.readFileSync(path.join(runtime,n),'utf8'));
(async()=>{
 const selected=process.argv[3]?JSON.parse(fs.readFileSync(process.argv[3],'utf8')):null;
 const linker=read(selected?'linker-'+selected.linker+'.json':'linker-last-run.json'),lanes=['groups','rings','classes'].map(n=>read(selected?'lane-'+n+'-'+selected[n]+'.json':'lane-'+n+'-last-run.json')),status=read(selected?'live-check-'+selected.status+'.json':'live-check-last-run.json');
 const ids=[...lanes.map(r=>r.run_id),linker.run_id];const filterByFormula='OR('+ids.map(id=>'{run-id}='+JSON.stringify(id)).join(',')+')';
 const out={observed_at:new Date().toISOString(),status,lanes,linker,logs:{}};
 for(const table of ['run-logs','heartbeat','errs-log']){
  const q=new URLSearchParams({pageSize:'100',filterByFormula});const r=await fetch('https://api.airtable.com/v0/appgzosUcVqA4vZdf/'+table+'?'+q,{headers:{Authorization:'Bearer '+process.env.WEF_LIVE_AIRTABLE_TOKEN},signal:AbortSignal.timeout(30000)});if(!r.ok)throw Error('Airtable GET '+table+' '+r.status);out.logs[table]=await r.json();await new Promise(r=>setTimeout(r,250));
 }
 const m=JSON.parse(fs.readFileSync(path.join(__dirname,'deployed.json'),'utf8'));out.runtime_hashes_match=m.hashes.every(x=>crypto.createHash('sha256').update(fs.readFileSync(path.join(root,x.file))).digest('hex')===x.sha256);
 fs.writeFileSync(file,JSON.stringify(out,null,2));console.log(JSON.stringify({file,linker:{run_id:linker.run_id,started:linker.started,completed:linker.completed,ok:linker.ok,counts:linker.counts,issues:linker.issues.length,stage_errors:linker.stage_errors,logging_errors:linker.logging_errors},lanes:lanes.map(r=>({lane:r.lane,started:r.started,completed:r.completed,ok:r.ok,issues:r.issues.length,decoded_requests:r.timings.requests.filter(x=>x.label.startsWith('identity-')).length})),native_logs:Object.fromEntries(Object.entries(out.logs).map(([k,v])=>[k,v.records?.length])),hashes_match:out.runtime_hashes_match}));
})().catch(e=>{console.error(e.message);process.exitCode=1});
