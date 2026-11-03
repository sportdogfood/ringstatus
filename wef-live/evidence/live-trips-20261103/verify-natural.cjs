'use strict';
// GET-only Airtable + existing task/runtime evidence; never invokes a collector/provider.
const fs=require('node:fs'),path=require('node:path'),crypto=require('node:crypto');
const label=process.argv[2];if(!/^[a-z0-9-]+$/.test(label||''))throw Error('Unique label required');
const output=path.join(__dirname,label+'.json');if(fs.existsSync(output))throw Error('Preserve evidence');
const root=path.resolve(__dirname,'../..'),runtime=path.join(root,'runtime');
(async()=>{
 const deployed=JSON.parse(fs.readFileSync(path.join(__dirname,'deployed.json'))),last=JSON.parse(fs.readFileSync(path.join(runtime,'live-check-last-run.json'))),cleanup=JSON.parse(fs.readFileSync(path.join(runtime,'cleanup-last-run.json')));
 const report={at:new Date().toISOString(),live_check:last,cleanup,new_tables:{},cleanup_config:[],run_records:{},no_manual_run:true};
 async function get(table,q={}){const r=await fetch(`https://api.airtable.com/v0/appgzosUcVqA4vZdf/${encodeURIComponent(table)}?${new URLSearchParams(q)}`,{headers:{Authorization:'Bearer '+process.env.WEF_LIVE_AIRTABLE_TOKEN},signal:AbortSignal.timeout(30000)});if(!r.ok)throw Error(table+' HTTP '+r.status);return(await r.json()).records;}
 for(const t of ['live-trips','live-trip-results','live-trip-messages','live-trip-comments'])report.new_tables[t]=await get(t,{pageSize:'3'});
 report.cleanup_config=await get('cleanup',{filterByFormula:'OR('+Object.keys(report.new_tables).map(t=>`{Name}="${t}"`).join(',')+')'});
 if(Date.parse(last.started)>Date.parse(deployed.deployed_at))for(const t of ['heartbeat','run-logs','errs-log'])report.run_records[t]=await get(t,{filterByFormula:`{run-id}="${last.run_id}"`});
 report.installed_hashes_match=deployed.hashes.every(x=>crypto.createHash('sha256').update(fs.readFileSync(path.join(root,x.file))).digest('hex')===x.sha256);
 const text=fs.readFileSync(path.join(runtime,'collector-2026-11-03.log'),'utf8').replace(/\u0000/g,'');report.task_log=text.split(/\r?\n/).filter(x=>x.startsWith('START ')||x.startsWith('END ')||x.includes('next-check-not-due')).slice(-12);
 report.no_cleanup_stop=!fs.existsSync(path.join(runtime,'cleanup-stop.json'));
 report.result=report.installed_hashes_match&&report.no_cleanup_stop&&report.cleanup_config.length===4&&report.cleanup_config.every(x=>x.fields.cleanup===true)?'PASS':'FAIL';
 fs.writeFileSync(output,JSON.stringify(report,null,2));console.log(JSON.stringify({output,result:report.result,live_check:{run_id:last.run_id,started:last.started,live_status:last.live_status,reason:last.reason},sample_counts:Object.fromEntries(Object.entries(report.new_tables).map(([t,x])=>[t,x.length])),config_count:report.cleanup_config.length,task_log:report.task_log}));if(report.result==='FAIL')process.exitCode=1;
})().catch(e=>{console.error(e.message);process.exitCode=1});
