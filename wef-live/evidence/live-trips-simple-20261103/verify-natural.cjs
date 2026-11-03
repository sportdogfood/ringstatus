'use strict';
const fs=require('node:fs'),path=require('node:path'),crypto=require('node:crypto');
const label=process.argv[2];if(!/^[a-z0-9-]+$/.test(label||''))throw Error('Unique label needed');const output=path.join(__dirname,label+'.json');if(fs.existsSync(output))throw Error('Preserve evidence');
const root=path.resolve(__dirname,'../..'),runtime=path.join(root,'runtime');
(async()=>{const d=JSON.parse(fs.readFileSync(path.join(__dirname,'deployed.json'))),last=JSON.parse(fs.readFileSync(path.join(runtime,'live-check-last-run.json'))),cleanup=JSON.parse(fs.readFileSync(path.join(runtime,'cleanup-last-run.json')));
async function get(table,q={}){const r=await fetch(`https://api.airtable.com/v0/appgzosUcVqA4vZdf/${encodeURIComponent(table)}?${new URLSearchParams(q)}`,{headers:{Authorization:'Bearer '+process.env.WEF_LIVE_AIRTABLE_TOKEN},signal:AbortSignal.timeout(30000)});if(!r.ok)throw Error(table+' HTTP '+r.status);return(await r.json()).records;}
const controls=await get('cleanup',{filterByFormula:'OR({Name}="live-trips",{Name}="live-trip-results",{Name}="live-trip-messages",{Name}="live-trip-comments")'}),trips=await get('live-trips',{pageSize:'1'});
const taskLog=fs.readFileSync(path.join(runtime,'collector-2026-11-03.log'),'utf8').replace(/\u0000/g,'').split(/\r?\n/).filter(x=>x.startsWith('START ')||x.startsWith('END ')||x.includes('next-check-not-due')).slice(-9);
const checks={hashes:d.hashes.every(x=>crypto.createHash('sha256').update(fs.readFileSync(path.join(root,x.file))).digest('hex')===x.sha256),no_stop:!fs.existsSync(path.join(runtime,'cleanup-stop.json')),single_trip_control:controls.length===1&&controls[0].fields.Name==='live-trips'&&controls[0].fields.cleanup===true};
const report={at:new Date().toISOString(),last,cleanup,controls,trips,taskLog,checks,result:Object.values(checks).every(Boolean)?'PASS':'FAIL'};fs.writeFileSync(output,JSON.stringify(report,null,2));console.log(JSON.stringify({output,result:report.result,checks,taskLog}));if(report.result==='FAIL')process.exitCode=1;
})().catch(e=>{console.error(e.message);process.exitCode=1});
