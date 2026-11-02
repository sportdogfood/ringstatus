'use strict';
// Airtable GET and saved-receipt inspection only. Never invokes a business runner/provider.
const fs=require('node:fs'),path=require('node:path');
const label=process.argv[2];if(!/^[a-z0-9-]+$/.test(label||''))throw Error('Need unique snapshot label');
const file=path.join(__dirname,label+'.json');if(fs.existsSync(file))throw Error('Preserve existing snapshot');
const runtime=path.resolve(__dirname,'../../runtime'),base='appgzosUcVqA4vZdf';
async function list(table,baseId=base){let offset;const rows=[];do{const q=new URLSearchParams({pageSize:'100',...(offset?{offset}:{})});
 const r=await fetch(`https://api.airtable.com/v0/${baseId}/${encodeURIComponent(table)}?${q}`,{headers:{Authorization:'Bearer '+process.env.WEF_LIVE_AIRTABLE_TOKEN},signal:AbortSignal.timeout(30000)});
 if(!r.ok)throw Error('Read-only '+table+' HTTP '+r.status);const d=await r.json();rows.push(...d.records);offset=d.offset;await new Promise(r=>setTimeout(r,260));}while(offset);return rows;
}
(async()=>{const snapshot={at:new Date().toISOString(),tables:{},receipts:{}};
 for(const t of ['live-rings','live-groups','live-classes','live-entries','live-class-appearances','heartbeat','run-logs','errs-log','collections','cleanup','cleanup-logs'])snapshot.tables[t]=await list(t);
 snapshot.shows=await list('shows','appCaSmL819KK7UFJ');
 for(const name of ['lane-groups-last-run.json','lane-rings-last-run.json','lane-classes-last-run.json','cleanup-last-run.json','cleanup-state.json','cleanup-stop.json']){
  const p=path.join(runtime,name);if(fs.existsSync(p))snapshot.receipts[name]=JSON.parse(fs.readFileSync(p,'utf8'));
 }
 fs.writeFileSync(file,JSON.stringify(snapshot,null,2));console.log(JSON.stringify({file,at:snapshot.at,counts:Object.fromEntries(Object.entries(snapshot.tables).map(([t,r])=>[t,r.length])),cleanup:snapshot.receipts['cleanup-last-run.json']?{run_id:snapshot.receipts['cleanup-last-run.json'].run_id,result:snapshot.receipts['cleanup-last-run.json'].result}:null}));
})().catch(e=>{console.error(e.message);process.exitCode=1;});
