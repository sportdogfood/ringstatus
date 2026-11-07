'use strict';
// Read-only Airtable evidence. Never loads or invokes the collector/linker.
const fs=require('node:fs'),path=require('node:path');
const label=process.argv[2];if(!/^[a-z0-9-]+$/.test(label||''))throw Error('Evidence label required');
const output=path.join(__dirname,label+'.json');if(fs.existsSync(output))throw Error('Preserve prior evidence');
const names=['live-rings','live-groups','live-classes','live-trips','live-entries','w-shows','w-days','wef-data.schedules'];
(async()=>{const out={observed_at:new Date().toISOString(),tables:{}};
 for(const table of names){const records=[];let offset;do{const q=new URLSearchParams({pageSize:'100',...(offset?{offset}:{}),...(table.startsWith('live-')?{filterByFormula:'AND({show-id}=200000077,{date}="2026-11-07")'}:{})});
  const r=await fetch('https://api.airtable.com/v0/appgzosUcVqA4vZdf/'+encodeURIComponent(table)+'?'+q,{headers:{Authorization:'Bearer '+process.env.WEF_LIVE_AIRTABLE_TOKEN},signal:AbortSignal.timeout(30000)});if(!r.ok)throw Error('GET '+table+' HTTP '+r.status);const d=await r.json();records.push(...d.records);offset=d.offset;await new Promise(r=>setTimeout(r,250));
 }while(offset);out.tables[table]=records;}
 fs.writeFileSync(output,JSON.stringify(out,null,2));console.log(JSON.stringify({output,counts:Object.fromEntries(Object.entries(out.tables).map(([k,v])=>[k,v.length]))}));
})().catch(e=>{console.error(e.message);process.exitCode=1});

