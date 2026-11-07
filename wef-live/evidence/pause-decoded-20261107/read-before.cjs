'use strict';
// Airtable GET only; never loads a collector or source endpoint.
const fs=require('node:fs'),path=require('node:path');
(async()=>{
 const out={at:new Date().toISOString(),tables:{}};
 for(const table of ['live-trips','live-rings']){
  const rows=[];let offset;
  do{const q=new URLSearchParams({pageSize:'100',filterByFormula:'AND({show-id}=200000077,{date}="2026-11-07")',...(offset?{offset}:{})});
   const r=await fetch('https://api.airtable.com/v0/appgzosUcVqA4vZdf/'+table+'?'+q,{headers:{Authorization:'Bearer '+process.env.WEF_LIVE_AIRTABLE_TOKEN},signal:AbortSignal.timeout(30000)});
   if(!r.ok)throw Error('Airtable GET '+r.status);const d=await r.json();rows.push(...d.records);offset=d.offset;await new Promise(r=>setTimeout(r,250));
  }while(offset);out.tables[table]=rows;
 }
 const file=path.join(__dirname,'before-records.json');if(fs.existsSync(file))throw Error('Preserve evidence');fs.writeFileSync(file,JSON.stringify(out,null,2));console.log(JSON.stringify(Object.fromEntries(Object.entries(out.tables).map(([k,v])=>[k,v.length]))));
})().catch(e=>{console.error(e.message);process.exitCode=1});

