"use strict";
const ENGINE='appam4opFT4NeL4mn',SCHEDULES='appUq5Qz9kARqFija',LIVE='appgzosUcVqA4vZdf';
const TABLES={answers:'tblHnV5iEJ13DOsBF',checkpoint:'tblepakByOH4G4ruV'};
const TIMING='appYOykKIy1kAquhP',SHOWS='appCaSmL819KK7UFJ',DATA_TABLES={answers:'tblhIjSjeGs5td23c',checkpoint:'tblfWyTqeyvIvhAjb'};
const allowed=new Map([[ENGINE,new Set(Object.values(TABLES))],[SCHEDULES,new Set(['shows','rings','schedules','show-collection'])],[LIVE,new Set(['collections'])]]);
function api({token,deadline,request=fetch,lane='schedules_live'}) {
  if(!['schedules_live','data_live'].includes(lane))throw Error('Unsupported API lane');
  const destination=lane==='data_live'?TIMING:ENGINE,tables=lane==='data_live'?DATA_TABLES:TABLES;
  const targets=lane==='data_live'?new Map([[TIMING,new Set(Object.values(DATA_TABLES))],[SHOWS,new Set(['shows'])],[LIVE,new Set(['collections'])]]):allowed;
  let clockVerified=false;
  async function call(base,table,method='GET',body,query='') {
    const writable=base===destination&&(method==='PATCH'||(method==='DELETE'&&table===tables.answers));
    if (!targets.get(base)?.has(table) || (method!=='GET'&&!writable)) throw Error('Forbidden Airtable target/method');
    const remaining=deadline-Date.now();if(remaining<1500)throw Error('Deadline exhausted before request');
    await new Promise(r=>setTimeout(r,210));
    const before=Date.now(),response=await request(`https://api.airtable.com/v0/${base}/${encodeURIComponent(table)}${query}`,{method,
      headers:{Authorization:'Bearer '+token,'Content-Type':'application/json'},...(body?{body:JSON.stringify(body)}:{}),signal:AbortSignal.timeout(Math.min(8000,remaining-400))});
    if(!response.ok)throw Error(`Airtable ${method} ${table} HTTP ${response.status}`);
    const server=Date.parse(response.headers.get('date'));
    if(!Number.isFinite(server)||Math.abs(server-(before+Date.now())/2)>5000)throw Error('Host clock not verified against Airtable Date');
    clockVerified=true;return response.json();
  }
  async function list(base,table,formula) {
    const rows=[];let offset;
    do {const q=new URLSearchParams({pageSize:'100',...(formula?{filterByFormula:formula}:{}),...(offset?{offset}:{})});const page=await call(base,table,'GET',undefined,'?'+q);rows.push(...page.records);offset=page.offset;}while(offset);
    return rows;
  }
  async function upsert(table,key,fields) {
    if(fields.length>10)throw Error('Write batch exceeds ten');
    return call(destination,table,'PATCH',{performUpsert:{fieldsToMergeOn:[key]},records:fields.map(fields=>({fields}))});
  }
  async function removeSnapshots(ids){
    if(!ids.length||ids.length>10||new Set(ids).size!==ids.length||ids.some(id=>!/^rec[A-Za-z0-9]{14}$/.test(id)))throw Error('Invalid snapshot deletion batch');
    const q=new URLSearchParams();for(const id of ids)q.append('records[]',id);
    const result=await call(destination,tables.answers,'DELETE',undefined,'?'+q);
    if(result.records?.length!==ids.length||new Set(result.records.map(r=>r.id)).size!==ids.length||result.records.some(r=>r.deleted!==true||!ids.includes(r.id)))throw Error('Snapshot deletion response mismatch');
    const remaining=await list(destination,tables.answers,'OR('+ids.map(id=>`RECORD_ID()='${id}'`).join(',')+')');
    if(remaining.some(r=>ids.includes(r.id)))throw Error('Snapshot deletion readback failed');
    return ids;
  }
  return{list,upsert,removeSnapshots,get clockVerified(){return clockVerified;}};
}
module.exports={api,ENGINE,SCHEDULES,LIVE,TABLES,TIMING,SHOWS,DATA_TABLES};
