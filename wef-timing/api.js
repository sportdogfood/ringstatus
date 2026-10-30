"use strict";
const ENGINE='appam4opFT4NeL4mn',SCHEDULES='appUq5Qz9kARqFija',LIVE='appgzosUcVqA4vZdf';
const TABLES={answers:'tblHnV5iEJ13DOsBF',checkpoint:'tblepakByOH4G4ruV'};
const TIMING='appYOykKIy1kAquhP',SHOWS='appCaSmL819KK7UFJ',DATA_TABLES={answers:'tblhIjSjeGs5td23c',checkpoint:'tblfWyTqeyvIvhAjb'};
const SYNC_GROUPS='tblSmVxVsHlpudxlQ',SYNC_RINGS='tbleertfMY7ISGZCh';
const CLASS_TIMING='appeqxxJyPGS1iqdm',CLASS_ANSWERS='tblmoBFySYW2TkDHf';
const allowed=new Map([[ENGINE,new Set(Object.values(TABLES))],[SCHEDULES,new Set(['shows','rings','schedules','show-collection'])],[LIVE,new Set(['collections','live-classes','live-groups'])]]);
function api({token,deadline,request=fetch,lane='schedules_live'}) {
  if(!['schedules_live','data_live','class_live'].includes(lane))throw Error('Unsupported API lane');
  const destination=lane==='class_live'?CLASS_TIMING:lane==='data_live'?TIMING:ENGINE,tables=lane==='class_live'?{answers:CLASS_ANSWERS}:lane==='data_live'?DATA_TABLES:TABLES;
  const targets=lane==='class_live'?new Map([[CLASS_TIMING,new Set([CLASS_ANSWERS])]]):lane==='data_live'?new Map([[TIMING,new Set([...Object.values(DATA_TABLES),SYNC_GROUPS,SYNC_RINGS])],[SHOWS,new Set(['shows'])],[LIVE,new Set(['collections','live-classes','live-groups'])]]):allowed;
  let clockVerified=false;
  async function call(base,table,method='GET',body,query='') {
    const writable=base===destination&&((method==='PATCH'&&Object.values(tables).includes(table))||(lane!=='class_live'&&method==='DELETE'&&table===tables.answers));
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
module.exports={SYNC_GROUPS,SYNC_RINGS,syncSample,syncToken,api,ENGINE,SCHEDULES,LIVE,TABLES,TIMING,SHOWS,DATA_TABLES,CLASS_TIMING,CLASS_ANSWERS};

// Stable native sync fields, not sync delivery/edit timestamps.
function syncToken(rows){const {hash}=require('./pace');return hash(rows.map(r=>({id:r.id,fields:r.fields})).sort((a,b)=>a.id.localeCompare(b.id)));}
function syncSample(row,rings,{customer,show,day,now}){
 const {hash,ageReason}=require('./pace'),f=row.fields,raw=JSON.parse(f['source-json']);
 const positive=n=>Number.isSafeInteger(n)&&n>0;
 const rn=Number(raw.ring_number),gid=f['class-group-id'];
 if(!/^rec[A-Za-z0-9]{14}$/.test(row.id)||f['customer-id']!==customer||f['show-id']!==show||f.date!==day||f['scope-status']!=='current'
  ||!positive(gid)||!positive(rn)||String(raw.class_group_id)!==String(gid)||raw.day!==day||String(raw.status)!==String(f.status)
  ||f['group-key']!==`${show}|${day}|${gid}`||!Array.isArray(raw.classes)||!raw.classes.length
  ||raw.classes.some(c=>!positive(Number(c)))||new Set(raw.classes.map(Number)).size!==raw.classes.length
  ||['future_observation','observation_time_missing'].includes(ageReason(f['last-seen'],now,600000)))throw Error('Synced group identity/clock invalid');
 const matches=rings.filter(r=>Array.isArray(f.ring)&&f.ring.includes(r.id)&&r.fields['show-id']===show&&r.fields['customer-id']===customer&&r.fields.date===day&&r.fields['scope-status']==='current'&&r.fields['ring-number']===rn&&r.fields['ring-id']===Number(raw.ring_id));
 if(matches.length!==1)throw Error('Synced ring identity invalid');
 const ring=matches[0],keep=['group-key','show-id','customer-id','date','class-group-id','last-seen','scope-status','status','total','gone','source-json','ring'];
 const fields=Object.fromEntries(keep.filter(k=>Object.hasOwn(f,k)).map(k=>[k,f[k]]));fields.ring=[...fields.ring].sort();
 const material={group:{id:row.id,fields},ring:{id:ring.id,fields:Object.fromEntries(['show-id','customer-id','date','ring-number','ring-id','scope-status'].map(k=>[k,ring.fields[k]]))}};
 const revision=hash(material),classes=raw.classes.map(c=>`${show}|${day}|${Number(c)}`).sort();
 const countConflict=Number(raw.total)!==f.total||Number(raw.gone)!==f.gone;
 return {at:f['last-seen'],total:f.total,gone:f.gone,status:f.status,classes,membership:hash({members:classes,ring:rn,gid,status:f.status}),
  source_issue:countConflict?'synced_count_projection_conflict':null,
  ref:{base:TIMING,table:SYNC_GROUPS,record:row.id,source_key:f['group-key'],revision,fingerprint:revision,observed_at:f['last-seen'],material},
  identity:{customer,show,date:day,ring:rn,group:gid}};
}
