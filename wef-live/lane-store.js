'use strict';
const {isDeepStrictEqual:equal}=require('node:util');
const OMIT=new Set(['last-seen','last-seen-at','last-success','last-changed','source-updated-at','curr_updated_at','run-id','collection','scope-status','participation-checked-at','participation-verified-at']);
const LINKS=new Set(['group','ring','groups','classes','class','schedules','current-group','next-group','completed-groups']);
function business(value){
 if(Array.isArray(value))return value.map(business);
 if(value&&typeof value==='object')return Object.fromEntries(Object.entries(value).filter(([k])=>!OMIT.has(k)).map(([k,v])=>[k,business(v)]));
 return value;
}
function value(fields,key){
 if(key==='source-json'){try{return business(JSON.parse(fields[key]||'{}'));}catch{return fields[key];}}
 return fields[key];
}
function diffs(before,after){
 const out=[];
 function visit(a,b,field){
  if(equal(a,b))return;
  if(a&&b&&!Array.isArray(a)&&!Array.isArray(b)&&typeof a==='object'&&typeof b==='object'){
   for(const k of new Set([...Object.keys(a),...Object.keys(b)]))visit(a[k],b[k],field+'.'+k);
  }else out.push({field,before:a===undefined?null:a,after:b===undefined?null:b});
 }
 for(const k of Object.keys(after))if(!OMIT.has(k)&&!sameStored(before[k],after[k],k))visit(value(before,k),value(after,k),k);
 return out;
}
function sameStored(a,b,key){
 // Airtable omits empty cells, false checkboxes and empty linked-record arrays.
 if((a===undefined||a===null||a==='')&&(b===null||b===''||b===false||(Array.isArray(b)&&!b.length)))return true;
 return LINKS.has(key)&&Array.isArray(a)&&Array.isArray(b)?equal([...a].sort(),[...b].sort()):equal(a,b);
}
function store(api,base,onIssue=()=>{}){
 const cache=new Map();
 async function list(table){if(!cache.has(table))cache.set(table,await api.list(table));return cache.get(table);}
 async function save(table,key,rows){
  const current=await list(table),changes=[],covered=[],written=[];
  const index=new Map();for(const r of current){const k=r.fields[key];if(!index.has(k))index.set(k,[]);index.get(k).push(r);}
  const seen=new Set(),pending=[];
  for(const proposed of rows){
   const k=proposed[key];if(!k||seen.has(k)){onIssue('Missing/duplicate '+table+' '+key+': '+k);continue;}seen.add(k);
   const found=index.get(k)||[];if(found.length>1){onIssue('Ambiguous saved '+table+' '+k);continue;}
   const old=found[0],delta=old?diffs(old.fields,proposed):diffs({},proposed);
   if(!delta.length){covered.push(k);continue;}
   const fields=old?Object.fromEntries(Object.entries(proposed).filter(([f,v])=>!sameStored(old.fields[f],v,f)&&(!OMIT.has(f)||f==='last-seen'||f==='source-updated-at'))):proposed;
   pending.push({old,key:k,fields,delta});
  }
  for(const mode of ['PATCH','POST']){
   const selected=pending.filter(p=>Boolean(p.old)===(mode==='PATCH'));
   for(let i=0;i<selected.length;i+=10){
    const batch=selected.slice(i,i+10);
    try{
     // Never blindly repeat an ambiguous create/write. Saved receipt retains the issue.
     const reply=await api.call(base,table,mode,{records:batch.map(p=>({...(p.old?{id:p.old.id}:{}),fields:p.fields}))});
     const ids=(reply.records||[]).map(r=>r.id);
     if(ids.length!==batch.length)throw Error('Incomplete write response');
     const filter='OR('+ids.map(id=>'RECORD_ID()='+JSON.stringify(id)).join(',')+')';
     const q=new URLSearchParams({filterByFormula:filter,pageSize:'100'});
     const readback=(await api.call(base,table,'GET',undefined,'?'+q)).records||[];
     for(const p of batch){
      const r=readback.find(r=>r.fields[key]===p.key);
      if(!r||Object.entries(p.fields).some(([k,v])=>!sameStored(r.fields[k],v,k))){onIssue('Unverified save '+table+' '+p.key);continue;}
      const at=current.findIndex(x=>x.id===r.id);if(at>=0)current[at]=r;else current.push(r);
      written.push(r);covered.push(p.key);changes.push({table,key:p.key,created:!p.old,fields:p.delta});
     }
    }catch(e){for(const p of batch)onIssue('Save '+table+' '+p.key+': '+e.message);}
   }
  }
  return{rows:current,covered,written,changes,attempted:pending.length};
 }
 return{list,save};
}
module.exports={business,diffs,store};
