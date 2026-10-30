"use strict";
const fs=require('node:fs'),path=require('node:path'),{spawn}=require('node:child_process');
const {hash}=require('./show-days');
const BASE='appdl16HJ5ETm8N69';
const quote=value=>JSON.stringify(String(value));
function bridge(input,{root=path.resolve(__dirname,'..'),script=path.join(__dirname,'day-replay.py'),python=path.join(root,'wef-data/.venv/Scripts/python.exe')}={}){
 return new Promise((resolve,reject)=>{
  const env={PYTHONDONTWRITEBYTECODE:'1',PYTHONIOENCODING:'utf-8'};for(const k of ['SystemRoot','WINDIR','PATH','TEMP','TMP'])if(process.env[k])env[k]=process.env[k];
  const child=spawn(python,['-B',script,root],{env,windowsHide:true,stdio:['pipe','pipe','pipe']});let output='',error='',settled=false;
  function fail(e){if(!settled){settled=true;clearTimeout(timer);child.kill();reject(e);}}
  const timer=setTimeout(()=>fail(Error('Count replay timed out')),45000);
  child.on('error',fail);child.stdin.on('error',fail);
  child.stdout.on('data',b=>{output+=b;if(output.length>8000000)fail(Error('Count replay output too large'));});
  child.stderr.on('data',b=>{error+=b;if(error.length>10000)fail(Error('Count replay diagnostic too large'));});
  child.on('close',code=>{if(settled)return;settled=true;clearTimeout(timer);if(code!==0)return reject(Error('Retained count validation failed: '+error.trim()));try{resolve(JSON.parse(output));}catch{reject(Error('Invalid count replay output'));}});
  child.stdin.end(JSON.stringify(input));
 });
}
function reader(api,{runtime=path.join(__dirname,'runtime'),replay=bridge,headKey}={}){
 return async show=>{
  const f=show.fields,context={'show-key':f['show-key'],'customer-id':f.customer_id,'show-id':f.show_id,'start-date':f.start_date,'end-date':f.end_date,'selected-date':f.selected_date};
  const filter=`AND({status}="Verified completed revision",FIND(${quote('"show-key":'+JSON.stringify(f['show-key']))},{state-json}))`;
  const rows=await api.list('runs',BASE,filter),runs={},candidates=[];
  for(const r of rows){const v=r.fields,s=JSON.parse(v['state-json']||'{}');if(v.status!=='Verified completed revision'||s['show-key']!==context['show-key'])continue;if(runs[v['run-key']])throw Error('Duplicate retained run key');runs[v['run-key']]=v;candidates.push({key:v['run-key'],state:s});}
  if(!candidates.length)return{days:[]};
  candidates.sort((a,b)=>b.state.sequence-a.state.sequence);if(candidates.length>1&&candidates[0].state.sequence===candidates[1].state.sequence)throw Error('Ambiguous retained revision');
  const head=headKey||candidates[0].key,collections={},refs=[],seen=new Set();
  const authority=[context['show-key'],context['start-date'],context['end-date'],context['selected-date']];
  const headState=JSON.parse(runs[head]?.['state-json']||'{}');
  if(!headKey&&(JSON.stringify(headState.authority)!==JSON.stringify(authority)||JSON.stringify(headState['authority-before'])!==JSON.stringify(authority)))return{days:[],reason:'Data has not accepted the current canonical show/date context'};
  const cacheName=ref=>ref.sha256+'-'+hash(ref.key+'|'+ref['record-id'])+'.json';
  let key=head,expected;
  while(key){if(seen.has(key))throw Error('Retained revision cycle');seen.add(key);const run=runs[key];if(!run||(expected&&hash(run['state-json'])!==expected))throw Error('Retained predecessor missing or changed');const s=JSON.parse(run['state-json']);if(s.phase!=='Complete'||s.pending||s['show-key']!==context['show-key'])throw Error('Incomplete retained journal');refs.push(...s.observations);key=s.predecessor?.['run-key'];expected=s.predecessor?.['state-sha256'];}
  const cache=path.join(runtime,'day-evidence-cache');fs.mkdirSync(cache,{recursive:true});const missing=[];
  for(const ref of refs){if(!/^[a-f0-9]{64}$/.test(ref.sha256))throw Error('Invalid retained hash');const file=path.join(cache,cacheName(ref));let row;try{row=JSON.parse(fs.readFileSync(file,'utf8'));}catch(e){if(e.code!=='ENOENT')throw Error('Invalid retained cache');}
   if(row){if(row.id!==ref['record-id']||row.fields['collection-key']!==ref.key||hash(row.fields['retained-json'])!==ref.sha256||row.fields.status!=='Verified retained observation')throw Error('Retained cache mismatch');collections[ref.key]={...row.fields,'_record-id':row.id};}else missing.push(ref);
  }
  for(let i=0;i<missing.length;i+=20){const batch=missing.slice(i,i+20),got=await api.list('collections',BASE,`OR(${batch.map(r=>`{collection-key}=${quote(r.key)}`).join(',')})`);
   for(const ref of batch){const matches=got.filter(r=>r.fields['collection-key']===ref.key);if(matches.length!==1)throw Error('Retained collection not unique');const row=matches[0];if(row.id!==ref['record-id']||row.fields['retained-sha256']!==ref.sha256||hash(row.fields['retained-json'])!==ref.sha256||row.fields.status!=='Verified retained observation')throw Error('Retained collection mismatch');collections[ref.key]={...row.fields,'_record-id':row.id};const dest=path.join(cache,cacheName(ref));fs.writeFileSync(dest+'.tmp',JSON.stringify(row));fs.renameSync(dest+'.tmp',dest);}
  }
  const accepted=await replay({head,runs,collections,context});
  // Recheck the exact head after all evidence reads; a completed revision must not mutate.
  const fence=await api.list('runs',BASE,`{run-key}=${quote(head)}`);if(fence.length!==1||fence[0].fields.status!=='Verified completed revision'||fence[0].fields['state-json']!==runs[head]['state-json'])throw Error('Retained head changed during replay');
  return{head,days:accepted.map(({body,sha256,evidence})=>{
   // Membership remains in original retained evidence, not a second large store.
   const {memberships,'input-collections':inputs,...compact}=body;
   return{...compact,evidence:{...evidence,'summary-sha256':sha256,base:BASE}};
  })};
 };
}
module.exports={reader,bridge};
