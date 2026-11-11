'use strict';
// Stage 7: fill empty native links from exact saved keys. No source calls or enrichment.
const fs=require('node:fs'),path=require('node:path'),crypto=require('node:crypto');
const settings=require('./settings');
const TABLES=['live-rings','live-groups','live-classes','live-trips','live-entries'];
const REFS=['w-shows','w-days','wef-data.schedules','w-horses','w-riders','w-trainers'];
const empty=v=>v==null||(Array.isArray(v)&&v.length===0);
const num=v=>v!==null&&v!==undefined&&v!==''&&Number.isSafeInteger(Number(v))&&Number(v)>0?Number(v):null;
const key=(...v)=>v.join('|');
function plan(data,scope,onIssue=()=>{}){
 const rows=t=>(data[t]||[]).filter(r=>Number(r.fields['show-id'])===scope.show&&r.fields.date===scope.date);
 const rings=rows('live-rings'),groups=rows('live-groups'),classes=rows('live-classes'),entries=rows('live-entries');
 const one=(list,field,value)=>{const found=list.filter(r=>r.fields[field]===value);return found.length===1?found[0]:null;};
 const raw=r=>{try{return JSON.parse(r.fields['source-json']||'{}');}catch{return {};}};
 const ringOf=(table,r)=>{
  const f=r.fields;if(table==='live-rings')return r;
  const direct=num(f['ring-id'])||num(raw(r).ring_id);
  if(direct)return one(rings,'ring-key',key(scope.show,scope.date,direct));
  if(f.ring?.length===1)return rings.find(x=>x.id===f.ring[0])||null;
  if(table==='live-entries'){const cls=classOf(table,r);return cls?ringOf('live-classes',cls):null;}
  const candidates=groups.filter(g=>Array.isArray(raw(g).classes)&&raw(g).classes.some(id=>Number(id)===num(f['class-id'])));
  const ids=[...new Set(candidates.map(g=>num(raw(g).ring_id)).filter(Boolean))];
  return ids.length===1?one(rings,'ring-key',key(scope.show,scope.date,ids[0])):null;
 };
 const classOf=(table,r)=>{
  if(table==='live-classes')return r;
  let cid=num(r.fields['class-id']);
  if(table==='live-entries'){
   const parts=String(r.fields['live-entry-key']||'').split('|'),observed=num(raw(r).live?.class_id);
   if(parts.length!==4||Number(parts[0])!==scope.show||parts[1]!==scope.date||Number(parts[3])!==num(r.fields['trip-id']))return null;
   cid=num(parts[2]);if(observed&&observed!==cid)return null;
  }
  return cid?one(classes,'class-key',key(scope.show,scope.date,cid)):null;
 };
 const scheduleRows=(data['wef-data.schedules']||[]).filter(r=>r.fields['record-status']!=='Dropped');
 const scheduleKeys=new Map();for(const r of scheduleRows){const k=r.fields['schedule-key'];scheduleKeys.set(k,(scheduleKeys.get(k)||0)+1);}
 const plans=[];
 for(const table of TABLES)for(const row of rows(table)){
  const f=row.fields,fields={};
  const fill=(field,resolve,identity)=>{
   if(!empty(f[field]))return;
   const matches=resolve();
   if(matches?.length)fields[field]=[...new Set(matches.map(x=>x.id))];
   else onIssue('No unique exact match for '+table+'.'+field+(identity?' ('+identity+')':'')+'. No retry recommended until source data changes.',{table,id:row.id,class_id:num(f['class-id']),trip_id:num(f['trip-id']),group_id:num(f['class-group-id'])});
  };
  fill('w-shows',()=>{const r=one(data[REFS[0]]||[],'show-key',key(scope.customer,scope.show));return r?[r]:[];});
  fill('w-days',()=>{const r=one(data[REFS[1]]||[],'show-day-key',key(scope.customer,scope.show,scope.date));return r?[r]:[];});
  fill('data-schedules',()=>{
   const ring=ringOf(table,row),n=num(ring?.fields['ring-number']);if(!n)return[];
   const prefix=key(scope.customer,scope.show,scope.date,n);
   let field='class',value;
   if(table==='live-rings'){field='ring';value=prefix;}
   else if(table==='live-groups'){field='group';const gid=num(f['class-group-id']);if(!gid)return[];value=key(prefix,gid);}
   else{const cls=classOf(table,row);if(!cls)return[];value=key(prefix,cls.fields['class-id']);}
   return scheduleRows.filter(r=>r.fields[field]===value&&scheduleKeys.get(r.fields['schedule-key'])===1);
  });
  if(table==='live-groups')fill('ring',()=>{const r=ringOf(table,row);return r?[r]:[];});
  if(table==='live-classes'||table==='live-trips'){
   fill('ring',()=>{const r=ringOf(table,row);return r?[r]:[];});
   fill('groups',()=>{const ring=ringOf(table,row);return ring?groups.filter(g=>num(raw(g).ring_id)===num(ring.fields['ring-id'])&&Array.isArray(raw(g).classes)&&raw(g).classes.some(id=>Number(id)===num(f['class-id']))):[];});
  }
  if(table==='live-trips'||table==='live-entries')fill('class',()=>{const r=classOf(table,row);return r?[r]:[];});
  if(table==='live-trips'||table==='live-entries')for(const [field,idField]of [['w-horses','horse-id'],['w-riders','rider-id'],['w-trainers','trainer-id']]){
   const id=num(f[idField]);
   fill(field,()=>{if(!id)return[];const matches=(data[field]||[]).filter(r=>num(r.fields[idField])===id);return matches.length===1?matches:[];},idField+'='+(id||'missing'));
  }
  if(table==='live-trips')fill('legacy-live-entry',()=>{
   const cid=num(f['class-id']),tid=num(f['trip-id']);if(!cid||!tid)return[];
   const r=one(entries,'live-entry-key',key(scope.show,scope.date,cid,tid));
   if(r&&num(f['entry-id'])&&num(r.fields['entry-id'])&&num(f['entry-id'])!==num(r.fields['entry-id']))return[];
   return r?[r]:[];
  });
  if(Object.keys(fields).length)plans.push({table,id:row.id,fields});
 }
 return plans;
}
async function link({scope,api=require('./airtable').airtable(),runtime=settings.RUNTIME,clock=()=>new Date().toISOString()}={}){
 const r={lane:'linker',run_id:crypto.randomUUID(),started:clock(),scope,ok:true,issues:[],stage_errors:[],logging_errors:[],changes:[],counts:{}};
 const issue=(message,ids={})=>r.issues.push({message,...ids});
 try{
  const data={};for(const t of [...TABLES,...REFS])data[t]=await api.list(t);
  const plans=plan(data,scope,issue);
  for(const table of TABLES){const selected=plans.filter(p=>p.table===table);
   for(let i=0;i<selected.length;i+=10){
    const batch=selected.slice(i,i+10),q='?'+new URLSearchParams({filterByFormula:'OR('+batch.map(p=>'RECORD_ID()='+JSON.stringify(p.id)).join(',')+')',pageSize:'100'});
    const current=(await api.call(settings.BASE_ID,table,'GET',undefined,q)).records||[];
    const patches=batch.map(p=>{const row=current.find(x=>x.id===p.id);return{id:p.id,fields:row?Object.fromEntries(Object.entries(p.fields).filter(([f])=>empty(row.fields[f]))):{}};}).filter(p=>Object.keys(p.fields).length);
    if(!patches.length)continue;
    await api.call(settings.BASE_ID,table,'PATCH',{records:patches});
    const saved=(await api.call(settings.BASE_ID,table,'GET',undefined,q)).records||[];
    for(const p of patches){const row=saved.find(x=>x.id===p.id);if(!row||Object.entries(p.fields).some(([f,ids])=>JSON.stringify([...(row.fields[f]||[])].sort())!==JSON.stringify([...ids].sort())))throw Error('Link save not confirmed: '+table+' '+p.id);
     r.changes.push({table,id:p.id,fields:p.fields});r.counts[table]=(r.counts[table]||0)+1;
    }
   }
  }
 }catch(e){r.ok=false;r.stage_errors.push({message:e.message});}
 r.completed=clock();
 const result=!r.ok?'Failed':r.issues.length?'Partial':'Success';
 const summary=JSON.stringify({lane:'linker',counts:r.counts,changed_records:r.changes.length,issues:r.issues.length});
 async function log(table,rows){for(let i=0;i<rows.length;i+=10){try{const batch=rows.slice(i,i+10);const saved=await api.call(settings.BASE_ID,table,'POST',{records:batch.map(fields=>({fields}))});if(saved.records?.length!==batch.length)throw Error('Incomplete linker log response');}catch(e){r.logging_errors.push(table+': '+e.message);}}}
 await log('run-logs',[{'run-id':r.run_id,started:r.started,completed:r.completed,result,'show-id':scope.show,date:scope.date,summary,errors:[...new Set([...r.issues,...r.stage_errors].map(x=>x.message))].join('\n').slice(0,60000)},...r.changes.map((c,i)=>({'run-id':r.run_id+'|diff|'+i,started:r.started,completed:r.completed,result:'Success','show-id':scope.show,date:scope.date,summary:JSON.stringify({lane:'linker',parent_run_id:r.run_id,...c})}))]);
 try{await require('./error-log').report({api,runtime,lane:'linker',rows:[...r.issues,...r.stage_errors].map((e,i)=>({'error-key':r.run_id+'|'+i,'run-id':r.run_id,'occurred-at':r.completed,'show-id':scope.show,date:scope.date,message:'linker: '+e.message,'err-class':e.class_id||null,'err-trip':e.trip_id||null,'err-group':e.group_id||null}))});}catch(e){r.logging_errors.push('errs-log: '+e.message);}
 await log('heartbeat',[{'heartbeat-key':r.run_id,'run-id':r.run_id,started:r.started,completed:r.completed,'checked-at':r.completed,'show-id':scope.show,date:scope.date,result,summary}]);
 try{fs.mkdirSync(runtime,{recursive:true});for(const name of ['linker-'+r.run_id+'.json','linker-last-run.json']){const file=path.join(runtime,name),tmp=file+'.tmp';fs.writeFileSync(tmp,JSON.stringify(r,null,2));fs.renameSync(tmp,file);}}catch(e){r.logging_errors.push('Local linker receipt: '+e.message);}
 return r;
}
module.exports={plan,link,empty,TABLES,REFS};
