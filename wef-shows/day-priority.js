"use strict";
const fs=require('node:fs');
const {local}=require('./discovery-policy');
const {atomicReport}=require('../wef-schedules/collect');
const BASE='appCaSmL819KK7UFJ';
const valid=day=>typeof day==='string'&&/^\d{4}-\d{2}-\d{2}$/.test(day)&&Number.isFinite(Date.parse(day))&&new Date(day+'T00:00:00Z').toISOString().slice(0,10)===day;
async function updatePriority({api,show,now,discovered=false,statePath}){
 const f=show.fields,p=local(now),anchor=f.selected_date;
 if(!valid(anchor)||anchor<f.start_date||anchor>f.end_date||f['show-key']!=='15|'+f.show_id||f.is_default!==true)throw Error('Invalid priority planning authority');
 if(fs.existsSync(statePath+'.stopped.json'))throw Error('Priority phase stopped after prior failure; owner review required');
 const state=fs.existsSync(statePath)?JSON.parse(fs.readFileSync(statePath,'utf8')):null;
 // Initialize new fields once on a normal wake; retain the existing due-work cadence thereafter.
 if(state?.version===2&&!discovered&&state.anchor===anchor&&state.show===show.id&&(p.hour<19||(state.date===p.date&&state.evening===true)))return{status:'Skipped',anchor};
 try{
  const shows=await api.list('shows'),parents=new Map(shows.map(r=>[r.id,r])),rows=await api.list('show-days'),seen=new Set(),wanted=[];
  for(const row of rows){const d=row.fields,parent=parents.get(d.show?.[0]),pf=parent?.fields;
   if(!row.id||d.show?.length!==1||!pf||!valid(d.date)||d.date<pf.start_date||d.date>pf.end_date||d['show-day-key']!==pf['show-key']+'|'+d.date||seen.has(d['show-day-key']))throw Error('Invalid or duplicate existing priority day');
   seen.add(d['show-day-key']);
   const lookup=d['is_default (from show)'];
   if(!Array.isArray(lookup)||lookup.length!==1||![true,false,null].includes(lookup[0])||(lookup[0]===true)!==(pf.is_default===true))throw Error('Invalid show-day default lookup');
   const active=lookup[0]===true;
   if(active&&parent.id!==show.id)throw Error('Priority authority changed');
   wanted.push({row,value:active?(Date.parse(d.date+'T00:00:00Z')-Date.parse(anchor+'T00:00:00Z'))/86400000:999,status:active?'Active':'Not active'});
  }
  async function authority(){const defaults=(await api.list('shows')).filter(r=>r.fields.is_default&&Number(r.fields.customer_id)===15);if(defaults.length!==1||defaults[0].id!==show.id||defaults[0].fields.selected_date!==anchor)throw Error('Priority authority changed');}
  async function patch(records){for(let i=0;i<records.length;i+=10)await api.call(BASE,'show-days','PATCH',{records:records.slice(i,i+10)});}
  async function verify(timestamp){const after=await api.list('show-days');for(const {row,value,status}of wanted){const matches=after.filter(r=>r.id===row.id),d=matches[0]?.fields;if(matches.length!==1||d['day-priority']!==value||d.last_checked_status!==status||d.date!==row.fields.date||d['show-day-key']!==row.fields['show-day-key']||JSON.stringify(d.show)!==JSON.stringify(row.fields.show)||JSON.stringify(d['is_default (from show)'])!==JSON.stringify(row.fields['is_default (from show)'])||(timestamp&&Date.parse(d.last_checked)!==Date.parse(now)))throw Error('Priority readback mismatch');}}
  await authority();
  const changes=wanted.filter(x=>x.row.fields['day-priority']!==x.value||x.row.fields.last_checked_status!==x.status);
  await patch(changes.map(x=>({id:x.row.id,fields:{'day-priority':x.value,last_checked_status:x.status}})));
  await verify(false);
  await authority();
  await patch(wanted.map(x=>({id:x.row.id,fields:{last_checked:now}})));
  await verify(true);
  await atomicReport(statePath,{version:2,date:p.date,anchor,show:show.id,evening:p.hour>=19,verified_at:now});
  return{status:'Verified',anchor,updated:changes.length,verified:wanted.length,active:wanted.filter(x=>x.status==='Active').length,inactive:wanted.filter(x=>x.status==='Not active').length};
 }catch(e){if(!fs.existsSync(statePath+'.stopped.json'))await atomicReport(statePath+'.stopped.json',{at:now,error:e.message});throw e;}
}
module.exports={updatePriority};
