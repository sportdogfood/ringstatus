"use strict";
const fs=require('node:fs');
const {local}=require('./discovery-policy');
const {atomicReport}=require('../wef-schedules/collect');
const BASE='appCaSmL819KK7UFJ';
const valid=day=>typeof day==='string'&&/^\d{4}-\d{2}-\d{2}$/.test(day)&&Number.isFinite(Date.parse(day))&&new Date(day+'T00:00:00Z').toISOString().slice(0,10)===day;
async function updatePriority({api,show,now,discovered=false,statePath}){
 const f=show.fields,p=local(now),anchor=f.selected_date;
 if(!valid(anchor)||anchor<f.start_date||anchor>f.end_date||f['show-key']!==`15|${f.show_id}`||f.is_default!==true)throw Error('Invalid priority planning authority');
 if(fs.existsSync(statePath+'.stopped.json'))throw Error('Priority phase stopped after prior failure; owner review required');
 const state=fs.existsSync(statePath)?JSON.parse(fs.readFileSync(statePath,'utf8')):null;
 if(!discovered&&(p.hour<19||(state?.date===p.date&&state?.anchor===anchor&&state?.show===show.id&&state?.evening===true)))return{status:'Skipped',anchor};
 try{
  const rows=await api.list('show-days'),seen=new Set(),wanted=[],outside=[];
  for(const row of rows){const d=row.fields;if(!(d.show||[]).includes(show.id))continue;
   if(d.show.length!==1||!row.id||!valid(d.date)||d.date<f.start_date||d.date>f.end_date||d['show-day-key']!==`${f['show-key']}|${d.date}`||seen.has(d.date))throw Error('Invalid or duplicate existing priority day');seen.add(d.date);
   const value=(Date.parse(d.date+'T00:00:00Z')-Date.parse(anchor+'T00:00:00Z'))/86400000;
   if(value< -3||value>3){outside.push(d.date);continue;}wanted.push({row,value});
  }
  // Recheck canonical selection before priority-only writes; never run discovery here.
  const defaults=(await api.list('shows')).filter(r=>r.fields.is_default&&Number(r.fields.customer_id)===15);
  if(defaults.length!==1||defaults[0].id!==show.id||defaults[0].fields.selected_date!==anchor)throw Error('Priority authority changed');
  const changes=wanted.filter(x=>x.row.fields['day-priority']!==x.value);
  for(let i=0;i<changes.length;i+=10)await api.call(BASE,'show-days','PATCH',{records:changes.slice(i,i+10).map(x=>({id:x.row.id,fields:{'day-priority':x.value}}))});
  const after=await api.list('show-days');
  for(const {row,value}of wanted){const match=after.filter(r=>r.id===row.id);if(match.length!==1||match[0].fields['day-priority']!==value||match[0].fields.date!==row.fields.date||match[0].fields['show-day-key']!==row.fields['show-day-key']||JSON.stringify(match[0].fields.show)!==JSON.stringify(row.fields.show))throw Error('Priority readback mismatch');}
  await atomicReport(statePath,{version:1,date:p.date,anchor,show:show.id,evening:p.hour>=19,verified_at:now});
  return{status:'Verified',anchor,updated:changes.length,verified:wanted.length,outside_untouched:outside};
 }catch(e){if(!fs.existsSync(statePath+'.stopped.json'))await atomicReport(statePath+'.stopped.json',{at:now,error:e.message});throw e;}
}
module.exports={updatePriority};
