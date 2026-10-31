"use strict";
const timezone='America/New_York';
const fmt=new Intl.DateTimeFormat('en-US',{timeZone:timezone,year:'numeric',month:'2-digit',day:'2-digit',hour:'2-digit',hourCycle:'h23'});
const dayAfter=day=>new Date(Date.parse(day+'T12:00:00Z')+86400000).toISOString().slice(0,10);
function local(now){const d=new Date(now);if(!Number.isFinite(d.getTime()))throw Error('Invalid discovery clock');const p=Object.fromEntries(fmt.formatToParts(d).map(x=>[x.type,x.value]));return{date:`${p.year}-${p.month}-${p.day}`,hour:Number(p.hour)};}
function atHour(date,hour){const probe=Date.parse(date+'T12:00:00Z'),p=local(probe);return Date.parse(date+'T'+String(hour).padStart(2,'0')+':00:00Z')-(p.hour-12)*3600000;}
function sunday(now){const p=local(now),d=new Date(p.date+'T12:00:00Z');d.setUTCDate(d.getUTCDate()-d.getUTCDay());let date=d.toISOString().slice(0,10);if(atHour(date,22)>Date.parse(now)){d.setUTCDate(d.getUTCDate()-7);date=d.toISOString().slice(0,10);}return new Date(atHour(date,22)).toISOString();}
function nextSunday(now){const current=sunday(now),d=new Date(local(current).date+'T12:00:00Z');d.setUTCDate(d.getUTCDate()+7);return new Date(atHour(d.toISOString().slice(0,10),22)).toISOString();}
function discoveryPlan(now,state,showId){
 const cycle=sunday(now);
 if(!state){const start=local(now).date===local(cycle).date;state={version:1,cycle,baseline:showId,complete:!start,next:start?cycle:nextSunday(now),failed:false};}
 if(state.version!==1||typeof state.complete!=='boolean'||typeof state.failed!=='boolean'||!Number.isFinite(Date.parse(state.next))||!Number.isFinite(Date.parse(state.cycle)))throw Error('Invalid discovery checkpoint');
 // Keep an unfinished cycle across the week boundary; only a changed show stops it.
 if(state.complete&&cycle!==state.cycle)state={version:1,cycle,baseline:showId,complete:false,next:cycle,failed:false};
 return{state,due:!state.complete&&Date.parse(now)>=Date.parse(state.next)};
}
function calendarSelection(fields,now){const p=local(now),tomorrow=dayAfter(p.date);return p.hour>=19&&tomorrow>=fields.start_date&&tomorrow<=fields.end_date&&tomorrow>fields.selected_date?tomorrow:fields.selected_date;}
function canonicalShow(rows){const matches=rows.filter(r=>r.fields.is_default===true&&Number(r.fields.customer_id)===15);if(matches.length!==1)throw Error('Exactly one retained canonical show required');const row=matches[0],f=row.fields;if(!Number.isSafeInteger(f.show_id)||f.show_id<=0||f['show-key']!==`15|${f.show_id}`)throw Error('Invalid retained canonical show identity');require('./show-days').dates(row);const day=require('../wef-schedules/collect').isoDate(f.selected_date);if(day<f.start_date||day>f.end_date||!Number.isFinite(Date.parse(f.last_checked)))throw Error('Invalid retained canonical show date/evidence time');return row;}
function selectionMatches(f,payload){
 const source=require('../wef-schedules/collect').isoDate(payload.show_date??payload.showDate);
 if(f.source_selected_date&&f.source_selected_date!==source)return false;
 if(!f.selection_basis||f.selection_basis==='source')return f.selected_date===source;
 if(f.selection_basis!=='calendar' || !Number.isFinite(Date.parse(f.selection_at)))return false;
 const p=local(f.selection_at),next=dayAfter(p.date);
 return p.hour>=19&&f.selected_date===next&&next>=f.start_date&&next<=f.end_date;
}
module.exports={timezone,local,sunday,nextSunday,discoveryPlan,calendarSelection,canonicalShow,selectionMatches};
