"use strict";
// Candidate replacement for wef-entries/model.js. Pure linking; no tenant inputs.
const positive=(value,label)=>{const n=Number(value);if(value==null||value===''||!Number.isSafeInteger(n)||n<=0)throw Error('Invalid '+label);return n;};
function day(value){const s=String(value??'').slice(0,10);if(!/^\d{4}-\d{2}-\d{2}$/.test(s)||!Number.isFinite(Date.parse(s+'T00:00:00Z'))||new Date(s+'T00:00:00Z').toISOString().slice(0,10)!==s)throw Error('Invalid source date');return s;}
function scope(shows,now){
 const selected=shows.filter(r=>r.fields.is_default===true);if(selected.length!==1)throw Error('Exactly one canonical default show required');
 const show=selected[0].fields;const sid=positive(show.show_id,'show ID'),customer=positive(show.customer_id,'customer ID');
 if(show['show-key']!==`${customer}|${sid}`)throw Error('Canonical show key mismatch');
 const start=day(show.start_date),end=day(show.end_date),selectedDate=day(show.selected_date);
 if(start>end||selectedDate<start||selectedDate>end||require('../wef-shows/authority-age').authorityAgeReason(show.last_checked,now,{futureMs:60000,inclusive:true}))throw Error('Invalid or stale canonical show');
 const dates=[];for(let d=start;d<=end;d=new Date(Date.parse(d+'T00:00:00Z')+86400000).toISOString().slice(0,10)){dates.push(d);if(dates.length>31)throw Error('Show exceeds supported 31 days');}
 return{show,dates,showRecord:selected[0].id};
}
function schedulesFor(show,local){
 return local.filter(r=>Number(r.fields['show-id'])===Number(show.show_id)&&r.fields.status==='active');
}
function sharedClassKey(showId,date,classId,ring){return [Number(showId),String(date).slice(0,10),Number(classId),Number(ring)].join('|');}
// Shared tenant-independent matcher; no production collector invocation.
const {linkClass}=require('./participation-linker');
module.exports={scope,schedulesFor,sharedClassKey,linkClass};
