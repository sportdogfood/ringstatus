"use strict";

// Canonical show authority only. Never use this allowance for count/live evidence.
const formatter=new Intl.DateTimeFormat('en-US',{timeZone:'America/New_York',
  year:'numeric',month:'2-digit',day:'2-digit',hour:'2-digit',minute:'2-digit',second:'2-digit',hourCycle:'h23'});
const parts=at=>Object.fromEntries(formatter.formatToParts(new Date(at)).map(p=>[p.type,p.value]));
const date=p=>`${p.year}-${p.month}-${p.day}`;
const allowance=30*60000;

function authorityExpires(at){
  const checked=Date.parse(at);
  if(!Number.isFinite(checked))return NaN;
  const p=parts(checked);
  let expires=checked+allowance;
  // A check within the last 30 minutes of the daily window remains valid
  // through the first 30 minutes of the next window. Calendar math uses NY,
  // including the 23/25-hour DST transition days; source time never changes.
  if(Number(p.hour)*60+Number(p.minute)>=19*60+30){
    const next=new Date(Date.parse(date(p)+'T12:00:00Z')+86400000).toISOString().slice(0,10);
    const sample=Date.parse(next+'T22:00:00Z'),q=parts(sample);
    const offset=Date.parse(`${date(q)}T${q.hour}:${q.minute}:${q.second}Z`)-sample;
    expires=Math.max(expires,Date.parse(next+'T16:30:00Z')-offset);
  }
  return expires;
}

function authorityAgeReason(at,now,{futureMs=0,inclusive=false}={}){
  const checked=Date.parse(at),current=Date.parse(now),expires=authorityExpires(at);
  if(!Number.isFinite(checked)||!Number.isFinite(current)||!Number.isFinite(expires))return 'observation_time_missing';
  if(checked-current>futureMs)return 'future_observation';
  return (inclusive?current>expires:current>=expires)?'stale_observation':null;
}
module.exports={authorityAgeReason,authorityExpires};
