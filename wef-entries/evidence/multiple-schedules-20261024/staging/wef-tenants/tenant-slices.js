"use strict";
const settings=require('./settings');
const fields=['active-team','team-status','team-checked-at'];
const inactive=record=>record.fields.disabled===true||record.fields['source-inactive']===true;
function plan({local,selected,horses,riders,now}){
 const keys=new Set(selected.map(r=>r['entry-key'])),seen=new Set();
 return local.map(record=>{
  const f=record.fields,key=f['entry-key'];if(!key||seen.has(key))throw Error('Duplicate or missing slice entry key');seen.add(key);
  let reason='Not in current team collection';
  if(keys.has(key)&&f.status==='active'&&String(f['trainer-ids']||'').trim()){
   const horse=horses.filter(r=>Number(r.fields['horse-id'])===Number(f['horse-id'])),rider=riders.filter(r=>Number(r.fields['rider-id'])===Number(f['rider-id']));
   if(horse.length!==1||rider.length!==1)throw Error('Team slice helper missing or ambiguous: '+key);
   const hidden=[];if(inactive(horse[0]))hidden.push('horse inactive');if(inactive(rider[0]))hidden.push('rider inactive');
   reason=hidden.length?hidden.join('; '):'Active team';
  }
  return{id:record.id,fields:{'active-team':reason==='Active team','team-status':reason,'team-checked-at':now}};
 });
}
async function save({api,selected,horses,riders,now}){
 const local=await api.list(settings.SYNC_ENTRIES_TABLE),wanted=plan({local,selected,horses,riders,now});
 for(let i=0;i<wanted.length;i+=10)await api.call(settings.BASE_ID,settings.SYNC_ENTRIES_TABLE,'PATCH',{records:wanted.slice(i,i+10)});
 const saved=await api.list(settings.SYNC_ENTRIES_TABLE);
 for(const row of wanted){const current=saved.find(r=>r.id===row.id);if(!current||Boolean(current.fields['active-team'])!==row.fields['active-team']||current.fields['team-status']!==row.fields['team-status']||Date.parse(current.fields['team-checked-at'])!==Date.parse(now))throw Error('Team slice readback mismatch');}
 return{active:wanted.filter(r=>r.fields['active-team']).length,hidden_inactive:wanted.filter(r=>r.fields['team-status'].includes('inactive')).length,other_hidden:wanted.filter(r=>r.fields['team-status']==='Not in current team collection').length,checked_at:now,private_follows_exported:false};
}
module.exports={fields,plan,save};
