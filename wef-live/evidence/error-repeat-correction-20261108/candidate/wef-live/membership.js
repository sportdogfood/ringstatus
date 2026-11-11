'use strict';
// Pure recurrence rule. No provider calls, schema changes or writes.
function nextStatus(old,present){return present?'Active':old==='Absent'||old==='Dropped'?'Dropped':'Absent';}
function reconcile(previous,current,{complete,scope}){
 const inside=r=>Number(r['show-id'])===Number(scope.show)&&r.date===scope.date;
 const prior=new Map(previous.filter(inside).map(r=>[r.key,r]));
 const seen=new Set(),out=[];
 for(const row of current){
  if(!inside(row))throw Error('Membership scope mismatch');
  if(!row.key||seen.has(row.key))throw Error('Missing/duplicate membership key');
  seen.add(row.key);out.push({...prior.get(row.key),...row,'record-status':'Active'});
 }
 if(complete)for(const [key,row] of prior)if(!seen.has(key))out.push({...row,'record-status':nextStatus(row['record-status'],false)});
 return out;
}
function appearances(payload,scope){
 if(!Array.isArray(payload))throw Error('Unreadable group payload');
 if(!payload.length)return{groups:[],classes:[],complete:true};
 const selected=payload.filter(s=>s&&Number(s.show_id)===Number(scope.show));
 if(selected.length!==1||!Array.isArray(selected[0].json_data))throw Error('Missing/ambiguous group inventory');
 const groups=[],classes=[],seenGroups=new Set(),seenClasses=new Set();let complete=payload.every(s=>s&&typeof s==='object');
 for(const raw of selected[0].json_data){
  if(!raw||typeof raw.day!=='string'||!/^\d{4}-\d{2}-\d{2}$/.test(raw.day)){complete=false;continue;}
  if(raw.day!==scope.date)continue;
  const gid=Number(raw.class_group_id);
  if(!Number.isSafeInteger(gid)||gid<=0||seenGroups.has(gid)){complete=false;continue;}seenGroups.add(gid);
  groups.push({key:`${scope.show}|${scope.date}|${gid}`,'show-id':scope.show,date:scope.date,'group-id':gid});
  if(!Array.isArray(raw.classes)){complete=false;continue;}
  for(const value of raw.classes){
   const cid=Number(value),key=`${scope.show}|${scope.date}|${gid}|${cid}`;
   if(!Number.isSafeInteger(cid)||cid<=0||seenClasses.has(key)){complete=false;continue;}seenClasses.add(key);
   classes.push({key,'show-id':scope.show,date:scope.date,'group-id':gid,'class-id':cid});
  }
 }
 return{groups,classes,complete};
}
module.exports={nextStatus,reconcile,appearances};
