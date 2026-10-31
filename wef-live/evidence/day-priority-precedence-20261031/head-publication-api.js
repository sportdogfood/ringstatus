'use strict';
const FIELD='live-observation-json';
const TARGETS=Object.freeze({
 schedules:{base:'appUq5Qz9kARqFija',tables:{rings:'tbliUwvXYdMhvJ3Fm',schedules:'tblqiTFqqO3zk2KJC'}},
 data:{base:'appdl16HJ5ETm8N69',tables:{rings:'tblhKkfYXppW3TbTq',groups:'tblQObOvktufkyq54',classes:'tbl0IFf067w4wUBj5',schedules:'tbl6KKlYuMIPHEJWe',trips:'tblt0GvQ39bHIu930'}},
});
function transport({token=require('./settings').AIRTABLE_TOKEN,request=globalThis.fetch,pause=ms=>new Promise(r=>setTimeout(r,ms))}={}){
 function signal(limit={}){const remaining=(limit.deadline??(Date.now()+15000))-Date.now();if(remaining<=0)throw Error('Publication lane budget exhausted');return AbortSignal.timeout(Math.max(1,Math.min(15000,remaining)));}
 async function call(lane,table,method='GET',id='',value,limit){
  const t=TARGETS[lane];if(!t?.tables[table]||!['GET','PATCH'].includes(method))throw Error('Forbidden publication target/method');
  if(id&&!/^rec[A-Za-z0-9]+$/.test(id))throw Error('Invalid destination record ID');
  if(method==='PATCH'&&(!id||typeof value!=='string'||value.length>80000))throw Error('Invalid observation-only patch');
  await pause(210);
  if(method==='PATCH')limit?.beforeDispatch?.();
  const url=`https://api.airtable.com/v0/${t.base}/${t.tables[table]}`;
  const response=await request(url+(method==='GET'&&id?'/'+id:''),{method,headers:{Authorization:'Bearer '+token,'Content-Type':'application/json'},...(method==='PATCH'?{body:JSON.stringify({records:[{id,fields:{[FIELD]:value}}]})}:{}),signal:signal(limit)});
  if(!response.ok)throw Error(`Publication ${lane}/${table} ${method} HTTP ${response.status}`);
  return response.json();
 }
 async function list(lane,table,scope,limit){
  const t=TARGETS[lane];if(!t?.tables[table])throw Error('Forbidden publication read');
  if(!scope||!Number.isSafeInteger(scope.customer)||!Number.isSafeInteger(scope.show)||!/^\d{4}-\d{2}-\d{2}$/.test(scope.date))throw Error('Invalid publication read scope');
  const key=lane==='schedules'?(table==='rings'?'ring-key':'stage-key'):({'rings':'ring-key','groups':'group-key','classes':'class-key','schedules':'schedule-key','trips':'trip-key'}[table]);
  const prefix=lane==='schedules'&&table==='schedules'?`${scope.show}|${scope.date}|`:`${scope.customer}|${scope.show}|${scope.date}|`;
  const filter=`LEFT({${key}},${prefix.length})="${prefix}"`;
  const rows=[];let offset;
  do{await pause(210);const q=new URLSearchParams({pageSize:'100',filterByFormula:filter,...(offset?{offset}:{})});const r=await request(`https://api.airtable.com/v0/${t.base}/${t.tables[table]}?${q}`,{headers:{Authorization:'Bearer '+token},signal:signal(limit)});if(!r.ok)throw Error(`Publication ${lane}/${table} GET HTTP ${r.status}`);const d=await r.json();rows.push(...d.records);offset=d.offset;if(rows.length>10000)throw Error('Publication inventory exceeds bound');}while(offset);
  return rows;
 }
 return{list,get:(l,t,id,limit)=>call(l,t,'GET',id,undefined,limit),patch:(l,t,id,v,limit)=>call(l,t,'PATCH',id,v,limit)};
}
module.exports={FIELD,TARGETS,transport};
