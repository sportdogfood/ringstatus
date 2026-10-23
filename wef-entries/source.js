"use strict";
const settings=require('./settings');
const {fetchTextWithConfiguredTransport}=require('../wef-tenants/lib/sgl_fetch_adapter');
const model=require('./model');
async function get(path){
 const endpoint='https://wef-schedules-decoded.gombcg.workers.dev'+path;
 return require('../lib/wef-source-read').readSource(endpoint,{decoded:true,request:url=>fetchTextWithConfiguredTransport(url,async target=>{const response=await fetch(target,{headers:{Accept:'application/json'},signal:AbortSignal.timeout(30000)});return{response,text:await response.text()};},{env:settings})});
}
async function fetchPeople(pid,show){const p=model.positive(pid,'trainer ID'),sid=model.positive(show.show_id,'show ID'),cid=model.positive(show.customer_id,'customer ID');return get(`/people/${p}?pid=${p}&show_id=${sid}&customer_id=${cid}`);}
async function fetchClass(classId,show){const id=model.positive(classId,'class ID'),sid=model.positive(show.show_id,'show ID'),cid=model.positive(show.customer_id,'customer ID');return get(`/classes/${id}?show_id=${sid}&customer_id=${cid}&cgid=-1`);}
function classEntries(payload,classId,showId){for(const id of [payload.show_id,payload.class?.show_id].filter(v=>v!=null))if(Number(id)!==Number(showId))throw Error('Follow source show mismatch');for(const id of [payload.class_id,payload.class?.class_id].filter(v=>v!=null))if(Number(id)!==Number(classId))throw Error('Follow source class mismatch');if(!Array.isArray(payload.entries))throw Error('Follow source missing explicit entries array');return payload.entries.map(t=>({...t,class_id:t.class_id??classId}));}
module.exports={fetchPeople,fetchClass,classEntries};
