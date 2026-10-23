"use strict";
const settings=require('./settings'),{fetchTextWithConfiguredTransport}=require('./lib/sgl_fetch_adapter');
async function fetchPeople(trainerId,showId,customerId){
 const {positive}=require('./model');const pid=positive(trainerId,'trainer ID'),sid=positive(showId,'show ID'),customer=positive(customerId,'customer ID');
 const endpoint=`https://ringstatus-proxy.gombcg.workers.dev/api/people/${pid}?pid=${pid}&show_id=${sid}&customer_id=${customer}&decode=1`;
 const result=await fetchTextWithConfiguredTransport(endpoint,async url=>{const response=await fetch(url,{headers:{Accept:'application/json'},signal:AbortSignal.timeout(30000)});return{response,text:await response.text()};},{env:settings});
 if(!result.response.ok)throw Error('People source HTTP '+result.response.status);
 if(result.response.headers?.get('x-rs-decoded')!=='1')throw Error('People source did not confirm protected-field decoding');
 let payload;try{payload=JSON.parse(result.text);}catch{throw Error('People response is not JSON');}
 require('./model').validatePeople(payload,pid,sid);return payload;
}
module.exports={fetchPeople};
