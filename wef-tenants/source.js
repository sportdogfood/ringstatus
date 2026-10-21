"use strict";
const settings=require('./settings'),{fetchTextWithConfiguredTransport}=require('./lib/sgl_fetch_adapter');
async function fetchPeople(trainerId,showId,customerId){
 const {positive}=require('./model');const pid=positive(trainerId,'trainer ID'),sid=positive(showId,'show ID'),customer=positive(customerId,'customer ID');
 const endpoint=`https://sglapi.wellingtoninternational.com/people/${pid}?pid=${pid}&show_id=${sid}&customer_id=${customer}`;
 const result=await fetchTextWithConfiguredTransport(endpoint,async url=>{const response=await fetch(url,{headers:{Accept:'application/json'},signal:AbortSignal.timeout(30000)});return{response,text:await response.text()};},{env:settings});
 if(!result.response.ok)throw Error('People source HTTP '+result.response.status);
 let payload;try{payload=JSON.parse(result.text);}catch{throw Error('People response is not JSON');}
 // Size is not success evidence. Identity, explicit trips and show consistency
 // are required by model.validatePeople before any dataset writes.
 require('./model').validatePeople(payload,pid,sid);return payload;
}
module.exports={fetchPeople};
