'use strict';
const schema=require('./schema.json');
const BASE=schema.baseId,T=Object.fromEntries(schema.tables.filter(t=>t.name!=='meta').map(t=>[t.name,t.id]));
function api({token,deadline,request=fetch}){
 if(!token||!Number.isFinite(deadline))throw Error('Entry transport credentials/deadline missing');
 let clockVerified=false;
 async function call(base,table,method='GET',body,query=''){
  if(base!==BASE||!Object.values(T).includes(table)||!['GET','PATCH'].includes(method))throw Error('Forbidden entry target/method');
  if(deadline-Date.now()<2000)throw Error('Entry request budget exhausted');
  await new Promise(resolve=>setTimeout(resolve,210));
  const remaining=deadline-Date.now();if(remaining<1500)throw Error('Entry request budget exhausted');
  const before=Date.now(),response=await request(`https://api.airtable.com/v0/${BASE}/${table}${query}`,{method,
   headers:{Authorization:'Bearer '+token,'Content-Type':'application/json'},...(body?{body:JSON.stringify(body)}:{}),signal:AbortSignal.timeout(Math.min(8000,remaining-400))});
  if(!response.ok)throw Error(`Entry Airtable ${method} HTTP ${response.status}`);
  const server=Date.parse(response.headers.get('date'));
  if(!Number.isFinite(server)||Math.abs(server-(before+Date.now())/2)>5000)throw Error('Entry clock not verified');
  clockVerified=true;return response.json();
 }
 async function list(base,table,formula){const rows=[];let offset;const seen=new Set();
  do{const q=new URLSearchParams({pageSize:'100',...(formula?{filterByFormula:formula}:{}),...(offset?{offset}:{})});
   const page=await call(base,table,'GET',undefined,'?'+q);if(!Array.isArray(page.records))throw Error('Entry list response malformed');
   rows.push(...page.records);offset=page.offset;if(offset&&seen.has(offset))throw Error('Entry pagination repeated');if(offset)seen.add(offset);
  }while(offset);return rows;
 }
 async function upsert(table,key,fields){if(!Array.isArray(fields)||!fields.length||fields.length>10)throw Error('Entry invalid write batch');
  const t=schema.tables.find(t=>t.id===table);if(!t||t.fields.find(f=>f.id===t.primaryFieldId)?.name!==key)throw Error('Entry upsert key mismatch');
  return call(BASE,table,'PATCH',{performUpsert:{fieldsToMergeOn:[key]},records:fields.map(fields=>({fields}))});
 }
 return{list,upsert,get clockVerified(){return clockVerified;}};
}
module.exports={api,BASE,T};
