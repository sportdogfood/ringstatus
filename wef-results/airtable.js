"use strict";
const S=require('./settings'),{tables}=require('./schema'),{packed}=require('./model');
function airtable({token,request=globalThis.fetch,pause=ms=>new Promise(r=>setTimeout(r,ms))}={}){
  if(!token)throw Error('Missing WEF_RESULTS_AIRTABLE_TOKEN');
  const sources=new Map([[S.DATA,new Set(['classes'])],[S.SHOWS,new Set(['shows'])],[S.LIVE,new Set(['live-classes'])],[S.BASE,new Set(Object.keys(tables))]]);
  const destinationIds=new Map(require('./schema.json').tables.map(t=>[t.name,t.id]));
  const sourceIds=new Map([[S.DATA+'|classes','tbl0IFf067w4wUBj5'],[S.SHOWS+'|shows','tblJA5CUon5s1sWib'],[S.LIVE+'|live-classes','tblNaM9ehOJFsrveV']]);
  const cache=new Map();
  async function call(base,table,method='GET',body,query='',record=''){
    if(!sources.get(base)?.has(table))throw Error('Forbidden Results read target');
    if(method!=='GET'&&(base!==S.BASE||!tables[table]||!['POST','PATCH'].includes(method)))throw Error('Forbidden Results write target');
    if(record&&!/^rec[A-Za-z0-9]+$/.test(record))throw Error('Invalid Airtable record ID');
    await pause(250);
    let response;
    const tableId=base===S.BASE?destinationIds.get(table):sourceIds.get(base+'|'+table);
    if(!tableId)throw Error('Unmapped Results table');
    try{response=await request(`https://api.airtable.com/v0/${base}/${tableId}${record?'/'+record:''}${query}`,{method,headers:{Authorization:'Bearer '+token,'Content-Type':'application/json'},...(body?{body:JSON.stringify(body)}:{}),signal:AbortSignal.timeout(30000)});}catch{throw Error(`Results Airtable ${method} ${table}: request outcome unknown`);}
    if(!response.ok)throw Error(`Results Airtable ${method} ${table}: HTTP ${response.status}`);
    return response.json();
  }
  async function list(base,table){let offset;const records=[],seen=new Set();do{const q=new URLSearchParams({pageSize:'100',...(offset?{offset}:{})});const data=await call(base,table,'GET',undefined,'?'+q);if(!Array.isArray(data.records))throw Error('Malformed Airtable records');records.push(...data.records);offset=data.offset;if(offset&&seen.has(offset))throw Error('Repeated Airtable cursor');seen.add(offset);}while(offset);if(base===S.BASE)cache.set(table,records);return records;}
  const get=(base,table,record)=>call(base,table,'GET',undefined,'',record);
  async function upsert(table,key,rows){
    if(!tables[table]||tables[table][0].name!==key)throw Error('Forbidden Results destination/key');
    const names=new Set(tables[table].map(f=>f.name)),input=new Set();
    for(const fields of rows){if(!fields[key]||input.has(fields[key]))throw Error('Duplicate or missing Results key');input.add(fields[key]);for(const [name,value]of Object.entries(fields)){if(!names.has(name))throw Error('Forbidden Results field '+name);if(typeof value==='string'&&value.length>100000)throw Error('Airtable text field limit exceeded; no truncation');}}
    const current=cache.get(table)||await list(S.BASE,table),index=new Map();
    for(const r of current){const k=r.fields?.[key];if(!k||index.has(k))throw Error('Duplicate or missing persisted Results key');index.set(k,r);}
    const result=[];
    for(const fields of rows){
      const old=index.get(fields[key]);
      const normalize=v=>v===undefined||v===null||v===''||v===false?null:v;
      const equal=(a,b)=>packed(normalize(a))===packed(normalize(b));
      const delta=Object.fromEntries(Object.entries(fields).filter(([k,v])=>!old||!equal(old.fields[k],v)));
      if(!Object.keys(delta).length){result.push(old);continue;}
      let saved;
      try{const data=await call(S.BASE,table,old?'PATCH':'POST',{records:[old?{id:old.id,fields:delta}:{fields}]});saved=data.records?.[0];}
      catch(error){
        // Read-only reconciliation of an unknown outcome, never repeat a write.
        const found=(await list(S.BASE,table)).filter(r=>r.fields?.[key]===fields[key]);
        if(found.length!==1||Object.entries(fields).some(([k,v])=>!equal(found[0].fields[k],v)))throw error;
        saved=found[0];
      }
      if(!saved?.id)throw Error('Missing saved Results record');
      const readback=await get(S.BASE,table,saved.id);
      if(Object.entries(fields).some(([k,v])=>!equal(readback.fields?.[k],v)))throw Error('Results readback mismatch '+table);
      if(old){const i=current.findIndex(r=>r.id===old.id);current[i]=readback;}else current.push(readback);
      index.set(fields[key],readback);result.push(readback);
    }
    cache.set(table,current);return result;
  }
  return {list,get,upsert};
}
module.exports={airtable};
