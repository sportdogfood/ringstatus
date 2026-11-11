export const SHOWS_BASE='appCaSmL819KK7UFJ';
export const HEALTH_TABLES={shows:'tblJA5CUon5s1sWib',days:'tbl14MaCCDw6dAywH',health:'tblcYgKFwKtILSdU8',tokens:'tbl8pJcHIaRchfDTP',warnings:'tblki6YL1Fe3nnfT0'};
export function healthStore(env,request=fetch){
 async function call(table,method='GET',body,query=''){
  if(!env.WEF_SHOWS_AIRTABLE_TOKEN)throw Error('Health logging credential unavailable');
  const r=await request(`https://api.airtable.com/v0/${SHOWS_BASE}/${table}${query}`,{method,headers:{Authorization:'Bearer '+env.WEF_SHOWS_AIRTABLE_TOKEN,'Content-Type':'application/json'},...(body?{body:JSON.stringify(body)}:{}),signal:AbortSignal.timeout(30000)});
  if(!r.ok)throw Error('Health log HTTP '+r.status);
  return r.json();
 }
 async function list(table,filter){let rows=[],offset;do{const q=new URLSearchParams({pageSize:'100',...(filter?{filterByFormula:filter}:{}),...(offset?{offset}:{})});const p=await call(table,'GET',undefined,'?'+q);rows.push(...p.records);offset=p.offset;}while(offset);return rows;}
 async function save(table,key,fields){
  const value=String(fields[key]).replace(/\\/g,'\\\\').replace(/'/g,"\\'");
  const filter=`{${key}}='${value}'`,rows=await list(table,filter);
  if(rows.length>1)throw Error('Duplicate health log identity');
  const result=await call(table,rows.length?'PATCH':'POST',{records:[{...(rows.length?{id:rows[0].id}:{}),fields}]});
  const check=await list(table,filter);
  if(check.length!==1||check[0].id!==result.records?.[0]?.id)throw Error('Health log readback failed');
  for(const [k,v]of Object.entries(fields)){
   const actual=check[0].fields[k];
   if(v===null||Array.isArray(v)&&v.length===0){if(actual!=null&&!(Array.isArray(actual)&&actual.length===0))throw Error('Health log readback mismatch');}
   else if(typeof v==='string'&&/^\d{4}-\d{2}-\d{2}T/.test(v)){if(Date.parse(actual)!==Date.parse(v))throw Error('Health log readback mismatch');}
   else if(JSON.stringify(actual)!==JSON.stringify(v))throw Error('Health log readback mismatch');
  }
  return check[0];
 }
 return {list,save};
}
export async function recordRenewal(env,fields,request=fetch){
 // Allowlisted metadata only. Logging failure never replaces a usable source response.
 try{const safe={};for(const key of ['refresh-id','started-at','completed-at','lane','endpoint','reason','result','http-status','token-expires-at','error'])if(fields[key]!==undefined)safe[key]=fields[key];await healthStore(env,request).save(HEALTH_TABLES.tokens,'refresh-id',safe);return true;}
 catch{console.error(JSON.stringify({message:'Token renewal log save failed',refresh_id:fields['refresh-id']}));return false;}
}
