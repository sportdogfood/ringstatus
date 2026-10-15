import {InputError} from './rs-inputs.js';
export const TENANTS_BASE='app7BXcw801n19QKF';
export const TABLES={sub:'tblldMTDEC697hPwE',target:'tblCX61u6pjSLrP5S',option:'tblFHcfIFjGuoE2hp',event:'tbluecPFIorOytASP'};
const fail=(code,status=503)=>{throw new InputError(code,status)};
export function createTenantSubmissionStore({env,fetchImpl=fetch,minimumIntervalMs=225}){
 if(env.RS_INPUTS_TENANT_SUBMISSIONS_MODE!=='isolated-trial'||env.RS_INPUTS_WRITE_MODE!=='isolated-trial')fail('tenant_submission_mode_required');if(!env.AIRTABLE_TOKEN)fail('storage_credentials_missing');let last=0;
 async function call(table,{method='GET',formula,fields,id}={}){
  if(!Object.values(TABLES).includes(table)||(method!=='GET'&&table!==TABLES.sub))fail('tenant_business_write_forbidden');
  const url=new URL('https://api.airtable.com/v0/'+TENANTS_BASE+'/'+table);if(formula)url.searchParams.set('filterByFormula',formula);const rows=[],offsets=new Set();
  do{const delay=Math.max(0,minimumIntervalMs-(Date.now()-last));if(delay)await new Promise(r=>setTimeout(r,delay));last=Date.now();let body;
   try{const response=await fetchImpl(url,{method,redirect:'manual',headers:{Authorization:'Bearer '+env.AIRTABLE_TOKEN,'Content-Type':'application/json'},signal:AbortSignal.timeout(15000),...(fields?{body:JSON.stringify({records:[{...(id?{id}:{}),fields}]})}:{})});if(!response.ok)throw Error();body=await response.json();if(!Array.isArray(body.records))throw Error();}catch{fail(method==='GET'?'storage_unavailable':'write_outcome_unknown')}
   rows.push(...body.records);if(!body.offset||method!=='GET')break;if(typeof body.offset!=='string'||offsets.has(body.offset))fail('invalid_storage_response');offsets.add(body.offset);url.searchParams.set('offset',body.offset);
  }while(true);return rows;
 }
 const query=(field,value)=>{if(!/^[A-Za-z0-9_-]{1,128}$/.test(value||''))fail('invalid_identity',400);return '{'+field+"} = '"+value+"'"};
 const one=async(table,field,value)=>{const rows=await call(table,{formula:query(field,value)});if(rows.length>1)fail('ambiguous_submission_or_target',409);return rows[0]||null};
 return {
  submission:eventId=>one(TABLES.sub,'Submission ID',eventId),target:entity=>one(TABLES.target,'entity_uid',entity),event:eventId=>one(TABLES.event,'event_uid',eventId),
  async options(targetId){return (await call(TABLES.option)).filter(r=>r.fields?.['Opt-in']?.includes(targetId))},
  async create(e){const rows=await call(TABLES.sub,{method:'POST',fields:{'Submission ID':e.eventId,'Actor ID':e.actorId,'Target table ID':e.targetTableId,'Target record ID':e.targetRecordId,Action:'Update','Processing status':'Pending','Submitted changes':JSON.stringify(e)}});if(rows.length!==1)fail('write_outcome_unknown');return rows[0]},
  async setStatus(id,status,error){if(!/^rec[A-Za-z0-9]+$/.test(id)||!['Applied','Needs review','Failed'].includes(status))fail('invalid_submission_status',400);const rows=await call(TABLES.sub,{method:'PATCH',id,fields:{'Processing status':status,'Error / review note':error}});if(rows.length!==1||rows[0].fields?.['Processing status']!==status)fail('write_outcome_unknown')}
 };
}
