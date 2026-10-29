"use strict";
const crypto=require('node:crypto');
const hash=text=>crypto.createHash('sha256').update(text).digest('hex');
const canonical=value=>JSON.stringify(value&&typeof value==='object'?Array.isArray(value)?value.map(v=>JSON.parse(canonical(v))):Object.fromEntries(Object.keys(value).sort().map(k=>[k,JSON.parse(canonical(value[k]))])):value);
const instant=value=>typeof value==='string'&&Number.isFinite(Date.parse(value));
function dates(show){
 const f=show.fields,days=String(f.available_dates||'').split(',').map(x=>x.trim()).filter(Boolean);
 if(!show.id||f['show-key']!==`${f.customer_id}|${f.show_id}`||!days.length||new Set(days).size!==days.length)throw Error('Invalid canonical day authority');
 for(const day of days)if(!/^\d{4}-\d{2}-\d{2}$/.test(day)||new Date(day+'T00:00:00Z').toISOString().slice(0,10)!==day||day<f.start_date||day>f.end_date)throw Error('Invalid canonical show date');
 return days.sort();
}
function projection(body){return{'unique-entry-count':body['unique-entry-count']??null,'trip-count':body['proven-trip-count']??null,'count-coverage':body.coverage,'count-rules-version':body['policy-version'],'count-observed-at':body['accepted-as-of']||null,'count-verified-at':body['verified-at']||null,'data-evidence-ref':canonical(body.evidence)};}
function validateBody(body,show,day){
 if(body.format!=='wef-day-count-summary-v1'||body['policy-version']!=='dated-registrations-v1'||body['show-key']!==show.fields['show-key']||String(body['customer-id'])!==String(show.fields.customer_id)||body.date!==day||body['day-key']!==`${show.fields['show-key']}|${day}`)throw Error('Count summary scope/policy mismatch');
 if(!['competition-class-participation','all-dated-activity'].includes(body.coverage)||!['complete','partial','unknown'].includes(body.completeness))throw Error('Count coverage invalid');
 for(const key of ['unique-entry-count','proven-trip-count'])if(body[key]!==null&&(!Number.isSafeInteger(body[key])||body[key]<0))throw Error('Count identity/number invalid');
 if((body['accepted-as-of']!==null&&!instant(body['accepted-as-of']))||(body.completeness!=='unknown'&&!instant(body['accepted-as-of']))||!instant(body['verified-at'])||Date.parse(body['accepted-as-of'])>Date.parse(body['verified-at']))throw Error('Count source time invalid');
 if(!body.evidence?.['run-key']||!body.evidence?.['state-sha256'])throw Error('Count retained evidence missing');
 if(body.completeness==='unknown'&&(body['unique-entry-count']!==null||body['proven-trip-count']!==null))throw Error('Unknown coverage cannot publish totals');
 if(body.completeness==='complete'&&(body['unique-entry-count']===null||body['proven-trip-count']===null))throw Error('Complete counts missing');
}
function status(body,now){return body.completeness!=='complete'?'Unknown':Date.parse(now)-Date.parse(body['accepted-as-of'])>=300000?'Stale':'Verified';}
function envelope(body){const text=canonical(body);if(Buffer.byteLength(text)>58000)throw Error('Summary exceeds Airtable cell budget');return canonical({body,sha256:hash(text)});}
function decode(text){const value=JSON.parse(text);if(!value.body||value.sha256!==hash(canonical(value.body)))throw Error('Accepted summary digest mismatch');return value.body;}
function equivalent(a,b){if(a==null&&b==null)return true;if(Array.isArray(a)&&Array.isArray(b))return canonical(a)===canonical(b);if(instant(a)&&instant(b))return Date.parse(a)===Date.parse(b);return a===b;}
async function readAccepted(read,key,show,now){
 const before=await read(),row=before.find(r=>r.fields['show-day-key']===key);if(before.filter(r=>r.fields['show-day-key']===key).length!==1||!row?.fields['accepted-summary'])throw Error('No unique accepted summary');
 const body=decode(row.fields['accepted-summary']);validateBody(body,show,row.fields.date);
 const after=await read(),again=after.find(r=>r.id===row.id);
 if(!again||again.fields['accepted-summary']!==row.fields['accepted-summary']||again.fields['publication-status']!==row.fields['publication-status'])throw Error('Publication fence changed');
 if(!equivalent(again.fields.show,[show.id])||again.fields['show-day-key']!==body['day-key']||again.fields.date!==body.date)throw Error('Publication native link/scope mismatch');
 for(const [name,value]of Object.entries(projection(body)))if(!equivalent(again.fields[name],value))throw Error('Publication projection mismatch '+name);
 const effective=status(body,now);
 if(again.fields['publication-status']!==effective&&!(again.fields['publication-status']==='Verified'&&effective==='Stale'))throw Error('Publication status mismatch');
 return{body,status:effective,record:again.id};
}
async function publishDays({api,show,clock=()=>new Date().toISOString(),readSummaries}){
 const report={ok:true,rows:0,days:[],errors:[]},days=dates(show),read=()=>api.list('show-days');
 const existing=await read(),keys=existing.map(r=>r.fields['show-day-key']);if(new Set(keys).size!==keys.length)throw Error('Duplicate canonical day keys');
 let summaries;
 try{summaries=await readSummaries(show);}catch(error){report.ok=false;report.errors.push(error.message);summaries={error:error.message,days:[]};}
 for(const day of days){
  const key=`${show.fields['show-key']}|${day}`,old=existing.find(r=>r.fields['show-day-key']===key),base={'show-day-key':key,show:[show.id],date:day},now=clock();
  try{
   const matches=summaries.days.filter(b=>b.date===day);if(matches.length>1)throw Error('Duplicate count summaries');
   const body=matches[0];
   if(!body){await api.upsert('show-days','show-day-key',[{...base,'publication-status':'Unknown','latest-attempt-at':now,'latest-attempt-status':summaries.error?'Failed':'Unknown','latest-attempt-reason':summaries.error||summaries.reason||'No accepted daily summary; counts unknown'}]);report.days.push({date:day,status:'Unknown',retained:!!old?.fields['accepted-summary']});continue;}
   validateBody(body,show,day);if(Date.parse(body['verified-at'])>Date.parse(now)+1000)throw Error('Summary time is in the future');
   const accepted=envelope(body),projections=projection(body);
   await api.upsert('show-days','show-day-key',[{...base,'publication-status':'Unknown','latest-attempt-at':now,'latest-attempt-status':'Publishing','latest-attempt-reason':''}]);
   await api.upsert('show-days','show-day-key',[{...base,...projections}]);
   const staged=(await read()).find(r=>r.fields['show-day-key']===key);if(!staged)throw Error('Staged day missing');
   for(const [name,value]of Object.entries({...base,...projections}))if(!equivalent(staged.fields[name],value))throw Error('Staged readback mismatch '+name);
   await api.upsert('show-days','show-day-key',[{'show-day-key':key,'accepted-summary':accepted,'publication-status':status(body,now),'latest-attempt-status':body.completeness==='complete'?'Verified':body.completeness,'latest-attempt-reason':(body.reasons||[]).join('; ')}]);
   const checked=await readAccepted(read,key,show,clock());report.rows++;report.days.push({date:day,status:checked.status,record:checked.record,revision:body.evidence['run-key'],digest:hash(canonical(body))});
  }catch(error){report.ok=false;report.errors.push(`${day}: ${error.message}`);report.days.push({date:day,status:'Failed'});break;}
 }
 return report;
}
module.exports={dates,canonical,hash,projection,validateBody,status,envelope,decode,readAccepted,publishDays};
