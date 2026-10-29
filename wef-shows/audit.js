"use strict";
const definitions={
 'default-ring':{name:'Default show discovery',kind:'Endpoint',location:'https://wef-schedules-decoded.gombcg.workers.dev/ring?customer_id=15',purpose:'Identify the source default show.',owner:'wef-shows'},
 'default-schedule':{name:'Default show dates',kind:'Endpoint',location:'https://wef-schedules-decoded.gombcg.workers.dev/schedule?date=00/00/00&show_id={show_id}&customer_id=15',purpose:'Validate show metadata, dates and source-selected date.',owner:'wef-shows'},
 'season-counts':{name:'Retained Season/Data day counts',kind:'Airtable',location:'https://airtable.com/appdl16HJ5ETm8N69',purpose:'Read and validate completed runs and retained collections for canonical day counts.',owner:'wef-season-coordinator'}
};
function createAudit({api,runId,clock}){
 const attempts=new Map(),errors=[],sourceIds=new Map();let phase='run-start',source=null;
 function stage(value,sourceKey=null){phase=value;source=sourceKey;}
 function capture(error){const message=error?.message||String(error);errors.push({at:clock(),phase,source,message});if(['source-read','source-validation','show-validation'].includes(phase)&&attempts.has(source)){const attempt=attempts.get(source);attempt.result='Failed';attempt.error=message;delete attempt.successAt;}}
 async function observe(key,action){
  if(!definitions[key])throw Error('Unregistered Shows source');stage('source-read',key);
  const attempt={key,at:clock(),result:'Running',error:''};attempts.set(key,attempt);
  try{const value=await action();attempt.result='Success';attempt.successAt=clock();return value;}
  catch(error){attempt.result='Failed';attempt.error=error.message;throw error;}
 }
 async function saveSources(){
  const fields=[...attempts.values()].map(a=>({'source-key':a.key,...definitions[a.key],'last-attempt-at':a.at,'last-result':a.result,'last-error':a.error,...(a.result==='Success'?{'last-success-at':a.successAt}:{})}));
  const records=await api.upsert('sources','source-key',fields);for(const r of records)sourceIds.set(r.fields['source-key'],r.id);
  return records.map(r=>r.id);
 }
 async function saveErrors(runRecord){
  const fields=errors.map((e,i)=>({'error-key':`${runId}|${i+1}`,'run-id':runId,at:e.at,stage:e.phase,message:e.message,...(runRecord?{run:[runRecord]}:{}),...(sourceIds.has(e.source)?{source:[sourceIds.get(e.source)]}:{}),'source-key':e.source||''}));
  return (await api.upsert('err-logs','error-key',fields)).map(r=>r.id);
 }
 function snapshot(){return{sources:[...attempts.values()],errors:errors.map(e=>({...e}))};}
 return{stage,capture,observe,saveSources,saveErrors,snapshot};
}
module.exports={definitions,createAudit};
