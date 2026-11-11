'use strict';
// Prepared integration only: this file is NOT the deployed data-runner.js.
async function run({ringApi,runId=require('node:crypto').randomUUID(),clock=()=>new Date().toISOString(),deadline=Date.now()+160000,readRingHistory,runRing,runClass,classApi,readClassHistory}={}){
 const started=clock();
 const warnings=[];
 const transport=ringApi||require('./api').api({token:process.env.WEF_TIMING_AIRTABLE_TOKEN,lane:'ring_state',deadline,onClockIssue:message=>warnings.push(message)});
 const ring=await(runRing||require('./ring-snapshot').run)({api:transport,now:started,runId,...(readRingHistory?{readHistory:readRingHistory}:{})});
 let classes;
 try{classes=await(runClass||require('./class-snapshot').run)({...(classApi?{api:classApi}:{}),now:clock(),runId,deadline,...(readClassHistory?{readHistory:readClassHistory}:{})});}
 catch(e){classes={ok:false,issues:[e.message],outcome:'unverified'};}
 const ok=ring.ok&&classes.ok;
 const errors=[...(!ring.ok?(ring.errors?.length?ring.errors:ring.issues?.length?ring.issues.map(x=>x.message||String(x)):['Ring snapshot stage failed']):[]),...(!classes.ok?classes.issues:[])];
 return {run_id:runId,started,completed:clock(),source_lane:'data_live',destination:'appYOykKIy1kAquhP',status:ok?'complete':'failed',class_status:classes.ok?'complete':'failed',ok,ring,classes,errors,issues:[...warnings,...classes.issues],counts:{evaluated:ring.rings?.length||0,written:ring.written||0,unchanged:0,purged:0,classes_evaluated:classes.evaluated??null,classes_written:classes.written??null,classes_unchanged:0}};
}
module.exports={run};
