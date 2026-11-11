import {scheduleHealth} from './schedule-health.mjs';
import {healthStore} from './health-records.mjs';
import {sourceHandler} from './shared-worker.mjs';
import {fetchFallback,renewFallbackToken} from './fallback.mjs';
export {SglPacer} from './shared-worker.mjs';
export default {async fetch(request,env){
 const saved=[],real=healthStore(env),sourceEnv={...env,WEF_SHOWS_AIRTABLE_TOKEN:undefined};
 const result=await scheduleHealth(new Request(new URL('/health',request.url)),env,{
  store:{list:(...args)=>real.list(...args),save:async(table,key,fields)=>{saved.push({table,fields});return{id:'preview-only'};}},
  original:(ping,report)=>sourceHandler.fetch(ping,{...sourceEnv,SGL_HEALTH_PROBE:true,SGL_HEALTH_REPORT:report}),
  alternate:(ping,profile,report)=>fetchFallback(ping,sourceEnv,{profile,report}),
  renew:(reason,report)=>renewFallbackToken(sourceEnv,reason,report)
 });
 return Response.json({...result,preview_saved:saved},{status:result.ok?200:503});
}};
