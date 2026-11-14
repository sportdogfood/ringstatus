'use strict';
// Existing schedule decoder; public class payloads. Recovery runs only in prepare.
const fs=require('node:fs'),path=require('node:path'),crypto=require('node:crypto');
const decoder='https://wef-schedules-decoded.gombcg.workers.dev';
const publicRoot='https://sgl.wellingtoninternational.com/iphonev2/index.php/esp/liveclassv2/getLiveClassData';
const profiles={
 iphone:{'User-Agent':'Mozilla/5.0 (iPhone; CPU iPhone OS 18_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.5 Mobile/15E148 Safari/604.1','Sec-Fetch-Site':'cross-site'},
 android:{'User-Agent':'Mozilla/5.0 (Linux; Android 16; Pixel 10) AppleWebKit/537.36 (KHTML, like Gecko) Edg/157.0.0.0 Mobile Safari/537.36','Sec-Fetch-Site':'same-site'},
 windows:{'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/158.0.0.0 Safari/537.36 Edg/158.0.0.0','Sec-Fetch-Site':'same-site'}
};
function diagnosticSafe(value,full=false){
 let text=String(value??'');
 for(const [name,secret] of Object.entries(process.env))if(/TOKEN|COOKIE|PASSWORD|SECRET|API_KEY|RUNNER_KEY/i.test(name)&&secret)text=text.split(secret).join('[redacted]');
 text=text.replace(/([\"']?cookie[\"']?\s*[:=]\s*)[^\r\n]*/gi,'$1[redacted]').replace(/([\"']?(?:_t|access_token|refresh_token|token)[\"']?\s*[:=]\s*)(?:\"[^\"]*\"|'[^']*'|[^\s,;}]+)/gi,'$1[redacted]');
 return full?text.replace(/Bearer\s+[^\s]+/gi,'Bearer [redacted]').replace(/([?&](?:token|key|authorization|cookie|access_token)=)[^\s&]+/gi,'$1[redacted]'):require('./attempt-log').safe(text);
}
async function once(route,params,profile,mode,request=fetch){
 const isSchedule=route==='/schedule',url=new URL(isSchedule?'/schedule':publicRoot,isSchedule?decoder:undefined);
 url.search=new URLSearchParams(isSchedule?params:{show_id:params.show_id,cid:route.split('/').pop(),t:Date.now()}).toString();
 const started=new Date().toISOString(),begin=Date.now();let status=null,diagnostic={};
 try{
  const headers={Accept:'application/json','Cache-Control':'no-cache',...(isSchedule?{'X-RS-Recovery':mode}:profiles[profile])};
  const response=await request(url,{headers,signal:AbortSignal.timeout(30000)});status=response.status;
  diagnostic.headers={};for(const name of ['content-type','cf-ray','x-rs-source-lane','x-rs-header-profile','x-rs-recovery-state','x-rs-failure-reason','x-rs-upstream-status']){const value=response.headers.get(name);if(value)diagnostic.headers[name]=diagnosticSafe(value);}
  const raw=await response.text();let body;
  try{body=JSON.parse(raw);}catch(error){
   if(!isSchedule){diagnostic.rejected_payload=diagnosticSafe(raw,true);diagnostic.validation={failed:['json_parse'],expected:'JSON response',actual:'Invalid JSON'};}
   throw error;
  }
  if(!isSchedule&&!response.ok){diagnostic.rejected_payload=rejectedPayload(body);diagnostic.validation={failed:['http_status'],expected:'Successful HTTP status',actual:status};}
  if(!response.ok){diagnostic.failure={};for(const key of ['message','reason','stage','upstreamStatus','tokenHttpStatus','statusCode'])if(body?.[key]!==undefined)diagnostic.failure[key]=typeof body[key]==='number'||body[key]===null?body[key]:diagnosticSafe(typeof body[key]==='string'?body[key]:JSON.stringify(safe(body[key])));throw Error('Source HTTP '+status+': '+[body?.message,body?.reason||diagnostic.headers['x-rs-failure-reason']].filter(Boolean).join('; '));}
  if(isSchedule){if(response.headers.get('x-rs-decoded')!=='1'||!Array.isArray(body.rings))throw Error('Schedule decoding/data unavailable');}
  else{
   const expectedId=route.split('/').pop(),idMatches=String(body?.ID)===expectedId,rowsArray=Array.isArray(body?.rows);
   if(!idMatches||!rowsArray){
    const failed=[...(!idMatches?['class_id_match']:[]),...(!rowsArray?['rows_array']:[])];
    diagnostic.rejected_payload=rejectedPayload(body);
    diagnostic.validation={failed,expected_id:expectedId,actual_id:rejectedPayload(body?.ID??null),rows_type:body?.rows===null?'null':Array.isArray(body?.rows)?'array':typeof body?.rows};
    throw Error('Class payload missing or mismatched ID/rows: '+failed.join(', '));
   }
  }
  const clean=safe(body),bytes=Buffer.from(JSON.stringify(clean));
  return {ok:true,body:bytes.toString('base64'),metric:{route,params,url:url.toString(),started,completed:new Date().toISOString(),http:status,elapsed_seconds:(Date.now()-begin)/1000,profile:isSchedule?response.headers.get('x-rs-header-profile'):profile,lane:isSchedule?response.headers.get('x-rs-source-lane'):'direct-iphonev2',bytes:bytes.length,sha256:crypto.createHash('sha256').update(bytes).digest('hex')}};
 }catch(e){return {ok:false,error:diagnosticSafe(require('./source-reader').errorText(e)),metric:{...diagnostic,route,params,url:url.toString(),started,completed:new Date().toISOString(),http:status,profile,elapsed_seconds:(Date.now()-begin)/1000}};}
}
async function read(route,params,prepare=false,laneFile=null,request=fetch){
 if(route==='/schedule'){
  const check=await once(route,params,null,'defer',request);
  if(check.ok||!prepare)return {...check,attempts:[check.metric]};
  const recovered=await once(route,params,null,'trouble',request);
  return {...recovered,attempts:[check.metric,recovered.metric]};
 }
 let saved;try{saved=JSON.parse(fs.readFileSync(laneFile,'utf8')).profile;}catch{}
 const choices=prepare?[...new Set([saved,...Object.keys(profiles)].filter(Boolean))]:[saved||'iphone'];
 const attempts=[];let result;
 for(const profile of choices){result=await once(route,params,profile,null,request);attempts.push(result.metric);if(result.ok){if(prepare&&laneFile){fs.mkdirSync(path.dirname(laneFile),{recursive:true});fs.writeFileSync(laneFile,JSON.stringify({profile}));}break;}}
 return {...result,attempts};
}
if(require.main===module)read(process.argv[2],JSON.parse(process.argv[3]),process.argv[4]==='prepare',process.argv[5]).then(x=>console.log(JSON.stringify(x))).catch(e=>{console.error(e.message);process.exitCode=1;});
module.exports={read,once,profiles};
function safe(v){if(Array.isArray(v))return v.map(safe);if(v&&typeof v==='object')return Object.fromEntries(Object.entries(v).filter(([k])=>!/^(_|_t|token|cookie|authorization|access_token|refresh_token)$/i.test(k)).map(([k,x])=>[k,safe(x)]));return v;}
function rejectedPayload(value){
 const clean=safe(value);
 const redact=v=>typeof v==='string'?diagnosticSafe(v,true):Array.isArray(v)?v.map(redact):v&&typeof v==='object'?Object.fromEntries(Object.entries(v).map(([k,x])=>[k,redact(x)])):v;
 return redact(clean);
}
