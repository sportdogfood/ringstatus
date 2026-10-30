"use strict";
// Adapted from the proven Schedules entry source-reader (29 October); owned and invoked only by scheduled Schedules.
const {fetchTextWithConfiguredTransport}=require('./lib/sgl_fetch_adapter');
const crypto=require('node:crypto');
function target(route,params){
 if(!params||typeof params!=='object'||params.customer_id!==15||!Number.isSafeInteger(params.show_id)||params.show_id<=0)throw Error('Invalid Schedules entry source scope');
 const allowed=route==='/schedule'?['customer_id','show_id','date']:route==='/entries'?['customer_id','show_id','page','sort_on','sort_type','search_text']:/^\/classes\/[1-9]\d*$/.test(route)?['customer_id','show_id']:null;
 if(!allowed||Object.keys(params).some(k=>!allowed.includes(k)))throw Error('Unapproved Schedules entry source route/parameters');
 if(route==='/schedule'&&!/^\d{4}-\d{2}-\d{2}$/.test(params.date||''))throw Error('Dated inventory required');
 if(route==='/entries'&&(!Number.isSafeInteger(params.page)||params.page<=0||params.sort_on!=='number'||params.sort_type!=='asc'||params.search_text!==''))throw Error('Invalid directory page/filter');
 const url=new URL(route,'https://wef-schedules-decoded.gombcg.workers.dev');url.search=new URLSearchParams(params).toString();return url.toString();
}
async function read(route,params,{transport=fetchTextWithConfiguredTransport,timeout=18}={}){
 const url=target(route,params),started=new Date().toISOString(),begin=Date.now();
 let result;
 const metric={route,params,started,http:null};
 const finish=()=>{metric.completed=new Date().toISOString();metric.elapsed_seconds=(Date.now()-begin)/1000;};
 try{
  result=await transport(url,async endpoint=>{
   const response=await fetch(endpoint,{signal:AbortSignal.timeout(timeout*1000),headers:{Accept:'application/json'}});
   const body=Buffer.from(await response.arrayBuffer());return {response,body,text:body.toString('utf8')};
  },{env:{SGL_FETCH_TRANSPORT:'node'}});
 }catch(error){finish();if(['TimeoutError','AbortError'].includes(error.name))metric.timeout=true;return {ok:false,error:'Source transport failed ('+(['TimeoutError','AbortError','TypeError'].includes(error.name)?error.name:'Error')+')',metric};}
 const response=result.response;metric.http=response.status;metric.headers={};
 for(const name of ['content-type','cf-ray','server','x-rs-decoded']){const value=response.headers?.get(name);if(value)metric.headers[name]=String(value).slice(0,160);}
 finish();
 if(response.status!==200)return {ok:false,error:'Source HTTP '+response.status,metric};
 if(response.headers?.get('x-rs-decoded')!=='1')return {ok:false,error:'Source decoding not confirmed',metric};
 if(!Buffer.isBuffer(result.body))return {ok:false,error:'Original source bytes unavailable',metric};
 const body=result.body;metric.bytes=body.length;metric.sha256=crypto.createHash('sha256').update(body).digest('hex');
 return {ok:true,body:body.toString('base64'),metric};
}
if(require.main===module){
 Promise.resolve().then(()=>read(process.argv[2],JSON.parse(process.argv[3]),{timeout:Number(process.argv[4])||18})).then(result=>{
  console.log(JSON.stringify(result));process.exitCode=result.ok?0:1;
 }).catch(()=>{console.log(JSON.stringify({ok:false,error:'Invalid scheduled Schedules entry source request'}));process.exitCode=1;});
}
module.exports={read,target};

async function readJson(route,params,options){
 const receipt=await read(route,params,options);if(!receipt.ok)throw Error(receipt.error);
 const bytes=Buffer.from(receipt.body,'base64');if(bytes.length!==receipt.metric.bytes||crypto.createHash('sha256').update(bytes).digest('hex')!==receipt.metric.sha256)throw Error('Entry source receipt mismatch');
 const payload=JSON.parse(bytes.toString('utf8'));
 const check=x=>{if(Array.isArray(x))x.forEach(check);else if(x&&typeof x==='object'){if(Object.hasOwn(x,'_t'))throw Error('Undecoded entry source envelope');Object.values(x).forEach(check);}};check(payload);
 return{payload,metric:receipt.metric};
}
module.exports.readJson=readJson;
