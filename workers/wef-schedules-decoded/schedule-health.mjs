import {healthStore,HEALTH_TABLES} from './health-records.mjs';
const profiles=['original-header','profile-fb-header1','profile-fb-header2','windows-clean','android-clean'];
const localDate=now=>new Intl.DateTimeFormat('en-CA',{timeZone:'America/New_York',year:'numeric',month:'2-digit',day:'2-digit'}).format(new Date(now));
export async function scheduleHealth(request,env,{original,alternate,renew,store=healthStore(env),sleep=ms=>new Promise(r=>setTimeout(r,ms)),now=()=>new Date().toISOString()}={}){
 const at=now(),attempts=[],errors=[];let show,day,data,status=null,successful=false,warning,renewalFailure;
 let classCheck={ok:null,status:null,reason:'Schedule unavailable'},scheduleStatus=null;
 let prior;
 try{
  const rows=await store.list(HEALTH_TABLES.health,"{health-key}='decoder'");if(rows.length>1)throw Error('Duplicate decoder health record');prior=rows[0]?.fields;
  const defaults=await store.list(HEALTH_TABLES.shows,'AND({is_default}=TRUE(),{customer_id}=15)');
  if(defaults.length!==1||!Number.isSafeInteger(defaults[0].fields.show_id)||defaults[0].fields.show_id<=0)throw Error('Exactly one valid default show required');
  show=defaults[0];
  const ping=new URL('/schedule',request.url);ping.search=new URLSearchParams({date:'00/00/00',show_id:String(show.fields.show_id),customer_id:'15'});
  for(let round=0;round<2&&!successful&&!renewalFailure;round++){
   if(round){
    await sleep(1000);
    try{await renew('schedule-health-recovery',failure=>{renewalFailure=failure;});}
    catch{errors.push('Token renewal failed');break;}
   }
   for(const profile of profiles){
    if(attempts.length||round)await sleep(1000);
    let response;
    const details={},report=fields=>Object.assign(details,fields);
    try{
     response=round===0&&profile===profiles[0]?await original(new Request(ping),report):await alternate(new Request(ping),profile,report);
     status=response.status;data=await response.json().catch(()=>null);
     successful=response.ok&&response.headers.get('x-rs-decoded')==='1'&&String(data?.show?.show_id)===String(show.fields.show_id)&&/^\d{4}-\d{2}-\d{2}$/.test(data?.show_date||'')&&Array.isArray(data?.rings);
     attempts.push({round:round+1,profile,lane:response.headers.get('x-rs-source-lane')||'cloudflare-fallback',status,usable:successful,...details});
    }catch{attempts.push({round:round+1,profile,status:null,usable:false,reason:'Request failed',...details});}
    if(details.stage==='token-before-schedule')renewalFailure=details;
    if(successful||renewalFailure)break;
   }
  }
  const date=successful?data.show_date:localDate(at);
  const days=await store.list(HEALTH_TABLES.days);
  const matching=days.filter(r=>r.fields.date===date&&r.fields.show?.includes(show.id));if(matching.length>1)throw Error('Duplicate matching show-day');day=matching[0];
  scheduleStatus=status;
  if(successful){
   const sample=data.rings.flatMap(r=>Array.isArray(r.classes)?r.classes:[]).find(c=>Number.isSafeInteger(Number(c.class_id))&&Number(c.class_id)>0);
   classCheck={ok:null,status:null,reason:'No class in returned schedule'};
   if(sample){
    const id=Number(sample.class_id),classPing=new URL('/classes/'+id,request.url);
    classPing.search=new URLSearchParams({show_id:String(show.fields.show_id),customer_id:'15'});
    classCheck={ok:false,status:null,class_id:id,endpoint:classPing.pathname+classPing.search};
    for(let round=0;round<2&&!classCheck.ok&&!renewalFailure;round++){
     if(round){
      await sleep(1000);
      try{await renew('class-health-recovery',failure=>{renewalFailure=failure;});}
      catch{errors.push('Class token renewal failed');break;}
     }
     for(const profile of profiles){
      await sleep(1000);
      const details={},report=fields=>Object.assign(details,fields);
      try{
       const response=round===0&&profile===profiles[0]?await original(new Request(classPing),report):await alternate(new Request(classPing),profile,report);
       const payload=await response.json().catch(()=>null);
       classCheck.status=response.status;
       classCheck.ok=response.ok&&response.headers.get('x-rs-decoded')==='1'&&Number(payload?.class?.class_id)===id&&Array.isArray(payload?.trips);
       if(classCheck.ok)classCheck.trips=payload.trips.length;
       attempts.push({endpoint:classCheck.endpoint,round:round+1,profile,lane:response.headers.get('x-rs-source-lane')||'cloudflare-fallback',status:response.status,usable:classCheck.ok,...details});
      }catch{attempts.push({endpoint:classCheck.endpoint,round:round+1,profile,status:null,usable:false,reason:'Class request failed',...details});}
      if(details.stage==='token-before-schedule')renewalFailure=details;
      if(classCheck.ok||renewalFailure)break;
     }
    }
   }
  }
 }catch(e){errors.push(e.message);}
 const links={show:show?[show.id]:[],'show-day':day?[day.id]:[]};
 const ok=successful&&classCheck.ok!==false&&errors.length===0;
 const message=ok?`Decoder recovered: show ${show.fields.show_id}; ${data.rings.length} rings; class detail ${classCheck.ok?'passed':'not checked (no scheduled class)'}.`:`Decoder ${successful?'class-detail':'schedule'} health failed${show?'; show '+show.fields.show_id:''}${classCheck.class_id?'; class '+classCheck.class_id:''}.`;
 try{
  const transition=!ok&&prior?.status!=='Outage'?'Outage':ok&&prior?.status==='Outage'?'Recovery':null;
  if(transition){
   const id=`decoder|${transition}|${transition==='Recovery'?prior['outage-started-at']||at:at}`;
   warning=await store.save(HEALTH_TABLES.warnings,'warning-id',{'warning-id':id,at,kind:transition,endpoint:request.url,...links,message,recipient:'Owner — existing decoder monitoring chat','delivery-status':'Pending'});
  }
  await store.save(HEALTH_TABLES.health,'health-key',{'health-key':'decoder',service:'decoder',endpoint:request.url,...links,status:ok?'Healthy':'Outage','checked-at':at,'http-status':classCheck.ok===false?classCheck.status:status,'last-success-at':ok?at:prior?.['last-success-at']||null,'outage-started-at':ok?null:prior?.status==='Outage'?prior['outage-started-at']||at:at,'recovered-at':ok&&prior?.status==='Outage'?at:prior?.['recovered-at']||null,details:JSON.stringify({attempts,errors,renewalFailure,rings:successful?data.rings.length:null,schedule_status:scheduleStatus??status,class_check:classCheck})});
 }catch{errors.push('Health or warning log save failed');}
 return {ok:ok&&errors.length===0,worker:'wef-schedules-decoded',show_id:show?String(show.fields.show_id):null,rings:successful?data.rings.length:0,schedule_status:scheduleStatus??status,class_check:classCheck,attempts,errors,...(warning?{warning_id:warning.id}: {})};
}
