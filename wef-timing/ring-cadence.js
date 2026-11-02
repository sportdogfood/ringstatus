'use strict';
const fs=require('node:fs'),path=require('node:path');
const {localDay}=require('./purge');
function inWindow(now){
 const time=new Intl.DateTimeFormat('en-GB',{timeZone:'America/New_York',hour:'2-digit',minute:'2-digit',hourCycle:'h23'}).format(new Date(now));
 return time>='05:00'&&time<='19:00';
}
function readReceipt(){
 const runtime=path.join(__dirname,'../wef-live/runtime');
 try{return JSON.parse(fs.readFileSync(path.join(runtime,'live-check-last-run.json'),'utf8'));}
 catch(e){if(e.code!=='ENOENT')throw e;}
 return JSON.parse(fs.readFileSync(path.join(runtime,'lane-rings-last-run.json'),'utf8'));
}
function check({previous,now,customer,show,read=readReceipt}){
 const ms=Date.parse(now);if(!Number.isFinite(ms))throw Error('Invalid Ring cadence clock');
 const day=localDay(now),window=inWindow(now),last=Date.parse(previous?.checked_at),next=Date.parse(previous?.next_check_at);
 const entersWindow=previous?.reason==='outside-window'&&window;
 if(previous?.day===day&&previous.customer===customer&&previous.show===show&&!entersWindow&&Number.isFinite(last)&&last<=ms&&Number.isFinite(next)&&next>ms&&next<=last+3600000&&!(previous.collect&&!window))return{...previous,due:false,collect:false};
 let receipt,reason='live-evidence-unavailable';try{receipt=read();}catch{}
 const observed=Date.parse(receipt?.started),completed=Date.parse(receipt?.completed);
 const valid=['live-check','rings'].includes(receipt?.lane)&&receipt.ok===true&&receipt.scope?.customer===customer&&receipt.scope?.show===show&&typeof receipt.live_status==='boolean'&&Number.isFinite(observed)&&Number.isFinite(completed)&&observed<=completed&&completed<=ms&&ms-observed<3600000;
 let collect=false;
 if(valid){
  if(!receipt.live_status)reason='live-off';
  else if(receipt.scope.date!==day)reason='live-date-mismatch';
  else if(!window)reason='outside-window';
  else{collect=true;reason='live-today';}
 }
 const minutes=collect?3:60;
 return{due:true,collect,minutes,reason,day,customer,show,checked_at:now,next_check_at:new Date(Math.floor(ms/60000)*60000+minutes*60000).toISOString(),live_status:valid?receipt.live_status:null,source_date:valid?receipt.scope.date:null,source_run:valid?receipt.run_id:null,source_observed_at:valid?receipt.started:null,warning:reason==='live-date-mismatch'?'Live is on but its source date is not today':null};
}
module.exports={check,readReceipt,inWindow,localDay};
