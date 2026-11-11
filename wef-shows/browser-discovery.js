"use strict";
const fs=require('node:fs');
const PAGE='https://www.wellingtoninternational.com/showgrounds/show-schedule/';
const API='https://sglapi.wellingtoninternational.com';
// Existing task limit is 240s; fallback source reads can consume 183s.
const BROWSER_TIMEOUT_MS=20000;
function date(value){
 const day=typeof value==='string'?value.slice(0,10):'';
 if(!/^\d{4}-\d{2}-\d{2}$/.test(day)||!Number.isFinite(Date.parse(day))||new Date(day+'T00:00:00Z').toISOString().slice(0,10)!==day)throw Error('Browser show date invalid');
 return day;
}
function normalize({url,defaultShow,payload}){
 const u=new URL(url),meta=payload?.show,sid=Number(u.searchParams.get('sid'));
 if(u.origin!==new URL(PAGE).origin||u.pathname!==new URL(PAGE).pathname||!Number.isSafeInteger(sid)||sid<=0||Number(defaultShow)!==sid||Number(meta?.show_id)!==sid)throw Error('Browser default show identity mismatch');
 if(!payload||Array.isArray(payload)||payload._t||!Array.isArray(payload.rings))throw Error('Browser schedule payload invalid');
 const start=date(meta.start_date),end=date(meta.end_date),selected=date(payload.show_date??payload.showDate);
 if(start>end||selected<start||selected>end||u.searchParams.get('date')!==selected)throw Error('Browser selected date outside show range or URL mismatch');
 if(typeof meta.show_name!=='string'||!meta.show_name.trim()||!Number.isSafeInteger(Number(meta.facility_id))||Number(meta.facility_id)<=0)throw Error('Browser show metadata incomplete');
 return {show:{fields:{show_id:sid,customer_id:15,show_name:meta.show_name,start_date:start,end_date:end,sgl_default_day:selected}},payload,endpoint:url,
  evidence:{page:PAGE,url,default_show:sid,source_selected_date:selected,show:meta,headless:true,isolated_context:true}};
}
async function capture({chromium=require('playwright').chromium,timeoutMs=BROWSER_TIMEOUT_MS}={}){
 let browser,timer;
 const job=(async()=>{
  browser=await chromium.launch({channel:'msedge',headless:true,timeout:15000});
  // launch() owns a temporary profile; newContext() never reads the owner's profile/storage.
  const context=await browser.newContext({timezoneId:'America/New_York'}),page=await context.newPage();
  const shows=[],schedules=[];let expectedUrl,accept,reject;
  const evidence=new Promise((resolve,fail)=>{accept=resolve;reject=fail;});
  // A response may arrive before or after history.pushState updates the URL.
  function complete(){
   if(!expectedUrl||!shows.length)return;
   const u=new URL(expectedUrl),matches=schedules.filter(p=>String(p?.show?.show_id)===u.searchParams.get('sid')&&(p.show_date??p.showDate)===u.searchParams.get('date'));
   if(!matches.length)return;
   try{accept(normalize({url:expectedUrl,defaultShow:shows[shows.length-1],payload:matches[matches.length-1]}));}catch(error){reject(error);}
  }
  page.on('response',response=>{
   const u=new URL(response.url());
   if(u.origin!==API||!['/shows','/schedule'].includes(u.pathname)||u.searchParams.get('customer_id')!=='15'||response.status()!==200)return;
   void (async()=>{try{const p=await response.json();if(u.pathname==='/shows')shows.push(p.default_show);else{const authorization=await response.request().headerValue('authorization');const decoded=await require('./browser-decode').decodeSglPayload(JSON.stringify(p),authorization,require('./browser-decode').SGL_BUNDLE_SALT,true);schedules.push(JSON.parse(decoded));}complete();}catch{/* Invalid JSON is withheld; the bounded wait still requires both responses. */}})();
  });
  await page.goto(PAGE,{waitUntil:'domcontentloaded',timeout:30000});
  await page.waitForURL(u=>/^[1-9]\d*$/.test(u.searchParams.get('sid')||'')&&/^\d{4}-\d{2}-\d{2}$/.test(u.searchParams.get('date')||''),{timeout:30000});
  expectedUrl=page.url();complete();
  return evidence;
 })();
 try{return await Promise.race([job,new Promise((_,reject)=>{timer=setTimeout(()=>reject(Error('Browser discovery timed out')),timeoutMs);})]);}
 catch(error){throw Error(error.message?.startsWith('Browser ')?error.message:'Browser discovery failed ['+String(error.name||'Error').replace(/[^A-Za-z]/g,'')+']');}
 finally{clearTimeout(timer);if(browser)await browser.close();}
}
async function discoverBrowser({statePath,now,capturePage=capture}={}){
 const found=await capturePage();
 // Retain the existing local focus/override contract without invoking either proxy endpoint.
 const previous=fs.existsSync(statePath)?JSON.parse(fs.readFileSync(statePath,'utf8')):{},f=found.show.fields;
 const manual=previous.overrides?.[String(f.show_id)]||{},shared=require('../wef-schedules/collect');
 Object.assign(f,{focus_day:manual.focus_day?shared.isoDate(manual.focus_day):shared.localDay(now),ring_collection:manual.ring_collection,has_full_schedule_payload:manual.has_full_schedule_payload===true,full_schedule_payload_file:manual.full_schedule_payload_file||[]});
 const today=shared.localDay(now),same=previous.show_id===f.show_id;
 const default_show={level:'INFO',code:same?'default_show_current':'default_show_changed',
  previous:previous.show_id?{show_id:previous.show_id,start_date:previous.fields?.start_date||null,end_date:previous.fields?.end_date||null}:null,
  current:{show_id:f.show_id,start_date:f.start_date,end_date:f.end_date},
  schedule_day:{previous:same?previous.fields?.sgl_default_day||null:null,current:f.sgl_default_day,priority_day:f.sgl_default_day,fallback_reason:null},
  today,phase:today<f.start_date?'upcoming':today>f.end_date?'ended':'current',message:'Verified default supplied by fresh browser discovery'};
 await shared.atomicReport(statePath,{...previous,version:1,show_id:f.show_id,fields:f,checked_at:now,discovery:same?'current':'new',overrides:previous.overrides||{},default_show});
 return found;
}
module.exports={PAGE,BROWSER_TIMEOUT_MS,normalize,capture,discoverBrowser};
