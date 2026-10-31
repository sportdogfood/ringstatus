"use strict";
// Read verified wef-shows data. Never performs SGL discovery or starts a job.
const fs=require('node:fs'),path=require('node:path'),crypto=require('node:crypto'),zlib=require('node:zlib');
const settings=require('./settings');
const SOURCE_BASE='appCaSmL819KK7UFJ';
const ARCHIVE_ROOT=path.resolve(__dirname,'../wef-shows/runtime/history');
async function listShows(request=globalThis.fetch){
 const rows=[];let offset;
 do{const query=new URLSearchParams({pageSize:'100',...(offset?{offset}:{})});
  const r=await request(`https://api.airtable.com/v0/${SOURCE_BASE}/shows?${query}`,{headers:{Authorization:'Bearer '+settings.AIRTABLE_TOKEN},signal:AbortSignal.timeout(30000)});
  if(!r.ok)throw Error('wef-shows read HTTP '+r.status);const data=await r.json();rows.push(...data.records);offset=data.offset;
 }while(offset);return rows;
}
function verifiedPayload(fields,archiveRoot=ARCHIVE_ROOT){
 const file=fields['default-payload-file'],hash=fields['default-payload-sha256'];
 if(typeof file!=='string'||!/^[a-f0-9]{64}$/.test(hash||''))throw Error('Missing verified show payload');
 const relative=path.relative(path.resolve(archiveRoot),path.resolve(file));
 if(!relative||relative.startsWith('..')||path.isAbsolute(relative))throw Error('Show payload outside archive root');
 const body=zlib.gunzipSync(fs.readFileSync(file));
 if(crypto.createHash('sha256').update(body).digest('hex')!==hash)throw Error('Show payload hash mismatch');
 return JSON.parse(body);
}
async function readShowFocus({statePath=settings.SCHEDULE_FOCUS_FILE,now=new Date().toISOString(),read=listShows,archiveRoot=ARCHIVE_ROOT}={}){
 const shared=require('./collect');
 const matches=(await read()).filter(r=>r.fields.is_default===true&&Number(r.fields.customer_id)===15);
 if(matches.length!==1)throw Error('Exactly one default wef-shows record required');
 const record=matches[0],f=record.fields,sid=Number(f.show_id);
 if(!Number.isSafeInteger(sid)||sid<=0||f['show-key']!==`15|${sid}`||require('../wef-shows/authority-age').showAuthorityReason(f,now,{futureMs:60000,inclusive:true}))throw Error('Invalid or stale wef-shows default');
 const payload=verifiedPayload(f,archiveRoot),meta=payload.show||{},start=shared.isoDate(f.start_date),end=shared.isoDate(f.end_date),selected=shared.isoDate(f.selected_date);
 if(start>end||selected<start||selected>end||Number(meta.show_id)!==sid||Number(meta.facility_id)!==Number(f.facility_id)||shared.isoDate(meta.start_date)!==start||shared.isoDate(meta.end_date)!==end||!require('../wef-shows/discovery-policy').selectionMatches(f,payload)||String(meta.show_name||'')!==String(f.show_name||''))throw Error('wef-shows metadata/payload mismatch');
 const previous=fs.existsSync(statePath)?JSON.parse(fs.readFileSync(statePath,'utf8')):{},manual=previous.overrides?.[String(sid)]||{};
 const fields={show_id:sid,customer_id:15,show_name:f.show_name,start_date:start,end_date:end,focus_day:manual.focus_day?shared.isoDate(manual.focus_day):shared.localDay(now),sgl_default_day:selected,ring_collection:manual.ring_collection,has_full_schedule_payload:manual.has_full_schedule_payload===true,full_schedule_payload_file:manual.full_schedule_payload_file||[]};
 const today=shared.localDay(now),changed=previous.show_id!==sid;
 const state={...previous,version:1,show_id:sid,fields,checked_at:now,discovery:changed?'new':'current',overrides:previous.overrides||{},show_source:{base:SOURCE_BASE,record:record.id,checked_at:f.last_checked},default_show:{level:'INFO',code:changed?'default_show_changed':'default_show_current',previous:previous.show_id?{show_id:previous.show_id,start_date:previous.fields?.start_date,end_date:previous.fields?.end_date}:null,current:{show_id:sid,start_date:start,end_date:end},schedule_day:{previous:previous.show_id===sid?previous.fields?.sgl_default_day||null:null,current:selected,priority_day:selected,fallback_reason:null},today,phase:today<start?'upcoming':today>end?'ended':'current',message:'Verified default supplied by wef-shows'}};
 await shared.atomicReport(statePath,state);
 return{show:{fields},state,endpoint:require('./collector-core').buildScheduleEmptyEndpoint(sid,15),payload};
}
module.exports={readShowFocus,verifiedPayload,SOURCE_BASE,ARCHIVE_ROOT};
