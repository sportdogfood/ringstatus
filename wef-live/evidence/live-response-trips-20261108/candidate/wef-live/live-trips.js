'use strict';
// Day-0 class-trip projection. No requests, helper reads, synthetic messages or comments.
const model=require('./model');
const RESULTS={placing:'placing',position:'position',...Object.fromEntries(Array.from({length:6},(_,i)=>['score'+(i+1),'score'+(i+1)])),...Object.fromEntries(['time','faults','time-fault'].flatMap(prefix=>['one','two','three'].map(round=>[prefix+'-'+round,prefix.replaceAll('-','_')+'_'+round]))),'points-earned':'points_earned','prize-money':'total_prize_money','gone-in':'gone_in',scratch:'scratch_trip'};
const supplied=v=>v!==null&&v!==undefined;
function project(decoded,live,s,c,at,onIssue=()=>{}){
 const cid=c['class-id'];
 if(Number(decoded?.class?.show_id)!==s.show||Number(decoded?.class?.class_id)!==cid||model.day(decoded.class_related_data?.date)!==s.date||!Array.isArray(decoded.trips))throw Error('Class trip source scope mismatch');
 const liveRows=Number(live?.ID)===cid&&Array.isArray(live.rows)&&Number(live.recs)===live.rows.length?live.rows:[];
 const rows=[];let complete=true;
 for(const raw of decoded.trips){try{
  const eid=model.integer(raw.entry_id,'trip entry ID'),tid=model.number(raw.trip_id),uuid=raw.entryxclasses_uuid==null?'':String(raw.entryxclasses_uuid);
  if(!uuid&&!(Number.isSafeInteger(tid)&&tid>0))throw Error('Trip source UUID and trip ID missing');
  const matches=liveRows.filter(r=>Number(r.id)===tid&&Number(r.class_id)===cid),observed=matches.length===1?matches[0]:null;
  if(matches.length>1)onIssue('Ambiguous live observations for trip '+tid,{class_id:cid,trip_id:tid});
  const ring=model.number(c._ring?.split('|').at(-1));
  const key=model.key(s.customer,s.show,s.date,ring??'',cid,eid,uuid||tid);
  const row={'trip-key':key,'show-id':s.show,'customer-id':s.customer,date:s.date,'last-seen':at,'class-id':cid,'class-key':c['class-key'],'entry-id':eid,'entry-key':model.key(s.show,eid),'record-status':'Active','source-json':JSON.stringify({identity:raw,...(observed?{live:observed}:{})})};
  if(uuid)row['source-uuid']=uuid;
  if(Number.isSafeInteger(tid)&&tid>0)row['trip-id']=tid;
  if(ring!==null)row['ring-id']=ring;
  for(const [target,source]of Object.entries({'horse-id':'horse_id','rider-id':'rider_id','trainer-id':'trainer_id','owner-id':'owner_id','entry-number':'number','order-of-go':'order_of_go','actual-order':'actual_order'})){
   const n=model.number(raw[source]);if(n!==null)row[target]=n;
  }
  for(const [target,value]of Object.entries({'horse-name':raw.horse??observed?.Hor,'rider-name':raw.rider_name??raw.rider??observed?.Rid,'trainer-name':raw.trainer??observed?.Tra,'owner-name':raw.Owner,'estimated-go-time':raw.estimated_go_time}))if(supplied(value))row[target]=String(value);
  if(supplied(raw.scratch_trip))row.scratch=model.flag(raw.scratch_trip);
  if(supplied(raw.gone_in))row['gone-in']=String(raw.gone_in);
  const result={};
  for(const [target,source]of Object.entries(RESULTS))if(supplied(raw[source]))result[target]=String(raw[source]);
  for(const [target,source]of Object.entries({'order-of-go':'OOG','actual-order':'Actual_OOG'}))if(supplied(observed?.[source])&&model.number(observed[source])!==null)row[target]=model.number(observed[source]);
  if(supplied(observed?.Scr))row.scratch=model.flag(observed.Scr);
  if(supplied(observed?.Gone)&&model.number(observed.Gone)!==null)row.gone=model.number(observed.Gone);
  if(supplied(observed?.ActualPos??observed?.Pos))result.position=String(observed.ActualPos??observed.Pos);
  if(supplied(decoded.class_related_data?.results_verified))result['results-verified']=String(decoded.class_related_data.results_verified);
  if(supplied(decoded.class_related_data?.status))result['class-status']=String(decoded.class_related_data.status);
  row['results-json']=JSON.stringify(result);
  for(const name of ['placing','results-verified','class-status'])if(supplied(result[name]))row[name]=result[name];
  if(model.number(result.position)!==null)row.position=model.number(result.position);
  rows.push(row);
 }catch(e){complete=false;onIssue(e.message,{class_id:cid,trip_id:model.number(raw?.trip_id)});}}
 return{rows,complete};
}
// Reuse normalized rows from the one live-detail request; no decoded request.
function fromLive(entries,s,c,prior,at,onIssue=()=>{}){
 const rows=[],seen=new Set();let complete=true;
 for(const entry of entries){try{
  const tid=entry['trip-id'],cid=c['class-id'];
  const matches=prior.filter(r=>Number(r['show-id'])===s.show&&r.date===s.date&&Number(r['class-id'])===cid&&Number(r['trip-id'])===tid);
  if(matches.length>1){complete=false;onIssue('Ambiguous saved live trip '+tid,{class_id:cid,trip_id:tid});continue;}
  const old=matches[0],ring=model.number(c._ring?.split('|').at(-1));
  if(seen.has(tid))throw Error('Duplicate live trip '+tid);seen.add(tid);
  const row={'trip-key':old?.['trip-key']||model.key(s.customer,s.show,s.date,ring??'',cid,'',tid),'show-id':s.show,'customer-id':s.customer,date:s.date,'class-id':cid,'class-key':c['class-key'],'trip-id':tid,'last-seen':at,'record-status':'Active'};
  if(ring!==null)row['ring-id']=ring;
  for(const field of ['entry-number','horse-name','rider-name','trainer-name','order-of-go','actual-order','scratch','gone','position'])if(supplied(entry[field]))row[field]=entry[field];
  const previousSource=old?.['source-json']?JSON.parse(old['source-json']):{};
  row['source-json']=JSON.stringify({...previousSource,...JSON.parse(entry['source-json'])});
  row['source-updated-at']=entry['source-updated-at'];
  if(supplied(entry.position))row['results-json']=JSON.stringify({...JSON.parse(old?.['results-json']||'{}'),position:String(entry.position)});
  rows.push(row);
 }catch(e){complete=false;onIssue(e.message,{class_id:c['class-id'],trip_id:entry['trip-id']});}}
 return{rows,complete};
}
module.exports={project,fromLive,RESULTS};
