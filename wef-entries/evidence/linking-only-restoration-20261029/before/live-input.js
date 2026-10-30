'use strict';
// Optional read-only enrichment from Live's own normalized class evidence.
// Never follows Live's reverse Entries binder or participation-link helpers.
const fs=require('node:fs'),path=require('node:path'),zlib=require('node:zlib'),crypto=require('node:crypto');
const settings=require('./settings');
const hash=value=>crypto.createHash('sha256').update(value).digest('hex');
const localDay=now=>new Intl.DateTimeFormat('en-CA',{timeZone:'America/New_York',year:'numeric',month:'2-digit',day:'2-digit'}).format(new Date(now));
const fresh=(value,now)=>{const age=Date.parse(now)-Date.parse(value);return Number.isFinite(age)&&age>=-60000&&age<=1800000;};
const id=value=>Number.isSafeInteger(Number(value))&&Number(value)>0;
const equal=(a,b)=>typeof a==='string'&&typeof b==='string'&&/^\d{4}-\d\d-\d\dT/.test(a)&&/^\d{4}-\d\d-\d\dT/.test(b)?Date.parse(a)===Date.parse(b):require('./runtime-utils').same(a,b);
function archiveMembers(collection,root){
 const f=collection.fields,file=fs.realpathSync(f['archive-file']),base=fs.realpathSync(root);
 if(!file.startsWith(base+path.sep)||!/^([a-f0-9]{64})\.json\.gz$/.test(path.basename(file))||path.basename(file)!==f.fingerprint+'.json.gz')throw Error('Live archive path/fingerprint mismatch');
 if(fs.statSync(file).size>16000000)throw Error('Live archive exceeds bounded class size');
 const body=zlib.gunzipSync(fs.readFileSync(file),{maxOutputLength:32000000});
 if(hash(body)!==f.fingerprint)throw Error('Live normalized archive hash mismatch');
 const rows=JSON.parse(body.toString('utf8'));
 if(!Array.isArray(rows)||rows.length!==f['row-count'])throw Error('Live archive count mismatch');
 return rows;
}
function verifyClass({show,now,cls,collection,entries,root}){
 const c=cls.fields,k=collection.fields,date=localDay(now),sid=Number(show.show_id),customer=Number(show.customer_id),cid=Number(c['class-id']);
 const classKey=[sid,date,cid].join('|'),collectionKey=[sid,date,'class',cid].join('|');
 if(!id(cid)||c['class-key']!==classKey||Number(c['show-id'])!==sid||Number(c['customer-id'])!==customer||c.date!==date||c['scope-status']!=='current'||!fresh(c['last-seen'],now))throw Error('Live class is outside fresh TODAY scope');
 if(k['collection-id']!==collectionKey||Number(k['show-id'])!==sid||k.date!==date||k['source-kind']!=='class'||Number(k['source-id'])!==cid||k.status!=='complete'||!c.collection?.includes(collection.id))throw Error('Live class collection is incomplete or mismatched');
 const members=archiveMembers(collection,root);
 if(Number(c.total)!==members.length)throw Error('Live class total differs from normalized archive');
 const observed=entries.filter(r=>r.fields.class?.includes(cls.id));
 if(observed.length!==members.length)throw Error('Live current class membership differs from archive');
 const seen=new Set(),out=[];
 for(const member of members){
  const trip=member['trip-id'],entry=member['entry-id'],horse=member['horse-id'],rider=member['rider-id'];
  if(![trip,entry,horse,rider].every(id)||member._class!==classKey||member['live-entry-key']!==[sid,date,cid,trip].join('|')||member['entry-key']!==[sid,date,entry,cid].join('|')||seen.has(member['live-entry-key']))throw Error('Live normalized member identity invalid or duplicated');
  seen.add(member['live-entry-key']);
  const matches=observed.filter(r=>r.fields['live-entry-key']===member['live-entry-key']);
  if(matches.length!==1)throw Error('Live archive member missing or ambiguous');
  const row=matches[0],f=row.fields;
  if(Number(f['show-id'])!==sid||Number(f['customer-id'])!==customer||f.date!==date||f['scope-status']!=='current'||!f.collection?.includes(collection.id)||!fresh(f['last-seen'],now))throw Error('Live member outside fresh class collection');
  for(const [name,value]of Object.entries(member))if(!name.startsWith('_')&&!equal(f[name],value))throw Error('Live member changed since normalized archive: '+name);
  const identity=JSON.parse(member['source-json']).identity;
  if(!identity||Number(identity.entry_id)!==Number(entry)||Number(identity.horse_id)!==Number(horse)||Number(identity.rider_id)!==Number(rider)||Number(identity.trip_id)!==Number(trip))throw Error('Live normalized source identity mismatch');
  out.push({entryKey:member['entry-key'],entryId:Number(entry),horseId:Number(horse),riderId:Number(rider),tripId:Number(trip),trainerId:member['trainer-id']??null,
   tripUuid:identity.entryxclasses_uuid??null,recordId:row.id,liveEntryKey:member['live-entry-key'],classId:cid,classRecordId:cls.id,collectionId:collection.id,collectionKey:k['collection-id'],
   fingerprint:k.fingerprint,archiveFile:k['archive-file'],sourceAsOf:member['last-seen'],sourceUpdatedAt:member['source-updated-at'],
   ringAssociation:{status:'unresolved',reason:'Ring number not asserted by normalized class member'},groupAssociation:{status:'unresolved',reason:'Class membership does not establish operational group occurrence'}});
 }
 return out;
}
const sourceFields=r=>Object.fromEntries(Object.entries(r.fields).filter(([k])=>k!=='entry'&&!k.startsWith('participation-link-')));
async function readLive(api,show,now,{root=settings.LIVE_ARCHIVE_ROOT}={}){
 const evidence=new Map(),issues=[],date=localDay(now);
 if(date<String(show.start_date).slice(0,10)||date>String(show.end_date).slice(0,10))return{status:'skipped',reason:'TODAY outside show',evidence,issues};
 const tables=[settings.LIVE_ENTRIES,settings.LIVE_CLASSES,settings.LIVE_COLLECTIONS],filter=`AND({show-id}=${Number(show.show_id)},{date}=${JSON.stringify(date)})`;
 try{
  const [entries,classes,collections]=await Promise.all(tables.map(t=>api.list(t,settings.LIVE_BASE_ID,filter)));
  const accepted=[];
  for(const cls of classes){
   try{
    const matching=collections.filter(r=>r.fields['collection-id']===[show.show_id,date,'class',cls.fields['class-id']].join('|'));
    if(matching.length!==1)throw Error('Live collection missing or ambiguous');
    accepted.push(...verifyClass({show,now,cls,collection:matching[0],entries,root}));
   }catch(error){issues.push({classId:cls.fields['class-id'],reason:error.message});}
  }
  if(accepted.length){
   const after=await Promise.all(tables.map(t=>api.list(t,settings.LIVE_BASE_ID,filter)));
   for(const classId of new Set(accepted.map(v=>v.classRecordId))){
    const item=accepted.find(v=>v.classRecordId===classId),cls=after[1].filter(r=>r.id===classId),collection=after[2].filter(r=>r.id===item.collectionId);
    if(cls.length!==1||collection.length!==1)throw Error('Live class/collection changed during read');
    verifyClass({show,now,cls:cls[0],collection:collection[0],entries:after[0],root});
   }
   for(const item of accepted){
    for(const [index,identity]of [[0,item.recordId],[1,item.classRecordId],[2,item.collectionId]]){
     const before=[entries,classes,collections][index].find(r=>r.id===identity),matches=after[index].filter(r=>r.id===identity);
     if(matches.length!==1||JSON.stringify(sourceFields(before))!==JSON.stringify(sourceFields(matches[0])))throw Error('Live evidence changed during read');
    }
    const values=evidence.get(item.entryKey)||[];values.push(item);evidence.set(item.entryKey,values);
   }
  }
  return{status:evidence.size?'verified':'skipped',evidence,issues};
 }catch(error){return{status:'skipped',reason:error.message,evidence:new Map(),issues};}
}
function forParticipation(row,observations){
 if(!observations?.length)return null;
 let trips=row['source-trips'];
 if(!trips){try{trips=JSON.parse(row['source-json']).trips;}catch{return null;}}
 if(!Array.isArray(trips)||!trips.length)return null;
 const matched=observations.filter(v=>v.entryKey===row['entry-key']&&v.entryId===Number(row['entry-id'])&&v.classId===Number(row['class-id'])&&v.horseId===Number(row['horse-id'])&&v.riderId===Number(row['rider-id'])&&trips.some(t=>Number(t.entry_id)===v.entryId&&Number(t.horse_id)===v.horseId&&Number(t.rider_id)===v.riderId&&((id(t.trip_id)&&Number(t.trip_id)===v.tripId)||(!id(t.trip_id)&&t.entryxclasses_uuid&&t.entryxclasses_uuid===v.tripUuid))&&(!t.entryxclasses_uuid||!v.tripUuid||t.entryxclasses_uuid===v.tripUuid)&&(!id(t.trainer_id)||!id(v.trainerId)||Number(t.trainer_id)===Number(v.trainerId))));
 if(matched.length!==observations.length)return null;
 return{status:'verified',coverage:'TODAY-class-membership',observations:matched};
}
module.exports={readLive,verifyClass,forParticipation,localDay};
