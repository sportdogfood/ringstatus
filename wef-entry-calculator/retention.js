'use strict';
const {instant}=require('../wef-timing/class-timing');
const {localDay,validDay}=require('../wef-timing/purge');
const {hash}=require('../wef-timing/pace');
// Planning only. No deletion transport is exposed until shared archival release.
// Protect all current/future days, malformed records, and explicit sync references.
function plan({rows,now,protectedIds=[]}){
 if(instant(now)===null)throw Error('Retention clock invalid');
 const day=localDay(now),protectedSet=new Set(protectedIds);
 const eligible=rows.filter(r=>{
  const f=r.fields,at=instant(f?.['evaluated-at']);
  return /^rec[A-Za-z0-9]{14}$/.test(r.id)&&!protectedSet.has(r.id)&&f?.['answer-kind']==='entry-timing'
   &&f['source-lane']==='data_live'&&validDay(f.date)&&f.date<day&&at!==null&&instant(now)-at>=86400000;
 });
 const manifest={policy:'entry-archive-before-delete-v1',base:'appyoLTcNmLrgpHcg',table:'tblg0E317kTVI71Wn',records:eligible};
 return{manifest,sha256:hash(manifest),deletion_enabled:false,reason:'shared_verified_archive_release_required'};
}
module.exports={plan};
