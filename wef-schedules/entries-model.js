"use strict";
// Pure Schedules-owned adaptation of data_contract.directory_page and the
// proven decoded-day-ring-class validator. No Data or other collector imports.
const positive=(n,label)=>{if(!Number.isSafeInteger(n)||n<=0)throw Error('Invalid '+label);return n;};
const day=v=>{const d=String(v||'').slice(0,10);if(!/^\d{4}-\d{2}-\d{2}$/.test(d)||!Number.isFinite(Date.parse(d))||new Date(d).toISOString().slice(0,10)!==d)throw Error('Invalid entry source date');return d;};
function directoryPage(p,show,requested,previous){
 for(const k of ['entries','total_entries','records_per_page','page','show_id','sort','term'])if(!Object.hasOwn(p,k))throw Error('Missing directory '+k);
 const {total_entries:total,records_per_page:size,page}=p;
 if(![total,size,page].every(Number.isSafeInteger)||total<0||size<=0||page!==requested||Number(p.show_id)!==show)throw Error('Invalid directory scope/paging');
 if(p.sort?.sort_on!=='number'||p.sort?.sort_type!=='asc'||p.term!==''||!Array.isArray(p.entries))throw Error('Invalid directory sort/filter/entries');
 const prior=previous||{total,size,next:1,ids:[]};if(prior.next!==requested||prior.total!==total||prior.size!==size)throw Error('Directory changed during collection');
 const pages=Math.max(1,Math.ceil(total/size));if(requested<1||requested>pages)throw Error('Unexpected directory page');
 const wanted=total===0?0:requested<pages?size:total-size*(pages-1);if(p.entries.length!==wanted)throw Error('Directory page count mismatch');
 const ids=[...prior.ids],seen=new Set(ids);
 for(const r of p.entries){if(Number(r.show_id)!==show||r.customer_id!==15)throw Error('Directory entry context mismatch');positive(r.entry_id,'entry ID');if(seen.has(r.entry_id))throw Error('Duplicate directory entry');seen.add(r.entry_id);ids.push(r.entry_id);}
 if(requested===pages&&ids.length!==total)throw Error('Directory unique count mismatch');
 return{total,size,next:requested+1,ids,complete:requested===pages};
}
function inventory(payloads,show){
 const classes=new Map();
 for(const [date,p] of payloads){
  if(Number(p.show?.show_id??p.show_id)!==show||day(p.show_date??p.showDate)!==date||!Array.isArray(p.rings))throw Error('Entry inventory context mismatch');
  for(const r of p.rings){positive(r.ring_number,'ring number');if(!Array.isArray(r.classes))throw Error('Missing source class inventory');
   for(const c of r.classes){if(c.schedule_break)continue;const id=positive(c.class_id,'decoded class ID'),prior=classes.get(id);if(prior&&(prior.date!==date||prior.ring!==r.ring_number))throw Error('Class appears in conflicting date/ring scopes');
    classes.set(id,{id,date,ring:r.ring_number,ringId:r.ring_id??null,number:c.class_number??null,name:c.class_name||'',groupIds:[...new Set([...(prior?.groupIds||[]),c.class_group_id].filter(x=>x!=null))]});
   }
  }
 }
 return[...classes.values()].sort((a,b)=>a.date.localeCompare(b.date)||a.ring-b.ring||a.id-b.id);
}
function classTrips(p,scope,show){
 if(Number(p.show_id)!==show||p.class?.class_id!==scope.id||day(p.class_related_data?.date)!==scope.date||Number(p.class_related_data?.ring)!==scope.ring)throw Error('Decoded class context changed');
 if(!Array.isArray(p.trips)||!Number.isSafeInteger(p.total_entry_trips)||p.total_entry_trips!==p.trips.length)throw Error('Incomplete class trips');
 const ids=new Set();for(const t of p.trips){for(const k of ['entry_id','horse_id','rider_id'])positive(t[k],k);if(t.class_id!=null&&Number(t.class_id)!==scope.id)throw Error('Trip class conflict');
  const identity=t.entryxclasses_uuid||t.trip_id;if(typeof identity!=='string'&&typeof identity!=='number'||!String(identity).trim()||String(identity)!==String(identity).trim()||(!t.entryxclasses_uuid&&(!Number.isSafeInteger(Number(identity))||Number(identity)<=0)))throw Error('Missing source trip identity');
  const key=String(identity);if(ids.has(key))throw Error('Duplicate class source trip identity');ids.add(key);
 }
 return p.trips;
}
function link(schedules,show,scope){const rows=schedules.filter(r=>r.fields.status==='active'&&Number(r.fields['show-id'])===show&&r.fields.date===scope.date&&Number(r.fields.ring)===scope.ring&&Number(r.fields['class-id'])===scope.id);
 return rows.length===1?{status:'linked',ids:[rows[0].id],keys:JSON.stringify([rows[0].fields['stage-key']])}:{status:rows.length?'Conflict':'unresolved',reason:rows.length?'Multiple schedule occurrences for this class/date/ring':'No matching active schedule occurrence'};
}
module.exports={directoryPage,inventory,classTrips,link,positive,day};
