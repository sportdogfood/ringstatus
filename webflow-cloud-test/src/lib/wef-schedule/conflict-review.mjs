import {APP_BASE,equals,recordId} from './shared-base.mjs';

// Plain review text only. This does not resolve conflicts or change publication.
export async function conflictReviewFields(api, conflict) {
  const d=conflict.details??{}, key=conflict.recordKey??'', parts=key.split('|');
  let table, row, label;
  if(d.source==='wef-data.schedules'&&recordId(d.sourceRecord)) {
    table='tbljK5BImDJnQcNB4';row={id:d.sourceRecord};label=['Schedule',d.name].filter(Boolean).join(' - ');
  } else if(d.source==='wef-data.trips') {
    table='tbl9dATHFaF8z0x0I';[row]=await api.read('trips',equals('trip-key',key));
    let raw;try{raw=typeof row?.['source-json']==='string'?JSON.parse(row['source-json']):row?.['source-json'];}catch{}
    label=raw?.number?`Trip — entry ${raw.number}`:'Trip';
  } else if(d.source==='wef-data.classes'||parts.length===5) {
    table='tblL2BwNM2Z8sdGDQ';[row]=await api.read('classes',equals('class-key',key));
    label=['Class',row?.['class-number'],row?.['class-name']].filter(Boolean).join(' — ');
  } else if(parts.length===3) {
    table='tblzRTVjHbR8HvMP1';[row]=await api.read('days',equals('show-day-key',key));
    label=`Schedule — ${conflict.date??parts[2]}`;
  } else if(parts.length===4) {
    const matches=await api.read('rings',equals('ring-number',Number(parts[3])));
    const scoped=matches.filter(r=>r['ring-key']?.startsWith(parts[0]+'|'));
    if(scoped.length===1)row=scoped[0];
    table='tblst0MKROc3puhm5';label=row?.['ring-nickname']||row?.name||`Ring ${parts[3]}`;
  }
  label??='Schedule record';
  const escaped=label.replace(/[\\\[\]*]/g,'\\$&');
  const record=recordId(row?.id)?`[${escaped}](https://airtable.com/${APP_BASE}/${table}/${row.id})`:label;
  const problems=[];
  if(conflict.reason==='shared_participant_conflict') {
    const kind=String(d.field??'participant'), title=kind[0].toUpperCase()+kind.slice(1);
    const ids=String(d.linked??'').split(/[,\s]+/).filter(Boolean);
    problems.push(ids.length>1?`${title} field lists multiple ${kind}s.`:`${title} field differs from the trip details.`);
    if(d.snapshot!=null)problems.push(`Trip details identify ${kind} ${d.snapshot}.`);
  } else if(conflict.reason==='conflicting_shared_group_identity')problems.push('The same group has different names.');
  else if(conflict.reason==='incomplete_shared_class_trips')problems.push('Trip details are incomplete.');
  else if(conflict.reason==='older_shared_dataset')problems.push('The source update time is older than the saved data.');
  else if(conflict.reason==='invalid_shared_class') {
    if(!row?.['source-json'])problems.push('Class details are empty.');
    else {
      if(d.snapshotShowId==null)problems.push('Show is missing from the class details.');
      if(d.snapshotClassId==null)problems.push('Class ID is missing from the class details.');
      else if(d.snapshotClassId!==d.classId)problems.push('Class ID differs from the class details.');
    }
    if(d.entryCount==null)problems.push('Entry count is missing.');
    else if(!Number.isSafeInteger(d.entryCount)||d.entryCount<0)problems.push('Entry count is invalid.');
  } else if(conflict.reason==='missing_shared_class')problems.push('Some scheduled classes are missing usable details.');
  else if(conflict.reason.startsWith('unconfirmed_empty_day:'))problems.push('No schedule records were found for this day.');
  else if(conflict.reason==='invalid_shared_json')problems.push('Saved source details cannot be read.');
  else if(conflict.reason.startsWith('conflicting_shared_'))problems.push('Related records disagree about this record.');
  else problems.push('Related schedule data needs review.');
  if(!problems.length)problems.push('Class details do not match the scheduled class.');
  return {Record:record,'Problem 1':problems[0],'Problem 2':problems[1]??null,'Problem 3':problems[2]??null};
}
