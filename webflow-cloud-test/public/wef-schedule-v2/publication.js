const text=v=>String(v??'').trim().toLowerCase();
// Same precedence as the approved schedule-data publication formulas.
export function publicationStatus(source,entryCount) {
  const flag=text(source?.['rs-flag']),sequence=text(source?.schedule_sequencetype);
  if(['absent','dropped'].includes(text(source?.['record-status']))||flag==='no entries'||
    (text(source?.kind)==='class'&&entryCount===0))return 'Hidden';
  if(text(source?.kind)==='break')return 'Optional: breaks';
  if((text(source?.['rs-lever'])==='lever1'&&source?.['rs-priority']===2)||flag==='second trip'||
    flag==='flat'||sequence==='under saddle/flat'||flag==='model'||sequence==='model'||
    (flag==='classic'&&text(source?.class_type)!=='jumpers'))return 'Trip detail only';
  if(['schooling pony','add back','mulligan','warmup'].includes(flag))return 'Optional: classes';
  return 'Visible';
}
export function publishedSchedule(records,{showBreaks=false,showOptionalClasses=false}={}) {
  return records.filter(r=>r.publication ? r.publication==='Visible'||
    (showBreaks&&r.publication==='Optional: breaks')||
    (showOptionalClasses&&r.publication==='Optional: classes') : r.kind!=='class'||r.entryCount!==0);
}
