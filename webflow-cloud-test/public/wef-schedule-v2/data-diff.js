// Compare saved server values, not fetch timestamps. Local edits/preferences are separate.
export function fingerprint(value){
 if(Array.isArray(value))return '['+value.map(fingerprint).join(',')+']';
 if(value&&typeof value==='object')return '{'+Object.keys(value).sort().map(k=>JSON.stringify(k)+':'+fingerprint(value[k])).join(',')+'}';
 return JSON.stringify(value);
}
export function diffRecords(previous,incoming,key='id'){
 const index=rows=>{
  const map=new Map();
  for(const row of rows){if(!row||typeof row[key]!=='string'||!row[key]||map.has(row[key]))throw Error('invalid_record_identity');map.set(row[key],row);}
  return map;
 };
 const before=index(previous),after=index(incoming),added=[],updated=[],removed=[];
 const records=incoming.map(row=>{
  const old=before.get(row[key]);
  if(!old){added.push(row[key]);return row;}
  if(fingerprint(old)!==fingerprint(row)){updated.push(row[key]);return row;}
  return old;
 });
 for(const id of before.keys())if(!after.has(id))removed.push(id);
 const reordered=previous.map(r=>r[key]).join('\0')!==incoming.map(r=>r[key]).join('\0');
 return {records,added,updated,removed,reordered,changed:!!(added.length||updated.length||removed.length||reordered)};
}
