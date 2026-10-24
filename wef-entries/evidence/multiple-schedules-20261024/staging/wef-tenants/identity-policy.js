"use strict";
const columns={horses:['horse-key','horse-id','horse-name','name','nick-name','show-name'],riders:['rider-key','rider-id','rider-name','name','nick-name']};
const empty=v=>v==null||String(v).trim()==='';
function canonical(field,value){
 if(field.endsWith('-id')){const n=Number(value);if(!Number.isSafeInteger(n)||n<=0)throw Error('Invalid '+field);return n;}
 if(field.endsWith('-key'))return String(value);
 return String(value).normalize('NFKC').trim().replace(/\s+/g,' ').toLocaleLowerCase('en-US');
}
function assertUniqueRoster(table,rows){
 if(!columns[table])throw Error('Unsupported identity table '+table);
 const key=columns[table][0],seen=new Map(columns[table].map(f=>[f,new Map()]));
 for(const row of rows){
  if(empty(row[key]))throw Error('Missing '+key);
  for(const field of columns[table]){
   if(empty(row[field]))continue;
   const value=canonical(field,row[field]),values=seen.get(field);
   if(values.has(value))throw Error('Duplicate '+table+'.'+field+' '+JSON.stringify(row[field])+' between '+values.get(value)+' and '+row[key]);
   values.set(value,row[key]);
  }
 }
}
function assertProspectiveRoster(table,key,before,wanted){
 if(!columns[table])return;
 if(key!==columns[table][0])throw Error('Invalid roster identity key '+key);
 assertUniqueRoster(table,before.map(r=>r.fields));
 const merged=new Map(before.map(r=>[r.fields[key],{...r.fields}]));
 const pending=new Set();
 for(const fields of wanted){
  if(empty(fields[key])||pending.has(fields[key]))throw Error('Missing or duplicate '+key);
  if(table==='riders'&&Object.hasOwn(fields,'show-name'))throw Error('Riders do not use show-name');
  pending.add(fields[key]);merged.set(fields[key],{...merged.get(fields[key]),...fields});
 }
 assertUniqueRoster(table,[...merged.values()]);
}
module.exports={columns,assertUniqueRoster,assertProspectiveRoster};
