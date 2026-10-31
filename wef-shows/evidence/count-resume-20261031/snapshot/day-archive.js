"use strict";
// Read existing immutable Data evidence only; never publish or repair archives.
const fs=require('node:fs'),path=require('node:path'),crypto=require('node:crypto'),zlib=require('node:zlib');
const hash=value=>crypto.createHash('sha256').update(value).digest('hex');
function archivedRow(root,ref,showKey){
 const proof=ref.archive;
 if(!/^\d+\|\d+$/.test(showKey)||ref['record-id']!=null||proof?.format!=='wef-data-evidence-v1')throw Error('Invalid archive reference');
 const prefix='wef-data/shows/'+showKey.replace('|','/')+'/retained-evidence/parts/';
 if(proof.path!==prefix+ref.sha256+'.json.gz')throw Error('Archive path outside show scope');
 const base=fs.realpathSync(root),file=fs.realpathSync(path.join(base,proof.path)),relative=path.relative(base,file);
 if(relative==='..'||relative.startsWith('..'+path.sep)||path.isAbsolute(relative))throw Error('Archive path escapes scope');
 const bytes=fs.readFileSync(file);
 if(bytes.length!==proof.bytes||hash(bytes)!==proof.sha256)throw Error('Archive object hash/length mismatch');
 const part=JSON.parse(zlib.gunzipSync(bytes));
 if(part['retained-sha256']!==ref.sha256||hash(part['retained-json'])!==ref.sha256)throw Error('Archive retained payload mismatch');
 return{...part,'collection-key':ref.key,scope:ref.scope,status:'Verified retained observation',_archive:proof};
}
function historyArchive(root,receipt,head,showKey){
 const state=JSON.parse(head.fields['state-json']),prefix='wef-data/shows/'+showKey.replace('|','/')+'/retained-evidence',sha=hash(head.fields['state-json']);
 const manifestName=prefix+'/revisions/'+sha+'.json';
 if(receipt?.format!=='wef-data-evidence-v1'||!receipt.verified_remote_commit||receipt.manifest!==manifestName||receipt['state-sha256']!==sha)throw Error('Historical archive receipt mismatch');
 const base=fs.realpathSync(root);
 function file(name){if(typeof name!=='string'||name.includes('\\')||path.posix.isAbsolute(name)||name.split('/').includes('..'))throw Error('Historical archive path escapes scope');const target=fs.realpathSync(path.join(base,name)),relative=path.relative(base,target);if(relative==='..'||relative.startsWith('..'+path.sep)||path.isAbsolute(relative))throw Error('Historical archive path escapes scope');return fs.readFileSync(target);}
 const manifest=JSON.parse(file(manifestName));
 if(manifest.format!=='wef-data-evidence-v1'||manifest['show-key']!==showKey||manifest['run-key']!==head.fields['run-key']||manifest['state-sha256']!==sha||JSON.stringify(manifest.authority)!==JSON.stringify(state.authority))throw Error('Historical archive head mismatch');
 const loaded=new Map();
 function read(kind,key){
  const cache=kind+'|'+key;if(loaded.has(cache))return loaded.get(cache);
  const entry=manifest.records[kind]?.[key];if(!entry)throw Error('Historical archive key missing');const proof=manifest.files[entry.path];
  if(!proof||entry.path!==prefix+'/'+kind+'/'+proof.sha256+'.json.gz')throw Error('Historical object outside show scope');
  const bytes=file(entry.path);if(bytes.length!==proof.bytes||hash(bytes)!==proof.sha256)throw Error('Historical archive object changed');
  const row=JSON.parse(zlib.gunzipSync(bytes)),field=kind==='runs'?'run-key':'collection-key';
  if(row[field]!==key||row['_record-id']!==entry['record-id'])throw Error('Historical archive key/native ID mismatch');
  if(kind==='runs'){const s=JSON.parse(row['state-json']);if(row.status!=='Verified completed revision'||s.phase!=='Complete'||s['show-key']!==showKey||s['run-record-id']!==entry['record-id'])throw Error('Historical run not accepted');}
  else if(hash(row['retained-json'])!==row['retained-sha256'])throw Error('Historical collection payload changed');
  loaded.set(cache,row);return row;
 }
 const current=read('runs',head.fields['run-key']);if(current['_record-id']!==head.id||current['state-json']!==head.fields['state-json'])throw Error('Historical native head changed');
 return{manifest,run:key=>read('runs',key),collection:key=>read('legacy-collections',key)};
}
module.exports={archivedRow,historyArchive};
