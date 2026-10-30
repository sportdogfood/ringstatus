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
module.exports={archivedRow};
