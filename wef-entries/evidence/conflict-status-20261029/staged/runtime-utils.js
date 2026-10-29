"use strict";
// Same archive-before-delete and compressed readback pattern as schedules/current-store.
const fs=require('node:fs'),path=require('node:path'),crypto=require('node:crypto'),zlib=require('node:zlib');
async function archiveFile(root,kind,value,scope={}){
 if(!/^[a-zA-Z0-9_-]+$/.test(kind))throw Error('Invalid archive kind');
 const body=Buffer.from(JSON.stringify(value)),sha256=crypto.createHash('sha256').update(body).digest('hex'),file=path.join(root,kind,sha256+'.json.gz');
 fs.mkdirSync(path.dirname(file),{recursive:true});let outcome='Reused';
 if(!fs.existsSync(file)){const tmp=file+'.'+process.pid+'.tmp';fs.writeFileSync(tmp,zlib.gzipSync(body));if(!zlib.gunzipSync(fs.readFileSync(tmp)).equals(body))throw Error('Archive readback failed');fs.renameSync(tmp,file);outcome='Created';}
 if(!zlib.gunzipSync(fs.readFileSync(file)).equals(body))throw Error('Archive collision or corruption');
 return{at:new Date().toISOString(),kind,outcome,file,records:Array.isArray(value)?value.length:1,sha256,...scope};
}

function acquireLock(file){fs.mkdirSync(path.dirname(file),{recursive:true});try{const fd=fs.openSync(file,'wx');fs.writeFileSync(fd,String(process.pid));fs.closeSync(fd);}catch(e){if(e.code!=='EEXIST')throw e;const pid=Number(fs.readFileSync(file,'utf8'));if(!Number.isInteger(pid)||pid<=0)throw Error('Invalid Entries linker lock');try{process.kill(pid,0);throw Error('Entries linker already running');}catch(probe){if(probe.code!=='ESRCH')throw probe;fs.unlinkSync(file);return acquireLock(file);}}return()=>fs.unlinkSync(file);}

const same=(a,b)=>JSON.stringify(a==null||a===''?null:a)===JSON.stringify(b==null||b===''?null:b)||((a==null||a==='')&&(b===false||Array.isArray(b)&&!b.length));
function unique(rows,key){const seen=new Set();for(const r of rows){const k=r.fields[key];if(k==null||k===''||seen.has(k))throw Error('Missing or duplicate '+key);seen.add(k);}}

module.exports={archiveFile,acquireLock,same,unique};
