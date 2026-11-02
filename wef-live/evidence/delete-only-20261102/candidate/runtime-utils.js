"use strict";
// Local utility behavior preserved from the deployed baseline. No collector imports.
const fs=require("node:fs"),path=require("node:path");
const same=(a,b)=>JSON.stringify(a==null||a===''?null:a)===JSON.stringify(b==null||b===''?null:b)||((a==null||a==='')&&(b===false||Array.isArray(b)&&!b.length));
function unique(rows,key){const seen=new Set();for(const r of rows){const k=r.fields[key];if(k==null||k===''||seen.has(k))throw Error('Missing or duplicate '+key);seen.add(k);}}
function acquireLock(file){fs.mkdirSync(path.dirname(file),{recursive:true});try{const fd=fs.openSync(file,'wx');fs.writeFileSync(fd,String(process.pid));fs.closeSync(fd);}catch(e){if(e.code!=='EEXIST')throw e;const pid=Number(fs.readFileSync(file,'utf8'));if(!Number.isInteger(pid)||pid<=0)throw Error('Invalid live collector lock');try{process.kill(pid,0);throw Error('Live collector already running');}catch(probe){if(probe.code!=='ESRCH')throw probe;fs.unlinkSync(file);return acquireLock(file);}}return()=>fs.unlinkSync(file);}

module.exports={same,unique,acquireLock};
