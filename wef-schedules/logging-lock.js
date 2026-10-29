'use strict';
const fs=require('node:fs'),path=require('node:path'),crypto=require('node:crypto'),{AsyncLocalStorage}=require('node:async_hooks');
function loggingLock(file,{timeoutMs=30000,pause=ms=>new Promise(r=>setTimeout(r,ms))}={}){
 const context=new AsyncLocalStorage();
 return async function serialize(work){
  if(context.getStore())return work();
  fs.mkdirSync(path.dirname(file),{recursive:true});const token=crypto.randomUUID(),started=Date.now();
  for(;;){
   try{const fd=fs.openSync(file,'wx');try{fs.writeFileSync(fd,JSON.stringify({pid:process.pid,token}));}finally{fs.closeSync(fd);}break;}
   catch(error){
    if(error.code!=='EEXIST')throw error;
    let owner;try{owner=JSON.parse(fs.readFileSync(file,'utf8'));}catch(readError){if(readError.code==='ENOENT')continue;if(readError instanceof SyntaxError&&Date.now()-started<timeoutMs){await pause(50);continue;}throw Error('Unreadable logging lock; inspect before clearing');}
    if(!Number.isInteger(owner.pid)||owner.pid<=0||typeof owner.token!=='string')throw Error('Invalid logging lock; inspect before clearing');
    try{process.kill(owner.pid,0);}catch(probe){if(probe.code==='ESRCH')throw Error('Dead logging lock; requires inspection, not automatic clearing');if(probe.code!=='EPERM')throw probe;}
    if(Date.now()-started>=timeoutMs)throw Error('Attempt logging lock wait exceeded');
    await pause(50);
   }
  }
  try{return await context.run(token,work);}
  finally{const owner=JSON.parse(fs.readFileSync(file,'utf8'));if(owner.token!==token)throw Error('Logging lock ownership changed');fs.unlinkSync(file);}
 };
}
module.exports={loggingLock};
