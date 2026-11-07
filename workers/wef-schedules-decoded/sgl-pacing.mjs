export function createPacer({notBefore=0,now=Date.now,sleep=ms=>new Promise(resolve=>setTimeout(resolve,ms)),persist}){
 if(!Number.isFinite(notBefore)||notBefore<0)throw Error('Invalid pacing timestamp');
 let tail=Promise.resolve(),storageFailed=false;
 return run=>{
  const result=tail.then(async()=>{
   if(storageFailed)throw Error('Pacing storage unavailable');
   while(now()<notBefore)await sleep(notBefore-now());
   try{return await run({startedAt:now()});}
   finally{
    notBefore=now()+1000;
    try{await persist(notBefore);}catch(error){storageFailed=true;throw error;}
   }
  });
  tail=result.then(()=>{},()=>{});
  return result;
 };
}
