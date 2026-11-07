import assert from 'node:assert/strict';
function queue({now,sleep,persist,notBefore=0}){
 let tail=Promise.resolve();
 return run=>{
  const result=tail.then(async()=>{
   const wait=Math.max(0,notBefore-now());if(wait)await sleep(wait);
   try{return await run();}finally{notBefore=now()+1000;await persist(notBefore);}
  });
  tail=result.then(()=>{},()=>{});return result;
 };
}
let clock=0,saved=0,active=0,max=0;
const events=[];
const q=queue({now:()=>clock,sleep:async ms=>{clock+=ms;},persist:async t=>{saved=t;}});
const results=await Promise.allSettled([1,2,3].map(id=>q(async()=>{
 active++;max=Math.max(max,active);events.push({id,start:clock});await Promise.resolve();clock+=25;active--;events.at(-1).end=clock;if(id===2)throw Error('Expected failed request');return id;
})));
assert.equal(max,1);assert.equal(results[1].status,'rejected');assert.equal(results[2].value,3);
for(let i=1;i<events.length;i++)assert.equal(events[i].start-events[i-1].end,1000);
const restarted=queue({now:()=>clock,sleep:async ms=>{clock+=ms;},persist:async()=>{},notBefore:saved});
let restartStart;await restarted(async()=>{restartStart=clock;});assert.equal(restartStart-events.at(-1).end,1000);
console.log(JSON.stringify({proposalOnly:true,productionChanged:false,checks:'PASS',concurrentCallers:3,maxConcurrentSourceRequests:max,gapMs:1000,failureDoesNotStallQueue:true,restartTimestampRespected:true,virtualClock:true,events}));
