import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import {createPacer} from '../sgl-pacing.mjs';

test('three simultaneous agents run serially with one second after each completion',async()=>{
 let clock=0,active=0,max=0,saved=0;const events=[];
 const q=createPacer({now:()=>clock,sleep:async ms=>{clock+=ms;},persist:async value=>{saved=value;}});
 const results=await Promise.allSettled([1,2,3].map(id=>q(async()=>{
  active++;max=Math.max(max,active);const event={id,start:clock};events.push(event);await Promise.resolve();clock+=50;event.end=clock;active--;if(id===2)throw Error('Expected source error');return id;
 })));
 assert.equal(max,1);assert.equal(results[1].status,'rejected');assert.equal(results[2].value,3);
 for(let i=1;i<events.length;i++)assert.equal(events[i].start-events[i-1].end,1000);
 assert.equal(saved,events[2].end+1000);
});
test('stored completion survives a new queue and early timer wake cannot shorten pause',async()=>{
 let clock=100,sleeps=0;const q=createPacer({notBefore:1100,now:()=>clock,sleep:async ms=>{sleeps++;clock+=sleeps===1?ms-1:ms;},persist:async()=>{}});
 const started=await q(async()=>clock);assert.equal(started,1100);assert.equal(sleeps,2);
});
test('real timer enforces one second between two request bodies',async()=>{
 const q=createPacer({persist:async()=>{}});let firstEnd,secondStart;
 await Promise.all([q(async()=>{firstEnd=Date.now();}),q(async({startedAt})=>{secondStart=startedAt;})]);
 assert.ok(secondStart-firstEnd>=1000);
});
test('storage failure blocks further source work instead of silently removing pacing',async()=>{
 let calls=0;const q=createPacer({persist:async()=>{throw Error('storage offline');}});
 await assert.rejects(()=>q(async()=>{calls++;}),/storage offline/);
 await assert.rejects(()=>q(async()=>{calls++;}),/Pacing storage unavailable/);assert.equal(calls,1);
 assert.throws(()=>createPacer({notBefore:NaN,persist:async()=>{}}),/Invalid pacing timestamp/);
});
test('all Worker instances use the same object name; absent queue fails closed',async()=>{
 const source=fs.readFileSync(new URL('../shared-worker.mjs',import.meta.url),'utf8');
 const outer=source.slice(source.lastIndexOf('export default'));
 const app=new Function('sourceHandler','reply',outer.replace('export default','return'))({fetch:async()=>new Response('method rejected',{status:405})},(data,status)=>Response.json(data,{status}));
 const names=[],forwarded=[];const env={SGL_PACER:{idFromName(name){names.push(name);return 'one-object';},get(id){assert.equal(id,'one-object');return{fetch:async request=>{forwarded.push(request);return new Response('paced');}};}}};
 await Promise.all([1,2,3].map(()=>app.fetch(new Request('https://worker.example/schedule'),env)));
 assert.deepEqual(names,['sgl-source','sgl-source','sgl-source']);assert.equal(forwarded.length,3);
 assert.equal((await app.fetch(new Request('https://worker.example/schedule'),{})).status,503);
 assert.equal((await app.fetch(new Request('https://worker.example/schedule',{method:'POST'}),env)).status,405);
});
