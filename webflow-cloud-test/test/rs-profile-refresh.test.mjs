import test from 'node:test';
import assert from 'node:assert/strict';
import {startProfileRefresh,REFRESH_INTERVAL} from '../public/rs-profile-v3/refresh.js';

test('refresh waits eight minutes, never overlaps, survives failure and stops on page exit',async()=>{
 let callback,delay,resolve,calls=0,cleared=false;
 const stop=startProfileRefresh({ready:()=>true,refresh:()=>{calls++;return new Promise(r=>resolve=r);},setTimer:(fn,ms)=>{callback=fn;delay=ms;return 1;},clearTimer:()=>{cleared=true;}});
 assert.equal(delay,480000);assert.equal(REFRESH_INTERVAL,480000);assert.equal(calls,0);
 const tick=callback();assert.equal(calls,1);stop();resolve();await tick;assert.equal(cleared,false);
 let scheduled=0;
 const cancel=startProfileRefresh({ready:()=>true,refresh:async()=>{throw Error('offline');},setTimer:fn=>{callback=fn;scheduled++;return 2;},clearTimer:()=>{cleared=true;}});
 await callback();assert.equal(scheduled,2);cancel();assert.equal(cleared,true);
});
test('signed-out pages do not refresh private data',async()=>{
 let callback,calls=0;
 const stop=startProfileRefresh({ready:()=>false,refresh:()=>calls++,setTimer:fn=>{callback=fn;return 1;},clearTimer:()=>{}});
 await callback();assert.equal(calls,0);stop();
});
