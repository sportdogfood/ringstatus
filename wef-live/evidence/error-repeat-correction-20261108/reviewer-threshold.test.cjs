'use strict';
const {test}=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),{createRequire}=require('node:module');
const fixtureText=fs.readFileSync(path.join(__dirname,'cleanup.test.cjs'),'utf8').split('const {dailyClearDay}=')[0];
const setup=Function('require',fixtureText+'\nreturn setup;')(createRequire(path.join(__dirname,'cleanup.test.cjs')));
const {cleanup}=require('./candidate/wef-live/cleanup');
test('review: one invocation consumes all excess through adapter calls no larger than500',async t=>{
 const a=setup(t);a.rows('run-logs',1501);const retire=a.options.api.retireCleanup,sizes=[];
 a.options.api.retireCleanup=async(table,rows,...rest)=>{assert(rows.length<=500);sizes.push(rows.length);return retire(table,rows,...rest);};
 const r=await cleanup(a.options);assert.equal(r.result,'Success');assert.deepEqual(sizes,[500,500,1]);assert.equal(a.db['run-logs'].length,500);assert.equal(r.actions[0].deleted,1001);
});
test('review: second chunk failure stops the same invocation and preserves actual deletion count',async t=>{
 const a=setup(t);a.rows('run-logs',1800);const retire=a.options.api.retireCleanup;let calls=0;
 a.options.api.retireCleanup=async(table,rows,...rest)=>{if(++calls===2)throw Error('offline second-chunk failure');return retire(table,rows,...rest);};
 const r=await cleanup(a.options);assert.equal(r.result,'Error');assert.equal(calls,2);assert.equal(a.db['run-logs'].length,1300);assert.equal(r.actions[0].deleted,500);assert.equal(r.actions[0].result,'Error');assert(fs.existsSync(path.join(a.runtime,'cleanup-stop.json')));
});
