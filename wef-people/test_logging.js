'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),os=require('node:os'),path=require('node:path');
const adapter=require('./logging'),log=require('../wef-data/attempt-log');
test('Every mismatch retains source context within existing redaction bound',()=>{
 const issues=Array.from({length:853},(_,n)=>({category:'Unresolved',target:'w-riders/rec'+n,message:'Missing historical day'}));
 const grouped=adapter.grouped(issues);
 const targets=grouped.flatMap(g=>JSON.parse(g.message.split('; source-context=')[1]));
 assert.deepEqual(targets,issues.map(i=>i.target));assert(grouped.every(g=>g.message.length<2000));
});
test('Existing logging retains partial attempt and errors when remote delivery fails',async()=>{
 const root=fs.mkdtempSync(path.join(os.tmpdir(),'wef-people-log-'));
 const config={base:'app6uHgUYCCKTVlJE',workflow:'wef-people',tables:{'run-logs':'tblRun','err-logs':'tblErr',heartbeat:'tblHeart'}};
 const attempt=log.begin(root,config);
 log.recordError(root,attempt.id,{category:'MissingRelationship',message:'Horse relationships absent'});
 log.finish(root,attempt.id,{status:'Partial',stage:'JoinReadback',counts:{added:1}});
 const result=await log.flush(root,config,async()=>{throw Error('Offline fixture failure')});
 assert.equal(result.ok,false);assert.equal(log.readAttempt(root,attempt.id).status,'Partial');
 assert.equal(fs.readdirSync(path.join(root,'err-logs')).length,1);
 // Temporary fixture intentionally retained; no production directories touched.
});
