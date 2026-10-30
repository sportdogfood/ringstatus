const fs=require('node:fs'),path=require('node:path'),vm=require('node:vm'),assert=require('node:assert/strict');
const root=__dirname,testFile=path.join(root,'staged/test/publication.test.js');
const context={require:require('node:module').createRequire(testFile),structuredClone,module:{exports:{}}};
vm.runInNewContext(fs.readFileSync(testFile,'utf8').split("test('seven existing targets")[0]+'\nmodule.exports={fixture,publish};',context);
const {fixture,publish}=context.module.exports;
const {journal}=require('./staged/publication-journal');
async function check(phase,afterPersist){
 const folder=path.join(root,'reviewer-journal-'+phase+'-'+afterPersist);fs.mkdirSync(folder,{recursive:true});
 const f=fixture();let j=journal(folder),failed=false;
 f.state.data=j.data;
 const list=f.api.list;f.api.list=async(l,...args)=>{if(l==='schedules')throw Error('offline other lane isolation');return list(l,...args);};
 f.state.save=()=>{j.data.data=f.state.data.data;j.data.schedules=f.state.data.schedules;
  const trigger=phase==='intent'?Object.keys(j.data.data.pending).length>0:f.writes.length>0&&Object.keys(j.data.data.pending).length===0;
  if(trigger&&!failed){failed=true;if(afterPersist)j.save();throw Error('reviewer persistence acknowledgement failure');}j.save();};
 const first=await publish(f.args);assert(failed);
 assert.equal(first.lanes.data.patched,phase==='intent'?0:1);
 assert.equal(first.lanes.data.deferred,phase==='intent'?5:4);
 assert.equal(first.lanes.data.uncertain,0);
 j=journal(folder);f.state.data=j.data;f.state.save=()=>j.save();
 const second=await publish(f.args);assert.equal(second.lanes.data.pending,0);assert.equal(f.writes.length,5);
 assert.equal((await publish(f.args)).lanes.data.unchanged,5);assert.equal(f.writes.length,5);
 return {phase,afterPersist,first:first.lanes.data,second:second.lanes.data};
}
(async()=>{const results=[];for(const phase of ['intent','completion'])for(const persisted of [false,true])results.push(await check(phase,persisted));
 const f=fixture(),list=f.api.list;f.state.data.data.cursor=2;f.api.list=async(l,...a)=>{if(l==='schedules')throw Error('offline lane isolation');return list(l,...a);};
 const originalNow=Date.now,originalCompare=String.prototype.localeCompare;let now=1000;
 try{Date.now=()=>now;String.prototype.localeCompare=function(...a){now=200000;return originalCompare.apply(this,a);};
  const r=await publish(f.args);assert.equal(r.lanes.data.planned,5);assert.equal(r.lanes.data.attempted,0);assert.equal(r.lanes.data.deferred,5);assert.equal(f.state.data.data.cursor,2);assert.equal(f.writes.length,0);results.push({phase:'deadline during planning before first candidate',stats:r.lanes.data,cursor:f.state.data.data.cursor});
 }finally{Date.now=originalNow;String.prototype.localeCompare=originalCompare;}
 fs.writeFileSync(path.join(root,'reviewer-offline-results.json'),JSON.stringify({result:'PASS',cases:results},null,2));console.log('PASS: four real-journal restart failure scenarios, each followed by unchanged repeat.');
})().catch(e=>{console.error(e);process.exitCode=1;});
