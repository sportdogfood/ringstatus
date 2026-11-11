'use strict';
const assert=require('node:assert/strict'),fs=require('node:fs'),os=require('node:os'),path=require('node:path');
const {choose,pruneLocal}=require('./candidate/cleanup-lane');
const rows=n=>Array.from({length:n},(_,i)=>({id:String(i),createdTime:new Date(1700000000000+i*1000).toISOString()})).reverse();
for(const total of [999,1000,1001,1421,1700,2500]){
 const selected=choose(rows(total),()=>true,'log-check');assert.equal(selected.length,total<1000?0:total-500);if(total>=1000)assert.equal(selected[0].id,'0');console.log('PASS total'+total+' deletes'+selected.length);
}
const kept=choose(rows(1500),r=>Number(r.id)<900,'log-check');assert.equal(kept.length,900);assert.equal(1500-kept.length,600);console.log('PASS protected records retained when500 cannot be reached');
assert.equal(choose(rows(1700),()=>true,'calendar-day').length,1700);assert.equal(choose(rows(1700),()=>true,'new-show').length,1700);console.log('PASS daily/newshow stillclearall eligible');
const runtime=fs.mkdtempSync(path.join(os.tmpdir(),'keep500-'));const families=[];
try{
 for(let i=0;i<1200;i++){
  const run=path.join(runtime,'run-'+i+'.json'),error=path.join(runtime,'error-'+i+'.json');fs.writeFileSync(run,'{}');fs.writeFileSync(error,'{}');
  families.push({workflow:'wef-data',run:{file:run,data:{started:new Date(1700000000000+i*1000).toISOString(),completed:'2026-11-08',status:'Succeeded'}},children:[{file:error}],delivered:true,current:false,runCount:1200,errorCount:1200});
 }
 const result=pruneLocal(families,'log-check');assert.equal(result.runs,700);assert.equal(result.errors,700);assert.equal(fs.readdirSync(runtime).length,1000);console.log('PASS local1200runs/1200errors bothleave500');
}finally{fs.rmSync(runtime,{recursive:true});}
console.log('9 keep500 checks passed; no live calls');
