const fs = require('node:fs');
const path = require('node:path');
const assert = require('node:assert/strict');
const scriptPath = path.resolve(__dirname, '../candidate/schedule-priority.js');
const {evaluate} = require(scriptPath);
const record = (id, number, name = 'Hunter', count = 4, status = 'Active', extra = {}) => ({id, fields: {
  'schedule-key': `15|77|2026-11-06|1|${id}`, kind: 'class', 'record-status': status,
  'source-json': JSON.stringify({class_number: number, class_name: name, class_type: 'Hunter', class_group_id: 3, total_trips: count, ...extra})
}});
const rows = [record('r111',111),record('r112',112),record('r177',177,'Other Hunter'),record('r178',178,'Other Hunter'),record('r179',179,'Another Hunter'),record('r180',180,'Another Hunter'),record('flat',181,'Flat',1,'Active',{schedule_sequencetype:'Under Saddle/Flat'}),record('zero',182,'Empty',0),record('missing',183,'Unknown',undefined)];
// Set truly absent count, rather than the helper default.
rows[8].fields['source-json'] = JSON.stringify({class_number:183,class_name:'Unknown',class_type:'Hunter',class_group_id:3});
const errors = [];
const plans = evaluate(rows, e => errors.push(e));
assert.equal(errors.length,0);
assert.deepEqual(plans.slice(0,6).map(p => p.fields['rs-priority']),[1,2,1,2,1,2]);
assert.equal(plans[6].fields['rs-priority'],9);
assert.equal(plans[7].fields['rs-flag'],'No entries');
assert.equal(plans[8].fields['rs-flag'],'First trip');
for (const [name,flag] of [['Handy Hunter','Handy'],['Warm Up Hunter','Warmup'],['Schooling Pony Hunter','Schooling pony'],['Mulligan Class','Mulligan'],['Add Back Hunter','Add back'],['Classic Hunter','Classic']]) {
  assert.equal(evaluate([record('category',1,name)], e=>{throw Error(e)})[0].fields['rs-flag'],flag);
}
assert.equal(evaluate([record('model',1,'Model',1,'Active',{schedule_sequencetype:'Model'})],e=>{throw Error(e)})[0].fields['rs-flag'],'Model');
console.log('PASS evaluator numbering, Flat9, zero/missing count, category rules');

// Actual Airtable wrapper must not renumber a current schedule using missing historical rows.
const current = record('current',112);
current.fields = {...current.fields,'rs-lever':'lever1','rs-flag':'First trip','rs-priority':1};
const absent = record('old',111,'Hunter',4,'Absent');
const saved = [absent,current];
const updates=[];
const base={getTable:()=>({selectRecordsAsync:async()=>({records:saved.map(r=>({id:r.id,getCellValue:n=>r.fields[n]}))}),updateRecordsAsync:async batch=>updates.push(...batch)})};
const output={set:()=>{}};
const code=fs.readFileSync(scriptPath,'utf8').split('if (typeof module')[0]+'\nawait evaluateAirtable();';
const AsyncFunction=Object.getPrototypeOf(async function(){}).constructor;
(async()=>{
 await new AsyncFunction('base','output',code)(base,output);
 console.log(JSON.stringify({automationUpdates:updates}));
 assert.ok(!updates.some(row=>row.id==='current'),'Airtable reevaluation changed current row priority because Absent row participated');
 console.log('PASS Airtable wrapper preserves current schedule priorities');
})().catch(e=>{console.error(e);process.exitCode=1;});
