'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const {enrichTimes,captureScheduleHtml}=require('../schedule-times');
const row=(fields={})=>({key:'same',fields:{show_id:74,show_date:'2026-10-15',ring_number:2,class_group_id:8,class_number:11,...fields}});
const html=(ring=2,group=8,classes='11',time='1:30PM')=>`<tr class="class_group_row"><td><a href="?cgid=${group}&amp;ring=${ring}">Class [${classes}]</a></td><td>${time}</td></tr>`;
const enrich=(rows,previous,body)=>enrichTimes({rows,previous,showId:74,date:'2026-10-15',capture:async()=>body});

test('structured matching avoids substring and wrong-ring matches; compact PM parses',async()=>{
 const r=await enrich([row()],[],html(2,88,'111','8 AM')+html(1,8,'11','9 AM')+html());
 assert.equal(r.rows[0].fields.estimated_start_time,'13:30:00');assert.equal(r.summary.matched,1);
});
test('ambiguous rows retain prior time and report unmatched',async()=>{
 const r=await enrich([row()], [row({estimated_start_time:'10:00:00'})],html()+html(2,8,'11','2 PM'));
 assert.equal(r.rows[0].fields.estimated_start_time,'10:00:00');assert.equal(r.summary.unmatched,1);
});
test('failed or empty capture preserves prior times without inventing new ones',async()=>{
 for(const capture of [async()=>{throw Error('timeout');},async()=>'<table></table>']) {
  const r=await enrichTimes({rows:[row(),{...row(),key:'new'}],previous:[row({estimated_start_time:'11:00:00'})],showId:74,date:'2026-10-15',capture});
  assert.equal(r.rows[0].fields.estimated_start_time,'11:00:00');assert.equal(r.rows[1].fields.estimated_start_time,undefined);assert.ok(r.summary.error);
 }
});
test('both existing manual override spellings and manual HTML retain their times',async()=>{
 for(const flag of ['manual_time_override','manual_time_overide']) {
  const r=await enrich([row()], [row({[flag]:true,estimated_start_time:'12:00:00'})],html());
  assert.equal(r.rows[0].fields.estimated_start_time,'12:00:00');assert.equal(r.summary.manual,1);
 }
 const r=await enrich([{...row({estimated_start_time:'14:00:00'}),refs:{schedule_html_time_overlay:'manual.html'}}],[],html());
 assert.equal(r.rows[0].fields.estimated_start_time,'14:00:00');
});
test('empty schedule day does not launch browser',async()=>{
 const r=await enrichTimes({rows:[],showId:74,date:'2026-10-15',capture:()=>{throw Error('must not run');}});assert.equal(r.summary.error,null);
});
test('copied browser loading checks show/day and closes browser on failure',async()=>{
 let closed=0;const page={goto:async()=>{},waitForTimeout:async()=>{},url:()=> 'https://www.wellingtoninternational.com/showgrounds/show-schedule/?sid=75&date=2026-10-15'};
 const chromium={launch:async()=>({newPage:async()=>page,close:async()=>{closed++;}})};
 await assert.rejects(captureScheduleHtml(74,'2026-10-15',{chromium}),/outside requested/);assert.equal(closed,1);
 page.url=()=> 'https://www.wellingtoninternational.com/showgrounds/show-schedule/?sid=74&date=2026-10-15';
 page.locator=()=>({innerHTML:async()=>html()});assert.equal(await captureScheduleHtml(74,'2026-10-15',{chromium}),html());assert.equal(closed,2);
});


test('EST START applies to all group classes and carries until next printed time, including zero entries',async()=>{
 const fields=[787,835,720,744,715].map((n,i)=>({...row({class_number:n,class_group_id:i<3?8:i===3?9:10,total_trips:0}),key:String(n)}));
 const body=html(2,8,'787,835,720','8:00 AM')+html(2,9,'744','')+html(2,10,'715','8:27 AM');
 const result=await enrich(fields,[],body);
 assert.deepEqual(result.rows.map(r=>r.fields.estimated_start_time),['08:00:00','08:00:00','08:00:00','08:00:00','08:27:00']);
 assert.equal(result.summary.matched,5);assert.ok(result.rows.every(r=>r.fields.total_trips===0));
});
test('time inheritance never crosses rings, days or captures',async()=>{
 const other={...row({ring_number:3,class_group_id:9,class_number:12}),key:'other'};
 const result=await enrich([row(),other],[],html(2,8,'11','8 AM')+html(3,9,'12',''));
 assert.equal(result.rows[0].fields.estimated_start_time,'08:00:00');assert.equal(result.rows[1].fields.estimated_start_time,undefined);
 const tomorrow=await enrichTimes({rows:[row()],showId:74,date:'2026-10-16',capture:async()=>html(2,8,'11','')});
 assert.equal(tomorrow.rows[0].fields.estimated_start_time,undefined);
});
test('inherited times still respect manual overrides and failed capture preservation',async()=>{
 const body=html(2,7,'10','8 AM')+html(2,8,'11','');
 const manual=await enrich([row()], [row({manual_time_override:true,estimated_start_time:'10:00:00'})],body);
 assert.equal(manual.rows[0].fields.estimated_start_time,'10:00:00');
 const first=await enrich([row()],[],body);
 const next=await enrichTimes({rows:[row()],previous:first.rows,showId:74,date:'2026-10-15',capture:async()=>{throw Error('timeout');}});
 assert.equal(next.rows[0].fields.estimated_start_time,'08:00:00');assert.equal(next.summary.preserved,1);
});


const actualTable=require('node:fs').readFileSync(require('node:path').join(__dirname,'fixtures/rendered-vankampen-20261016.html'),'utf8');
const actualRows=()=>[720,787,835].map(n=>({key:String(n),fields:{show_id:200000074,show_date:'2026-10-16',ring_number:3,class_number:n,class_group_id:200027635}}));
test('actual rendered VanKampen rows inherit table ring and match720/787/835 to08:00',async()=>{
 const r=await enrichTimes({rows:actualRows(),showId:200000074,date:'2026-10-16',capture:async()=>actualTable});
 assert.deepEqual(r.rows.map(x=>x.fields.estimated_start_time),['08:00:00','08:00:00','08:00:00']);assert.equal(r.summary.matched,3);
 const parsed=require('../collector-core').parseScheduleHtmlTimeOverlay(actualTable).parsedRows;
 assert.equal(parsed[0].ring_number,3);assert.equal(parsed[0].class_group_id,null);
 assert.deepEqual(parsed[0].class_numbers,[787,835,720]);assert.equal(parsed[1].estimated_start_time,'08:30:00');
});
test('actual table correction retains manual overrides and previous values on capture failure',async()=>{
 const previous=actualRows().map(r=>({...r,fields:{...r.fields,manual_time_override:true,estimated_start_time:'07:45:00'}}));
 const r=await enrichTimes({rows:actualRows(),previous,showId:200000074,date:'2026-10-16',capture:async()=>actualTable});
 assert.ok(r.rows.every(x=>x.fields.estimated_start_time==='07:45:00'));assert.equal(r.summary.manual,3);
 const failed=await enrichTimes({rows:actualRows(),previous:r.rows,showId:200000074,date:'2026-10-16',capture:async()=>{throw Error('unavailable');}});
 assert.ok(failed.rows.every(x=>x.fields.estimated_start_time==='07:45:00'));
});
test('table ring identity cannot leak into unknown or conflicting tables or other days',()=>{
 const parse=require('../collector-core').parseScheduleHtmlTimeOverlay;
 const unknown=actualTable.replaceAll(/&amp;ring=3/g,'');
 const joined=parse(actualTable+unknown).parsedRows;
 assert.equal(joined[0].ring_number,3);assert.equal(joined[2].ring_number,null);
 const conflicting=actualTable.replace('</tbody>',html(4,99,'999','9 AM')+'</tbody>');
 assert.equal(parse(conflicting).parsedRows[0].ring_number,null);
 assert.equal(parse(unknown).parsedRows[0].ring_number,null);
 const blank=actualTable.replaceAll('8:00 AM','').replaceAll('8:30 AM','');
 assert.equal(parse(actualTable+blank).parsedRows.length,2);
 assert.equal(parse(blank).parsedRows.length,0);
});
test('missing-link group inherits preceding time only within its own ring table',()=>{
 const parse=require('../collector-core').parseScheduleHtmlTimeOverlay;
 const body='<table>'+html(3,1,'999','8 AM')+actualTable.match(/<tr class=" class_group_row">[\s\S]*?<\/tr>/)[0].replace('8:00 AM','')+'</table>';
 const r=parse(body).parsedRows;assert.equal(r[1].ring_number,3);assert.equal(r[1].estimated_start_time,'08:00:00');
});
