const test=require('node:test'),assert=require('node:assert/strict'),path=require('node:path'),{execFileSync}=require('node:child_process');
const {authorityAgeReason,authorityExpires}=require('../authority-age');
const cases=[
 ['2026-10-30T23:45:00Z','2026-10-31T12:00:00Z',true],
 ['2026-10-30T23:45:00Z','2026-10-31T20:29:59Z',true],
 ['2026-10-30T23:45:00Z','2026-10-31T20:30:01Z',false],
 ['2026-10-30T23:29:59Z','2026-10-31T12:00:00Z',false],
 ['2026-10-30T23:30:00Z','2026-10-31T12:00:00Z',true],
 ['2026-10-30T23:45:00Z','2026-11-01T12:00:00Z',false],
 ['2026-10-31T20:04:00Z','2026-10-31T20:33:59Z',true],
 ['2026-10-31T20:04:00Z','2026-10-31T20:34:01Z',false],
 // Fall and spring DST: next window still starts at 16:00 Eastern.
 ['2026-10-31T23:45:00Z','2026-11-01T21:29:59Z',true],
 ['2026-10-31T23:45:00Z','2026-11-01T21:30:01Z',false],
 ['2027-03-14T00:45:00Z','2027-03-14T20:29:59Z',true],
 ['2027-03-14T00:45:00Z','2027-03-14T20:30:01Z',false],
 ['2026-10-31T20:04:00Z','2026-10-31T20:03:00Z',false],
 ['invalid','2026-10-31T20:03:00Z',false]
];
test('retained authority follows Eastern windows, expires after a missed window, and never changes timestamps',()=>{
 for(const [at,now,valid]of cases)assert.equal(authorityAgeReason(at,now)===null,valid,`${at} -> ${now}`);
 assert.equal(new Date(authorityExpires('2026-10-31T23:45:00Z')).toISOString(),'2026-11-01T21:30:00.000Z');
 assert.equal(authorityAgeReason('2026-10-30T23:45:00Z','2026-10-31T20:30:00Z'),'stale_observation');
 assert.equal(authorityAgeReason('2026-10-30T23:45:00Z','2026-10-31T20:30:00Z',{inclusive:true}),null);
 assert.equal(authorityAgeReason('2026-10-31T20:04:00Z','2026-10-31T20:03:00Z',{futureMs:60000,inclusive:true}),null);
});

test('Data Python validator agrees on daily and DST authority windows',()=>{
 const root=path.resolve(__dirname,'../..'),python=path.join(process.env.WEF_DAY_TEST_SOURCE_ROOT||root,'wef-data/.venv/Scripts/python.exe');
 const script=`import sys,json,datetime
sys.path.insert(0,sys.argv[1])
from wef_data_context import validate_context,SHOWS_BASE
out=[]
for at,now,expected in json.loads(sys.argv[2]):
 row={'default':True,'customer-id':15,'show-id':76,'show-key':'15|76','last-checked':at,'start-date':'2026-10-29','end-date':'2026-11-01','selected-date':'2026-10-30'}
 try:
  result=validate_context({'authority-base':SHOWS_BASE,'records':[row]},datetime.datetime.fromisoformat(now.replace('Z','+00:00')))
  assert result['last-checked']==at
  out.append(True)
 except ValueError: out.append(False)
print(json.dumps(out))`;
 const result=execFileSync(python,['-B','-c',script,path.join(root,'wef-schedules/tests'),JSON.stringify(cases)],{encoding:'utf8',windowsHide:true});
 assert.deepEqual(JSON.parse(result),cases.map(c=>c[2]));
});

test('Live, Entries and Timing accept retained show authority, preserving unrelated age rules',()=>{
 const row={id:'recShow',fields:{'show-key':'15|76',show_id:76,customer_id:15,is_default:true,last_checked:'2026-10-30T23:45:00Z',start_date:'2026-10-29',end_date:'2026-11-01',selected_date:'2026-10-31',available_dates:'2026-10-29, 2026-10-30, 2026-10-31, 2026-11-01'}};
 const now='2026-10-31T12:00:00Z',before=JSON.stringify(row);
 assert.equal(require('../../wef-live/model').scope([row],now).show,76);
 assert.equal(require('../../wef-entries/model').scope([row],now).show.show_id,76);
 assert.equal(require('../../wef-timing/purge').authority(row,new Map([['2026-10-31',null]]),now).reason,null);
 assert.equal(require('../../wef-timing/data-runner').selectShow([row],now).context['show-id'],76);
 assert.equal(JSON.stringify(row),before);
 assert.equal(require('../../wef-timing/pace').ageReason(row.fields.last_checked,now,1800000),'stale_observation');
 for(const module of ['../../wef-live/model','../../wef-entries/model']){
  assert.throws(()=>require(module).scope([row],'2026-11-01T12:00:00Z'),/stale/);
  assert.throws(()=>require(module).scope([row,row],now),/one/);
 }
});
