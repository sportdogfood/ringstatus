const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict');
const repo=path.resolve(__dirname,'../../../..');
const {normalizeSchedulePayload,chooseScheduleVariant}=require(path.join(repo,'wef-schedules/normalizer'));
const payload=require(path.join(repo,'wef-shows/staged/browser-primary-20261107/wef-shows/tests/fixtures/browser-schedule.json'));
const scope={app_show_idv2:200000077,customer_id:15,app_sql_datev2:'2026-11-07',app_dow_rawv2:'Sat'};
const empty={ok:true,source:'empty_ping_schedule',...normalizeSchedulePayload(payload,{scope,source:'empty_ping_schedule',generatedAt:'2026-11-07T23:00:00Z',generatedDate:'2026-11-07'})};
const chosen=chooseScheduleVariant({ok:false,rows:[],error:'fixture unavailable'},empty);
assert.equal(chosen,empty);assert(chosen.rows.length>0);assert(chosen.rows.every(r=>r.fields.class_id==null));
console.log(JSON.stringify({selected:chosen.source,rows:chosen.rows.length,null_class_ids:chosen.rows.filter(r=>r.fields.class_id==null).length,consequence:'Existing default fallback accepts raw browser rows without decoded class identity; catalog failure is caught in prepareScheduleDay.'}));
