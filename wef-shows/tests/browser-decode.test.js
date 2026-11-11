"use strict";
const test=require('node:test'),assert=require('node:assert/strict'),crypto=require('node:crypto');
const {decodeSglPayload,SGL_BUNDLE_SALT}=require('../browser-decode');
const authorization='Bearer synthetic-test-only';
function encrypted(fields){const minute='29901578',iv=Buffer.alloc(12,3),key=crypto.createHash('sha256').update(SGL_BUNDLE_SALT+'|'+authorization+'|'+minute).digest();const cipher=crypto.createCipheriv('aes-256-gcm',key,iv);const body=Buffer.concat([cipher.update(JSON.stringify(fields)),cipher.final(),cipher.getAuthTag()]);return Buffer.concat([Buffer.from(minute),iv,body]).toString('base64');}
test('existing decoder restores authenticated nested fields, retaining schedule blobs and nonclass breaks',async()=>{
 const blob=encrypted({class_id:12345,total_trips:10});const body={rings:[{classes:[{class_id:null,_:blob},{schedule_break:1}]}]};const decoded=JSON.parse(await decodeSglPayload(JSON.stringify(body),authorization,SGL_BUNDLE_SALT,true));assert.equal(decoded.rings[0].classes[0].class_id,12345);assert.equal(decoded.rings[0].classes[0].total_trips,10);assert.equal(decoded.rings[0].classes[0]._,blob);assert.equal(decoded.rings[0].classes[1].schedule_break,1);
});
test('missing/wrong browser request authentication and refresh payload fail closed',async()=>{
 const body=JSON.stringify({_:encrypted({class_id:12345})});await assert.rejects(decodeSglPayload(body,null,SGL_BUNDLE_SALT,true));await assert.rejects(decodeSglPayload(body,'wrong',SGL_BUNDLE_SALT,true));await assert.rejects(decodeSglPayload('{"_t":true}',authorization,SGL_BUNDLE_SALT,true));
});
test('decoded live-browser fixture preserves schedule class IDs for downstream empty-ping fallback',()=>{
 const p=require('./fixtures/browser-schedule.json');const {normalizeSchedulePayload,chooseScheduleVariant}=require('../../wef-schedules/normalizer');
 const rows=normalizeSchedulePayload(p,{scope:{app_show_idv2:200000077,app_sql_date:'2026-11-07'},source:'empty_ping_schedule',generatedAt:'2026-11-07T23:46:13Z',generatedDate:'2026-11-07'}).rows;assert.ok(rows.length>0);assert.ok(rows.some(r=>Number(r.fields.class_id)>0));const chosen=chooseScheduleVariant({ok:false,rows:[]},{ok:true,rows});assert.equal(chosen.rows,rows);
});
