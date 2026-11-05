'use strict';
// Offline prompt/configuration and calendar checks only. No automation invocation.
const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict'),crypto=require('node:crypto');
const e=__dirname,r=path.resolve(e,'../../..'),before=JSON.parse(fs.readFileSync(path.join(e,'before-config.json'),'utf8')),c=JSON.parse(fs.readFileSync(path.join(e,'prepared-config.json'),'utf8')),checks=[];
function check(name,ok){assert(ok,name);checks.push({name,result:'PASS'});}
for(const name of ['id','name','kind','status','rrule'])check('Preserve existing '+name,c[name]===before[name]);
check('Same existing Shows chat',c.targetThreadId===before.target_thread_id);
check('Daily7p.m.target unchanged',c.rrule==='FREQ=DAILY;BYHOUR=19;BYMINUTE=0;BYSECOND=0');
check('Prompt exactly matches reviewed UTF8 text',c.prompt===fs.readFileSync(path.join(e,'prompt.txt'),'utf8')&&!c.prompt.includes('â'));
check('Recurring20minute tolerance stated',c.prompt.includes('from18:40Eastern')&&c.prompt.includes('up to20minutes early')&&c.prompt.includes('every normal daily wake, not a one-off'));
const anchor='Use ONLY these five existing local Codex chats';
check('All recipient/dedup/receipt/protection instructions byte-identical',c.prompt.slice(c.prompt.indexOf(anchor))===before.prompt.slice(before.prompt.indexOf(anchor)));
check('Original strict19gate removed',!c.prompt.includes('If the supplied trigger is before19:00Eastern'));
const fmt=new Intl.DateTimeFormat('en-CA',{timeZone:'America/New_York',year:'numeric',month:'2-digit',day:'2-digit',hour:'2-digit',minute:'2-digit',hourCycle:'h23'});
function eligible(at){const p=Object.fromEntries(fmt.formatToParts(new Date(at)).map(v=>[v.type,v.value]));return Number(p.hour)*60+Number(p.minute)>=18*60+40;}
for(const [at,want]of [['2026-11-04T23:39:59Z',false],['2026-11-04T23:40:00Z',true],['2026-11-04T23:57:03.026Z',true],['2026-11-05T00:00:00Z',true],['2026-11-05T01:23:00Z',true],['2026-11-05T04:59:59Z',true],['2026-11-05T05:00:00Z',false],['2026-10-28T22:40:00Z',true],['2027-03-15T22:40:00Z',true]])check('Eastern window '+at,eligible(at)===want);
const lock=JSON.parse(fs.readFileSync(path.join(r,'wef-shows/evidence/locked-20261102/lock.json'),'utf8'));
check('All21 locked collector/runtime hashes unchanged',Object.entries(lock.runtime_files).every(([p,h])=>crypto.createHash('sha256').update(fs.readFileSync(path.join(r,p))).digest('hex')===h));
const state=JSON.parse(fs.readFileSync(path.join(r,'.git/ringstatus-control/wef-shows-weekday-notice-state.json'),'utf8'));
check('Original failed normal trigger retained',state.rejected_triggers.some(x=>x.trigger==='2026-11-04T23:57:03.026Z'&&x.sent===0));
const result={at:new Date().toISOString(),scope:'Offline configuration/calendar preflight; no native run, send or collector proof.',checks,count:checks.length,normal_receipts:'PENDING actual normal scheduled wake'};fs.writeFileSync(path.join(e,'preflight.json'),JSON.stringify(result,null,2),'utf8');console.log(JSON.stringify({passed:checks.length,source_failure_trigger_now_eligible:eligible('2026-11-04T23:57:03.026Z'),manual_runs:0}));
