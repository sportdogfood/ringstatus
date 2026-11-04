'use strict';
// Offline configuration/calendar checks; never sends a notice or runs a collector.
const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict'),crypto=require('node:crypto');
const e=__dirname,r=path.resolve(e,'../../..'),c=JSON.parse(fs.readFileSync(path.join(e,'prepared-config.json'))),task=JSON.parse(fs.readFileSync(path.join(e,'task.json'))),checks=[];
function check(name,condition){assert(condition,name);checks.push({name,result:'PASS'});}
const fmt=new Intl.DateTimeFormat('en-CA',{timeZone:'America/New_York',year:'numeric',month:'2-digit',day:'2-digit',weekday:'long',hour:'2-digit',hourCycle:'h23'});
const local=at=>Object.fromEntries(fmt.formatToParts(new Date(at)).map(p=>[p.type,p.value]));
check('Daily19:00 local recurrence',c.rrule==='FREQ=DAILY;BYHOUR=19;BYMINUTE=0;BYSECOND=0');
check('Existing Shows chat heartbeat',c.kind==='heartbeat'&&c.targetThreadId==='01a16976-f0a6-72a1-a75f-75de7f19a815');
const expected=['wef-records-coordinator','wef-timing-coordinator','wef-messages-coordinator','wef-waze-coordinator','wef-publish-coordinator'];
check('Exact five verified recipient identities',task.recipients.length===5&&new Set(task.recipients.map(x=>x.threadId)).size===5&&expected.every(n=>task.recipients.some(x=>x.name===n))&&task.recipients.every(x=>c.prompt.includes(x.threadId)));
const weekdays=['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday'];
for(let i=0;i<7;i++){const d=new Date(Date.UTC(2026,10,2+i,12)),next=new Date(Date.UTC(2026,10,3+i,12));check(weekdays[i]+' to '+weekdays[(i+1)%7],local(d).weekday===weekdays[i]&&local(next).weekday===weekdays[(i+1)%7]);}
const winter=local('2026-11-05T00:00:00Z'),summer=local('2026-03-09T23:00:00Z');
check('Eastern scheduled trigger uses local date',winter.day==='04'&&winter.weekday==='Wednesday'&&winter.hour==='19');
check('19:00 handles summer and winter offsets',summer.hour==='19'&&winter.hour==='19');
check('First event is future Nov4; no backfill/manual proof',task.firstExpectedNormalOccurrence==='2026-11-04T19:00:00-05:00'&&c.prompt.includes('First eligible event date is November 4, 2026')&&c.prompt.includes('Never manually run this automation'));
check('Actual acknowledgments and safe uncertain-send handling required',c.prompt.includes('Sending alone is NOT receipt')&&c.prompt.includes('never blindly duplicate')&&c.prompt.includes('same old event ID'));
check('UTF8 deployed prompt exactly matches reviewed text',c.prompt===fs.readFileSync(path.join(e,'prompt.txt'),'utf8').trimEnd()&&!c.prompt.includes('â'));
check('Durable send and daily followup intents before sends',c.prompt.includes('BEFORE each send tool call')&&c.prompt.includes('BEFORE that follow-up')&&c.prompt.includes('never assume PendingSend means undelivered'));
check('Cleanup reminder grants no execution',c.prompt.includes('does not authorize or trigger cleanup')&&c.prompt.includes('cleanup/task starts'));
check('Show day0/priorities protected',c.prompt.includes('must not change canonical show day0, selected_date, show-day priorities'));
const lock=JSON.parse(fs.readFileSync(path.join(r,'wef-shows/evidence/locked-20261102/lock.json')));
check('All locked collector/runtime hashes unchanged',Object.entries(lock.runtime_files).every(([p,h])=>crypto.createHash('sha256').update(fs.readFileSync(path.join(r,p))).digest('hex')===h));
const result={checked_at:new Date().toISOString(),runtime:process.version,scope:'Offline configuration/calendar preflight only; no send, automation run, business execution or native scheduler proof.',checks,count:checks.length,normal_execution:'UNVERIFIED; next normal occurrence Nov4 19:00Eastern'};
fs.writeFileSync(path.join(e,'preflight.json'),JSON.stringify(result,null,2));console.log(JSON.stringify({checks_passed:checks.length,scope:result.scope,first_normal:task.firstExpectedNormalOccurrence}));
