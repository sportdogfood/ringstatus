import fs from 'node:fs';
import {spawnSync} from 'node:child_process';
const root=new URL('.',import.meta.url),cwd=process.cwd(),commands=[];
for(const [id,args] of [['syntax-worker',['--check','workers/wef-schedules-decoded/evidence/contract-release-20261111/worker.candidate.js']],['syntax-relay',['--check','workers/wef-schedules-decoded/evidence/contract-release-20261111/runner-relay.candidate.mjs']],['token-suite',['--test','workers/wef-schedules-decoded/evidence/contract-release-20261111/runner-token.test.mjs']]]){const started_at=new Date().toISOString();const r=spawnSync(process.execPath,args,{cwd,encoding:'utf8'});commands.push({id,command:JSON.stringify([process.execPath,...args]),cwd,runtime:process.version,started_at,ended_at:new Date().toISOString(),exit_code:r.status,stdout:r.stdout,stderr:r.stderr});}
fs.writeFileSync(new URL('review-execution.json',root),JSON.stringify({reviewer:'wef-decoder',commands},null,2));
console.log(JSON.stringify(commands.map(c=>({id:c.id,exit_code:c.exit_code,summary:c.stdout.split('\n').filter(l=>/^# (tests|pass|fail)/.test(l))}))));
if(commands.some(c=>c.exit_code!==0))process.exitCode=1;
