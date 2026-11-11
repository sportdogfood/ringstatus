// Called only by the registered Windows time trigger. Never use as manual workflow proof.
import { readFile, writeFile, rename, mkdir } from 'node:fs/promises';
import { resolve, join } from 'node:path';
import { sharedBase } from '../src/lib/wef-schedule/shared-base.mjs';
import { runDueFeeds } from '../src/lib/wef-schedule/cadence.mjs';

const directory=resolve(process.argv[2]??'');
if(!process.argv[2])throw Error('missing_private_schedule_state_directory');
await mkdir(directory,{recursive:true});
const filename=join(directory,'state.json');
let state;
try{state=JSON.parse(await readFile(filename,'utf8'));}
catch(error){if(error.code!=='ENOENT')throw error;state={};}
const save=async value=>{
  const temporary=filename+'.'+process.pid+'.tmp';
  await writeFile(temporary,JSON.stringify(value,null,2)+'\n');
  await rename(temporary,filename);
};
const receipt=await runDueFeeds({api:sharedBase({token:process.env.AIRTABLE_TOKEN,writable:true}),state,save});
if(receipt.outcomes.length)await writeFile(join(directory,receipt.slot.replace(/[:.]/g,'-')+'.json'),JSON.stringify(receipt,null,2)+'\n');
console.log(JSON.stringify(receipt));
if(receipt.stopped)process.exitCode=1;
