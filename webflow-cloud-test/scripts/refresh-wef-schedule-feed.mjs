import { sharedBase } from '../src/lib/wef-schedule/shared-base.mjs';
import { runScheduleRefresh } from '../src/lib/wef-schedule/refresh-run.mjs';

// The authorized scheduler supplies its UTC slot and runs this as the sole writer.
const [lane,slot] = process.argv.slice(2);
try {
  const api = sharedBase({token:process.env.AIRTABLE_TOKEN,writable:true});
  const receipt = await runScheduleRefresh(api,{lane,slot});
  console.log(JSON.stringify(receipt));
  if (receipt.log.result !== 'Success' || receipt.loggingErrors.length) process.exitCode = 1;
} catch(error) {
  const code = /^[a-z0-9_:-]{1,100}$/.test(error?.message ?? '') ? error.message : 'schedule_feed_runner_failed';
  console.error(JSON.stringify({result:'Error',error:code}));
  process.exitCode = 1;
}
