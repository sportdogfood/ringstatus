import { equals } from './shared-base.mjs';
import { refreshScheduleFeed } from './shared-feed.mjs';

const safeError = error => /^[a-z0-9_:-]{1,100}$/.test(error?.message ?? '') ? error.message : 'schedule_feed_failed';

export async function runScheduleRefresh(api,{lane,slot,clock = () => new Date().toISOString()}) {
  if (!['full','day0'].includes(lane) || !/^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d+)?Z$/.test(slot ?? '') ||
      !Number.isFinite(Date.parse(slot))) throw Error('invalid_scheduled_feed_slot');
  const runId = `wef-schedule-app|${lane}|${new Date(slot).toISOString()}`;
  const existing = await api.read('runs',equals('run-id',runId));
  if (existing.length) throw Error('schedule_run_already_recorded');
  const started = clock(), written = [];
  let result, failure;
  try {result = await refreshScheduleFeed(api,lane,started,written);}
  catch (error) {failure = safeError(error);}
  const completed = clock();
  const scope = result ? {'show-id':result.showId,date:result.date} : {};
  const summary = JSON.stringify({lane,...(result ?? {failed:failure,written})});
  const log = {'run-id':runId,started,completed,result:failure ? 'Error' : 'Success',summary,...scope,
    ...(failure ? {errors:failure} : {})};
  const receipt = {log,loggingErrors:[]};
  async function audit(table, fields) {
    try {await api.upsert(table,fields);}
    catch (error) {receipt.loggingErrors.push({table,error:safeError(error)});}
  }
  if (failure) await audit('errors',{'error-key':runId+'|refresh','run-id':runId,'occurred-at':completed,message:failure,...scope});
  await audit('runs',log);
  const heartbeatKey = 'wef-schedule-app|'+lane;
  try {
    const old = await api.read('heartbeat',equals('heartbeat-key',heartbeatKey));
    if (old.length > 1) throw Error('duplicate_schedule_heartbeat');
    if (!old.length || Date.parse(old[0]['checked-at']) <= Date.parse(completed)) {
      await audit('heartbeat',{'heartbeat-key':heartbeatKey,'checked-at':completed,...log,errors:failure ?? null});
    }
  } catch(error) {receipt.loggingErrors.push({table:'heartbeat',error:safeError(error)});}
  return receipt;
}
