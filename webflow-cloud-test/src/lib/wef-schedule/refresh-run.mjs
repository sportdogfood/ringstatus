import { equals } from './shared-base.mjs';
import { refreshScheduleFeed } from './shared-feed.mjs';
import { refreshReadableSchedule } from './readable-feed.mjs';
import { isSharedDataConflict } from './shared-projection.mjs';
import { createHash } from 'node:crypto';

const safeError = error => /^[a-z0-9_:-]{1,100}$/.test(error?.message ?? '') ? error.message : 'schedule_feed_failed';

export async function runScheduleRefresh(api,{lane,slot,clock = () => new Date().toISOString(),readable=refreshReadableSchedule}) {
  if (!['full','day0'].includes(lane) || !/^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d+)?Z$/.test(slot ?? '') ||
      !Number.isFinite(Date.parse(slot))) throw Error('invalid_scheduled_feed_slot');
  const runId = `wef-schedule-app|${lane}|${new Date(slot).toISOString()}`;
  const existing = await api.read('runs',equals('run-id',runId));
  if (existing.length) throw Error('schedule_run_already_recorded');
  const started = clock(), written = [], conflicts=[];
  const workingApi={...api,onConflict:value=>conflicts.push(value)};
  let result, failure, stage='feed';
  try {
    result = await refreshScheduleFeed(workingApi,lane,started,written);
    if(result.upserts)result.upserts=[...result.upserts];
    stage='readable';
    result.readable = await readable(workingApi,lane,started,written);
  }
  catch (error) {
    if (isSharedDataConflict(error)) conflicts.push({reason:safeError(error),recordKey:error.details?.recordKey??result?.date??lane,
      date:result?.date,details:{stage,...error.details,handling:'Retain confirmed output; continue the next scheduled lane and cadence.'}});
    else failure = safeError(error);
  }
  try {
    for(const conflict of conflicts) {
      const details=JSON.stringify(conflict.details??{});
      const key=createHash('sha256').update(JSON.stringify([lane,conflict.date??'',conflict.recordKey,conflict.reason,details])).digest('hex');
      const previous=await api.read('conflicts',equals('conflict-key',key));
      // Admin status/notes belong to the admin. An unchanged conflict is not rewritten.
      if (!previous.length) await api.upsert('conflicts',{'conflict-key':key,'detected-at':started,'run-id':runId,lane,
        date:conflict.date??null,'record-key':conflict.recordKey,reason:conflict.reason,'details-json':details,'admin-status':'Open'});
    }
  } catch(error) {failure=safeError(error);}
  const completed = clock();
  const scope = result ? {'show-id':result.showId,date:result.date} : {};
  const summary = JSON.stringify({lane,...(result??{}),...(conflicts.length?{conflicts:conflicts.length,reviewTable:'schedule-fails'}:{}),...(failure?{stage,failed:failure,written}:{})});
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
