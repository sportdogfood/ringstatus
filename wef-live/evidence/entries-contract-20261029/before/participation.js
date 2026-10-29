"use strict";
const fresh = (value, now) => {
  const age = Date.parse(now) - Date.parse(value);
  return Number.isFinite(age) && age >= -60000 && age <= 30 * 60000;
};
function scheduleKeys(fields, arrayFieldPresent = Object.hasOwn(fields, 'schedule-keys')) {
  const scalar = fields['schedule-key'] || '';
  if (!arrayFieldPresent) {
    if (typeof scalar !== 'string' || !scalar) throw Error('Missing schedule key');
    return [scalar];
  }
  let keys;
  try { keys = JSON.parse(fields['schedule-keys']); } catch { throw Error('Malformed schedule-keys'); }
  if (!Array.isArray(keys) || !keys.length || keys.some(k=>typeof k!=='string'||!k) ||
      JSON.stringify(keys)!==JSON.stringify([...new Set(keys)].sort())) throw Error('Invalid schedule-keys array');
  if ((keys.length===1 && scalar!==keys[0]) || (keys.length>1 && scalar!=='')) throw Error('Conflicting scalar/array schedule keys');
  return keys;
}
function resolve({entry, previous = {}, binder, localSchedules, sourceSchedules, health,
  arrayFieldPresent, observationField, observationStatusField, now, classUnresolved = false}) {
  const prior = previous.entry || [];
  const problem = reason => ({fields:{
    entry:prior,
    'participation-link-status':prior.length?'historical':'unresolved',
    'participation-link-reason':reason,
    'participation-checked-at':now,
  }, issue:reason});
  if (classUnresolved) return problem('Live class identity unresolved; prior link is historical');
  if (!binder) return problem('Participation reference unavailable');
  const matches = binder.filter(r=>r.fields['entry-key']===entry['entry-key']);
  if (!matches.length) return prior.length ? problem('Previously linked participation is missing') : {
    fields:{entry:[], 'participation-link-status':'unmatched', 'participation-link-reason':'No available validated entry reference', 'participation-checked-at':now}};
  if (matches.length!==1) return problem('Ambiguous participation identity');
  const match=matches[0], f=match.fields;
  try {
    const [show,date,eid,cid] = String(entry['entry-key']).split('|');
    if (Number(show)!==Number(entry['show-id']) || date!==entry.date || Number(eid)!==Number(entry['entry-id']) || !cid)
      throw Error('Malformed live participation key');
    for (const [name,value] of Object.entries({'show-id':show,'entry-id':eid,'class-id':cid,'horse-id':entry['horse-id'],'rider-id':entry['rider-id']})) {
      if (!Number.isSafeInteger(Number(value)) || Number(value)<=0 || Number(f[name])!==Number(value)) throw Error('Missing or conflicting '+name);
    }
    if (f.date!==date || f.status!=='active') throw Error('Participation date/status is not current');
    if (!observationField || !observationStatusField || f[observationStatusField]!=='successful' || !fresh(f[observationField],now))
      throw Error('Missing, stale or unsuccessful participation observation');
    const keys=scheduleKeys(f,arrayFieldPresent);
    if(new Set(keys.map(k=>k.split('_')[0])).size!==keys.length) throw Error('Duplicate eligible schedule group');
    if (!localSchedules || !sourceSchedules || !health) throw Error('Schedule reference/health unavailable');
    const dated=health.filter(r=>Number(r.fields['show-id'])===Number(show)&&r.fields.date===date)
      .sort((a,b)=>String(b.fields.started||'').localeCompare(String(a.fields.started||'')));
    const h=dated[0]?.fields;
    if (!h || h.status!=='complete' || !fresh(h.completed,now)) throw Error('Missing, stale or incomplete dated schedule collection');
    const activeKeys = rows => rows.filter(r=>r.fields.status==='active'&&Number(r.fields['show-id'])===Number(show)&&r.fields.date===date).map(r=>r.fields['stage-key']).sort();
    const localKeys=activeKeys(localSchedules), sourceKeys=activeKeys(sourceSchedules);
    if (localKeys.some(k=>typeof k!=='string'||!k) || sourceKeys.some(k=>typeof k!=='string'||!k) ||
        new Set(localKeys).size!==localKeys.length || new Set(sourceKeys).size!==sourceKeys.length ||
        JSON.stringify(localKeys)!==JSON.stringify(sourceKeys)) throw Error('Dated schedule source/sync key mismatch');
    for (const key of keys) {
      if (!key.startsWith(`${show}|${date}|`) || !/^\d+_/.test(key.split('|')[2]||'')) throw Error('Schedule key scope mismatch');
      const local=localSchedules.filter(r=>r.fields['stage-key']===key), source=sourceSchedules.filter(r=>r.fields['stage-key']===key);
      if (local.length!==1||source.length!==1) throw Error('Missing or duplicate schedule reference');
      for(const r of [local[0],source[0]]) if(r.fields.status!=='active'||Number(r.fields['show-id'])!==Number(show)||r.fields.date!==date||Number(r.fields['class-id'])!==Number(cid)) throw Error('Dropped or conflicting schedule reference');
      const prefix=key.slice(0,key.lastIndexOf('_')+1);
      for(const rows of [localSchedules,sourceSchedules]) if(rows.filter(r=>r.fields.status==='active'&&Number(r.fields['show-id'])===Number(show)&&r.fields.date===date&&Number(r.fields['class-id'])===Number(cid)&&String(r.fields['stage-key']).startsWith(prefix)).length!==1) throw Error('Ambiguous eligible schedule group');
      for(const name of ['stage-key','show-id','date','class-id','status','ring']) if(String(local[0].fields[name]??'')!==String(source[0].fields[name]??'')) throw Error('Schedule source/sync mismatch');
    }
    return {fields:{entry:[match.id], 'participation-link-status':'current', 'participation-link-reason':'Validated participation identity, observation and schedules', 'participation-checked-at':now,'participation-verified-at':now}};
  } catch(error) { return problem(error.message); }
}
module.exports={fresh,scheduleKeys,resolve};
