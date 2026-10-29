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
function resolve({entry, previous = {}, binder, localSchedules, now, classUnresolved = false}) {
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
    if (f['observation-status']!=='successful' || !fresh(f['last-seen'],now))
      throw Error('Missing, stale or unsuccessful participation observation');
    let evidence;
    try { evidence=JSON.parse(f['source-json']); } catch { throw Error('Entries source evidence unavailable'); }
    if (evidence?.format!=='wef-entry-link-v1' || !evidence.revision ||
        evidence.sourceEvidence?.revision!==evidence.revision || evidence.sourceEvidence?.scope!==evidence.scope)
      throw Error('Entries revision evidence missing or conflicting');
    const scope=String(evidence.scope).split('|');
    if (scope.length!==5 || scope[0]!=='15' || Number(scope[1])!==Number(show) || scope[2]!==date || Number(scope[4])!==Number(cid))
      throw Error('Entries evidence scope mismatch');
    if (!fresh(evidence.sourceAsOf,now) || Date.parse(evidence.sourceAsOf)!==Date.parse(f['last-seen']))
      throw Error('Original source observation stale or inconsistent');
    if (!Array.isArray(evidence.latestApplicableAttempts) || evidence.latestApplicableAttempts.length)
      throw Error('Newer applicable source attempt unresolved or evidence unavailable');
    if (f['schedule-link-status']!=='linked')
      throw Error('Entries schedule link unresolved: '+(f['link-issues']||'No accepted mapping'));
    const basis=f['schedule-link-basis'];
    if (!['class-date','observed-group'].includes(basis)) throw Error('Unknown Entries schedule link basis');
    const keys=scheduleKeys(f,true);
    if (basis==='class-date' && keys.length!==1) throw Error('Class-date association must identify one schedule');
    if (basis==='observed-group' && (f['group-link-status']!=='linked' || evidence.groupEvidence!==evidence.revision ||
        !Number.isSafeInteger(evidence.groupId) || evidence.groupId<=0))
      throw Error('Entries group attribution is not verified');
    if (!localSchedules) throw Error('Local schedule references unavailable');
    for (const key of keys) {
      if (!key.startsWith(`${show}|${date}|`) || !/^\d+_/.test(key.split('|')[2]||'')) throw Error('Schedule key scope mismatch');
      const local=localSchedules.filter(r=>r.fields['stage-key']===key);
      if (local.length!==1 || !local[0].id) throw Error('Missing or duplicate local schedule reference');
      const sf=local[0].fields;
      if (sf.status!=='active' || Number(sf['show-id'])!==Number(show) || sf.date!==date || Number(sf['class-id'])!==Number(cid))
        throw Error('Dropped or conflicting local schedule reference');
      if (basis==='observed-group' && !key.startsWith(`${show}|${date}|${evidence.groupId}_`))
        throw Error('Local schedule does not match verified group');
    }
    return {fields:{entry:[match.id], 'participation-link-status':'current', 'participation-link-reason':'Validated Entries link, original source freshness and local references', 'participation-checked-at':now,'participation-verified-at':now}};
  } catch(error) { return problem(error.message); }
}
module.exports={fresh,scheduleKeys,resolve};
