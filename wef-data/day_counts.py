"""Pure scoped counts from hash-verified retained source units. No acquisition or writes."""
import datetime, json
from data_contract import packed, digest

FORMAT='wef-day-count-summary-v1'
POLICY='dated-registrations-v1'

def instant(value):
    result=datetime.datetime.fromisoformat(value.replace('Z','+00:00'))
    if result.tzinfo is None:raise ValueError('Count source time lacks timezone')
    return result

def class_key(show,day,ring,cid):return f'{show}|{day}|{ring}|{cid}'

def summary_dates(context):
    start=datetime.date.fromisoformat(context['start-date'][:10]);end=datetime.date.fromisoformat(context['end-date'][:10])
    days=context.get('canonical-days')
    if days is None:return [(start+datetime.timedelta(days=i)).isoformat() for i in range((end-start).days+1)]
    if not isinstance(days,list) or not days or sorted(set(days))!=days:raise ValueError('Invalid canonical count day set')
    if any(datetime.date.fromisoformat(day)<start or datetime.date.fromisoformat(day)>end for day in days):raise ValueError('Canonical count date outside show')
    return days

def summaries(context, units, run_key, predecessor, verified_at):
    """Units are (verified frozen unit, exact ordered part references), oldest first.

    Only explicit accepted source snapshots establish class coverage. Bootstrap and
    mutable tables never acquire a new observation time through this projection.
    """
    show=context['show-key'];sid=context['show-id'];customer=context['customer-id']
    if show!=f'{customer}|{sid}':raise ValueError('Count canonical identity mismatch')
    inventory={};snapshots={};history={}
    for unit,references in units:
        scope=unit['scope'];source=unit['source'];metric=unit['metric']
        if scope=='dated-inventory-page':
            if str(source.get('show',{}).get('show_id'))!=str(sid):raise ValueError('Count inventory show mismatch')
            day=source['show_date'];expected=set()
            for ring in source['rings']:
                for cls in ring['classes']:
                    if not cls.get('schedule_break'):
                        expected.add(class_key(show,day,ring['ring_number'],cls['class_id']))
            candidate=(expected,metric,references)
            if day not in inventory or instant(metric['completed'])>=instant(inventory[day][1]['completed']):inventory[day]=candidate
        elif scope=='class':
            payloads=source['classes'];metrics=metric.get('requests',[])
            if len(payloads)!=len(metrics):raise ValueError('Class source receipt count mismatch')
            for payload,receipt in zip(payloads,metrics):
                if str(payload.get('show_id'))!=str(sid):raise ValueError('Count class show mismatch')
                related=payload['class_related_data'];day=str(related['date'])[:10]
                ck=class_key(show,day,related['ring'],payload['class']['class_id'])
                valid=True;entries=set();trips=set();scratch=set();source_ids=set()
                if payload.get('total_entry_trips')!=len(payload['trips']):valid=False
                for trip in payload['trips']:
                    eid=trip.get('entry_id');uuid=trip.get('entryxclasses_uuid');tid=uuid or trip.get('trip_id')
                    identity_valid=(isinstance(tid,str) and bool(tid.strip()) and tid==tid.strip()) if uuid else (type(tid) is int and tid>0 or isinstance(tid,str) and tid.isdecimal() and int(tid)>0)
                    if type(eid) is not int or eid<=0 or not identity_valid:valid=False;continue
                    source_id=('uuid' if uuid else 'trip-id',str(tid))
                    if source_id in source_ids:valid=False
                    source_ids.add(source_id)
                    ek=f'{sid}|{eid}';tk=f'{ck}|{eid}|{tid}'
                    if tk in trips:valid=False
                    entries.add(ek);trips.add(tk)
                    if str(trip.get('scratch_trip','')).lower() in ('1','true','yes'):scratch.add(tk)
                if valid:history.setdefault(ck,{}).update({tk:references for tk in trips})
                candidate={'entries':entries,'trips':trips,'scratch':scratch,'valid':valid,'metric':receipt,'references':references}
                if ck not in snapshots or instant(receipt['completed'])>=instant(snapshots[ck]['metric']['completed']):snapshots[ck]=candidate
    output=[]
    for day in summary_dates(context):
        dk=show+'|'+day
        inv=inventory.get(day);expected=inv[0] if inv else set()
        accepted=sorted(k for k in expected if k in snapshots and snapshots[k]['valid'])
        missing=sorted(expected-set(accepted));entries=set();trips=set();scratches=set();dropped=set();refs={};metrics=[]
        def add_refs(values):
            for value in values:refs[value['key']]=value
        if inv:metrics.append(inv[1]);add_refs(inv[2])
        for ck in accepted:
            snap=snapshots[ck];entries.update(snap['entries']);trips.update(snap['trips']);scratches.update(snap['scratch'])
            removed=set(history.get(ck,{}))-snap['trips'];dropped.update(removed)
            for tk in removed:add_refs(history[ck][tk])
            metrics.append(snap['metric']);add_refs(snap['references'])
        known=bool(inv) and (bool(accepted) or not expected)
        complete=bool(inv) and not missing
        body={'format':FORMAT,'policy-version':POLICY,'provider':'wellington','customer-id':customer,
              'show-key':show,'day-key':dk,'date':day,'coverage':'competition-class-participation',
              'completeness':'complete' if complete else 'partial' if known else 'unknown',
              'all-dated-activity-completeness':'unknown','unique-entry-count':len(entries) if known else None,
              'proven-trip-count':len(trips) if known else None,'participation-count':None,
              'scratch-count':len(scratches) if known else None,'drop-count':len(dropped) if known else None,
              'expected-scope-keys':sorted(expected),'accepted-scope-keys':accepted,'covered-scope-keys':accepted,
              'outcomes':{'valid-empty':sorted(k for k in accepted if not snapshots[k]['trips']),
                          'missing':missing,'failed':[],'invalid-identities':sorted(k for k in expected if k in snapshots and not snapshots[k]['valid'])},
              'memberships':{'entry-keys':sorted(entries),'trip-keys':sorted(trips),'dropped-trip-keys':sorted(dropped)},
              'input-collections':sorted(refs.values(),key=lambda x:x['key']),
              'run-key':run_key,'predecessor':predecessor,
              'observed-from':min((m['started'] for m in metrics),key=instant,default=None),
              'observed-through':max((m['completed'] for m in metrics),key=instant,default=None),
              'accepted-as-of':min((m['completed'] for m in metrics),key=instant,default=None),
              'verified-at':verified_at,
              'reasons':['Schooling and classless activity coverage is unproved.','Distinct participation identity is not established.']}
        envelope={'body':body,'sha256':digest(packed(body))}
        validate_summary(envelope,context);output.append(envelope)
    return output

def validate_summary(envelope,context):
    body=envelope['body']
    if digest(packed(body))!=envelope.get('sha256'):raise ValueError('Day summary digest mismatch')
    if body['format']!=FORMAT or body['policy-version']!=POLICY:raise ValueError('Unsupported day count policy')
    if body['show-key']!=context['show-key'] or body['customer-id']!=context['customer-id']:raise ValueError('Day summary context mismatch')
    if body['day-key']!=body['show-key']+'|'+body['date'] or not context['start-date'][:10]<=body['date']<=context['end-date'][:10]:raise ValueError('Day summary date mismatch')
    if body['date'] not in summary_dates(context):raise ValueError('Day summary outside canonical scope')
    if body['coverage']!='competition-class-participation' or body['all-dated-activity-completeness']!='unknown':raise ValueError('Unsupported broad coverage claim')
    if body['completeness'] not in ('complete','partial','unknown'):raise ValueError('Invalid count completeness')
    for field in ('unique-entry-count','proven-trip-count','scratch-count','drop-count'):
        count=body[field]
        if count is not None and (type(count) is not int or count<0):raise ValueError('Invalid count type')
        if body['completeness']=='unknown' and count is not None:raise ValueError('Unknown count must be null')
    if body['participation-count'] is not None:raise ValueError('Unproved participation count')
    if body['completeness']=='complete' and (body['outcomes']['missing'] or body['outcomes']['invalid-identities']):raise ValueError('Incomplete scopes claimed complete')
    if body['unique-entry-count'] is not None and body['unique-entry-count']!=len(set(body['memberships']['entry-keys'])):raise ValueError('Entry membership count mismatch')
    if body['proven-trip-count'] is not None and body['proven-trip-count']!=len(set(body['memberships']['trip-keys'])):raise ValueError('Trip membership count mismatch')
    for name in ('observed-from','observed-through','accepted-as-of','verified-at'):
        if body[name] is not None:instant(body[name])
    return body

def retained_units(state,collections):
    """Verify exact current reservation or completed journal; do not query a service."""
    from data_contract import thaw,validate_reference
    output=[];parts=[];references=[]
    def append():
        if parts:output.append((thaw(parts),list(references)))
    for reference in state['observations']:
        row=collections[reference['key']]
        validate_reference(reference,row)
        if json.loads(row['retained-json'])['part']==0 and parts:append();parts=[];references=[]
        parts.append(row);references.append(reference)
    append()
    return output

def accepted_summaries(head,runs,collections,context):
    """Read-only consumer contract; validates chain, dependencies and recomputed body."""
    from wef_data_handoff import revision_chain
    chain=revision_chain(head,runs,collections);units=[];accepted=[]
    days=chain[-1][0].get('canonical-days')
    if days is not None:
        if context.get('canonical-days') is not None and context['canonical-days']!=days:raise ValueError('Count canonical day scope mismatch')
        context={**context,'canonical-days':days};summary_dates(context)
    fence=[context[k] for k in ('show-key','start-date','end-date','selected-date')]
    if chain[-1][0].get('authority')!=fence or chain[-1][0].get('authority-before')!=fence:raise ValueError('Count canonical fence mismatch')
    for state,_ in chain:
        if state['show-key']!=context['show-key']:raise ValueError('Count revision canonical mismatch')
        state_key=next(k for k,r in runs.items() if r.get('state-json')==packed(state))
        for unit,refs in retained_units(state,collections):
            if unit['scope'].startswith('day-count-summary|'):
                if state is not chain[-1][0]:continue
                envelope=unit['source'];validate_summary(envelope,context)
                if unit['scope']!='day-count-summary|'+envelope['body']['day-key']:raise ValueError('Count summary scope mismatch')
                if envelope['body']['run-key']!=state_key:raise ValueError('Summary run identity mismatch')
                expected={e['body']['day-key']:e for e in summaries(context,units,state_key,state.get('predecessor'),envelope['body']['verified-at'])}
                if expected[envelope['body']['day-key']]!=envelope:raise ValueError('Day summary does not match immutable inputs')
                accepted.append({**envelope,'evidence':{'run-key':head,'state-sha256':digest(runs[head]['state-json']),
                                'summary-collections':refs}})
            else:units.append((unit,refs))
    if accepted:
        wanted=len(summary_dates(context))
        if len(accepted)!=wanted or len({e['body']['day-key'] for e in accepted})!=wanted:raise ValueError('Incomplete summary day set')
    return accepted
