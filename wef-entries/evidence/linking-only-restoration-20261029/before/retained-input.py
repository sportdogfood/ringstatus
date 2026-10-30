"""Inert completed-revision projection; no credentials, acquisition or writes."""
import json, pathlib, sys
ROOT = pathlib.Path(sys.argv[1]) if __name__ == '__main__' else pathlib.Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT / 'wef-data'))
sys.path.insert(0, str(ROOT / 'wef-schedules' / 'tests'))
from wef_data_handoff import revision_chain
from day_counts import retained_units, instant
from data_contract import packed, digest


def project(head, runs, collections, context):
    chain = revision_chain(head, runs, collections)
    fence = [context[k] for k in ('show-key', 'start-date', 'end-date', 'selected-date')]
    last = chain[-1][0]
    if last.get('authority') != fence or last.get('authority-before') != fence:
        raise ValueError('Canonical authority changed')
    if context['show-key'] != f"{context['customer-id']}|{context['show-id']}":
        raise ValueError('Canonical identity mismatch')
    classes, inventories, work = {}, {}, {}
    facts, identities, schedule_evidence, schedule_dates = {}, {}, {}, {}
    from wef_data_handoff import TABLE_KEYS
    for state, _ in chain:
        if state['show-key'] != context['show-key']:
            raise ValueError('Cross-show retained input')
        for unit, refs in retained_units(state, collections):
            graph = unit['graph']
            if unit['scope'] == 'retained-bootstrap':
                graph = unit['source']['retained-projection']
            mapping = (unit['source'].get('record-map', {}) if unit['scope']=='retained-bootstrap'
                       else unit['verification']['record-map'])
            for table, rows in graph.items():
                for row in rows:
                    key=str(row[TABLE_KEYS[table]])
                    facts.setdefault(table, {}).setdefault(key, {}).update(row)
                    record_id=mapping.get(table, {}).get(key)
                    previous_id=identities.setdefault(table, {}).get(key)
                    if record_id and previous_id and previous_id!=record_id:
                        raise ValueError('Retained Data record identity changed')
                    if record_id:identities[table][key]=record_id
                    if table=='schedules':
                        observed=(unit['metric']['completed'] if unit['scope']=='dated-inventory-page' else None)
                        if observed:instant(observed)
                        if observed or key not in schedule_evidence:
                            schedule_evidence[key]={'revision':head,'collections':refs,'sourceAsOf':observed}
            for row in graph.get('work-items', []):
                work.setdefault(row['work-key'], {}).update(row)
            source, metric = unit['source'], unit['metric']
            if unit['scope'] == 'dated-inventory-page':
                date = source['show_date']
                if str(source['show']['show_id']) != str(context['show-id']):
                    raise ValueError('Inventory show mismatch')
                groups = {}
                for ring in source['rings']:
                    for cls in ring['classes']:
                        if cls.get('schedule_break') or not cls.get('class_group_id'):
                            continue
                        key = (ring['ring_number'], cls['class_group_id'])
                        group = groups.setdefault(key, {'groupId': cls['class_group_id'], 'date': date,
                            'ringNumber': ring['ring_number'], 'ringId': ring['ring_id'], 'classIds': [], 'occurrences': []})
                        if cls['class_id'] not in group['classIds']:
                            group['classIds'].append(cls['class_id'])
                        if cls.get('class_groupxclasses_id'):
                            group['occurrences'].append({'classId': cls['class_id'], 'occurrenceId': cls['class_groupxclasses_id']})
                candidate = {'complete': True, 'revision': head, 'showId': context['show-id'],
                    'customerId': context['customer-id'], 'observedAt': metric['completed'],
                    'groups': list(groups.values()), 'collections': refs}
                if date not in inventories or instant(metric['completed']) > instant(inventories[date]['observedAt']):
                    inventories[date] = candidate
                    # The producer retains source pages separately from its later
                    # normalized graph. Match source occurrences to STORED keys;
                    # never rebuild those keys in this consumer.
                    schedule_dates[date]={'payload':source,'observedAt':metric['completed'],'collections':refs}
            if unit['scope'] != 'class':
                continue
            payloads, receipts = source['classes'], metric.get('requests', [])
            if len(payloads) != len(receipts):
                raise ValueError('Class receipt count mismatch')
            for payload, receipt in zip(payloads, receipts):
                if str(payload.get('show_id')) != str(context['show-id']):
                    raise ValueError('Class show mismatch')
                related = payload['class_related_data']; date = str(related['date'])[:10]
                if not context['start-date'][:10] <= date <= context['end-date'][:10]:
                    raise ValueError('Class date outside canonical show')
                observed = receipt['completed']; instant(observed)
                key = f"{context['show-key']}|{date}|{related['ring']}|{payload['class']['class_id']}"
                compact = {k: payload[k] for k in ('show_id','class','class_related_data','trips','total_entry_trips')}
                compact['class_group_order_of_go'] = payload.get('class_group_order_of_go')
                candidate = {'payload': compact, 'observedAt': observed, 'scopeKey': key,
                    'sourceEvidence': {'revision': head, 'scope': key, 'collections': refs,
                                       'payloadSha256': digest(packed(payload))}}
                previous = classes.get(key)
                if previous and instant(previous['observedAt']) == instant(observed) and previous['payload'] != compact:
                    raise ValueError('Conflicting equal-time class observation')
                if not previous or instant(previous['observedAt']) < instant(observed):
                    classes[key] = candidate
    for key, item in classes.items():
        # Share one exact inventory per day rather than repeating its full roster
        # for every class. The collector restores the same evidence before linking.
        item['inventoryDate'] = str(item['payload']['class_related_data']['date'])[:10]
        item['producerWork'] = work.get(key)
    def one(row,field,table):
        links=row.get(field,{}).get('$links',[])
        if len(links)!=1 or links[0][0]!=table or links[0][1] not in facts.get(table,{}):
            raise ValueError('Invalid retained schedule context: '+field)
        return links[0][1],facts[table][links[0][1]]
    schedules=[];schedule_proofs=[];proof_indexes={}
    for key,row in sorted(facts.get('schedules',{}).items()):
        showkey,show=one(row,'show','shows');daykey,day=one(row,'day','days');ringkey,ring=one(row,'ring','rings')
        if showkey!=context['show-key'] or one(day,'show','shows')[0]!=showkey or one(ring,'show','shows')[0]!=showkey or one(ring,'day','days')[0]!=daykey:
            raise ValueError('Cross-scope retained schedule')
        date=day['date']
        if not context['start-date'][:10]<=date<=context['end-date'][:10]:raise ValueError('Schedule day outside canonical show')
        source_page=schedule_dates.get(date)
        if source_page:
            rings=[r for r in source_page['payload']['rings'] if r['ring_number']==ring['ring-number']]
            position=row.get('source-position')
            if len(rings)!=1 or type(position) is not int or not 0<=position<len(rings[0]['classes']):continue
            occurrence=rings[0]['classes'][position]
            source_id=occurrence.get('class_groupxclasses_id')
            if (str(source_id) if source_id is not None else None)!=(row.get('source-id') or None):continue
            if bool(occurrence.get('schedule_break'))!=(row.get('kind')=='break'):continue
        cid=None
        if row.get('kind')=='class':
            _,cls=one(row,'class','classes');cid=cls['class-id']
            if source_page and occurrence.get('class_id')!=cid:continue
            if one(cls,'show','shows')[0]!=showkey or one(cls,'day','days')[0]!=daykey or one(cls,'ring','rings')[0]!=ringkey:
                raise ValueError('Schedule/class context mismatch')
        record=identities.get('schedules',{}).get(key)
        if not record:raise ValueError('Unmapped retained schedule')
        evidence=schedule_evidence[key]
        if source_page:evidence={'revision':head,'collections':source_page['collections'],
            'projectionCollections':evidence['collections'],'sourceAsOf':source_page['observedAt']}
        proof=packed(evidence)
        if proof not in proof_indexes:
            proof_indexes[proof]=len(schedule_proofs);schedule_proofs.append(evidence)
        schedules.append({'key':key,'recordId':record,'customerId':show['customer-id'],'showId':show['show-id'],
            'date':date,'ringNumber':ring['ring-number'],'classId':cid,'kind':row['kind'],
            'sourceAsOf':evidence['sourceAsOf'],'projectionAsOf':row.get('last-success'),
            'sourceEvidenceIndex':proof_indexes[proof]})
    return {'head': head, 'classes': [classes[k] for k in sorted(classes)],
            'inventories':inventories,'schedules':schedules,'scheduleEvidence':schedule_proofs,
            'coverage': 'retained-class-snapshots', 'wholeDayComplete': False}


if __name__ == '__main__':
    try:
        value = json.load(sys.stdin)
        result = project(value['head'], value['runs'], value['collections'], value['context'])
        json.dump(result, sys.stdout, ensure_ascii=False, separators=(',', ':'))
    except Exception as error:
        print(str(error), file=sys.stderr)
        sys.exit(1)
