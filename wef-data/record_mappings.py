"""Established class-trip, entity and result mappings, shared by both stages."""
import json

def packed(value):return json.dumps(value,separators=(',',':'),sort_keys=True,ensure_ascii=False)
def refs(table,*keys):return {'$links':[[table,str(key)] for key in keys if key is not None]}
def number(value):
    try:return int(value) if value not in (None,'') else None
    except (ValueError,TypeError):return None
def text(value):return str(value) if value is not None else None

RESULT_FIELDS={'placing':'placing','position':'position',**{f'score{i}':f'score{i}' for i in range(1,7)},
    **{f'time-{word}':f'time_{word}' for word in ('one','two','three')},
    **{f'faults-{word}':f'faults_{word}' for word in ('one','two','three')},
    **{f'time-fault-{word}':f'time_fault_{word}' for word in ('one','two','three')},
    'points-earned':'points_earned','prize-money':'total_prize_money','gone-in':'gone_in','scratch':'scratch_trip'}

def trip_records(trip,class_key,class_status,observed):
    """Same native identities/values as the established graph/delta mappings."""
    customer,sid=class_key.split('|')[:2];show=customer+'|'+sid
    eid=int(trip['entry_id']);ek=sid+'|'+str(eid)
    key=class_key+'|'+str(eid)+'|'+str(trip.get('entryxclasses_uuid') or trip.get('trip_id') or '')
    graph={name:[] for name in ('entries','horses','riders','trainers','owners','trips','results')}
    identities={}
    for table,id_field,name_field,source_name in [('horses','horse_id','horse-name','horse'),
            ('riders','rider_id','rider-name','rider_name'),('trainers','trainer_id','trainer-name','trainer')]:
        value=number(trip.get(id_field));identity=str(value) if value is not None and value>0 else None
        identities[table]=identity
        if identity:
            name=trip.get(source_name)
            if table=='riders':name=name or trip.get('rider')
            graph[table].append({table[:-1]+'-key':identity,table[:-1]+'-id':value,
                name_field:name,'last-success':observed})
    oid=number(trip.get('owner_id'));owner=str(oid) if oid is not None and oid>0 else None
    if not owner and trip.get('Owner'):owner=f'unknown-id|{class_key}|{eid}|{trip.get("trip_id") or trip.get("entryxclasses_uuid")}'
    if owner:graph['owners'].append({'owner-key':owner,'owner-id':str(oid) if oid is not None and oid>0 else None,
        'owner-name':trip.get('Owner'),'identity-source':'source owner_id' if oid is not None and oid>0 else 'Source occurrence; owner_id absent; not merged by name','last-success':observed})
    graph['entries'].append({'entry-key':ek,'show':refs('shows',show),'entry-id':eid,
        **({'entry-number':str(trip['number'])} if trip.get('number') is not None else {}),'horse':refs('horses',identities['horses']),
        'riders':refs('riders',identities['riders']),'trainers':refs('trainers',identities['trainers']),
        'owners':refs('owners',owner),'last-success':observed})
    row={'trip-key':key,'class':refs('classes',class_key),'entry':refs('entries',ek),
        'horse':refs('horses',identities['horses']),'rider':refs('riders',identities['riders']),
        'trainer':refs('trainers',identities['trainers']),'owner':refs('owners',owner),
        'source-json':packed(trip),'last-success':observed,'record-status':'active'}
    for field,source in {'trip-id':'trip_id','source-uuid':'entryxclasses_uuid','order-of-go':'order_of_go',
            'actual-order':'actual_order','gone-in':'gone_in','placing':'placing','scratch':'scratch_trip',
            'estimated-go-time':'estimated_go_time'}.items():
        if trip.get(source) is not None:
            if field in ('order-of-go','actual-order'):
                value=number(trip[source])
                if value is not None:row[field]=value
            else:row[field]=str(trip[source])
    graph['trips'].append(row)
    values={name:trip.get(source) for name,source in RESULT_FIELDS.items()}
    graph['results'].append({'result-key':key,'trip':refs('trips',key),'class':refs('classes',class_key),
        'entry':refs('entries',ek),'class-status':class_status,
        **{name:text(value) for name,value in values.items()},'source-json':packed(values),'last-success':observed})
    return graph
