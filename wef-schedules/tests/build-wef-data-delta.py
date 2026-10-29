"""Apply validated adaptive observations to the same isolated linked graph.
Keeps the original baseline and immutable observation capture for evidence.
"""
import argparse,collections,copy,datetime,hashlib,json,pathlib

def main():
    parser=argparse.ArgumentParser();parser.add_argument('--evidence-root',type=pathlib.Path,required=True);args=parser.parse_args()
    ROOT=args.evidence_root.resolve()
    if (ROOT/'latest-graph.json').exists():raise ValueError('Refusing to overwrite a retained final graph')
    base=json.loads((ROOT/'graph.json').read_text(encoding='utf-8'));trial=json.loads((ROOT/'adaptive/trial.json').read_text(encoding='utf-8'));assert trial['status']=='PASS' and not trial['errors']
    def packed(v):return json.dumps(v,separators=(',',':'),sort_keys=True,ensure_ascii=False)
    def digest(v):return hashlib.sha256(packed(v).encode()).hexdigest()
    def links(t,*ks):return {'$links':[[t,str(k)] for k in ks if k is not None]}
    def num(v):
     try:return int(v) if v not in [None,''] else None
     except (TypeError,ValueError):return None
    def text(v):return str(v) if v is not None else None
    def iso(epoch):return datetime.datetime.fromtimestamp(epoch,datetime.timezone.utc).isoformat()
    # JSON ordering is not identity. Keep the established primary field mapping.
    primary=dict(zip(['runs','shows','days','rings','groups','classes','schedules','horses','riders','trainers','owners','entries','trips','lanes','work-items','slots','collections','requests','results'],['run-key','show-key','day-key','ring-key','group-key','class-key','schedule-key','horse-key','rider-key','trainer-key','owner-key','entry-key','trip-key','lane-key','work-key','slot-key','collection-key','request-key','result-key']))
    allrows={t:{r[primary[t]]:copy.deepcopy(r) for r in rows} for t,rows in base.items()};delta={t:{} for t in base}
    def put(t,row):
     if t in ('horses','riders','trainers','owners') and row.get('last-success'):
      prior=allrows[t].get(row[primary[t]],{})
      if prior.get('last-success') and datetime.datetime.fromisoformat(prior['last-success'].replace('Z','+00:00'))>datetime.datetime.fromisoformat(row['last-success'].replace('Z','+00:00')):return prior
     key=row[primary[t]];allrows[t][key]={**allrows[t].get(key,{}),**row};delta[t][key]={**delta[t].get(key,{}),**row};return allrows[t][key]
    run=trial['run_key'];sk=trial.get('show_key',base['shows'][0]['show-key']);assert len(base['shows'])==1 and base['shows'][0]['show-key']==sk,'Mixed show graph'
    customer,sid=map(int,sk.split('|'))
    put('runs',{'run-key':run,'mode':'Bounded live adaptive lane/slot trial','status':'Source verified; Airtable readback pending','started':trial['started'],'completed':trial['completed'],'source-requests':trial['request_count'],'source-bytes':sum(o['metric']['bytes'] for o in trial['observations']),'errors':0,'elapsed-seconds':trial['elapsed_seconds'],'summary':f"Live due-driven observations: {trial['request_count']} requests, {trial['repeated_classes']} repeated classes, {trial['replans']} actual interval/lane changes. Initial bounded segment exhausted its request allowance before recurrence; resumed from saved state without replaying successful units. No permanent scheduler installed."})
    policies=trial['policies'];groupids={k:r['group-id'] for k,r in allrows['groups'].items()};changed_trips=0;removed_trips=0
    for index,o in enumerate(trial['observations']):
     assert o['class_key'].startswith(sk+'|'),'Observation scope mismatch'
     assert not o['error'];state=o['new'];ck=o['class_key'];p=json.loads((ROOT/'adaptive'/o['source_file']).read_text(encoding='utf-8'));metric=o['metric'];at=metric['completed'];cls=allrows['classes'][ck];related=p['class_related_data']
     linked_groups=[groupids[key] for table,key in cls['group']['$links']]
     assert related.get('class_group_id') in linked_groups,('Inventory revalidation required',ck,related.get('class_group_id'),linked_groups)
     put('classes',{'class-key':ck,'status':related['status'],'entry-count':len(p['trips']),'source-json':packed(p),'fingerprint':state['fingerprint'],'last-success':state['last_success'],'last-change':state['last_change'],'last-run':links('runs',run)})
     pcy=policies[state['lane']]
     put('work-items',{'work-key':ck,'lane':links('lanes',state['lane']),'last-run':links('runs',run),'status':'Waiting','priority':pcy['priority'],'interval-seconds':state['interval'],'estimated-seconds':metric['elapsed_seconds'],'last-success':state['last_success'],'last-change':state['last_change'],'next-due':iso(state['next_due']),'unchanged-count':state['unchanged'],'failure-count':state['failures'],'fingerprint':state['fingerprint'],'plan-revision':state['revision'],'reason':o['reason'],'state-json':packed(state)})
     current=set()
     for t in p['trips']:
      eid=int(t['entry_id']);hid=int(t['horse_id']);rid=int(t['rider_id']);tid=num(t.get('trainer_id'));oid=num(t.get('owner_id'));ek=f'{sid}|{eid}';hk=str(hid);rrk=str(rid);tk=str(tid) if tid and tid>0 else None
      for table,key,idfield,namefield,name in [('horses',hk,'horse-id','horse-name',t.get('horse')),('riders',rrk,'rider-id','rider-name',t.get('rider_name') or t.get('rider')),('trainers',tk,'trainer-id','trainer-name',t.get('trainer'))]:
       if key:put(table,{table[:-1]+'-key':key,idfield:int(key),namefield:name,'last-success':at})
      ok=str(oid) if oid and oid>0 else None
      if not ok and t.get('Owner'):ok=f'unknown-id|{ck}|{eid}|{t.get("trip_id") or t.get("entryxclasses_uuid")}'
      if ok:put('owners',{'owner-key':ok,'owner-id':str(oid) if oid and oid>0 else None,'owner-name':t.get('Owner'),'identity-source':'source owner_id' if oid and oid>0 else 'Source occurrence; owner_id absent; not merged by name','last-success':at})
      e=copy.deepcopy(allrows['entries'].get(ek,{'entry-key':ek,'show':links('shows',sk),'entry-id':eid,'entry-number':text(t.get('number')),'horse':links('horses',hk),'riders':links('riders'),'trainers':links('trainers'),'owners':links('owners')}));assert e['horse']==links('horses',hk)
      for field,table,key in [('riders','riders',rrk),('trainers','trainers',tk),('owners','owners',ok)]:
       if key and [table,key] not in e[field]['$links']:e[field]['$links'].append([table,key])
      e['last-success']=max([v for v in (e.get('last-success'),at) if v],key=lambda value:datetime.datetime.fromisoformat(value.replace('Z','+00:00')));put('entries',e)
      key=f'{ck}|{eid}|{t.get("entryxclasses_uuid") or t.get("trip_id") or ""}';current.add(key);changed_trips+=1
      put('trips',{'trip-key':key,'class':links('classes',ck),'entry':links('entries',ek),'horse':links('horses',hk),'rider':links('riders',rrk),'trainer':links('trainers',tk),'owner':links('owners',ok),'trip-id':text(t.get('trip_id')),'source-uuid':t.get('entryxclasses_uuid'),'order-of-go':num(t.get('order_of_go')),'actual-order':num(t.get('actual_order')),'gone-in':text(t.get('gone_in')),'placing':text(t.get('placing')),'scratch':text(t.get('scratch_trip')),'estimated-go-time':text(t.get('estimated_go_time')),'source-json':packed(t),'fingerprint':digest(t),'last-success':at,'last-run':links('runs',run),'record-status':'active'})
     for key,t in list(allrows['trips'].items()):
      if t['class']==links('classes',ck) and key not in current and t.get('record-status')!='dropped':put('trips',{'trip-key':key,'record-status':'dropped','last-success':at,'last-run':links('runs',run)});removed_trips+=1
     collection=f'{run}|observation-{index+1:03}';put('collections',{'collection-key':collection,'run':links('runs',run),'show':cls['show'],'day':cls['day'],'ring':cls['ring'],'class':links('classes',ck),'scope':'class','status':'Source complete; readback pending','expected-records':len(p['trips']),'collected-records':len(p['trips']),'started':metric['started'],'completed':at,'fingerprint':state['fingerprint']})
     put('requests',{'request-key':run+f'|{index+1:04}','run':links('runs',run),'slot':links('slots',o['slot']),'work-item':links('work-items',ck),'collection':links('collections',collection),'route':'/classes/'+str(state['class_id']),'params':packed({'show_id':sid,'customer_id':customer}),'http':metric['http'],'elapsed-seconds':metric['elapsed_seconds'],'bytes':metric['bytes'],'sha256':metric['sha256'],'started':metric['started'],'completed':at,'status':'Validated'})
    for i,s in enumerate(trial['slots']):
     lane_keys=sorted(set(o['old']['lane'] for o in trial['observations'] if o['slot']==s['key']))
     put('slots',{'slot-key':s['key'],'run':links('runs',run),'lane':links('lanes',*lane_keys),'work-items':links('work-items',*s['class_keys']),'sequence':i+1,'started':s['started'],'completed':s['completed'],'budget-seconds':25,'elapsed-seconds':s['elapsed_seconds'],'request-count':len(s['requests']),'trip-count':sum(len(json.loads((ROOT/'adaptive'/o['source_file']).read_text(encoding='utf-8'))['trips']) for o in trial['observations'] if o['slot']==s['key']),'status':'Completed source reads','summary':'Actual due-driven isolated slot; cap four requests across lanes. Work is checkpointed between slots; this is not a legacy A-D deployment.'})
    # Aggregate counts update without falsely advancing a whole-day freshness time.
    for table,field in [('rings','ring'),('days','day')]:
     for key,row in allrows[table].items():
      count=sum(c.get('entry-count',0) for c in allrows['classes'].values() if [table,key] in c[field]['$links']);put(table,{primary[table]:key,'trip-count':count})
    for t,rows in allrows.items():
     for key,row in rows.items():
      for v in row.values():
       if isinstance(v,dict) and '$links' in v:
        for target,k in v['$links']:assert k in allrows[target],(t,target,k)
    active=[r for r in allrows['trips'].values() if r.get('record-status','active')=='active']
    assert len(active)==sum(c.get('entry-count',0) for c in allrows['classes'].values()),'Active trip count must equal latest class counts'
    put('runs',{'run-key':run,'records':sum(len(r) for r in delta.values())})
    primary['results']='result-key';allrows.setdefault('results',{});delta['results']={};observed_classes={o['class_key'] for o in trial['observations']}
    result_fields={'placing':'placing','position':'position',**{f'score{i}':f'score{i}' for i in range(1,7)},**{f'time-{word}':f'time_{word}' for word in ['one','two','three']},**{f'faults-{word}':f'faults_{word}' for word in ['one','two','three']},**{f'time-fault-{word}':f'time_fault_{word}' for word in ['one','two','three']},'points-earned':'points_earned','prize-money':'total_prize_money','gone-in':'gone_in','scratch':'scratch_trip'}
    for key,t in allrows['trips'].items():
     if t['class']['$links'][0][1] not in observed_classes:continue
     raw=json.loads(t['source-json']);ck=t['class']['$links'][0][1]
     values={name:raw.get(source) for name,source in result_fields.items()}
     put('results',{'result-key':key,'trip':links('trips',key),'class':t['class'],'entry':t['entry'],'class-status':allrows['classes'][ck]['status'],**{name:text(value) for name,value in values.items()},'source-json':packed(values),'last-success':t['last-success'],'last-run':t['last-run']})
    put('runs',{'run-key':run,'records':sum(len(r) for r in delta.values())})
    (ROOT/'delta-graph.json').write_text(json.dumps({t:list(rows.values()) for t,rows in delta.items()},separators=(',',':')))
    (ROOT/'latest-graph.json').write_text(json.dumps({t:list(rows.values()) for t,rows in allrows.items()},separators=(',',':')))
    print(json.dumps({'delta_counts':{t:len(r) for t,r in delta.items()},'latest_counts':{t:len(r) for t,r in allrows.items()},'active_trips':len(active),'dropped_trips':removed_trips,'processed_trip_observations':changed_trips},indent=2))

if __name__=='__main__':
    main()
