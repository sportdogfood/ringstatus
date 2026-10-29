"""Normalize the verified source capture into the isolated wef-data graph.
References are symbolic until resolved from actual Airtable record IDs.
"""
import argparse,collections,datetime,hashlib,json,pathlib,uuid
from zoneinfo import ZoneInfo

def inventory_graph(payloads,context,existing,observed):
    """Reuse dated schedule identities; inventory never claims class success."""
    sk=context['show-key'];sid=context['show-id'];out={t:{} for t in ('shows','days','rings','groups','classes','schedules','work-items')}
    def refs(t,*ks):return {'$links':[[t,str(k)] for k in ks if k is not None]}
    def add(t,row):out[t][row[t[:-1]+'-key' if t not in ('classes','entries','days','work-items') else {'classes':'class-key','entries':'entry-key','days':'day-key','work-items':'work-key'}[t]]]=row
    out['shows'][sk]={'show-key':sk,'show-id':sid,'customer-id':15,'show-name':context['show-name'],
        'start-date':context['start-date'],'end-date':context['end-date'],'selected-date':context['selected-date']}
    for p in payloads:
        day=p.get('show_date');rings=p.get('rings')
        if p.get('show',{}).get('show_id')!=sid or not isinstance(rings,list):raise ValueError('Invalid inventory scope')
        dk=f'{sk}|{day}';add('days',{'day-key':dk,'show':refs('shows',sk),'date':day})
        for ring in rings:
            rn=ring['ring_number'];rk=f'{dk}|{rn}'
            add('rings',{'ring-key':rk,'show':refs('shows',sk),'day':refs('days',dk),'ring-id':ring.get('ring_id'),'ring-number':rn,'ring-name':ring['ring_name']})
            if not isinstance(ring.get('classes'),list):raise ValueError('Missing schedule occurrence array')
            for pos,c in enumerate(ring['classes']):
                gid=c.get('class_group_id');gk=f'{rk}|{gid}' if gid is not None else None
                if gk:add('groups',{'group-key':gk,'ring':refs('rings',rk),'group-id':gid,'group-name':c.get('group_name'),
                    'source-json':json.dumps({k:v for k,v in c.items() if k.startswith('class_group') or k in ('group_name','class_list')},sort_keys=True,separators=(',',':')),'last-success':observed})
                cid=c.get('class_id');ck=f'{rk}|{cid}' if cid is not None and not c.get('schedule_break') else None
                if not ck and not c.get('schedule_break'):raise ValueError('Non-break class missing source ID')
                schedulekey=f'{rk}|'+(str(c['class_groupxclasses_id']) if c.get('class_groupxclasses_id') is not None else f'position-{pos}')
                add('schedules',{'schedule-key':schedulekey,'show':refs('shows',sk),'day':refs('days',dk),'ring':refs('rings',rk),
                    'group':refs('groups',gk),'class':refs('classes',ck),'kind':'break' if c.get('schedule_break') else 'class',
                    'source-position':pos,'source-id':str(c['class_groupxclasses_id']) if c.get('class_groupxclasses_id') is not None else None,
                    'name':c.get('class_name') or c.get('group_name'),'start-time':c.get('estimated_start_time') or c.get('start_time_default'),
                    'end-time':c.get('estimated_end_time'),'source-json':json.dumps(c,sort_keys=True,separators=(',',':')),'last-success':observed})
                if not ck:continue
                prior=existing.get('classes',{}).get(ck,{})
                row=out['classes'].setdefault(ck,{'class-key':ck,'show':refs('shows',sk),'day':refs('days',dk),
                    'ring':refs('rings',rk),'group':refs('groups'),'class-id':cid,'class-number':str(c['class_number']),'class-name':c['class_name']})
                if gk and ['groups',gk] not in row['group']['$links']:row['group']['$links'].append(['groups',gk])
                if ck not in existing.get('work-items',{}):
                    state={'day':day,'ring':rn,'class_id':cid,'class_key':ck,'last_success':None,'last_change':None,
                        'fingerprint':None,'status':prior.get('status','Unknown'),'interval':1800,'unchanged':0,'failures':0,'revision':0,'lane':'other','next_due':0}
                    add('work-items',{'work-key':ck,'class':refs('classes',ck),'lane':refs('lanes','other'),'status':'Waiting','priority':50,
                        'interval-seconds':1800,'next-due':'1970-01-01T00:00:00Z','state-json':json.dumps(state,separators=(',',':'))})
    return {t:list(rows.values()) for t,rows in out.items() if rows}

def main():
    parser=argparse.ArgumentParser();parser.add_argument('--evidence-root',type=pathlib.Path,required=True);parser.add_argument('--run-key');args=parser.parse_args()
    ROOT=args.evidence_root.resolve()
    if (ROOT/'graph.json').exists():raise ValueError('Refusing to overwrite an existing graph')
    BASE=ROOT/'baseline';report=json.loads((BASE/'results.json').read_text(encoding='utf-8'))
    assert report['status']=='PASS' and not report['errors']
    source=BASE/'source';discovery=json.loads((source/'discovery.json').read_text(encoding='utf-8'));show=discovery['show'];sid=int(show['show_id']);customer=15
    run=args.run_key or 'baseline-'+report['started'];now=report['completed'];today=datetime.datetime.now(ZoneInfo('America/New_York')).date().isoformat()
    graph=collections.OrderedDict((k,[]) for k in ['runs','shows','days','rings','groups','classes','schedules','horses','riders','trainers','owners','entries','trips','lanes','work-items','slots','collections','requests'])
    def links(table,*keys):return {'$links':[[table,str(k)] for k in keys if k is not None]}
    def add(table,fields):graph[table].append({k:v for k,v in fields.items() if v is not None});return fields
    def compact(x):return json.dumps(x,separators=(',',':'),sort_keys=True,ensure_ascii=False)
    def digest(x):return hashlib.sha256(compact(x).encode()).hexdigest()
    def stamp_after(stamp,seconds):return (datetime.datetime.fromisoformat(stamp)+datetime.timedelta(seconds=seconds)).isoformat()
    def text(x):return None if x is None else str(x)
    def num(x):
     try:return int(x) if x not in [None,''] else None
     except (ValueError,TypeError):return None
    sk=f'{customer}|{sid}'
    add('runs',{'run-key':run,'mode':'Live complete-show baseline','status':'Source complete; awaiting Airtable readback','started':report['started'],'completed':report['completed'],'source-requests':len(report['requests']),'source-bytes':report['bytes'],'errors':0,'elapsed-seconds':report['elapsed_seconds'],'summary':'Fresh GET-only decoded source collection. Airtable status will be confirmed by independent readback; no production collector changed.'})
    add('shows',{'show-key':sk,'show-id':sid,'customer-id':customer,'facility-id':show['facility_id'],'show-name':show['show_name'],'start-date':show['start_date'][:10],'end-date':show['end_date'][:10],'selected-date':discovery['show_date'],'last-success':now,'last-run':links('runs',run),'status':'Collected; awaiting readback'})
    details={(d['day'],d['ring'],d['class_id']):d for d in report['details']}
    request_by_class={int(r['route'].split('/')[-1]):r for r in report['requests'] if r['route'].startswith('/classes/')}
    entities={k:{} for k in ['groups','horses','riders','trainers','owners','entries']};class_rows={};work_rows={};ring_keys={};day_keys={};class_keys={};collection_keys={};slot_keys={};warnings=[]
    policies={
     'inventory':('Inventory',50,300,120,600,'Dated schedule membership and source-selected scope. Revalidate all dates; prioritize today.'),
     'current':('Current activity',10,90,60,180,'Today underway or approaching. Promote on source change; stable interval bounded at 180s.'),
     'today':('Today coverage',30,300,120,900,'Today remaining coverage. Deadline escalation prevents starvation.'),
     'other':('Other dates',50,1800,600,7200,'Other in-range dates. Promote as date/start approaches.'),
     'completed':('Completed corrections',60,3600,900,14400,'Completed classes still revalidate for result and entry corrections.'),
     'reconcile':('Complete reconciliation',70,7200,1800,14400,'Complete show sweep, resumable and deduplicated against qualifying coverage reads.'),
     'recovery':('Recovery',20,60,30,300,'Bounded failed-unit retry; retains last success and shares the global concurrency budget.')}
    for key,(name,priority,interval,minimum,maximum,rules) in policies.items():add('lanes',{'lane-key':key,'name':name,'priority':priority,'interval-seconds':interval,'minimum-seconds':minimum,'maximum-seconds':maximum,'concurrency':4,'policy-status':'Isolated trial seed; not approved production cadence','rules':rules+' Four requests maximum across ALL lanes, not four per lane. These trial values are proposals derived for evaluation, not owner-mandated intervals.'})
    def classify(day,status):
     if day==today and status=='Underway':return 'current'
     if status=='Completed':return 'completed'
     return 'today' if day==today else 'other'
    for day_report in report['days']:
     day=day_report['day'];dk=f'{sk}|{day}';day_keys[day]=dk;p=json.loads((source/f'schedule-{day}.json').read_text(encoding='utf-8'));daydetails=[d for d in report['details'] if d['day']==day]
     add('days',{'day-key':dk,'show':links('shows',sk),'date':day,'status':'Collected; awaiting readback','expected-classes':len(daydetails),'collected-classes':len(daydetails),'trip-count':sum(d['rows'] for d in daydetails),'last-success':now,'last-run':links('runs',run)})
     for ring in p['rings']:
      rn=ring['ring_number'];rk=f'{dk}|{rn}';ring_keys[(day,rn)]=rk;rd=[d for d in daydetails if d['ring']==rn]
      add('rings',{'ring-key':rk,'show':links('shows',sk),'day':links('days',dk),'ring-id':ring.get('ring_id'),'ring-number':rn,'ring-name':ring['ring_name'],'status':'Collected; awaiting readback','expected-classes':len(rd),'collected-classes':len(rd),'trip-count':sum(d['rows'] for d in rd),'last-success':now,'last-run':links('runs',run)})
      slot=f'{run}|{day}|ring-{rn}';slot_keys[(day,rn)]=slot
      metrics=[request_by_class[d['class_id']] for d in rd];timing=next(x for x in report['ring_timings'] if x['day']==day and x['ring']==rn)
      slotrow={'slot-key':slot,'run':links('runs',run),'lane':links('lanes','reconcile'),'sequence':len(graph['slots'])+1,'elapsed-seconds':timing['seconds'],'request-count':len(rd),'trip-count':sum(d['rows'] for d in rd),'status':'Completed source reads','summary':'Observed baseline ring pass; not mapped to legacy A-D slots. At most four requests in flight.'}
      if metrics:slotrow.update(started=min(m['requested_at'] for m in metrics),completed=max(m['completed_at'] for m in metrics))
      add('slots',slotrow)
      for pos,c in enumerate(ring['classes']):
       gid=c.get('class_group_id');gk=f'{rk}|{gid}' if gid is not None else None
       if gk and gk not in entities['groups']:entities['groups'][gk]={'group-key':gk,'ring':links('rings',rk),'group-id':gid,'group-name':c.get('group_name'),'sequence':c.get('class_group_sequence'),'source-json':compact({k:v for k,v in c.items() if k.startswith('class_group') or k in ['group_name','class_list']}),'last-success':now}
       cid=c.get('class_id');ck=f'{rk}|{cid}' if cid is not None and not c.get('schedule_break') else None
       schedulekey=f'{rk}|'+(str(c.get('class_groupxclasses_id')) if c.get('class_groupxclasses_id') is not None else f'position-{pos}')
       add('schedules',{'schedule-key':schedulekey,'show':links('shows',sk),'day':links('days',dk),'ring':links('rings',rk),'group':links('groups',gk),'class':links('classes',ck),'kind':'break' if c.get('schedule_break') else 'class','source-position':pos,'source-id':text(c.get('class_groupxclasses_id')),'name':c.get('class_name') or c.get('group_name'),'start-time':c.get('estimated_start_time') or c.get('start_time_default'),'end-time':c.get('estimated_end_time'),'source-json':compact(c),'last-success':now})
       if not ck:continue
       if ck in class_rows:
        # One source class can be scheduled in several groups. Preserve every
        # schedule occurrence and group link, but fetch/store its detail only once.
        if gk and ['groups',gk] not in class_rows[ck]['group']['$links']:class_rows[ck]['group']['$links'].append(['groups',gk])
        continue
       raw=json.loads((source/f'classes/{day}-{rn}-{cid}.json').read_text(encoding='utf-8'));related=raw['class_related_data'];d=details[(day,rn,cid)];metric=request_by_class[cid];fingerprint=digest(raw);class_keys[cid]=ck
       cls=add('classes',{'class-key':ck,'show':links('shows',sk),'day':links('days',dk),'ring':links('rings',rk),'group':links('groups',gk),'class-id':cid,'class-number':text(c['class_number']),'class-name':c['class_name'],'status':related['status'],'entry-count':len(raw['trips']),'fingerprint':fingerprint,'last-success':metric['completed_at'],'last-change':metric['completed_at'],'last-run':links('runs',run),'source-json':compact(raw)});class_rows[ck]=cls
       lane=classify(day,related['status']);policy=policies[lane];wk=ck;state={'day':day,'ring':rn,'class_id':cid,'class_key':ck,'last_success':metric['completed_at'],'last_change':metric['completed_at'],'fingerprint':fingerprint,'status':related['status'],'interval':policy[2],'unchanged':0,'failures':0,'revision':1,'lane':lane}
       wr=add('work-items',{'work-key':wk,'class':links('classes',ck),'lane':links('lanes',lane),'last-run':links('runs',run),'status':'Waiting','priority':policy[1],'interval-seconds':policy[2],'estimated-seconds':metric['seconds'],'last-success':metric['completed_at'],'last-change':metric['completed_at'],'next-due':stamp_after(metric['completed_at'],policy[2]),'unchanged-count':0,'failure-count':0,'fingerprint':fingerprint,'plan-revision':1,'reason':f'Baseline evaluated: {day}; {related["status"]}; {len(raw["trips"])} trips. Trial seed, no permanent activation.','state-json':compact(state)});work_rows[wk]=wr
       for t in raw['trips']:
        eid=int(t['entry_id']);hid=int(t['horse_id']);rid=int(t['rider_id']);tid=num(t.get('trainer_id'));oid=num(t.get('owner_id'));ek=f'{sid}|{eid}';hk=str(hid);rrk=str(rid);tk=str(tid) if tid and tid>0 else None
        for table,key,idname,namefield,name in [('horses',hk,'horse-id','horse-name',t.get('horse')),('riders',rrk,'rider-id','rider-name',t.get('rider_name') or t.get('rider')),('trainers',tk,'trainer-id','trainer-name',t.get('trainer'))]:
         if key:
          prior=entities[table].get(key)
          if prior and prior.get(namefield) and name and prior[namefield]!=name:warnings.append({'table':table,'key':key,'previous':prior[namefield],'observed':name})
          entities[table][key]={table[:-1]+'-key':key,idname:int(key),namefield:name,'last-success':metric['completed_at']}
        ok=str(oid) if oid and oid>0 else None
        if not ok and t.get('Owner'):ok=f'unknown-id|{ck}|{eid}|{t.get("trip_id") or t.get("entryxclasses_uuid")}'
        if ok:entities['owners'][ok]={'owner-key':ok,'owner-id':str(oid) if oid and oid>0 else None,'owner-name':t.get('Owner'),'identity-source':'source owner_id' if oid and oid>0 else 'Source occurrence; owner_id absent; not merged by name','last-success':metric['completed_at']}
        e=entities['entries'].setdefault(ek,{'entry-key':ek,'show':links('shows',sk),'entry-id':eid,'entry-number':text(t.get('number')),'horse':links('horses',hk),'riders':links('riders'),'trainers':links('trainers'),'owners':links('owners'),'last-success':metric['completed_at']})
        assert e['horse']==links('horses',hk),'Entry points to conflicting horse'
        for field,table,key in [('riders','riders',rrk),('trainers','trainers',tk),('owners','owners',ok)]:
         if key and [table,key] not in e[field]['$links']:e[field]['$links'].append([table,key])
        e['last-success']=max(e['last-success'],metric['completed_at'])
        tripkey=f'{ck}|{eid}|{t.get("entryxclasses_uuid") or t.get("trip_id") or ""}'
        add('trips',{'trip-key':tripkey,'class':links('classes',ck),'entry':links('entries',ek),'horse':links('horses',hk),'rider':links('riders',rrk),'trainer':links('trainers',tk),'owner':links('owners',ok),'trip-id':text(t.get('trip_id')),'source-uuid':t.get('entryxclasses_uuid'),'order-of-go':num(t.get('order_of_go')),'actual-order':num(t.get('actual_order')),'gone-in':text(t.get('gone_in')),'placing':text(t.get('placing')),'scratch':text(t.get('scratch_trip')),'estimated-go-time':text(t.get('estimated_go_time')),'source-json':compact(t),'fingerprint':digest(t),'last-success':metric['completed_at'],'last-run':links('runs',run)})
    for table,items in entities.items():graph[table]=[{k:v for k,v in f.items() if v is not None} for f in items.values()]
    for slot in graph['slots']:
     matched=[w['work-key'] for w in graph['work-items'] if w['work-key'].startswith(slot['slot-key'].split('|ring-')[0].split(run+'|')[-1])]
     # Exact day/ring match through source state, not prefix/name inference.
     day=slot['slot-key'].split('|')[-2];rn=int(slot['slot-key'].split('|ring-')[-1]);slot['work-items']=links('work-items',*[k for k,w in work_rows.items() if json.loads(w['state-json'])['day']==day and json.loads(w['state-json'])['ring']==rn])
    def collection(key,scope,expected,day=None,rn=None,cid=None):
     ck=class_keys.get(cid);fields={'collection-key':key,'run':links('runs',run),'show':links('shows',sk),'day':links('days',day_keys.get(day)),'ring':links('rings',ring_keys.get((day,rn))),'class':links('classes',ck),'scope':scope,'status':'Source complete; readback pending','expected-records':expected,'collected-records':expected,'started':report['started'],'completed':report['completed']};add('collections',fields);return key
    collection(run+'|show','show',len(report['details']))
    for day in day_keys:
     dd=[d for d in report['details'] if d['day']==day];collection(run+'|'+day,'day',len(dd),day)
     for (rd,rn),rk in ring_keys.items():
      if rd==day:collection(run+'|'+day+'|'+str(rn),'ring',len([d for d in dd if d['ring']==rn]),day,rn)
    for d in report['details']:
     key=run+'|class|'+str(d['class_id']);collection_keys[d['class_id']]=collection(key,'class',d['rows'],d['day'],d['ring'],d['class_id'])
    for i,m in enumerate(report['requests']):
     cid=int(m['route'].split('/')[-1]) if m['route'].startswith('/classes/') else None;ck=class_keys.get(cid);dd=next((d for d in report['details'] if d['class_id']==cid),None)
     add('requests',{'request-key':f'{run}|{i:04}','run':links('runs',run),'slot':links('slots',slot_keys.get((dd['day'],dd['ring'])) if dd else None),'work-item':links('work-items',ck),'collection':links('collections',collection_keys.get(cid)),'route':m['route'],'params':compact(m['params']),'http':int(m['http']),'elapsed-seconds':m['seconds'],'bytes':m['bytes'],'sha256':m['sha256'],'started':m['requested_at'],'completed':m['completed_at'],'status':'Validated' if int(m['http'])==200 else 'Failed transport; recovery recorded separately'})
    for table,rows in graph.items():
     keyfield=rows[0] and next(iter(rows[0])) if rows else None
     if keyfield:assert len({r[keyfield] for r in rows})==len(rows),(table,'duplicate stable keys')
     for row in rows:
      for value in row.values():
       if isinstance(value,dict) and '$links' in value:
        for target,key in value['$links']:
         assert any(str(next(iter(r.values())))==key for r in graph[target]),(table,target,key)
    graph['runs'][0]['records']=sum(len(v) for v in graph.values())
    (ROOT/'graph.json').write_text(json.dumps(graph,separators=(',',':')))
    (ROOT/'identity-name-warnings.json').write_text(json.dumps(warnings,indent=2))
    print(json.dumps({'counts':{k:len(v) for k,v in graph.items()},'total':sum(map(len,graph.values())),'name_variations':len(warnings)},indent=2))

if __name__=='__main__':
    main()
