"""One scheduled day: synced priority1, schedules, serial class details, save, report."""
import datetime,json,pathlib,subprocess,sys,uuid,os,time,base64,argparse
from wef_data_airtable import Airtable,error_text
from wef_data_store import DataStore,DATA_BASE
from record_mappings import packed,trip_records
from day_failures import Failures
from fast_collector import Source,synced_source,select_class_details,schedule_payload,atomic,FLAGS_TABLE,FLAG_NAME,FLAG_SORT


def collect(root,day_number=1,mode="normal"):
    """One selected show-day; the scheduler supplies cadence and offset."""
    started=datetime.datetime.now(datetime.timezone.utc);errors=[];counts={};api=Airtable()
    store=DataStore(api,json.loads((root/'schema.json').read_text(encoding='utf-8')),cache=True)
    def save(table,rows):
        rows=[{k:v for k,v in r.items() if not(isinstance(v,dict) and '$links' in v)} for r in rows]
        for offset in range(0,len(rows),10):
            batch=rows[offset:offset+10]
            try:
                if mode!='normal' and 'last-success' in store.fields[table]:
                    clock=store.fields[table]['last-success']['id'];current=store.inventory(table,refresh=True,keys=[str(r[store.primary[table]]) for r in batch])
                    batch=[r for r in batch if not ((r.get('last-success') or (at if table=='classes' else None)) and current.get(str(r[store.primary[table]]),{}).get('fields',{}).get(clock)
                        and datetime.datetime.fromisoformat(current[str(r[store.primary[table]])]['fields'][clock].replace('Z','+00:00'))>datetime.datetime.fromisoformat((r.get('last-success') or at).replace('Z','+00:00')))]
                    if not batch:continue
                keys=list(dict.fromkeys(str(r[store.primary[table]]) for r in batch))
                ids={k:r['id'] for k,r in store.inventory(table).items()}
                missing=[k for k in keys if k not in ids]
                if missing:ids.update(store.reserve(table,missing))
                records=[{'id':ids[str(r[store.primary[table]])],'fields':store.fields_for(table,r,{table:ids})} for r in batch]
                response=api.request('PATCH',DATA_BASE,store.schema[table]['id'],body={'records':records})
                acknowledged={r['id'] for r in response['records']}
                counts[table]=counts.get(table,0)+sum(r['id'] in acknowledged for r in records)
                if len(acknowledged)!=len(records):raise ValueError('Write response missing records')
            except Exception as e:
                message=error_text(e);errors.append({'key':table,'error':message})
                if table=='classes' and 'Airtable HTTP 422' in message and 'Field "source-json" cannot accept' in message:
                    save(table,[{k:v for k,v in row.items() if k!='source-json'} for row in batch])
    show_table,sf=synced_source('w-shows',root/'schema.json');day_table,df=synced_source('w-days',root/'schema.json')
    shows=api.records(DATA_BASE,show_table);owner=next(r['fields'] for r in shows if r['fields'].get(sf['is_default']) is True)
    show=owner[sf['show-key']];sid=owner[sf['show_id']];customer=owner[sf['customer_id']]
    selected=[r['fields'] for r in api.records(DATA_BASE,day_table) if r['fields'].get(df['show'])==show and r['fields'].get(df['day-priority'])==day_number and owner[sf['start_date']][:10]<=r['fields'][df['date']][:10]<=owner[sf['end_date']][:10]]
    if not selected:return {'ok':True,'state':'Skipped','counts':{},'item-errors':[]}
    day=selected[0][df['date']][:10];source=Source(root/'runtime'/('day'+str(day_number))/'responses',day=day);params={'customer_id':customer,'show_id':sid}
    runkey='collection-'+os.environ.get('WEF_LOG_ATTEMPT_ID',str(uuid.uuid4()))+'|'+day
    try:failures=Failures(api,root,show,day)
    except Exception as e:
        failures=None;errors.append({'key':'Failure logging','error':error_text(e)})
    pending=failures.pending() if mode!='normal' and failures else {}
    if mode!='normal' and not pending:return {'ok':not errors,'state':'Skipped' if not errors else 'Partial','counts':{},'item-errors':errors}
    save('runs',[{'run-key':runkey,'mode':'Scheduled day-'+str(day_number)+' collection','started':started.isoformat(),'status':'Started'}])
    requested=0;reprocessed=0;schedule_requests=0;skips=[]
    def finish():
        completed=datetime.datetime.now(datetime.timezone.utc).isoformat()
        saved=sum(v for t,v in counts.items() if t!='runs')
        save('runs',[{'run-key':runkey,'mode':'Scheduled day-'+str(day_number)+' collection','started':started.isoformat(),'completed':completed,'status':'Partial' if errors else 'Succeeded','records':saved,'errors':len(errors),'elapsed-seconds':(datetime.datetime.now(datetime.timezone.utc)-started).total_seconds()}])
        if mode=='normal':
            state_path=root/'runtime/day-cadence.json'
            state=json.loads(state_path.read_text(encoding='utf-8')) if state_path.exists() else {}
            state[show+'|'+day]={'attempted':started.timestamp(),'completed':completed,'priority':day_number,'status':'Partial' if errors else 'Saved','run-key':runkey}
            atomic(state_path,state)
        return {'ok':not errors,'state':'Saved' if not errors else 'Partial','show-key':show,'day':day,'run-key':runkey,'counts':{'saved':saved,'by-table':counts,'schedule-requested':schedule_requests,'class-requested':requested,'class-skipped':len(skips),'reprocessed':reprocessed,'day-number':day_number,'mode':mode,'elapsed-seconds':(datetime.datetime.now(datetime.timezone.utc)-started).total_seconds()},'item-errors':errors}

    def observed(route,metric,error=None,save_failed=False):
        if failures is None:return
        try:failures.record(route,metric,runkey,error,save_failed)
        except Exception as e:errors.append({'key':'Failure logging '+route,'error':error_text(e)})
    def read_saved(route):
        evidence=failures.evidence(route)
        capture=json.loads(pathlib.Path(evidence['capture']).read_text(encoding='utf-8'))
        outer=json.loads(base64.b64decode(capture['stdout-base64']))
        if not outer.get('ok'):raise ValueError('Saved response is a source failure; requires a source rerun')
        body=json.loads(base64.b64decode(outer['body']));metric=outer['metric']
        metric.update({'saved-response':evidence['capture'],'params':{**params,**({'date':day} if route=='/schedule' else {})}})
        return body,metric
    refresh_schedule=mode=='normal' or '/schedule' in pending
    if refresh_schedule:
        try:
            if mode!='normal' and pending['/schedule']['fields'].get(failures.fields['status'])=='Save failed':
                payload,metric=read_saved('/schedule');reprocessed+=1
            else:
                schedule_requests+=1;payload,metric=schedule_payload(source,{**params,'date':day})
            at=metric['completed']
        except Exception as e:
            metric={**getattr(e,'diagnostic',{}),'params':{**params,'date':day}}
            metric.setdefault('completed',datetime.datetime.now(datetime.timezone.utc).isoformat())
            errors.append({'key':'/schedule','error':error_text(e)})
            observed('/schedule',metric,error_text(e))
            return finish()
    else:at=started.isoformat()
    if refresh_schedule:
        before_schedule=len(errors)
        rings={};groups={};classes={};schedules={}
        for ring in payload['rings']:
            rk=show+'|'+day+'|'+str(ring['ring_number'])
            rings[rk]={'ring-key':rk,'ring-id':ring.get('ring_id'),'ring-number':ring['ring_number'],'ring-name':ring.get('ring_name'),'last-success':at}
            for pos,item in enumerate(ring['classes']):
                gid=item.get('class_group_id');gk=rk+'|'+str(gid) if gid is not None else None
                if gk:groups[gk]={'group-key':gk,'group-id':gid,'group-name':item.get('group_name'),'source-json':packed({k:v for k,v in item.items() if k.startswith('class_group') or k in ('group_name','class_list')}),'last-success':at}
                cid=item.get('class_id');key=rk+'|'+str(cid) if cid is not None and not item.get('schedule_break') else None
                sk=rk+'|'+(str(item['class_groupxclasses_id']) if item.get('class_groupxclasses_id') is not None else 'position-'+str(pos))
                schedules[sk]={'schedule-key':sk,'kind':'break' if item.get('schedule_break') else 'class','source-position':pos,'source-id':str(item['class_groupxclasses_id']) if item.get('class_groupxclasses_id') is not None else None,'name':item.get('class_name') or item.get('group_name'),'start-time':item.get('estimated_start_time') or item.get('start_time_default'),'end-time':item.get('estimated_end_time'),'source-json':packed(item),'last-success':at,'total-trips':item.get('total_trips'),'record-status':'Active — no entries' if not item.get('schedule_break') and item.get('total_trips')==0 else 'Active'}
                if key:classes[key]={'class-key':key,'class-id':cid,'class-number':str(item['class_number']) if item.get('class_number') is not None else None,'class-name':item.get('class_name'),'record-status':'Active'}
        prior=store.inventory('classes');prior_schedules=store.inventory('schedules')
        for table,rows in [('rings',rings),('groups',groups),('classes',classes),('schedules',schedules)]:save(table,list(rows.values()))
        records=[{'id':k,'fields':r} for k,r in schedules.items()]
        evaluated=subprocess.run(['node',str(root/'schedule-priority.js')],input=packed(records),text=True,capture_output=True,check=True,encoding='utf-8')
        evaluation=json.loads(evaluated.stdout);priorities={r['id']:r['fields'] for r in evaluation['updates']}
        save('schedules',[{'schedule-key':key,**fields,'last-success':at} for key,fields in priorities.items()])
        observed('/schedule',metric,errors[before_schedule]['error'] if len(errors)>before_schedule else None,len(errors)>before_schedule)
    if mode!='normal':
        prior=store.inventory('classes',refresh=True);prior_schedules=store.inventory('schedules',refresh=True)
        def authored(table,row):return {name:row['fields'][f['id']] for name,f in store.fields[table].items() if f['id'] in row['fields']}
        prefix=show+'|'+day+'|'
        classes={k:authored('classes',r) for k,r in prior.items() if k.startswith(prefix)}
        schedules={k:authored('schedules',r) for k,r in prior_schedules.items() if k.startswith(prefix)}
        priorities={k:{name:r.get(name) for name in ('rs-flag','rs-lever','rs-priority')} for k,r in schedules.items()}
    flags={r['id']:r['fields'] for r in api.records(DATA_BASE,FLAGS_TABLE,fields=[FLAG_NAME,FLAG_SORT])}
    rows,skips=select_class_details(list(classes.values()),schedules,prior,store.fields['classes'],prior_schedules,flags,priorities)
    if mode!='normal':
        rows=[r for r in rows if '/classes/'+str(r['class-id']) in pending]
        replay=[r for k,r in classes.items() if k not in {x['class-key'] for x in rows}
                and '/classes/'+str(r['class-id']) in pending
                and pending['/classes/'+str(r['class-id'])]['fields'].get(failures.fields['status'])=='Save failed']
        rows.extend(replay)
    requested=0
    for row in rows:
        key=row['class-key'];route='/classes/'+str(row['class-id']);before=len(errors);metric={}
        replay=mode!='normal' and pending.get(route,{}).get('fields',{}).get(failures.fields['status'])=='Save failed' 
        try:
            if replay:
                body,metric=read_saved(route);reprocessed+=1
            else:
                requested+=1;body,metric=source.fetch(route,params)
            observed_at=metric['completed']
            if body.get('class',{}).get('class_id')!=row['class-id']:raise ValueError('Returned class does not match request')
            status=body.get('class_related_data',{}).get('status')
            if day_number==0 and body.get('class_group_order_of_go') is not None:
                group_file=root.parent.parent/'ringstatus-data'/'wef-data'/'shows'/str(customer)/str(sid)/'day0-order-of-go'/day/(str(row['class-id'])+'.json')
                try:
                    current_clock=prior.get(key,{}).get('fields',{}).get(store.fields['classes']['last-success']['id'])
                    newer_archive=mode!='normal' and group_file.exists() and current_clock and datetime.datetime.fromisoformat(current_clock.replace('Z','+00:00'))>datetime.datetime.fromisoformat(observed_at.replace('Z','+00:00'))
                    if not newer_archive:atomic(group_file,body['class_group_order_of_go'])
                except Exception as e:errors.append({'key':'Order-of-go archive '+key,'error':error_text(e)})
            class_body={k:v for k,v in body.items() if k!='class_group_order_of_go'}
            save('classes',[{'class-key':key,'source-json':packed(class_body),'last-success':observed_at,**({'status':status} if status is not None else {}),**({'entry-count':body['total_entry_trips']} if body.get('total_entry_trips') is not None else {})}])
            graph={t:{} for t in ('entries','horses','riders','trainers','owners','trips','results')}
            for trip in body['trips']:
                for table,values in trip_records(trip,key,status,observed_at).items():
                    for value in values:
                        if table in ('horses','riders','trainers','owners'):
                            value={k:v for k,v in value.items() if v is not None and v!=''}
                        graph[table][str(value[store.primary[table]])]=value
            for table,values in graph.items():save(table,list(values.values()))
            observed(route,metric,errors[before]['error'] if len(errors)>before else None,len(errors)>before)
        except Exception as e:
            errors.append({'key':key,'error':error_text(e)})
            metric={**getattr(e,'diagnostic',metric),'params':params}
            metric.setdefault('completed',datetime.datetime.now(datetime.timezone.utc).isoformat())
            observed(route,metric,error_text(e),replay)
    return finish()



def rerun(root):
    api=Airtable()
    table,fields=synced_source('w-days',root/'schema.json')
    show_table,sf=synced_source('w-shows',root/'schema.json')
    owner=next(r['fields'] for r in api.records(DATA_BASE,show_table) if r['fields'].get(sf['is_default']) is True)
    priorities=sorted({int(r['fields'][fields['day-priority']]) for r in api.records(DATA_BASE,table)
        if r['fields'].get(fields['show'])==owner[sf['show-key']] and r['fields'].get(fields['day-priority']) in range(6)})
    reports=[collect(root,p,'rerun') for p in priorities]
    errors=[{**e,'day':r.get('day')} for r in reports for e in r.get('item-errors',[])]
    work=any(r['counts'].get('saved') or r['counts'].get('schedule-requested') or r['counts'].get('class-requested') or r['counts'].get('reprocessed') for r in reports)
    return {'ok':not errors,'state':'Partial' if errors else 'Saved' if work else 'Skipped','show-key':owner[sf['show-key']],
        'run-key':'collection-'+os.environ.get('WEF_LOG_ATTEMPT_ID','rerun'),
        'counts':{'saved':sum(r['counts'].get('saved',0) for r in reports),'days':reports},'item-errors':errors}


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--day',type=int,default=1);parser.add_argument('--rerun',action='store_true');args=parser.parse_args()
    root=pathlib.Path(__file__).resolve().parent
    try:result=rerun(root) if args.rerun else collect(root,args.day)
    except Exception as e:result={'ok':False,'state':'Partial','counts':{},'item-errors':[{'key':'Collector','error':error_text(e)}]}
    print(json.dumps(result))
