"""One day: prepare source lanes, collect once, save independently, retain existing keys."""
import base64,datetime,json,os,pathlib,subprocess,time,uuid
from fast_collector import atomic,capture_safe,synced_source,select_class_details,FLAGS_TABLE,FLAG_NAME,FLAG_SORT
from wef_data_airtable import Airtable,error_text
from wef_data_store import DataStore,DATA_BASE
from record_mappings import packed,refs,trip_records

def now():return datetime.datetime.now(datetime.timezone.utc).isoformat()
def raw(value):
    try:return json.loads(value or '{}')
    except (ValueError,TypeError):return {}

class Reader:
    def __init__(self,root,day):self.root=root;self.dir=root/'runtime'/('day'+str(day))/'responses';self.lane=root/'runtime'/('day'+str(day))/'class-lane.json'
    def fetch(self,route,params,prepare=False):
        command=['node',str(self.root/'fast-source.js'),route,packed(params),'prepare' if prepare else 'once',str(self.lane)]
        p=subprocess.run(command,capture_output=True,text=True,encoding='utf-8')
        if p.returncode:raise RuntimeError(p.stderr.strip() or 'Source reader failed')
        response=json.loads(p.stdout);metric=response.get('metric',{})
        body=json.loads(base64.b64decode(response['body'])) if response.get('ok') else None
        capture={'ok':bool(response.get('ok')),'payload':body,'metric':metric,'attempts':response.get('attempts',[]),'error':response.get('error')}
        file=self.dir/(str(uuid.uuid4())+'.json');atomic(file,capture_safe(capture));metric['capture']=str(file)
        if not response.get('ok'):
            e=RuntimeError(response.get('error','Source request failed'));e.metric=metric;raise e
        return body,metric

def schedule_rows(payload,show,day):
    rings={};groups={};classes={};schedules={}
    for ring in payload['rings']:
        rk=show+'|'+day+'|'+str(ring['ring_number'])
        rings[rk]={'ring-key':rk,'ring-id':ring.get('ring_id'),'ring-number':ring['ring_number'],'ring-name':ring.get('ring_name')}
        for pos,item in enumerate(ring['classes']):
            gid=item.get('class_group_id');gk=rk+'|'+str(gid) if gid is not None else None
            if gk:groups[gk]={'group-key':gk,'group-id':gid,'group-name':item.get('group_name'),'source-json':packed({k:v for k,v in item.items() if k.startswith('class_group') or k in ('group_name','class_list')})}
            cid=item.get('class_id');ck=rk+'|'+str(cid) if cid is not None and not item.get('schedule_break') else None
            sk=rk+'|'+(str(item['class_groupxclasses_id']) if item.get('class_groupxclasses_id') is not None else 'position-'+str(pos))
            schedules[sk]={'schedule-key':sk,'kind':'break' if item.get('schedule_break') else 'class','source-position':pos,'source-id':str(item['class_groupxclasses_id']) if item.get('class_groupxclasses_id') is not None else None,'name':item.get('class_name') or item.get('group_name'),'start-time':item.get('estimated_start_time') or item.get('start_time_default'),'end-time':item.get('estimated_end_time'),'source-json':packed(item),'total-trips':item.get('total_trips'),'record-status':'Active — no entries' if not item.get('schedule_break') and item.get('total_trips')==0 else 'Active'}
            if ck:classes[ck]={'class-key':ck,'class-id':cid,'class-number':str(item['class_number']) if item.get('class_number') is not None else None,'class-name':item.get('class_name'),'record-status':'Active'}
    return {'rings':rings,'groups':groups,'classes':classes,'schedules':schedules}

def schedule_changed(old,new):
    return bool(schedule_diffs(old,new))


def schedule_diffs(old,new):
    before=raw(old.get('source-json'));after=raw(new.get('source-json'))
    before.pop('_',None);after.pop('_',None)
    changes=[{'field':name,'old':before.get(name),'new':after.get(name)} for name in sorted(set(before)|set(after)) if before.get(name)!=after.get(name)]
    if old.get('source-position')!=new.get('source-position'):
        changes.append({'field':'source-position','old':old.get('source-position'),'new':new.get('source-position')})
    return changes

def live_confirmed(root,show,day):
    """Existing Live saved coverage, same scope and current scheduled interval."""
    try:
        check=raw((root.parent/'wef-live/runtime/live-check-last-run.json').read_text())
        report=raw((root.parent/'wef-live/runtime/lane-classes-last-run.json').read_text())
        scope=report.get('scope',{});expected=int(show.split('|')[1])
        if not report.get('ok') or report.get('stage_errors') or report.get('logging_errors'):return set()
        if any(s.get('show')!=expected or s.get('customer')!=int(show.split('|')[0]) or s.get('date')!=day for s in (scope,check.get('scope',{}))):return set()
        if not check.get('ok') or not check.get('collect') or not check.get('next_check_at'):return set()
        if datetime.datetime.fromisoformat(check['next_check_at'].replace('Z','+00:00'))<datetime.datetime.now(datetime.timezone.utc):return set()
        if report.get('started','')<check.get('started','') or not report.get('completed'):return set()
        return {int(c['key'].split('|')[-1]) for c in report.get('coverage',[]) if c.get('table')=='live-classes' and c.get('key','').startswith(str(expected)+'|'+day+'|') and c.get('last_success','')>=report['started'] and c['last_success']<=report['completed']}
    except (OSError,ValueError,KeyError,TypeError):return set()

class Writer:
    def __init__(self,api,schema,show,day,errors):
        self.api=api;self.store=DataStore(api,schema,cache=True);self.show=show;self.day=day;self.errors=errors;self.counts={};self.notes=[]
    def authored(self,table,row):return {n:row['fields'][f['id']] for n,f in self.store.fields[table].items() if f['id'] in row['fields']}
    def rows(self,table):return {k:self.authored(table,r) for k,r in self.store.inventory(table).items()}
    def save(self,table,rows,diff=False,at=None):
        values=[];cached=self.store.inventory(table)
        for row in rows:
            row=dict(row)
            if table=='entries':row.pop('show',None) # Native w-shows is the current basic show link.
            key=str(row[self.store.primary[table]]);old=self.authored(table,cached[key]) if key in cached else {}
            if diff and old and all(old.get(k)==v for k,v in row.items() if k!='last-success'):continue
            if at:row['last-success']=at
            for field,target,identity in [('w-shows','w-shows',self.show),('w-days','w-days',self.show+'|'+self.day)]:
                if field in self.store.fields[table] and not old.get(field):row[field]=refs(target,identity)
            out={}
            for field,value in row.items():
                if isinstance(value,dict) and '$links' in value:
                    if old.get(field):continue
                    ids=[self.store.inventory(t).get(str(k),{}).get('id') for t,k in value['$links']]
                    if ids and all(ids):out[self.store.fields[table][field]['id']]=list(dict.fromkeys(ids))
                else:out[self.store.fields[table][field]['id']]=value
            values.append((key,out))
        for offset in range(0,len(values),10):
            batch=values[offset:offset+10]
            try:
                missing=[k for k,v in batch if k not in cached]
                if missing:
                    for key,identity in self.store.reserve(table,missing).items():cached[key]={'id':identity,'fields':{}}
                records=[{'id':cached[k]['id'],'fields':v} for k,v in batch]
                saved=self.api.request('PATCH',DATA_BASE,self.store.schema[table]['id'],body={'records':records})['records']
                if {r['id'] for r in saved}!={r['id'] for r in records}:raise ValueError('Save response missing records')
                for k,v in batch:cached[k]['fields'].update(v)
                self.counts[table]=self.counts.get(table,0)+len(batch)
            except Exception as e:self.errors.append({'key':'Save '+table,'error':error_text(e)})
    def save_schedule(self,graph,priorities,at):
        for table,items in graph.items():
            rows=[]
            for key,r in items.items():
                r=dict(r);rk=key.rsplit('|',1)[0]
                if table in ('classes','groups','schedules'):r['ring']=refs('rings',rk)
                if table=='classes':
                    matching=[raw(s['source-json']).get('class_group_id') for sk,s in graph['schedules'].items() if sk.rsplit('|',1)[0]==rk and raw(s['source-json']).get('class_id')==r['class-id']]
                    group_ids={g for g in matching if g is not None}
                    if len(group_ids)==1:r['group']=refs('groups',rk+'|'+str(next(iter(group_ids))))
                if table=='schedules':
                    old=self.rows('schedules').get(key,{})
                    if old and not schedule_changed(old,r):continue
                    r.update(priorities.get(key,{}));item=raw(r['source-json'])
                    if item.get('class_group_id') is not None:r['group']=refs('groups',rk+'|'+str(item['class_group_id']))
                    if item.get('class_id') is not None and not item.get('schedule_break'):r['class']=refs('classes',rk+'|'+str(item['class_id']))
                rows.append(r)
            self.save(table,rows,diff=True,at=at)
    def failed(self,keys,message):
        self.save('schedules',[{'schedule-key':k,'class-detail-failed':True,'class-detail-error':message} for k in keys])
    def save_class(self,key,capture):
        data=json.loads(pathlib.Path(capture).read_text(encoding='utf-8'));body=data['payload'];at=data['metric']['completed']
        previous=raw(self.rows('classes').get(key,{}).get('source-json'))
        # Keep class metadata used by existing rules; trip payloads are saved separately.
        class_body={k:v for k,v in previous.items() if k not in ('trips','class_group_order_of_go','iphonev2')}
        class_body['iphonev2']={k:v for k,v in body.items() if k!='rows'}
        class_body['capture']=capture
        self.save('classes',[{'class-key':key,'source-json':packed(class_body),'entry-count':len(body['rows'])}],at=at)
        existing_trips=self.rows('trips');entries=self.rows('entries');customer,sid=key.split('|')[:2]
        pending={table:{} for table in ('horses','riders','trainers','owners','entries','trips')}
        for source in body['rows']:
            tid=str(source['id']);matches=[(k,v) for k,v in existing_trips.items() if k.startswith(key+'|') and str(v.get('trip-id'))==tid]
            old_key,old=(matches[0] if len(matches)==1 else (None,{}));protected=raw(old.get('source-json'));entry_id=protected.get('entry_id')
            norm=lambda value:' '.join(str(value or '').split()).casefold()
            if protected.get('number') is not None and str(protected['number'])!=str(source.get('ENo')):entry_id=None
            for identity,old_name,new_name in [('horse_id','horse','Hor'),('rider_id','rider_name','Rid'),('trainer_id','trainer','Tra'),('owner_id','Owner','Own')]:
                if source.get(new_name) and norm(source[new_name])!=norm(protected.get(old_name)):protected.pop(identity,None)
            if not entry_id:
                candidates=[v for k,v in entries.items() if k.startswith(sid+'|') and str(v.get('entry-number'))==str(source.get('ENo'))]
                if len(candidates)==1:entry_id=candidates[0].get('entry-id')
            mapped={**protected,'trip_id':int(tid),'number':source.get('ENo'),'horse':source.get('Hor'),'rider_name':source.get('Rid'),'trainer':source.get('Tra'),'Owner':source.get('Own')}
            if entry_id:mapped['entry_id']=int(entry_id)
            else:mapped.pop('entry_id',None)
            for target,origin in [('order_of_go','OOG'),('actual_order','Actual_OOG'),('scratch_trip','Scr'),('gone_in','Gone')]:
                if source.get(origin) not in (None,''):mapped[target]=source[origin]
            if entry_id:
                graph=trip_records(mapped,key,None,at)
                for table in ('horses','riders','trainers','owners','entries'):
                    for item in graph[table]:pending[table][str(item[self.store.primary[table]])]=item
                trip=graph['trips'][0]
            else:
                trip={'trip-key':key+'||'+tid,'trip-id':tid,'class':refs('classes',key),'record-status':'active'}
                for field,origin in [('order-of-go','OOG'),('actual-order','Actual_OOG'),('scratch','Scr'),('gone-in','Gone')]:
                    if source.get(origin) not in (None,''):trip[field]=int(source[origin]) if field in ('order-of-go','actual-order') else str(source[origin])
                self.notes.append({'trip-id':tid,'entry-number':source.get('ENo'),'message':'Source has no entry ID; no entry identity invented. Trip and complete capture saved; entry link left empty.'})
            if old_key:trip['trip-key']=old_key
            trip['source-json']=packed({**mapped,'iphonev2':source})
            pending['trips'][trip['trip-key']]=trip
        for table,items in pending.items():self.save(table,list(items.values()),at=at)
        # The existing independent linking task fills remaining required empty links.

def manifests(root):
    return sorted((root/'runtime/fast').glob('day*/collections/*.json'),key=lambda file:datetime.datetime.fromisoformat(json.loads(file.read_text(encoding='utf-8'))['started']).timestamp())


def collect(root,day_number=1,api=None,reader=None):
    """Acquire and capture only. No Airtable business writes, saving or linking."""
    started=now();begin=time.monotonic();errors=[];api=api or Airtable()
    schema=json.loads((root/'schema.json').read_text(encoding='utf-8'))
    st,sf=synced_source('w-shows',root/'schema.json');dt,df=synced_source('w-days',root/'schema.json')
    owner=next(r['fields'] for r in api.records(DATA_BASE,st) if r['fields'].get(sf['is_default']) is True);show=owner[sf['show-key']]
    selected=[r['fields'] for r in api.records(DATA_BASE,dt) if r['fields'].get(df['show'])==show and r['fields'].get(df['day-priority'])==day_number and owner[sf['start_date']][:10]<=r['fields'][df['date']][:10]<=owner[sf['end_date']][:10]]
    if not selected:return {'ok':True,'state':'Skipped','counts':{},'item-errors':[]}
    day=selected[0][df['date']][:10]
    # Only this actual date waits; other dates continue through the same entry.
    for file in manifests(root):
        previous=json.loads(file.read_text(encoding='utf-8'))
        if previous['show-key']!=show or previous['day']!=day:continue
        saved_file=file.with_suffix('.saved');linked_file=file.with_suffix('.linked')
        saved=raw(saved_file.read_text(encoding='utf-8')) if saved_file.exists() else {}
        linked=raw(linked_file.read_text(encoding='utf-8')) if linked_file.exists() else {}
        if not saved.get('complete') or not linked.get('complete'):
            return {'ok':True,'state':'Deferred','show-key':show,'day':day,'reason':'Previous pass for this date has not finished saving and linking','counts':{'class-requested':0,'day-number':day_number},'item-errors':[]}
    attempt=os.environ.get('WEF_LOG_ATTEMPT_ID',str(uuid.uuid4()))
    runkey='collection-'+attempt+'|'+day;params={'customer_id':owner[sf['customer_id']],'show_id':owner[sf['show_id']]}
    reader=reader or Reader(root,day_number);store=DataStore(api,schema,cache=True)
    prior_schedule=store.inventory('schedules')
    class_done=live_confirmed(root,show,day) if day_number==0 else set()
    index=root/'runtime/fast'/('day'+str(day_number))/'collections'/(attempt+'.json')
    plan={'show-key':show,'day':day,'day-number':day_number,'run-key':runkey,'started':started,'operations':[],'complete':False}
    def capture(operation):
        plan['operations'].append(operation);atomic(index,capture_safe(plan))
    failed=set()
    for file in manifests(root):
        previous=json.loads(file.read_text(encoding='utf-8'))
        receipt_file=file.with_suffix('.saved')
        delivered=raw(receipt_file.read_text(encoding='utf-8')) if receipt_file.exists() else {}
        if previous['show-key']==show and previous['day']==day and not delivered.get('complete'):
            failed.update(op['class-key'] for op in previous['operations'] if op['kind']=='failure')
    requested=0;skips=[];health=[];diffs=[];preparation=[]
    def class_request(row,prepare):
        nonlocal requested
        key=row['class-key'];requested+=1
        try:
            body,metric=reader.fetch('/classes/'+str(row['class-id']),params,prepare=prepare)
            return {'kind':'class','class-key':key,'capture':metric['capture']},metric
        except Exception as error:
            message=error_text(error);errors.append({'key':key,'error':message})
            operation={'kind':'failure','class-key':key,'schedule-keys':routes.get(key,[]),'error':message}
            if prepare:preparation.append(operation)
            else:capture(operation)
            return None,getattr(error,'metric',{})
    try:
        payload,metric=reader.fetch('/schedule',{**params,'date':day},prepare=True);health.append(metric)
        graph=schedule_rows(payload,show,day)
        p=subprocess.run(['node',str(root/'schedule-priority.js')],input=packed([{'id':k,'fields':v} for k,v in graph['schedules'].items()]),text=True,capture_output=True,encoding='utf-8',check=True)
        priorities={r['id']:r['fields'] for r in json.loads(p.stdout)['updates']}
        flags={r['id']:r['fields'] for r in api.records(DATA_BASE,FLAGS_TABLE,fields=[FLAG_NAME,FLAG_SORT])}
        rows,skips=select_class_details(list(graph['classes'].values()),graph['schedules'],{},store.fields['classes'],prior_schedule,flags,priorities)
        routes={}
        for sk,row in graph['schedules'].items():
            cid=raw(row['source-json']).get('class_id')
            if cid is not None:routes.setdefault(sk.rsplit('|',1)[0]+'|'+str(cid),[]).append(sk)
        fail_field=store.fields['schedules']['class-detail-failed']['id']
        eligible=[row for row in rows if row['class-id'] not in class_done and row['class-key'] not in failed and not any(prior_schedule.get(sk,{}).get('fields',{}).get(fail_field) for sk in routes.get(row['class-key'],[]))]
        for key,row in graph['schedules'].items():
            saved=prior_schedule.get(key,{}).get('fields',{})
            old={name:saved[field['id']] for name,field in store.fields['schedules'].items() if field['id'] in saved}
            changes=schedule_diffs(old,row)
            if changes:diffs.append({'schedule-key':key,'changes':changes})
        # Preparation confirms the class lane before the single-attempt collection loop.
        probe=None;next_class=0
        while next_class<len(eligible) and probe is None:
            probe,check=class_request(eligible[next_class],True);next_class+=1;health.append(check)
        # Both source checks finish before responses enter the independent saving stage.
        plan['schedule-diffs']=diffs
        capture({'kind':'schedule','capture':metric.get('capture'),'graph':graph,'priorities':priorities,'at':metric['completed']})
        for operation in preparation:capture(operation)
        if probe is not None:
            capture(probe)
            for row in eligible[next_class:]:
                operation,_=class_request(row,False)
                if operation:capture(operation)
    except Exception as error:errors.append({'key':'Prepare/schedule','error':error_text(error)})
    completed=now();counts={'class-requested':requested,'class-skipped':len(skips),'live-owned-skipped':len(class_done),'schedule-diff-count':len(diffs),'schedule-diffs-capture':str(index),'day-number':day_number,'elapsed-seconds':time.monotonic()-begin}
    plan.update({'complete':True,'completed':completed,'health':health,'counts':counts,'item-errors':errors});atomic(index,capture_safe(plan))
    return {'ok':not errors,'state':'Collected','show-key':show,'day':day,'run-key':runkey,'manifest':str(index),'health':health,'source-completed':completed,'schedule-diffs':diffs,'counts':counts,'item-errors':errors}


def save_captures(root,api=None):
    """Independent saving task. Reads captured data; never invokes a source."""
    api=api or Airtable();schema=json.loads((root/'schema.json').read_text(encoding='utf-8'))
    errors=[];days=[];writers={}
    for file in manifests(root):
        plan=json.loads(file.read_text(encoding='utf-8'));receipt_file=file.with_suffix('.saved')
        receipt=json.loads(receipt_file.read_text(encoding='utf-8')) if receipt_file.exists() else {'processed':0,'counts':{},'item-errors':[],'started':now()}
        if receipt.get('complete'):continue
        show,day=plan['show-key'],plan['day'];identity=(show,day)
        if identity not in writers:writers[identity]=Writer(api,schema,show,day,errors)
        writer=writers[identity];before=dict(writer.counts)
        if not receipt.get('run-started'):
            prior=len(errors)
            writer.save('runs',[{'run-key':plan['run-key'],'started':plan['started'],'mode':'Scheduled day-'+str(plan['day-number'])+' collection','status':'Started'}]);receipt['run-started']=True
            receipt['item-errors'].extend(errors[prior:])
        for operation in plan['operations'][receipt['processed']:]:
            prior=len(errors)
            try:
                if operation['kind']=='schedule':writer.save_schedule(operation['graph'],operation['priorities'],operation['at'])
                elif operation['kind']=='class':writer.save_class(operation['class-key'],operation['capture'])
                elif operation['kind']=='failure':writer.failed(operation['schedule-keys'],operation['error'])
            except Exception as error:errors.append({'key':'Save '+operation['kind'],'error':error_text(error)})
            receipt['item-errors'].extend(errors[prior:]);receipt['processed']+=1
            for table,total in writer.counts.items():
                receipt['counts'][table]=receipt['counts'].get(table,0)+total-before.get(table,0)
            before=dict(writer.counts);atomic(receipt_file,receipt)
        if plan['complete']:
            completed=now();all_errors=plan['item-errors']+receipt['item-errors'];saved=sum(n for t,n in receipt['counts'].items() if t!='runs')
            prior=len(errors)
            writer.save('runs',[{'run-key':plan['run-key'],'started':plan['started'],'completed':completed,'mode':'Scheduled day-'+str(plan['day-number'])+' collection','status':'Partial' if all_errors else 'Succeeded','records':saved,'errors':len(all_errors),'elapsed-seconds':(datetime.datetime.fromisoformat(completed)-datetime.datetime.fromisoformat(plan['started'])).total_seconds()}])
            receipt['item-errors'].extend(errors[prior:]);all_errors=plan['item-errors']+receipt['item-errors']
            receipt.update({'complete':True,'completed':completed,'ok':not all_errors,'saved':saved,'day':day,'day-number':plan['day-number'],'run-key':plan['run-key']});atomic(receipt_file,receipt)
            state_file=root/'runtime/day-cadence.json';state=raw(state_file.read_text(encoding='utf-8')) if state_file.exists() else {}
            state[show+'|'+day]={'attempted':datetime.datetime.fromisoformat(plan['started']).timestamp(),'completed':completed,'priority':plan['day-number'],'status':'Partial' if all_errors else 'Saved','run-key':plan['run-key']};atomic(state_file,state)
            days.append({**receipt,'manifest':str(file)})
    return {'ok':not errors,'state':'Saved','counts':{'saved':sum(d['saved'] for d in days),'days':days},'item-errors':errors}
