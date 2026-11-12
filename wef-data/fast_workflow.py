"""One day: prepare source lanes, collect once, save independently, retain existing keys."""
import base64,datetime,json,os,pathlib,subprocess,time,uuid
from concurrent.futures import ThreadPoolExecutor
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
    return old.get('source-position')!=new['source-position'] or raw(old.get('source-json'))!=raw(new['source-json'])

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
        for table in ('w-shows','w-days','w-horses','w-riders','w-trainers','rings','groups','classes','schedules','entries','horses','riders','trainers','owners','trips','results','runs'):self.store.inventory(table)
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
        data=json.loads(pathlib.Path(capture).read_text());body=data['payload'];at=data['metric']['completed']
        previous=raw(self.rows('classes').get(key,{}).get('source-json'))
        # Keep class metadata used by existing rules; trip payloads are saved separately.
        class_body={k:v for k,v in previous.items() if k not in ('trips','class_group_order_of_go','iphonev2')}
        class_body['iphonev2']={k:v for k,v in body.items() if k!='rows'}
        class_body['capture']=capture
        self.save('classes',[{'class-key':key,'source-json':packed(class_body),'entry-count':len(body['rows'])}],at=at)
        existing_trips=self.rows('trips');entries=self.rows('entries');customer,sid=key.split('|')[:2]
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
                for table in ('horses','riders','trainers','owners','entries'):self.save(table,graph[table],at=at)
                trip=graph['trips'][0]
            else:
                trip={'trip-key':key+'||'+tid,'trip-id':tid,'class':refs('classes',key),'record-status':'active'}
                for field,origin in [('order-of-go','OOG'),('actual-order','Actual_OOG'),('scratch','Scr'),('gone-in','Gone')]:
                    if source.get(origin) not in (None,''):trip[field]=int(source[origin]) if field in ('order-of-go','actual-order') else str(source[origin])
                self.notes.append({'trip-id':tid,'entry-number':source.get('ENo'),'message':'Source has no entry ID; no entry identity invented. Trip and complete capture saved; entry link left empty.'})
            if old_key:trip['trip-key']=old_key
            trip['source-json']=packed({**mapped,'iphonev2':source})
            self.save('trips',[trip],at=at)
        # The existing independent linking task fills remaining required empty links.

def collect(root,day_number=1,api=None,reader=None):
    started=now();begin=time.monotonic();errors=[];api=api or Airtable();schema=json.loads((root/'schema.json').read_text());run='collection-'+os.environ.get('WEF_LOG_ATTEMPT_ID',str(uuid.uuid4()))
    st,sf=synced_source('w-shows',root/'schema.json');dt,df=synced_source('w-days',root/'schema.json')
    owner=next(r['fields'] for r in api.records(DATA_BASE,st) if r['fields'].get(sf['is_default']) is True);show=owner[sf['show-key']]
    selected=[r['fields'] for r in api.records(DATA_BASE,dt) if r['fields'].get(df['show'])==show and r['fields'].get(df['day-priority'])==day_number and owner[sf['start_date']][:10]<=r['fields'][df['date']][:10]<=owner[sf['end_date']][:10]]
    if not selected:return {'ok':True,'state':'Skipped','counts':{},'item-errors':[]}
    day=selected[0][df['date']][:10];runkey=run+'|'+day;params={'customer_id':owner[sf['customer_id']],'show_id':owner[sf['show_id']]};reader=reader or Reader(root,day_number)
    writer=Writer(api,schema,show,day,errors);prior_classes=writer.store.inventory('classes');prior_schedule=writer.store.inventory('schedules')
    writer.save('runs',[{'run-key':runkey,'started':started,'mode':'Scheduled day-'+str(day_number)+' collection','status':'Started'}])
    requested=0;skips=[];health=[];class_done=live_confirmed(root,show,day) if day_number==0 else set();source_finished=None
    with ThreadPoolExecutor(max_workers=1) as saving:
        futures=[]
        try:
            payload,metric=reader.fetch('/schedule',{**params,'date':day},prepare=True);health.append(metric)
            graph=schedule_rows(payload,show,day)
            p=subprocess.run(['node',str(root/'schedule-priority.js')],input=packed([{'id':k,'fields':v} for k,v in graph['schedules'].items()]),text=True,capture_output=True,encoding='utf-8',check=True)
            priorities={r['id']:r['fields'] for r in json.loads(p.stdout)['updates']}
            flags={r['id']:r['fields'] for r in api.records(DATA_BASE,FLAGS_TABLE,fields=[FLAG_NAME,FLAG_SORT])}
            rows,skips=select_class_details(list(graph['classes'].values()),graph['schedules'],prior_classes,writer.store.fields['classes'],prior_schedule,flags,priorities)
            old=writer.rows('schedules');routes={}
            for sk,r in graph['schedules'].items():
                cid=raw(r['source-json']).get('class_id')
                if cid is not None:routes.setdefault(sk.rsplit('|',1)[0]+'|'+str(cid),[]).append(sk)
            failed_field=writer.store.fields['schedules']['class-detail-failed']['id']
            eligible=[r for r in rows if r['class-id'] not in class_done and not any(prior_schedule.get(sk,{}).get('fields',{}).get(failed_field) for sk in routes.get(r['class-key'],[]))]
            futures.append(saving.submit(writer.save_schedule,graph,priorities,metric['completed']))
            prepared=False
            for row in eligible:
                key=row['class-key'];requested+=1
                try:
                    body,m=reader.fetch('/classes/'+str(row['class-id']),params,prepare=not prepared);prepared=True
                    if len(health)==1:health.append(m)
                    futures.append(saving.submit(writer.save_class,key,m['capture']))
                except Exception as e:
                    # Lane discovery belongs to preparation; later requests never retry.
                    prepared=True;message=error_text(e);errors.append({'key':key,'error':message})
                    futures.append(saving.submit(writer.failed,routes.get(key,[]),message))
            source_finished=now()
        except Exception as e:errors.append({'key':'Prepare/schedule','error':error_text(e)})
        for f in futures:
            try:f.result()
            except Exception as e:errors.append({'key':'Save/link','error':error_text(e)})
    completed=now();saved=sum(v for t,v in writer.counts.items() if t!='runs')
    writer.save('runs',[{'run-key':runkey,'started':started,'completed':completed,'mode':'Scheduled day-'+str(day_number)+' collection','status':'Partial' if errors else 'Succeeded','records':saved,'errors':len(errors),'elapsed-seconds':time.monotonic()-begin}])
    state_file=root/'runtime/day-cadence.json';state=raw(state_file.read_text()) if state_file.exists() else {};state[show+'|'+day]={'attempted':datetime.datetime.fromisoformat(started).timestamp(),'completed':completed,'priority':day_number,'status':'Partial' if errors else 'Saved','run-key':runkey};atomic(state_file,state)
    return {'ok':not errors,'state':'Partial' if errors else 'Saved','show-key':show,'day':day,'run-key':runkey,'health':health,'source-completed':source_finished,'mapping-notes':writer.notes,'counts':{'saved':saved,'by-table':writer.counts,'class-requested':requested,'class-skipped':len(skips),'live-owned-skipped':len(class_done),'day-number':day_number,'elapsed-seconds':time.monotonic()-begin},'item-errors':errors}
