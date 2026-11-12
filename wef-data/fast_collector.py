"""Fresh scheduled collection: select day, schedules, classes, save, report."""
import base64, concurrent.futures, datetime, json, os, pathlib, re, subprocess, sys, time
from data_contract import packed, digest
from wef_data_airtable import Airtable, error_text
from wef_data_store import DataStore, DATA_BASE

DAY_INTERVALS={0:180,1:3600,2:7200,3:10800}
FLAGS_TABLE='tbltWFlkHKRhQSkyp'
FLAG_NAME='fldfOgdWgBi8Ic07S'
FLAG_SORT='fldjYKmLF9qlssjbX'
SCHEDULE_FLAGS='fldFVOPRFWbGUmmFS'

SHOW_CONTEXT_FIELDS={'show-key':'show-key','show-id':'show_id',
    'customer-id':'customer_id','show-name':'show_name',
    'start-date':'start_date','end-date':'end_date',
    'selected-date':'selected_date','default':'is_default','last-checked':'last_checked'}


def safe(v):
    if isinstance(v,dict):return {k:safe(x) for k,x in v.items() if k.lower() not in {'_','_t','token','cookie','authorization','access_token','refresh_token'}}
    if isinstance(v,list):return [safe(x) for x in v]
    return v


def capture_safe(value,secrets=None):
    """Redact the existing reader's JSON/base64 capture without trimming payloads."""
    if secrets is None:secrets=tuple(secret for name,secret in os.environ.items() if secret and re.search(r'TOKEN|COOKIE|PASSWORD|SECRET|API_KEY',name,re.I))
    if isinstance(value,dict):
        output={}
        for key,item in value.items():
            if re.search(r'(^_t$|token|cookie|authorization|password|secret|api[_-]?key)',key,re.I):
                output[key]='[redacted]';continue
            if key in ('body','stdout-base64') and isinstance(item,str):
                try:
                    text=base64.b64decode(item,validate=True).decode('utf-8','replace')
                    try:decoded=json.loads(text)
                    except ValueError:decoded=text
                    cleaned=capture_safe(decoded,secrets)
                    output[key]=base64.b64encode((cleaned if isinstance(cleaned,str) else packed(cleaned)).encode('utf-8')).decode('ascii')
                    continue
                except (ValueError,TypeError):pass
            output[key]=capture_safe(item,secrets)
        return output
    if isinstance(value,list):return [capture_safe(item,secrets) for item in value]
    if not isinstance(value,str):return value
    if value.lstrip().startswith(('{','[')):
        try:return packed(capture_safe(json.loads(value),secrets))
        except ValueError:pass
    for secret in secrets:value=value.replace(secret,'[redacted]')
    value=re.sub(r'Bearer\s+[^\s"\'<>]+','Bearer [redacted]',value,flags=re.I)
    value=re.sub(r'\b[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\b','[redacted]',value)
    value=re.sub(r'(?im)^((?:set-cookie|cookie|authorization)\s*:)[^\r\n]*',r'\1 [redacted]',value)
    return re.sub(r'''(?i)((?:["']?(?:_t|token|cookie|authorization|access_token|refresh_token|password|secret|api_key)["']?)\s*[:=]\s*)("(?:\\.|[^"\\])*(?:"|(?:\\)?\Z)|'(?:\\.|[^'\\])*(?:'|(?:\\)?\Z)|[^\s,;<>]+)''',r'\1"[redacted]"',value)


def synced_source(table,schema_file=None):
    schema=json.loads(pathlib.Path(schema_file or pathlib.Path(__file__).with_name('schema.json')).read_text(encoding='utf-8'))['tables'][table]
    return schema['id'],{field['name']:field['id'] for field in schema['fields']}


def stamp():return datetime.datetime.now(datetime.timezone.utc).isoformat()


def refs(table,*keys):return {'$links':[[table,str(k)] for k in keys if k is not None]}


def atomic(path,value):
    path=pathlib.Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    temporary=path.with_suffix('.tmp');temporary.write_text(packed(value),encoding='utf-8')
    for attempt in range(3):
        try:os.replace(temporary,path);return
        except PermissionError as error:
            if getattr(error,'winerror',None) not in (5,32) or attempt==2:raise
            # Retry only placement of identical bytes; no source/API work is repeated.
            time.sleep(0.05*(attempt+1))


class SourceFailure(RuntimeError):
    def __init__(self,message,diagnostic):super().__init__(message);self.diagnostic=diagnostic


class Source:
    def __init__(self,capture_root=None,day=None):
        self.capture_root=pathlib.Path(capture_root) if capture_root is not None else None
        self.day=day
    def fetch(self,route,params,timeout=0):
        try:result=subprocess.run(['node',str(pathlib.Path(__file__).with_name('source-reader.js')),route,packed(params),str(timeout)],capture_output=True,timeout=timeout+10 if timeout>0 else None)
        except subprocess.TimeoutExpired:raise SourceFailure('Source reader timed out',{'route':route,'params':params,'timeout':True,'completed':stamp()}) from None
        diagnostic={'route':route,'params':params,'exit-code':result.returncode}
        if self.capture_root is not None:
            try:outer=json.loads(result.stdout)
            except (ValueError,TypeError):outer=result.stdout.decode('utf-8','replace')
            source_metric=outer.get('metric',{}) if isinstance(outer,dict) else {}
            if not isinstance(source_metric,dict):source_metric={}
            received={'route':route,'params':params,'exit-code':result.returncode,
                'url':source_metric.get('url'),'show-id':params.get('show_id'),'customer-id':params.get('customer_id'),
                'day':params.get('date') or self.day,'class-id':int(route.rsplit('/',1)[1]) if route.startswith('/classes/') else None,
                'source-started':source_metric.get('started'),'source-completed':source_metric.get('completed'),
                'http':source_metric.get('http'),'http-response-received':isinstance(outer,dict) and 'body' in outer,
                'reader-ok':bool(isinstance(outer,dict) and outer.get('ok')),
                'stdout-base64':base64.b64encode(result.stdout).decode('ascii'),
                'stdout-sha256':__import__('hashlib').sha256(result.stdout).hexdigest(),'redacted':True,'received-at':stamp()}
            received=capture_safe(received)
            path=self.capture_root/(digest(packed(received))+'.json')
            try:atomic(path,received);diagnostic['saved-response']=str(path)
            except Exception as error:diagnostic['capture-error']=error_text(error)
        try:response=json.loads(result.stdout)
        except (ValueError,TypeError):raise SourceFailure('Source reader did not return a valid result',diagnostic) from None
        if not isinstance(response,dict) or not isinstance(response.get('metric',{}),dict):raise SourceFailure('Source reader did not return a valid result',diagnostic)
        metric={**response.get('metric',{}),**diagnostic}
        if result.returncode or not response.get('ok'):raise SourceFailure(response.get('error','Source read failed'),metric)
        def check(v):
            if isinstance(v,dict):
                if '_t' in v:raise ValueError('Unresolved encrypted envelope')
                for x in v.values():check(x)
            elif isinstance(v,list):
                for x in v:check(x)
        try:
            body=base64.b64decode(response['body'],validate=True)
            if metric['bytes']!=len(body) or metric['sha256']!=__import__('hashlib').sha256(body).hexdigest():raise ValueError('Source body receipt mismatch')
            payload=json.loads(body);check(payload)
        except (ValueError,KeyError,TypeError) as error:
            raise SourceFailure('Saved response quarantined: '+str(error),metric) from error
        return safe(payload),metric


def schedule_payload(source, params):
    try:
        return source.fetch('/schedule',params)
    except SourceFailure as exc:
        http=exc.diagnostic.get('http')
        if not (http in (500,502,503,504) or (http is None and (exc.diagnostic.get('timeout') or 'TIMEOUT' in str(exc).upper()))):
            raise
        first=str(exc)
    time.sleep(1)
    try:
        return source.fetch('/schedule',params)
    except SourceFailure as exc:
        raise SourceFailure(f'Schedule retry failed: {exc}; first attempt: {first}',exc.diagnostic) from exc


def blocking_errors(errors):
    return any(not item.get('nonblocking') for item in errors)


def class_detail_required(saved, fields):
    return saved.get(fields['record-status']['id']) != 'Dropped' and not (
        saved.get(fields['status']['id']) == 'Completed'
        and saved.get(fields['class_related_data.results_verified']['id']) == 1)


def select_class_details(rows,schedules,prior_classes,class_fields,prior_schedules,flags,priorities):
    """Keep occurrences intact; one eligible occurrence is enough for its class."""
    classes={row['class-key']:row for row in rows};ranked={}
    order={'First trip':0,'Second trip':1,'Classic':2}
    for key,schedule in schedules.items():
        raw=json.loads(schedule['source-json'])
        class_key=key.rsplit('|',1)[0]+'|'+str(raw.get('class_id'))
        if class_key not in classes:continue
        linked=[flags[value] for value in prior_schedules.get(key,{}).get('fields',{}).get(SCHEDULE_FLAGS,[]) if value in flags]
        flag=priorities.get(key,{}).get('rs-flag')
        labels={flag,*[value.get(FLAG_NAME) for value in linked]}
        count=raw.get('total_trips')
        if (schedule.get('kind')=='break' or labels.intersection({'Flat','Model','No entries'})
                or str(raw.get('schedule_sequencetype','')).strip().lower() in ('model','under saddle/flat')
                or type(count) in (int,float) and count==0
                or any(value.get(FLAG_NAME)==flag and value.get(FLAG_SORT)==999 for value in flags.values())
                or any(value.get(FLAG_SORT)==999 for value in linked)):continue
        current={**prior_classes.get(class_key,{}).get('fields',{}),
                 class_fields['record-status']['id']:classes[class_key]['record-status']}
        if not class_detail_required(current,class_fields):continue
        rank=order.get(flag,min((order.get(value.get(FLAG_NAME),3) for value in linked),default=3))
        if class_key not in ranked or rank<ranked[class_key]:ranked[class_key]=rank
    selected=sorted((row for row in rows if row['class-key'] in ranked),key=lambda row:ranked[row['class-key']])
    skips=[{'class-key':row['class-key'],'outcome':'Skipped by rule'} for row in rows if row['class-key'] not in ranked]
    return selected,skips


def collect_day(schema_file, selection):
    """Existing fresh single-day collection; no class work is queued or resumed."""
    errors=[];saved=set();show=None;day=None;runkey=None;run_started=stamp()
    started=time.perf_counter();stage_started=started;timings={}
    detail_counts={'class-detail-requested':0,'class-detail-skipped':0}
    stage_outcomes={}
    def timed(stage):
        nonlocal stage_started
        now=time.perf_counter();timings[stage]=round(now-stage_started,6);stage_started=now
    def error(target,exception,endpoint=None,source=None):
        message=capture_safe(error_text(exception) if isinstance(exception,Exception) else str(exception))
        diagnostic=source or getattr(exception,'diagnostic',{})
        for label,name in [('Requested URL','url'),('Source completed','completed'),('Saved response','saved-response'),('Capture error','capture-error')]:
            if diagnostic.get(name):message+='\n'+label+': '+capture_safe(str(diagnostic[name]))
        errors.append({'key':target,'error':message,
                       **({'endpoint':endpoint} if endpoint else {})})
    def report():
        if runkey:
            save('runs',[{'run-key':runkey,'mode':'Scheduled day-zero collection' if priority==0 else 'Scheduled day-'+str(selection[2])+' collection',
                'started':run_started,'completed':stamp(),'status':'Partial' if blocking_errors(errors) else 'Succeeded',
                'records':len(saved),'errors':len(errors),'elapsed-seconds':round(time.perf_counter()-started,6)}])
            timed('save-run-summary')
        return {'ok':not blocking_errors(errors),'state':'Partial' if blocking_errors(errors) else 'Saved',
                'show-key':show,'day':day,'run-key':runkey,'counts':{'saved':len(saved),
                'stage-seconds':dict(timings),'stage-outcomes':stage_outcomes,
                'processing-outcome':'Completed with warnings' if errors else 'Completed',
                'total-seconds':round(time.perf_counter()-started,6),**detail_counts},'item-errors':errors}
    try:
        client=Airtable()
        store=DataStore(client,json.loads(pathlib.Path(schema_file).read_text(encoding='utf-8')),cache=True)
        context,day,priority=selection
        show=context['show-key']
        source=Source(pathlib.Path(schema_file).parent/'runtime'/'source-responses',day=day)
    except Exception as exception:
        error('Show and day selection',exception);return report()
    timed('select-show-and-day')

    tables=('rings','groups','classes','entries','trips','schedules','runs','results')
    def save(table,rows):
        rows=[{k:v for k,v in row.items() if not (isinstance(v,dict) and '$links' in v)} for row in rows];written=set()
        for start in range(0,len(rows),10):
            batch=rows[start:start+10]
            try:
                targets={table}
                for row in batch:
                    for value in row.values():
                        if isinstance(value,dict) and '$links' in value:targets.update(t for t,k in value['$links'])
                mapping={t:{k:r['id'] for k,r in store.inventory(t).items()} for t in targets}
                missing=[str(row[store.primary[table]]) for row in batch if str(row[store.primary[table]]) not in mapping[table]]
                if missing:mapping[table].update(store.reserve(table,missing))
            except Exception as exception:
                error(table,exception);continue
            records=[];keys=[]
            for row in batch:
                key=str(row[store.primary[table]])
                try:
                    records.append({'id':mapping[table][key],'fields':store.fields_for(table,row,mapping)});keys.append(key)
                except Exception as exception:error(table+' '+key,exception)
            if not records:continue
            def patch_records(items, item_keys):
                response=client.request('PATCH',DATA_BASE,store.schema[table]['id'],body={'records':items})
                acknowledged={row['id'] for row in response['records']}
                for key,record in zip(item_keys,items):
                    if record['id'] in acknowledged:
                        written.add(key)
                        if table!='runs':saved.add((table,key))
                    else:error(table+' '+key,'Write response did not acknowledge this record')
            try:
                patch_records(records,keys)
            except Exception as exc:
                if table!='classes' or 'Airtable HTTP 422' not in error_text(exc):
                    error(table,exc)
                    continue
                for key,record in zip(keys,records):
                    try:
                        patch_records([record],[key])
                    except Exception as rejected:
                        message=error_text(rejected)
                        field=store.fields[table]['source-json']['id']
                        if ('Airtable HTTP 422' not in message or 'Field "source-json" cannot accept' not in message
                                or field not in record['fields']):
                            error(table+' '+key,rejected)
                            continue
                        remaining={k:v for k,v in record['fields'].items() if k!=field}
                        try:
                            patch_records([{'id':record['id'],'fields':remaining}],[key])
                        except Exception as fallback:
                            error(table+' '+key,fallback)
                        errors.append({'key':table+' '+key+' source-json','error':message,'nonblocking':True})
        return written

    def missing_rows(table,present,prefix,terminal,absent='Absent'):
        # Saved status is the first-miss marker; absence never renews source clocks.
        output=[]
        try:
            status_id=store.fields[table]['record-status']['id']
            for key,prior in store.inventory(table).items():
                if not key.startswith(prefix) or key in present:continue
                old=prior['fields'].get(status_id)
                status=terminal if old in (absent,terminal) else absent
                if old!=status:output.append({store.primary[table]:key,'record-status':status})
        except Exception as exception:error(table+' membership',exception)
        return output

    def source_rows(rows):
        output=[]
        for row in rows:
            value={}
            for name,field in row.items():
                if isinstance(field,dict) and '$links' in field:
                    if not field['$links'] or any(t not in tables for t,k in field['$links']):continue
                if field is not None:value[name]=field
            output.append(value)
        return output

    runkey='collection-'+(os.environ.get('WEF_LOG_ATTEMPT_ID') or run_started)
    if priority!=0:runkey+='|'+day
    run_fields={'last-run':refs('runs',runkey)} if runkey in save('runs',[
        {'run-key':runkey,'mode':'Scheduled day-zero collection' if priority==0 else 'Scheduled day-'+str(selection[2])+' collection','started':run_started,'status':'Started'}]) else {}
    timed('save-run-start')
    params={'show_id':context['show-id'],'customer_id':context['customer-id']}
    try:
        payload,metric=schedule_payload(source,{**params,'date':day})
    except Exception as exception:
        error('Day inventory collection',exception);stage_outcomes['schedule']='Failed';stage_outcomes['class-detail']='Not run';return report()
    timed('fetch-rings-groups-classes')
    if metric.get('capture-error'):error('Schedule response capture',metric['capture-error'],source=metric)
    inventory={table:{} for table in tables[:3]}
    schedules={};inventory_complete=True
    try:
        if not isinstance(payload['rings'],list):raise ValueError('Schedule rings must be a list')
        rings=iter(payload['rings'])
    except Exception as exception:
        error('Day inventory collection',exception);stage_outcomes['schedule']='Failed';stage_outcomes['class-detail']='Not run';return report()
    for ring in rings:
        try:
            rk=show+'|'+day+'|'+str(ring['ring_number'])
            inventory['rings'][rk]={'ring-key':rk,'ring-id':ring.get('ring_id'),
                'ring-number':ring['ring_number'],'ring-name':ring.get('ring_name'),
                'last-success':metric['completed'],**run_fields}
            if not isinstance(ring['classes'],list):raise ValueError('Schedule classes must be a list')
            items=iter(ring['classes'])
        except Exception as exception:
            inventory_complete=False;error('Ring',exception);continue
        for pos,item in enumerate(items):
            try:
                gid=item.get('class_group_id');gk=rk+'|'+str(gid) if gid is not None else None
                if gk:
                    inventory['groups'][gk]={'group-key':gk,'ring':refs('rings',rk),'group-id':gid,
                        'group-name':item.get('group_name'),'last-success':metric['completed'],
                        'source-json':packed({k:v for k,v in item.items() if k.startswith('class_group') or k in ('group_name','class_list')})}
                cid=item.get('class_id')
                ck=rk+'|'+str(cid) if cid is not None and not item.get('schedule_break') else None
                schedulekey=rk+'|'+(str(item['class_groupxclasses_id']) if item.get('class_groupxclasses_id') is not None else 'position-'+str(pos))
                schedules[schedulekey]={'schedule-key':schedulekey,'show':refs('shows',show),
                    'day':refs('days',show+'|'+day),'ring':refs('rings',rk),'group':refs('groups',gk),
                    'class':refs('classes',ck),'kind':'break' if item.get('schedule_break') else 'class',
                    'source-position':pos,'source-id':str(item['class_groupxclasses_id']) if item.get('class_groupxclasses_id') is not None else None,
                    'name':item.get('class_name') or item.get('group_name'),
                    'start-time':item.get('estimated_start_time') or item.get('start_time_default'),
                    'end-time':item.get('estimated_end_time'),'source-json':packed(item),'last-success':metric['completed'],
                    'total-trips':item.get('total_trips'),
                    'record-status':'Active \u2014 no entries' if not item.get('schedule_break') and type(item.get('total_trips')) in (int,float) and item['total_trips']==0 else 'Active'}
                if ck is None:
                    if not item.get('schedule_break'):
                        inventory_complete=False;error(rk,'Source class has no class_id')
                    continue
                row=inventory['classes'].setdefault(ck,{'class-key':ck,'ring':refs('rings',rk),
                    'class-id':cid,'group':refs('groups'),'record-status':'Active'})
                if item.get('class_number') is not None:row['class-number']=str(item['class_number'])
                if item.get('class_name') is not None:row['class-name']=item['class_name']
                if gk and ['groups',gk] not in row['group']['$links']:row['group']['$links'].append(['groups',gk])
            except Exception as exception:
                inventory_complete=False;error(rk,exception)
    graph={table:list(rows.values()) for table,rows in inventory.items()}
    timed('prepare-rings-groups-classes')
    try:prior_classes=store.inventory('classes') if graph.get('classes') else {}
    except Exception as exception:
        error('Class-detail selection',exception);prior_classes={}
    for table in tables[:3]:
        save(table,source_rows(graph[table]));timed('save-inventory-'+table)

    # Save the schedule stage before making any class-detail request.
    absent_schedules=missing_rows('schedules',schedules,show+'|'+day+'|','Dropped') if inventory_complete else []
    saved_schedules=save('schedules',list(schedules.values())+absent_schedules)
    timed('save-schedules')
    priority_fields={}
    try:
        records=[{'id':store.inventory('schedules').get(key,{}).get('id',key),'fields':row} for key,row in schedules.items()]
        evaluation=subprocess.run(['node',str(pathlib.Path(__file__).with_name('schedule-priority.js'))],
            input=packed(records),capture_output=True,text=True,encoding='utf-8',check=True)
        priorities=json.loads(evaluation.stdout)
        by_id={record['id']:record['fields']['schedule-key'] for record in records}
        priority_fields={by_id[row['id']]:row['fields'] for row in priorities['updates']}
        save('schedules',[{'schedule-key':by_id[row['id']],**row['fields']} for row in priorities['updates']
            if by_id[row['id']] in saved_schedules])
        for message in priorities['errors']:error('Schedule priority',message)
    except Exception as exception:error('Schedule priority',exception)
    timed('save-schedule-priorities')
    stage_outcomes['schedule']='Failed' if blocking_errors(errors) else 'Succeeded'
    detail_error_start=len(errors)
    try:
        flags={row['id']:row['fields'] for row in client.records(DATA_BASE,FLAGS_TABLE,fields=[FLAG_NAME,FLAG_SORT])} if graph.get('classes') else {}
    except Exception as exception:error('Class-detail flags',exception);flags={}
    try:prior_schedules=store.inventory('schedules')
    except Exception as exception:error('Class-detail schedule flags',exception);prior_schedules={}
    detail_rows,skips=select_class_details(graph.get('classes',[]),schedules,prior_classes,store.fields['classes'],prior_schedules,flags,priority_fields)
    detail_counts.update({'class-detail-skipped':len(skips),'class-detail-skips':skips,
                          'class-detail-order':[row['class-key'] for row in detail_rows]})

    # Class-detail stage: existing requests and detail-table writes follow.
    from record_mappings import RESULT_FIELDS,trip_records
    classes={};entries={};trips={};results={};missing_trips=[]
    entities={table:{} for table in ('horses','riders','trainers','owners')}
    def fetch(row):return source.fetch('/classes/'+str(row['class-id']),params)
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        requests={pool.submit(fetch,row):row for row in detail_rows}
        detail_counts['class-detail-requested']=len(requests)
        for future in concurrent.futures.as_completed(requests):
            row=requests[future];key=row['class-key']
            metric={}
            try:
                payload,metric=future.result();at=metric['completed']
                if metric.get('capture-error'):error(key+' response capture',metric['capture-error'],source=metric)
                if not isinstance(payload,dict) or not isinstance(payload.get('class'),dict):raise ValueError('Class object must be present')
                if payload['class'].get('class_id')!=row['class-id']:raise ValueError('Class identity does not match requested class')
                if not isinstance(payload.get('class_related_data',{}),dict):raise ValueError('Class related data must be an object')
                if not isinstance(payload.get('trips'),list):raise ValueError('Class trips must be a list')
                if any(not isinstance(trip,dict) for trip in payload['trips']):raise ValueError('Class trip must be an object')
                classes[key]={'class-key':key,'source-json':packed(payload),'last-success':at,**run_fields,'record-status':'Active'}
                if payload.get('class_related_data',{}).get('status') is not None:
                    classes[key]['status']=payload['class_related_data']['status']
                if payload.get('total_entry_trips') is not None:
                    classes[key]['entry-count']=payload['total_entry_trips']
                    if type(payload['total_entry_trips']) in (int,float) and payload['total_entry_trips']==0:
                        classes[key]['record-status']='Active \u2014 no entries'
                current_trips=set();trip_errors=len(errors)
                for trip in payload['trips']:
                    try:
                        eid=int(trip['entry_id']);ek=str(context['show-id'])+'|'+str(eid)
                        mapped=trip_records(trip,key,None,at)
                        for table,entity_rows in entities.items():
                            for entity in mapped[table]:
                                entity_key=entity[store.primary[table]];prior=entity_rows.get(entity_key,{})
                                supplied={field:value for field,value in entity.items() if value is not None and value!=''}
                                entity_rows[entity_key]=({**prior,**supplied} if not prior or at>=prior['last-success'] else {**supplied,**prior})
                        entry={'entry-key':ek,'entry-id':eid,'last-success':at}
                        if trip.get('number') is not None:entry['entry-number']=str(trip['number'])
                        if ek not in entries or at>entries[ek]['last-success']:entries[ek]=entry
                        tk=key+'|'+str(eid)+'|'+str(trip.get('entryxclasses_uuid') or trip.get('trip_id') or '')
                        value={'trip-key':tk,'class':refs('classes',key),'entry':refs('entries',ek),
                            'source-json':packed(trip),'last-success':at,'record-status':'active',**run_fields}
                        for field,raw in {'trip-id':'trip_id','source-uuid':'entryxclasses_uuid','order-of-go':'order_of_go',
                            'actual-order':'actual_order','gone-in':'gone_in','placing':'placing','scratch':'scratch_trip',
                            'estimated-go-time':'estimated_go_time'}.items():
                            if trip.get(raw) is not None:
                                if field in ('order-of-go','actual-order'):
                                    try:value[field]=int(trip[raw])
                                    except (ValueError,TypeError):pass
                                else:value[field]=str(trip[raw])
                        trips[tk]=value;current_trips.add(tk)
                        result_values={name:trip.get(source) for name,source in RESULT_FIELDS.items()}
                        status=classes[key].get('status',store.inventory('classes').get(key,{}).get('fields',{}).get(store.fields['classes']['status']['id']))
                        results[tk]={'result-key':tk,'class-status':status,**{name:str(v) if v is not None else None for name,v in result_values.items()},'source-json':packed(result_values),'last-success':at}
                    except Exception as exception:error(key,exception,source=metric)
                if len(errors)==trip_errors:
                    missing_trips.extend(missing_rows('trips',current_trips,key+'|','dropped','absent'))
            except Exception as exception:error(key,exception,endpoint='/classes/'+str(row['class-id']),source=metric)
    timed('fetch-and-prepare-entries-trips')
    absent_classes=missing_rows('classes',inventory['classes'],show+'|'+day+'|','Dropped') if inventory_complete else []
    updated_classes=save('classes',list(classes.values())+absent_classes);timed('save-class-details')
    save('entries',entries.values())
    for table,entity_rows in entities.items():save(table,entity_rows.values())
    timed('save-entries')
    save('trips',list(trips.values())+missing_trips);timed('save-trips')
    save('results',results.values());timed('save-results')
    stage_outcomes['class-detail']='Failed' if blocking_errors(errors[detail_error_start:]) else 'Succeeded'
    result=report()
    result['confirmed-classes']=sorted(updated_classes.intersection(classes))
    return result


def run(schema_file):
    """Run eligible days in priority order on the existing three-minute wake."""
    started=time.perf_counter();errors=[];reports=[];show=None
    state_path=pathlib.Path(schema_file).parent/'runtime'/'day-cadence.json'
    def error(target,exception):
        errors.append({'key':target,'error':error_text(exception)})
    try:
        client=Airtable()
        show_table,show_fields=synced_source('w-shows',schema_file)
        day_table,day_fields=synced_source('w-days',schema_file)
        context_fields={name:show_fields[field] for name,field in SHOW_CONTEXT_FIELDS.items()}
        shows=client.records(DATA_BASE,show_table,fields=list(context_fields.values()))
        defaults=[row for row in shows if row['fields'].get(show_fields['is_default']) is True]
        if len(defaults)!=1:raise ValueError('Expected one default show')
        owner=defaults[0];context={name:owner['fields'].get(field) for name,field in context_fields.items()}
        show=context['show-key']
        rows=client.records(DATA_BASE,day_table,fields=[day_fields[name] for name in ('show-day-key','show','date','day-priority')])
        days=[];seen_days=set()
        for row in rows:
            fields=row['fields'];priority=fields.get(day_fields['day-priority'])
            if fields.get(day_fields['show'])!=show:continue
            if type(priority) not in (int,float) or priority not in (0,1,2,3,4):continue
            try:
                day=datetime.date.fromisoformat(fields[day_fields['date']][:10]).isoformat()
                if fields.get(day_fields['show-day-key'])!=show+'|'+day or day in seen_days:raise ValueError('Invalid show-day key/date')
                days.append((int(priority),day));seen_days.add(day)
            except Exception as exception:error('Show-day '+str(row.get('id')),exception)
        days.sort()
        if not days:raise ValueError('No synced days with priority 0-4')
    except Exception as exception:
        error('Show and day selection',exception);days=[]
    try:
        state=json.loads(state_path.read_text(encoding='utf-8')) if state_path.exists() else {}
        if not isinstance(state,dict):raise ValueError('Day cadence timestamps must be an object')
    except Exception as exception:
        error('Day cadence read',exception);state={}
    timings={'select-show-and-day':round(time.perf_counter()-started,6)}
    future_due=None
    for priority,day in days:
        key=show+'|'+day;previous=state.get(key,{})
        attempted=time.time()
        # Day zero retains its proven every-wake behavior. Future days use
        # elapsed time since their last attempt, including incomplete attempts.
        try:
            if priority==0:
                from zoneinfo import ZoneInfo
                today=datetime.datetime.fromtimestamp(attempted,datetime.timezone.utc).astimezone(ZoneInfo('America/New_York')).date().isoformat()
                interval=0 if day==today else 3600
            else:
                # One half-hour cycle: days 1 through 4, completing each in order.
                if future_due is None:future_due=previous.get('force-next') is True or attempted-float(previous.get('attempted',0))>=1800
                if not future_due:continue
                interval=0
            if attempted-float(previous.get('attempted',0))<interval:continue
        except (AttributeError,TypeError,ValueError) as exception:error('Day cadence '+day,exception)
        at=stamp();lane='fast-discovery' if key not in state else 'fast-collector'
        try:result=collect_day(schema_file,(context,day,priority))
        except Exception as exception:
            result={'ok':False,'state':'Partial','counts':{'saved':0},'item-errors':[{'key':day,'error':type(exception).__name__+': '+str(exception)}]}
        state[key]={'attempted':attempted,'priority':priority,'status':result['state'],'completed':stamp(),'run-key':result.get('run-key')}
        try:atomic(state_path,state)
        except Exception as exception:error('Day cadence save '+day,exception)
        for item in result.get('item-errors',[]):errors.append({**item,'day':day,'priority':priority})
        reports.append({'day':day,'priority':priority,'lane':lane,'started':at,'completed':state[key]['completed'],
                        'state':result['state'],'run-key':result.get('run-key'),'counts':result['counts'],
                        'confirmed-classes':result.get('confirmed-classes',[]),
                        'errors':len(result.get('item-errors',[]))})
    for report in reports:
        for stage,elapsed in report['counts'].get('stage-seconds',{}).items():
            timings[stage]=round(timings.get(stage,0)+elapsed,6)
    return {'ok':not blocking_errors(errors),'state':'Partial' if blocking_errors(errors) else 'Saved','show-key':show,
            'day':reports[0]['day'] if reports else None,'run-key':reports[0].get('run-key') if reports else None,
            'counts':{'saved':sum(r['counts'].get('saved',0) for r in reports),
                      'processing-outcome':'Completed with warnings' if errors else 'Completed',
                      'day-priorities':{day:priority for priority,day in days},
                      'stage-seconds':timings,'days':reports,'total-seconds':round(time.perf_counter()-started,6)},'item-errors':errors}

