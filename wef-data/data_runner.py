"""Data-owned scheduled orchestration around the existing trial modules.

Only entry.js invokes run() on the deployed scheduled path. Imports are inert.
"""
import base64, concurrent.futures, copy, datetime, gzip, importlib.util, json, os, pathlib, subprocess, sys, time
from data_contract import packed, digest, directory_page, DirectoryDrift, freeze, thaw

SOURCE=pathlib.Path(__file__).resolve().parents[1]/'wef-schedules/tests'
sys.path.insert(0,str(SOURCE))
from wef_data_airtable import Airtable, canonical_fence, TransportDeferred
from wef_data_store import DataStore, DATA_BASE, equivalent
from wef_data_handoff import verify_snapshot
from wef_data_context import validate_source_show

def module(name):
    spec=importlib.util.spec_from_file_location(name,SOURCE/(name+'.py'))
    value=importlib.util.module_from_spec(spec);spec.loader.exec_module(value);return value

planner=module('wef-data-adaptive-trial')

COUNT_FRESH_SECONDS=300
COUNT_REFRESH_SECONDS=180

def read_canonical_days(client,context):
    """Read Shows' native day membership, never synthesize dates from a range."""
    base='appCaSmL819KK7UFJ';show=context['show-key']
    selected=client.request('GET',base,'tblJA5CUon5s1sWib',params={
        'filterByFormula':'{show-key}='+json.dumps(show),'fields[]':['show-key']})['records']
    if len(selected)!=1 or selected[0]['fields'].get('show-key')!=show:raise ValueError('Invalid canonical day show owner')
    identity=selected[0]['id'];params={'pageSize':100,'fields[]':['show-day-key','date','show']}
    dates=set();offsets=set();ids=set()
    while True:
        page=client.request('GET',base,'tbl14MaCCDw6dAywH',params=params)
        if not isinstance(page.get('records'),list):raise ValueError('Missing canonical day records')
        for row in page['records']:
            if row['id'] in ids:raise ValueError('Duplicate canonical day record')
            ids.add(row['id']);fields=row['fields'];key=fields.get('show-day-key','');links=fields.get('show',[])
            linked=identity in links
            if not linked and not key.startswith(show+'|'):continue
            day=fields.get('date','')[:10]
            datetime.date.fromisoformat(day)
            if links!=[identity] or key!=show+'|'+day or day in dates or not context['start-date'][:10]<=day<=context['end-date'][:10]:
                raise ValueError('Invalid canonical day key/date/native link')
            dates.add(day)
        offset=page.get('offset')
        if offset is None:break
        if not isinstance(offset,str) or not offset or offset in offsets:raise ValueError('Invalid canonical day offset')
        offsets.add(offset);params['offset']=offset
    if not dates:raise ValueError('Missing canonical day scope')
    return sorted(dates)

def day_interval(day,at):
    from zoneinfo import ZoneInfo
    today=datetime.datetime.fromtimestamp(at,datetime.timezone.utc).astimezone(ZoneInfo('America/New_York')).date()
    offset=(datetime.date.fromisoformat(day)-today).days
    return {0:180,1:3600,2:7200}.get(offset)

def due_days(days,passes,at):
    from zoneinfo import ZoneInfo
    today=datetime.datetime.fromtimestamp(at,datetime.timezone.utc).astimezone(ZoneInfo('America/New_York')).date()
    due=[]
    for day in days:
        offset=(datetime.date.fromisoformat(day)-today).days
        if offset not in (0,1,2,3):continue
        prior=passes.get(day,{})
        interval=day_interval(day,at)
        if not prior.get('completed') or interval is not None and at-prior['completed']>=interval:due.append(day)
    return sorted(due)

def active_class_keys(state):
    """Legacy lists and lossless compact journal controls contain the same keys."""
    value=state['active-classes']
    if not isinstance(value,list):
        import base64,zlib
        try:
            if value['encoding']!='zlib-base64-json':raise ValueError('Unknown control encoding')
            decoder=zlib.decompressobj()
            raw=decoder.decompress(base64.b64decode(value['data'],validate=True),1000001)
            if len(raw)>1000000 or not decoder.eof or decoder.unused_data or decoder.unconsumed_tail:raise ValueError('Invalid control size')
            if digest(raw.decode('utf-8'))!=value['sha256']:raise ValueError('Control digest mismatch')
            keys=json.loads(raw)
            if len(keys)!=value['count']:raise ValueError('Control count mismatch')
        except (KeyError,TypeError,ValueError,zlib.error) as error:
            raise ValueError('Invalid active-class journal control') from error
    else:keys=value
    if not isinstance(keys,list) or any(not isinstance(k,str) or not k.startswith(state['show-key']+'|') for k in keys) or len(set(keys))!=len(keys):
        raise ValueError('Invalid active-class keys')
    return keys

def count_due(states,policies,at,limit=None,slot=None):
    """Compatibility selector; canonical day policy has no fairness/count cap."""
    due=[]
    for state in states:
        interval=day_interval(state['day'],at)
        if interval is None or state.get('inflight'):continue
        last=planner.epoch(state['last_success']) if state.get('last_success') else 0
        if not last or last+interval<=at:due.append((state['day'],state['ring'],state['class_key'],state))
    return [item[-1] for item in sorted(due,key=lambda item:item[:3])]
archiver=module('archive-wef-data')
KEYS=archiver.TABLE_KEYS

def stamp():return datetime.datetime.now(datetime.timezone.utc).isoformat()
def refs(table,*keys):return {'$links':[[table,str(k)] for k in keys if k is not None]}

class CanonicalTransition(ValueError):pass

def atomic(path,value):
    path=pathlib.Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    temporary=path.with_suffix('.tmp');temporary.write_text(packed(value),encoding='utf-8')
    for attempt in range(3):
        try:os.replace(temporary,path);return
        except PermissionError as error:
            if getattr(error,'winerror',None) not in (5,32) or attempt==2:raise
            # Retry only placement of identical bytes; no source/API work is repeated.
            time.sleep(0.05*(attempt+1))

def validate_work_owner(repo,state,runkey,day,reference,plan):
    """A carried job keeps its owner; every intervening accepted head pins it."""
    owner=plan['run-key'];journal=state;key=runkey;seen=set()
    while True:
        if key in seen:raise ValueError('Work owner predecessor cycle')
        seen.add(key)
        held={**journal.get('withheld-day-work',{}),**journal.get('day-work',{})}
        if held.get(day)!=reference or journal.get('show-key')!=plan['show-key']:raise ValueError('Work owner continuity mismatch')
        if repo is not None:
            from retained_feed import run_identity
            if run_identity(repo,key,plan['show-key'])!=journal.get('run-record-id'):raise ValueError('Work owner native ID changed')
        if key==owner:return
        parent=journal.get('predecessor')
        if repo is None or not parent:raise ValueError('Missing pinned work owner')
        from retained_feed import run_rows
        key=parent['run-key'];row=run_rows(repo,plan['show-key']).get(key,{})
        if row.get('status')!='Verified completed revision' or digest(row.get('state-json',''))!=parent['state-sha256']:raise ValueError('Work owner predecessor changed')
        journal=json.loads(row['state-json'])
        if journal.get('phase')!='Complete':raise ValueError('Incomplete work owner predecessor')

def saved_work_plans(runtime,state,runkey,repo=None):
    root=pathlib.Path(runtime).resolve();output={}
    for day,ref in {**state.get('withheld-day-work',{}),**state.get('day-work',{})}.items():
        folder=(root/ref['path']).resolve()
        if not folder.is_relative_to(root):raise ValueError('Work path escapes runtime')
        plan=json.loads((folder/'plan.json').read_text(encoding='utf-8'))
        if digest(packed(plan))!=ref['sha256'] or plan['show-key']!=state['show-key'] or plan['day']!=day:
            raise ValueError('Retained work owner/hash mismatch')
        validate_work_owner(repo,state,runkey,day,ref,plan)
        output[day]=(folder,plan)
    return output

def validate_inventory(payloads,context,dates,existing):
    if len(payloads)!=len(dates):raise ValueError('Incomplete dated inventory')
    seen={}
    for payload,day in zip(payloads,dates):
        validate_source_show(payload,context)
        if payload.get('show_date')!=day or not isinstance(payload.get('rings'),list):raise ValueError('Wrong dated schedule')
        ring_numbers=set()
        for ring in payload['rings']:
            rn=ring.get('ring_number')
            if type(rn) is not int or rn in ring_numbers or not isinstance(ring.get('classes'),list):raise ValueError('Invalid/duplicate ring')
            ring_numbers.add(rn);schedule_ids=set()
            for pos,row in enumerate(ring['classes']):
                source=row.get('class_groupxclasses_id');key=f"{context['show-key']}|{day}|{rn}|"+(str(source) if source is not None else f'position-{pos}')
                if key in schedule_ids:raise ValueError('Duplicate schedule occurrence')
                schedule_ids.add(key)
                prior=existing.get('schedules',{}).get(key)
                if source is None and prior:
                    old=json.loads(prior['source-json'])
                    if (old.get('class_id'),bool(old.get('schedule_break'))) != (row.get('class_id'),bool(row.get('schedule_break'))):
                        raise ValueError('Position-based schedule moved; identity review required')
                if row.get('schedule_break'):continue
                cid=row.get('class_id')
                if type(cid) is not int or cid<=0:raise ValueError('Invalid source class identity')
                occurrence=(day,rn)
                if cid in seen and seen[cid]!=occurrence:raise ValueError('Ambiguous dated/ring class ID; detail association withheld')
                seen[cid]=occurrence

def validate_class_response(payload,state,show):
    """The existing class attribution/identity checks, shared by saved recovery."""
    if not isinstance(payload,dict):raise ValueError('Invalid class response')
    related=payload.get('class_related_data',{});trips=payload.get('trips')
    if str(payload.get('show_id'))!=show.split('|')[1] or payload.get('class',{}).get('class_id')!=state['class_id']:raise ValueError('Wrong show/class')
    if str(related.get('date'))[:10]!=state['day'] or str(related.get('ring'))!=str(state['ring']):raise ValueError('Class moved; inventory required')
    if not isinstance(trips,list) or payload.get('total_entry_trips')!=len(trips):raise ValueError('Incomplete class trips')
    ids=[];source_ids=set()
    for trip in trips:
        if any(type(trip.get(k)) is not int or trip[k]<=0 for k in ('entry_id','horse_id','rider_id')):raise ValueError('Missing trip identity')
        identity=trip.get('entryxclasses_uuid') or trip.get('trip_id')
        valid=(isinstance(identity,str) and bool(identity.strip()) and identity==identity.strip()) if trip.get('entryxclasses_uuid') else (type(identity) is int and identity>0 or isinstance(identity,str) and identity.isdecimal() and int(identity)>0)
        if not valid:raise ValueError('Missing UUID and trip ID')
        source_id=('uuid' if trip.get('entryxclasses_uuid') else 'trip-id',str(identity))
        if source_id in source_ids:raise ValueError('Conflicting class source trip identity')
        source_ids.add(source_id);ids.append((trip['entry_id'],identity))
    if len(set(ids))!=len(ids):raise ValueError('Duplicate class trips')

def read_saved_checkpoint(folder,show,runkey):
    folder=pathlib.Path(folder);reservation_bytes=(folder/'reservation.json').read_bytes()
    reservation=json.loads(reservation_bytes);states=reservation['states'];keys=reservation['class-keys']
    if not reservation['slot'].startswith(runkey+'|slot-') or not reservation['slot'].rsplit('-',1)[-1].isdecimal():raise ValueError('Wrong saved slot owner')
    if not states or keys!=[s['class_key'] for s in states] or len(keys)!=len(set(keys)):raise ValueError('Invalid saved reservation keys')
    for state in states:
        if state['class_key']!=f"{show}|{state['day']}|{state['ring']}|{state['class_id']}":raise ValueError('Saved class identity mismatch')
        datetime.date.fromisoformat(state['day'])
    path=folder/'responses.json';body=path.read_bytes() if path.exists() else None
    saved=json.loads(body) if body else {'scope':{'show-key':show,'keys':keys},'responses':{}}
    if saved['scope']!={'show-key':show,'keys':keys} or not set(saved['responses'])<=set(keys):raise ValueError('Saved response scope changed')
    accepted={}
    for state in states:
        item=saved['responses'].get(state['class_key'])
        if item is None:continue
        if digest(packed(item['value']))!=item['sha256']:raise ValueError('Saved source response changed')
        payload,metric=item['value'];validate_class_response(payload,state,show)
        accepted[state['class_key']]=item['value']
    return {'reservation':reservation,'accepted':accepted,'missing':[k for k in keys if k not in accepted],
            'reservation-sha256':__import__('hashlib').sha256(reservation_bytes).hexdigest(),
            'responses-sha256':__import__('hashlib').sha256(body).hexdigest() if body else None}

def pending_candidate(plan):
    graph=copy.deepcopy(plan['graph']);run=plan['run-key'];start=plan['start'];scope=plan['scope']
    first=run+f'|observation-{start:03}'
    metrics=plan['metric'].get('requests',[plan['metric']])
    requests=[]
    for i,metric in enumerate(metrics,plan['request-start']):
        if 'route' not in metric:continue
        requests.append({'request-key':run+f'|{i:04}','run':refs('runs',run),'collection':refs('collections',first),
            'route':metric['route'],'params':packed(metric['params']),'http':metric['http'],'bytes':metric['bytes'],
            'elapsed-seconds':metric['elapsed_seconds'],'sha256':metric['sha256'],'started':metric['started'],'completed':metric['completed'],'status':'Validated'})
        if plan.get('evidence-storage'):requests[-1].pop('collection')
    graph['requests']=requests
    frozen={'show-key':plan['show-key'],'scope':scope,'source':plan['source'],'metric':plan['metric'],'graph':graph,'next-state':plan['next-state']}
    frozen['verification']=plan.get('verification')
    budget=plan.get('retained-budget',60000)
    if budget not in (60000,1000000) or budget>60000 and not plan.get('evidence-storage'):
        raise ValueError('Invalid retained part budget')
    parts=freeze(frozen,budget=budget);records=[]
    for i,part in enumerate(parts,start):
        records.append({'collection-key':run+f'|observation-{i:03}','run':refs('runs',run),'show':refs('shows',plan['show-key']),
            'scope':scope,'status':'Writing; readback pending','started':plan['metric']['started'],'completed':plan['metric']['completed'],
            'expected-records':sum(map(len,graph.values())),'collected-records':sum(map(len,graph.values())),**part})
    return {**graph,'runs':[{'run-key':run,'status':'Writing; readback pending'}],'collections':records}

def latest_state(rows,show):
    states=[]
    for row in rows:
        if row.get('status')!='Verified completed revision':continue
        state=json.loads(row.get('state-json') or '{}')
        if state.get('show-key')==show:
            state['_head']={'run-key':row['run-key'],'state-sha256':digest(row['state-json'])};states.append(state)
    return copy.deepcopy(max(states,key=lambda x:x.get('sequence',0))) if states else {}

def directory_graph(payload,show,at,old,conflicts=None):
    sid=int(show.split('|')[1]);output={t:{} for t in ('entries','horses','riders','trainers','owners')}
    def put(table,key,row):
        prior=output[table].get(key,old.get(table,{}).get(key,{}))
        if prior.get('last-success') and row.get('last-success') and planner.epoch(prior['last-success'])>planner.epoch(row['last-success']):return
        output[table][key]=row
    for entry in payload['entries']:
        eid=entry['entry_id'];hid=entry.get('horse_id')
        if type(hid) is not int or hid<=0:raise ValueError('Directory horse identity missing')
        key=f'{sid}|{eid}';horse=str(hid)
        row=copy.deepcopy(old.get('entries',{}).get(key,{'entry-key':key,'show':refs('shows',show),
            'entry-id':eid,'entry-number':str(entry.get('number','')),'horse':refs('horses',horse),
            'riders':refs('riders'),'trainers':refs('trainers'),'owners':refs('owners')}))
        if row['horse']!=refs('horses',horse):
            if conflicts is None:raise ValueError('Entry horse identity conflict')
            conflicts[key]={'prior':copy.deepcopy(row),'observed':copy.deepcopy(entry),'observed-at':at}
            continue
        if conflicts is not None and key in conflicts:continue
        put('horses',horse,{'horse-key':horse,'horse-id':hid,'horse-name':entry.get('horse'),'last-success':at})
        for person in entry.get('riders',[]):
            rid=person.get('rider_id')
            if type(rid) is not int or rid<=0:raise ValueError('Directory rider identity missing')
            put('riders',str(rid),{'rider-key':str(rid),'rider-id':rid,'rider-name':person.get('rider_name'),'last-success':at})
            if ['riders',str(rid)] not in row['riders']['$links']:row['riders']['$links'].append(['riders',str(rid)])
        for table,source_id,source_name in [('trainers','trainer_id','trainer'),('owners','entryowner_id','owner')]:
            identity=entry.get(source_id)
            if identity is None or identity==0:continue
            if type(identity) is not int or identity<0:raise ValueError('Directory person identity invalid')
            person_key=str(identity);stem=table[:-1]
            value={stem+'-key':person_key,stem+'-id':person_key if table=='owners' else identity,
                stem+'-name':entry.get(source_name),'last-success':at}
            if table=='owners':value['identity-source']='source owner_id'
            put(table,person_key,value)
            if [table,person_key] not in row[table]['$links']:row[table]['$links'].append([table,person_key])
        row['last-success']=max([v for v in (row.get('last-success'),at) if v],key=planner.epoch);put('entries',key,row)
    return {t:list(rows.values()) for t,rows in output.items() if rows}

class Repository:
    def __init__(self,client,schema,authored):
        self.client=client;self.schema=schema;self.store=DataStore(client,schema,cache=True);self.authored=authored
        self.reload()
    def reload(self,refresh=False):
        self.inventory={t:self.store.inventory(t,refresh=refresh) for t in KEYS}
        self.byid={r['id']:(t,k) for t,rows in self.inventory.items() for k,r in rows.items()}
        self.graph={}
        for table,rows in self.inventory.items():
            definitions=self.store.fields[table];self.graph[table]={}
            for key,record in rows.items():
                row={KEYS[table]:key}
                for name,field in definitions.items():
                    if name not in self.authored[table] or name==KEYS[table]:continue
                    value=record['fields'].get(field['id'])
                    if value is None:continue
                    if field['type']=='multipleRecordLinks':
                        value={'$links':[list(self.byid[v['id'] if isinstance(v,dict) else v]) for v in value]}
                    row[name]=value
                self.graph[table][key]=row
    def patch(self,table,key,fields):
        record=self.store.inventory(table).get(key)
        if not record:raise ValueError('Missing exact record '+table+' '+key)
        out={self.store.fields[table][n]['id']:v for n,v in fields.items()}
        self.client.request('PATCH',DATA_BASE,self.schema['tables'][table]['id'],body={'records':[{'id':record['id'],'fields':out}]})
        actual=self.store.inventory(table,refresh=True,keys=[key]).get(key)
        if actual['id']!=record['id']:raise ValueError('Record ID changed')
        for name,wanted in fields.items():
            f=self.store.fields[table][name]
            if not equivalent(actual['fields'].get(f['id']),wanted,f):raise ValueError('Control readback mismatch '+name)
        self.graph[table].setdefault(key,{KEYS[table]:key}).update(fields)
    def stage(self,graph):
        receipt=self.store.stage(graph)
        for table,rows in graph.items():
            for row in rows:self.graph[table].setdefault(row[KEYS[table]],{}).update(copy.deepcopy(row))
        # Keep newly created IDs available to scope/archive checks in this process.
        for table,keys in receipt['record_map'].items():
            for key,identity in keys.items():self.inventory[table].setdefault(key,{'id':identity})
        return receipt

    def scoped(self,show):
        selected={'shows':{show},'lanes':set(self.graph['lanes'])}
        def take(table,field,parent):
            selected[table]={k for k,r in self.graph[table].items() if any(t==parent and key in selected[parent] for t,key in r.get(field,refs(parent))['$links'])}
        for t in ('days','rings','classes','schedules','entries','collections'):take(t,'show','shows')
        take('groups','ring','rings');take('trips','class','classes');take('results','class','classes');take('work-items','class','classes')
        selected['runs']={k for c in selected['collections'] for t,k in self.graph['collections'][c].get('run',refs('runs'))['$links']}
        selected['runs'].update(k for k,r in self.graph['runs'].items() if json.loads(r.get('state-json') or '{}').get('show-key')==show)
        for table in ('shows','classes','days','rings','trips'):
            selected['runs'].update(k for r in selected[table] for t,k in self.graph[table][r].get('last-run',refs('runs'))['$links'])
        take('requests','run','runs');take('slots','run','runs')
        for table,entry_field,trip_field in [('horses','horse','horse'),('riders','riders','rider'),('trainers','trainers','trainer'),('owners','owners','owner')]:
            selected[table]={key for k in selected['entries'] for t,key in self.graph['entries'][k].get(entry_field,refs(table))['$links']}
            selected[table].update(key for k in selected['trips'] for t,key in self.graph['trips'][k].get(trip_field,refs(table))['$links'])
        # Include dependencies of authored forward links without importing unrelated shows.
        changed=True
        while changed:
            changed=False
            for table,keys in list(selected.items()):
                for key in list(keys):
                    for value in self.graph[table][key].values():
                        if isinstance(value,dict) and '$links' in value:
                            for target,k in value['$links']:
                                if target=='shows' and k!=show:raise ValueError('Cross-show archive dependency')
                                if k not in selected[target]:selected[target].add(k);changed=True
        return {t:[copy.deepcopy(self.graph[t][k]) for k in sorted(selected[t])] for t in KEYS}

class SourceFailure(RuntimeError):
    def __init__(self,message,diagnostic):super().__init__(message);self.diagnostic=diagnostic

def transient_source(error):
    return isinstance(error,SourceFailure) and (error.diagnostic.get('http') in (502,503,504) or error.diagnostic.get('timeout') is True)

def capture_source_batch(path,scope,items,key_for,fetch):
    """Keep validated parallel responses before raising; resume missing keys only."""
    path=pathlib.Path(path)
    saved=json.loads(path.read_text(encoding='utf-8')) if path.exists() else {'scope':scope,'responses':{}}
    if saved['scope']!=scope:raise ValueError('Saved source batch scope changed')
    allowed=set(scope['keys']);responses=saved['responses'];wanted=[key_for(item) for item in items]
    if len(set(wanted))!=len(wanted) or not set(wanted)<=allowed or not set(responses)<=allowed:raise ValueError('Invalid saved source keys')
    for item in responses.values():
        if digest(packed(item['value']))!=item['sha256']:raise ValueError('Saved source response changed')
    errors=[]
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
        futures={pool.submit(fetch,item):key_for(item) for item in items if key_for(item) not in responses}
        for future in concurrent.futures.as_completed(futures):
            try:value=future.result()
            except Exception as error:errors.append(error);continue
            responses[futures[future]]={'value':value,'sha256':digest(packed(value))};atomic(path,saved)
    if errors:raise next((e for e in errors if not transient_source(e)),errors[0])
    return [responses[key]['value'] for key in wanted]

def defer_source(runtime,run_key,error,policy):
    """Three scheduled retries; preserve each error independently of API writes."""
    if not transient_source(error):return None
    runtime=pathlib.Path(runtime);path=runtime/'source-deferred.json'
    prior=json.loads(path.read_text(encoding='utf-8')) if path.exists() else {}
    counts=prior.get('counts',{}) if prior.get('run-key')==run_key else {}
    signature=digest(packed({'route':error.diagnostic.get('route'),'params':error.diagnostic.get('params')}))
    failures=counts.get(signature,0)+1;counts[signature]=failures;at=stamp()
    evidence={'at':at,'run-key':run_key,'failures':failures,'source':error.diagnostic,'error':str(error)}
    atomic(runtime/'source-errors'/(at.replace(':','_')+'-'+signature[:12]+'.json'),evidence)
    if failures>3:return None
    delay=min(policy.get('maximum-seconds',300),policy.get('interval-seconds',60)*2**(failures-1))
    atomic(path,{**evidence,'counts':counts,'next-due':time.time()+delay})
    return {'ok':False,'state':'Deferred','run-key':run_key,'source':error.diagnostic,'source-failures':failures,
            'retry-after-seconds':delay,'error':str(error),'operation':'GET','target':error.diagnostic.get('route'),'write-outcome':'NotApplicable'}

class Source:
    def fetch(self,route,params,timeout=18):
        try:result=subprocess.run(['node',str(pathlib.Path(__file__).with_name('source-reader.js')),route,packed(params),str(timeout)],capture_output=True,timeout=timeout+10)
        except subprocess.TimeoutExpired:raise SourceFailure('Source reader timed out',{'route':route,'params':params,'timeout':True,'completed':stamp()}) from None
        try:response=json.loads(result.stdout)
        except (ValueError,TypeError):raise SourceFailure('Source reader did not return a valid result',{'route':route,'params':params,'exit-code':result.returncode}) from None
        if result.returncode or not response.get('ok'):raise SourceFailure(response.get('error','Source read failed'),response.get('metric',{'route':route,'params':params}))
        body=base64.b64decode(response['body'],validate=True);metric=response['metric']
        if metric['bytes']!=len(body) or metric['sha256']!=__import__('hashlib').sha256(body).hexdigest():raise ValueError('Source body receipt mismatch')
        payload=json.loads(body)
        def check(v):
            if isinstance(v,dict):
                if '_t' in v:raise ValueError('Unresolved encrypted envelope')
                for x in v.values():check(x)
            elif isinstance(v,list):
                for x in v:check(x)
        check(payload)
        return planner.safe(payload),metric

class Runner:
    def __init__(self,repo,source,runtime,data_repo,evidence=None):
        self.repo=repo;self.client=repo.client;self.source=source;self.runtime=pathlib.Path(runtime);self.data_repo=pathlib.Path(data_repo)
        self.evidence=evidence;self.repo.evidence=evidence
        self.repo.archive_repository=self.data_repo
    def begin(self,show,state,mode='Scheduled Data acquisition'):
        self.at=stamp();self.runkey=('archive-' if mode=='Historical archive closure' else 'adaptive-')+self.at
        self.show=show;self.state=copy.deepcopy(state);self.state.update({'show-key':show,'sequence':state.get('sequence',0)+1,'phase':'Writing','pending':None,'observations':[],'requests':0})
        self.repo.history_show=show
        self.state['predecessor']=self.state.pop('_head',None);self.state['slots']=0
        self.state.pop('completion-at',None)
        self.state.pop('publication-day',None)
        self.state.pop('publication-kind',None)
        recovery=self.state.get('saved-checkpoint-recovery',{})
        if recovery.get('phase')=='Complete' and recovery.get('published-in'):
            self.state.pop('saved-checkpoint-recovery',None)
        if self.evidence:
            from archive_evidence import FORMAT
            self.state['evidence-storage']=FORMAT
        run={'run-key':self.runkey,'mode':mode,'status':'Writing; readback pending','started':self.at,'state-json':packed(self.state)}
        atomic(self.runtime/'active.json',{'run-key':self.runkey,'reserved-run':run})
        receipt=self.repo.stage({'runs':[run]})
        self.state['run-record-id']=receipt['record_map']['runs'][self.runkey];self.journal()
    def journal(self):
        value=self.state.get('active-classes')
        if isinstance(value,list) and len(packed(value).encode('utf-8'))>8000:
            import base64,zlib
            raw=packed(active_class_keys(self.state))
            self.state['active-classes']={'encoding':'zlib-base64-json','sha256':digest(raw),'count':len(value),
                'data':base64.b64encode(zlib.compress(raw.encode('utf-8'))).decode('ascii')}
        text=packed(self.state)
        if len(text.encode())>60000:raise ValueError('Run journal exceeds accepted cell budget')
        self.repo.patch('runs',self.runkey,{'state-json':text})
    def work_next_state(self,day,plan,base=None):
        base=self.state if base is None else base
        next_state=copy.deepcopy(plan['next-state'])
        active=next_state.pop('active-day',None)
        if active is not None:
            current=active_class_keys(base) if 'active-classes' in base else []
            next_state['active-classes']=sorted(k for k in current if k.split('|')[2]!=day)+active
        passes=next_state.pop('day-passes',None)
        if passes:next_state['day-passes']={**base.get('day-passes',{}),**passes}
        next_state['day-work']={k:v for k,v in base.get('day-work',{}).items() if k!=day}
        return next_state

    def preflight_work(self,plan,reference):
        """Forecast every queued acceptance before any identity or business write."""
        jobs={**self.state.get('day-work',{}),plan['day']:reference};prospective=copy.deepcopy(self.state)
        extra=[];pending=[]
        for day,ref in jobs.items():
            job=plan if day==plan['day'] else json.loads((self.runtime/ref['path']/'plan.json').read_text(encoding='utf-8'))
            if digest(packed(job))!=ref['sha256']:raise ValueError('Saved work hash changed')
            pins=copy.deepcopy(job['pins'])
            for table,rows in job['graph'].items():
                for row in rows:pins.setdefault(table,{}).setdefault(str(row[KEYS[table]]),'rec'+'X'*14)
            requests=job['metric'].get('requests',[job['metric']])
            pins['requests']={self.runkey+'|'+str(i).zfill(10):'rec'+'X'*14 for i,m in enumerate(requests) if 'route' in m}
            proof={'tables':{t:{'expected':len(rows),'collected':len(rows),'readback':len(rows)} for t,rows in job['graph'].items()},
                   'record-map':pins,'verified-at':'2000-01-01T00:00:00.000000+00:00'}
            frozen={'show-key':self.show,'scope':job['scope'],'source':job['source'],'metric':job['metric'],
                    'graph':job['graph'],'next-state':job['next-state'],'verification':proof}
            budget=1000000 if self.state.get('evidence-storage') else 60000
            frozen['graph']={**frozen['graph'],'requests':[{'request-key':self.runkey+'|'+'9'*10,'run':refs('runs',self.runkey),
                'collection':refs('collections',self.runkey+'|observation-'+'9'*10),
                **m,'route':m.get('route'),'params':packed(m.get('params',{}))} for m in requests if 'route' in m]}
            parts=freeze(frozen,budget=budget)
            for i,part in enumerate(parts):
                key=self.runkey+'|observation-'+str(i).zfill(10)
                ref={'key':key,'sha256':'f'*64,'record-id':'rec'+'X'*14}
                if self.state.get('evidence-storage'):
                    from archive_evidence import FORMAT,prefix_for
                    ref={'key':key,'sha256':'f'*64,'scope':job['scope'],'archive':{'format':FORMAT,'path':prefix_for(self.show)+'/parts/'+'f'*64+'.json.gz','sha256':'f'*64,'bytes':9999999999}}
                extra.append(ref);pending.append(key)
            prospective.update(self.work_next_state(day,job,prospective))
        prospective['observations']=self.state['observations']+extra
        prospective['pending']={'keys':pending,'hashes':['f'*64]*len(pending)}
        active=prospective.get('active-classes')
        if isinstance(active,list) and len(packed(active).encode())>8000:
            import zlib
            raw=packed(active)
            prospective['active-classes']={'encoding':'zlib-base64-json','sha256':digest(raw),'count':len(active),
                'data':base64.b64encode(zlib.compress(raw.encode())).decode('ascii')}
        if len(packed(prospective).encode())>60000:raise ValueError('Work acceptance exceeds journal budget')

    def queue_work(self,day,scope,payload,metric,graph,next_state,queue_state=None):
        """Save one exact source unit; only small file references enter the journal."""
        jobs=self.state.setdefault('day-work',{})
        if day in jobs:raise ValueError('Unfinished day work must resume first')
        mapping={t:{k:r['id'] for k,r in rows.items()} for t,rows in self.repo.inventory.items()}
        proposed={t:{str(r[KEYS[t]]) for r in rows} for t,rows in graph.items()}
        for table,rows in graph.items():
            if len(proposed[table])!=len(rows):raise ValueError('Duplicate work key')
            for row in rows:
                if set(row)-set(self.repo.store.fields[table]):raise ValueError('Unknown work field')
                for field,value in row.items():
                    if isinstance(value,str) and len(value.encode('utf-8'))>80000:raise ValueError('Work field exceeds byte budget')
                    old=self.repo.graph[table].get(str(row[KEYS[table]]),{})
                    if field.endswith('-id') and old.get(field) is not None and value is not None and old[field]!=value:raise ValueError('Conflicting work source identity')
                    if isinstance(value,dict) and '$links' in value:
                        for target,key in value['$links']:
                            if key not in mapping.get(target,{}) and key not in proposed.get(target,set()):raise ValueError('Unresolved work link')
        wanted={t:set(keys) for t,keys in proposed.items()}
        for rows in graph.values():
            for row in rows:
                for value in row.values():
                    if isinstance(value,dict) and '$links' in value:
                        for target,key in value['$links']:wanted.setdefault(target,set()).add(key)
        pins={t:{k:mapping[t][k] for k in keys if k in mapping.get(t,{})} for t,keys in wanted.items()}
        plan={'run-key':self.runkey,'show-key':self.show,'day':day,'scope':scope,'source':payload,'metric':metric,
              'graph':graph,'next-state':next_state,'pins':pins}
        sha=digest(packed(plan));pointer=(pathlib.Path(self.runkey.replace(':','_'))/'day-work'/sha).as_posix()
        reference={'path':pointer,'sha256':sha}
        self.preflight_work(plan,reference)
        folder=self.runtime/pointer;folder.mkdir(parents=True,exist_ok=True)
        path=folder/'plan.json'
        if path.exists() and path.read_text(encoding='utf-8')!=packed(plan):raise ValueError('Saved work collision')
        atomic(path,plan)
        candidate={**self.state,**(queue_state or {}),'day-work':{**jobs,day:reference}}
        if len(packed(candidate).encode())>60000:raise ValueError('Work reference exceeds journal budget')
        self.state=candidate;self.journal()

    def step_work(self,day):
        """One transport batch with durable readback; no class/entry business cap."""
        reference=self.state['day-work'][day];folder=(self.runtime/reference['path']).resolve()
        if not folder.is_relative_to(self.runtime.resolve()):raise ValueError('Work path escapes runtime')
        plan=json.loads((folder/'plan.json').read_text(encoding='utf-8'))
        if digest(packed(plan))!=reference['sha256'] or plan['show-key']!=self.show or plan['day']!=day:
            raise ValueError('Saved work owner/hash mismatch')
        validate_work_owner(self.repo,self.state,self.runkey,day,reference,plan)
        self.preflight_work(plan,reference)
        progress_path=folder/'progress.json'
        progress=json.loads(progress_path.read_text(encoding='utf-8')) if progress_path.exists() else {'pins':copy.deepcopy(plan['pins']),'blocks':0,'reservations':0}
        for table,pins in plan['pins'].items():
            actual=self.repo.store.inventory(table)
            if any(actual.get(key,{}).get('id')!=identity or progress['pins'].get(table,{}).get(key)!=identity for key,identity in pins.items()):
                raise ValueError('Original work native ID changed')
        reserved=copy.deepcopy(plan['pins']);reservations=sorted(folder.glob('reservation-*.json'),key=lambda p:int(p.stem.split('-')[-1]))
        for index,path in enumerate(reservations):
            saved=json.loads(path.read_text(encoding='utf-8'));sha=saved.pop('sha256')
            if digest(packed(saved))!=sha or saved['plan-sha256']!=reference['sha256'] or saved['reservation']!=index:raise ValueError('Work reservation receipt changed')
            for table,pins in saved['pins'].items():
                allowed={str(r[KEYS[table]]) for r in plan['graph'].get(table,[])}
                if not set(pins)<=allowed or set(pins)&set(reserved.get(table,{})):raise ValueError('Work reservation scope changed')
                reserved.setdefault(table,{}).update(pins)
        if progress.get('reservations',0)>len(reservations):raise ValueError('Missing work reservation receipt')
        for table,pins in progress['pins'].items():
            if any(reserved.get(table,{}).get(key)!=identity for key,identity in pins.items()):raise ValueError('Work reservation native ID changed')
        progress['pins']=reserved;progress['reservations']=len(reservations)
        for table,pins in progress['pins'].items():
            actual=self.repo.store.inventory(table)
            if any(actual.get(key,{}).get('id')!=identity for key,identity in pins.items()):raise ValueError('Saved work native ID changed')
        for table,rows in plan['graph'].items():
            missing=[str(row[KEYS[table]]) for row in rows if str(row[KEYS[table]]) not in progress['pins'].get(table,{})]
            if missing:
                pinned=self.repo.store.reserve(table,missing[:10])
                saved={'plan-sha256':reference['sha256'],'reservation':progress['reservations'],'pins':{table:pinned}}
                saved['sha256']=digest(packed(saved));atomic(folder/('reservation-'+str(progress['reservations'])+'.json'),saved)
                progress['reservations']+=1
                progress['pins'].setdefault(table,{}).update(pinned)
                for key,identity in pinned.items():self.repo.inventory[table][key]=copy.deepcopy(self.repo.store.inventory(table)[key])
                atomic(progress_path,progress);return False
        # Preserve full cached records; reservations never replace authored fields.
        blocks=[(table,rows[start:start+10]) for table,rows in plan['graph'].items() for start in range(0,len(rows),10)]
        def read_block(index):
            saved=json.loads((folder/('block-'+str(index)+'.json')).read_text(encoding='utf-8'));sha=saved.pop('sha256')
            if digest(packed(saved))!=sha:raise ValueError('Work readback receipt changed')
            table,expected=blocks[index];keys=[str(row[KEYS[table]]) for row in expected]
            if saved['plan-sha256']!=reference['sha256'] or saved['block']!=index or saved['table']!=table or [str(row[KEYS[table]]) for row in saved['rows']]!=keys or saved['pins']!={k:progress['pins'][table][k] for k in keys}:
                raise ValueError('Work readback receipt ownership mismatch')
            if any(got!=wanted and got!={KEYS[table]:wanted[KEYS[table]]} for got,wanted in zip(saved['rows'],expected)):raise ValueError('Work readback fields changed')
            return saved
        if progress['blocks']<len(blocks):
            index=progress['blocks']
            if (folder/('block-'+str(index)+'.json')).exists():
                read_block(index);progress['blocks']+=1;atomic(progress_path,progress);return False
            # Reuse the store's run fence once for a bounded cohort group. Its
            # individual HTTP writes/readbacks still contain at most ten rows.
            # Keep each original block receipt so old plans and lost responses
            # resume without replacing keys or advancing unverified progress.
            width=10 if len(plan.get('source',{}).get('classes',[]))>1 else 1
            group=[];staged={'runs':[{'run-key':self.runkey}]}
            for number in range(index,min(len(blocks),index+width)):
                if (folder/('block-'+str(number)+'.json')).exists():break
                table,rows=blocks[number];effective=[]
                for row in rows:
                    old=self.repo.graph[table].get(str(row[KEYS[table]]),{})
                    if old.get('last-success') and row.get('last-success') and planner.epoch(old['last-success'])>planner.epoch(row['last-success']):row={KEYS[table]:row[KEYS[table]]}
                    effective.append(row)
                staged.setdefault(table,[]).extend(effective);group.append((number,table,rows,effective))
            receipt=self.repo.stage(staged)
            for number,table,rows,effective in group:
                pins=progress['pins'][table]
                if any(receipt['record_map'][table][str(r[KEYS[table]])]!=pins[str(r[KEYS[table]])] for r in rows):raise ValueError('Work readback ID changed')
                saved={'plan-sha256':reference['sha256'],'block':number,'table':table,'rows':effective,
                       'pins':{str(r[KEYS[table]]):pins[str(r[KEYS[table]])] for r in rows}}
                saved['sha256']=digest(packed(saved));atomic(folder/('block-'+str(number)+'.json'),saved)
                progress['blocks']=number+1;atomic(progress_path,progress)
            return False
        graph={}
        for index in range(len(blocks)):
            saved=read_block(index);table=saved['table']
            for row in saved['rows']:
                old=self.repo.graph[table].get(str(row[KEYS[table]]),{})
                if old.get('last-success') and row.get('last-success') and planner.epoch(old['last-success'])>planner.epoch(row['last-success']):row={KEYS[table]:row[KEYS[table]]}
                graph.setdefault(table,[]).append(row)
        verification={'tables':{t:{'expected':len(rows),'collected':len(rows),'readback':len(rows)} for t,rows in graph.items()},
                      'record-map':progress['pins'],'verified-at':stamp()}
        self.observation(plan['scope'],plan['source'],plan['metric'],graph,self.work_next_state(day,plan),verification=verification)
        return True
    def recover(self,authority):
        path=self.runtime/'active.json'
        if not path.exists():return False
        reserved=json.loads(path.read_text(encoding='utf-8'));key=reserved['run-key'];row=self.repo.graph['runs'].get(key)
        if not row:
            self.repo.stage({'runs':[reserved['reserved-run']]});row=self.repo.graph['runs'][key]
        elif row.get('status') in (None,'','Writing; readback pending') and not row.get('state-json'):
            # An accepted identity-reservation POST may lose its response before
            # authored fields are staged. Complete that same reserved ID first.
            self.repo.stage({'runs':[reserved['reserved-run']]});row=self.repo.graph['runs'][key]
        if row.get('status')=='Verified completed revision':
            path.unlink();(self.runtime/'pending.json').unlink(missing_ok=True);return False
        resume=self.runtime/'resume-approved.json'
        approval=json.loads(resume.read_text(encoding='utf-8')) if resume.exists() else None
        failed=row.get('status')=='Failed; prior accepted state retained'
        if row.get('status')!='Writing; readback pending' and not (failed and approval and approval.get('run-key')==key and approval.get('approval')):raise ValueError('Active run is not resumable')
        self.runkey=key;self.at=row['started'];self.state=json.loads(row['state-json']);self.show=self.state['show-key']
        self.repo.history_show=self.show
        self.state['run-record-id']=self.repo.inventory['runs'][key]['id']
        if self.state.get('authority-before') and self.state['authority-before']!=list(canonical_fence(authority)):
            raise ValueError('Interrupted run authority changed; no replay')
        if failed:
            self.state.setdefault('authorized-resumptions',[]).append({'at':stamp(),'approval':approval['approval']})
            self.repo.patch('runs',key,{'status':'Writing; readback pending'});self.journal()
            resume.unlink()
        pending=self.runtime/'pending.json'
        if pending.exists():
            plan=json.loads(pending.read_text(encoding='utf-8'))
            if plan['run-key']!=key:raise ValueError('Pending unit belongs to another run')
            if plan['start']==len(self.state['observations'])+1:self.apply_pending(plan)
            elif plan['start']<=len(self.state['observations']):pending.unlink()
            else:raise ValueError('Pending observation sequence gap')
        elif self.state.get('pending'):raise ValueError('Missing exact pending replay input')
        return True
    def observation(self,scope,payload,metric,graph,next_state=None,verification=None):
        plan={'run-key':self.runkey,'show-key':self.show,'scope':scope,'source':payload,'metric':metric,
            'graph':graph,'next-state':next_state or {},'start':len(self.state['observations'])+1,'request-start':self.state.get('requests',0)+1}
        if self.state.get('evidence-storage'):
            plan['evidence-storage']=self.state['evidence-storage'];plan['retained-budget']=1000000
        if verification:plan['verification']=verification;plan['business-verified']=True
        atomic(self.runtime/'pending.json',plan)
        return self.apply_pending(plan)
    def apply_pending(self,plan):
        candidate=pending_candidate(plan);records=candidate['collections']
        self.state['pending']={'keys':[r['collection-key'] for r in records],'hashes':[r['retained-sha256'] for r in records]};self.journal()
        archived=bool(plan.get('evidence-storage'))
        if plan.get('business-verified'):
            for table,rows in plan['verification']['record-map'].items():
                if any(self.repo.store.inventory(table).get(k,{}).get('id')!=identity for k,identity in rows.items()):raise ValueError('Verified work native ID changed')
        staged={t:rows for t,rows in candidate.items() if not (archived and t=='collections') and (not plan.get('business-verified') or t in ('runs','requests','collections'))}
        receipt=self.repo.stage(staged)
        if plan.get('business-verified'):
            plan['verification']['record-map']['requests']={r['request-key']:receipt['record_map']['requests'][r['request-key']] for r in candidate.get('requests',[])}
            plan['verification']['tables']['requests']=receipt['tables'].get('requests',{'expected':0,'collected':0,'readback':0})
            atomic(self.runtime/'pending.json',plan);candidate=pending_candidate(plan);records=candidate['collections']
            if not archived:receipt=self.repo.stage({t:candidate[t] for t in ('runs','collections')})
        if not plan.get('verification'):
            wanted={t:{str(r[KEYS[t]]) for r in rows} for t,rows in candidate.items() if t not in ('runs','collections')}
            for table,rows in candidate.items():
                if archived and table=='collections':continue
                for row in rows:
                    for value in row.values():
                        if isinstance(value,dict) and '$links' in value:
                            for table,key in value['$links']:wanted.setdefault(table,set()).add(key)
            plan['verification']={'tables':receipt['tables'],'record-map':{t:{k:receipt['record_map'][t][k] for k in keys} for t,keys in wanted.items()},'verified-at':stamp()}
            atomic(self.runtime/'pending.json',plan)
            candidate=pending_candidate(plan);records=candidate['collections']
            self.state['pending']={'keys':[r['collection-key'] for r in records],'hashes':[r['retained-sha256'] for r in records]};self.journal()
            if not archived:receipt=self.repo.stage({t:candidate[t] for t in ('runs','collections')})
        if archived:
            references=self.evidence.stage(self.show,records)
        else:
            for row in records:self.repo.patch('collections',row['collection-key'],{'readback-records':row['expected-records']})
            references=[{'key':r['collection-key'],'sha256':r['retained-sha256'],'record-id':receipt['record_map']['collections'][r['collection-key']]} for r in records]
        self.state.update(plan['next-state'])
        self.state['observations'].extend(references)
        self.state['requests']=plan['request-start']-1+len(candidate.get('requests',[]))
        self.state['pending']=None;self.journal();(self.runtime/'pending.json').unlink(missing_ok=True)
        return receipt
    def finish(self,authority):
        day=self.state.get('publication-day');recovery=self.state.get('saved-checkpoint-recovery',{})
        recovered_boundary=self.state.get('publication-kind')=='checkpoint-recovery' and recovery.get('phase')=='Complete' and recovery.get('published-in')==self.runkey
        if self.state.get('day-work') and not recovered_boundary and (not day or day in self.state['day-work'] or not self.state.get('day-passes',{}).get(day,{}).get('completed')):
            raise ValueError('Unfinished day work has no verified publication boundary')
        after=self.client.canonical_context()
        if canonical_fence(authority)!=canonical_fence(after):raise CanonicalTransition('Canonical scope changed; completion withheld')
        # Read the immutable candidate itself, never just mutable business rows.
        archived=bool(self.state.get('evidence-storage'))
        from retained_feed import collection_rows,run_rows
        logical=collection_rows(self.repo,self.state)
        rows={} if archived else self.repo.store.inventory('collections',refresh=True,keys=[i['key'] for i in self.state['observations']]);parts=[]
        for item in self.state['observations']:
            fs=self.repo.store.fields['collections']
            got=logical[item['key']] if archived else {n:rows[item['key']]['fields'].get(fs[n]['id']) for n in ('retained-json','retained-sha256')}
            if got['retained-sha256']!=item['sha256'] or digest(got['retained-json'])!=item['sha256']:raise ValueError('Completion evidence changed')
            part=json.loads(got['retained-json'])
            if part['part']==0 and parts:thaw(parts);parts=[]
            parts.append(got)
        if parts:thaw(parts)
        from retained_feed import facts_for_state
        from wef_data_handoff import assignment_evidence
        assignment_counts=assignment_evidence(facts_for_state(self.repo,self.state))['summary']
        for item in ([] if archived else self.state['observations']):
            status=rows[item['key']]['fields'].get(self.repo.store.fields['collections']['status']['id'])
            if status!='Verified retained observation':self.repo.patch('collections',item['key'],{'status':'Verified retained observation'})
        evidence_receipt=None
        if archived:
            self.state.setdefault('completion-at',stamp());self.journal()
            completed={**self.state,'phase':'Complete','authority':list(canonical_fence(after)),'completed':self.state['completion-at']}
            evidence_receipt=self.evidence.publish(self.repo,completed,self.runkey)
            if canonical_fence(after)!=canonical_fence(self.client.canonical_context()):raise CanonicalTransition('Canonical scope changed during archive publication')
            self.state=completed
        else:self.state.update({'phase':'Complete','authority':list(canonical_fence(after)),'completed':stamp()})
        self.journal()
        self.repo.patch('runs',self.runkey,{'status':'Verified completed revision','completed':self.state['completed'],
            'elapsed-seconds':(datetime.datetime.now(datetime.timezone.utc)-datetime.datetime.fromisoformat(self.at)).total_seconds(),
            'summary':packed({'show-key':self.show,'coverage':self.state.get('coverage','Verified bounded observations'),
                'observations':len(self.state['observations']),'directory-complete':self.state.get('directory',{}).get('complete',False),
                'identity-conflicts':self.state.get('directory-conflicts',[]),
                'withheld-class-contexts':sorted(self.state.get('withheld-class-contexts',{})),
                'timing':self.state.get('timing',{}),'capacity':self.state.get('capacity',{}),'assignment-counts':assignment_counts,
                **({'evidence-archive':evidence_receipt} if evidence_receipt else {})}),
            'source-requests':self.state.get('requests',0),'errors':len(self.state.get('directory-conflicts',[]))})
        (self.runtime/'active.json').unlink(missing_ok=True)
        return {'ok':True,'show-key':self.show,'run-key':self.runkey,'observations':len(self.state['observations']),
                'identity-conflicts':self.state.get('directory-conflicts',[]),
                'retention-waits':getattr(self,'retention_waits',[]),'assignment-counts':assignment_counts,
                **({'evidence-archive':evidence_receipt} if evidence_receipt else {})}

    def close75(self,authority):
        key='15|200000075';row=self.repo.graph['shows'].get(key)
        if not row or row.get('status')=='Archived retained snapshot':return
        completed=[r for r in self.repo.graph['runs'].values() if r.get('mode')=='Historical archive closure' and r.get('status')=='Verified completed revision' and json.loads(r.get('state-json') or '{}').get('show-key')==key]
        if completed:
            self.repo.patch('shows',key,{'status':'Archived retained snapshot'});return
        commit='ddcde068ca3323242ec1fe81d96f20aee82fa6a3'
        prefix='wef-data/shows/15/200000075/4c88a40a2a03efa5d1e48c6be92f528139061f7bbb4d16bee26d3731df323d16'
        subprocess.run(['git','-C',str(self.data_repo),'fetch','origin','main'],check=True,capture_output=True)
        subprocess.run(['git','-C',str(self.data_repo),'merge-base','--is-ancestor',commit,'origin/main'],check=True,capture_output=True)
        manifest,tables=verify_snapshot(self.data_repo/prefix)
        remote_manifest=subprocess.check_output(['git','-C',str(self.data_repo),'show',commit+':'+prefix+'/manifest.json'])
        if json.loads(remote_manifest)!=manifest:raise ValueError('Pinned manifest mismatch')
        archived_ids={}
        for path,proof in manifest['files'].items():
            body=subprocess.check_output(['git','-C',str(self.data_repo),'show',commit+':'+prefix+'/'+path])
            if len(body)!=proof['bytes'] or __import__('hashlib').sha256(body).hexdigest()!=proof['sha256']:raise ValueError('Pinned remote archive mismatch')
            if path.startswith('tables/'):
                table=path.split('/')[1].split('.')[0]
                archived_ids[table]={str(r['fields'][KEYS[table]]):r['id'] for r in json.loads(gzip.decompress(body))}
        live=self.repo.scoped(key)
        advances=[]
        for table,rows in live.items():
            if {r[KEYS[table]] for r in rows}!=set(tables[table]):raise ValueError('Historical scope membership changed: '+table)
            for r in rows:
                identity=self.repo.inventory[table][r[KEYS[table]]]['id']
                if identity!=archived_ids[table][r[KEYS[table]]]:raise ValueError('Historical record ID changed')
                for field,old in tables[table][r[KEYS[table]]].items():
                    if table in ('horses','riders','trainers','owners') and field=='last-success' and not equivalent(r.get(field),old,self.repo.store.fields[table][field]):
                        if planner.epoch(r[field])<planner.epoch(old):raise ValueError('Historical shared timestamp regressed')
                        linkfield={'horses':'horse','riders':'riders','trainers':'trainers','owners':'owners'}[table]
                        if not any(e['show']==refs('shows','15|200000076') and [table,r[KEYS[table]]] in e.get(linkfield,refs(table))['$links'] for e in self.repo.graph['entries'].values()):raise ValueError('Shared advance has no show76 evidence')
                        advances.append([table,r[KEYS[table]]]);continue
                    if field not in self.repo.store.fields[table]:raise ValueError('Unknown historic field')
                    if isinstance(old,dict):
                        if sorted(old['$links'])!=sorted(r.get(field,refs(''))['$links']):raise ValueError('Historical links changed')
                    elif not equivalent(r.get(field),old,self.repo.store.fields[table][field]):raise ValueError('Historical authored value changed: '+table+'.'+field)
        self.begin(key,{},'Historical archive closure')
        self.state['authority-before']=list(canonical_fence(authority));self.journal()
        receipt={'commit':commit,'revision':prefix,'manifest':manifest,'scope':'October24 retained scheduled-class snapshot; not final-show or complete directory',
            'records-retained':manifest['record_count'],'closed-at':stamp(),'retirement':'None; all records retained',
            'accepted-shared-timestamp-advances':advances,'original-runs':list(tables['runs']),
            'verification':'All payload hashes/bytes, nineteen scoped sets, exact Airtable IDs, authored values and forward links reconciled'}
        self.observation('archive',receipt,{'started':self.at,'completed':stamp()}, {})
        self.finish(authority)
        self.repo.patch('shows',key,{'status':'Archived retained snapshot'})

    def recover_saved_checkpoint(self,context,folder):
        """Restore validated saved facts; reconcile only missing source contexts."""
        folder=pathlib.Path(folder).resolve()
        if not folder.is_relative_to(self.runtime.resolve()):raise ValueError('Saved checkpoint escapes runtime')
        saved=read_saved_checkpoint(folder,self.show,self.runkey);reservation=saved['reservation']
        if any(k not in self.repo.graph['work-items'] for k in reservation['class-keys']):raise ValueError('Saved checkpoint native work item missing')
        recovery=folder/'recovery';path=recovery/'context.json'
        marker={'slot':reservation['slot'],'path':recovery.relative_to(self.runtime.resolve()).as_posix(),
                'reservation-sha256':saved['reservation-sha256'],'responses-sha256':saved['responses-sha256']}
        prior=self.state.get('saved-checkpoint-recovery',{})
        if any(prior.get(k)!=v for k,v in marker.items()) and prior:raise ValueError('Saved checkpoint recovery identity changed')
        days=context.get('canonical-days',self.state.get('canonical-days'))
        if not days:raise ValueError('Canonical recovery days missing')
        if path.exists():
            envelope=json.loads(path.read_text(encoding='utf-8'));body=envelope['body']
            if digest(packed(body))!=envelope['sha256'] or body['owner']!=marker or body['dates']!=days:raise ValueError('Saved recovery context changed')
            if prior.get('prepared-sha256') not in (None,envelope['sha256']):raise ValueError('Pinned recovery context changed')
        else:
            self.state['saved-checkpoint-recovery']={**marker,'phase':'Capturing'};self.journal()
            def fetch(day):
                payload,metric=self.source.fetch('/schedule',{'date':day,'show_id':context['show-id'],'customer_id':context['customer-id']})
                validate_inventory([payload],context,[day],self.repo.graph)
                facility=payload.get('show',{}).get('facility_id');old=self.repo.graph['shows'][self.show].get('facility-id')
                if facility is None or old not in (None,facility):raise ValueError('Recovery inventory facility conflict')
                return payload,metric
            inventory=capture_source_batch(recovery/'inventory.json',{'show-key':self.show,'keys':days},days,str,fetch)
            locations={}
            for payload,metric in inventory:
                for ring in payload['rings']:
                    for cls in ring['classes']:
                        if not cls.get('schedule_break'):
                            locations.setdefault(cls['class_id'],set()).add(f"{self.show}|{payload['show_date']}|{ring['ring_number']}|{cls['class_id']}")
            additional={};held={};raw_observations=[]
            for state in reservation['states']:
                key=state['class_key']
                if key not in saved['missing']:continue
                scopes=sorted(locations.get(state['class_id'],set()))
                if scopes!=[key]:
                    held[key]={'reason':'Reserved occurrence is absent, changed or ambiguous in fresh canonical inventory','observed-scope-keys':scopes}
                    continue
                raw_path=recovery/('class-'+digest(key)+'.json')
                if raw_path.exists():
                    raw=json.loads(raw_path.read_text(encoding='utf-8'))
                    if raw.get('key')!=key or digest(packed(raw['value']))!=raw.get('sha256'):raise ValueError('Saved missing-class response changed')
                else:
                    value=self.source.fetch('/classes/'+str(state['class_id']),{'show_id':context['show-id'],'customer_id':context['customer-id']})
                    raw={'key':key,'value':value,'sha256':digest(packed(value))};atomic(raw_path,raw)
                payload,metric=raw['value']
                try:validate_class_response(payload,state,self.show)
                except ValueError as error:
                    held[key]={'reason':str(error),'saved-response':raw_path.name,'sha256':raw['sha256']};raw_observations.append(raw)
                else:additional[key]=raw['value']
            body={'owner':marker,'dates':days,'inventory':inventory,'additional':additional,'held':held,'unverified-responses':raw_observations}
            envelope={'body':body,'sha256':digest(packed(body))};atomic(path,envelope)
        marker={**marker,'prepared-sha256':envelope['sha256'],'held-keys':sorted(body['held']),'evidence-accepted':True}
        if not prior.get('evidence-accepted'):
            metrics=[m for p,m in body['inventory']]+[r['value'][1] for r in body['unverified-responses']]
            evidence={'reservation':reservation,'reservation-sha256':saved['reservation-sha256'],'responses-sha256':saved['responses-sha256'],
                'saved-response-hashes':{k:digest(packed(v)) for k,v in saved['accepted'].items()},
                'reconciliation':body,'coverage':'Retained context evidence; held responses are not accepted class facts or empty results'}
            self.observation('checkpoint-context-reconciliation',evidence,{'started':min(m['started'] for m in metrics),'completed':stamp(),'requests':metrics},{},
                {'saved-checkpoint-recovery':{**marker,'phase':'Prepared'},
                 'withheld-class-contexts':{**self.state.get('withheld-class-contexts',{}),**body['held']}})
        accepted={**saved['accepted'],**body['additional']}
        selected=[s for s in reservation['states'] if s['class_key'] in accepted]
        if selected:
            self.class_batch(selected,self.repo.scoped(self.show),self.repo.graph['lanes'],defer=True,
                recovery={'reservation':reservation,'accepted':accepted,'marker':marker})
        else:
            self.state.update({'slots':int(reservation['slot'].rsplit('-',1)[-1]),'reserved-class':None,
                'saved-checkpoint-recovery':{**marker,'phase':'Complete'}});self.journal()

    def class_batch(self,selected,base,policies,defer=False,recovery=None):
        observations=[];batch=[];batch_begin=time.monotonic()
        sequence=self.state.get('slots',0)+1;slot=self.runkey+'|slot-'+str(sequence)
        if recovery:slot=recovery['reservation']['slot'];sequence=int(slot.rsplit('-',1)[-1])
        folder=self.runtime/self.runkey.replace(':','_')/('class-batch-'+str(sequence))
        if recovery:folder=folder/'recovery'/'projection'
        saved=folder/'source-batch.json'
        def get(state):
            p,m=self.source.fetch('/classes/'+str(state['class_id']),{'show_id':int(self.show.split('|')[1]),'customer_id':15})
            validate_class_response(p,state,self.show)
            return p,m
        reservation={'class-keys':[s['class_key'] for s in selected],'slot':slot,'states':selected}
        folder.mkdir(parents=True,exist_ok=True)
        if (folder/'reservation.json').exists() and json.loads((folder/'reservation.json').read_text(encoding='utf-8'))!=reservation:raise ValueError('Reserved class batch scope changed')
        atomic(folder/'reservation.json',reservation)
        if not recovery:self.state['reserved-class']={'slot':sequence,'class-keys':reservation['class-keys']};self.journal()
        if recovery:
            batch=[recovery['accepted'][s['class_key']] for s in selected]
            if saved.exists() and json.loads(saved.read_text(encoding='utf-8'))!=batch:raise ValueError('Saved recovery source changed')
            if not saved.exists():atomic(saved,batch)
        elif saved.exists():batch=json.loads(saved.read_text(encoding='utf-8'))
        else:
            batch=capture_source_batch(folder/'responses.json',{'show-key':self.show,'keys':reservation['class-keys']},selected,lambda s:s['class_key'],get)
            atomic(saved,batch)
        (folder/'adaptive').mkdir(exist_ok=True)
        for index,(old,(p,m)) in enumerate(zip(selected,batch)):
            state,reason=planner.decide(old,p,planner.epoch(m['completed']),policies)
            observed=planner.epoch(m['completed'])
            today=datetime.datetime.fromtimestamp(observed,datetime.timezone.utc).astimezone(__import__('zoneinfo').ZoneInfo('America/New_York')).date().isoformat()
            interval=day_interval(state['day'],observed)
            if interval is not None:
                state['interval']=interval
                state['next_due']=observed+interval
                reason+='; canonical day refresh after '+str(interval)+' seconds from source observation'
            elif state['interval']<policies[state['lane']]['minimum-seconds']:
                state['interval']=policies[state['lane']]['minimum-seconds']
                state['next_due']=observed+state['interval']
            filename=str(index)+'.json';(folder/'adaptive'/filename).write_text(packed(p),encoding='utf-8')
            observations.append({'class_key':old['class_key'],'source_file':filename,'metric':m,'new':state,'old':old,'reason':reason,'error':None,'slot':slot})
        trial={'status':'PASS','errors':[],'show_key':self.show,'run_key':self.runkey,'started':self.at,'completed':stamp(),
            'request_count':len(batch),'observations':observations,'repeated_classes':0,'replans':0,'elapsed_seconds':0,'policies':policies,
            'slots':[{'key':slot,'class_keys':[s['class_key'] for s in selected],'started':min(m['started'] for p,m in batch),'completed':stamp(),
                'elapsed_seconds':planner.epoch(max(m['completed'] for p,m in batch))-planner.epoch(min(m['started'] for p,m in batch)),'requests':[m for p,m in batch]}]}
        (folder/'graph.json').write_text(packed(base),encoding='utf-8');(folder/'adaptive/trial.json').write_text(packed(trial),encoding='utf-8')
        if not (folder/'latest-graph.json').exists():subprocess.run([sys.executable,'-B',str(SOURCE/'build-wef-data-delta.py'),'--evidence-root',str(folder)],check=True,capture_output=True)
        graph=json.loads((folder/'delta-graph.json').read_text(encoding='utf-8'))
        graph.pop('runs',None);graph.pop('collections',None)
        graph.pop('requests',None)
        for row in graph.get('slots',[]):
            row['sequence']=sequence;row['summary']='Scheduled due-driven source batch; at most four concurrent requests; persistence time reported separately.'
            if recovery:
                row['work-items']=refs('work-items',*recovery['reservation']['class-keys'])
                row['status']='Partial saved recovery; contexts withheld' if recovery['marker']['held-keys'] else 'Completed saved source recovery'
                row['summary']=f"Saved responses preserved; {len(selected)} validated of {len(recovery['reservation']['states'])} reserved; unverified contexts are not empty results."
        next_state={'slots':sequence,'reserved-class':None}
        metric={'started':min(m['started'] for p,m in batch),'completed':stamp(),'requests':[m for p,m in batch]}
        if defer:
            if recovery:
                marker={**recovery['marker'],'phase':'Complete'}
                self.queue_work('checkpoint-recovery','class',{'classes':[p for p,m in batch]},metric,graph,
                    {'saved-checkpoint-recovery':marker},queue_state={**next_state,'saved-checkpoint-recovery':{**marker,'phase':'Projecting'}})
            else:self.queue_work(selected[0]['day'],'class',{'classes':[p for p,m in batch]},metric,graph,{},queue_state=next_state)
        else:self.observation('class',{'classes':[p for p,m in batch]},metric,graph,next_state)
        duration=time.monotonic()-batch_begin
        timing=self.state.setdefault('timing',{})
        if defer:timing['last-class-capture-and-queue-seconds']=duration
        else:
            timing['last-class-batch-seconds']=duration;timing['last-class-batch-size']=len(selected)
            timing['class-batch-seconds-per-class']=duration/len(selected)
        self.journal()

    def inventory_day(self,context,day):
        """One canonical date; the saved response survives every persistence wake."""
        folder=self.runtime/self.runkey.replace(':','_')/('inventory-'+day+'-'+str(len(self.state['observations'])))
        pointer={'day':day,'path':folder.relative_to(self.runtime).as_posix()}
        reservations=self.state.get('inventory-day-reservations',{});prior=reservations.get(day)
        if prior:
            if prior['day']!=day:raise ValueError('Reserved inventory day must resume first')
            folder=self.runtime/prior['path']
        else:self.state['inventory-day-reservations']={**reservations,day:pointer};self.journal()
        def fetch(value):
            payload,metric=self.source.fetch('/schedule',{'date':value,'show_id':context['show-id'],'customer_id':context['customer-id']})
            validate_inventory([payload],context,[value],self.repo.graph);return payload,metric
        payload,metric=capture_source_batch(folder/'responses.json',{'show-key':self.show,'keys':[day]},[day],str,fetch)[0]
        graph=module('build-wef-data-graph').inventory_graph([payload],context,self.repo.graph,metric['completed'])
        facility=payload['show'].get('facility_id');old=self.repo.graph['shows'].get(self.show,{})
        if facility is None or old.get('facility-id') not in (None,facility):raise ValueError('Inventory facility context conflict')
        graph['shows'][0]['facility-id']=facility
        active=sorted(r['class-key'] for r in graph.get('classes',[]))
        existing=active_class_keys(self.state) if 'active-classes' in self.state else []
        other={k.rsplit('|',1)[-1] for k in existing if k.split('|')[2]!=day and k.split('|')[2] in self.state.get('canonical-days',[]) and k not in self.state.get('withheld-class-contexts',{})}
        if any(k.rsplit('|',1)[-1] in other for k in active):raise ValueError('Ambiguous dated/ring class ID; detail association withheld')
        for table,parent in [('days','day'),('rings','ring')]:
            for row in graph.get(table,[]):
                classes=[c['class-key'] for c in graph.get('classes',[]) if [table,row[KEYS[table]]] in c[parent]['$links']]
                row.update({'expected-classes':len(classes),'collected-classes':sum(bool(self.repo.graph['classes'].get(k,{}).get('last-success')) for k in classes),
                            'status':'Verified inventory; class freshness tracked individually'})
        passes=copy.deepcopy(self.state.get('day-passes',{}).get(day,{}));passes['inventory']=True
        self.queue_work(day,'dated-inventory-page',payload,metric,graph,{'active-day':active,'day-passes':{day:passes}},
            queue_state={'inventory-day-reservations':{d:r for d,r in reservations.items() if d!=day}})

    def dispatch_days(self,context,deadline):
        """Select days, then ring/group/class; yield after each verified API block."""
        first=True
        while first or time.monotonic()<deadline:
            first=False;now=time.time();passes=self.state.setdefault('day-passes',{})
            jobs=self.state.setdefault('day-work',{});held=self.state.setdefault('withheld-day-work',{})
            reasons=self.state.setdefault('withheld-day-reasons',{});changed=False
            for day in list(jobs):
                if day!='directory' and day not in self.state['canonical-days']:
                    held[day]=jobs.pop(day);reasons[day]='Saved source work is outside current canonical day scope; retained, not accepted';changed=True
            for day in list(held):
                if day in self.state['canonical-days']:
                    jobs[day]=held.pop(day);reasons.pop(day,None);changed=True
            if changed:self.journal()
            due=due_days(self.state['canonical-days'],passes,now)
            from zoneinfo import ZoneInfo
            today=datetime.datetime.fromtimestamp(now,datetime.timezone.utc).astimezone(ZoneInfo('America/New_York')).date().isoformat()
            candidates=sorted(set(due)|{d for d in jobs if d!='directory' and d in self.state['canonical-days']},
                              key=lambda d:(0 if d==today else 1 if d>today else 2,d))
            if candidates:
                day=candidates[0]
                if day in jobs:self.step_work(day);continue
                progress=passes.get(day,{})
                if not progress.get('inventory') or progress.get('completed'):
                    progress={'started':now,'inventory':False};passes[day]=progress;self.journal()
                if not progress['inventory']:self.inventory_day(context,day);continue
                active=set(active_class_keys(self.state));states=[]
                for key,row in self.repo.graph['work-items'].items():
                    if key not in active or key.split('|')[2]!=day:continue
                    if key in self.state.get('withheld-class-contexts',{}):continue
                    value=json.loads(row['state-json'])
                    if not value.get('last_success') or planner.epoch(value['last_success'])<progress['started']:states.append(value)
                if states:
                    # Capture this admitted canonical-day cohort with the existing
                    # four-reader/saved-response path, then persist its shared graph
                    # through the same bounded transport blocks. Existing queued
                    # plans above finish under their original keys before admission.
                    states.sort(key=lambda s:(s['ring'],tuple(k for t,k in self.repo.graph['classes'][s['class_key']].get('group',refs('groups'))['$links']),s['class_key']))
                    self.class_batch(states,self.repo.scoped(self.show),self.repo.graph['lanes'],defer=True);continue
                progress['completed']=progress['started'];self.state['publication-day']=day;self.journal()
                return True
            if 'directory' in jobs:self.step_work('directory');continue
            if self.state.get('directory',{}).get('complete') and now>=self.state.get('directory-due',0):self.state.pop('directory',None)
            if self.collect_directory(context,request_limit=4,seconds=max(1,deadline-time.monotonic()),defer=True):continue
            return not self.state.get('day-work')
        return False

    def inventory(self,context):
        start=datetime.date.fromisoformat(context['start-date'][:10]);end=datetime.date.fromisoformat(context['end-date'][:10])
        dates=[(start+datetime.timedelta(days=i)).isoformat() for i in range((end-start).days+1)]
        completed=self.state.get('inventory-progress') or {'dates':dates,'next':0,'units':[]}
        if completed['dates']!=dates:raise ValueError('Inventory date range changed during pass')
        payloads=[];metrics=[]
        for first in completed['units']:
            from retained_feed import collection_rows
            rows=collection_rows(self.repo,self.state);part=json.loads(rows[first]['retained-json'])
            number=int(first.rsplit('-',1)[1]);prefix=first.rsplit('-',1)[0]
            unit=thaw([rows[prefix+f'-{i:03}'] for i in range(number,number+part['parts'])])
            payloads.append(unit['source']);metrics.append(unit['metric'])
        remaining=dates[completed['next']:]
        capture_start=int(completed['units'][0].rsplit('-',1)[1]) if completed['units'] else len(self.state['observations'])+1
        def get(day):
            payload,metric=self.source.fetch('/schedule',{'date':day,'show_id':context['show-id'],'customer_id':15})
            validate_inventory([payload],context,[day],self.repo.graph)
            return payload,metric
        for offset in range(0,len(remaining),4):
            batch=capture_source_batch(self.runtime/self.runkey.replace(':','_')/('inventory-responses-'+str(capture_start)+'.json'),
                {'show-key':self.show,'keys':dates},remaining[offset:offset+4],str,get)
            for payload,metric in batch:
                day=dates[completed['next']]
                validate_inventory([payload],context,[day],self.repo.graph)
                first=self.runkey+f"|observation-{len(self.state['observations'])+1:03}"
                completed={'dates':dates,'next':completed['next']+1,'units':completed['units']+[first]}
                self.observation('dated-inventory-page',payload,metric,{}, {'inventory-progress':completed})
                payloads.append(payload);metrics.append(metric)
        validate_inventory(payloads,context,dates,self.repo.graph)
        graph=module('build-wef-data-graph').inventory_graph(payloads,context,self.repo.graph,stamp())
        facilities={p['show'].get('facility_id') for p in payloads}
        if len(facilities)!=1 or None in facilities:raise ValueError('Inventory facility context missing/inconsistent')
        old=self.repo.graph['shows'].get(self.show,{})
        if old.get('facility-id') is not None and old['facility-id'] not in facilities:raise ValueError('Facility changed')
        graph['shows'][0]['facility-id']=next(iter(facilities))
        graph['shows'][0]['status']='Verified dated inventory; acquisition in progress'
        active=sorted(r['class-key'] for r in graph.get('classes',[]))
        for table,parent in [('days','day'),('rings','ring')]:
            for row in graph.get(table,[]):
                key=row[KEYS[table]];classes=[k for k in active if [table,key] in next(c for c in graph['classes'] if c['class-key']==k)[parent]['$links']]
                row['expected-classes']=len(classes)
                row['collected-classes']=sum(bool(self.repo.graph['classes'].get(k,{}).get('last-success')) for k in classes)
                row['status']='Verified inventory; class freshness tracked individually'
        state={'active-classes':active,'inventory-at':time.time(),'inventory-due':time.time()+self.repo.graph['lanes']['inventory']['interval-seconds'],'inventory-progress':None}
        self.observation('dated-inventory',{'inventory-parts':completed['units'],'dates':dates},{'started':min(m['started'] for m in metrics),'completed':stamp(),'requests':[]},graph,state)

    def collect_directory(self,context,request_limit=12,seconds=90,defer=False):
        """Capture first, then stage once. One bounded attempt per scheduled run."""
        if request_limit<=0 or seconds<=0:return False
        if self.state.get('directory-dispatch-run')==self.runkey or time.time()<self.state.get('directory-next-due',0):return False
        if self.state.get('directory',{}).get('complete'):return False
        begin=time.monotonic();pointer=self.state.get('directory-capture')
        path=(self.runtime/pointer).resolve() if pointer else (self.runtime/self.runkey.replace(':','_')/'directory-capture.json').resolve()
        if not path.is_relative_to(self.runtime.resolve()):raise ValueError('Directory capture path escapes runtime')
        pointer=path.relative_to(self.runtime.resolve()).as_posix()
        capture=json.loads(path.read_text(encoding='utf-8')) if path.exists() else {'show-key':self.show,'pages':[],'events':[],'progress':None,'verified':False}
        if capture['show-key']!=self.show:raise ValueError('Directory capture show conflict')
        for event in capture['events']:
            if event.get('payload-sha256')!=digest(packed(event['payload'])):raise ValueError('Captured directory payload changed')
        for page in capture['pages']:
            if page.get('payload-sha256')!=digest(packed(page['payload'])):raise ValueError('Captured directory page changed')
        published=self.state.get('directory-published',{}).get(pointer,0)
        self.state['directory-reservation']={'capture':pointer,'start':len(self.state['observations'])+1,'request-start':self.state.get('requests',0)+1}
        self.journal();calls=0;drift=None
        def fetch(page):
            return self.source.fetch('/entries',{'customer_id':15,'show_id':context['show-id'],'page':page,'sort_on':'number','sort_type':'asc','search_text':''})
        def accept_saved():
            while True:
                progress=capture.get('progress')
                if progress and progress['complete']:return
                number=progress['next'] if progress else 1
                event=next((e for e in capture['events'] if e.get('purpose')=='page' and e.get('requested')==number and not e.get('accepted')),None)
                if not event:return
                progress=directory_page(event['payload'],context['show-id'],number,capture.get('progress'))
                capture['pages'].append({'payload':event['payload'],'metric':event['metric'],'payload-sha256':event['payload-sha256']})
                capture['progress']=progress;event['accepted']=True;atomic(path,capture)
        try:accept_saved()
        except DirectoryDrift as error:drift=error
        while not drift and not capture.get('verified') and calls<request_limit and time.monotonic()-begin<seconds:
            progress=capture.get('progress')
            if progress and progress['complete']:
                saved_check=next((e for e in capture['events'] if e.get('purpose')=='final-first-page-recheck'),None)
                if saved_check:payload,metric=saved_check['payload'],saved_check['metric']
                else:
                    payload,metric=fetch(1);calls+=1;capture['events'].append({'payload':payload,'metric':metric,'purpose':'final-first-page-recheck','payload-sha256':digest(packed(payload))})
                atomic(path,capture)
                fresh=directory_page(payload,context['show-id'],1,None)
                expected={'total':progress['total'],'size':progress['size'],'page':1}
                received={'total':fresh['total'],'size':fresh['size'],'page':1}
                if (progress['total'],progress['size'])!=(fresh['total'],fresh['size']) or set(fresh['ids'])!={r['entry_id'] for r in capture['pages'][0]['payload']['entries']}:
                    drift=DirectoryDrift('Directory changed during final recheck',expected,received);break
                capture['verified']=True;atomic(path,capture);break
            page=progress['next'] if progress else 1
            pages=__import__('math').ceil(progress['total']/progress['size']) if progress else 1
            limit=min(4,request_limit-calls,max(1,pages-page+1))
            saved_numbers={e['requested'] for e in capture['events'] if e.get('purpose')=='page'}
            selected=[n for n in range(page,max(page,pages)+1) if n not in saved_numbers][:limit]
            if not selected:raise ValueError('Directory capture cursor cannot advance')
            # All admitted responses are retained, including a listing that drifts.
            def attempt(number):
                try:return number,fetch(number),None
                except Exception as error:return number,None,error
            with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:batch=list(pool.map(attempt,selected))
            calls+=len(batch)
            for number,result,error in batch:
                if error:
                    capture.setdefault('errors',[]).append({'page':number,'error':type(error).__name__,'diagnostic':getattr(error,'diagnostic',None)})
                else:
                    payload,metric=result;capture['events'].append({'payload':payload,'metric':metric,'purpose':'page','requested':number,'payload-sha256':digest(packed(payload))})
            atomic(path,capture)
            failure=next((error for number,result,error in batch if error),None)
            if failure:raise failure
            try:accept_saved()
            except DirectoryDrift as error:drift=error
            if drift:break
        graph={};working={t:copy.deepcopy(self.repo.graph[t]) for t in ('entries','horses','riders','trainers','owners')}
        # Apply only individually validated pages. Preserve later facts on replay.
        valid_pages=list(capture['pages'])
        if capture.get('verified'):
            valid_pages.extend(e for e in capture['events'] if e.get('purpose')=='final-first-page-recheck')
        # Detect conflicts across the entire capture before planning any writes.
        # A late recheck cannot refresh a previously seen conflicting entry.
        conflicts={};preflight=copy.deepcopy(working)
        for item in valid_pages:
            for table,rows in directory_graph(item['payload'],self.show,item['metric']['completed'],preflight,conflicts).items():
                for row in rows:preflight[table][row[KEYS[table]]]=row
        for key,conflict in conflicts.items():
            conflict['prior-record-id']=self.repo.inventory['entries'].get(key,{}).get('id')
            conflict['prior-retained']=key in self.repo.graph['entries']
            if conflict['prior-retained']:conflict['prior']=copy.deepcopy(self.repo.graph['entries'][key])
        unpublished={e['payload-sha256'] for e in capture['events'][published:]}
        for item in valid_pages:
            if item['payload-sha256'] not in unpublished:continue
            payload={**item['payload'],'entries':[e for e in item['payload']['entries'] if str(context['show-id'])+'|'+str(e['entry_id']) not in conflicts]}
            for table,rows in directory_graph(payload,self.show,item['metric']['completed'],working).items():
                for row in rows:
                    key=row[KEYS[table]];working[table][key]=copy.deepcopy(row);graph.setdefault(table,{})[key]=row
        graph={t:list(rows.values()) for t,rows in graph.items()}
        progress=copy.deepcopy(capture.get('progress') or {'total':0,'size':1,'next':1,'ids':[],'complete':False})
        acquired=bool(capture.get('verified')) and drift is None
        progress['acquisition-complete']=acquired
        progress['complete']=acquired and not conflicts
        policy=self.repo.graph['lanes']['recovery'];previous=self.state.get('directory-reconcile',{})
        failures=0 if progress['complete'] else previous.get('attempts',0)+(1 if drift else 0)
        retry=min(policy.get('maximum-seconds',300),max(policy.get('minimum-seconds',30),policy['interval-seconds']*(2**min(max(0,failures-1),3))))
        reconciliation={'attempts':failures,'status':'Verified coherent pass' if progress['complete'] else 'Pending coherent directory pass'}
        if conflicts:reconciliation.update({'status':'Partial identity reconciliation','held-entries':sorted(conflicts),'evidence':pointer})
        if drift:reconciliation.update({'last-drift':drift.evidence(),'evidence':pointer})
        new_pointer=None if drift or acquired else pointer
        progress['complete']=bool(progress['complete'])
        timing={**self.state.get('timing',{}),'directory-acquisition-seconds':time.monotonic()-begin,'directory-source-requests':calls}
        next_state={'directory':progress,'directory-capture':new_pointer,'directory-published':{pointer:len(capture['events'])} if new_pointer else {},
            'directory-reservation':None,'directory-dispatch-run':self.runkey,'directory-next-due':time.time()+self.repo.graph['lanes']['reconcile']['interval-seconds'] if acquired and conflicts else time.time()+retry if not progress['complete'] else 0,
            'directory-conflicts':sorted(conflicts),
            'directory-due':time.time()+self.repo.graph['lanes']['reconcile']['interval-seconds'],'directory-reconcile':reconciliation,'timing':timing}
        metrics=[e['metric'] for e in capture['events'][published:]]
        frozen={'capture':capture,'drift':drift.evidence() if drift else None,'identity-conflicts':conflicts,
            'coverage':'Verified show-wide entry-directory pass; no day attendance inference' if progress['complete'] else 'Partial directory observations; completeness withheld'}
        scope='entry-directory-reconciled' if progress['complete'] else 'entry-directory-partial'
        metric={'started':min((m['started'] for m in metrics),default=self.at),'completed':stamp(),'requests':metrics}
        if defer:self.queue_work('directory',scope,frozen,metric,graph,next_state)
        else:self.observation(scope,frozen,metric,graph,next_state)
        timing_key='directory-queue-seconds' if defer else 'directory-persistence-seconds'
        self.state.setdefault('timing',{})[timing_key]=time.monotonic()-begin-timing['directory-acquisition-seconds'];self.journal()
        return True

    def archive_outgoing(self,key,authority):
        from full_archive import operational,coverage,retained_checkpoints,include_reserved
        from archive_evidence import outgoing_snapshot
        saved=outgoing_snapshot(self.runtime,key)
        if saved:return self.publish_outgoing(key,*saved[:2])
        config=json.loads((pathlib.Path(__file__).parent/'logging-schema.json').read_text(encoding='utf-8'))
        current=next(r['show-key'] for r in authority['records'] if r['default'])
        ops,accounting,global_ops=operational(self.repo,key,config)
        # Take and verify a fresh retained projection; no recollection or deletion.
        checkpoints=retained_checkpoints(self.runtime,key)
        for runkey,row in self.repo.graph['runs'].items():
            state=json.loads(row.get('state-json') or '{}')
            if state.get('show-key')!=key:continue
            for day,(saved,plan) in saved_work_plans(self.runtime,state,runkey,self.repo).items():
                # The existing reserved-row classifier includes actual sparse
                # identities only; these plans never assert accepted source facts.
                pending={**plan,'start':len(state.get('observations',[]))+1,'request-start':state.get('requests',0)+1}
                checkpoints.append({'active':{'run-key':runkey},'pending':pending,'classification':'Unaccepted saved day work'})
        retained,parts=self.evidence.retained(self.repo,key) if self.evidence else ({},{})
        graph=include_reserved(self.repo,self.repo.scoped(key),checkpoints)
        signature=coverage(self.repo,key,graph,ops,current)
        if retained:signature=digest(packed({'coverage':signature,'retained-journals':retained,'parts':{n:__import__('hashlib').sha256(b).hexdigest() for n,b in parts.items()}}))
        checkpoint=self.runtime/'outgoing-receipts'/(key.replace('|','-')+'.json')
        if checkpoint.exists():
            old=json.loads(checkpoint.read_text(encoding='utf-8'))
            if old.get('coverage-sha256')==signature:
                archiver.verify(self.data_repo,pathlib.Path(old['path']),{name:(self.data_repo/old['path']/name).read_bytes() for name in old['verified-files']},old['verified_remote_commit'])
                return old
        mapping={t:{r[KEYS[t]]:self.repo.inventory[t][r[KEYS[t]]]['id'] for r in rows} for t,rows in graph.items()}
        check=Repository(self.client,self.repo.schema,self.repo.authored);actual=include_reserved(check,check.scoped(key),checkpoints)
        if packed(actual)!=packed(graph):raise ValueError('Outgoing retained projection changed during readback')
        directory=latest_state(list(self.repo.graph['runs'].values()),key).get('directory',{})
        folder=self.runtime/('outgoing-'+key.replace('|','-'));folder.mkdir(exist_ok=True)
        proof={'status':'PASS','base':DATA_BASE,'checkedAt':stamp(),'records':sum(map(len,graph.values())),
            'tables':{t:{'status':'PASS','errors':[],'expected':len(rows),'read':len(rows)} for t,rows in graph.items()}}
        evidence={'latest-graph.json':graph,'record-map-final.json':mapping,'final-readback.json':proof,'operational-tables.json':ops,'operational-reference-context.json':global_ops,'retained-checkpoints.json':checkpoints,
            'operational-schema.json':{'configuration':config['workflows']['wef-data'],'fields':json.loads((pathlib.Path(__file__).parent/'logging-fields.json').read_text(encoding='utf-8')),'meta':{'table':'tblwBvigcmqRlSYUb','fields':{'Name':'singleLineText'}}},
            'schema-final.json':self.repo.schema,'archive-context.json':{'scope':'Verified retained outgoing show projection; all existing records retained',
                'coverage':'Verified retained projection at cutoff, not fresh source completeness; interrupted observations remain incomplete','directory-checkpoint':{n:directory.get(n) for n in ('complete','total','next','size')},'accounting':accounting}}
        if key=='15|200000075':evidence['historical-source-reference.json']={'commit':'ddcde068ca3323242ec1fe81d96f20aee82fa6a3','path':'wef-data/shows/15/200000075/4c88a40a2a03efa5d1e48c6be92f528139061f7bbb4d16bee26d3731df323d16','scope':'Original October24 raw source/evidence, preserved unchanged'}
        if retained:evidence['retained-evidence-journals.json']=retained
        for name,value in evidence.items():(folder/name).write_text(packed(value),encoding='utf-8')
        for name,body in parts.items():
            target=folder/name;target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(body)
        (folder/'evidence-manifest.json').write_text(packed(list(evidence)+list(parts)),encoding='utf-8')
        prefix,files=archiver.build(folder)
        pin={'show-key':key,'folder':folder.name,'path':prefix.as_posix(),'coverage-sha256':signature,'files':{name:{'bytes':len(body),'sha256':__import__('hashlib').sha256(body).hexdigest()} for name,body in files.items()}}
        atomic(self.runtime/'outgoing-publications'/(key.replace('|','-')+'.json'),pin)
        return self.publish_outgoing(key,pin,folder)

    def publish_outgoing(self,key,pin,folder):
        from archive_evidence import pending_outgoing
        pending=pending_outgoing(self.runtime);pending.pop(pin['path'],None)
        receipt=archiver.archive(folder,self.data_repo,publish=True,**({'pending_bundles':pending} if pending else {}))
        if receipt['path']!=pin['path']:raise ValueError('Outgoing publication identity changed')
        # The publication receipt is local immutable control evidence, not a new
        # old-show Airtable row that would escape its own completed snapshot.
        manifest=json.loads((self.data_repo/receipt['path']/'manifest.json').read_text(encoding='utf-8'))
        receipt.update({'show-key':key,'coverage-sha256':pin['coverage-sha256'],'verified-files':list(manifest['files'])+['manifest.json'],'counts':manifest['counts'],'operational-counts':manifest['operational_counts'],'closed-at':stamp()})
        atomic(self.runtime/'outgoing-receipts'/(key.replace('|','-')+'.json'),receipt)
        if self.repo.graph['shows'][key].get('status')!='Archived retained snapshot':self.repo.patch('shows',key,{'status':'Archived retained snapshot'})
        (self.runtime/'outgoing-publications'/(key.replace('|','-')+'.json')).unlink()
        return receipt

    def retain_outgoing(self,key,authority):
        from retirement import RetentionWait
        from archive_evidence import ArchiveDeferred
        try:return self.archive_outgoing(key,authority)
        except ArchiveDeferred as error:
            wait_error=RetentionWait('Outgoing archive publication deferred: '+str(error))
        except (subprocess.CalledProcessError,subprocess.TimeoutExpired) as error:
            # Only unavailable remote publication is a readiness wait. Invalid
            # archives, wrong targets, failed local readbacks and IDs stay fatal.
            if not any(v in getattr(error,'cmd',[]) for v in ('fetch','push','ls-remote')):raise
            wait_error=RetentionWait('Outgoing archive remote publication unavailable')
        except RetentionWait as wait:wait_error=wait
        self.retention_waits.append({'show-key':key,'reason':str(wait_error),'phase':'archive-readiness','at':stamp()})
        atomic(self.runtime/'retention-waits.json',self.retention_waits)

    def tick(self):
        self.retention_waits=[]
        start=time.monotonic();authority=self.client.canonical_context();context=next(r for r in authority['records'] if r['default'])
        canonical_days=read_canonical_days(self.client,context)
        active=self.runtime/'active.json'
        if active.exists():
            reserved=json.loads(active.read_text(encoding='utf-8'));row=self.repo.graph['runs'].get(reserved['run-key'],reserved['reserved-run']);old=json.loads(row.get('state-json') or reserved['reserved-run']['state-json'])
            if old.get('authority-before') and old['authority-before']!=list(canonical_fence(authority)):
                if row.get('status')=='Failed; prior accepted state retained':
                    approval=self.runtime/'resume-approved.json'
                    permitted=json.loads(approval.read_text(encoding='utf-8')) if approval.exists() else {}
                    if permitted.get('run-key')!=reserved['run-key'] or not permitted.get('approval'):raise ValueError('Failed canonical transition requires exact approval')
                if row.get('status')!='Verified completed revision':self.repo.patch('runs',reserved['run-key'],{'status':'Retained incomplete; canonical context changed'})
                if old['show-key']!=context['show-key']:self.retain_outgoing(old['show-key'],authority)
                folder=self.runtime/'retired-checkpoints'/reserved['run-key'].replace(':','_');folder.mkdir(parents=True,exist_ok=True)
                for name in ['active.json','pending.json']:
                    source=self.runtime/name
                    if source.exists():
                        target=folder/name;body=source.read_bytes()
                        if target.exists() and target.read_bytes()!=body:raise ValueError('Retired checkpoint collision')
                        target.write_bytes(body);source.unlink()
        recovered=self.recover(authority)
        if recovered:
            mode=self.repo.graph['runs'][self.runkey]['mode']
            if mode in ('Historical archive closure','Outgoing archive closure'):
                if not self.state['observations']:raise ValueError('Interrupted archive closure requires retained receipt reconciliation')
                self.finish(authority);self.repo.patch('shows',self.show,{'status':'Archived retained snapshot'});recovered=False
        if not recovered:
            for key,row in list(self.repo.graph['shows'].items()):
                if key!=context['show-key']:self.retain_outgoing(key,authority)
            self.begin(context['show-key'],latest_state(list(self.repo.graph['runs'].values()),context['show-key']))
            self.state['authority-before']=list(canonical_fence(authority));self.journal()
        if self.show!=context['show-key']:raise ValueError('Active acquisition show differs from canonical show')
        # A partially published summary resumes its exact pinned day set. New
        # work uses the current canonical native scope read above.
        context={**context,'canonical-days':self.state.get('canonical-days',canonical_days)}
        if recovered and self.state.get('completion-at'):return self.finish(authority)
        # Once summary publication starts, close that exact generation before new
        # source admission, even when a crash made inventory/class work due again.
        from retained_feed import collection_rows
        recovered_rows=collection_rows(self.repo,self.state)
        if recovered and any((recovered_rows[r['key']].get('scope') or '').startswith('day-count-summary|') for r in self.state['observations']):
            self.day_summaries(context)
            return self.finish(authority)
        # Recover the exact reserved class order before any new directory/inventory read.
        reserved_class=self.state.get('reserved-class') or {}
        reservation=self.runtime/self.runkey.replace(':','_')/('class-batch-'+str(reserved_class.get('slot',self.state.get('slots',0)+1)))/'reservation.json'
        if recovered and reservation.exists():
            saved=json.loads(reservation.read_text(encoding='utf-8'))
            if not any(d in self.state.get('day-work',{}) for d in (saved['states'][0]['day'],'checkpoint-recovery')):
                captured=read_saved_checkpoint(reservation.parent,self.show,self.runkey)
                if len(saved['states'])>1 or captured['missing']:self.recover_saved_checkpoint(context,reservation.parent)
                else:self.class_batch(saved['states'],self.repo.scoped(self.show),self.repo.graph['lanes'],defer=True)
        if self.show not in self.repo.graph['shows']:
            self.repo.stage({'runs':[{'run-key':self.runkey}],'shows':[{k:context[k] for k in ('show-key','show-id','customer-id','show-name','start-date','end-date','selected-date')}]})
        if not self.state.get('bootstrap-run'):
            retained=self.repo.scoped(self.show)
            mapping={t:{r[KEYS[t]]:self.repo.inventory[t][r[KEYS[t]]]['id'] for r in rows} for t,rows in retained.items()}
            self.observation('retained-bootstrap',{'retained-projection':retained,'record-map':mapping,
                'coverage':'Previously retained bounded records with original source times; not a new source observation'},
                {'started':self.at,'completed':stamp()}, {}, {'bootstrap-run':self.runkey})
        self.state['canonical-days']=canonical_days;context['canonical-days']=canonical_days;self.journal()
        while 'checkpoint-recovery' in self.state.get('day-work',{}) and time.monotonic()<start+180:self.step_work('checkpoint-recovery')
        marker=self.state.get('saved-checkpoint-recovery',{})
        recovery_ready=marker.get('phase')=='Complete' and marker.get('published-in') in (None,self.runkey)
        if recovery_ready:
            self.state['publication-kind']='checkpoint-recovery'
            self.state['saved-checkpoint-recovery']={**marker,'published-in':self.runkey};self.journal()
        pending_recovery='checkpoint-recovery' in self.state.get('day-work',{})
        if pending_recovery or not recovery_ready and not self.dispatch_days(context,start+180):
            self.journal()
            return {'ok':False,'state':'Deferred','run-key':self.runkey,'show-key':self.show,'source-requests':self.state.get('requests',0),
                    'operation':'Continue','target':'Saved canonical day work','error':'Verified work saved; exact remaining blocks continue at the existing scheduled wake','write-outcome':'Verified partial'}
        from retained_feed import dependencies
        retained=dependencies(self.repo,self.state)
        if retained:self.observation('retained-dependencies',{'coverage':'Previously retained linked facts with their original timestamps; not fresh acquisition'}, {'started':self.at,'completed':stamp()},retained)
        self.day_summaries(context)
        return self.finish(authority)

    def day_summaries(self,context):
        from day_counts import summaries,retained_units
        from wef_data_handoff import revision_chain
        from retained_feed import collection_rows,run_rows
        collections=collection_rows(self.repo,self.state)
        units=[]
        previous=self.state.get('predecessor')
        if previous:
            chain=revision_chain(previous['run-key'],run_rows(self.repo,self.show),collections)
            for state,_ in chain:units.extend(retained_units(state,collections))
        units.extend(retained_units(self.state,collections))
        existing={u['scope'] for u,refs in units if u['scope'].startswith('day-count-summary|') and any(r['key'].startswith(self.runkey+'|') for r in refs)}
        for envelope in summaries(context,units,self.runkey,previous,stamp()):
            body=envelope['body'];scope='day-count-summary|'+body['day-key']
            if scope in existing:continue
            self.observation(scope,envelope,{'started':body['verified-at'],'completed':body['verified-at']},{})

def run(runtime,schema_file,authored_file,data_repo,archive_storage=False):
    runtime=pathlib.Path(runtime);runtime.mkdir(parents=True,exist_ok=True)
    if (runtime/'stopped.json').exists():return {'ok':False,'state':'Stopped; owner authorization required','source-requests':0}
    from archive_evidence import EvidenceStore,ArchiveDeferred
    archive_deferred=runtime/'archive-deferred.json'
    archive_prior=json.loads(archive_deferred.read_text(encoding='utf-8')) if archive_deferred.exists() else {}
    if archive_prior and time.time()<archive_prior['next-due']:return {'ok':False,'state':'Deferred','source-requests':0,'error':'Saved archive publication awaiting bounded recovery time'}
    source_deferred=runtime/'source-deferred.json'
    source_prior=json.loads(source_deferred.read_text(encoding='utf-8')) if source_deferred.exists() else None
    if source_prior and time.time()<source_prior.get('next-due',0):return {'ok':False,'state':'Deferred','source-requests':0,'error':'Source checkpoint awaiting bounded recovery time'}
    deferred=runtime/'deferred.json'
    retirement_deferred=runtime/'retirement-deferred.json'
    retirement_prior=json.loads(retirement_deferred.read_text(encoding='utf-8')) if retirement_deferred.exists() else None
    prior=json.loads(deferred.read_text(encoding='utf-8')) if deferred.exists() else None
    if prior and time.time()<prior.get('next-due',0):return {'ok':False,'state':'Deferred','source-requests':0,'error':'Retained checkpoint awaiting its bounded recovery time'}
    runner=None;repo=None;retired=None;retiring=False;retention_waits=[]
    try:
        from retirement import retire,pending_approval,mutation_pending,RetentionWait
        retiring=pending_approval(runtime)
        repo=Repository(Airtable(),json.loads(pathlib.Path(schema_file).read_text(encoding='utf-8')),json.loads(pathlib.Path(authored_file).read_text(encoding='utf-8')))
        from history_cleanup import retire as cleanup_history,pending as history_mutation_pending
        try:
            history_cleanup=cleanup_history(repo,runtime,data_repo)
            if history_cleanup:retention_waits.append({'phase':'history-cleanup',**history_cleanup})
        except Exception as error:
            # Cleanup readiness must not stop acquisition. An uncertain DELETE
            # must settle through its durable exact-ID intent before new work.
            if history_mutation_pending(runtime):
                return {'ok':False,'state':'Deferred','source-requests':0,'operation':'Historical cleanup',
                    'error':type(error).__name__+': '+str(error)}
            retention_waits.append({'phase':'history-cleanup','status':'Waiting' if isinstance(error,RetentionWait) else 'Blocked',
                'reason':type(error).__name__+': '+str(error)})
        retiring=True
        if retirement_prior and retirement_prior.get('operation',{}).get('method')=='DELETE' and not mutation_pending(runtime):
            raise ValueError('Deferred retirement DELETE lacks its exact mutation checkpoint')
        if retirement_prior and time.time()<retirement_prior.get('next-due',0):
            if mutation_pending(runtime):return {'ok':False,'state':'Deferred','source-requests':0,'error':'Retirement mutation awaiting bounded recovery time'}
            retention_waits.append({'phase':'retirement-backoff','reason':'Pre-mutation retention retry not due','at':stamp()})
        else:
            try:
                retired=retire(repo,runtime)
                retirement_deferred.unlink(missing_ok=True)
            except RetentionWait as error:
                if mutation_pending(runtime):raise ValueError('Unresolved retirement mutation cannot be bypassed') from error
                failures=(retirement_prior or {}).get('failures',0)+1
                wait={'at':stamp(),'reason':str(error),'phase':'retirement-readiness','failures':failures,'next-due':time.time()+min(300,60*2**min(failures-1,3))}
                atomic(retirement_deferred,wait);retention_waits.append(wait)
        retiring=False
        if prior and prior['operation']['method']=='POST' and prior['operation'].get('write-outcome')!='NotSent':
            operation=prior['operation'];table=next((t for t,v in repo.schema['tables'].items() if v['id']==operation['table']),None)
            if table is None:raise ValueError('Deferred create outside Data schema')
            primary=repo.store.fields[table][KEYS[table]]['id']
            wanted=[str(r['fields'][primary]) for r in operation['body']['records']]
            actual=repo.store.inventory(table,refresh=True,keys=wanted)
            if any(k not in actual for k in wanted):
                # A lost legacy POST cannot be blindly repeated. Match its exact
                # reserved keys server-side, after proving saved current ownership.
                active=json.loads((runtime/'active.json').read_text(encoding='utf-8'))
                state=json.loads(active['reserved-run']['state-json'])
                current=repo.client.canonical_context()
                selected=next(r for r in current['records'] if r['default'])
                if state['show-key']==selected['show-key']:
                    pending=runtime/'pending.json'
                    if pending.exists():
                        plan=json.loads(pending.read_text(encoding='utf-8'))
                        if plan['run-key']!=active['run-key'] or plan['show-key']!=state['show-key']:raise ValueError('Deferred reservation owner mismatch')
                        graph=pending_candidate(plan)
                    else:graph={'runs':[active['reserved-run']]}
                    allowed={str(r[KEYS[table]]) for r in graph.get(table,[])}
                    if operation['base']!=repo.schema['base'] or len(set(wanted))!=len(wanted) or not set(wanted)<=allowed:raise ValueError('Deferred reservation differs from saved owner')
                    repo.store.reserve(table,wanted)
                    repo.reload()
        runner=Runner(repo,Source(),runtime,data_repo,evidence=EvidenceStore(runtime/'retained-evidence',data_repo) if archive_storage else None)
        result=runner.tick();deferred.unlink(missing_ok=True);source_deferred.unlink(missing_ok=True);archive_deferred.unlink(missing_ok=True)
        result['retention-waits']=retention_waits+result.get('retention-waits',[])
        if retired:result['retirement']=retired
        if not result.get('ok'):return result
        try:
            from retained_feed import publish
            publish(repo,runner.state,runtime/'handoff',runner.state['authority'])
        except Exception as error:
            result['handoff-error']=type(error).__name__+': '+str(error)
        return result
    except ArchiveDeferred as error:
        failures=archive_prior.get('failures',0)+1
        if failures>3:
            report={'ok':False,'state':'Stopped','run-key':getattr(runner,'runkey',None),'at':stamp(),'error':'Archive recovery exhausted after three scheduled retries: '+str(error)}
            atomic(runtime/'stopped.json',report)
            if getattr(runner,'runkey',None):repo.patch('runs',runner.runkey,{'status':'Failed; prior accepted state retained','errors':1,'summary':packed(report)})
            return report
        atomic(archive_deferred,{'at':stamp(),'run-key':getattr(runner,'runkey',None),'failures':failures,'next-due':time.time()+min(300,60*2**(failures-1))})
        return {'ok':False,'state':'Deferred','run-key':getattr(runner,'runkey',None),'error':str(error),'operation':'Archive','target':'ringstatus-data','source-requests':getattr(runner,'state',{}).get('requests',0),'write-outcome':'Unknown'}
    except TransportDeferred as error:
        failures=((retirement_prior if retiring else prior) or {}).get('failures',0)+1
        operation=error.operation
        # A read outage must not erase an unverified Source POST reservation.
        if not retiring and prior and prior['operation']['method']=='POST' and prior['operation'].get('write-outcome')!='NotSent' and operation['method']=='GET':operation=prior['operation']
        atomic(retirement_deferred if retiring else deferred,{'at':stamp(),'operation':operation,'failures':failures,'next-due':time.time()+min(300,60*2**min(failures-1,3))})
        result={'ok':False,'state':'Deferred','error':str(error),'operation':error.operation['method'],'target':error.operation['table'],'source-requests':getattr(runner,'state',{}).get('requests',0),'write-outcome':error.operation['write-outcome'] if error.operation['method']!='GET' else 'NotApplicable'}
        if retired:result['retirement']=retired
        return result
    except CanonicalTransition as error:
        atomic(deferred,{'at':stamp(),'operation':{'method':'GET','base':'appCaSmL819KK7UFJ','table':'tblJA5CUon5s1sWib','body':None},'failures':0,'next-due':time.time()+60})
        return {'ok':False,'state':'Deferred','error':str(error),'source-requests':getattr(runner,'state',{}).get('requests',0)}
    except Exception as error:
        if transient_source(error) and runner is not None:
            recovery=defer_source(runtime,getattr(runner,'runkey',None),error,repo.graph['lanes'].get('recovery',{}))
            if recovery:return recovery
        report={'ok':False,'state':'Stopped','error':type(error).__name__+': '+str(error),'at':stamp()}
        if isinstance(error,SourceFailure):report['source']=error.diagnostic
        if getattr(runner,'runkey',None):
            try:repo.patch('runs',runner.runkey,{'status':'Failed; prior accepted state retained','errors':1,'summary':packed(report)})
            except Exception:pass
        (runtime/'stopped.json').write_text(packed(report),encoding='utf-8')
        raise
