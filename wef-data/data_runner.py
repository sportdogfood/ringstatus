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
COUNT_REFRESH_SECONDS=150

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

def count_due(states,policies,at,limit=4,slot=0):
    """Refresh today's original count inputs first, with a bounded other-day turn."""
    from zoneinfo import ZoneInfo
    today=datetime.datetime.fromtimestamp(at,datetime.timezone.utc).astimezone(ZoneInfo('America/New_York')).date().isoformat()
    limit=max(0,min(40,limit));due=[]
    for state in states:
        if state['day']!=today or state.get('inflight'):continue
        last=planner.epoch(state['last_success']) if state.get('last_success') else 0
        deadline=min(state.get('next_due',last+state['interval']),last+COUNT_REFRESH_SECONDS)
        if deadline<=at:due.append((last,state['class_key'],state))
    due.sort(key=lambda item:item[:2])
    chosen=[item[2] for item in due[:limit]]
    other=planner.due([s for s in states if s['day']!=today],policies,at,min(4,limit))
    if chosen and other and len(chosen)==limit and slot%4==3:
        chosen=chosen[:-1]
    return chosen+other[:limit-len(chosen)]
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
    def begin(self,show,state,mode='Scheduled Data acquisition'):
        self.at=stamp();self.runkey=('archive-' if mode=='Historical archive closure' else 'adaptive-')+self.at
        self.show=show;self.state=copy.deepcopy(state);self.state.update({'show-key':show,'sequence':state.get('sequence',0)+1,'phase':'Writing','pending':None,'observations':[],'requests':0})
        self.state['predecessor']=self.state.pop('_head',None);self.state['slots']=0
        self.state.pop('completion-at',None)
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
    def observation(self,scope,payload,metric,graph,next_state=None):
        plan={'run-key':self.runkey,'show-key':self.show,'scope':scope,'source':payload,'metric':metric,
            'graph':graph,'next-state':next_state or {},'start':len(self.state['observations'])+1,'request-start':self.state.get('requests',0)+1}
        if self.state.get('evidence-storage'):
            plan['evidence-storage']=self.state['evidence-storage'];plan['retained-budget']=1000000
        atomic(self.runtime/'pending.json',plan)
        return self.apply_pending(plan)
    def apply_pending(self,plan):
        candidate=pending_candidate(plan);records=candidate['collections']
        self.state['pending']={'keys':[r['collection-key'] for r in records],'hashes':[r['retained-sha256'] for r in records]};self.journal()
        archived=bool(plan.get('evidence-storage'))
        receipt=self.repo.stage({t:rows for t,rows in candidate.items() if t!='collections'} if archived else candidate)
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
        after=self.client.canonical_context()
        if canonical_fence(authority)!=canonical_fence(after):raise CanonicalTransition('Canonical scope changed; completion withheld')
        # Read the immutable candidate itself, never just mutable business rows.
        archived=bool(self.state.get('evidence-storage'))
        from retained_feed import collection_rows
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

    def class_batch(self,selected,base,policies):
        observations=[];batch=[];batch_begin=time.monotonic()
        sequence=self.state.get('slots',0)+1;slot=self.runkey+'|slot-'+str(sequence)
        folder=self.runtime/self.runkey.replace(':','_')/('class-batch-'+str(sequence));saved=folder/'source-batch.json'
        def get(state):
            p,m=self.source.fetch('/classes/'+str(state['class_id']),{'show_id':int(self.show.split('|')[1]),'customer_id':15})
            related=p.get('class_related_data',{});trips=p.get('trips')
            if str(p.get('show_id'))!=self.show.split('|')[1] or p.get('class',{}).get('class_id')!=state['class_id']:raise ValueError('Wrong show/class')
            if str(related.get('date'))[:10]!=state['day'] or str(related.get('ring'))!=str(state['ring']):raise ValueError('Class moved; inventory required')
            if not isinstance(trips,list) or p.get('total_entry_trips')!=len(trips):raise ValueError('Incomplete class trips')
            ids=[];source_ids=set()
            for trip in trips:
                if any(type(trip.get(k)) is not int or trip[k]<=0 for k in ('entry_id','horse_id','rider_id')):raise ValueError('Missing trip identity')
                identity=trip.get('entryxclasses_uuid') or trip.get('trip_id')
                valid=(isinstance(identity,str) and bool(identity.strip()) and identity==identity.strip()) if trip.get('entryxclasses_uuid') else (type(identity) is int and identity>0 or isinstance(identity,str) and identity.isdecimal() and int(identity)>0)
                if not valid:raise ValueError('Missing UUID and trip ID')
                source_id=('uuid' if trip.get('entryxclasses_uuid') else 'trip-id',str(identity))
                if source_id in source_ids:raise ValueError('Conflicting class source trip identity')
                source_ids.add(source_id)
                ids.append((trip['entry_id'],identity))
            if len(set(ids))!=len(ids):raise ValueError('Duplicate class trips')
            return p,m
        reservation={'class-keys':[s['class_key'] for s in selected],'slot':slot,'states':selected}
        folder.mkdir(parents=True,exist_ok=True)
        if (folder/'reservation.json').exists() and json.loads((folder/'reservation.json').read_text(encoding='utf-8'))!=reservation:raise ValueError('Reserved class batch scope changed')
        atomic(folder/'reservation.json',reservation)
        self.state['reserved-class']={'slot':sequence,'class-keys':reservation['class-keys']};self.journal()
        if saved.exists():batch=json.loads(saved.read_text(encoding='utf-8'))
        else:
            batch=capture_source_batch(folder/'responses.json',{'show-key':self.show,'keys':reservation['class-keys']},selected,lambda s:s['class_key'],get)
            atomic(saved,batch)
        (folder/'adaptive').mkdir(exist_ok=True)
        for index,(old,(p,m)) in enumerate(zip(selected,batch)):
            state,reason=planner.decide(old,p,planner.epoch(m['completed']),policies)
            observed=planner.epoch(m['completed'])
            today=datetime.datetime.fromtimestamp(observed,datetime.timezone.utc).astimezone(__import__('zoneinfo').ZoneInfo('America/New_York')).date().isoformat()
            if state['day']==today:
                state['interval']=min(state['interval'],COUNT_REFRESH_SECONDS)
                state['next_due']=min(state['next_due'],observed+COUNT_REFRESH_SECONDS)
                reason+='; current-day count inputs next due within 150 seconds of source observation'
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
        next_state={'slots':sequence,'reserved-class':None,'count-turn':self.state.get('count-turn',0)+1}
        if len(selected)>4:next_state['count-pass-run']=self.runkey
        self.observation('class',{'classes':[p for p,m in batch]}, {'started':min(m['started'] for p,m in batch),'completed':stamp(),'requests':[m for p,m in batch]},graph,next_state)
        duration=time.monotonic()-batch_begin
        timing=self.state.setdefault('timing',{});timing['last-class-batch-seconds']=duration;timing['last-class-batch-size']=len(selected)
        timing['class-batch-seconds-per-class']=duration/len(selected)
        self.journal()

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

    def collect_directory(self,context,request_limit=12,seconds=90):
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
        self.observation('entry-directory-reconciled' if progress['complete'] else 'entry-directory-partial',frozen,
            {'started':min((m['started'] for m in metrics),default=self.at),'completed':stamp(),'requests':metrics},graph,next_state)
        self.state['timing']['directory-persistence-seconds']=time.monotonic()-begin-timing['directory-acquisition-seconds'];self.journal()
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
        if recovered and self.state.get('completion-at'):return self.finish(authority)
        # Once summary publication starts, close that exact generation before new
        # source admission, even when a crash made inventory/class work due again.
        from retained_feed import collection_rows
        recovered_rows=collection_rows(self.repo,self.state)
        if recovered and any((recovered_rows[r['key']].get('scope') or '').startswith('day-count-summary|') for r in self.state['observations']):
            self.day_summaries(context)
            return self.finish(authority)
        if recovered and self.state.get('directory-reservation'):
            self.state['directory-capture']=self.state['directory-reservation']['capture']
            self.collect_directory(context)
        # Recover the exact reserved class order before any new directory/inventory read.
        reservation=self.runtime/self.runkey.replace(':','_')/('class-batch-'+str(self.state.get('slots',0)+1))/'reservation.json'
        if recovered and reservation.exists():
            saved=json.loads(reservation.read_text(encoding='utf-8'))
            self.class_batch(saved['states'],self.repo.scoped(self.show),self.repo.graph['lanes'])
        if self.show not in self.repo.graph['shows']:
            self.repo.stage({'runs':[{'run-key':self.runkey}],'shows':[{k:context[k] for k in ('show-key','show-id','customer-id','show-name','start-date','end-date','selected-date')}]})
        if not self.state.get('bootstrap-run'):
            retained=self.repo.scoped(self.show)
            mapping={t:{r[KEYS[t]]:self.repo.inventory[t][r[KEYS[t]]]['id'] for r in rows} for t,rows in retained.items()}
            self.observation('retained-bootstrap',{'retained-projection':retained,'record-map':mapping,
                'coverage':'Previously retained bounded records with original source times; not a new source observation'},
                {'started':self.at,'completed':stamp()}, {}, {'bootstrap-run':self.runkey})
        policies=self.repo.graph['lanes']
        if time.time()>=self.state.get('inventory-due',0):self.inventory(context)
        if not self.state.get('reconcile-start') or (self.state.get('reconciled-at',0)>=self.state['reconcile-start'] and time.time()>=self.state.get('reconcile-due',0)):
            self.state['reconcile-start']=time.time();self.state['reconcile-due']=time.time()+policies['reconcile']['interval-seconds'];self.journal()
        if self.state.get('directory',{}).get('complete') and time.time()>=self.state.get('directory-due',0):self.state.pop('directory',None)
        # Admission deadline is 180s; an admitted read/write/readback unit may finish later.
        # Alternate page and class batches so neither directory nor live work starves.
        first=True
        while first or time.monotonic()-start<180:
            first=False
            directory_work=self.collect_directory(context,seconds=max(0,min(90,180-(time.monotonic()-start))))
            states=[]
            active=set(active_class_keys(self.state))
            for key,row in self.repo.graph['work-items'].items():
                if key in active and row.get('status')=='Waiting':
                    s=json.loads(row['state-json']);s['next_due']=planner.epoch(row['next-due'])
                    today=datetime.datetime.now(__import__('zoneinfo').ZoneInfo('America/New_York')).date().isoformat()
                    wanted='current' if s['day']==today and s['status']=='Underway' else 'completed' if s['status']=='Completed' else 'today' if s['day']==today else 'other'
                    if wanted!=s['lane']:s['next_due']=0
                    if not s.get('last_success') or planner.epoch(s['last_success'])<self.state['reconcile-start']:s['next_due']=0
                    states.append(s)
            # Coalesce one bounded current-day pass; existing capture still uses
            # four concurrent requests and saves each response before persistence.
            limit=4 if self.state.get('count-pass-run')==self.runkey else 40
            selected=count_due(states,policies,time.time(),limit,self.state.get('count-turn',0))
            if selected and time.monotonic()-start<180:self.class_batch(selected,self.repo.scoped(self.show),policies)
            pending=[s for s in states if not s.get('last_success') or planner.epoch(s['last_success'])<self.state['reconcile-start']]
            if not pending:self.state['reconciled-at']=time.time()
            remaining=max(0,self.state.get('directory',{}).get('total',0)-len(self.state.get('directory',{}).get('ids',[])))
            self.state['capacity']={'unreconciled-classes':len(pending),'remaining-directory-entries':remaining,
                'directory-pages':__import__('math').ceil(remaining/max(1,self.state.get('directory',{}).get('size',1))),
                'class-estimate-seconds':len(pending)*self.state.get('timing',{}).get('class-batch-seconds-per-class',0),
                'measured':bool(self.state.get('timing',{})),'scheduler-wake-seconds':60,'admission-budget-seconds':180}
            today_states=[s for s in states if s['day']==datetime.datetime.now(__import__('zoneinfo').ZoneInfo('America/New_York')).date().isoformat()]
            measured=self.state.get('timing',{}).get('class-batch-seconds-per-class')
            estimate=len(today_states)*measured if measured else None
            self.state['capacity'].update({'today-count-scopes':len(today_states),'today-count-target-seconds':COUNT_FRESH_SECONDS,
                'today-class-work-estimate-seconds':estimate,'today-target-infeasible-before-publication':estimate>=COUNT_FRESH_SECONDS if estimate is not None else None})
            self.journal()
            if not selected and not directory_work:break
        from retained_feed import dependencies
        retained=dependencies(self.repo,self.state)
        if retained:self.observation('retained-dependencies',{'coverage':'Previously retained linked facts with their original timestamps; not fresh acquisition'}, {'started':self.at,'completed':stamp()},retained)
        self.day_summaries(context)
        return self.finish(authority)

    def day_summaries(self,context):
        from day_counts import summaries,retained_units
        from wef_data_handoff import revision_chain
        from retained_feed import collection_rows
        collections=collection_rows(self.repo,self.state)
        units=[]
        previous=self.state.get('predecessor')
        if previous:
            chain=revision_chain(previous['run-key'],self.repo.graph['runs'],collections)
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
