"""Local materialization of exact verified Data evidence, never a second authority."""
import copy,json,pathlib,sys,hashlib
from data_contract import packed,digest

def history_archive(repo,show=None,head_key=None):
    show=show or getattr(repo,'history_show',None)
    if show is None:
        scopes={json.loads(r.get('state-json') or '{}').get('show-key') for r in repo.graph['runs'].values() if r.get('status')=='Verified completed revision'}-{None}
        if len(scopes)>1:raise ValueError('Explicit historical show scope required')
        show=next(iter(scopes),None)
    candidates=[]
    for row in repo.graph['runs'].values():
        if row.get('status')=='Verified completed revision':
            state=json.loads(row.get('state-json') or '{}')
            if state.get('show-key')==show and (head_key is None or row['run-key']==head_key):candidates.append((state.get('sequence',0),row))
    if not candidates:return None
    head=max(candidates,key=lambda item:item[0])[1]
    receipt=json.loads(head.get('summary') or '{}').get('evidence-archive')
    if not receipt:return None
    repository=getattr(repo,'archive_repository',None)
    if repository is None:
        evidence=getattr(repo,'evidence',None)
        if evidence is None:return None
        repository=evidence.repository
    identity=(head['run-key'],digest(head['state-json']),digest(packed(receipt)))
    cached=getattr(repo,'_history_archive',None)
    if cached and cached[0]==identity:return cached[1]
    from archive_evidence import HistoryArchive
    history=HistoryArchive(repository,receipt,head,repo.inventory['runs'][head['run-key']]['id'])
    repo._history_archive=(identity,history);return history

def run_rows(repo,show=None,head_key=None):
    history=history_archive(repo,show,head_key)
    class Runs(dict):
        def __missing__(self,key):
            if history is None:raise KeyError(key)
            row=history.read('runs',key);self[key]=row;return row
        def get(self,key,default=None):
            try:return self[key]
            except KeyError:return default
    return Runs(repo.graph['runs'])

def run_identity(repo,key,show=None):
    native=repo.inventory['runs'].get(key)
    return native['id'] if native else run_rows(repo,show)[key]['_record-id']

def collection_rows(repo,state=None):
    """Legacy native IDs and archive objects share validation, never fabricated IDs."""
    references={}
    history=history_archive(repo,(state or {}).get('show-key'))
    if history:
        for ref in history.manifest['records']['observations']:
            if 'archive' in ref:references[ref['key']]=ref
    for row in repo.graph['runs'].values():
        for ref in json.loads(row.get('state-json') or '{}').get('observations',[]):
            if 'archive' in ref:references[ref['key']]=ref
    for ref in (state or {}).get('observations',[]):
        if 'archive' in ref:references[ref['key']]=ref
    class Rows(dict):
        def __missing__(self,key):
            if key not in references:
                if history is None:raise KeyError(key)
                row=history.read('legacy-collections',key);self[key]=row;return row
            store=getattr(repo,'evidence',None)
            if store is not None:row=store.read(references[key])
            else:
                if history is None:raise ValueError('Archive-backed observation requires retained evidence reader')
                from archive_evidence import archived_row,checked_path
                ref=references[key];name=ref['archive']['path']
                if name!=history.prefix+'/parts/'+ref['sha256']+'.json.gz':raise ValueError('Archive part outside historical show scope')
                row=archived_row(checked_path(history.repository,name).read_bytes(),ref)
            self[key]=row;return row
    return Rows({k:{**v,'_record-id':repo.inventory['collections'][k]['id']} for k,v in repo.graph['collections'].items()})

def replay(units):
    from wef_data_handoff import TABLE_KEYS
    facts={}
    for unit in units:
        if unit['scope']=='retained-bootstrap':
            for table,rows in unit['source']['retained-projection'].items():facts.setdefault(table,{}).update({r[TABLE_KEYS[table]]:copy.deepcopy(r) for r in rows})
        for table,rows in unit['graph'].items():
            for row in rows:facts.setdefault(table,{}).setdefault(row[TABLE_KEYS[table]],{}).update(copy.deepcopy(row))
    return facts

def facts_for_state(repo,state):
    from wef_data_handoff import revision_chain
    from day_counts import retained_units
    collections=collection_rows(repo,state)
    units=[];previous=state.get('predecessor')
    if previous:
        for _,saved in revision_chain(previous['run-key'],run_rows(repo,state['show-key']),collections):units.extend(saved)
    units.extend(u for u,refs in retained_units(state,collections))
    return replay(units)

def dependencies(repo,state):
    facts=facts_for_state(repo,state);extra={}
    domain={'shows','days','rings','groups','classes','schedules','entries','horses','riders','trainers','owners','trips','results'}
    changed=True
    while changed:
        changed=False
        for rows in list(facts.values()):
            for row in list(rows.values()):
                for value in row.values():
                    if not isinstance(value,dict) or '$links' not in value:continue
                    for table,key in value['$links']:
                        if table not in domain or key in facts.get(table,{}):continue
                        if table=='shows' and key!=state['show-key']:raise ValueError('Cross-show retained dependency')
                        actual=copy.deepcopy(repo.graph[table][key]);facts.setdefault(table,{})[key]=actual;extra.setdefault(table,[]).append(actual);changed=True
    return extra

def put(root,name,value):
    path=pathlib.Path(root)/name;path.parent.mkdir(parents=True,exist_ok=True)
    body=packed(value)
    if path.exists():
        if path.read_text(encoding='utf-8')!=body:raise ValueError('Immutable retained cache collision')
        return
    temporary=path.with_suffix('.tmp');temporary.write_text(body,encoding='utf-8');temporary.replace(path)

def publish(repo,state,root,authority):
    from wef_data_handoff import revision_chain
    runs=run_rows(repo,state['show-key']);head=next(k for k,r in runs.items() if r.get('state-json')==packed(state))
    collections=collection_rows(repo,state)
    chain=revision_chain(head,runs,collections);run_names={}
    for journal,_ in chain:
        key=next(k for k,r in runs.items() if r.get('state-json')==packed(journal));sha=digest(runs[key]['state-json'])
        put(root,'objects/run-'+sha+'.json',{n:runs[key][n] for n in ('run-key','status','state-json')});run_names[key]=sha
        # Equal frozen bytes can belong to distinct existing collection records.
        for ref in journal['observations']:
            if 'archive' in ref:
                from archive_evidence import immutable,archived_row,checked_path
                store=getattr(repo,'evidence',None)
                if store is not None:
                    local=store.local_path(ref)
                    source=local if local.exists() else checked_path(store.repository,ref['archive']['path'])
                else:
                    history=history_archive(repo,state['show-key'])
                    if history is None or ref['archive']['path']!=history.prefix+'/parts/'+ref['sha256']+'.json.gz':raise ValueError('Archive part outside historical show scope')
                    source=checked_path(history.repository,ref['archive']['path'])
                body=source.read_bytes();archived_row(body,ref)
                immutable(pathlib.Path(root)/'objects'/('part-archive-'+ref['archive']['sha256']+'.json.gz'),body)
            else:put(root,'objects/part-'+ref['record-id']+'-'+ref['sha256']+'.json',{n:collections[ref['key']][n] for n in ('collection-key','status','retained-json','retained-sha256','_record-id')})
    from data_runner import atomic
    atomic(pathlib.Path(root)/('head-'+state['show-key'].replace('|','-')+'.json'),{'head':head,'run-sha256':run_names[head],'show-key':state['show-key'],'authority':authority})

def load(root,pointer):
    from wef_data_handoff import revision_chain
    root=pathlib.Path(root);runs={};collections={};key=pointer['head'];sha=pointer['run-sha256'];seen=set()
    while key:
        if key in seen:raise ValueError('Retained cache cycle')
        seen.add(key);row=json.loads((root/'objects'/('run-'+sha+'.json')).read_text(encoding='utf-8'))
        if row['run-key']!=key or digest(row['state-json'])!=sha:raise ValueError('Retained run hash/key mismatch')
        runs[key]=row;state=json.loads(row['state-json'])
        for ref in state['observations']:
            if 'archive' in ref:
                from archive_evidence import archived_row
                part=archived_row((root/'objects'/('part-archive-'+ref['archive']['sha256']+'.json.gz')).read_bytes(),ref)
            else:part=json.loads((root/'objects'/('part-'+ref['record-id']+'-'+ref['sha256']+'.json')).read_text(encoding='utf-8'))
            if part['collection-key']!=ref['key']:raise ValueError('Retained part key mismatch')
            collections[ref['key']]=part
        previous=state.get('predecessor');key=previous['run-key'] if previous else None;sha=previous['state-sha256'] if previous else None
    chain=revision_chain(pointer['head'],runs,collections);facts=replay([u for state,units in chain for u in units])
    if set(facts.get('shows',{}))!={pointer['show-key']}:raise ValueError('Retained show projection mismatch')
    return facts,{'run-key':pointer['head'],'state-sha256':pointer['run-sha256'],'show-key':pointer['show-key'],'sequence':chain[-1][0]['sequence']}
