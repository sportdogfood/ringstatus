"""Local materialization of exact verified Data evidence, never a second authority."""
import copy,json,pathlib,sys,hashlib
from data_contract import packed,digest

def replay(units):
    from wef_data_handoff import TABLE_KEYS
    facts={}
    for unit in units:
        if unit['scope']=='retained-bootstrap':
            for table,rows in unit['source']['retained-projection'].items():facts.setdefault(table,{}).update({r[TABLE_KEYS[table]]:copy.deepcopy(r) for r in rows})
        for table,rows in unit['graph'].items():
            for row in rows:facts.setdefault(table,{}).setdefault(row[TABLE_KEYS[table]],{}).update(copy.deepcopy(row))
    return facts

def dependencies(repo,state):
    from wef_data_handoff import revision_chain
    from day_counts import retained_units
    collections={k:{**v,'_record-id':repo.inventory['collections'][k]['id']} for k,v in repo.graph['collections'].items()}
    units=[];previous=state.get('predecessor')
    if previous:
        for _,saved in revision_chain(previous['run-key'],repo.graph['runs'],collections):units.extend(saved)
    units.extend(u for u,refs in retained_units(state,collections));facts=replay(units);extra={}
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
    runs=repo.graph['runs'];head=next(k for k,r in runs.items() if r.get('state-json')==packed(state))
    collections={k:{**v,'_record-id':repo.inventory['collections'][k]['id']} for k,v in repo.graph['collections'].items()}
    chain=revision_chain(head,runs,collections);run_names={}
    for journal,_ in chain:
        key=next(k for k,r in runs.items() if r.get('state-json')==packed(journal));sha=digest(runs[key]['state-json'])
        put(root,'objects/run-'+sha+'.json',{n:runs[key][n] for n in ('run-key','status','state-json')});run_names[key]=sha
        # Equal frozen bytes can belong to distinct existing collection records.
        for ref in journal['observations']:put(root,'objects/part-'+ref['record-id']+'-'+ref['sha256']+'.json',{n:collections[ref['key']][n] for n in ('collection-key','status','retained-json','retained-sha256','_record-id')})
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
            part=json.loads((root/'objects'/('part-'+ref['record-id']+'-'+ref['sha256']+'.json')).read_text(encoding='utf-8'))
            if part['collection-key']!=ref['key']:raise ValueError('Retained part key mismatch')
            collections[ref['key']]=part
        previous=state.get('predecessor');key=previous['run-key'] if previous else None;sha=previous['state-sha256'] if previous else None
    chain=revision_chain(pointer['head'],runs,collections);facts=replay([u for state,units in chain for u in units])
    if set(facts.get('shows',{}))!={pointer['show-key']}:raise ValueError('Retained show projection mismatch')
    return facts,{'run-key':pointer['head'],'state-sha256':pointer['run-sha256'],'show-key':pointer['show-key'],'sequence':chain[-1][0]['sequence']}
