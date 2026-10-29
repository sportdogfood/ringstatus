"""Complete retained table accounting, including operational logs; no deletions."""
import json,pathlib,copy
from data_contract import packed,digest

def retained_checkpoints(runtime,show):
    root=pathlib.Path(runtime);output=[]
    for folder in [root,*sorted((root/'retired-checkpoints').glob('*'))]:
        active=folder/'active.json'
        if not active.exists():continue
        saved=json.loads(active.read_text(encoding='utf-8'));state=json.loads(saved.get('reserved-run',{}).get('state-json') or '{}')
        if state.get('show-key')!=show:continue
        item={'active':saved}
        if (folder/'pending.json').exists():item['pending']=json.loads((folder/'pending.json').read_text(encoding='utf-8'))
        output.append(item)
    return output

def include_reserved(repo,graph,checkpoints):
    """Classify actual sparse rows from explicit saved ownership, never fill facts."""
    from data_runner import pending_candidate
    from wef_data_handoff import TABLE_KEYS
    rows={t:{r[TABLE_KEYS[t]]:copy.deepcopy(r) for r in values} for t,values in graph.items()}
    expected_show=graph['shows'][0]['show-key']
    for checkpoint in checkpoints:
        candidates={'runs':[{'run-key':checkpoint['active']['run-key']}]}
        if checkpoint.get('pending'):candidates=pending_candidate(checkpoint['pending'])
        for table,planned in candidates.items():
            for proposed in planned:
                key=proposed[TABLE_KEYS[table]]
                if key in repo.graph[table]:rows[table].setdefault(key,copy.deepcopy(repo.graph[table][key]))
    changed=True
    while changed:
        changed=False
        for table,values in list(rows.items()):
            for row in list(values.values()):
                for value in row.values():
                    if not isinstance(value,dict) or '$links' not in value:continue
                    for target,key in value['$links']:
                        if target=='shows' and key!=expected_show:raise ValueError('Reserved archive cross-show dependency')
                        if key not in rows[target]:rows[target][key]=copy.deepcopy(repo.graph[target][key]);changed=True
    return {t:[values[k] for k in sorted(values)] for t,values in rows.items()}

def named_records(client,base,table):
    output=[];offset=None;seen=set();ids=set()
    while True:
        query={'pageSize':100,'returnFieldsByFieldId':'false'}
        if offset:query['offset']=offset
        value=client.request('GET',base,table,params=query)
        for row in value['records']:
            if row['id'] in ids:raise ValueError('Operational duplicate record ID')
            ids.add(row['id']);output.append(row)
        offset=value.get('offset')
        if not offset:return output
        if offset in seen:raise ValueError('Operational pagination cycle')
        seen.add(offset)

def operational(repo,key,config):
    data=config['workflows']['wef-data'];tables=data['tables'];client=repo.client;base=data['base']
    attempts=named_records(client,base,tables['run-logs']);owners={};unscoped=[]
    for row in attempts:
        fields=row['fields'];show=fields.get('show-key');run=repo.graph['runs'].get(fields.get('run-key'))
        if not show and run:show=json.loads(run.get('state-json') or '{}').get('show-key')
        owners[row['id']]=show
        if not show:unscoped.append(row['id'])
    selected=[r for r in attempts if owners[r['id']]==key];errors=named_records(client,base,tables['err-logs']);selected_errors=[]
    for row in errors:
        links=row['fields'].get('run',[]);shows={owners.get(v['id'] if isinstance(v,dict) else v) for v in links}
        if key in shows:
            if len(shows)!=1:raise ValueError('Ambiguous cross-show operational error')
            selected_errors.append(row)
    hearts=named_records(client,base,tables['heartbeat']);meta=named_records(client,base,'tblwBvigcmqRlSYUb')
    return {'run-logs':selected,'err-logs':selected_errors,'heartbeat':[],'meta':meta},{'global-heartbeat-ids':[r['id'] for r in hearts],'unscoped-operational-ids':unscoped,'global-meta-copy-ids':[r['id'] for r in meta]}, {'run-logs':attempts,'err-logs':errors,'heartbeat':hearts}

def coverage(repo,key,graph,ops,current):
    from wef_data_handoff import TABLE_KEYS
    current_graph=repo.scoped(current) if current in repo.graph['shows'] else {t:[] for t in TABLE_KEYS}
    current_keys={t:{r[TABLE_KEYS[t]] for r in rows} for t,rows in current_graph.items()}
    value={}
    for table,rows in graph.items():
        selected=[]
        for source in rows:
            row=copy.deepcopy(source);identity=row[TABLE_KEYS[table]]
            if table=='lanes' or (table in ('horses','riders','trainers','owners') and identity in current_keys[table]):
                row={k:v for k,v in row.items() if k==TABLE_KEYS[table] or k.endswith('-id')}
            if table=='shows':row.pop('status',None)
            selected.append({'id':repo.inventory[table][identity]['id'],'fields':row})
        value[table]=selected
    value['operational']=ops
    return digest(packed(value))
