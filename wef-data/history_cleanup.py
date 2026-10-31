"""Bounded historical-control retirement, called only by the existing Data task.

The deployment receipt enables this policy only after all readers are deployed.
No business identities, entity records, native forward links or task cadences change.
"""
import hashlib,json,pathlib,subprocess
from data_contract import digest,packed
from retirement import RetentionWait,save,present_ids,same
from archive_evidence import HistoryArchive,REMOTES

FORMAT='wef-data-history-cleanup-v1'
READERS=('wef-data/retained_feed.py','wef-data/archive_evidence.py',
         'wef-data/data_runner.py','wef-data/history_cleanup.py',
         'wef-shows/day-source.js','wef-shows/day-archive.js','wef-season/season.py')

def pending(runtime):
    path=pathlib.Path(runtime)/'history-cleanup-state.json'
    return bool(path.exists() and json.loads(path.read_text(encoding='utf-8')).get('pending'))

def held_summary(state,path,held):
    """Keep exact diagnostics locally; never emit thousands of keys to stdout."""
    counts={}
    for reason in held.values():counts[reason]=counts.get(reason,0)+1
    state['held']=held;save(path,state)
    return {'held-records':len(held),'held-reasons':counts,'held-detail-path':str(path)}

def policy(runtime):
    path=pathlib.Path(runtime)/'history-cleanup-policy.json'
    if not path.exists():return None
    value=json.loads(path.read_text(encoding='utf-8'))
    if value.get('format')!=FORMAT or set(value.get('tables',[]))!={'runs','collections'} or set(value.get('reader-sha256',{}))!=set(READERS):
        raise ValueError('Historical cleanup deployment receipt is incomplete')
    root=pathlib.Path(runtime).resolve().parent.parent
    for name,sha in value['reader-sha256'].items():
        if hashlib.sha256((root/name).read_bytes()).hexdigest()!=sha:
            raise ValueError('Historical cleanup reader differs from verified deployment: '+name)
    return value

def verify_remote(history,items):
    """Check the exact published manifest and this bounded batch before deleting."""
    def git(*args):
        return subprocess.run(['git','-C',str(history.repository),*args],check=True,
            capture_output=True,timeout=60).stdout
    try:
        remote=git('remote','get-url','origin').decode().strip().removesuffix('.git')
        if remote not in REMOTES:raise ValueError('Wrong historical cleanup archive repository')
        git('fetch','origin','main')
        commit=history.receipt['verified_remote_commit']
        git('merge-base','--is-ancestor',commit,'origin/main')
        name=history.receipt['manifest']
        if git('show',commit+':'+name)!=(history.repository/name).read_bytes():
            raise ValueError('Historical cleanup published manifest differs')
        for item in items:
            kind='runs' if item['table']=='runs' else 'legacy-collections'
            entry=history.manifest['records'][kind][item['key']]
            if entry['record-id']!=item['record-id'] or git('show',commit+':'+entry['path'])!=history.object_bytes(kind,item['key']):
                raise ValueError('Historical cleanup published object differs')
    except (subprocess.CalledProcessError,subprocess.TimeoutExpired) as error:
        raise RetentionWait('Historical cleanup archive verification unavailable') from error

def season_cutoff(repo,runtime,history):
    """All four checkpoints must name accepted ancestors of the native head."""
    from retained_feed import run_rows,collection_rows
    from wef_data_handoff import revision_chain
    config=json.loads((pathlib.Path(runtime).resolve().parent.parent/'wef-season/schema.json').read_text(encoding='utf-8'))
    if set(config['entities'])!={'horses','riders','trainers','rings'}:raise ValueError('Four Season destinations required')
    chain=revision_chain(history.head['run-key'],run_rows(repo,history.show),collection_rows(repo))
    accepted={digest(packed(state)):state['sequence'] for state,units in chain};cutoffs=[]
    for entity,item in config['entities'].items():
        heart=item['heartbeat']
        rows=repo.client.records(item['base'],heart['id'],formula='{heartbeat-key}='+json.dumps('wef-season-'+entity))
        if len(rows)!=1:raise RetentionWait('Historical cleanup Season checkpoint missing/ambiguous: '+entity)
        checkpoint=json.loads(rows[0]['fields'].get(heart['checkpoint-field']) or '{}').get(history.show,{})
        sha=checkpoint.get('revision')
        if checkpoint.get('kind')!='retained' or sha not in accepted or checkpoint.get('sequence')!=accepted[sha]:
            raise RetentionWait('Historical cleanup awaits accepted Season revision: '+entity)
        cutoffs.append(accepted[sha])
    return min(cutoffs)

def eligible(repo,history,cutoff):
    """Only exact archived rows with no retained forward-link consumer qualify."""
    pinned=set()
    for table,rows in repo.graph.items():
        for row in rows.values():
            for value in row.values():
                if isinstance(value,dict) and '$links' in value:
                    pinned.update((target,key) for target,key in value['$links'] if target in ('runs','collections'))
    candidates=[];held={};sequence={}
    for key in history.manifest['records']['runs']:
        sequence[key]=json.loads(history.read('runs',key)['state-json'])['sequence']
    for table,kind in (('collections','legacy-collections'),('runs','runs')):
        for key,entry in sorted(history.manifest['records'][kind].items()):
            row=repo.graph[table].get(key)
            if row is None:continue
            reason=None
            saved=history.read(kind,key) if table=='runs' else history.read_raw(kind,key)
            prior={k:v for k,v in saved.items() if k!='_record-id'}
            owners=[key] if table=='runs' else [k for t,k in prior.get('run',{}).get('$links',[]) if t=='runs']
            if key==history.head['run-key'] and table=='runs':reason='current-head'
            elif (table,key) in pinned:reason='native-forward-link'
            elif not owners or any(k not in sequence or sequence[k]>cutoff for k in owners):reason='Season-or-recovery-owner'
            elif repo.inventory[table].get(key,{}).get('id')!=entry['record-id']:raise ValueError('Historical cleanup native ID changed')
            elif not same(row,prior,repo.store.fields[table]):reason='native-fields-not-identical-to-archive'
            if reason:held[table+'|'+key]=reason
            else:candidates.append({'table':table,'key':key,'record-id':entry['record-id']})
    return candidates,held

def retire(repo,runtime,repository):
    runtime=pathlib.Path(runtime);enabled=policy(runtime)
    if enabled is None:
        if pending(runtime):raise RetentionWait('Historical cleanup pending DELETE requires reconciliation; policy absent')
        return None
    # Never compete with an acquisition/recovery reservation, including a stop.
    if any((runtime/name).exists() for name in ('stopped.json','active.json','pending.json','deferred.json','source-deferred.json','archive-deferred.json')):
        raise RetentionWait('Historical cleanup preserves active/recovery checkpoint')
    from retained_feed import history_archive
    from wef_data_airtable import canonical_fence
    context=repo.client.canonical_context();fence=canonical_fence(context)
    show=next(r['show-key'] for r in context['records'] if r['default'])
    repo.archive_repository=pathlib.Path(repository);repo.history_show=show
    history=history_archive(repo,show)
    if history is None:raise RetentionWait('Historical cleanup requires published accepted head')
    path=runtime/'history-cleanup-state.json'
    state=json.loads(path.read_text(encoding='utf-8')) if path.exists() else {'format':FORMAT,'removed':[],'pending':None}
    if state.get('format')!=FORMAT:raise ValueError('Historical cleanup journal format changed')
    # Reconcile a lost DELETE response by its original IDs before any new batch.
    if state['pending']:
        batch=state['pending']['items'];group=batch[0]['table']
        if any(i['table']!=group for i in batch) or not 0<len(batch)<=10:raise ValueError('Historical cleanup pending batch invalid')
        actual=present_ids(repo,group,[i['record-id'] for i in batch])
        for item in batch:
            if item['record-id'] not in actual:
                if item not in state['removed']:state['removed'].append(item)
            elif actual[item['record-id']]!=item['key']:raise ValueError('Historical cleanup pending key changed')
        state['pending']=None;save(path,state);repo.reload(refresh=True)
    cutoff=season_cutoff(repo,runtime,history)
    candidates,held=eligible(repo,history,cutoff)
    report=held_summary(state,path,held)
    if not candidates:return {'removed-this-wake':0,**report}
    table=candidates[0]['table'];batch=[i for i in candidates if i['table']==table][:10]
    verify_remote(history,batch)
    # A fresh native inventory closes the initial read/publication interval.
    repo.reload(refresh=True)
    if canonical_fence(repo.client.canonical_context())!=fence:raise RetentionWait('Historical cleanup canonical fence changed')
    current=history_archive(repo,show)
    if current is None or current.head['run-key']!=history.head['run-key']:raise RetentionWait('Historical cleanup head advanced')
    allowed,_=eligible(repo,current,cutoff)
    if any(item not in allowed for item in batch):raise RetentionWait('Historical cleanup candidate became referenced or changed')
    state['pending']={'items':batch,'show-key':show,'receipt':history.receipt};save(path,state)
    ids=[i['record-id'] for i in batch]
    response=repo.client.request('DELETE',repo.schema['base'],repo.schema['tables'][table]['id'],params={'records[]':ids})
    if {r.get('id') for r in response.get('records',[])}!=set(ids) or any(r.get('deleted') is not True for r in response['records']):
        raise ValueError('Historical cleanup DELETE response invalid; intent retained')
    if present_ids(repo,table,ids):raise ValueError('Historical cleanup DELETE readback incomplete; intent retained')
    state['removed'].extend(i for i in batch if i not in state['removed']);state['pending']=None;save(path,state)
    repo.reload(refresh=True)
    return {'removed-this-wake':len(batch),'table':table,**report}
