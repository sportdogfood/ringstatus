"""Owner-approved, archive-gated retirement. Invoked only by the scheduled Data runner."""
import contextlib,gzip,json,os,pathlib,subprocess
from data_contract import packed,digest
from wef_data_handoff import verify_snapshot,TABLE_KEYS
from wef_data_store import equivalent

ENTITIES={'horses','riders','trainers','owners'}
APPROVAL='APPROVED TO EDIT AND REMOVE ARCHIVED ROWS'

class WorkerBusy(RuntimeError):pass

class RetentionWait(RuntimeError):
    """Positively identified retention readiness wait, before any deletion."""

def mutation_pending(runtime):
    for file in (pathlib.Path(runtime)/'retirements').glob('*.json'):
        state=json.loads(file.read_text(encoding='utf-8'))
        if state.get('status')!='Complete' and (state.get('pending') or state.get('removed')):
            return True
    return False

@contextlib.contextmanager
def worker_lock(runtime):
    path=pathlib.Path(runtime)/'python-runner.lock';path.parent.mkdir(parents=True,exist_ok=True)
    with path.open('a+b') as stream:
        if path.stat().st_size==0:stream.write(b'0');stream.flush()
        stream.seek(0)
        if os.name=='nt':
            import msvcrt
            acquire=lambda:msvcrt.locking(stream.fileno(),msvcrt.LK_NBLCK,1)
            release=lambda:msvcrt.locking(stream.fileno(),msvcrt.LK_UNLCK,1)
        else:
            import fcntl
            acquire=lambda:fcntl.flock(stream.fileno(),fcntl.LOCK_EX|fcntl.LOCK_NB)
            release=lambda:fcntl.flock(stream.fileno(),fcntl.LOCK_UN)
        try:acquire()
        except OSError:raise WorkerBusy('Existing Python Data worker still owns the runner lock') from None
        try:yield
        finally:stream.seek(0);release()

def save(path,value):
    from data_runner import atomic
    atomic(path,value)
    with pathlib.Path(path).open('r+b') as stream:os.fsync(stream.fileno())

def archive(runtime,review):
    receipt=review['archive-receipt'];root=(pathlib.Path(runtime)/'archive-repository').resolve()
    folder=(root/receipt['path']).resolve()
    if not folder.is_relative_to(root):raise ValueError('Retirement archive path escapes repository')
    if not folder.exists():raise RetentionWait('Retirement archive is not available locally')
    try:manifest,facts=verify_snapshot(folder)
    except FileNotFoundError as error:raise ValueError('Existing retirement archive is incomplete') from error
    if manifest['show_key']!=review['target-show'] or not receipt.get('verified_remote_commit'):raise ValueError('Retirement archive scope/remote receipt invalid')
    def git(*args):
        return subprocess.run(['git','-C',str(root),*args],check=True,capture_output=True,timeout=90).stdout
    remote=git('remote','get-url','origin').decode().strip().removesuffix('.git')
    if remote not in ('https://github.com/sportdogfood/ringstatus-data','git@github.com:sportdogfood/ringstatus-data'):raise ValueError('Wrong retirement archive repository')
    try:git('fetch','origin','main')
    except (subprocess.CalledProcessError,subprocess.TimeoutExpired) as error:
        raise RetentionWait('Retirement archive remote is temporarily unavailable') from error
    git('merge-base','--is-ancestor',receipt['verified_remote_commit'],'origin/main')
    for name in ['manifest.json',*manifest['files']]:
        if git('show',receipt['verified_remote_commit']+':'+receipt['path']+'/'+name)!=(folder/name).read_bytes():raise ValueError('Published retirement archive differs')
    identities={}
    for table in TABLE_KEYS:
        rows=json.loads(gzip.decompress((folder/('tables/'+table+'.json.gz')).read_bytes()))
        identities[table]={str(r['fields'][TABLE_KEYS[table]]):r['id'] for r in rows}
    return facts,identities

def season_ack(repo,runtime,review):
    config=json.loads((pathlib.Path(runtime).parent.parent/'wef-season/schema.json').read_text())
    receipt=review['archive-receipt'];revision=pathlib.Path(receipt['path']).name
    if set(config['entities'])!={'horses','riders','trainers','rings'}:raise ValueError('Four Season destinations required')
    for entity,item in config['entities'].items():
        heart=item['heartbeat']
        rows=repo.client.records(item['base'],heart['id'],formula='{heartbeat-key}='+json.dumps('wef-season-'+entity))
        if not rows:raise RetentionWait('Season retirement acknowledgement missing: '+entity)
        if len(rows)!=1:raise ValueError('Season retirement acknowledgement ambiguous')
        checkpoint=json.loads(rows[0]['fields'].get(heart['checkpoint-field']) or '{}').get(review['target-show'],{})
        if not isinstance(checkpoint,dict):raise ValueError('Invalid Season archive checkpoint')
        if checkpoint.get('revision')!=revision or checkpoint.get('commit')!=receipt['verified_remote_commit']:raise RetentionWait('Season has not accepted approved archive: '+entity)

def same(row,prior,fields):
    for name in set(row)|set(prior):
        a=row.get(name);b=prior.get(name);field=fields[name]
        if field['type']=='multipleRecordLinks':
            if sorted((a or {}).get('$links',[]))!=sorted((b or {}).get('$links',[])):return False
        elif name.endswith('-id') and a!=b:return False
        elif not equivalent(a,b,field):return False
    return True

def order(graph,selected):
    edges={t:set() for t,keys in selected.items() if keys}
    for table in edges:
        for key in selected[table]:
            for value in graph[table][key].values():
                if isinstance(value,dict) and '$links' in value:
                    edges[table].update(t for t,k in value['$links'] if k in selected.get(t,set()))
    result=[]
    while edges:
        referenced=set().union(*edges.values())
        ready=sorted(set(edges)-referenced)
        if not ready:raise ValueError('Retirement forward-link cycle')
        result.extend(ready)
        for table in ready:edges.pop(table)
        for targets in edges.values():targets.difference_update(ready)
    return result

def guard_context(repo,target):
    context=repo.client.canonical_context()
    current=next(r['show-key'] for r in context['records'] if r['default'])
    if current==target:raise ValueError('Retirement target is canonical current show')
    return current

def fresh(repo,table,keys):
    records=repo.store.inventory(table,refresh=True,keys=keys);result={}
    for key,record in records.items():
        row={}
        for name in repo.authored[table]:
            field=repo.store.fields[table][name];value=record['fields'].get(field['id'])
            if value is None:continue
            if field['type']=='multipleRecordLinks':value={'$links':[list(repo.byid[v['id'] if isinstance(v,dict) else v]) for v in value]}
            row[name]=value
        result[key]={'id':record['id'],'row':row}
    return result

def present_ids(repo,table,ids):
    formula='OR('+','.join('RECORD_ID()='+json.dumps(v) for v in ids)+')'
    rows=repo.client.records(repo.schema['base'],repo.schema['tables'][table]['id'],formula=formula)
    primary=repo.schema['tables'][table]['primaryFieldId']
    return {r['id']:str(r['fields'][primary]) for r in rows}

def source_checkpoint(runtime,target):
    active=runtime/'active.json';pending=runtime/'pending.json';saved={}
    if (runtime/'deferred.json').exists():saved['deferred.json']=(runtime/'deferred.json').read_bytes()
    if not active.exists() and not pending.exists():return saved,set()
    # An unplanned source reservation must settle through the existing workflow.
    if not active.exists() or not pending.exists():return None,set()
    saved.update({p.name:p.read_bytes() for p in (active,pending)})
    owner=json.loads(saved['active.json']);plan=json.loads(saved['pending.json'])
    show=json.loads(owner['reserved-run']['state-json']).get('show-key')
    if show==target:return None,set()
    if not show or owner['run-key']!=plan['run-key'] or show!=plan['show-key']:raise ValueError('Current source checkpoint ownership is inconsistent')
    protected=set()
    for table,rows in plan['graph'].items():
        for row in rows:
            if table in ENTITIES:protected.add((table,str(row[TABLE_KEYS[table]])))
            for value in row.values():
                if isinstance(value,dict) and '$links' in value:protected.update((t,k) for t,k in value['$links'])
    return saved,protected

def pending_approval(runtime):
    runtime=pathlib.Path(runtime)
    for file in (runtime/'retirement-approvals').glob('*.json'):
        item=json.loads(file.read_text(encoding='utf-8'))
        path=runtime/'retirements'/(item['manifest']['target-show'].replace('|','-')+'.json')
        if not path.exists() or json.loads(path.read_text(encoding='utf-8')).get('status')!='Complete':return True
    return False

def protect(repo,selected,current,planned,identities,removed):
    skipped=[]
    def remaining():return {t:{k for k in keys if identities[t][k] not in removed} for t,keys in selected.items()}
    protected=repo.scoped(current) if current in repo.graph['shows'] else {t:[] for t in TABLE_KEYS}
    for table,keys in remaining().items():
        current_keys={r[TABLE_KEYS[table]] for r in protected[table]}|{k for t,k in planned if t==table}
        for key in keys&current_keys:
            if table not in ENTITIES:raise ValueError('Show-scoped retirement row became current')
            selected[table].remove(key);skipped.append(identities[table][key])
    # Re-run on every wake: a newly linked entity may not yet have show membership.
    active=remaining()
    for table,rows in repo.graph.items():
        for key,row in rows.items():
            if key in active[table]:continue
            for value in row.values():
                if not isinstance(value,dict) or '$links' not in value:continue
                for target,linked in value['$links']:
                    if linked not in active[target]:continue
                    if target not in ENTITIES:raise ValueError('Retirement still referenced by retained record')
                    selected[target].remove(linked);active[target].remove(linked);skipped.append(identities[target][linked])
    return skipped

def retire(repo,runtime):
    from data_runner import stamp
    runtime=pathlib.Path(runtime)
    totals=[]
    for file in sorted((runtime/'retirement-approvals').glob('*.json')):
        approval=json.loads(file.read_text(encoding='utf-8'));review=approval['manifest']
        if approval.get('approval')!=APPROVAL or approval.get('manifest-sha256')!=digest(packed(review)):raise ValueError('Retirement approval/hash missing')
        if review.get('blockers') or review.get('business-writes')!=0:raise ValueError('Retirement review did not pass')
        target=review['target-show'];statefile=runtime/'retirements'/(target.replace('|','-')+'.json')
        state=json.loads(statefile.read_text(encoding='utf-8')) if statefile.exists() else None
        signature=digest(packed(approval))
        if state and state['approval-sha256']!=signature:raise ValueError('Retirement approval changed after checkpoint')
        if state and state.get('status')=='Complete':continue
        checkpoint,planned=source_checkpoint(runtime,target)
        if checkpoint is None:return None
        current=guard_context(repo,target)
        try:
            facts,identities=archive(runtime,review)
            season_ack(repo,runtime,review)
        except Exception as error:
            from wef_data_airtable import TransportDeferred
            waiting=isinstance(error,RetentionWait) or isinstance(error,TransportDeferred) and error.operation['method']=='GET'
            if not waiting:raise
            if mutation_pending(runtime):raise ValueError('Retirement mutation must reconcile before retention can wait') from error
            raise RetentionWait(str(error)) from error
        items=review['candidates'];lookup={};selected={t:set() for t in TABLE_KEYS}
        for item in items:
            table=item['table'];key=item['key'];identity=item['record-id']
            if table not in TABLE_KEYS or table=='lanes':raise ValueError('Retirement excludes global/operational tables')
            if identities.get(table,{}).get(key)!=identity:raise ValueError('Retirement ID not in verified archive')
            if identity in lookup:raise ValueError('Repeated retirement record ID')
            lookup[identity]=item;selected[table].add(key)
        if not state:
            scoped=repo.scoped(target)
            if any({r[TABLE_KEYS[t]] for r in rows}!=set(facts[t]) for t,rows in scoped.items()):raise ValueError('Outgoing scope differs from approved full archive')
            skipped=protect(repo,selected,current,planned,identities,set())
            for table,keys in selected.items():
                for key in keys:
                    if repo.inventory[table].get(key,{}).get('id')!=identities[table][key] or not same(repo.graph[table][key],facts[table][key],repo.store.fields[table]):raise ValueError('Retirement candidate changed since archive')
            state={'approval-sha256':signature,'target':target,'status':'Deleting','started':stamp(),'order':order(repo.graph,selected),
                   'selected':{t:sorted(keys) for t,keys in selected.items()},'removed':[],'skipped-current':skipped,'pending':None,'tables':{}}
            save(statefile,state)
        selected={t:set(keys) for t,keys in state['selected'].items()}
        removed=set(state['removed']);started_count=len(removed)
        # A completed delete may have lost its response. Confirm its exact IDs first.
        if state['pending']:
            pending=state['pending'];actual=present_ids(repo,pending['table'],pending['ids'])
            for key,identity in zip(pending['keys'],pending['ids']):
                if identity not in actual:removed.add(identity)
                elif actual[identity]!=key:raise ValueError('Pending retirement record key changed')
            state.update({'removed':sorted(removed),'pending':None});save(statefile,state)
        skipped=protect(repo,selected,current,planned,identities,removed)
        state['skipped-current']=sorted(set(state['skipped-current']+skipped))
        state['selected']={t:sorted(keys) for t,keys in selected.items()}
        # Revalidate every remaining row before any resumed deletion, not just its table.
        for table,keys in selected.items():
            for key in keys:
                if identities[table][key] in removed:continue
                if repo.inventory[table].get(key,{}).get('id')!=identities[table][key] or not same(repo.graph[table][key],facts[table][key],repo.store.fields[table]):raise ValueError('Retirement remaining row changed since archive')
        save(statefile,state)
        for table in state['order']:
            guard_context(repo,target)
            keys=[k for k in state['selected'][table] if identities[table][k] not in removed]
            actual=fresh(repo,table,keys) if keys else {}
            for key in keys:
                identity=identities[table][key]
                if key not in actual or actual[key]['id']!=identity or not same(actual[key]['row'],facts[table][key],repo.store.fields[table]):raise ValueError('Retirement remaining row changed or disappeared without intent')
            for start in range(0,len(keys),10):
                batch=keys[start:start+10];ids=[identities[table][k] for k in batch]
                state['pending']={'table':table,'keys':batch,'ids':ids};save(statefile,state)
                response=repo.client.request('DELETE',repo.schema['base'],repo.schema['tables'][table]['id'],params={'records[]':ids})
                if {r.get('id') for r in response.get('records',[])}!=set(ids) or any(r.get('deleted') is not True for r in response['records']):raise ValueError('Retirement delete response invalid')
                if present_ids(repo,table,ids):raise ValueError('Retirement deletion readback failed')
                removed.update(ids);state.update({'removed':sorted(removed),'pending':None});save(statefile,state)
            state['tables'][table]={'status':'Verified removed','records':len(state['selected'][table])};save(statefile,state)
        # All retained authored rows/IDs must still match the before-delete snapshot.
        before={t:{k:(repo.inventory[t][k]['id'],row) for k,row in rows.items() if k not in selected[t] or repo.inventory[t][k]['id']!=identities[t].get(k)} for t,rows in repo.graph.items()}
        repo.reload(refresh=True);guard_context(repo,target)
        if target in repo.graph['shows']:raise ValueError('Retired show is still in Data')
        for table,rows in before.items():
            for key,(identity,row) in rows.items():
                if repo.inventory[table].get(key,{}).get('id')!=identity or not same(repo.graph[table][key],row,repo.store.fields[table]):raise ValueError('Retained record/identity/forward links changed')
        if any(set(r['id'] for r in repo.inventory[t].values())&{identities[t][k] for k in keys} for t,keys in selected.items()):raise ValueError('Retirement candidate still present')
        if any(not (runtime/name).exists() or (runtime/name).read_bytes()!=body for name,body in checkpoint.items()):raise ValueError('Current source checkpoint changed during retirement')
        state.update({'status':'Complete','completed':stamp(),'pending':None});save(statefile,state)
        totals.append({'show-key':target,'records-removed':len(removed),'removed-this-wake':len(removed)-started_count,'retained-newly-current':len(state['skipped-current']),'tables':state['tables']})
    return totals or None
