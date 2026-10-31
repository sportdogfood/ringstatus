"""Immutable retained evidence in the existing Data archive; no business acquisition."""
import copy,gzip,hashlib,io,json,os,pathlib,subprocess,tarfile,time
from data_contract import packed,digest

FORMAT='wef-data-evidence-v1'
REMOTES={'https://github.com/sportdogfood/ringstatus-data','git@github.com:sportdogfood/ringstatus-data'}

class ArchiveDeferred(RuntimeError):pass

def prefix_for(show):
    parts=show.split('|')
    if len(parts)!=2 or any(not p.isdecimal() for p in parts):raise ValueError('Invalid archive show key')
    return 'wef-data/shows/'+parts[0]+'/'+parts[1]+'/retained-evidence'

def checked_path(root,name):
    relative=pathlib.PurePosixPath(name)
    if relative.is_absolute() or '\\' in name or '..' in relative.parts:raise ValueError('Archive path escapes scope')
    root=pathlib.Path(root).resolve();path=(root/name).resolve()
    if not path.is_relative_to(root):raise ValueError('Archive path escapes scope')
    return path

def immutable(path,body):
    path=pathlib.Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    temporary=path.with_suffix(path.suffix+'.tmp')
    if path.exists():
        if path.read_bytes()!=body:raise ValueError('Immutable evidence collision')
        temporary.unlink(missing_ok=True)
        return
    with temporary.open('wb') as stream:stream.write(body);stream.flush();os.fsync(stream.fileno())
    for attempt in range(3):
        try:os.replace(temporary,path);return
        except PermissionError as error:
            if getattr(error,'winerror',None) not in (5,32) or attempt==2:raise
            time.sleep(0.05*(attempt+1))

def archived_row(body,reference):
    proof=reference['archive']
    if proof.get('format')!=FORMAT or len(body)!=proof['bytes'] or hashlib.sha256(body).hexdigest()!=proof['sha256']:raise ValueError('Archive object hash/length mismatch')
    part=json.loads(gzip.decompress(body))
    if part.get('retained-sha256')!=reference['sha256'] or digest(part['retained-json'])!=reference['sha256']:raise ValueError('Archive retained payload mismatch')
    return {**part,'collection-key':reference['key'],'scope':reference.get('scope'),'status':'Verified retained observation','_archive':proof}

class HistoryArchive:
    """Read existing published objects; never put archived IDs into write caches."""
    def __init__(self,repository,receipt,head,native_id):
        self.repository=pathlib.Path(repository);self.receipt=receipt;self.head=head
        state=json.loads(head['state-json']);self.show=state['show-key'];self.prefix=prefix_for(self.show)
        sha=digest(head['state-json']);name=self.prefix+'/revisions/'+sha+'.json'
        if receipt.get('format')!=FORMAT or not receipt.get('verified_remote_commit') or receipt.get('manifest')!=name or receipt.get('state-sha256')!=sha:
            raise ValueError('Historical archive receipt mismatch')
        self.manifest=json.loads(checked_path(self.repository,name).read_text(encoding='utf-8'))
        manifest=self.manifest
        if manifest.get('format')!=FORMAT or manifest.get('show-key')!=self.show or manifest.get('run-key')!=head['run-key'] or manifest.get('state-sha256')!=sha or manifest.get('authority')!=state.get('authority'):
            raise ValueError('Historical archive head mismatch')
        self.loaded={}
        row=self.read('runs',head['run-key'])
        if row['_record-id']!=native_id or row['state-json']!=head['state-json']:raise ValueError('Historical native head changed')

    def object_bytes(self,kind,key):
        entry=self.manifest['records'][kind][key];name=entry['path'];proof=self.manifest['files'][name]
        if name!=self.prefix+'/'+kind+'/'+proof['sha256']+'.json.gz':raise ValueError('Historical archive object outside scope')
        body=checked_path(self.repository,name).read_bytes()
        if len(body)!=proof['bytes'] or hashlib.sha256(body).hexdigest()!=proof['sha256']:raise ValueError('Historical archive object changed')
        return body

    def read_raw(self,kind,key):
        """Preserve exact original rows, including non-observation readbacks."""
        if kind not in ('runs','legacy-collections'):raise ValueError('Unexpected historical record kind')
        identity=(kind,key)
        if identity in self.loaded:return self.loaded[identity]
        row=json.loads(gzip.decompress(self.object_bytes(kind,key)))
        entry=self.manifest['records'][kind][key];field='run-key' if kind=='runs' else 'collection-key'
        if row.get(field)!=key or row.get('_record-id')!=entry['record-id']:raise ValueError('Historical archive key/native ID mismatch')
        self.loaded[identity]=row;return row

    def read(self,kind,key):
        """Accepted consumers require payload evidence even after a raw cache hit."""
        row=self.read_raw(kind,key);entry=self.manifest['records'][kind][key]
        if kind=='runs':
            state=json.loads(row['state-json'])
            if row.get('status')!='Verified completed revision' or state.get('phase')!='Complete' or state.get('show-key')!=self.show or state.get('run-record-id')!=entry['record-id']:
                raise ValueError('Historical run is not accepted')
        elif not isinstance(row.get('retained-json'),str) or row.get('retained-sha256')!=digest(row['retained-json']):
            raise ValueError('Historical accepted observation payload missing or changed')
        return row

def publish_bundle(repository,prefix,files,allowed_remotes=REMOTES,pending_bundles=None):
    """Publish without rewriting history; reconcile only this producer's local commits."""
    repository=pathlib.Path(repository);primary_count=len(files);files=dict(files);prefixes=[prefix]
    for pending,parts in (pending_bundles or {}).items():
        if pending==prefix:
            if parts!=files:raise ValueError('Conflicting pending archive bundle')
            continue
        prefixes.append(pending)
        for name,body in parts.items():
            if not name.startswith(pending+'/') or name in files:raise ValueError('Pending archive scope conflict')
            checked_path(repository,name);files[name]=body
    def git(*args,binary=False):
        try:return subprocess.check_output(['git','-C',str(repository),*args],stderr=subprocess.PIPE,timeout=60,text=not binary)
        except (subprocess.CalledProcessError,subprocess.TimeoutExpired) as error:raise ArchiveDeferred('Archive Git '+args[0]+' did not complete; saved evidence retained') from error
    remote=git('remote','get-url','origin').strip().removesuffix('.git')
    if remote not in allowed_remotes:raise ValueError('Wrong retained evidence repository')
    if git('branch','--show-current').strip()!='main':raise ValueError('Evidence publication requires existing main')
    # Only this exact immutable bundle may remain dirty after an interrupted publication.
    dirty=git('status','--porcelain','--untracked-files=all').splitlines()
    for line in dirty:
        name=line[3:].strip('"')
        if line.startswith('?? ') and name.endswith('.tmp') and name[:-4] in files:
            target=checked_path(repository,name[:-4])
            if target.exists() and target.read_bytes()!=files[name[:-4]]:raise ValueError('Interrupted archive destination collision')
            checked_path(repository,name).unlink();continue
        if name not in files:raise ValueError('Archive checkout has unrelated changes')
    git('fetch','origin','main')
    ahead,behind=map(int,git('rev-list','--left-right','--count','HEAD...origin/main').split())
    if ahead:
        # Inspect every unpublished commit, including changes later reverted.
        # A merge-tree preview rejects conflicts without changing the checkout.
        for commit in git('rev-list','origin/main..HEAD').splitlines():
            names=git('diff-tree','--root','--no-commit-id','--name-only','-r',commit).splitlines()
            if any(not any(name.startswith(scope+'/') for scope in prefixes) for name in names):raise ValueError('Unpublished archive history has unrelated changes')
    if ahead and behind:
        git('merge-tree','--write-tree','HEAD','origin/main')
        git('merge','--no-edit','origin/main')
    else:git('merge','--ff-only','origin/main')
    for name,body in files.items():
        if not any(name.startswith(scope+'/') for scope in prefixes):raise ValueError('Evidence file outside approved show prefix')
        immutable(checked_path(repository,name),body)
    git('add','--',*prefixes)
    staged=git('diff','--cached','--name-only').splitlines()
    if any(name not in files for name in staged):raise ValueError('Unrelated staged archive changes')
    if staged:git('commit','-m','Retain verified Data observation evidence '+prefix.split('/')[3])
    commit=git('rev-parse','HEAD').strip();git('push','origin','HEAD:main');git('fetch','origin','main')
    remote_commit=git('rev-parse','origin/main').strip();git('merge-base','--is-ancestor',commit,remote_commit)
    with tarfile.open(fileobj=io.BytesIO(git('archive',remote_commit,'--',*prefixes,binary=True))) as archive:
        remaining=set(files)
        for member in archive:
            if member.name not in files:continue
            if not member.isfile():raise ValueError('Remote evidence object is not a file')
            body=archive.extractfile(member).read()
            if body!=files[member.name]:raise ValueError('Remote retained evidence differs')
            remaining.remove(member.name)
        if remaining:raise ValueError('Remote retained evidence is incomplete')
    return {'commit':commit,'verified_remote_commit':remote_commit,'path':prefix,'files':primary_count}

def outgoing_snapshot(runtime,key):
    """Validate exact saved native/readback evidence; never recapture a retry."""
    runtime=pathlib.Path(runtime);prefix_for(key)
    control=runtime/'outgoing-publications'/(key.replace('|','-')+'.json')
    if not control.exists():return None
    pin=json.loads(control.read_text(encoding='utf-8'))
    if pin.get('show-key')!=key or pin.get('folder')!='outgoing-'+key.replace('|','-'):raise ValueError('Pending outgoing owner mismatch')
    folder=checked_path(runtime,pin['folder'])
    from data_runner import archiver
    prefix,parts=archiver.build(folder)
    if prefix.parent.as_posix()!=prefix_for(key).rsplit('/',1)[0]:raise ValueError('Pending outgoing show scope mismatch')
    proof={name:{'bytes':len(body),'sha256':hashlib.sha256(body).hexdigest()} for name,body in parts.items()}
    if prefix.as_posix()!=pin.get('path') or proof!=pin.get('files'):raise ValueError('Pending outgoing evidence changed')
    return pin,folder,{(prefix/name).as_posix():body for name,body in parts.items()}

def pending_outgoing(runtime):
    output={};runtime=pathlib.Path(runtime)
    for control in sorted((runtime/'outgoing-publications').glob('*.json')):
        pin=json.loads(control.read_text(encoding='utf-8'));key=pin['show-key']
        if control.name!=key.replace('|','-')+'.json':raise ValueError('Pending outgoing control mismatch')
        saved=outgoing_snapshot(runtime,key);output[saved[0]['path']]=saved[2]
    return output

class EvidenceStore:
    def __init__(self,root,repository,publisher=publish_bundle):
        self.root=pathlib.Path(root);self.repository=pathlib.Path(repository);self.publisher=publisher
    def local_path(self,reference):return checked_path(self.root,reference['archive']['path'])
    def stage(self,show,rows):
        prefix=prefix_for(show);output=[]
        for row in rows:
            body=gzip.compress(packed({n:row[n] for n in ('retained-json','retained-sha256')}).encode('utf-8'),mtime=0)
            proof={'format':FORMAT,'path':prefix+'/parts/'+row['retained-sha256']+'.json.gz','sha256':hashlib.sha256(body).hexdigest(),'bytes':len(body)}
            reference={'key':row['collection-key'],'sha256':row['retained-sha256'],'archive':proof}
            if row.get('scope'):reference['scope']=row['scope']
            immutable(self.local_path(reference),body);self.read(reference);output.append(reference)
        return output
    def read(self,reference):
        proof=reference['archive'];path=proof['path']
        if not path.startswith('wef-data/shows/') or '/retained-evidence/parts/' not in path:raise ValueError('Unexpected evidence object location')
        local=self.local_path(reference)
        body=(local if local.exists() else checked_path(self.repository,path)).read_bytes()
        return archived_row(body,reference)
    def retained(self,repo,show):
        """All journaled archive bytes, including incomplete/retired generations.

        This is retention evidence only. It never joins the accepted revision chain.
        Pending source bodies remain in the existing retained-checkpoints archive.
        """
        prefix=prefix_for(show);files={};journals={}
        for key,row in sorted(repo.graph['runs'].items()):
            state=json.loads(row.get('state-json') or '{}')
            if state.get('show-key')!=show:continue
            references=[r for r in state.get('observations',[]) if 'archive' in r]
            from data_runner import saved_work_plans
            work=saved_work_plans(self.root.parent,state,key,repo)
            captures=[]
            folders={folder for folder,plan in work.values()}
            # Finished jobs leave the small journal, but their exact receipts
            # remain retention evidence under the same archive prefix.
            for folder in (self.root.parent/key.replace(':','_')/'day-work').glob('*'):
                plan=json.loads((folder/'plan.json').read_text(encoding='utf-8'))
                if digest(packed(plan))!=folder.name or plan['run-key']!=key or plan['show-key']!=show:raise ValueError('Checkpoint plan owner/hash mismatch')
                folders.add(folder)
            for folder in folders:
                captures.extend([folder/'plan.json',*sorted(folder.glob('block-*.json')),*sorted(folder.glob('reservation-*.json'))])
                if (folder/'progress.json').exists():captures.append(folder/'progress.json')
            reserved=state.get('reserved-class')
            if reserved:
                folder=self.root.parent/key.replace(':','_')/('class-batch-'+str(reserved['slot']))
                captures.extend(p for p in [folder/'reservation.json',folder/'responses.json',folder/'source-batch.json'] if p.exists())
            recovery=state.get('saved-checkpoint-recovery')
            if recovery:
                folder=checked_path(self.root.parent,recovery['path'])
                captures.extend(sorted(folder.glob('*.json')))
                from data_runner import read_saved_checkpoint
                owner=recovery['slot'].rsplit('|slot-',1)[0]
                original=read_saved_checkpoint(folder.parent,show,owner)
                if any(original[k]!=recovery[k] for k in ('reservation-sha256','responses-sha256')):raise ValueError('Retained original checkpoint changed')
                captures.extend(p for p in [folder.parent/'reservation.json',folder.parent/'responses.json'] if p.exists())
            for reservation in state.get('inventory-day-reservations',{}).values():
                capture=checked_path(self.root.parent,reservation['path'])/'responses.json'
                if capture.exists():captures.append(capture)
            directory=state.get('directory-reservation',{} ) or {}
            pointer=directory.get('capture') or state.get('directory-capture')
            if pointer:
                capture=checked_path(self.root.parent,pointer)
                if capture.exists():captures.append(capture)
            if not references and not captures:continue
            checkpoints={}
            for capture in sorted(set(captures)):
                body=capture.read_bytes();json.loads(body)
                compressed=gzip.compress(body,mtime=0);sha=hashlib.sha256(compressed).hexdigest()
                name=prefix+'/checkpoints/'+sha+'.json.gz';files[name]=compressed
                checkpoints[capture.relative_to(self.root.parent).as_posix()]={'path':name,'bytes':len(compressed),'sha256':sha}
            for reference in references:
                proof=reference['archive']
                if not proof['path'].startswith(prefix+'/parts/'):raise ValueError('Cross-show retained evidence')
                local=self.local_path(reference)
                body=(local if local.exists() else checked_path(self.repository,proof['path'])).read_bytes()
                archived_row(body,reference);files[proof['path']]=body
            journals[key]={'record-id':repo.inventory['runs'][key]['id'],'row':copy.deepcopy(row),'observations':copy.deepcopy(references)}
            if checkpoints:journals[key]['checkpoint-files']=checkpoints
        return journals,files
    def bundle(self,repo,state,runkey):
        """Preserve legacy native rows verbatim and all logical archive observations."""
        from retained_feed import collection_rows,run_rows,run_identity,history_archive
        from wef_data_handoff import revision_chain
        prefix=prefix_for(state['show-key']);files={};runs=copy.deepcopy(run_rows(repo,state['show-key']))
        runs[runkey].update({'status':'Verified completed revision','state-json':packed(state)})
        # The runner key retains the original start value; Airtable may round its
        # dateTime readback. Freeze that original spelling, without changing rows.
        if runkey.startswith(('adaptive-','archive-')):
            from wef_data_store import equivalent
            started=runkey.split('-',1)[1]
            if not equivalent(runs[runkey].get('started'),started,{'type':'dateTime'}):raise ValueError('Archive run start identity conflict')
            runs[runkey]['started']=started
        collections=collection_rows(repo,state)
        chain=revision_chain(runkey,runs,collections)
        manifests={'runs':{},'legacy-collections':{},'observations':[]}
        def mirror(kind,key,row):
            body=gzip.compress(packed(row).encode('utf-8'),mtime=0);name=prefix+'/'+kind+'/'+hashlib.sha256(body).hexdigest()+'.json.gz'
            files[name]=body;manifests[kind][key]={'path':name,'record-id':row['_record-id']}
        for journal,units in chain:
            key=next(k for k,r in runs.items() if r.get('state-json')==packed(journal))
            mirror('runs',key,{**runs[key],'_record-id':run_identity(repo,key,state['show-key'])})
            for reference in journal['observations']:
                manifests['observations'].append(reference)
                if 'archive' in reference:
                    proof=reference['archive'];local=self.local_path(reference)
                    body=(local if local.exists() else checked_path(self.repository,proof['path'])).read_bytes();archived_row(body,reference)
                    files[proof['path']]=body
        retained,parts=self.retained(repo,state['show-key']);files.update(parts)
        incomplete={k:v for k,v in retained.items() if k not in manifests['runs']}
        if incomplete:manifests['incomplete-runs']=incomplete
        # Include unreferenced trial readback rows as well; none are deleted or rewritten.
        for key,row in repo.graph['collections'].items():
            if ['shows',state['show-key']] in row.get('show',{}).get('$links',[]):mirror('legacy-collections',key,{**row,'_record-id':repo.inventory['collections'][key]['id']})
        previous=history_archive(repo,state['show-key'])
        if previous:
            for key,entry in previous.manifest['records']['legacy-collections'].items():
                if key in manifests['legacy-collections']:continue
                previous.read_raw('legacy-collections',key)
                files[entry['path']]=previous.object_bytes('legacy-collections',key)
                manifests['legacy-collections'][key]=copy.deepcopy(entry)
        manifest={'format':FORMAT,'show-key':state['show-key'],'run-key':runkey,'state-sha256':digest(packed(state)),
                  'authority':state['authority'],'records':manifests,'files':{n:{'bytes':len(b),'sha256':hashlib.sha256(b).hexdigest()} for n,b in sorted(files.items())}}
        name=prefix+'/revisions/'+manifest['state-sha256']+'.json';files[name]=packed(manifest).encode('utf-8')
        # Keep interrupted same-run candidate bytes without rewriting the frozen
        # revision manifest or allowing unrelated dirty files into publication.
        attempts={}
        for path in sorted((self.repository/prefix/'runs').glob('*.json.gz')):
            relative=path.relative_to(self.repository).as_posix()
            if relative in files:continue
            body=checked_path(self.repository,relative).read_bytes()
            try:row=json.loads(gzip.decompress(body))
            except (ValueError,OSError,EOFError):continue
            if not isinstance(row,dict) or row.get('run-key')!=runkey:continue
            if path.name!=hashlib.sha256(body).hexdigest()+'.json.gz':raise ValueError('Archive attempt content address mismatch')
            prior=json.loads(row['state-json'])
            if row.get('_record-id')!=repo.inventory['runs'][runkey]['id'] or prior.get('run-record-id')!=state.get('run-record-id') or prior.get('show-key')!=state['show-key'] or prior.get('sequence')!=state['sequence'] or prior.get('observations')!=state['observations'] or prior.get('predecessor')!=state.get('predecessor') or prior.get('phase')!='Complete':raise ValueError('Archive attempt identity mismatch')
            files[relative]=body;attempts[relative]={'bytes':len(body),'sha256':hashlib.sha256(body).hexdigest(),'record-id':row['_record-id'],'candidate-state-sha256':digest(row['state-json'])}
        if attempts:
            note=packed({'format':'wef-data-unaccepted-archive-attempts-v1','classification':'Unaccepted prior archive publication candidates','show-key':state['show-key'],'run-key':runkey,'files':attempts}).encode('utf-8')
            files[prefix+'/unaccepted-attempts/'+hashlib.sha256(note).hexdigest()+'.json']=note
        for path in sorted((self.repository/prefix/'unaccepted-attempts').glob('*.json')):
            relative=path.relative_to(self.repository).as_posix()
            if relative in files:continue
            body=checked_path(self.repository,relative).read_bytes()
            try:note=json.loads(body)
            except ValueError:continue
            if not isinstance(note,dict) or note.get('run-key')!=runkey:continue
            if path.name!=hashlib.sha256(body).hexdigest()+'.json' or note.get('format')!='wef-data-unaccepted-archive-attempts-v1' or note.get('show-key')!=state['show-key']:raise ValueError('Archive attempt note identity mismatch')
            for saved,proof in note['files'].items():
                if not saved.startswith(prefix+'/runs/') or saved not in files or proof.get('record-id')!=repo.inventory['runs'][runkey]['id'] or proof.get('sha256')!=hashlib.sha256(files[saved]).hexdigest() or proof.get('bytes')!=len(files[saved]):raise ValueError('Archive attempt note evidence mismatch')
            files[relative]=body
        return prefix,name,files
    def publish(self,repo,state,runkey):
        prefix,manifest,files=self.bundle(repo,state,runkey)
        pending=pending_outgoing(self.root.parent)
        result=self.publisher(self.repository,prefix,files,**({'pending_bundles':pending} if pending else {}))
        if not result.get('verified_remote_commit'):raise ValueError('Missing remote evidence verification')
        return {**result,'manifest':manifest,'state-sha256':digest(packed(state)),'format':FORMAT}
