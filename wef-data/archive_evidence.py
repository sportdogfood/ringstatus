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

def publish_bundle(repository,prefix,files,allowed_remotes=REMOTES):
    """Publish without rewriting history; reconcile only this producer's local commits."""
    repository=pathlib.Path(repository)
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
    if ahead and behind:
        # Inspect every unpublished commit, including changes later reverted.
        # A merge-tree preview rejects conflicts without changing the checkout.
        for commit in git('rev-list','origin/main..HEAD').splitlines():
            names=git('diff-tree','--root','--no-commit-id','--name-only','-r',commit).splitlines()
            if any(not name.startswith(prefix+'/') for name in names):raise ValueError('Unpublished archive history has unrelated changes')
        git('merge-tree','--write-tree','HEAD','origin/main')
        git('merge','--no-edit','origin/main')
    else:git('merge','--ff-only','origin/main')
    for name,body in files.items():
        if not name.startswith(prefix+'/'):raise ValueError('Evidence file outside approved show prefix')
        immutable(checked_path(repository,name),body)
    git('add','--',prefix)
    staged=git('diff','--cached','--name-only').splitlines()
    if any(name not in files for name in staged):raise ValueError('Unrelated staged archive changes')
    if staged:git('commit','-m','Retain verified Data observation evidence '+prefix.split('/')[3])
    commit=git('rev-parse','HEAD').strip();git('push','origin','HEAD:main');git('fetch','origin','main')
    remote_commit=git('rev-parse','origin/main').strip();git('merge-base','--is-ancestor',commit,remote_commit)
    with tarfile.open(fileobj=io.BytesIO(git('archive',remote_commit,'--',prefix,binary=True))) as archive:
        remaining=set(files)
        for member in archive:
            if member.name not in files:continue
            if not member.isfile():raise ValueError('Remote evidence object is not a file')
            body=archive.extractfile(member).read()
            if body!=files[member.name]:raise ValueError('Remote retained evidence differs')
            remaining.remove(member.name)
        if remaining:raise ValueError('Remote retained evidence is incomplete')
    return {'commit':commit,'verified_remote_commit':remote_commit,'path':prefix,'files':len(files)}

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
            if not references:continue
            for reference in references:
                proof=reference['archive']
                if not proof['path'].startswith(prefix+'/parts/'):raise ValueError('Cross-show retained evidence')
                local=self.local_path(reference)
                body=(local if local.exists() else checked_path(self.repository,proof['path'])).read_bytes()
                archived_row(body,reference);files[proof['path']]=body
            journals[key]={'record-id':repo.inventory['runs'][key]['id'],'row':copy.deepcopy(row),'observations':copy.deepcopy(references)}
        return journals,files
    def bundle(self,repo,state,runkey):
        """Preserve legacy native rows verbatim and all logical archive observations."""
        from retained_feed import collection_rows
        from wef_data_handoff import revision_chain
        prefix=prefix_for(state['show-key']);files={};runs=copy.deepcopy(repo.graph['runs'])
        runs[runkey].update({'status':'Verified completed revision','state-json':packed(state)})
        collections=collection_rows(repo,state)
        chain=revision_chain(runkey,runs,collections)
        manifests={'runs':{},'legacy-collections':{},'observations':[]}
        def mirror(kind,key,row):
            body=gzip.compress(packed(row).encode('utf-8'),mtime=0);name=prefix+'/'+kind+'/'+hashlib.sha256(body).hexdigest()+'.json.gz'
            files[name]=body;manifests[kind][key]={'path':name,'record-id':row['_record-id']}
        for journal,units in chain:
            key=next(k for k,r in runs.items() if r.get('state-json')==packed(journal))
            mirror('runs',key,{**runs[key],'_record-id':repo.inventory['runs'][key]['id']})
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
        manifest={'format':FORMAT,'show-key':state['show-key'],'run-key':runkey,'state-sha256':digest(packed(state)),
                  'authority':state['authority'],'records':manifests,'files':{n:{'bytes':len(b),'sha256':hashlib.sha256(b).hexdigest()} for n,b in sorted(files.items())}}
        name=prefix+'/revisions/'+manifest['state-sha256']+'.json';files[name]=packed(manifest).encode('utf-8')
        return prefix,name,files
    def publish(self,repo,state,runkey):
        prefix,manifest,files=self.bundle(repo,state,runkey)
        result=self.publisher(self.repository,prefix,files)
        if not result.get('verified_remote_commit'):raise ValueError('Missing remote evidence verification')
        return {**result,'manifest':manifest,'state-sha256':digest(packed(state)),'format':FORMAT}
