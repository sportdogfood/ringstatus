"""Latest successful schedule/class payloads; Git supplies the saver diff."""
import copy,datetime,json,pathlib,subprocess
from fast_collector import atomic,capture_safe

BRANCH='codex/fast-snapshots'
REMOTE='https://github.com/sportdogfood/ringstatus-data'

class Snapshots:
    def __init__(self,root,repository=None,allowed_remote=REMOTE):
        self.root=pathlib.Path(root)
        repository=pathlib.Path(repository or self.root.parent.parent/'ringstatus-data')
        self.checkout=self.root/'runtime/fast/git-latest'
        def repo_git(*args):return self.git_at(repository,*args)
        actual=repo_git('remote','get-url','origin').strip().removesuffix('.git')
        if actual.replace('\\','/')!=allowed_remote.replace('\\','/').removesuffix('.git'):raise ValueError('Wrong schedule/class data repository')
        if not self.checkout.exists():
            self.checkout.parent.mkdir(parents=True,exist_ok=True)
            refs=repo_git('for-each-ref','--format=%(refname)','refs/heads/'+BRANCH).strip()
            if not refs:
                repo_git('fetch','origin')
                remote_ref=repo_git('for-each-ref','--format=%(refname)','refs/remotes/origin/'+BRANCH).strip()
                if remote_ref:repo_git('branch',BRANCH,'origin/'+BRANCH)
                else:
                    tree=self.git_at(repository,'mktree',input='').strip()
                    commit=self.git_at(repository,'commit-tree',tree,input='Schedule and class snapshots\n').strip()
                    repo_git('branch',BRANCH,commit)
            repo_git('worktree','add',str(self.checkout),BRANCH)
        if self.git('branch','--show-current').strip()!=BRANCH:raise ValueError('Wrong snapshot branch')

    @staticmethod
    def git_at(path,*args,input=None):
        result=subprocess.run(['git','-C',str(path),*args],input=input,capture_output=True,text=True,encoding='utf-8',timeout=60)
        if result.returncode:raise RuntimeError('Snapshot Git '+args[0]+' failed: '+result.stderr.strip())
        return result.stdout

    def git(self,*args,**kwargs):return self.git_at(self.checkout,*args,**kwargs)

    @staticmethod
    def location(plan,operation):
        customer,show=plan['show-key'].split('|');day=plan['day']
        if not customer.isdecimal() or not show.isdecimal() or str(datetime.date.fromisoformat(day))!=day:
            raise ValueError('Invalid snapshot scope')
        if operation['kind']=='schedule':return f'schedules/{customer}/{show}/{day}.json'
        key=operation['class-key'].split('|')
        if len(key)!=5 or key[:3]!=[customer,show,day] or not all(p.isdecimal() for p in key[3:]):raise ValueError('Class snapshot scope mismatch')
        return f'classes/{customer}/{show}/{day}/{key[3]}-{key[4]}.json'

    def prepare(self,plan,baseline=None):
        paths={};payloads={}
        index_file=self.checkout/'index.json'
        catalog=json.loads(index_file.read_text(encoding='utf-8')) if index_file.exists() else {}
        scope=plan['show-key']+'|'+plan['day']
        latest=catalog.get(scope,{'show-key':plan['show-key'],'day':plan['day'],'classes':{}})
        for index,operation in enumerate(plan['operations']):
            if operation['kind'] not in ('schedule','class'):continue
            name=self.location(plan,operation);paths[index]=name
            if operation['kind']=='schedule':
                body=copy.deepcopy({'graph':operation['graph'],'priorities':operation['priorities']})
                for row in body['graph']['schedules'].values():
                    source=json.loads(row['source-json']);source.pop('_',None)
                    row['source-json']=json.dumps(source,sort_keys=True,separators=(',',':'),ensure_ascii=False)
                latest['schedule']={'path':name,'observed-at':operation['at']}
            else:
                captured=json.loads(pathlib.Path(operation['capture']).read_text(encoding='utf-8'))
                if not captured['ok']:raise ValueError('Failed response cannot replace a successful Git payload')
                body=captured['payload']
                latest['classes'][operation['class-key']]={'path':name,'observed-at':captured['metric']['completed']}
            payloads[name]=capture_safe(body)
        for name,body in payloads.items():
            target=self.checkout/name;target.parent.mkdir(parents=True,exist_ok=True)
            target.write_text(json.dumps(body,sort_keys=True,indent=2,ensure_ascii=False)+'\n',encoding='utf-8')
        latest.update({'run-key':plan['run-key'],'checked-at':plan.get('completed',plan['started']),'errors':capture_safe(plan.get('item-errors',[]))})
        catalog[scope]=latest
        atomic(index_file,capture_safe(catalog))
        self.git('add','--',*payloads,'index.json')
        if self.git('diff','--cached','--name-only').strip():self.git('commit','-m','Latest schedule/classes '+plan['show-key']+' '+plan['day'])
        commit=self.git('rev-parse','HEAD').strip()
        changed=set(self.git('diff','--name-only',baseline,commit,'--',*payloads).splitlines()) if baseline and payloads else set(payloads)
        diff=self.git('diff',baseline,commit,'--',*payloads) if baseline and payloads else ''
        self.git('push','origin','HEAD:refs/heads/'+BRANCH)
        return {'commit':commit,'branch':BRANCH,'changed':sorted(changed),'paths':paths,'diff':diff}

    def read(self,commit,name):return json.loads(self.git('show',commit+':'+name))
