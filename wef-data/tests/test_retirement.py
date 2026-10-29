import copy,json,pathlib,re,sys,tempfile,unittest
from unittest.mock import patch
sys.path.insert(0,str(pathlib.Path(__file__).resolve().parents[1]))
from test_scheduled_contract import API,SCHEMA,AUTHORED,CONTEXT
from data_runner import Repository,refs
from data_contract import packed,digest
from wef_data_airtable import TransportDeferred
import retirement

OLD='15|200000075';CURRENT=CONTEXT['show-key']

class DeleteAPI(API):
    def __init__(self):super().__init__();self.deletes=[];self.lose=False
    def records(self,base,table,fields=None,formula=None):
        if formula and 'RECORD_ID()' in formula:
            ids=set(json.loads(s) for s in re.findall(r'RECORD_ID\(\)=("[^"]+")',formula))
            return copy.deepcopy([r for r in self.db.get(table,[]) if r['id'] in ids])
        return super().records(base,table,fields,formula)
    def request(self,method,base,table,params=None,body=None):
        if method!='DELETE':return super().request(method,base,table,params,body)
        ids=params['records[]'];assert len(ids)<=10
        self.deletes.append((table,list(ids)))
        self.db[table]=[r for r in self.db[table] if r['id'] not in ids]
        for rows in self.db.values():
            for row in rows:
                for field,value in row['fields'].items():
                    if isinstance(value,list):row['fields'][field]=[v for v in value if v not in ids]
        if self.lose:self.lose=False;raise TransportDeferred('DELETE',base,table)
        return {'records':[{'id':v,'deleted':True} for v in ids]}

def setup():
    api=DeleteAPI();graph={t:{} for t in SCHEMA['tables']}
    def add(table,key,**fields):graph[table][key]={next(f['name'] for f in SCHEMA['tables'][table]['fields'] if f['id']==SCHEMA['tables'][table]['primaryFieldId']):key,**fields}
    for old,show in [(True,OLD),(False,CURRENT)]:
        suffix='old' if old else 'current';run='run-'+suffix;day=show+'|2026-10-29';ring=day+'|1';group=ring+'|group-3';cls=ring+'|2';entry=('200000075|20' if old else '200000076|21')
        horse='100' if old else '200';trainer='600' if old else '800';owner='700' if old else '900'
        add('runs',run,status='Verified completed revision',**{'state-json':packed({'show-key':show})})
        add('shows',show,**{'last-run':refs('runs',run)})
        add('days',day,show=refs('shows',show),**{'last-run':refs('runs',run)})
        add('rings',ring,show=refs('shows',show),day=refs('days',day),**{'last-run':refs('runs',run)})
        add('groups',group,ring=refs('rings',ring))
        add('classes',cls,show=refs('shows',show),day=refs('days',day),ring=refs('rings',ring),group=refs('groups',group),**{'last-run':refs('runs',run)})
        add('schedules','schedule-'+suffix,show=refs('shows',show),day=refs('days',day),ring=refs('rings',ring),group=refs('groups',group),**{'class':refs('classes',cls)})
        add('horses',horse,**{'horse-id':int(horse),'horse-name':suffix})
        add('riders','500',**{'rider-id':500,'rider-name':'shared'})
        add('trainers',trainer,**{'trainer-id':int(trainer)})
        add('owners',owner,**{'owner-id':owner})
        add('entries',entry,show=refs('shows',show),horse=refs('horses',horse),riders=refs('riders','500'),trainers=refs('trainers',trainer),owners=refs('owners',owner))
        add('trips','trip-'+suffix,entry=refs('entries',entry),horse=refs('horses',horse),rider=refs('riders','500'),trainer=refs('trainers',trainer),owner=refs('owners',owner),**{'class':refs('classes',cls),'last-run':refs('runs',run)})
        add('results','result-'+suffix,trip=refs('trips','trip-'+suffix),entry=refs('entries',entry),**{'class':refs('classes',cls),'last-run':refs('runs',run)})
        add('work-items',cls,**{'class':refs('classes',cls),'last-run':refs('runs',run)})
        add('slots','slot-'+suffix,run=refs('runs',run),**{'work-items':refs('work-items',cls)})
        add('collections','collection-'+suffix,run=refs('runs',run),show=refs('shows',show),day=refs('days',day),ring=refs('rings',ring),**{'class':refs('classes',cls)})
        add('requests','request-'+suffix,run=refs('runs',run),slot=refs('slots','slot-'+suffix),collection=refs('collections','collection-'+suffix),**{'work-item':refs('work-items',cls)})
    add('lanes','inventory',name='global')
    identities={t:{k:'rec'+str(i)+'X'+str(n) for n,k in enumerate(rows)} for i,(t,rows) in enumerate(graph.items())}
    for table,rows in graph.items():
        fields={f['name']:f['id'] for f in SCHEMA['tables'][table]['fields']}
        api.db[SCHEMA['tables'][table]['id']]=[]
        for key,row in rows.items():
            value={fields[n]:[identities[t][k] for t,k in v['$links']] if isinstance(v,dict) and '$links' in v else v for n,v in row.items()}
            api.db[SCHEMA['tables'][table]['id']].append({'id':identities[table][key],'fields':value})
    repo=Repository(api,SCHEMA,AUTHORED);old=repo.scoped(OLD);current=repo.scoped(CURRENT)
    facts={t:{r[retirement.TABLE_KEYS[t]]:r for r in rows} for t,rows in old.items()}
    protected={t:{r[retirement.TABLE_KEYS[t]] for r in rows} for t,rows in current.items()}
    items=[{'table':t,'key':k,'record-id':identities[t][k]} for t,rows in facts.items() for k in rows if t!='lanes' and k not in protected[t]]
    review={'target-show':OLD,'candidates':items,'blockers':[],'business-writes':0,'archive-receipt':{'path':'wef-data/shows/15/200000075/revision','verified_remote_commit':'pinned'}}
    approval={'approval':retirement.APPROVAL,'manifest-sha256':digest(packed(review)),'manifest':review}
    return api,repo,facts,identities,approval

class RetirementTests(unittest.TestCase):
    def execute(self,repo,folder,facts,identities,approval):
        root=pathlib.Path(folder);(root/'retirement-approvals').mkdir(exist_ok=True)
        (root/'retirement-approvals/75.json').write_text(packed(approval),encoding='utf-8')
        with patch.object(retirement,'archive',return_value=(facts,identities)),patch.object(retirement,'season_ack'):
            return retirement.retire(repo,root)
    def test_all_old_only_rows_removed_child_first_shared_and_current_ids_preserved_repeat_noop(self):
        api,repo,facts,ids,approval=setup();protected={t:{k:v for k,v in rows.items() if not any(i['table']==t and i['key']==k for i in approval['manifest']['candidates'])} for t,rows in ids.items()}
        with tempfile.TemporaryDirectory() as folder:
            result=self.execute(repo,folder,facts,ids,approval)
            self.assertEqual(result[0]['records-removed'],len(approval['manifest']['candidates']))
            self.assertNotIn(OLD,repo.graph['shows'])
            for table,rows in protected.items():
                for key,identity in rows.items():self.assertEqual(repo.inventory[table][key]['id'],identity)
            order=[t for t,records in api.deletes]
            self.assertLess(order.index(SCHEMA['tables']['requests']['id']),order.index(SCHEMA['tables']['collections']['id']))
            self.assertLess(order.index(SCHEMA['tables']['trips']['id']),order.index(SCHEMA['tables']['classes']['id']))
            calls=len(api.deletes);self.assertIsNone(self.execute(repo,folder,facts,ids,approval));self.assertEqual(len(api.deletes),calls)
    def test_lost_delete_response_reconciles_ids_and_never_deletes_the_batch_again(self):
        api,repo,facts,ids,approval=setup();api.lose=True
        with tempfile.TemporaryDirectory() as folder:
            with self.assertRaises(TransportDeferred):self.execute(repo,folder,facts,ids,approval)
            first=copy.deepcopy(api.deletes[0]);repo=Repository(api,SCHEMA,AUTHORED)
            result=self.execute(repo,folder,facts,ids,approval)
            self.assertEqual(result[0]['records-removed'],len(approval['manifest']['candidates']))
            self.assertEqual(api.deletes.count(first),1)
    def test_current_show_target_and_missing_season_ack_never_delete(self):
        for condition in ('current','season'):
            api,repo,facts,ids,approval=setup()
            if condition=='current':api.context['records'][0]['show-key']=OLD
            with self.subTest(condition=condition),tempfile.TemporaryDirectory() as folder:
                root=pathlib.Path(folder);(root/'retirement-approvals').mkdir();(root/'retirement-approvals/75.json').write_text(packed(approval))
                with patch.object(retirement,'archive',return_value=(facts,ids)),patch.object(retirement,'season_ack',side_effect=ValueError('Missing Season ack') if condition=='season' else None):
                    with self.assertRaises(ValueError):retirement.retire(repo,root)
                self.assertEqual(api.deletes,[])
    def test_tampered_approval_or_changed_archived_row_fails_before_first_delete(self):
        for condition in ('approval','row','id'):
            api,repo,facts,ids,approval=setup()
            if condition=='approval':approval['manifest']['candidates'].pop()
            if condition=='row':repo.graph['horses']['100']['horse-name']='Unarchived edit'
            if condition=='id':ids['horses']['100']='recDifferent'
            with self.subTest(condition=condition),tempfile.TemporaryDirectory() as folder:
                with self.assertRaises(ValueError):self.execute(repo,folder,facts,ids,approval)
                self.assertEqual(api.deletes,[])
    def test_newly_current_entity_is_retained_and_not_reidentified(self):
        api,repo,facts,ids,approval=setup()
        entry='200000076|21';field=repo.store.fields['entries']['horse']['id']
        row=next(r for r in api.db[SCHEMA['tables']['entries']['id']] if r['id']==ids['entries'][entry]);row['fields'][field]=[ids['horses']['100']]
        repo.reload(refresh=True)
        with tempfile.TemporaryDirectory() as folder:
            result=self.execute(repo,folder,facts,ids,approval)
            self.assertEqual(result[0]['retained-newly-current'],1);self.assertEqual(repo.inventory['horses']['100']['id'],ids['horses']['100'])
            self.assertEqual(repo.graph['entries'][entry]['horse'],refs('horses','100'))
    def test_active_source_checkpoint_defers_retirement(self):
        api,repo,facts,ids,approval=setup()
        for name in ('active.json','pending.json'):
            with self.subTest(name=name),tempfile.TemporaryDirectory() as folder:
                pathlib.Path(folder,name).write_text('{}')
                self.assertIsNone(self.execute(repo,folder,facts,ids,approval));self.assertEqual(api.deletes,[])
    def test_owned_current_pending_checkpoint_and_planned_entities_are_preserved(self):
        api,repo,facts,ids,approval=setup()
        with tempfile.TemporaryDirectory() as folder:
            root=pathlib.Path(folder)
            active={'run-key':'run-current','reserved-run':{'state-json':packed({'show-key':CURRENT})}}
            pending={'run-key':'run-current','show-key':CURRENT,'graph':{'entries':[{'entry-key':'200000076|99','horse':refs('horses','100')}]}}
            for name,value in [('active.json',active),('pending.json',pending)]:root.joinpath(name).write_text(packed(value))
            root.joinpath('deferred.json').write_text(packed({'operation':{'method':'POST','write-outcome':'Unknown','body':{'reserved':'current'}}}))
            before={name:root.joinpath(name).read_bytes() for name in ('active.json','pending.json','deferred.json')}
            result=self.execute(repo,folder,facts,ids,approval)
            self.assertEqual(result[0]['retained-newly-current'],1)
            self.assertEqual(repo.inventory['horses']['100']['id'],ids['horses']['100'])
            self.assertEqual(before,{name:root.joinpath(name).read_bytes() for name in before})
    def test_multiple_batches_and_process_interruption_preserve_checkpoint_and_remaining_rows(self):
        api,repo,facts,ids,approval=setup()
        table=SCHEMA['tables']['entries'];primary=table['primaryFieldId']
        template=copy.deepcopy(next(r for r in api.db[table['id']] if r['id']==ids['entries']['200000075|20']))
        for number in range(25):
            key='200000075|'+str(1000+number);identity='recExtra'+str(number)
            row=copy.deepcopy(template);row['id']=identity;row['fields'][primary]=key
            api.db[table['id']].append(row);ids['entries'][key]=identity
        repo.reload(refresh=True)
        facts={t:{r[retirement.TABLE_KEYS[t]]:r for r in rows} for t,rows in repo.scoped(OLD).items()}
        prior={i['record-id'] for i in approval['manifest']['candidates']}
        approval['manifest']['candidates'].extend({'table':'entries','key':k,'record-id':v} for k,v in ids['entries'].items() if k.startswith('200000075|') and v not in prior)
        approval['manifest-sha256']=digest(packed(approval['manifest']))
        original=api.request;calls=[0]
        def interrupted(method,*args,**kwargs):
            if method=='DELETE':
                calls[0]+=1
                if calls[0]==2:raise KeyboardInterrupt()
            return original(method,*args,**kwargs)
        with tempfile.TemporaryDirectory() as folder:
            with patch.object(api,'request',side_effect=interrupted):
                with self.assertRaises(KeyboardInterrupt):self.execute(repo,folder,facts,ids,approval)
            first=copy.deepcopy(api.deletes[0]);repo=Repository(api,SCHEMA,AUTHORED)
            result=self.execute(repo,folder,facts,ids,approval)
            self.assertEqual(result[0]['records-removed'],len(approval['manifest']['candidates']))
            self.assertEqual(api.deletes.count(first),1)
            batches=[v for t,v in api.deletes if t==table['id']]
            self.assertEqual([len(v) for v in batches],[10,10,6])
    def test_unknown_delete_outcome_checks_record_id_not_only_its_old_key(self):
        api,repo,facts,ids,approval=setup();original=api.request
        def not_sent(method,*args,**kwargs):
            if method=='DELETE':raise TransportDeferred('DELETE',SCHEMA['base'],args[1])
            return original(method,*args,**kwargs)
        with tempfile.TemporaryDirectory() as folder:
            with patch.object(api,'request',side_effect=not_sent):
                with self.assertRaises(TransportDeferred):self.execute(repo,folder,facts,ids,approval)
            state=json.loads(pathlib.Path(folder,'retirements/15-200000075.json').read_text())
            pending=state['pending'];definition=SCHEMA['tables'][pending['table']]
            row=next(r for r in api.db[definition['id']] if r['id']==pending['ids'][0])
            row['fields'][definition['primaryFieldId']]='unapproved-renamed-key'
            repo=Repository(api,SCHEMA,AUTHORED)
            with self.assertRaisesRegex(ValueError,'record key changed'):self.execute(repo,folder,facts,ids,approval)
            self.assertEqual(api.deletes,[])
    def test_current_show_change_before_first_batch_blocks_delete(self):
        api,repo,facts,ids,approval=setup();original=api.canonical_context;calls=[0]
        def changed():
            calls[0]+=1
            value=original()
            if calls[0]>1:value['records'][0]['show-key']=OLD
            return value
        with tempfile.TemporaryDirectory() as folder,patch.object(api,'canonical_context',side_effect=changed):
            with self.assertRaisesRegex(ValueError,'canonical current'):self.execute(repo,folder,facts,ids,approval)
            self.assertEqual(api.deletes,[])
    def test_resume_rechecks_new_current_references_before_deleting_parent(self):
        api,repo,facts,ids,approval=setup();original=api.request
        def paused(method,*args,**kwargs):
            if method=='DELETE':raise KeyboardInterrupt()
            return original(method,*args,**kwargs)
        with tempfile.TemporaryDirectory() as folder:
            with patch.object(api,'request',side_effect=paused):
                with self.assertRaises(KeyboardInterrupt):self.execute(repo,folder,facts,ids,approval)
            field=repo.store.fields['entries']['horse']['id'];row=next(r for r in api.db[SCHEMA['tables']['entries']['id']] if r['id']==ids['entries']['200000076|21'])
            row['fields'][field]=[ids['horses']['100']]
            repo=Repository(api,SCHEMA,AUTHORED)
            result=self.execute(repo,folder,facts,ids,approval)
            self.assertEqual(result[0]['retained-newly-current'],1)
            self.assertEqual(repo.inventory['horses']['100']['id'],ids['horses']['100'])
            self.assertEqual(repo.graph['entries']['200000076|21']['horse'],refs('horses','100'))
    def test_retirement_get_and_delete_deferral_never_overwrite_source_post_hold(self):
        from data_runner import run
        for method in ('GET','DELETE'):
            api,repo,facts,ids,approval=setup()
            with self.subTest(method=method),tempfile.TemporaryDirectory() as folder:
                root=pathlib.Path(folder);(root/'schema.json').write_text(packed(SCHEMA));(root/'authored.json').write_text(packed(AUTHORED))
                original={'operation':{'method':'POST','base':SCHEMA['base'],'table':SCHEMA['tables']['trips']['id'],'body':{'records':[{'fields':{'fixture':'reserved-key'}}]},'write-outcome':'Unknown'},'next-due':0,'failures':2}
                (root/'deferred.json').write_text(packed(original));before=(root/'deferred.json').read_bytes()
                with patch('data_runner.Airtable',return_value=api),patch.object(retirement,'retire',side_effect=TransportDeferred(method,SCHEMA['base'],SCHEMA['tables']['entries']['id'])):
                    result=run(root,root/'schema.json',root/'authored.json',root)
                self.assertEqual(result['state'],'Deferred')
                self.assertEqual((root/'deferred.json').read_bytes(),before)
                self.assertTrue((root/'retirement-deferred.json').exists())
                self.assertFalse((root/'stopped.json').exists())
    def test_retirement_repository_read_failure_preserves_source_hold(self):
        from data_runner import run
        api,repo,facts,ids,approval=setup()
        with tempfile.TemporaryDirectory() as folder:
            root=pathlib.Path(folder);(root/'schema.json').write_text(packed(SCHEMA));(root/'authored.json').write_text(packed(AUTHORED))
            (root/'retirement-approvals').mkdir();(root/'retirement-approvals/75.json').write_text(packed(approval))
            held={'operation':{'method':'POST','write-outcome':'Unknown','body':{'records':[{'reserved':'unchanged'}]}},'next-due':0}
            (root/'deferred.json').write_text(packed(held));before=(root/'deferred.json').read_bytes()
            with patch('data_runner.Airtable',return_value=api),patch('data_runner.Repository',side_effect=TransportDeferred('GET',SCHEMA['base'],SCHEMA['tables']['runs']['id'])):
                result=run(root,root/'schema.json',root/'authored.json',root)
            self.assertEqual(result['state'],'Deferred');self.assertEqual((root/'deferred.json').read_bytes(),before)
            self.assertTrue((root/'retirement-deferred.json').exists())
    def test_symbolic_link_change_is_not_hidden_by_date_equivalence(self):
        fields={'horse':{'type':'multipleRecordLinks'},'last-success':{'type':'dateTime'}}
        a={'horse':refs('horses','100'),'last-success':'2026-10-29T12:00:00Z'}
        b={'horse':refs('horses','200'),'last-success':'2026-10-29T12:00:00.000Z'}
        self.assertFalse(retirement.same(a,b,fields));b['horse']=a['horse'];self.assertTrue(retirement.same(a,b,fields))
    def test_python_worker_exclusive_lock_is_held_until_context_exit(self):
        with tempfile.TemporaryDirectory() as folder:
            with retirement.worker_lock(folder):
                with self.assertRaises(retirement.WorkerBusy):
                    with retirement.worker_lock(folder):self.fail('Concurrent worker admitted')
            with retirement.worker_lock(folder):pass
    def test_archive_requires_published_ancestor_and_exact_git_bytes(self):
        api,repo,facts,ids,approval=setup();review=approval['manifest']
        with tempfile.TemporaryDirectory() as folder:
            runtime=pathlib.Path(folder);snapshot=runtime/'archive-repository'/review['archive-receipt']['path'];snapshot.mkdir(parents=True)
            files={}
            for table,rows in facts.items():
                name='tables/'+table+'.json.gz';body=__import__('gzip').compress(json.dumps([{'id':ids[table][key],'fields':value} for key,value in rows.items()]).encode())
                file=snapshot/name;file.parent.mkdir(exist_ok=True);file.write_bytes(body);files[name]={}
            (snapshot/'manifest.json').write_text('{}');manifest={'show_key':OLD,'files':files};calls=[]
            def git(args,**kwargs):
                calls.append(args[3:])
                command=args[3]
                body=(b'https://github.com/sportdogfood/ringstatus-data.git\n' if command=='remote' else (snapshot/args[4].split(':',1)[1].removeprefix(review['archive-receipt']['path']+'/')).read_bytes() if command=='show' else b'')
                return __import__('subprocess').CompletedProcess(args,0,stdout=body)
            with patch.object(retirement,'verify_snapshot',return_value=(manifest,facts)),patch.object(retirement.subprocess,'run',side_effect=git):
                actual,record_map=retirement.archive(runtime,review)
            self.assertEqual(record_map,{t:{k:ids[t][k] for k in rows} for t,rows in facts.items()})
            self.assertIn(['fetch','origin','main'],calls)
            self.assertIn(['merge-base','--is-ancestor','pinned','origin/main'],calls)
            def changed(args,**kwargs):
                if args[3]=='show':return __import__('subprocess').CompletedProcess(args,0,stdout=b'changed')
                return git(args,**kwargs)
            with patch.object(retirement,'verify_snapshot',return_value=(manifest,facts)),patch.object(retirement.subprocess,'run',side_effect=changed):
                with self.assertRaisesRegex(ValueError,'Published retirement archive differs'):retirement.archive(runtime,review)

if __name__=='__main__':unittest.main()
