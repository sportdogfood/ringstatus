"""Bounded capture/reconciliation using offline source and persistence boundaries."""
import copy,json,pathlib,sys,tempfile,threading,time,unittest
from unittest.mock import patch
sys.path.insert(0,str(pathlib.Path(__file__).resolve().parents[1]))
from test_scheduled_contract import API,SCHEMA,AUTHORED,Source,seed,SHOW,CONTEXT
from data_runner import Runner,Repository,refs

class DirectoryReconciliationTests(unittest.TestCase):
    def test_large_initial_directory_is_bounded_and_resumes_without_refetch(self):
        class Large(Source):
            def fetch(self,route,params):
                payload,metric=super().fetch(route,params)
                if route=='/entries':payload.update(total_entries=27,page_size=1)
                return payload,metric
        api=API();seed(api);source=Large()
        with tempfile.TemporaryDirectory() as folder:
            runner=Runner(Repository(api,SCHEMA,AUTHORED),source,folder,folder);runner.begin(SHOW,{})
            runner.repo.stage({'runs':[{'run-key':runner.runkey}],'shows':[{k:CONTEXT[k] for k in ('show-key','show-id','customer-id','show-name','start-date','end-date','selected-date')}]})
            runner.inventory(CONTEXT);source.calls=[]
            runner.collect_directory(CONTEXT)
            self.assertLessEqual(len(source.calls),12);self.assertFalse(runner.state['directory']['complete'])
            first=len(runner.repo.graph['entries']);self.assertLessEqual(first,12)
            runner.state['directory-dispatch-run']=None;runner.state['directory-next-due']=0
            before=len(source.calls);runner.collect_directory(CONTEXT)
            self.assertTrue(all(p['page']>first for route,p in source.calls[before:]))
            self.assertLessEqual(len(source.calls)-before,12)
            runner.state['directory-dispatch-run']=None;runner.state['directory-next-due']=0
            runner.collect_directory(CONTEXT)
            self.assertTrue(runner.state['directory']['complete'])
            self.assertEqual(len(runner.repo.graph['entries']),27)
            self.assertEqual(len({r['id'] for r in runner.repo.inventory['entries'].values()}),27)
            pages=[p['page'] for route,p in source.calls if route=='/entries']
            self.assertEqual(pages.count(1),2)
            self.assertTrue(all(pages.count(n)==1 for n in range(2,28)))

    def test_conflicting_entry_is_held_without_blocking_unaffected_directory_or_repeat(self):
        from data_runner import latest_state
        from wef_data_handoff import revision_chain
        class Conflicting(Source):
            def fetch(self,route,params):
                payload,metric=super().fetch(route,params)
                if route=='/entries':
                    payload['entries'][0]['horse']='CHANGED'
                    if params['page']==1:payload['entries'][0]['horse_id']=999
                return payload,metric
        api=API();seed(api)
        with tempfile.TemporaryDirectory() as folder:
            repo=Repository(api,SCHEMA,AUTHORED);runner=Runner(repo,Source(),folder,folder);runner.tick()
            held=copy.deepcopy(repo.graph['entries']['200000076|21'])
            ids={k:v['id'] for k,v in repo.inventory['entries'].items()}
            for _ in range(2):
                runner=Runner(repo,Conflicting(),folder,folder)
                runner.begin(SHOW,latest_state(list(repo.graph['runs'].values()),SHOW))
                runner.state['directory']={};runner.state['directory-next-due']=0
                runner.collect_directory(CONTEXT)
                self.assertFalse(runner.state['directory']['complete'])
                self.assertTrue(runner.state['directory']['acquisition-complete'])
                self.assertEqual(runner.state['directory-conflicts'],['200000076|21'])
                self.assertEqual(repo.graph['entries']['200000076|21'],held)
                self.assertNotIn('999',repo.graph['horses'])
                self.assertEqual(repo.graph['horses']['122']['horse-name'],'CHANGED')
                self.assertEqual({k:v['id'] for k,v in repo.inventory['entries'].items()},ids)
                self.assertGreater(runner.state['directory-next-due'],time.time()+7000)
                result=runner.finish(api.canonical_context())
                self.assertTrue(result['ok']);self.assertEqual(result['identity-conflicts'],['200000076|21'])
                collections={k:{**v,'_record-id':repo.inventory['collections'][k]['id']} for k,v in repo.graph['collections'].items()}
                units=revision_chain(runner.runkey,repo.graph['runs'],collections)[-1][1]
                unit=next(u for u in units if u['scope']=='entry-directory-partial')
                conflict=unit['source']['identity-conflicts']['200000076|21']
                self.assertEqual(conflict['prior'],held)
                self.assertEqual(conflict['observed']['horse_id'],999)
                self.assertEqual(conflict['prior-record-id'],ids['200000076|21'])
            runner=Runner(repo,Source(),folder,folder)
            runner.begin(SHOW,latest_state(list(repo.graph['runs'].values()),SHOW))
            runner.state['directory']={};runner.state['directory-next-due']=0
            runner.collect_directory(CONTEXT)
            self.assertTrue(runner.state['directory']['complete'])
            self.assertEqual(runner.state['directory-conflicts'],[])
            self.assertEqual(repo.graph['entries']['200000076|21']['horse'],held['horse'])
            self.assertEqual({k:v['id'] for k,v in repo.inventory['entries'].items()},ids)
    def test_final_recheck_identity_change_holds_earlier_candidate_and_its_timestamp(self):
        from data_runner import latest_state
        class LateConflict(Source):
            first_reads=0
            def fetch(self,route,params):
                payload,metric=super().fetch(route,params)
                if route=='/entries' and params['page']==1:
                    self.first_reads+=1
                    if self.first_reads==2:payload['entries'][0]['horse_id']=999
                return payload,metric
        api=API();seed(api)
        with tempfile.TemporaryDirectory() as folder:
            repo=Repository(api,SCHEMA,AUTHORED);runner=Runner(repo,Source(),folder,folder);runner.tick()
            held=copy.deepcopy(repo.graph['entries']['200000076|21']);horse=copy.deepcopy(repo.graph['horses']['121'])
            runner=Runner(repo,LateConflict(),folder,folder);runner.begin(SHOW,latest_state(list(repo.graph['runs'].values()),SHOW))
            runner.state['directory']={};runner.state['directory-next-due']=0
            runner.collect_directory(CONTEXT)
            self.assertFalse(runner.state['directory']['complete'])
            self.assertEqual(repo.graph['entries']['200000076|21'],held)
            self.assertEqual(repo.graph['horses']['121'],horse)
            self.assertNotIn('999',repo.graph['horses'])
    def test_capture_concurrency_never_exceeds_four(self):
        class Many(Source):
            def __init__(self):super().__init__();self.active=0;self.maximum=0;self.lock=threading.Lock()
            def fetch(self,route,params):
                if route!='/entries':return super().fetch(route,params)
                with self.lock:self.active+=1;self.maximum=max(self.maximum,self.active)
                try:
                    time.sleep(0.01);payload,metric=super().fetch(route,params);payload['total_entries']=5;return payload,metric
                finally:
                    with self.lock:self.active-=1
        api=API();seed(api);source=Many()
        with tempfile.TemporaryDirectory() as folder:
            runner=Runner(Repository(api,SCHEMA,AUTHORED),source,folder,folder);runner.tick()
            self.assertTrue(runner.state['directory']['complete']);self.assertEqual(len(runner.state['directory']['ids']),5)
            self.assertLessEqual(source.maximum,4);self.assertGreater(source.maximum,1)
    def test_next_coherent_attempt_preserves_prior_entry_id_and_clears_drift(self):
        class Moving(Source):
            drifting=True
            def fetch(self,route,params):
                payload,metric=super().fetch(route,params)
                if self.drifting and route=='/entries' and params['page']==2:payload['total_entries']=3
                return payload,metric
        api=API();seed(api);source=Moving()
        with tempfile.TemporaryDirectory() as folder:
            runner=Runner(Repository(api,SCHEMA,AUTHORED),source,folder,folder);runner.tick()
            identity=runner.repo.store.inventory('entries')['200000076|21']['id']
            source.drifting=False;runner.begin(SHOW,runner.state);runner.state['directory-next-due']=0
            runner.collect_directory(CONTEXT)
            self.assertTrue(runner.state['directory']['complete']);self.assertEqual(runner.state['directory-reconcile']['attempts'],0)
            self.assertEqual(runner.repo.store.inventory('entries')['200000076|21']['id'],identity)
    def test_shrink_cross_page_repeat_and_final_recheck_drift_remain_partial(self):
        for condition in ('shrink','repeat','final'):
            class Changing(Source):
                first_reads=0
                def fetch(self,route,params):
                    payload,metric=super().fetch(route,params)
                    if route=='/entries':
                        if params['page']==1:self.first_reads+=1
                        if condition=='shrink' and params['page']==2:payload['total_entries']=1
                        if condition=='repeat' and params['page']==2:payload['entries'][0].update(entry_id=21,horse_id=121)
                        if condition=='final' and self.first_reads==2:payload['total_entries']=3
                    return payload,metric
            with self.subTest(condition=condition),tempfile.TemporaryDirectory() as folder:
                api=API();seed(api);runner=Runner(Repository(api,SCHEMA,AUTHORED),Changing(),folder,folder)
                self.assertTrue(runner.tick()['ok']);self.assertFalse(runner.state['directory']['complete'])
                self.assertIn('last-drift',runner.state['directory-reconcile'])
    def test_captured_pass_resumes_through_tick_after_death_without_source_replay(self):
        api=API();seed(api);source=Source()
        with tempfile.TemporaryDirectory() as folder:
            runner=Runner(Repository(api,SCHEMA,AUTHORED),source,folder,folder);runner.tick()
            runner.begin(SHOW,runner.state);runner.state['directory']={}
            with patch.object(runner,'observation',side_effect=KeyboardInterrupt):
                with self.assertRaises(KeyboardInterrupt):runner.collect_directory(CONTEXT)
            calls=len(source.calls);key=runner.runkey
            replay=Runner(Repository(api,SCHEMA,AUTHORED),source,folder,folder)
            self.assertTrue(replay.tick()['ok']);self.assertEqual(replay.runkey,key)
            self.assertEqual(len(source.calls),calls);self.assertTrue(replay.state['directory']['complete'])
    def test_tampered_capture_is_rejected_before_new_source_or_data_writes(self):
        api=API();seed(api);source=Source()
        with tempfile.TemporaryDirectory() as folder:
            runner=Runner(Repository(api,SCHEMA,AUTHORED),source,folder,folder);runner.tick()
            runner.begin(SHOW,runner.state);runner.state['directory']={};runner.collect_directory(CONTEXT,request_limit=1)
            path=pathlib.Path(folder)/runner.state['directory-capture'];capture=json.loads(path.read_text())
            capture['events'][0]['payload']['entries'][0]['horse']='CHANGED';path.write_text(json.dumps(capture))
            runner.begin(SHOW,runner.state);runner.state['directory-next-due']=0;before=len(source.calls);api.patches=[]
            with self.assertRaisesRegex(ValueError,'payload changed'):runner.collect_directory(CONTEXT)
            self.assertEqual(len(source.calls),before);self.assertEqual(api.patches,[])
    def test_capture_file_transient_replace_is_bounded_without_reexecuting_work(self):
        from data_runner import atomic
        with tempfile.TemporaryDirectory() as folder:
            path=pathlib.Path(folder)/'capture.json';error=PermissionError('Windows file temporarily busy');error.winerror=5
            import os
            real=os.replace;calls=[]
            def once(source,target):
                calls.append((source,target))
                if len(calls)==1:raise error
                return real(source,target)
            with patch('data_runner.os.replace',side_effect=once),patch('data_runner.time.sleep'):atomic(path,{'checkpoint':'same'})
            self.assertEqual(len(calls),2);self.assertEqual(json.loads(path.read_text()),{'checkpoint':'same'})
            with patch('data_runner.os.replace',side_effect=error) as fail,patch('data_runner.time.sleep'):
                with self.assertRaises(PermissionError):atomic(path,{'checkpoint':'new'})
            self.assertEqual(fail.call_count,3);self.assertEqual(json.loads(path.read_text()),{'checkpoint':'same'})
    def test_complete_capture_rechecks_source_before_batched_upsert_and_repeat_keeps_ids(self):
        api=API();seed(api);source=Source()
        with tempfile.TemporaryDirectory() as folder:
            runner=Runner(Repository(api,SCHEMA,AUTHORED),source,folder,folder);runner.tick()
            self.assertEqual([p['page'] for r,p in source.calls if r=='/entries'],[1,2,1])
            self.assertTrue(runner.state['directory']['complete']);self.assertEqual(len(runner.state['directory']['ids']),2)
            ids={k:r['id'] for k,r in runner.repo.store.inventory('entries').items()}
            runner.begin(SHOW,runner.state);runner.state.pop('directory-due',None);runner.state['directory']={}
            runner.collect_directory(CONTEXT)
            self.assertEqual(ids,{k:r['id'] for k,r in runner.repo.store.inventory('entries').items()})
    def test_metadata_drift_is_retained_and_defers_only_directory(self):
        class Growing(Source):
            def fetch(self,route,params):
                payload,metric=super().fetch(route,params)
                if route=='/entries' and params['page']==2:payload['total_entries']=3
                return payload,metric
        api=API();seed(api);source=Growing()
        with tempfile.TemporaryDirectory() as folder:
            runner=Runner(Repository(api,SCHEMA,AUTHORED),source,folder,folder);result=runner.tick()
            self.assertTrue(result['ok']);self.assertFalse(runner.state['directory']['complete'])
            mismatch=runner.state['directory-reconcile']['last-drift']
            self.assertEqual(mismatch['expected']['total'],2);self.assertEqual(mismatch['received']['total'],3)
            self.assertGreater(runner.state['directory-next-due'],0)
            self.assertEqual([p['page'] for r,p in source.calls if r=='/entries'],[1,2])
            self.assertTrue(any(r.startswith('/classes/') for r,p in source.calls))
    def test_request_budget_checkpoints_then_resumes_without_replaying_saved_pages(self):
        api=API();seed(api);source=Source()
        with tempfile.TemporaryDirectory() as folder:
            runner=Runner(Repository(api,SCHEMA,AUTHORED),source,folder,folder);runner.tick()
            runner.begin(SHOW,runner.state);runner.state['directory']={};runner.state.pop('directory-next-due',None)
            source.calls=[];runner.collect_directory(CONTEXT,request_limit=1)
            self.assertFalse(runner.state['directory']['complete']);self.assertEqual(runner.state['directory']['next'],2)
            runner.begin(SHOW,runner.state);runner.state['directory-next-due']=0
            runner.collect_directory(CONTEXT)
            self.assertTrue(runner.state['directory']['complete'])
            self.assertEqual([p['page'] for r,p in source.calls if r=='/entries'],[1,2,1])
    def test_older_captured_directory_fact_cannot_regress_newer_shared_name(self):
        from data_runner import directory_graph
        payload={'entries':[{'entry_id':21,'horse_id':121,'horse':'OLDER','riders':[]}]}
        old={'entries':{},'horses':{'121':{'horse-key':'121','horse-id':121,'horse-name':'NEWER','last-success':'2026-10-29T12:01:00Z'}}}
        graph=directory_graph(payload,SHOW,'2026-10-29T12:00:00Z',old)
        self.assertNotIn('horses',graph);self.assertEqual(graph['entries'][0]['horse'],refs('horses','121'))

if __name__=='__main__':unittest.main()
