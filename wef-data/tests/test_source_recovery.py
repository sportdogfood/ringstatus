"""Offline source failures/captures; no real HTTP or business runner execution."""
import json,pathlib,sys,tempfile,unittest,subprocess
from unittest.mock import patch
sys.path.insert(0,str(pathlib.Path(__file__).resolve().parents[1]))
from data_runner import capture_source_batch,defer_source,SourceFailure,Source,run

class SourceRecoveryTests(unittest.TestCase):
    def test_partial_batch_keeps_successes_and_fetches_only_missing_after_restart(self):
        calls=[];failed=[True]
        def get(key):
            calls.append(key)
            if key=='b' and failed[0]:raise SourceFailure('Source HTTP 502',{'http':502,'route':'/classes/2','params':{}})
            return {'key':key},{'http':200}
        with tempfile.TemporaryDirectory() as folder:
            path=pathlib.Path(folder)/'partial.json';scope={'keys':['a','b','c','d']}
            with self.assertRaises(SourceFailure):capture_source_batch(path,scope,['a','b','c','d'],str,get)
            self.assertEqual(set(json.loads(path.read_text())['responses']),{'a','c','d'})
            failed[0]=False;values=capture_source_batch(path,scope,['a','b','c','d'],str,get)
            self.assertEqual([v[0]['key'] for v in values],['a','b','c','d'])
            self.assertEqual(calls.count('b'),2)
            self.assertTrue(all(calls.count(k)==1 for k in ('a','c','d')))
            self.assertEqual(capture_source_batch(path,scope,['c','d'],str,get),values[2:]);self.assertEqual(len(calls),5)

    def test_tampered_or_wrong_scope_cache_fails_before_fetch(self):
        with tempfile.TemporaryDirectory() as folder:
            path=pathlib.Path(folder)/'partial.json';scope={'keys':['a']}
            capture_source_batch(path,scope,['a'],str,lambda k:({'key':k},{'http':200}))
            with self.assertRaises(ValueError):capture_source_batch(path,{'keys':['b']},['b'],str,lambda _:self.fail('Fetch forbidden'))
            body=json.loads(path.read_text());body['responses']['a']['value'][0]['key']='tampered';path.write_text(json.dumps(body))
            with self.assertRaises(ValueError):capture_source_batch(path,scope,['a'],str,lambda _:self.fail('Fetch forbidden'))

    def test_fatal_error_is_not_hidden_by_parallel_transient_error(self):
        def get(key):raise SourceFailure('Source HTTP '+str(key),{'http':key})
        with tempfile.TemporaryDirectory() as folder:
            with self.assertRaises(SourceFailure) as caught:capture_source_batch(pathlib.Path(folder)/'p.json',{'keys':['502','403']},[502,403],str,get)
            self.assertEqual(caught.exception.diagnostic['http'],403)

    def test_three_scheduled_retries_then_exhaustion_preserves_evidence(self):
        error=SourceFailure('Source HTTP 502',{'http':502,'route':'/classes/2','params':{'show_id':76}})
        # Separate scheduled invocations must not depend on host clock resolution.
        times=[f'2026-10-29T22:0{i}:00+00:00' for i in range(4)]
        with tempfile.TemporaryDirectory() as folder,patch('data_runner.stamp',side_effect=times):
            for number in range(1,4):
                result=defer_source(pathlib.Path(folder),'same-run',error,{'interval-seconds':60,'maximum-seconds':300})
                self.assertEqual(result['source-failures'],number);self.assertEqual(result['state'],'Deferred')
            self.assertIsNone(defer_source(pathlib.Path(folder),'same-run',error,{'interval-seconds':60,'maximum-seconds':300}))
            self.assertEqual(len(list((pathlib.Path(folder)/'source-errors').glob('*.json'))),4)
            self.assertIsNone(defer_source(pathlib.Path(folder),'same-run',SourceFailure('Denied',{'http':403}),{}))

    def test_parent_timeout_is_classified_without_immediate_retry(self):
        with patch('data_runner.subprocess.run',side_effect=subprocess.TimeoutExpired('node',28)) as boundary:
            with self.assertRaises(SourceFailure) as caught:Source().fetch('/classes/2',{'customer_id':15,'show_id':76})
        self.assertTrue(caught.exception.diagnostic['timeout']);self.assertEqual(boundary.call_count,1)

    def test_scheduled_deferral_recovers_same_run_and_preserves_last_good(self):
        from test_scheduled_contract import API,SCHEMA,AUTHORED,Source as FixtureSource,seed
        class Flaky(FixtureSource):
            failing=True
            def fetch(self,route,params):
                if route.startswith('/classes/') and self.failing:raise SourceFailure('Source HTTP 502',{'http':502,'route':route,'params':params})
                return super().fetch(route,params)
        api=API();seed(api);source=Flaky()
        with tempfile.TemporaryDirectory() as folder:
            root=pathlib.Path(folder);schema=root/'schema.json';schema.write_text(json.dumps(SCHEMA));authored=root/'authored.json';authored.write_text(json.dumps(AUTHORED))
            with patch('data_runner.Airtable',return_value=api),patch('data_runner.Source',return_value=source):
                first=run(root,schema,authored,root);self.assertEqual(first['state'],'Deferred');self.assertFalse((root/'stopped.json').exists())
                reserved=json.loads((root/'active.json').read_text())['run-key'];calls=len(source.calls)
                self.assertEqual(run(root,schema,authored,root)['state'],'Deferred');self.assertEqual(len(source.calls),calls)
                wait=json.loads((root/'source-deferred.json').read_text());wait['next-due']=0;(root/'source-deferred.json').write_text(json.dumps(wait));source.failing=False
                final=run(root,schema,authored,root)
            self.assertTrue(final['ok']);self.assertEqual(final['run-key'],reserved)
            self.assertFalse((root/'source-deferred.json').exists());self.assertEqual(len(list((root/'source-errors').glob('*.json'))),1)

    def test_new_inventory_pass_in_same_run_does_not_reuse_old_capture(self):
        from test_scheduled_contract import API,SCHEMA,AUTHORED,Source as FixtureSource,seed,SHOW,CONTEXT
        from data_runner import Runner,Repository
        api=API();seed(api);source=FixtureSource()
        with tempfile.TemporaryDirectory() as folder:
            runner=Runner(Repository(api,SCHEMA,AUTHORED),source,folder,folder);runner.begin(SHOW,{})
            runner.repo.stage({'runs':[{'run-key':runner.runkey}],'shows':[{k:CONTEXT[k] for k in ('show-key','show-id','customer-id','show-name','start-date','end-date','selected-date')}]})
            runner.inventory(CONTEXT);first=len(source.calls);runner.inventory(CONTEXT)
            self.assertEqual(len(source.calls),first*2)

    def test_persistent_transient_error_stops_after_three_scheduled_retries(self):
        from test_scheduled_contract import API,SCHEMA,AUTHORED,Source as FixtureSource,seed
        class Failed(FixtureSource):
            failures=0
            def fetch(self,route,params):
                if route.startswith('/classes/'):
                    self.failures+=1;raise SourceFailure('Source HTTP 503',{'http':503,'route':route,'params':params})
                return super().fetch(route,params)
        api=API();seed(api);source=Failed()
        with tempfile.TemporaryDirectory() as folder:
            root=pathlib.Path(folder);schema=root/'schema.json';schema.write_text(json.dumps(SCHEMA));authored=root/'authored.json';authored.write_text(json.dumps(AUTHORED))
            with patch('data_runner.Airtable',return_value=api),patch('data_runner.Source',return_value=source):
                for number in range(3):
                    self.assertEqual(run(root,schema,authored,root)['state'],'Deferred')
                    wait=json.loads((root/'source-deferred.json').read_text());wait['next-due']=0;(root/'source-deferred.json').write_text(json.dumps(wait))
                with self.assertRaises(SourceFailure):run(root,schema,authored,root)
                self.assertTrue((root/'stopped.json').exists());self.assertEqual(source.failures,4)
                self.assertIn('Stopped',run(root,schema,authored,root)['state']);self.assertEqual(source.failures,4)

if __name__=='__main__':unittest.main()
