import pathlib,sys,tempfile,unittest,json
sys.path.insert(0,str(pathlib.Path(__file__).resolve().parents[1]))
from test_scheduled_contract import API,SCHEMA,AUTHORED,Source,seed,SHOW
from data_runner import Runner,Repository
from retained_feed import publish,load

class FeedTests(unittest.TestCase):
    def test_equal_payload_parts_keep_distinct_collection_ids_across_observations(self):
        from data_runner import latest_state
        from collections import Counter
        api=API();seed(api);source=Source()
        with tempfile.TemporaryDirectory() as folder:
            repo=Repository(api,SCHEMA,AUTHORED);runner=Runner(repo,source,folder,folder);runner.tick()
            runner.begin(SHOW,latest_state(list(repo.graph['runs'].values()),SHOW))
            metric={'started':runner.at,'completed':runner.at,'requests':[]}
            for _ in range(2):runner.observation('repeated-large-body',{'padding':'x'*180000},metric,{})
            runner.finish(api.canonical_context())
            refs=runner.state['observations'];counts=Counter(r['sha256'] for r in refs)
            self.assertTrue(any(n>1 for n in counts.values()))
            self.assertEqual(len({r['record-id'] for r in refs}),len(refs))
            original=json.loads(json.dumps(refs));root=pathlib.Path(folder,'feed')
            publish(repo,runner.state,root,api.canonical_context())
            pointer=json.loads((root/'head-15-200000076.json').read_text())
            facts,proof=load(root,pointer)
            self.assertIn('200000076|21',facts['entries'])
            self.assertEqual(proof['run-key'],runner.runkey)
            before={p.name:p.read_bytes() for p in (root/'objects').iterdir()}
            publish(repo,runner.state,root,api.canonical_context())
            self.assertEqual(before,{p.name:p.read_bytes() for p in (root/'objects').iterdir()})
            self.assertEqual(refs,original)
    def test_cache_failure_cannot_downgrade_already_completed_data(self):
        from unittest.mock import patch
        from data_runner import run
        api=API();seed(api);source=Source()
        with tempfile.TemporaryDirectory() as folder:
            root=pathlib.Path(folder);(root/'schema.json').write_text(json.dumps(SCHEMA));(root/'authored.json').write_text(json.dumps(AUTHORED))
            with patch('data_runner.Airtable',return_value=api),patch('data_runner.Source',return_value=source),patch('retained_feed.publish',side_effect=RuntimeError('Cache fixture failure')):
                result=run(root,root/'schema.json',root/'authored.json',root)
            self.assertTrue(result['ok']);self.assertIn('handoff-error',result);self.assertFalse((root/'stopped.json').exists())
            self.assertEqual(Repository(api,SCHEMA,AUTHORED).graph['runs'][result['run-key']]['status'],'Verified completed revision')
    def test_publish_replay_repeat_and_tamper_without_additional_requests(self):
        api=API();seed(api);source=Source()
        with tempfile.TemporaryDirectory() as folder:
            runner=Runner(Repository(api,SCHEMA,AUTHORED),source,folder,folder);runner.tick();calls=len(source.calls)
            root=pathlib.Path(folder,'feed');publish(runner.repo,runner.state,root,api.canonical_context())
            pointer=json.loads((root/'head-15-200000076.json').read_text());facts,proof=load(root,pointer)
            self.assertIn('200000076|21',facts['entries']);self.assertEqual(len(source.calls),calls)
            before=sorted(p.name for p in (root/'objects').iterdir());publish(runner.repo,runner.state,root,api.canonical_context());self.assertEqual(before,sorted(p.name for p in (root/'objects').iterdir()))
            # Airtable normalizes control timestamps outside the frozen JSON.
            for table in ('collections','runs'):
                for row in runner.repo.graph[table].values():
                    for name in ('started','completed'):
                        if row.get(name):row[name]=row[name][:19]+'.000Z'
            publish(runner.repo,runner.state,root,api.canonical_context())
            file=root/'objects'/('run-'+pointer['run-sha256']+'.json');file.write_text('{}')
            with self.assertRaises((ValueError,KeyError)):load(root,pointer)

if __name__=='__main__':unittest.main()
