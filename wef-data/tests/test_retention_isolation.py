"""Offline retention waits never substitute for Source execution or loosen deletes."""
import json,pathlib,sys,tempfile,unittest
from unittest.mock import patch
sys.path.insert(0,str(pathlib.Path(__file__).resolve().parents[1]))
from test_scheduled_contract import API,SCHEMA,AUTHORED,Source,seed
from data_runner import run,Runner,Repository
import retirement

class IsolationTests(unittest.TestCase):
    def test_pre_mutation_wait_and_backoff_continue_current_source(self):
        for kind in ('ack','backoff'):
            api=API();seed(api);source=Source()
            with self.subTest(kind=kind),tempfile.TemporaryDirectory() as folder:
                root=pathlib.Path(folder)
                for name,value in [('schema',SCHEMA),('authored',AUTHORED)]:root.joinpath(name+'.json').write_text(json.dumps(value))
                if kind=='backoff':root.joinpath('retirement-deferred.json').write_text(json.dumps({'next-due':10**20,'failures':1}))
                with patch('data_runner.Airtable',return_value=api),patch('data_runner.Source',return_value=source),patch.object(retirement,'retire',side_effect=retirement.RetentionWait('Season pending')) as retire:
                    result=run(root,root/'schema.json',root/'authored.json',root)
                self.assertTrue(result['ok']);self.assertTrue(source.calls)
                self.assertIn('retention-waits',result)
                self.assertFalse(root.joinpath('stopped.json').exists())
                if kind=='backoff':retire.assert_not_called()

    def test_outgoing_archive_not_ready_does_not_gate_current_show(self):
        api=API();seed(api);source=Source()
        with tempfile.TemporaryDirectory() as folder:
            runner=Runner(Repository(api,SCHEMA,AUTHORED),source,folder,folder)
            runner.repo.graph['shows']['15|200000075']={'show-key':'15|200000075'}
            with patch.object(runner,'archive_outgoing',side_effect=retirement.RetentionWait('Git temporarily unavailable')):
                result=runner.tick()
            self.assertTrue(result['ok']);self.assertTrue(source.calls)
            self.assertEqual(result['retention-waits'][0]['show-key'],'15|200000075')

    def test_existing_mutation_or_bad_archive_is_never_a_soft_wait(self):
        from test_retirement import setup
        from data_contract import packed
        for kind in ('pending','invalid'):
            api,repo,facts,ids,approval=setup()
            with self.subTest(kind=kind),tempfile.TemporaryDirectory() as folder:
                root=pathlib.Path(folder);root.joinpath('retirement-approvals').mkdir()
                root.joinpath('retirement-approvals/75.json').write_text(packed(approval))
                if kind=='pending':
                    root.joinpath('retirements').mkdir();root.joinpath('retirements/15-200000075.json').write_text(packed({'approval-sha256':retirement.digest(packed(approval)),'status':'Deleting','removed':[],'pending':{'ids':['uncertain']}}))
                error=retirement.RetentionWait('Not yet accepted') if kind=='pending' else ValueError('Bad hash')
                with patch.object(retirement,'archive',side_effect=error):
                    with self.assertRaises(ValueError):retirement.retire(repo,root)
                self.assertFalse(api.deletes)

    def test_missing_whole_archive_waits_but_missing_manifest_payload_is_invalid(self):
        from test_retirement import setup
        api,repo,facts,ids,approval=setup()
        with tempfile.TemporaryDirectory() as folder:
            root=pathlib.Path(folder);review=approval['manifest']
            with self.assertRaises(retirement.RetentionWait):retirement.archive(root,review)
            (root/'archive-repository'/review['archive-receipt']['path']).mkdir(parents=True)
            with patch.object(retirement,'verify_snapshot',side_effect=FileNotFoundError('manifest-declared payload missing')):
                with self.assertRaisesRegex(ValueError,'incomplete'):retirement.archive(root,review)

    def test_unknown_delete_without_exact_intent_cannot_skip_into_source(self):
        api=API();seed(api);source=Source()
        with tempfile.TemporaryDirectory() as folder:
            root=pathlib.Path(folder)
            for name,value in [('schema',SCHEMA),('authored',AUTHORED)]:root.joinpath(name+'.json').write_text(json.dumps(value))
            root.joinpath('retirement-deferred.json').write_text(json.dumps({'next-due':10**20,'operation':{'method':'DELETE','write-outcome':'Unknown'}}))
            with patch('data_runner.Airtable',return_value=api),patch('data_runner.Source',return_value=source):
                with self.assertRaisesRegex(ValueError,'exact mutation checkpoint'):run(root,root/'schema.json',root/'authored.json',root)
            self.assertFalse(source.calls);self.assertTrue(root.joinpath('stopped.json').exists())

if __name__=='__main__':unittest.main()
