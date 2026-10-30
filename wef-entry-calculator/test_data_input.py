"""Offline retained-chain acceptance. All producer IO is in-memory/temp files."""
import importlib.util,json,pathlib,sys,tempfile,unittest
ROOT=pathlib.Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'wef-data/tests'),str(ROOT/'wef-data')]
from test_scheduled_contract import API,SCHEMA,AUTHORED,Source,seed,CONTEXT
from data_runner import Runner,Repository
from retained_feed import publish
spec=importlib.util.spec_from_file_location('entry_projection',ROOT/'wef-entry-calculator/data-input.py')
entry=importlib.util.module_from_spec(spec);spec.loader.exec_module(entry)

class ProjectionTests(unittest.TestCase):
    def build(self,folder,empty=False):
        api=API();seed(api);source=Source();source.empty=empty
        runner=Runner(Repository(api,SCHEMA,AUTHORED),source,folder,folder)
        runner.tick();root=pathlib.Path(folder,'feed')
        publish(runner.repo,runner.state,root,runner.state['authority'])
        pointer=json.loads((root/'head-15-200000076.json').read_text())
        context={k:CONTEXT[k] for k in ('show-key','show-id','customer-id','start-date','end-date','selected-date')}
        return root,pointer,context,source

    def test_trip_projection_repeat_preserves_observation_and_identity_without_requests(self):
        with tempfile.TemporaryDirectory() as folder:
            root,pointer,context,source=self.build(folder);calls=len(source.calls)
            before={p.name:p.read_bytes() for p in (root/'objects').iterdir()}
            a=entry.project(root,pointer,context);b=entry.project(root,pointer,context)
            self.assertEqual(a,b);self.assertEqual(len(source.calls),calls)
            self.assertEqual(before,{p.name:p.read_bytes() for p in (root/'objects').iterdir()})
            self.assertEqual(len(a['entry_classes']),1)
            item=a['entry_classes'][0];self.assertEqual(item['identity'],[15,200000076,'2026-10-29',1,2])
            self.assertEqual(item['payload']['trips'][0]['trip_id'],9);self.assertTrue(item['refs'])
            self.assertEqual(a['proof'],entry.baseline.project(root,pointer,context)['proof'])

    def test_valid_empty_is_not_missing(self):
        with tempfile.TemporaryDirectory() as folder:
            root,pointer,context,_=self.build(folder,empty=True)
            a=entry.project(root,pointer,context)
            self.assertEqual(len(a['entry_classes']),1);self.assertEqual(a['entry_classes'][0]['payload']['trips'],[])

    def test_journal_tamper_rejected(self):
        with tempfile.TemporaryDirectory() as folder:
            root,pointer,context,_=self.build(folder)
            file=root/'objects'/('run-'+pointer['run-sha256']+'.json')
            value=json.loads(file.read_text());value['state-json']+=' ';file.write_text(json.dumps(value))
            with self.assertRaises(ValueError):entry.project(root,pointer,context)

    def test_changed_authority_rejected(self):
        with tempfile.TemporaryDirectory() as folder:
            root,pointer,context,_=self.build(folder)
            with self.assertRaises(ValueError):entry.project(root,pointer,{**context,'selected-date':'2026-10-30'})

if __name__=='__main__':unittest.main()
