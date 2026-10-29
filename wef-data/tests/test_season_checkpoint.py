import importlib.util,pathlib,json,sys,tempfile,unittest,copy
from unittest.mock import patch
ROOT=pathlib.Path(__file__).resolve().parents[2];sys.path[:0]=[str(ROOT/'wef-data'),str(ROOT/'wef-schedules/tests')]
spec=importlib.util.spec_from_file_location('season_consumer_test',ROOT/'wef-season/season.py');season=importlib.util.module_from_spec(spec);spec.loader.exec_module(season)
from test_season_store import MemoryAPI,schema,graph
from test_scheduled_contract import CONTEXT
from wef_data_airtable import TransportDeferred

class API(MemoryAPI):
    def __init__(self):super().__init__();self.authority={'records':[copy.deepcopy(CONTEXT)]};self.db['tblHeart']=[{'id':'recHeart','fields':{'fldHeartKey':'wef-season-horses'}}]
    def canonical_context(self):return copy.deepcopy(self.authority)
    def records(self,base,table,fields=None,formula=None):return super().records(base,table,fields)

class CheckpointTests(unittest.TestCase):
    def test_data_publication_and_season_read_use_same_private_checkout(self):
        entry_spec=importlib.util.spec_from_file_location('scheduled_path_fixture',ROOT/'wef-data/scheduled.py')
        entry=importlib.util.module_from_spec(entry_spec);entry_spec.loader.exec_module(entry)
        with tempfile.TemporaryDirectory() as folder:
            parent=pathlib.Path(folder);runtime=parent/'wef-data/runtime';(runtime/'handoff').mkdir(parents=True)
            (runtime/'outgoing-receipts').mkdir()
            pointer={'authority':[CONTEXT[k] for k in ('show-key','start-date','end-date','selected-date')]}
            (runtime/'handoff/head-15-200000076.json').write_text(json.dumps(pointer))
            receipt={'path':'wef-data/shows/15/200000075/verified-revision','show-key':'15|200000075','verified_remote_commit':'pinned-commit'}
            (runtime/'outgoing-receipts/15-200000075.json').write_text(json.dumps(receipt))
            with patch.object(entry,'ROOT',parent/'wef-data'),patch.object(entry,'run',return_value={'ok':True}) as run,patch('builtins.print'):
                self.assertEqual(entry.main(),0)
            publication=run.call_args.args[3]
            self.assertEqual(publication,runtime/'archive-repository')
            with patch.object(season,'PARENT',parent),patch.object(season,'load',return_value=({}, {'state-sha256':'current-revision'})),patch.object(season,'verify_snapshot',return_value=({'show_key':'15|200000075'},{})) as verify:
                sources=season.sources(CONTEXT)
            verify.assert_called_once_with(publication/receipt['path'])
            self.assertEqual(sources[0][1]['commit'],'pinned-commit')
    def config(self):return {**schema(),'heartbeat':{'id':'tblHeart','checkpoint-field':'fldCheckpoint'}}
    def source(self):return [({}, {'show-key':'15|75','revision':'fixed-source-sha','kind':'archive'})]
    def test_checkpoint_only_after_readback_and_repeat_is_noop(self):
        api=API()
        with tempfile.TemporaryDirectory() as folder,patch.object(season,'ROOT',pathlib.Path(folder)),patch.object(season,'project',return_value=graph()):
            first=season.process('horses',self.config(),api,self.source(),api.canonical_context());before=copy.deepcopy(api.db);api.creates=[];api.patches=[]
            second=season.process('horses',self.config(),api,self.source(),api.canonical_context())
            self.assertEqual(first['status'],'Succeeded');self.assertEqual(second['status'],'Skipped');self.assertEqual(api.db,before);self.assertEqual(api.creates,[]);self.assertEqual(api.patches,[])
    def test_fence_change_holds_checkpoint_while_preserving_applied_records(self):
        api=API();before=api.canonical_context();api.authority['records'][0]['selected-date']='2026-10-30'
        with tempfile.TemporaryDirectory() as folder,patch.object(season,'ROOT',pathlib.Path(folder)),patch.object(season,'project',return_value=graph()):
            with self.assertRaises(TransportDeferred):season.process('horses',self.config(),api,self.source(),before)
            self.assertNotIn('fldCheckpoint',api.db['tblHeart'][0]['fields']);self.assertEqual(len(api.db['tbl_horses']),1)
    def test_one_entity_failure_does_not_stop_other_three(self):
        api=API()
        with tempfile.TemporaryDirectory() as folder:
            root=pathlib.Path(folder);(root/'schema.json').write_text(json.dumps({'entities':{e:self.config() for e in ['horses','riders','trainers','rings']}}))
            def process(entity,*args):
                if entity=='horses':raise ValueError('Identity conflict fixture')
                return {'status':'Succeeded'}
            with patch.object(season,'ROOT',root),patch.object(season,'Airtable',return_value=api),patch.object(season,'sources',return_value=self.source()),patch.object(season,'process',side_effect=process):result=season.run()
            self.assertEqual(result['horses']['status'],'Failed');self.assertTrue((root/'runtime/horses/stopped.json').exists())
            self.assertTrue(all(result[e]['status']=='Succeeded' for e in ['riders','trainers','rings']))

if __name__=='__main__':unittest.main()
