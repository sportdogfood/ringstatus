"""Realistic journal/control and file-backed part budgets; no production IO."""
import base64,copy,json,pathlib,sys,tempfile,unittest,zlib
from unittest.mock import patch
ROOT=pathlib.Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT))
import data_runner
from data_contract import packed,thaw
from test_scheduled_contract import API,SCHEMA,AUTHORED,seed,Source,SHOW

def large_state():
 classes=[f'{SHOW}|{day}|{1+i%8}|{200030000+i}' for day,n in [('2026-10-30',37),('2026-10-31',166),('2026-11-01',118)] for i in range(n)]
 refs=[]
 for i in range(82):
  h=f'{i:064x}'
  refs.append({'key':'adaptive-2026-10-30T17:12:39.091110+00:00|observation-'+f'{i+1:03}','scope':'class','sha256':h,'archive':{'format':'wef-data-evidence-v1','path':f'wef-data/shows/15/200000076/retained-evidence/parts/{h}.json.gz','sha256':h,'bytes':12000}})
 return {'show-key':SHOW,'active-classes':classes,'directory':{'ids':list(range(200000000,200000883)),'total':883,'complete':False,'acquisition-complete':True},'observations':refs,'test-padding':'x'*5000}

class JournalBudgetTests(unittest.TestCase):
 def test_321_classes_883_directory_ids_82_refs_fit_without_losing_any_key(self):
  state=large_state();self.assertGreater(len(packed(state).encode()),60000)
  api=API();seed(api)
  with tempfile.TemporaryDirectory() as folder:
   runner=data_runner.Runner(data_runner.Repository(api,SCHEMA,AUTHORED),Source(),folder,folder);runner.state=copy.deepcopy(state);runner.runkey='fixture'
   with patch.object(runner.repo,'patch') as write:runner.journal()
   saved=json.loads(write.call_args.args[2]['state-json']);self.assertLess(len(packed(saved).encode()),60000)
   self.assertEqual(data_runner.active_class_keys(saved),state['active-classes'])
   self.assertEqual(saved['directory'],state['directory']);self.assertEqual(saved['observations'],state['observations'])
 def test_legacy_active_keys_and_corrupt_control(self):
  helper=getattr(data_runner,'active_class_keys',None);self.assertIsNotNone(helper)
  state=large_state();self.assertEqual(helper(state),state['active-classes'])
  raw=packed(state['active-classes']);state['active-classes']={'encoding':'zlib-base64-json','sha256':'0'*64,'count':321,'data':base64.b64encode(zlib.compress(raw.encode())).decode()}
  with self.assertRaises(ValueError):helper(state)
 def test_future_archive_file_parts_are_larger_and_legacy_pending_bytes_are_identical(self):
  plan={'run-key':'fixture','show-key':SHOW,'scope':'class','source':{'body':'x'*240000},'metric':{'started':'old','completed':'old','requests':[]},'graph':{},'next-state':{},'start':1,'request-start':1,'evidence-storage':'wef-data-evidence-v1'}
  old=data_runner.pending_candidate(plan)['collections'];self.assertGreater(len(old),4)
  large=copy.deepcopy(plan);large['retained-budget']=1000000
  new=data_runner.pending_candidate(large)['collections'];self.assertEqual(len(new),1)
  self.assertEqual(thaw(old),thaw(new));self.assertEqual(data_runner.pending_candidate(plan)['collections'],old)
  native=copy.deepcopy(large);native.pop('evidence-storage')
  with self.assertRaises(ValueError):data_runner.pending_candidate(native)

if __name__=='__main__':unittest.main()
