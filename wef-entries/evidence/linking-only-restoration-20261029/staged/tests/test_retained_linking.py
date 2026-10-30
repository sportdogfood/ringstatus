import importlib.util,json,pathlib,sys,unittest
from unittest.mock import patch
ROOT=next(p for p in pathlib.Path(__file__).resolve().parents if (p/'wef-data/data_runner.py').exists())
sys.path[:0]=[str(ROOT/'wef-data'),str(ROOT/'wef-schedules/tests')]
spec=importlib.util.spec_from_file_location('restored_projection',pathlib.Path(__file__).resolve().parents[1]/'retained-input.py');m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
class RetainedLinking(unittest.TestCase):
 def test_compact_inventory_preserves_all_classes_original_times_and_evidence(self):
  day='2026-10-30';stamp=day+'T12:00:00Z';head='original-head';ctx={'show-key':'15|76','customer-id':15,'show-id':76,'start-date':day,'end-date':day,'selected-date':day}
  state={'show-key':'15|76','authority':['15|76',day,day,day],'authority-before':['15|76',day,day,day]}
  refs=[{'key':'existing-part','record-id':'recOriginal','sha256':'original'}]
  roster=[{'class_group_id':2000+i,'class_id':1000+i,'class_groupxclasses_id':3000+i} for i in range(500)]
  inventory={'scope':'dated-inventory-page','graph':{},'source':{'show':{'show_id':76},'show_date':day,'rings':[{'ring_number':3,'ring_id':30,'classes':roster}]},'metric':{'completed':stamp}}
  payloads=[{'show_id':76,'class':{'class_id':1000+i},'class_related_data':{'date':day,'ring':3},'trips':[],'total_entry_trips':0} for i in range(400)]
  classes={'scope':'class','graph':{},'source':{'classes':payloads},'metric':{'requests':[{'completed':stamp} for _ in payloads]}}
  with patch.object(m,'revision_chain',return_value=[(state,[])]),patch.object(m,'retained_units',return_value=[(inventory,refs),(classes,refs)]):result=m.project(head,{}, {},ctx)
  self.assertEqual(len(result['classes']),400);self.assertEqual(len(result['inventories']),1);self.assertNotIn('schedules',result);self.assertNotIn('scheduleEvidence',result)
  for item in result['classes']:
   self.assertEqual(item['inventoryDate'],day);self.assertNotIn('inventory',item);self.assertEqual(item['observedAt'],stamp);self.assertEqual(item['sourceEvidence']['collections'],refs)
   self.assertEqual(result['inventories'][item['inventoryDate']]['observedAt'],stamp)
  self.assertLess(len(json.dumps(result).encode()),8000000)
  expanded=[{**item,'inventory':result['inventories'][day]} for item in result['classes']]
  self.assertGreater(len(json.dumps(expanded).encode()),8000000)
if __name__=='__main__':unittest.main()
