import copy,importlib.util,json,pathlib,sys,unittest
from unittest.mock import patch
ROOT=pathlib.Path(__file__).resolve().parents[2];sys.path[:0]=[str(ROOT/'wef-data'),str(ROOT/'wef-schedules/tests')]
spec=importlib.util.spec_from_file_location('projection',ROOT/'wef-entries/retained-input.py');m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
from data_runner import refs

class ProjectionTests(unittest.TestCase):
    def setUp(self):
        self.context={'show-key':'15|76','customer-id':15,'show-id':76,'start-date':'2026-10-29','end-date':'2026-10-29','selected-date':'2026-10-29'}
        self.state={'show-key':'15|76','authority':['15|76','2026-10-29','2026-10-29','2026-10-29'],'authority-before':['15|76','2026-10-29','2026-10-29','2026-10-29']}
        self.graph={'shows':[{'show-key':'15|76','show-id':76,'customer-id':15}], 'days':[{'day-key':'day','show':refs('shows','15|76'),'date':'2026-10-29'}], 'rings':[{'ring-key':'ring','show':refs('shows','15|76'),'day':refs('days','day'),'ring-number':3}], 'classes':[{'class-key':'class','show':refs('shows','15|76'),'day':refs('days','day'),'ring':refs('rings','ring'),'class-id':7}], 'schedules':[{'schedule-key':'EXISTING','show':refs('shows','15|76'),'day':refs('days','day'),'ring':refs('rings','ring'),'class':refs('classes','class'),'kind':'class','last-success':'2026-10-29T13:00:00Z'}]}
    def project(self,units):
        with patch.object(m,'revision_chain',return_value=[(self.state,[])]),patch.object(m,'retained_units',return_value=[(u,[{'key':'existing-part','record-id':'recPart','sha256':'original'}]) for u in units]):
            return m.project('head',{}, {},self.context)
    def test_bootstrap_then_inventory_preserves_actual_key_id_and_original_time(self):
        bootstrap={'scope':'retained-bootstrap','source':{'retained-projection':self.graph,'record-map':{'schedules':{'EXISTING':'recData'}}},'metric':{},'graph':{}}
        self.graph['schedules'][0]['source-position']=0
        unit={'scope':'dated-inventory-page','source':{'show':{'show_id':76},'show_date':'2026-10-29','rings':[{'ring_number':3,'ring_id':30,'classes':[{'class_id':7}]}]},'metric':{'completed':'2026-10-29T12:00:00Z'},'graph':{},'verification':{'record-map':{}}}
        projection={'scope':'dated-inventory','source':{},'metric':{'completed':'2026-10-29T13:00:00Z'},'graph':self.graph,'verification':{'record-map':{'schedules':{'EXISTING':'recData'}}}}
        before=self.project([bootstrap]);self.assertIsNone(before['schedules'][0]['sourceAsOf'])
        value=self.project([bootstrap,unit,projection]);row=value['schedules'][0]
        self.assertEqual((row['key'],row['recordId']),('EXISTING','recData'))
        self.assertEqual(row['sourceAsOf'],'2026-10-29T12:00:00Z');self.assertEqual(row['projectionAsOf'],'2026-10-29T13:00:00Z')
        self.assertEqual(row['ringNumber'],3);self.assertEqual(row['classId'],7)
    def test_invalid_forward_context_is_rejected(self):
        self.graph['rings'][0]['day']=refs('days','missing')
        unit={'scope':'retained-bootstrap','source':{'retained-projection':self.graph,'record-map':{'schedules':{'EXISTING':'recData'}}},'metric':{},'graph':{}}
        with self.assertRaises(ValueError):self.project([unit])

if __name__=='__main__':unittest.main()
