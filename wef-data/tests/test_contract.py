"""Offline contract tests. No live source, Airtable or Git writes."""
import copy, importlib.util, json, pathlib, sys, unittest
ROOT=pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT));sys.path.insert(0,str(ROOT.parent/'wef-schedules/tests'))

class ContractTests(unittest.TestCase):
    def core(self):
        import data_contract
        return data_contract
    def page(self,ids=(10,11),total=2,page=1):
        return {'entries':[{'entry_id':i,'show_id':200000076,'customer_id':15} for i in ids],
            'show_id':'200000076','total_entries':total,'records_per_page':2,'page':page,
            'sort':{'sort_on':'number','sort_type':'asc'},'term':''}
    def test_directory_repeated_identity_never_completes(self):
        c=self.core(); p=self.page(total=4)
        state=c.directory_page(p,200000076,1,None)
        with self.assertRaisesRegex(ValueError,'Duplicate'):
            c.directory_page(self.page(total=4,page=2),200000076,2,state)
    def test_directory_missing_metadata_not_empty(self):
        p=self.page();del p['records_per_page']
        with self.assertRaises(ValueError):self.core().directory_page(p,200000076,1,None)
    def test_directory_drift_and_wrong_show_rejected(self):
        c=self.core();state=c.directory_page(self.page(total=4),200000076,1,None)
        with self.assertRaises(ValueError):c.directory_page(self.page((12,13),5,2),200000076,2,state)
        p=self.page();p['show_id']='200000075'
        with self.assertRaises(ValueError):c.directory_page(p,200000076,1,None)
    def test_drift_exposes_received_metadata_but_wrong_entry_scope_is_still_fatal(self):
        c=self.core();state=c.directory_page(self.page(total=4),200000076,1,None)
        with self.assertRaises(c.DirectoryDrift) as caught:c.directory_page(self.page((12,13),5,2),200000076,2,state)
        self.assertEqual(caught.exception.received,{'total':5,'size':2,'page':2})
        wrong=self.page((12,13),5,2);wrong['entries'][0]['customer_id']=99
        with self.assertRaises(ValueError) as fatal:c.directory_page(wrong,200000076,2,state)
        self.assertNotIsInstance(fatal.exception,c.DirectoryDrift)
    def test_empty_directory_is_explicit(self):
        s=self.core().directory_page(self.page((),0),200000076,1,None)
        self.assertTrue(s['complete']);self.assertEqual(s['ids'],[])
    def test_payload_parts_roundtrip_and_corruption(self):
        c=self.core();v={'key':'15|200000076','facts':['é'*1000]*5}
        parts=c.freeze(v,1200);self.assertGreater(len(parts),1)
        self.assertTrue(all(len(p['retained-json'].encode())<=1200 for p in parts))
        self.assertEqual(c.thaw(parts),v)
        bad=copy.deepcopy(parts);bad[0]['retained-json']+=' '
        with self.assertRaises(ValueError):c.thaw(bad)
        with self.assertRaises(ValueError):c.thaw(parts[:-1])
    def test_completed_manifest_uses_frozen_observation(self):
        c=self.core();v={'entities':{'horse-key':'100','horse-name':'OLD'}}
        parts=c.freeze(v);v['entities']['horse-name']='NEW'
        self.assertEqual(c.thaw(parts)['entities']['horse-name'],'OLD')
    def test_graph_imports_have_no_business_effect(self):
        for name in ['decoded-day-ring-class','build-wef-data-graph','build-wef-data-delta','prepare-wef-data-batches']:
            spec=importlib.util.spec_from_file_location(name,ROOT.parent/'wef-schedules/tests'/(name+'.py'))
            module=importlib.util.module_from_spec(spec)
            try:spec.loader.exec_module(module)
            except SystemExit:self.fail(name+' executes CLI while imported')

if __name__=='__main__':unittest.main()
