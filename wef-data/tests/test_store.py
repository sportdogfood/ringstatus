import copy, pathlib, sys, unittest
sys.path.insert(0,str(pathlib.Path(__file__).resolve().parents[2]/'wef-schedules/tests'))
from wef_data_store import DataStore

def schema():
    result={'base':'appdl16HJ5ETm8N69','tables':{}}
    for table,names in {'runs':['run-key','status','summary'],'horses':['horse-key','horse-id','horse-name'],
                        'entries':['entry-key','horse'],'collections':['collection-key','status']}.items():
        fields=[{'name':n,'id':'fld_'+n,'type':'multipleRecordLinks' if n=='horse' else 'number' if n=='horse-id' else 'singleLineText'} for n in names]
        result['tables'][table]={'id':'tbl_'+table,'primaryFieldId':fields[0]['id'],'fields':fields}
    return result

class MemoryAPI:
    def __init__(self):self.db={};self.creates=[];self.patches=[];self.serial=0
    def records(self,base,table,fields=None):return copy.deepcopy(self.db.get(table,[]))
    def request(self,method,base,table,params=None,body=None):
        rows=self.db.setdefault(table,[]);out=[]
        for item in body['records']:
            merge=body.get('performUpsert',{}).get('fieldsToMergeOn')
            matched=[r for r in rows if merge and all(r['fields'].get(f)==item['fields'].get(f) for f in merge)]
            if len(matched)>1:raise ValueError('Ambiguous upsert match')
            if method=='POST' or (merge and not matched):
                self.serial+=1;r={'id':'rec'+str(self.serial),'fields':copy.deepcopy(item['fields'])};rows.append(r);self.creates.append((table,r['id']))
            else:
                r=matched[0] if merge else next(r for r in rows if r['id']==item['id']);r['fields'].update(copy.deepcopy(item['fields']));self.patches.append((table,copy.deepcopy(item['fields'])))
            out.append(copy.deepcopy(r))
        if method=='POST':self.assert_batch=len(body['records'])
        return {'records':out}

class StoreTests(unittest.TestCase):
    def test_reservation_upsert_reuses_matching_id_without_overwriting_fields(self):
        api=MemoryAPI();api.db['tbl_horses']=[{'id':'recH','fields':{'fld_horse-key':'100','fld_horse-name':'KEPT'}}]
        store=DataStore(api,schema())
        first=store.reserve('horses',['100','200']);second=store.reserve('horses',['100','200'])
        self.assertEqual(first,second);self.assertEqual(first['100'],'recH')
        self.assertEqual(len(api.db['tbl_horses']),2);self.assertEqual(api.db['tbl_horses'][0]['fields']['fld_horse-name'],'KEPT')

    def test_completed_collection_rejected_before_any_write(self):
        api=MemoryAPI();api.db['tbl_collections']=[{'id':'recC','fields':{'fld_collection-key':'old','fld_status':'Verified retained observation'}}]
        with self.assertRaises(ValueError):DataStore(api,schema()).stage({'runs':[{'run-key':'new'}],'collections':[{'collection-key':'old','status':'Pending'}]})
        self.assertEqual(api.creates,[]);self.assertEqual(api.patches,[])
    def test_sparse_link_lookup_does_not_rewrite_existing_horse(self):
        api=MemoryAPI();api.db['tbl_horses']=[{'id':'recH','fields':{'fld_horse-key':'100','fld_horse-id':100,'fld_horse-name':'KEPT'}}]
        store=DataStore(api,schema());graph={'runs':[{'run-key':'adaptive-a','status':'Pending'}],
            'entries':[{'entry-key':'200000076|20','horse':{'$links':[['horses','100']]}}]}
        store.stage(graph)
        self.assertEqual(api.db['tbl_entries'][0]['fields']['fld_horse'],['recH'])
        self.assertEqual(api.db['tbl_horses'][0]['fields']['fld_horse-name'],'KEPT')
    def test_repeat_retains_ids_and_skips_equivalent_entity_patch(self):
        api=MemoryAPI();store=DataStore(api,schema())
        graph={'runs':[{'run-key':'adaptive-a','status':'Pending'}],'horses':[{'horse-key':'100','horse-id':100,'horse-name':'KEPT'}]}
        store.stage(graph);ids=copy.deepcopy(api.db['tbl_horses']);api.patches=[]
        graph['runs'][0]['run-key']='adaptive-b';store.stage(graph)
        self.assertEqual(api.db['tbl_horses'],ids)
        self.assertFalse(any(t=='tbl_horses' for t,f in api.patches))
    def test_verified_run_replay_cannot_mutate(self):
        api=MemoryAPI();api.db['tbl_runs']=[{'id':'recR','fields':{'fld_run-key':'adaptive-a','fld_status':'Verified completed revision'}}]
        with self.assertRaises(ValueError):DataStore(api,schema()).stage({'runs':[{'run-key':'adaptive-a','status':'Pending'}]})
        self.assertEqual(api.db['tbl_runs'][0]['fields']['fld_status'],'Verified completed revision')
        self.assertEqual(api.patches,[])
    def test_identity_conflict_rejected_before_any_mutation(self):
        api=MemoryAPI();api.db['tbl_horses']=[{'id':'recH','fields':{'fld_horse-key':'100','fld_horse-id':100}}]
        graph={'runs':[{'run-key':'adaptive-a','status':'Pending'}],'horses':[{'horse-key':'100','horse-id':200}]}
        with self.assertRaises(ValueError):DataStore(api,schema()).stage(graph)
        self.assertEqual(api.creates,[]);self.assertEqual(api.patches,[])

if __name__=='__main__':unittest.main()
