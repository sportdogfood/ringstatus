import pathlib,sys,tempfile,copy,unittest
sys.path.insert(0,str(pathlib.Path(__file__).resolve().parents[1]))
sys.path.insert(0,str(pathlib.Path(__file__).resolve().parents[2]/'wef-schedules/tests'))
from season_store import SeasonStore
from test_store import MemoryAPI
from wef_data_airtable import TransportDeferred

def schema():
    fields={'shows':[('show-key','singleLineText'),('show-id','number')],'show-days':[('day-key','singleLineText'),('show','multipleRecordLinks')],'horses':[('horse-key','singleLineText'),('horse-id','number'),('horse-name','singleLineText'),('last-success','dateTime'),('shows','multipleRecordLinks'),('show-days','multipleRecordLinks')]}
    return {'base':'appETrjio9Yj9Pv17','tables':{t:{'id':'tbl_'+t,'primaryFieldId':'fld_'+t+'_'+fs[0][0],'fields':[{'id':'fld_'+t+'_'+n,'name':n,'type':kind} for n,kind in fs]} for t,fs in fields.items()}}
def graph():return {'shows':{'15|75':{'show-key':'15|75','show-id':75}},'show-days':{'15|75|day':{'day-key':'15|75|day','show':['15|75']}},'horses':{'100':{'horse-key':'100','horse-id':100,'horse-name':'H','last-success':'2026-10-29T12:00:00Z','shows':['15|75'],'show-days':['15|75|day']}}}

class SeasonStoreTests(unittest.TestCase):
    def test_repeat_keeps_keys_ids_links_and_owner_fields(self):
        api=MemoryAPI()
        with tempfile.TemporaryDirectory() as folder:
            store=SeasonStore(api,schema(),folder);first=store.apply(graph());before=copy.deepcopy(api.db)
            second=store.apply(graph());self.assertEqual(first['records-added'],3);self.assertEqual(second['records-added'],0);self.assertEqual(second['records-updated'],0);self.assertEqual(api.db,before)
    def test_new_show_membership_unions_but_older_name_does_not_regress(self):
        api=MemoryAPI()
        with tempfile.TemporaryDirectory() as folder:
            store=SeasonStore(api,schema(),folder);store.apply(graph());new=graph();new['shows']={'15|76':{'show-key':'15|76','show-id':76}};new['show-days']={'15|76|day':{'day-key':'15|76|day','show':['15|76']}}
            new['horses']['100'].update({'horse-name':'OLD','last-success':'2026-10-28T12:00:00Z','shows':['15|76'],'show-days':['15|76|day']});store.apply(new)
            row=api.db['tbl_horses'][0]['fields'];self.assertEqual(row['fld_horses_horse-name'],'H');self.assertEqual(len(row['fld_horses_shows']),2);self.assertEqual(len(row['fld_horses_show-days']),2)
            new['horses']['100'].pop('last-success');new['horses']['100']['horse-name']='UNKNOWN TIME';store.apply(new)
            self.assertEqual(api.db['tbl_horses'][0]['fields']['fld_horses_horse-name'],'H')
    def test_lost_create_response_reconciles_same_id_without_duplicate(self):
        class Lost(MemoryAPI):
            lost=False
            def request(self,method,base,table,params=None,body=None):
                result=super().request(method,base,table,params,body)
                if method=='POST' and not self.lost:self.lost=True;raise TransportDeferred(method,base,table,body)
                return result
        api=Lost()
        with tempfile.TemporaryDirectory() as folder:
            with self.assertRaises(TransportDeferred):SeasonStore(api,schema(),folder).apply(graph())
            identity=api.db['tbl_shows'][0]['id'];SeasonStore(api,schema(),folder).apply(graph());self.assertEqual(len(api.db['tbl_shows']),1);self.assertEqual(api.db['tbl_shows'][0]['id'],identity)
    def test_identity_conflict_prevents_all_writes(self):
        api=MemoryAPI()
        with tempfile.TemporaryDirectory() as folder:
            store=SeasonStore(api,schema(),folder);store.apply(graph());api.patches=[];api.creates=[];new=graph();new['horses']['100']['horse-id']=200
            with self.assertRaises(ValueError):store.apply(new)
            self.assertEqual(api.patches,[]);self.assertEqual(api.creates,[])

if __name__=='__main__':unittest.main()
