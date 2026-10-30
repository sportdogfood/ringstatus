"""Offline helper tests; business collection is never invoked."""
import copy,json,pathlib,sys,tempfile,unittest
from unittest.mock import patch
ROOT=pathlib.Path(__file__).resolve().parents[2]
sys.path[:0]=[str(ROOT/'wef-data'),str(ROOT/'wef-data/tests'),str(ROOT/'wef-schedules/tests')]
from test_store import MemoryAPI
from test_trip_assignments import fixture,SHOW
from test_season_checkpoint import season,API,CheckpointTests

def config():
    fields=[('ring-key','singleLineText'),('ring-number','number'),('ring-id','number'),('name','singleLineText'),('ring-nickname','singleLineText')]
    return {'base':'appxMk9wfbBHEvC5e','tables':{'rings-helper':{'id':'tblHelper','primaryFieldId':'fldring-key','fields':[{'id':'fld'+n,'name':n,'type':t} for n,t in fields]}},'existing-identities':[{'ring-key':'15|500|number:6','ring-number':6,'customer-id':15,'facility-id':500}]}

def source():
    facts=fixture();facts['shows'][SHOW]['facility-id']=500
    for i,row in enumerate(facts['rings'].values()):row.update({'ring-name':'Current ring','last-success':'2026-10-30T12:00:00Z'})
    return [(facts,{'show-key':SHOW,'revision':'rev','kind':'retained','sequence':1})]

class HelperTests(unittest.TestCase):
    def test_unique_existing_number_key_is_not_rekeyed_when_id_appears(self):
        from ring_helpers import candidates
        rows=candidates(source(),config(),[])
        self.assertEqual(list(rows),['15|500|number:6']);self.assertEqual(rows['15|500|number:6'],{'ring-key':'15|500|number:6','ring-number':6,'ring-id':600,'name':'Current ring'})
    def test_repeat_keeps_native_id_nickname_and_existing_records(self):
        from ring_helpers import sync
        api=MemoryAPI();api.db['tblHelper']=[{'id':'recExisting','fields':{'fldring-key':'15|500|number:6','fldring-number':6,'fldring-nickname':'My nickname'}},{'id':'recOther','fields':{'fldring-key':'15|516|id:1','fldring-id':1,'fldname':'Historical'}}]
        with tempfile.TemporaryDirectory() as folder:
            sync(api,config(),source(),pathlib.Path(folder));before=copy.deepcopy(api.db);result=sync(api,config(),source(),pathlib.Path(folder))
        self.assertEqual(before,api.db);self.assertEqual(result['records-updated'],0);self.assertEqual(len(api.db['tblHelper']),2)
        self.assertEqual(api.db['tblHelper'][0]['fields']['fldring-nickname'],'My nickname')
        self.assertTrue(all('fldring-nickname' not in fields for table,fields in api.patches))
    def test_same_name_different_facility_is_not_a_merge_and_new_key_uses_existing_formula(self):
        from ring_helpers import candidates
        values=source();other=copy.deepcopy(values[0]);other[0]['shows'][SHOW]['facility-id']=516
        rows=candidates(values+[other],config(),[])
        self.assertEqual(set(rows),{'15|500|number:6','15|516|id:600'})
    def test_conflicting_scope_or_identity_is_rejected_before_writes(self):
        from ring_helpers import sync
        api=MemoryAPI();values=source();values[0][0]['shows'][SHOW].pop('facility-id')
        with tempfile.TemporaryDirectory() as folder,self.assertRaises(ValueError):sync(api,config(),values,pathlib.Path(folder))
        self.assertEqual(api.creates,[]);self.assertEqual(api.patches,[])
    def test_latest_dated_name_wins_and_missing_name_never_clears(self):
        from ring_helpers import candidates
        values=source();rows=list(values[0][0]['rings'].values());rows[0].update({'ring-name':'Old','last-success':'2026-10-29T12:00:00Z'});rows[-1].pop('ring-name')
        result=candidates(values,config(),[]);self.assertEqual(next(iter(result.values()))['name'],'Current ring')
    def test_helper_runs_even_when_main_revision_already_checkpointed(self):
        api=API();cfg=CheckpointTests().config();cfg['ring-helpers']=config();api.db['tblHeart'][0]['fields']['fldCheckpoint']=json.dumps({'15|75':{'revision':'fixed-source-sha','kind':'archive'}})
        with tempfile.TemporaryDirectory() as folder,patch.object(season,'ROOT',pathlib.Path(folder)),patch.object(season,'sync_ring_helpers',return_value={'records-added':1,'records-updated':0,'records-unchanged':0}) as helper:
            result=season.process('horses',cfg,api,CheckpointTests().source(),api.canonical_context())
        helper.assert_called_once();self.assertEqual(result['counts']['ring-helper-records-added'],1);self.assertEqual(result['status'],'Succeeded')
    def test_lost_create_key_only_number_identity_is_recovered_without_rekey(self):
        from ring_helpers import candidates
        cfg=config();cfg['existing-identities']=[]
        rows=candidates(source(),cfg,[{'ring-key':'15|500|number:6'}])
        self.assertEqual(list(rows),['15|500|number:6'])
    def test_older_rejected_revision_cannot_update_helper(self):
        api=API();cfg=CheckpointTests().config();cfg['ring-helpers']=config()
        api.db['tblHeart'][0]['fields']['fldCheckpoint']=json.dumps({SHOW:{'revision':'newer','kind':'retained','sequence':2}})
        with tempfile.TemporaryDirectory() as folder,patch.object(season,'ROOT',pathlib.Path(folder)),patch.object(season,'sync_ring_helpers',return_value={'records-added':0,'records-updated':0,'records-unchanged':0}) as helper:
            result=season.process('horses',cfg,api,source(),api.canonical_context())
        helper.assert_not_called();self.assertEqual(result['status'],'Deferred')

if __name__=='__main__':unittest.main()
