"""Offline retained-chain acceptance. All producer IO is in-memory/temp files."""
import importlib.util,json,pathlib,sys,tempfile,unittest,datetime,copy
from unittest.mock import patch
ROOT=pathlib.Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'wef-data/tests'),str(ROOT/'wef-data')]
from test_scheduled_contract import API as ProducerAPI,SCHEMA,AUTHORED,Source,seed,CONTEXT
from data_runner import Runner,Repository
from retained_feed import publish
spec=importlib.util.spec_from_file_location('entry_projection',ROOT/'wef-entry-calculator/data-input.py')
entry=importlib.util.module_from_spec(spec);spec.loader.exec_module(entry)

# Entry-owned fixture adapter: honor current native Shows/day protocol, never
# alter the shared producer or replace its canonical-membership validation.
FIXED=datetime.datetime.fromisoformat('2026-10-29T12:00:00+00:00')
class FixtureDateTime(datetime.datetime):
    @classmethod
    def now(cls,tz=None):return FIXED.astimezone(tz) if tz else FIXED.replace(tzinfo=None)

class API(ProducerAPI):
    def __init__(self):
        super().__init__();self.context['records'][0]['last-checked']=FIXED.isoformat()
        self.native_owner={'id':'recFixtureShowOwner','fields':{'fldjn5SUt9HBgQdBs':'15|200000076','show-key':'15|200000076'}}
        self.native_day={'id':'recFixtureDay','fields':{'fldCIGv6kGhrIZh2K':'15|200000076|2026-10-29','fldASKXpAHgTv0sTM':['recFixtureShowOwner'],'fldA41GJyjFTr5ZSA':'2026-10-29','fldbK4VEp3aNOdJlB':0}}
    def records(self,base,table,fields=None,formula=None):
        if base=='appCaSmL819KK7UFJ':
            if table=='tblJA5CUon5s1sWib':
                if formula is not None and formula!='{show-key}='+json.dumps('15|200000076'):raise AssertionError('Unexpected fixture show filter')
                return [copy.deepcopy(self.native_owner)]
            if table=='tbl14MaCCDw6dAywH':return [copy.deepcopy(self.native_day)]
            raise AssertionError('Unexpected canonical fixture table')
        return super().records(base,table,fields,formula)
    def request(self,method,base,table,params=None,body=None):
        if method=='GET':return {'records':self.records(base,table,(params or {}).get('fields[]'),(params or {}).get('filterByFormula'))}
        return super().request(method,base,table,params,body)

class ProjectionTests(unittest.TestCase):
    def build(self,folder,empty=False):
        with patch('data_runner.datetime.datetime',FixtureDateTime),patch('data_runner.time.time',return_value=FIXED.timestamp()):
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

    def test_native_fixture_checks_exact_owner_links_and_priority(self):
        from data_runner import read_canonical_days,CanonicalTransition
        api=API();context=copy.deepcopy(CONTEXT)
        self.assertEqual(read_canonical_days(api,context),['2026-10-29'])
        self.assertEqual(context['day-priorities'],{'2026-10-29':0})
        api.native_day['fields']['fldASKXpAHgTv0sTM']=['recWrongOwner']
        with self.assertRaises(ValueError):read_canonical_days(api,copy.deepcopy(CONTEXT))
        api.native_day['fields']['fldASKXpAHgTv0sTM']=['recFixtureShowOwner'];api.native_day['fields']['fldbK4VEp3aNOdJlB']=1
        with self.assertRaises(CanonicalTransition):read_canonical_days(api,copy.deepcopy(CONTEXT))

    def test_changed_authority_rejected(self):
        with tempfile.TemporaryDirectory() as folder:
            root,pointer,context,_=self.build(folder)
            with self.assertRaises(ValueError):entry.project(root,pointer,{**context,'selected-date':'2026-10-30'})

if __name__=='__main__':unittest.main()
