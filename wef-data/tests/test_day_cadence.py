"""Offline scheduled selection/storage boundaries. All providers are fakes."""
import copy,json,pathlib,socket,tempfile,unittest
from unittest.mock import patch
from fast_fixture import API,Source,SCHEMA
import data_runner as dr

ROOT=pathlib.Path(dr.__file__).parent

class DayCadenceTests(unittest.TestCase):
    def setUp(self):
        self.folder=tempfile.TemporaryDirectory();self.addCleanup(self.folder.cleanup)
        self.schema=pathlib.Path(self.folder.name)/'schema.json'
        self.schema.write_bytes((ROOT/'schema.json').read_bytes())
        self.api=API();self.source=Source(self.api);self.attempt=0
        fields={f['name']:f['id'] for f in SCHEMA['tables']['shows']['fields']}
        self.api.db[SCHEMA['tables']['shows']['id']]=[{'id':'show','fields':{fields['show-key']:'15|200000076'}}]
        fields={f['name']:f['id'] for f in SCHEMA['tables']['days']['fields']}
        self.api.db[SCHEMA['tables']['days']['id']]=[{'id':'day'+str(i),'fields':{fields['day-key']:r['fields']['show-day-key']}} for i,r in enumerate(self.api.days)]
        self.net=patch.object(socket,'create_connection',side_effect=AssertionError('network forbidden'));self.net.start();self.addCleanup(self.net.stop)
    def invoke(self):
        self.attempt+=1;self.source.calls=[]
        with patch.object(dr,'Airtable',return_value=self.api),patch.object(dr,'Source',return_value=self.source),patch.object(dr.time,'time',return_value=self.api.at),patch.dict(dr.os.environ,{'WEF_LOG_ATTEMPT_ID':'cadence-'+str(self.attempt)}):
            return dr.run(self.schema)
    def test_discovery_fully_saves_each_day_before_next_source_read(self):
        fetch=self.source.fetch
        def source(route,params):
            if route=='/schedule':
                priority=int(params['date'][-2:])-10
                if priority:
                    fs={f['name']:f['id'] for f in SCHEMA['tables']['trips']['fields']}
                    self.assertTrue(any('|2026-11-'+str(9+priority).zfill(2)+'|' in r['fields'][fs['trip-key']] for r in self.api.db[SCHEMA['tables']['trips']['id']]))
            return fetch(route,params)
        self.source.fetch=source;result=self.invoke()
        self.assertTrue(result['ok'],result)
        self.assertEqual([r['priority'] for r in result['counts']['days']],[0,1,2,3])
        self.assertEqual([p['date'] for r,p in self.source.calls if r=='/schedule'],['2026-11-10','2026-11-11','2026-11-12','2026-11-13'])
    def test_repeat_cadence_exact_boundaries_and_native_ids(self):
        first=self.invoke();self.assertTrue(first['ok'],first)
        ids={t:{r['id'] for r in rs} for t,rs in self.api.db.items()}
        origin=self.api.at
        for seconds,expected in [(180,[0]),(3599,[0]),(3600,[0,1]),(7200,[0,1,2]),(10800,[0,1,3])]:
            self.api.at=origin+seconds;result=self.invoke();self.assertTrue(result['ok'],result)
            self.assertEqual([r['priority'] for r in result['counts']['days']],expected)
            for table,records in ids.items():
                if table!=SCHEMA['tables']['runs']['id']:self.assertEqual(records,{r['id'] for r in self.api.db[table]})
    def test_failed_day_continues_others_and_is_not_retried_early(self):
        fetch=self.source.fetch
        def source(route,params):
            if route=='/schedule' and params['date']=='2026-11-11':raise RuntimeError('fixture day1 outage')
            return fetch(route,params)
        self.source.fetch=source;result=self.invoke()
        self.assertEqual(result['state'],'Partial');self.assertEqual([r['state'] for r in result['counts']['days']],['Saved','Partial','Saved','Saved'])
        self.assertGreater(result['counts']['saved'],0)
        self.api.at+=180;self.assertEqual([r['priority'] for r in self.invoke()['counts']['days']],[0])
    def test_interruption_preserves_prior_days_without_work_checkpoint(self):
        collect=dr.collect_day
        def interrupted(schema,selection):
            if selection[2]==2:raise KeyboardInterrupt()
            return collect(schema,selection)
        with patch.object(dr,'collect_day',side_effect=interrupted):
            with self.assertRaises(KeyboardInterrupt):self.invoke()
        self.api.at+=180;result=self.invoke()
        self.assertEqual([r['priority'] for r in result['counts']['days']],[0,2,3])
        self.assertTrue(result['ok'],result)
    def test_malformed_future_row_does_not_block_day0(self):
        self.api.days[-1]['fields']['date']=None
        result=self.invoke();self.assertEqual(result['state'],'Partial')
        self.assertEqual([r['priority'] for r in result['counts']['days']],[0,1,2])
    def test_original_top_level_timing_format_single_and_multiple_days(self):
        first=self.invoke();self.assertTrue(first['ok'],first)
        for result in [first,self.invoke()]:
            expected={}
            for day in result['counts']['days']:
                for name,seconds in day['counts']['stage-seconds'].items():expected[name]=expected.get(name,0)+seconds
            timings=result['counts']['stage-seconds']
            self.assertEqual(set(timings),set(expected))
            for name,seconds in expected.items():
                if name=='select-show-and-day':self.assertGreaterEqual(timings[name],seconds)
                else:self.assertAlmostEqual(timings[name],seconds,places=5)
    def test_end_of_show_does_not_invent_missing_days(self):
        self.api.days=[r for r in self.api.days if r['fields']['day-priority']<=1]
        result=self.invoke();self.assertTrue(result['ok'],result)
        self.assertEqual([r['priority'] for r in result['counts']['days']],[0,1])
    def test_priority_rollover_uses_current_priority_and_skips_new_past_day(self):
        self.invoke();self.api.at+=180
        for row in self.api.days:row['fields']['day-priority']-=1
        result=self.invoke();self.assertEqual([r['priority'] for r in result['counts']['days']],[0])
        self.assertEqual(result['day'],'2026-11-11')

if __name__=='__main__':unittest.main()
