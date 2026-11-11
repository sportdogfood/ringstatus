"""Offline actual collector checks, reusing the proven schedule-stage fixture."""
import copy,importlib.util,json,pathlib,socket,sys,unittest
from unittest.mock import patch
HERE=pathlib.Path(__file__).resolve().parent
ROOT=HERE.parents[1]
sys.path[:0]=[str(ROOT),str(ROOT/'tests')]
spec=importlib.util.spec_from_file_location('schedule_fixture',ROOT/'evidence/schedule-stage-20261106/test_schedule_stage.py')
base=importlib.util.module_from_spec(spec);spec.loader.exec_module(base)
spec=importlib.util.spec_from_file_location('filtered_collector',HERE/'candidate/fast_collector.py')
collector=importlib.util.module_from_spec(spec);spec.loader.exec_module(collector)
base.collector=collector
VERIFIED={'id':'fldO6Duw7yrbEgN4i','name':'class_related_data.results_verified','type':'formula','options':{}}
base.SCHEMA['tables']['classes']['fields'].append(VERIFIED)

class ClassFilter(base.ScheduleStage):
    def setUp(self):
        super().setUp()
        self.cf={f['name']:f for f in base.SCHEMA['tables']['classes']['fields']}
        self.ct=base.SCHEMA['tables']['classes']['id']
    def seed_class(self,number,status='Completed',verified=1,membership='Active'):
        key=base.SHOW+'|2026-11-10|1|'+str(number)
        fields={'class-key':key,'class-id':number,'status':status,'record-status':membership,
                'class_related_data.results_verified':verified,'last-success':'2026-11-01T10:00:00Z','source-json':'{"saved":"keep"}'}
        row={'id':'kept-'+str(number),'fields':{self.cf[k]['id']:v for k,v in fields.items() if v is not None}}
        self.api.db.setdefault(self.ct,[]).append(row)
        return row
    def test_predicate_exact_combination(self):
        f=self.cf
        def required(status,verified,membership='Active'):
            values={'status':status,'record-status':membership,'class_related_data.results_verified':verified}
            return collector.class_detail_required({f[k]['id']:v for k,v in values.items()},f)
        self.assertFalse(required('Completed',1))
        self.assertFalse(required('In Progress',0,'Dropped'))
        for status,verified in [('Completed',0),('Completed',None),('In Progress',1),(None,1)]:self.assertTrue(required(status,verified))
        self.assertTrue(collector.class_detail_required({},f))
    def test_filter_skips_requests_keeps_schedule_and_saved_detail(self):
        old=self.seed_class(111);snapshot=copy.deepcopy(old)
        self.seed_class(112,'In Progress',0,'Dropped')
        self.seed_class(113,'Completed',0)
        self.seed_class(114,'In Progress',1)
        result=self.collect([self.item(n) for n in [100,111,112,113,114]])
        self.assertTrue(result['ok'],result)
        self.assertNotIn('/classes/111',self.calls);self.assertNotIn('/classes/112',self.calls)
        for n in [100,113,114]:self.assertIn('/classes/'+str(n),self.calls)
        self.assertEqual(result['counts']['class-detail-requested'],3)
        self.assertEqual(result['counts']['class-detail-skipped'],2)
        actual=next(r for r in self.api.db[self.ct] if r['id']==old['id'])
        for name in ['class-key','status','last-success','source-json']:
            self.assertEqual(actual['fields'][self.cf[name]['id']],snapshot['fields'][self.cf[name]['id']])
        self.assertEqual(len(self.schedule_rows()),5)
    def test_all_skipped_still_saves_schedule_without_detail_clock_or_trip_changes(self):
        old=self.seed_class(111);detail=copy.deepcopy(old)
        kept={}
        for table in ['trips','results']:
            meta=base.SCHEMA['tables'][table];primary=meta['primaryFieldId'];key=base.SHOW+'|2026-11-10|1|111|99|kept'
            row={'id':'kept-'+table,'fields':{primary:key}}
            self.api.db.setdefault(meta['id'],[]).append(row);kept[table]=copy.deepcopy(row)
        result=self.collect([self.item(111)])
        self.assertTrue(result['ok'],result);self.assertEqual(self.calls,['/schedule'])
        self.assertEqual(result['counts']['class-detail-requested'],0);self.assertEqual(result['counts']['class-detail-skipped'],1)
        for table,row in kept.items():self.assertIn(row,self.api.db[base.SCHEMA['tables'][table]['id']])
        actual=next(r for r in self.api.db[self.ct] if r['id']==old['id'])
        for name in ['source-json','last-success']:self.assertEqual(actual['fields'][self.cf[name]['id']],detail['fields'][self.cf[name]['id']])
    def test_class_state_read_failure_is_logged_and_schedule_and_details_continue(self):
        records=self.api.records;failed=False
        def read(base_id,table,fields=None,formula=None):
            nonlocal failed
            if table==self.ct and not failed:failed=True;raise RuntimeError('fixture class read failed')
            return records(base_id,table,fields,formula)
        with patch.object(self.api,'records',side_effect=read):result=self.collect([self.item(100)])
        self.assertFalse(result['ok']);self.assertIn('/classes/100',self.calls);self.assertEqual(len(self.schedule_rows()),1)
        self.assertTrue(any('fixture class read failed' in e['error'] for e in result['item-errors']))

if __name__=='__main__':unittest.main(verbosity=2)
