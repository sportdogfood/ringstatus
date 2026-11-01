"""Two-miss membership using saved statuses; all source/storage is in memory."""
import copy,unittest
from unittest.mock import patch
import test_stage_separation as stage

class Membership(unittest.TestCase):
    setUp=stage.StageSeparation.setUp
    fast=stage.StageSeparation.fast

    def collect(self,mode='present'):
        self.api.at+=180
        fetch=self.source.fetch
        def response(route,params):
            if mode=='schedule-failure' and route=='/schedule':raise RuntimeError('schedule unavailable')
            if mode=='class-failure' and route.startswith('/classes/'):raise RuntimeError('class unavailable')
            body,metric=fetch(route,params)
            if route=='/schedule':
                if mode=='class-absent':body['rings'][0]['classes']=[]
                if mode=='malformed-schedule':body['rings'].append({'ring_number':2,'classes':None})
            elif mode=='trip-absent':body['trips']=[]
            elif mode=='malformed-trips':body['trips']=[{'entry_id':'bad'}]
            return body,metric
        with patch.object(self.source,'fetch',side_effect=response):return self.fast()

    def snapshot(self,table):return copy.deepcopy(stage.rows(self.api,table)[0])

    def test_class_two_misses_preserve_facts_and_restore_same_record(self):
        self.assertTrue(self.collect()['ok']);before=self.snapshot('classes')
        self.assertEqual(before['fields']['record-status'],'Active')
        for expected in ('Absent','Dropped','Dropped'):
            self.assertTrue(self.collect('class-absent')['ok'])
            after=self.snapshot('classes');self.assertEqual(after['id'],before['id'])
            expected_fields={**before['fields'],'record-status':expected}
            self.assertEqual(after['fields'],expected_fields)
        self.assertTrue(self.collect()['ok']);after=self.snapshot('classes')
        self.assertEqual(after['id'],before['id']);self.assertEqual(after['fields']['record-status'],'Active')
        self.assertEqual(after['fields']['status'],'Completed')

    def test_trip_two_misses_preserve_facts_and_restore_same_record(self):
        self.collect();before=self.snapshot('trips')
        for expected in ('absent','dropped','dropped'):
            self.assertTrue(self.collect('trip-absent')['ok'])
            after=self.snapshot('trips');self.assertEqual(after['id'],before['id'])
            self.assertEqual(after['fields'],{**before['fields'],'record-status':expected})
        self.assertTrue(self.collect()['ok']);after=self.snapshot('trips')
        self.assertEqual(after['id'],before['id']);self.assertEqual(after['fields']['record-status'],'active')

    def test_failed_schedule_does_not_count_as_a_miss(self):
        self.collect();self.collect('class-absent');before=self.snapshot('classes')
        self.assertFalse(self.collect('schedule-failure')['ok'])
        self.assertEqual(self.snapshot('classes'),before)
        self.collect('class-absent');self.assertEqual(self.snapshot('classes')['fields']['record-status'],'Dropped')

    def test_failed_class_does_not_count_as_a_trip_miss(self):
        self.collect();self.collect('trip-absent');before=self.snapshot('trips')
        self.assertFalse(self.collect('class-failure')['ok'])
        self.assertEqual(self.snapshot('trips'),before)
        self.collect('trip-absent');self.assertEqual(self.snapshot('trips')['fields']['record-status'],'dropped')

    def test_incomplete_schedule_does_not_remove_other_classes(self):
        self.collect();before=self.snapshot('classes');fetch=self.source.fetch
        def incomplete(route,params):
            body,metric=fetch(route,params)
            if route=='/schedule':body['rings']=[{'ring_number':2,'classes':None}]
            return body,metric
        self.api.at+=180
        with patch.object(self.source,'fetch',side_effect=incomplete):result=self.fast()
        self.assertFalse(result['ok']);self.assertEqual(self.snapshot('classes'),before)

    def test_malformed_trips_do_not_advance_absence_and_other_tables_save(self):
        self.collect();self.collect('trip-absent');before=self.snapshot('trips')
        result=self.collect('malformed-trips');self.assertFalse(result['ok'])
        self.assertEqual(self.snapshot('trips'),before)
        self.assertGreater(self.snapshot('rings')['fields']['last-success'],before['fields']['last-success'])

    def test_missing_parent_does_not_invent_a_trip_response(self):
        self.collect();before=self.snapshot('trips')
        self.collect('class-absent');self.collect('class-absent')
        self.assertEqual(self.snapshot('trips'),before)

    def test_other_day_and_show_statuses_are_unchanged(self):
        self.collect()
        for table in ('classes','trips'):
            native=self.api.db[stage.SCHEMA['tables'][table]['id']]
            keyname='class-key' if table=='classes' else 'trip-key'
            keyfield=next(f['id'] for f in stage.SCHEMA['tables'][table]['fields'] if f['name']==keyname)
            for suffix,prefix in [('past','15|200000076|2026-11-09|'),('other','15|999|2026-11-10|')]:
                row=copy.deepcopy(native[0]);row['id']+='-'+suffix
                row['fields'][keyfield]=prefix+row['fields'][keyfield].split('|',3)[3];native.append(row)
        before={t:copy.deepcopy(self.api.db[stage.SCHEMA['tables'][t]['id']][1:]) for t in ('classes','trips')}
        self.collect('class-absent');self.collect('class-absent')
        for t in before:self.assertEqual(self.api.db[stage.SCHEMA['tables'][t]['id']][1:],before[t])

    def test_schedule_two_misses_and_reappearance_preserve_same_record(self):
        self.collect();before=self.snapshot('schedules')
        for expected in ('Absent','Dropped','Dropped'):
            self.assertTrue(self.collect('class-absent')['ok'])
            after=self.snapshot('schedules');self.assertEqual(after['id'],before['id'])
            self.assertEqual(after['fields'],{**before['fields'],'record-status':expected})
        self.collect();self.assertEqual(self.snapshot('schedules')['id'],before['id'])
        self.assertEqual(self.snapshot('schedules')['fields']['record-status'],'Active')

    def test_verified_zero_tags_class_and_schedule_without_skipping_source(self):
        self.collect();fetch=self.source.fetch
        def zero(route,params):
            body,metric=fetch(route,params)
            if route.startswith('/classes/'):body['total_entry_trips']=0;body['trips']=[]
            return body,metric
        self.api.at+=180;count=len(self.source.calls)
        with patch.object(self.source,'fetch',side_effect=zero):result=self.fast()
        self.assertTrue(result['ok'],result)
        for t in ('classes','schedules'):
            self.assertEqual(self.snapshot(t)['fields']['record-status'],'Active \u2014 no entries')
        self.assertTrue(any(route.startswith('/classes/') for route,params in self.source.calls[count:]))
        self.collect();self.assertEqual(self.snapshot('classes')['fields']['record-status'],'Active')
        self.assertEqual(self.snapshot('schedules')['fields']['record-status'],'Active')

    def test_schedule_zero_does_not_override_nonzero_or_unknown_class_entries(self):
        self.collect();fetch=self.source.fetch
        def counts(route,params):
            body,metric=fetch(route,params)
            if route=='/schedule':body['rings'][0]['classes'][0]['total_trips']=0
            else:body.pop('total_entry_trips',None)
            return body,metric
        self.api.at+=180
        with patch.object(self.source,'fetch',side_effect=counts):result=self.fast()
        self.assertTrue(result['ok'],result)
        for t in ('classes','schedules'):self.assertEqual(self.snapshot(t)['fields']['record-status'],'Active')

    def test_all_four_days_receive_states_and_negative_days_are_untouched(self):
        self.api.days=stage.BaseAPI().days
        result=self.collect();self.assertTrue(result['ok'],result)
        self.assertEqual([r['priority'] for r in result['counts']['days']],[0,1,2,3])
        for table,key in [('classes','class-key'),('schedules','schedule-key')]:
            current=stage.rows(self.api,table)
            self.assertEqual(len(current),4)
            self.assertEqual({r['fields']['record-status'] for r in current},{'Active'})
            self.assertEqual({r['fields'][key].split('|')[2] for r in current},{'2026-11-10','2026-11-11','2026-11-12','2026-11-13'})

if __name__=='__main__':unittest.main()
