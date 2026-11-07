"""Offline schedule-before-details regression checks. No source or Airtable calls."""
import copy
import datetime
import importlib.util
import json
import pathlib
import socket
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

HERE = pathlib.Path(__file__).resolve().parent
ROOT = HERE.parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / 'tests')]
from fast_fixture import API as FixtureAPI, Source, SCHEMA, SHOW
from test_store import MemoryAPI


class API(FixtureAPI):
    def records(self, base, table, fields=None, formula=None):
        return MemoryAPI.records(self, base, table, fields)

spec = importlib.util.spec_from_file_location('candidate_collector', HERE / 'candidate/fast_collector.py')
collector = importlib.util.module_from_spec(spec)
spec.loader.exec_module(collector)
SCHEMA['tables']['schedules'] = json.loads((HERE / 'candidate/schema.json').read_text())['tables']['schedules']


class ScheduleStage(unittest.TestCase):
    def setUp(self):
        self.api = API()
        self.source = Source(self.api)
        self.folder = tempfile.TemporaryDirectory()
        self.addCleanup(self.folder.cleanup)
        self.schema = pathlib.Path(self.folder.name) / 'schema.json'
        self.schema.write_text(json.dumps(SCHEMA))
        self.context = self.api.context['records'][0]
        self.selection = (self.context, '2026-11-10', 0)
        self.field_ids = {f['name']: f['id'] for f in SCHEMA['tables']['schedules']['fields']}
        self.table = SCHEMA['tables']['schedules']['id']
        self.calls = []
        self.network = patch.object(socket, 'create_connection', side_effect=AssertionError('Network forbidden'))
        self.network.start()
        self.addCleanup(self.network.stop)

    def schedule_rows(self):
        return [{name: row['fields'].get(fid) for name, fid in self.field_ids.items()}
                for row in self.api.db.get(self.table, [])]

    def collect(self, items, detail_fails=False, schedule_fails=False):
        def fetch(route, params):
            self.calls.append(route)
            payload, metric = self.source.fetch(route, params)
            if route == '/schedule':
                if schedule_fails:
                    raise RuntimeError('schedule failed')
                payload['rings'][0]['classes'] = copy.deepcopy(items)
            else:
                # This assertion runs at the first detail request, before any detail result exists.
                rows = {row['source-id']: row for row in self.schedule_rows()}
                for item in items:
                    row = rows[str(item['class_groupxclasses_id'])]
                    self.assertEqual(row['total-trips'], item.get('total_trips'))
                    if item.get('total_trips') == 0:
                        self.assertEqual(row['record-status'], 'Active — no entries')
                        self.assertEqual(row['rs-flag'], 'No entries')
                    else:
                        self.assertEqual(row['record-status'], 'Active')
                if detail_fails:
                    raise RuntimeError('detail failed')
            return payload, metric

        with patch.object(collector, 'Airtable', return_value=self.api), patch.object(collector, 'Source') as source:
            source.return_value.fetch.side_effect = fetch
            return collector.collect_day(self.schema, self.selection)

    @staticmethod
    def item(number, count=4, name='Hunter', sequence='Over Fences'):
        return {'class_id': number, 'class_group_id': 3, 'class_number': number,
                'class_name': name, 'class_type': 'Hunter', 'schedule_sequencetype': sequence,
                'class_groupxclasses_id': number + 1000, 'total_trips': count}

    def test_saved_before_details_and_details_still_save(self):
        result = self.collect([self.item(111), self.item(112), self.item(177, 0), self.item(180, 3, 'Flat', 'Under Saddle/Flat')])
        self.assertTrue(result['ok'], result)
        rows = {row['source-id']: row for row in self.schedule_rows()}
        self.assertEqual([rows[str(n + 1000)]['rs-priority'] for n in (111, 112, 180)], [1, 2, 9])
        self.assertEqual(rows['1177']['record-status'], 'Active — no entries')
        for table in ('entries', 'trips', 'results', 'horses', 'riders', 'trainers'):
            self.assertTrue(self.api.db.get(SCHEMA['tables'][table]['id']), table)

    def test_detail_failure_keeps_schedule_and_priority(self):
        result = self.collect([self.item(111, 0)], detail_fails=True)
        self.assertFalse(result['ok'])
        row = self.schedule_rows()[0]
        self.assertEqual(row['rs-flag'], 'No entries')
        self.assertEqual(row['record-status'], 'Active — no entries')

    def test_membership_repeat_reappearance_and_failed_schedule(self):
        item = self.item(111)
        self.collect([item])
        original = copy.deepcopy(self.api.db[self.table][0])
        self.collect([])
        self.assertEqual(self.schedule_rows()[0]['record-status'], 'Absent')
        self.collect([])
        self.assertEqual(self.schedule_rows()[0]['record-status'], 'Dropped')
        self.collect([item])
        self.assertEqual(self.schedule_rows()[0]['record-status'], 'Active')
        self.assertEqual(self.api.db[self.table][0]['id'], original['id'])
        self.collect([], schedule_fails=True)
        self.assertEqual(self.schedule_rows()[0]['record-status'], 'Active')

    def test_schedule_write_failure_does_not_prevent_details(self):
        request = self.api.request
        def fail_schedule(method, base, table, **kwargs):
            if table == self.table and method == 'PATCH':
                raise RuntimeError('Airtable rejected schedules')
            return request(method, base, table, **kwargs)
        # No assertion about a rejected schedule: source confirms details are still requested.
        def fetch(route, params):
            self.calls.append(route)
            return self.source.fetch(route, params)
        with patch.object(self.api, 'request', side_effect=fail_schedule), patch.object(collector, 'Airtable', return_value=self.api), patch.object(collector, 'Source') as source:
            source.return_value.fetch.side_effect = fetch
            result = collector.collect_day(self.schema, self.selection)
        self.assertIn('/classes/100', self.calls)
        self.assertTrue(self.api.db.get(SCHEMA['tables']['trips']['id']))
        self.assertFalse(result['ok'])


if __name__ == '__main__':
    unittest.main(verbosity=2)
