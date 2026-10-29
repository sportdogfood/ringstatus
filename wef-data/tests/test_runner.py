"""Scheduled orchestration with real persistence and an in-memory HTTP boundary."""
import datetime, importlib.util, pathlib, sys, unittest
ROOT=pathlib.Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT))

class RunnerTests(unittest.TestCase):
    def test_multiple_dated_class_occurrences_are_not_guessed(self):
        from data_runner import validate_inventory
        context={'show-key':'15|200000076','show-id':200000076,'start-date':'2026-10-29','end-date':'2026-10-30'}
        payloads=[{'show':{'show_id':200000076,'start_date':'2026-10-29','end_date':'2026-10-30'},'show_date':d,'rings':[{'ring_number':1,'classes':[{'class_id':2}]}]} for d in ('2026-10-29','2026-10-30')]
        with self.assertRaises(ValueError):validate_inventory(payloads,context,['2026-10-29','2026-10-30'],{})
    def test_frozen_replay_uses_same_reserved_keys_and_state(self):
        from data_runner import pending_candidate
        plan={'run-key':'adaptive-EXISTING','show-key':'15|200000076','scope':'entry-directory-page','source':{},
              'metric':{'started':'2026-10-29T12:00:00Z','completed':'2026-10-29T12:00:01Z'},'graph':{},'next-state':{'directory':{'next':2}},'start':3,'request-start':7}
        first=pending_candidate(plan);second=pending_candidate(plan)
        self.assertEqual(first,second)
        self.assertEqual(first['collections'][0]['collection-key'],'adaptive-EXISTING|observation-003')
        self.assertEqual(first['collections'][0]['show'],{'$links':[['shows','15|200000076']]})
    def test_inventory_preserves_dated_keys_and_has_no_fabricated_success(self):
        from data_runner import module
        context={'show-key':'15|200000076','show-id':200000076,'customer-id':15,'show-name':'TEST','start-date':'2026-10-29','end-date':'2026-11-01','selected-date':'2026-10-29'}
        payload={'show':{'show_id':200000076,'facility_id':500},'show_date':'2026-10-29','rings':[{'ring_number':5,'ring_id':1,'ring_name':'RING','classes':[{'class_id':7,'class_group_id':8,'class_number':9,'class_name':'CLASS','class_groupxclasses_id':10}]}]}
        graph=module('build-wef-data-graph').inventory_graph([payload],context,{},'2026-10-29T12:00:00Z')
        self.assertEqual(graph['classes'][0]['class-key'],'15|200000076|2026-10-29|5|7')
        self.assertEqual(graph['schedules'][0]['schedule-key'],'15|200000076|2026-10-29|5|10')
        self.assertNotIn('last-success',graph['classes'][0])
        self.assertNotIn('last-success',graph['days'][0])
    def test_directory_show_only_projection_does_not_invent_days(self):
        from data_runner import directory_graph
        old={'entries':{},'horses':{},'riders':{},'trainers':{},'owners':{}}
        payload={'entries':[{'entry_id':20,'show_id':200000076,'customer_id':15,'number':1,
            'horse_id':100,'horse':'HORSE','trainer_id':30,'trainer':'TRAINER','entryowner_id':40,'owner':'OWNER',
            'riders':[{'rider_id':50,'rider_name':'RIDER'}]}]}
        graph=directory_graph(payload,'15|200000076','2026-10-29T12:00:00Z',old)
        self.assertEqual(graph['entries'][0]['entry-key'],'200000076|20')
        self.assertEqual(graph['entries'][0]['show'],{'$links':[['shows','15|200000076']]})
        self.assertNotIn('days',graph);self.assertNotIn('trips',graph)
        self.assertEqual(graph['horses'][0]['horse-key'],'100')
    def test_directory_rejects_changed_entry_horse(self):
        from data_runner import directory_graph
        old={'entries':{'200000076|20':{'entry-key':'200000076|20','horse':{'$links':[['horses','999']]}}}}
        with self.assertRaises(ValueError):directory_graph({'entries':[{'entry_id':20,'horse_id':100}]},'15|200000076','t',old)
    def test_state_head_excludes_incomplete_and_other_show(self):
        from data_runner import latest_state
        rows=[{'run-key':'adaptive-a','status':'Verified completed revision','state-json':'{"show-key":"15|200000076","sequence":3}'},
              {'run-key':'adaptive-b','status':'Writing','state-json':'{"show-key":"15|200000076","sequence":4}'},
              {'run-key':'adaptive-c','status':'Verified completed revision','state-json':'{"show-key":"15|200000075","sequence":99}'}]
        self.assertEqual(latest_state(rows,'15|200000076')['sequence'],3)

if __name__=='__main__':unittest.main()
