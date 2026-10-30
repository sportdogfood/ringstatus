"""Offline fixtures only; no live collection, credentials or Airtable writes."""
import copy,json,pathlib,sys,unittest
ROOT=pathlib.Path(__file__).resolve().parents[2]
sys.path[:0]=[str(ROOT/'wef-data'),str(ROOT/'wef-schedules/tests')]
import wef_data_handoff as handoff
from season_projection import project

SHOW='15|200000076';ENTRY='200000076|200242738'
def link(table,*keys):return {'$links':[[table,str(k)] for k in keys]}
def fixture():
    facts={t:{} for t in ('shows','days','rings','classes','entries','horses','riders','trainers','trips')}
    facts['shows'][SHOW]={'show-key':SHOW,'show-id':200000076,'customer-id':15,'start-date':'2026-10-29','end-date':'2026-11-01'}
    facts['horses']['200002009']={'horse-key':'200002009','horse-id':200002009}
    for rid in (84935,1000001274):facts['riders'][str(rid)]={'rider-key':str(rid),'rider-id':rid}
    facts['trainers']['47261']={'trainer-key':'47261','trainer-id':47261}
    facts['entries'][ENTRY]={'entry-key':ENTRY,'entry-id':200242738,'show':link('shows',SHOW),'horse':link('horses','200002009'),'riders':link('riders','84935','1000001274'),'trainers':link('trainers','47261')}
    for index,(date,cid,rid,uuid) in enumerate([
        ('2026-10-30',200030763,84935,'13BB4941F7314991A82794CA50EB8635'),
        ('2026-10-31',200030798,1000001274,'7B6DE1D3E19B4963BA53319492A0247A'),
        ('2026-11-01',200030799,1000001274,'6C309F181DC2473FAB0D969950291438')]):
        day=SHOW+'|'+date;ring=day+'|6';cls=ring+'|'+str(cid);key=cls+'|200242738|'+uuid
        facts['days'][day]={'day-key':day,'date':date,'show':link('shows',SHOW)}
        facts['rings'][ring]={'ring-key':ring,'ring-id':600,'ring-number':6,'show':link('shows',SHOW),'day':link('days',day)}
        facts['classes'][cls]={'class-key':cls,'class-id':cid,'show':link('shows',SHOW),'day':link('days',day),'ring':link('rings',ring)}
        raw={'entry_id':200242738,'class_id':cid,'horse_id':200002009,'rider_id':rid,'trainer_id':47261,'trip_id':index+9,'entryxclasses_uuid':uuid}
        facts['trips'][key]={'trip-key':key,'entry':link('entries',ENTRY),'class':link('classes',cls),'horse':link('horses','200002009'),'rider':link('riders',rid),'trainer':link('trainers','47261'),'trip-id':str(index+9),'source-uuid':uuid,'source-json':json.dumps(raw),'last-success':'2026-10-29T12:00:00Z','record-status':'active'}
    return facts

class AssignmentTests(unittest.TestCase):
    def test_saved_three_trip_example_preserves_keys_and_withholds_parent(self):
        facts=fixture();before=copy.deepcopy(facts);audit=handoff.assignment_evidence(facts)
        self.assertEqual(len(audit['trips']),3);self.assertIn(ENTRY,audit['withheld-entries'])
        self.assertEqual(audit['summary']['complete-trips'],3);self.assertEqual(audit['summary']['withheld-entries'],1)
        self.assertEqual(facts,before)
        graph=project(facts,'riders',audit)
        self.assertEqual(graph['riders']['84935']['show-days'],[SHOW+'|2026-10-30'])
        self.assertEqual(graph['riders']['1000001274']['show-days'],[SHOW+'|2026-10-31',SHOW+'|2026-11-01'])
    def test_valid_trip_only_rider_is_projected_despite_parent_list(self):
        for values in ([],['84935'],['1000001274']):
            facts=fixture();facts['entries'][ENTRY]['riders']=link('riders',*values)
            graph=project(facts,'riders')
            self.assertEqual(set(graph['riders']),{'84935','1000001274'})
            self.assertEqual(graph['riders']['84935']['shows'],[SHOW])
    def test_missing_trainer_withholds_tuple_without_parent_fallback(self):
        facts=fixture();trip=next(iter(facts['trips'].values()));raw=json.loads(trip['source-json']);raw.pop('trainer_id');trip['source-json']=json.dumps(raw);trip['trainer']=link('trainers')
        audit=handoff.assignment_evidence(facts)
        self.assertFalse(audit['trips'][trip['trip-key']]['complete']);self.assertIsNone(audit['trips'][trip['trip-key']]['trainer'])
        self.assertEqual(audit['summary']['incomplete-trips'],1)
        self.assertIn(SHOW+'|2026-10-30',project(facts,'riders',audit)['riders']['84935']['show-days'])
        self.assertNotIn(SHOW+'|2026-10-30',project(facts,'trainers',audit)['trainers']['47261']['show-days'])
    def test_source_identity_or_scope_conflicts_are_rejected(self):
        for bad in ('rider','multiple','entry','horse','source','missing','day','uuid'):
            facts=fixture();trip=next(iter(facts['trips'].values()))
            if bad=='rider':trip['rider']=link('riders','1000001274')
            if bad=='multiple':trip['rider']=link('riders','84935','1000001274')
            if bad=='entry':facts['entries'][ENTRY]['show']=link('shows','missing')
            if bad=='horse':facts['entries'][ENTRY]['horse']=link('horses','wrong')
            if bad=='source':trip['source-json']='{}'
            if bad=='missing':facts['riders'].pop('84935')
            if bad=='day':facts['days'][SHOW+'|2026-10-30']['show']=link('shows','missing')
            if bad=='uuid':trip['source-uuid']='wrong'
            with self.subTest(bad=bad),self.assertRaises(ValueError):handoff.assignment_evidence(facts)
    def test_directory_only_has_show_without_assignment_or_day(self):
        facts=fixture();facts['trips']={};audit=handoff.assignment_evidence(facts)
        self.assertIn(ENTRY,audit['withheld-entries']);graph=project(facts,'riders',audit)
        self.assertEqual(graph['riders']['84935']['shows'],[SHOW]);self.assertEqual(graph['riders']['84935']['show-days'],[])
    def test_dropped_trips_preserve_history_without_entry_assignment(self):
        facts=fixture()
        for t in facts['trips'].values():t['record-status']='dropped'
        audit=handoff.assignment_evidence(facts);self.assertIn(ENTRY,audit['withheld-entries'])
        self.assertEqual(len(audit['trips']),3);self.assertEqual(len(project(facts,'horses',audit)['horses']['200002009']['show-days']),3)
    def test_single_supported_tuple_and_repeat_are_stable(self):
        facts=fixture();facts['trips']={k:v for k,v in facts['trips'].items() if json.loads(v['source-json'])['rider_id']==84935};facts['entries'][ENTRY]['riders']=link('riders','84935')
        first=handoff.assignment_evidence(facts);self.assertNotIn(ENTRY,first['withheld-entries']);self.assertEqual(first,handoff.assignment_evidence(facts))
    def test_same_names_do_not_merge_source_ids_or_retime_observations(self):
        facts=fixture()
        for row in facts['riders'].values():row['rider-name']='SAME NAME'
        audit=handoff.assignment_evidence(facts);graph=project(facts,'riders',audit)
        self.assertEqual(set(graph['riders']),{'84935','1000001274'})
        for key,item in audit['trips'].items():self.assertEqual(item['source-as-of'],facts['trips'][key]['last-success'])

if __name__=='__main__':unittest.main()
