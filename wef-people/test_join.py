import copy
import importlib.util
import pathlib
import unittest

spec=importlib.util.spec_from_file_location('people_join',pathlib.Path(__file__).with_name('join.py'))
join=importlib.util.module_from_spec(spec);spec.loader.exec_module(join)

def row(id,**fields):return {'id':id,'fields':fields}
def sources():
    return {'w-riders':[row('recR',**{'rider-key':'1','rider-id':1,'rider-name':'Same','shows':'15|9','show-days':'15|9|2026-11-01','last-success':'2026-11-01T12:00:00Z'})],
        'w-trainers':[row('recT',**{'trainer-key':'1','trainer-id':1,'trainer-name':'Same','shows':'15|9'})],
        'w-shows':[row('recS',**{'show-key':'15|9'})],
        'w-days':[row('recD',**{'show-day-key':'15|9|2026-11-01','show':'15|9'})],
        'w-horses':[row('recH',**{'horse-key':'1','shows':'15|9'})]}

class Fake:
    def __init__(self):self.rows=[];self.writes=[];self.fail=False;self.corrupt=False
    def records(self,*args,**kwargs):return copy.deepcopy(self.rows)
    def request(self,method,base,table,body):
        self.writes.append((method,copy.deepcopy(body)))
        for value in body['records']:
            if method=='POST':self.rows.append({'id':'rec'+str(len(self.rows)),'fields':copy.deepcopy(value['fields'])})
            else:next(r for r in self.rows if r['id']==value['id'])['fields'].update(value['fields'])
        if self.fail:raise TimeoutError('Unknown write outcome')
        if self.corrupt:self.rows[-1]['fields']['person-name']='wrong'
        return {'records':self.rows}

SCHEMA={'tables':{'people':{'id':'tblPeople','fields':[]}}}
class JoinTests(unittest.TestCase):
    def test_role_collision_and_no_attendance_horse_inference(self):
        wanted,issues=join.project(sources())
        self.assertEqual(set(wanted),{'rider:1','trainer:1'})
        self.assertNotIn('horses',wanted['rider:1'])
        self.assertEqual(wanted['rider:1']['days'],['recD'])
        self.assertTrue(any('cross-role' in i['message'] for i in issues))
    def test_duplicate_and_invalid_identity_retained(self):
        s=sources();s['w-riders'].append(row('recR2',**s['w-riders'][0]['fields']))
        s['w-riders'].append(row('recR3',**{'rider-name':'No ID'}))
        wanted,_=join.project(s)
        self.assertEqual(len(wanted),4)
        self.assertIn('rider:record:recR3',wanted)
    def test_missing_and_conflicting_memberships_withheld_locally(self):
        s=sources();s['w-riders'][0]['fields']['shows']='15|9, absent'
        s['w-days'][0]['fields']['show']='15|8'
        wanted,issues=join.project(s)
        self.assertEqual(wanted['rider:1']['shows'],['recS'])
        self.assertEqual(wanted['rider:1']['days'],[])
        self.assertTrue(any('mismatch' in i['message'] for i in issues))
    def test_repeat_unchanged_and_preserved_native_ids_links(self):
        api=Fake();wanted,issues=join.project(sources())
        first=join.apply(api,SCHEMA,wanted,issues)
        self.assertEqual(first['added'],2)
        ids=[r['id'] for r in api.rows];calls=len(api.writes)
        second=join.apply(api,SCHEMA,wanted,[])
        self.assertEqual(second['unchanged'],2);self.assertEqual(len(api.writes),calls)
        self.assertEqual([r['id'] for r in api.rows],ids)
        api.rows[0]['fields']['shows'].append('recHistorical')
        wanted['rider:1']['person-name']='New name'
        join.apply(api,SCHEMA,wanted,[])
        self.assertIn('recHistorical',api.rows[0]['fields']['shows'])
    def test_duplicate_destination_does_not_block_valid_people(self):
        api=Fake();wanted,issues=join.project(sources())
        api.rows=[row('rec1',**wanted['rider:1']),row('rec2',**wanted['rider:1'])]
        counts=join.apply(api,SCHEMA,wanted,issues)
        self.assertEqual(counts['withheld'],1);self.assertEqual(counts['added'],1)
    def test_uncertain_write_not_retried(self):
        api=Fake();api.fail=True;wanted,issues=join.project(sources())
        with self.assertRaises(TimeoutError):join.apply(api,SCHEMA,wanted,issues)
        self.assertEqual(len(api.writes),1)
    def test_readback_mismatch_fails(self):
        api=Fake();api.corrupt=True;wanted,issues=join.project(sources())
        with self.assertRaisesRegex(ValueError,'readback'):join.apply(api,SCHEMA,wanted,issues)
    def test_source_clock_not_renewed_or_regressed(self):
        api=Fake();wanted,issues=join.project(sources());join.apply(api,SCHEMA,wanted,issues)
        wanted['rider:1']['source-observed-at']='2026-10-30T12:00:00Z'
        join.apply(api,SCHEMA,wanted,[])
        self.assertEqual(api.rows[0]['fields']['source-observed-at'],'2026-11-01T12:00:00Z')
    def test_more_than_one_page_and_batch(self):
        s=sources();s['w-riders']=[row('recR'+str(n),**{'rider-key':str(n),'rider-id':n}) for n in range(205)]
        wanted,issues=join.project(s);api=Fake();join.apply(api,SCHEMA,wanted,issues)
        self.assertEqual(len(api.rows),206)
        self.assertTrue(all(len(v['records'])<=10 for _,v in api.writes))
    def test_empty_sources_do_not_invent_excluded_requirements(self):
        wanted,issues=join.project({k:[] for k in sources()})
        self.assertEqual(wanted,{})
        self.assertEqual(len(issues),0)

if __name__=='__main__':unittest.main()
