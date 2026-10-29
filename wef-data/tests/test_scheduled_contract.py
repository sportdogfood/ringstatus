"""Offline integration at HTTP/storage boundaries; never invokes a live collector."""
import copy, datetime, json, pathlib, re, sys, tempfile, unittest
from unittest.mock import patch
ROOT=pathlib.Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT))
from test_store import MemoryAPI
from data_runner import Repository,Runner,refs,stamp,KEYS,run
from data_contract import packed,thaw

SHOW='15|200000076'
CONTEXT={'show-key':SHOW,'customer-id':15,'show-id':200000076,'show-name':'TEST',
    'start-date':'2026-10-29','end-date':'2026-10-29','selected-date':'2026-10-29','default':True,'last-checked':stamp()}

class API(MemoryAPI):
    def __init__(self):super().__init__();self.context={'authority-base':'appCaSmL819KK7UFJ','records':[copy.deepcopy(CONTEXT)]}
    def canonical_context(self):return copy.deepcopy(self.context)
    def records(self,base,table,fields=None,formula=None):
        rows=super().records(base,table,fields)
        if formula:
            keys=re.findall(r'\{([^}]+)\}=("(?:[^"\\]|\\.)*")',formula)
            fieldmap={f['name']:f['id'] for t in SCHEMA['tables'].values() if t['id']==table for f in t['fields']}
            rows=[r for r in rows if any(str(r['fields'].get(fieldmap[name]))==json.loads(value) for name,value in keys)]
        return rows

SCHEMA=json.loads((ROOT/'schema.json').read_text())
AUTHORED=json.loads((ROOT/'authored-fields.json').read_text())

class Source:
    def __init__(self):self.calls=[];self.empty=False;self.horse='HÖRSE'
    def fetch(self,route,params):
        self.calls.append((route,params.copy()));now=stamp()
        if route=='/schedule':
            value={'show':{'show_id':200000076,'facility_id':500,'start_date':'2026-10-29','end_date':'2026-10-29'},'show_date':params['date'],
                'rings':[{'ring_number':1,'ring_id':10,'ring_name':'RING','classes':[{'class_id':2,'class_group_id':3,'class_number':4,'class_name':'CLASS','class_groupxclasses_id':5}]}]}
        elif route=='/entries':
            eid=20+params['page'];value={'show_id':'200000076','entries':[{'entry_id':eid,'show_id':200000076,'customer_id':15,'number':eid,
                'horse_id':100+eid,'horse':self.horse,'riders':[{'rider_id':50,'rider_name':'RIDER'}],'trainer_id':60,'trainer':'TRAINER'}],
                'total_entries':2,'records_per_page':1,'page':params['page'],'sort':{'sort_on':'number','sort_type':'asc'},'term':''}
        else:
            value={'show_id':200000076,'class':{'class_id':2},'class_related_data':{'date':'2026-10-29','ring':1,'class_group_id':3,'status':'Completed'},
                'total_entry_trips':1,'trips':[{'entry_id':21,'horse_id':121,'rider_id':50,'rider_name':'RIDER','trainer_id':60,'horse':'HORSE',
                    'trainer':'TRAINER','entryxclasses_uuid':'uuid-existing','trip_id':9,'number':21}]}
            value['trips'][0]['horse']=self.horse
        if self.empty and route.startswith('/classes/'):
            value['trips']=[];value['total_entry_trips']=0
        return value,{'route':route,'params':params,'started':now,'completed':now,'elapsed_seconds':0.1,'bytes':len(packed(value)),
            'sha256':__import__('hashlib').sha256(packed(value).encode()).hexdigest(),'http':200}

def seed(api):
    fields={f['name']:f['id'] for f in SCHEMA['tables']['lanes']['fields']}
    rows=[]
    for lane,interval in [('inventory',300),('current',90),('today',300),('other',1800),('completed',3600),('reconcile',7200),('recovery',60)]:
        rows.append({'id':'recLane'+lane,'fields':{fields['lane-key']:lane,fields['interval-seconds']:interval,
            fields['minimum-seconds']:interval,fields['maximum-seconds']:interval*4,fields['priority']:50}})
    api.db[SCHEMA['tables']['lanes']['id']]=rows

class ScheduledContractTests(unittest.TestCase):
    def test_failed_run_resume_requires_explicit_matching_authorization_and_keeps_keys(self):
        api=API();seed(api);source=Source()
        with tempfile.TemporaryDirectory() as folder:
            runner=Runner(Repository(api,SCHEMA,AUTHORED),source,folder,folder);runner.begin(SHOW,{})
            key=runner.runkey;runner.repo.patch('runs',key,{'status':'Failed; prior accepted state retained'})
            replay=Runner(Repository(api,SCHEMA,AUTHORED),source,folder,folder)
            with self.assertRaisesRegex(ValueError,'not resumable'):replay.recover(api.canonical_context())
            pathlib.Path(folder,'resume-approved.json').write_text(packed({'run-key':key,'approval':'Owner APPROVED TO EDIT source transport and same checkpoint resume'}),encoding='utf-8')
            identity=replay.repo.inventory['runs'][key]['id']
            self.assertTrue(replay.recover(api.canonical_context()));self.assertEqual(replay.runkey,key)
            self.assertEqual(replay.repo.inventory['runs'][key]['id'],identity);self.assertEqual(source.calls,[])
            self.assertFalse(pathlib.Path(folder,'resume-approved.json').exists())
    def test_shared_entities_do_not_regress_in_concurrent_selection_order(self):
        class TwoClasses(Source):
            def fetch(self,route,params):
                payload,metric=super().fetch(route,params)
                if route=='/schedule':
                    another=copy.deepcopy(payload['rings'][0]['classes'][0]);another['class_id']=3;another['class_groupxclasses_id']=6
                    payload['rings'][0]['classes'].append(another)
                elif route=='/entries':payload['total_entries']=1
                else:
                    cid=int(route.rsplit('/',1)[1]);payload['class']['class_id']=cid
                    newer=cid==2;payload['trips'][0]['horse']='NEWER' if newer else 'OLDER'
                    payload['trips'][0]['rider_name']='NEWER RIDER' if newer else 'OLDER RIDER'
                    metric['completed']=(datetime.datetime.now(datetime.timezone.utc)+datetime.timedelta(seconds=2 if newer else 1)).isoformat()
                return payload,metric
        api=API();seed(api)
        with tempfile.TemporaryDirectory() as folder:
            result=Runner(Repository(api,SCHEMA,AUTHORED),TwoClasses(),folder,folder).tick()
            repo=Repository(api,SCHEMA,AUTHORED)
            self.assertTrue(result['ok']);self.assertEqual(repo.graph['horses']['121']['horse-name'],'NEWER')
            self.assertEqual(repo.graph['riders']['50']['rider-name'],'NEWER RIDER')
            self.assertEqual(repo.graph['entries']['200000076|21']['last-success'],repo.graph['horses']['121']['last-success'])
    def test_tick_recovery_uses_reserved_order_before_new_admission(self):
        api=API();seed(api);source=Source()
        with tempfile.TemporaryDirectory() as folder:
            Runner(Repository(api,SCHEMA,AUTHORED),source,folder,folder).tick()
            from data_runner import latest_state
            repo=Repository(api,SCHEMA,AUTHORED);runner=Runner(repo,source,folder,folder)
            runner.begin(SHOW,latest_state(list(repo.graph['runs'].values()),SHOW))
            selected=[json.loads(next(iter(repo.graph['work-items'].values()))['state-json'])]
            with patch.object(runner,'observation',side_effect=KeyboardInterrupt):
                with self.assertRaises(KeyboardInterrupt):runner.class_batch(selected,repo.scoped(SHOW),repo.graph['lanes'])
            source.calls=[]
            replay=Runner(Repository(api,SCHEMA,AUTHORED),source,folder,folder)
            # Proves tick restores exact old scope rather than consulting a new due order first.
            result=replay.tick();self.assertTrue(result['ok']);self.assertEqual(source.calls,[])
    def test_source_request_receipts_equal_actual_calls(self):
        api=API();seed(api);source=Source()
        with tempfile.TemporaryDirectory() as folder:
            result=Runner(Repository(api,SCHEMA,AUTHORED),source,folder,folder).tick()
            repo=Repository(api,SCHEMA,AUTHORED)
            self.assertEqual(repo.graph['runs'][result['run-key']]['source-requests'],len(source.calls))
            self.assertEqual(len(repo.graph['requests']),len(source.calls))
    def test_completed_chain_reconstructs_immutable_context_ids_and_show_only_membership(self):
        from wef_data_handoff import revision_chain,show_memberships
        api=API();seed(api);source=Source()
        with tempfile.TemporaryDirectory() as folder:
            runner=Runner(Repository(api,SCHEMA,AUTHORED),source,folder,folder);runner.tick()
            repo=Repository(api,SCHEMA,AUTHORED);head=Runner(repo,source,folder,folder).tick()['run-key']
            repo=Repository(api,SCHEMA,AUTHORED)
            collections={k:{**v,'_record-id':repo.inventory['collections'][k]['id']} for k,v in repo.graph['collections'].items()}
            chain=revision_chain(head,repo.graph['runs'],collections);self.assertEqual(len(chain),2)
            facts={t:{} for t in KEYS};ids={t:{} for t in KEYS}
            for state,units in chain:
                for unit in units:
                    if unit['scope']=='retained-bootstrap':
                        for table,rows in unit['source']['retained-projection'].items():facts[table].update({r[KEYS[table]]:r for r in rows})
                        for table,mapping in unit['source']['record-map'].items():ids[table].update(mapping)
                    for table,rows in unit['graph'].items():
                        for row in rows:facts[table].setdefault(row[KEYS[table]],{}).update(row)
                    for table,mapping in unit['verification']['record-map'].items():ids[table].update(mapping)
            self.assertEqual(facts['classes'][SHOW+'|2026-10-29|1|2']['day'],refs('days',SHOW+'|2026-10-29'))
            self.assertEqual(ids['entries']['200000076|22'],repo.inventory['entries']['200000076|22']['id'])
            self.assertEqual(show_memberships(facts)['horses']['122'],[SHOW])
            collections[next(iter(collections))]['_record-id']='recChanged'
            with self.assertRaises(ValueError):revision_chain(head,repo.graph['runs'],collections)
    def test_second_due_inventory_and_unicode_source_names(self):
        api=API();seed(api);source=Source()
        with tempfile.TemporaryDirectory() as folder:
            runner=Runner(Repository(api,SCHEMA,AUTHORED),source,folder,folder);runner.tick()
            repo=Repository(api,SCHEMA,AUTHORED)
            self.assertEqual(repo.graph['horses']['121']['horse-name'],'HÖRSE')
            from data_runner import latest_state
            state=latest_state(list(repo.graph['runs'].values()),SHOW);runner=Runner(repo,source,folder,folder)
            runner.begin(SHOW,state);self.assertIsNone(runner.state.get('inventory-progress'))
            runner.inventory(CONTEXT);runner.finish(api.canonical_context())
            self.assertEqual(repo.graph['horses']['121']['horse-name'],'HÖRSE')
    def test_class_source_files_resume_after_process_death_without_recollecting(self):
        api=API();seed(api);source=Source()
        with tempfile.TemporaryDirectory() as folder:
            runner=Runner(Repository(api,SCHEMA,AUTHORED),source,folder,folder);runner.tick()
            repo=Repository(api,SCHEMA,AUTHORED);runner=Runner(repo,source,folder,folder);runner.begin(SHOW,{})
            state=json.loads(next(iter(repo.graph['work-items'].values()))['state-json'])
            with patch.object(runner,'observation',side_effect=KeyboardInterrupt):
                with self.assertRaises(KeyboardInterrupt):runner.class_batch([state],repo.scoped(SHOW),repo.graph['lanes'])
            count=len(source.calls)
            replay=Runner(Repository(api,SCHEMA,AUTHORED),source,folder,folder);self.assertTrue(replay.recover(api.canonical_context()))
            replay.class_batch([state],replay.repo.scoped(SHOW),replay.repo.graph['lanes']);replay.finish(api.canonical_context())
            self.assertEqual(len(source.calls),count)
    def test_positive_drop_then_repeat_does_not_reactivate_or_delete_trip(self):
        api=API();seed(api);source=Source()
        with tempfile.TemporaryDirectory() as folder:
            runner=Runner(Repository(api,SCHEMA,AUTHORED),source,folder,folder);runner.tick()
            source.empty=True
            for i in range(2):
                repo=Repository(api,SCHEMA,AUTHORED);identity=repo.inventory['trips'][SHOW+'|2026-10-29|1|2|21|uuid-existing']['id']
                runner=Runner(repo,source,folder,folder);runner.begin(SHOW,{})
                state=json.loads(next(iter(repo.graph['work-items'].values()))['state-json'])
                runner.class_batch([state],repo.scoped(SHOW),repo.graph['lanes']);runner.finish(api.canonical_context())
                after=Repository(api,SCHEMA,AUTHORED)
                self.assertEqual(len(after.graph['trips']),1)
                trip=next(iter(after.graph['trips'].values()));self.assertEqual(trip['record-status'],'dropped')
                self.assertEqual(next(iter(after.inventory['trips'].values()))['id'],identity)
                self.assertEqual(len(after.graph['results']),1)
    def test_interrupted_unit_replays_same_reserved_keys_without_source_call(self):
        api=API();seed(api);source=Source()
        with tempfile.TemporaryDirectory() as folder:
            repo=Repository(api,SCHEMA,AUTHORED);runner=Runner(repo,source,folder,folder);runner.tick()
            runner.begin(SHOW,{})
            payload,metric=source.fetch('/entries',{'page':1});runkey=runner.runkey
            # Process death occurs before stage() can finish. No production failure/retry is manufactured.
            with patch.object(repo,'stage',side_effect=KeyboardInterrupt):
                with self.assertRaises(KeyboardInterrupt):runner.observation('entry-directory-page',payload,metric,{}, {'directory':{'next':2}})
            count=len(source.calls)
            replay=Runner(Repository(api,SCHEMA,AUTHORED),source,folder,folder)
            self.assertTrue(replay.recover(api.canonical_context()));self.assertEqual(replay.runkey,runkey)
            self.assertEqual(replay.state['directory']['next'],2);self.assertEqual(len(source.calls),count)
            replay.finish(api.canonical_context())
            self.assertEqual(len(replay.state['observations']),1)
    def test_new_show_pages_class_and_unchanged_repeat_keep_ids(self):
        api=API();seed(api);source=Source()
        with tempfile.TemporaryDirectory() as folder:
            repo=Repository(api,SCHEMA,AUTHORED);runner=Runner(repo,source,folder,folder);first=runner.tick()
            self.assertTrue(first['ok']);self.assertEqual([p['page'] for r,p in source.calls if r=='/entries'],[1,2,1])
            current=Repository(api,SCHEMA,AUTHORED)
            entry=current.graph['entries']['200000076|21'];self.assertEqual(entry['horse'],refs('horses','121'))
            self.assertEqual(set(current.graph['trips']),{SHOW+'|2026-10-29|1|2|21|uuid-existing'})
            directory_only=current.graph['entries']['200000076|22'];self.assertEqual(directory_only['show'],refs('shows',SHOW))
            self.assertNotIn('day',directory_only)
            ids={t:{k:r['id'] for k,r in rows.items()} for t,rows in current.inventory.items() if t in ('horses','entries','riders','trainers','classes','trips')}
            prior=copy.deepcopy(api.db[SCHEMA['tables']['runs']['id']]);source.calls=[]
            result=Runner(current,source,folder,folder).tick();self.assertTrue(result['ok']);self.assertEqual(source.calls,[])
            after=Repository(api,SCHEMA,AUTHORED)
            self.assertEqual(ids,{t:{k:r['id'] for k,r in after.inventory[t].items()} for t in ids})
            for row in prior:self.assertIn(row,api.db[SCHEMA['tables']['runs']['id']])
            for r in after.graph['runs'].values():self.assertEqual(r['status'],'Verified completed revision')
    def test_changed_authority_withholds_completion(self):
        api=API();seed(api)
        with tempfile.TemporaryDirectory() as folder:
            runner=Runner(Repository(api,SCHEMA,AUTHORED),Source(),folder,folder)
            runner.begin(SHOW,{})
            before=api.canonical_context();api.context['records'][0]['selected-date']='2026-10-30'
            with self.assertRaisesRegex(ValueError,'Canonical scope changed'):runner.finish(before)
            self.assertNotEqual(runner.repo.graph['runs'][runner.runkey]['status'],'Verified completed revision')
    def test_failure_stops_later_wakes_before_any_collection(self):
        with tempfile.TemporaryDirectory() as folder:
            with patch('data_runner.Repository',side_effect=RuntimeError('fixture failure')) as constructor:
                with self.assertRaises(RuntimeError):run(folder,ROOT/'schema.json',ROOT/'authored-fields.json',folder)
                self.assertEqual(constructor.call_count,1)
                stopped=run(folder,ROOT/'schema.json',ROOT/'authored-fields.json',folder)
                self.assertEqual(stopped['source-requests'],0);self.assertEqual(constructor.call_count,1)

if __name__=='__main__':unittest.main()
