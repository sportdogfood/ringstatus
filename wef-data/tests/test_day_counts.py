"""Offline count policy and immutable scheduled handoff acceptance."""
import copy, json, pathlib, sys, tempfile, unittest
sys.path.insert(0,str(pathlib.Path(__file__).resolve().parents[1]))
from test_scheduled_contract import API, SCHEMA, AUTHORED, Source, seed, SHOW, CONTEXT
from data_runner import Runner, Repository
from day_counts import accepted_summaries, summaries, validate_summary

class DayCountTests(unittest.TestCase):
    def test_scheduled_repeat_replays_exact_summary_without_new_source(self):
        api=API();seed(api);source=Source()
        with tempfile.TemporaryDirectory() as folder:
            runner=Runner(Repository(api,SCHEMA,AUTHORED),source,folder,folder)
            first=runner.tick();calls=len(source.calls)
            repo=Repository(api,SCHEMA,AUTHORED)
            collections={k:{**v,'_record-id':repo.inventory['collections'][k]['id']} for k,v in repo.graph['collections'].items()}
            output=accepted_summaries(first['run-key'],repo.graph['runs'],collections,CONTEXT)
            body=output[0]['body'];self.assertEqual(body['unique-entry-count'],1)
            self.assertEqual(body['proven-trip-count'],1)
            self.assertEqual(body['all-dated-activity-completeness'],'unknown')
            self.assertEqual(body['memberships']['entry-keys'],['200000076|21'])
            self.assertEqual(len(source.calls),calls)
            self.assertTrue(body['input-collections'])
            with self.assertRaisesRegex(ValueError,'fence'):
                accepted_summaries(first['run-key'],repo.graph['runs'],collections,{**CONTEXT,'selected-date':'2026-10-30'})
            changed=copy.deepcopy(output[0]);changed['body']['unique-entry-count']=0
            with self.assertRaises(ValueError):validate_summary(changed,CONTEXT)
            # A newly hashed self-consistent false body still fails immutable-input recomputation.
            from data_contract import freeze,thaw,packed,digest
            bad=copy.deepcopy(collections);state=json.loads(repo.graph['runs'][first['run-key']]['state-json'])
            reference=state['observations'][-1];unit=thaw([bad[reference['key']]])
            unit['source']['body']['unique-entry-count']=0;unit['source']['body']['memberships']['entry-keys']=[]
            unit['source']['sha256']=digest(packed(unit['source']['body']))
            part=freeze(unit)[0];bad[reference['key']].update(part);reference['sha256']=part['retained-sha256']
            badruns=copy.deepcopy(repo.graph['runs']);badruns[first['run-key']]['state-json']=packed(state)
            with self.assertRaisesRegex(ValueError,'immutable inputs'):accepted_summaries(first['run-key'],badruns,bad,CONTEXT)
            second=Runner(repo,source,folder,folder).tick()
            repo=Repository(api,SCHEMA,AUTHORED)
            collections={k:{**v,'_record-id':repo.inventory['collections'][k]['id']} for k,v in repo.graph['collections'].items()}
            again=accepted_summaries(second['run-key'],repo.graph['runs'],collections,CONTEXT)[0]['body']
            self.assertEqual(again['accepted-as-of'],body['accepted-as-of'])
            self.assertEqual(again['memberships'],body['memberships'])
            collections[body['input-collections'][0]['key']]['_record-id']='wrong'
            with self.assertRaises(ValueError):accepted_summaries(second['run-key'],repo.graph['runs'],collections,CONTEXT)

    def test_summary_interruption_resumes_reserved_parts_without_duplicate(self):
        from unittest.mock import patch
        api=API();seed(api);source=Source()
        with tempfile.TemporaryDirectory() as folder:
            runner=Runner(Repository(api,SCHEMA,AUTHORED),source,folder,folder)
            original=runner.finish
            with patch.object(runner,'finish',side_effect=KeyboardInterrupt):
                with self.assertRaises(KeyboardInterrupt):runner.tick()
            runkey=runner.runkey;before=list(runner.state['observations']);calls=len(source.calls)
            runner.state['inventory-due']=0;runner.journal()
            replay=Runner(Repository(api,SCHEMA,AUTHORED),source,folder,folder)
            result=replay.tick()
            self.assertEqual(result['run-key'],runkey);self.assertEqual(replay.state['observations'],before)
            self.assertEqual(len(source.calls),calls)

    def fixture(self,empty=False):
        source=Source();source.empty=empty
        inventory,im=source.fetch('/schedule',{'date':'2026-10-29'})
        payload,cm=source.fetch('/classes/2',{})
        return [({'scope':'dated-inventory-page','source':inventory,'metric':im},[{'key':'inventory','record-id':'recInventory','sha256':'a'*64}]),
                ({'scope':'class','source':{'classes':[payload]},'metric':{'requests':[cm]}},[{'key':'class','record-id':'recClass','sha256':'b'*64}])]

    def test_empty_missing_scratch_drop_dedup_and_stale_times(self):
        units=self.fixture();payload=units[1][0]['source']['classes'][0]
        payload['trips'][0]['scratch_trip']=1
        body=summaries(CONTEXT,units,'run',None,'2026-10-29T14:00:00Z')[0]['body']
        self.assertEqual(body['scratch-count'],1);self.assertEqual(body['proven-trip-count'],1)
        payload2=copy.deepcopy(payload);payload2['class']['class_id']=3
        units[0][0]['source']['rings'][0]['classes'].append({'class_id':3})
        units[1][0]['source']['classes'].append(payload2)
        units[1][0]['metric']['requests']*=2
        body=summaries(CONTEXT,units,'run',None,'2026-10-29T14:00:00Z')[0]['body']
        self.assertEqual(body['unique-entry-count'],1);self.assertEqual(body['proven-trip-count'],2)
        missing=summaries(CONTEXT,units[:1],'run',None,'2026-10-29T14:00:00Z')[0]['body']
        self.assertIsNone(missing['unique-entry-count']);self.assertEqual(missing['completeness'],'unknown')
        empty=summaries(CONTEXT,self.fixture(True),'run',None,'2026-10-29T14:00:00Z')[0]['body']
        self.assertEqual(empty['unique-entry-count'],0);self.assertEqual(empty['completeness'],'complete')
        self.assertEqual(empty['all-dated-activity-completeness'],'unknown')
        self.assertNotEqual(empty['accepted-as-of'],empty['verified-at'])
        units=self.fixture();later=self.fixture(True)[1]
        units[1][0]['metric']['requests'][0].update(started='2026-10-29T12:00:00Z',completed='2026-10-29T12:00:00Z')
        later[0]['metric']['requests'][0].update(started='2026-10-29T13:00:00Z',completed='2026-10-29T13:00:00Z')
        later[1][0]['key']='class-removal';units.append(later)
        dropped=summaries(CONTEXT,units,'run',None,'2026-10-29T14:00:00Z')[0]['body']
        self.assertEqual(dropped['drop-count'],1);self.assertEqual(dropped['proven-trip-count'],0)
        self.assertEqual({r['key'] for r in dropped['input-collections']},{'inventory','class','class-removal'})

    def test_wrong_context_invalid_identity_and_missing_day_stay_safe(self):
        units=self.fixture();units[1][0]['source']['classes'][0]['trips'][0].pop('entry_id')
        body=summaries(CONTEXT,units,'run',None,'2026-10-29T14:00:00Z')[0]['body']
        self.assertIsNone(body['unique-entry-count']);self.assertEqual(body['completeness'],'unknown')
        wrong={**CONTEXT,'show-key':'15|1','show-id':1}
        with self.assertRaises(ValueError):summaries(wrong,self.fixture(),'run',None,'2026-10-29T14:00:00Z')
        context={**CONTEXT,'end-date':'2026-10-30'}
        day=summaries(context,self.fixture(),'run',None,'2026-10-29T14:00:00Z')[1]['body']
        self.assertIsNone(day['unique-entry-count']);self.assertEqual(day['completeness'],'unknown')

    def test_source_identity_conflicts_and_invalid_types_never_claim_complete(self):
        for identity in (True,{},' ',['uuid']):
            units=self.fixture();payload=units[1][0]['source']['classes'][0]
            payload['trips'][0]['entryxclasses_uuid']=identity
            if not identity:payload['trips'][0]['trip_id']=identity
            body=summaries(CONTEXT,units,'run',None,'2026-10-29T14:00:00Z')[0]['body']
            self.assertEqual(body['completeness'],'unknown');self.assertIsNone(body['proven-trip-count'])
        units=self.fixture();payload=units[1][0]['source']['classes'][0]
        payload['trips'].append({**payload['trips'][0],'entry_id':22});payload['total_entry_trips']=2
        body=summaries(CONTEXT,units,'run',None,'2026-10-29T14:00:00Z')[0]['body']
        self.assertEqual(body['completeness'],'unknown');self.assertTrue(body['outcomes']['invalid-identities'])
        empty=self.fixture(True)[1];empty[1][0]['key']='valid-empty'
        body=summaries(CONTEXT,[*units,empty],'run',None,'2026-10-29T14:00:00Z')[0]['body']
        self.assertEqual(body['drop-count'],0);self.assertEqual(body['memberships']['dropped-trip-keys'],[])
        units=self.fixture();payload=units[1][0]['source']['classes'][0];payload['trips'][0].pop('entryxclasses_uuid')
        body=summaries(CONTEXT,units,'run',None,'2026-10-29T14:00:00Z')[0]['body']
        self.assertEqual(body['proven-trip-count'],1);self.assertTrue(body['memberships']['trip-keys'][0].endswith('|9'))

if __name__=='__main__':unittest.main()
