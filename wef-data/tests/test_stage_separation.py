"""Offline storage/source boundaries for scalar collection and saved linking."""
import copy,datetime,json,pathlib,re,socket,tempfile,unittest
from unittest.mock import patch
from fast_fixture import API as BaseAPI,Source,SCHEMA
from test_store import MemoryAPI
import data_runner as dr
import linking
from record_mappings import RESULT_FIELDS
ROOT=pathlib.Path(dr.__file__).parent

class API(BaseAPI):
    def __init__(self):
        super().__init__();self.writes=[]
        self.context['records'][0]['facility-id']=500
    def records(self,base,table,fields=None,formula=None):
        if table in (dr.SYNC_SHOWS,dr.SYNC_DAYS):
            rows=super().records(base,table,fields,formula)
            if table==dr.SYNC_SHOWS:
                f=next(f for f in SCHEMA['tables']['wef-shows.shows']['fields'] if f['name']=='facility_id')
                rows[0]['fields'][f['id']]=500
            return rows
        rows=MemoryAPI.records(self,base,table,fields)
        self.reads.append((base,table,formula))
        definitions=next(t['fields'] for t in SCHEMA['tables'].values() if t['id']==table)
        ids={f['name']:f['id'] for f in definitions}
        if formula:
            left=re.findall(r'LEFT\(\{([^}]+)\},(\d+)\)=("(?:[^"\\]|\\.)*")',formula)
            exact=re.findall(r'\{([^}]+)\}=("(?:[^"\\]|\\.)*")',formula)
            if left:rows=[r for r in rows if any(str(r['fields'].get(ids[n],''))[:int(count)]==json.loads(v) for n,count,v in left)]
            elif exact:rows=[r for r in rows if any(str(r['fields'].get(ids[n]))==json.loads(v) for n,v in exact)]
            else:raise AssertionError('Unmodelled formula '+formula)
        return rows
    def request(self,method,base,table,params=None,body=None):
        if method!='GET':self.writes.append((table,copy.deepcopy(body)))
        return super().request(method,base,table,params,body)

def fields(table,values):
    m={f['name']:f['id'] for f in SCHEMA['tables'][table]['fields']}
    return {m[k]:v for k,v in values.items()}
def rows(api,table):
    m={f['id']:f['name'] for f in SCHEMA['tables'][table]['fields']}
    return [{**r,'fields':{m[k]:v for k,v in r['fields'].items()}} for r in api.db.get(SCHEMA['tables'][table]['id'],[])]

class StageSeparation(unittest.TestCase):
    def setUp(self):
        self.folder=tempfile.TemporaryDirectory();self.addCleanup(self.folder.cleanup)
        self.schema=pathlib.Path(self.folder.name)/'schema.json';self.schema.write_bytes((ROOT/'schema.json').read_bytes())
        self.api=API();self.api.days=[r for r in self.api.days if r['fields']['day-priority']==0]
        self.source=Source(self.api);self.attempt=0
        for table,values in [('shows',{'show-key':'15|200000076'}),('days',{'day-key':'15|200000076|2026-11-10','date':'2026-11-10'})]:
            self.api.db[SCHEMA['tables'][table]['id']]=[{'id':'native-'+table,'fields':fields(table,values)}]
        self.socket=patch.object(socket,'create_connection',side_effect=AssertionError('network forbidden'));self.socket.start();self.addCleanup(self.socket.stop)
    def fast(self):
        self.attempt+=1
        fetch=self.source.fetch
        def timed(route,params):
            self.api.at+=1;return fetch(route,params)
        with patch.object(dr,'Airtable',return_value=self.api),patch.object(dr,'Source',return_value=self.source),patch.object(dr.time,'time',side_effect=lambda:self.api.at),patch.object(dr,'stamp',side_effect=lambda:datetime.datetime.fromtimestamp(self.api.at,datetime.timezone.utc).isoformat()),patch.object(self.source,'fetch',side_effect=timed),patch.dict(dr.os.environ,{'WEF_LOG_ATTEMPT_ID':'fast-'+str(self.attempt)}):
            result=dr.run(self.schema)
            (self.schema.parent/'runtime/last-run.json').write_text(json.dumps(result),encoding='utf-8')
            return result
    def masters(self):
        for table,key,idfield in [('w-horses','1100','horse-id'),('w-riders','50','rider-id'),('w-trainers','60','trainer-id')]:
            self.api.db[SCHEMA['tables'][table]['id']]=[{'id':'master-'+table,'fields':fields(table,{table[2:-1]+'-key':key,idfield:int(key)})}]
        self.api.db[SCHEMA['tables']['w-rings']['id']]=[
            {'id':'wrong-facility','fields':fields('w-rings',{'ring-uid':'15|no:1|id:10-wrong','ring-key':'15|999|id:10','ring-id':10})},
            {'id':'master-ring','fields':fields('w-rings',{'ring-uid':'15|no:1|id:10','ring-key':'15|500|id:10','ring-id':10})}]
        fast=rows(self.api,'runs')[-1]['fields']['run-key']
        self.api.db[SCHEMA['tables']['run-logs']['id']]=[{'id':'operational-fast','fields':fields('run-logs',{'attempt-id':'fast-op','run-key':fast,'workflow':'wef-data'})}]
    def link(self):
        with patch.object(linking,'Airtable',return_value=self.api),patch.dict(dr.os.environ,{'WEF_LOG_ATTEMPT_ID':'linker-own-attempt'}):return linking.run(self.schema)
    def test_fast_writes_scalars_results_and_no_native_links(self):
        result=self.fast();self.assertTrue(result['ok'],result)
        types={t['id']:{f['id']:f['type'] for f in t['fields']} for t in SCHEMA['tables'].values()}
        for table,body in self.api.writes:
            for row in body['records']:
                self.assertFalse(any(types[table][f]=='multipleRecordLinks' for f in row['fields']))
        resultrow=rows(self.api,'results')[0]['fields'];trip=rows(self.api,'trips')[0]['fields'];cls=rows(self.api,'classes')[0]['fields']
        self.assertEqual(resultrow['result-key'],trip['trip-key']);self.assertEqual(resultrow['class-status'],cls['status']);self.assertIsNone(resultrow['placing'])
        self.assertFalse(rows(self.api,'horses'));self.assertFalse(rows(self.api,'riders'));self.assertFalse(rows(self.api,'trainers'))
    def test_results_preserve_zero_and_clear_absent_source_values(self):
        self.fast();fetch=self.source.fetch
        def changed(route,params):
            body,metric=fetch(route,params)
            if route.startswith('/classes/'):
                body['trips'][0].update(placing=0,gone_in=0,score1=0)
            return body,metric
        with patch.object(self.source,'fetch',side_effect=changed):result=self.fast()
        self.assertTrue(result['ok'],result);r=rows(self.api,'results')[0]['fields']
        self.assertEqual((r['placing'],r['gone-in'],r['score1']),('0','0','0'));self.assertIsNone(r['score2'])
        self.assertEqual(set(json.loads(r['source-json'])),set(RESULT_FIELDS))
    def test_independent_linking_uses_saved_keys_master_context_and_fast_run(self):
        self.fast();self.masters();before=copy.deepcopy(self.api.db);self.api.writes=[]
        result=self.link();self.assertTrue(result['ok'],result)
        ring=rows(self.api,'rings')[0];cls=rows(self.api,'classes')[0];trip=rows(self.api,'trips')[0];res=rows(self.api,'results')[0];entry=rows(self.api,'entries')[0];fast=rows(self.api,'runs')[0]
        self.assertEqual(ring['fields']['w-rings'],['master-ring']);self.assertEqual(cls['fields']['wef-shows.shows'],['canonicalShow'])
        self.assertEqual(trip['fields']['class'],[cls['id']]);self.assertEqual(trip['fields']['entry'],[entry['id']]);self.assertEqual(res['fields']['trip'],[trip['id']])
        self.assertEqual(res['fields']['last-run'],[fast['id']]);self.assertEqual(trip['fields']['last-run'],[fast['id']])
        schedule=rows(self.api,'schedules')[0]['fields'];self.assertEqual(schedule['runs'],[fast['id']]);self.assertEqual(schedule['run-logs'],['operational-fast'])
        for table,body in self.api.writes:
            types={f['id']:f['type'] for t in SCHEMA['tables'].values() if t['id']==table for f in t['fields']}
            self.assertTrue(all(types[f]=='multipleRecordLinks' or ('performUpsert' in body and f in body['performUpsert']['fieldsToMergeOn']) for r in body['records'] for f in r['fields']))
        self.api.writes=[];again=self.link();self.assertTrue(again['ok'],again);self.assertEqual(again['counts']['saved'],0);self.assertEqual(self.api.writes,[])
    def test_missing_master_does_not_clear_saved_links_or_block_other_links(self):
        self.fast();self.masters();self.link();horse=rows(self.api,'horses')[0];field=next(f['id'] for f in SCHEMA['tables']['horses']['fields'] if f['name']=='w-horses')
        self.api.db[SCHEMA['tables']['horses']['id']][0]['fields'][field]=['preserved-owner-link']
        self.api.db[SCHEMA['tables']['w-horses']['id']]=[]
        trip_field=next(f['id'] for f in SCHEMA['tables']['results']['fields'] if f['name']=='trip')
        self.api.db[SCHEMA['tables']['results']['id']][0]['fields'].pop(trip_field)
        result=self.link();self.assertEqual(result['state'],'Partial');self.assertGreater(result['counts']['saved'],0)
        self.assertEqual(rows(self.api,'horses')[0]['fields']['w-horses'],['preserved-owner-link'])
    def test_later_unrelated_run_cannot_claim_saved_records(self):
        self.fast();self.masters();fast=rows(self.api,'runs')[0]
        self.api.db[SCHEMA['tables']['runs']['id']].append({'id':'unrelated-newer-run','fields':fields('runs',{'run-key':'collection-later','started':'2026-11-10T17:00:00Z','completed':'2026-11-10T17:01:00Z'})})
        self.assertTrue(self.link()['ok'])
        self.assertEqual(rows(self.api,'results')[0]['fields']['last-run'],[fast['id']])
    def test_negative_priority_rows_are_not_relinked(self):
        self.fast();self.masters()
        for day in self.api.days:day['fields']['day-priority']=-1
        self.api.writes=[];result=self.link()
        self.assertTrue(result['ok'],result);self.assertEqual(self.api.writes,[])
    def test_future_day_run_keys_survive_later_day_zero_only_wake(self):
        self.api.days=BaseAPI().days
        first=self.fast();self.assertEqual([r['priority'] for r in first['counts']['days']],[0,1,2,3])
        self.api.at+=180
        latest=self.fast();self.assertEqual([r['priority'] for r in latest['counts']['days']],[0])
        self.masters();self.api.reads=[];self.link()
        runids={r['fields']['run-key']:r['id'] for r in rows(self.api,'runs')}
        expected={r['day']:r['run-key'] for r in first['counts']['days']};expected[latest['day']]=latest['run-key']
        for r in rows(self.api,'results'):
            day=r['fields']['result-key'].split('|')[2]
            self.assertEqual(r['fields']['last-run'],[runids[expected[day]]])
        queries=[f for b,t,f in self.api.reads if t==SCHEMA['tables']['runs']['id']]
        self.assertTrue(queries);self.assertTrue(all(q.startswith('OR({run-key}=') for q in queries))
    def test_unfinished_old_run_cannot_claim_newer_saved_observation(self):
        self.fast();self.masters()
        run_fields={f['name']:f['id'] for f in SCHEMA['tables']['runs']['fields']}
        self.api.db[SCHEMA['tables']['runs']['id']][0]['fields'].pop(run_fields['completed'])
        result_field=next(f['id'] for f in SCHEMA['tables']['results']['fields'] if f['name']=='last-run')
        self.api.db[SCHEMA['tables']['results']['id']][0]['fields'][result_field]=['preserved-earlier-run']
        result=self.link();self.assertTrue(result['ok'],result)
        self.assertEqual(rows(self.api,'results')[0]['fields']['last-run'],['preserved-earlier-run'])
    def test_omitted_inventory_and_entry_values_preserve_baseline_fields(self):
        self.fast();fetch=self.source.fetch
        def omitted(route,params):
            body,metric=fetch(route,params)
            if route=='/schedule':
                body['rings'][0].pop('ring_id');body['rings'][0].pop('ring_name')
            else:body['trips'][0].pop('number')
            return body,metric
        with patch.object(self.source,'fetch',side_effect=omitted):result=self.fast()
        self.assertTrue(result['ok'],result)
        ring=rows(self.api,'rings')[0]['fields'];entry=rows(self.api,'entries')[0]['fields']
        self.assertEqual((ring['ring-id'],ring['ring-name'],entry['entry-number']),(10,'RING','100'))
    def test_one_write_failure_does_not_block_unaffected_tables(self):
        self.fast();self.masters();request=self.api.request
        def fail(method,base,table,params=None,body=None):
            if method=='PATCH' and table==SCHEMA['tables']['rings']['id']:raise RuntimeError('fixture link outage')
            return request(method,base,table,params,body)
        with patch.object(self.api,'request',side_effect=fail):result=self.link()
        self.assertEqual(result['state'],'Partial');self.assertIn('fixture link outage',str(result['item-errors']))
        self.assertTrue(rows(self.api,'results')[0]['fields']['trip'])
    def test_owner_occurrence_identity_and_result_mapping_reuse(self):
        from record_mappings import trip_records
        trip={'entry_id':123,'trip_id':456,'Owner':'Same display name','placing':0}
        graph=trip_records(trip,'15|76|2026-11-10|1|9','Completed','2026-11-10T16:00:00Z')
        self.assertEqual(graph['owners'][0]['owner-key'],'unknown-id|15|76|2026-11-10|1|9|123|456')
        self.assertEqual(graph['results'][0]['placing'],'0');self.assertIsNone(graph['results'][0]['score1'])

if __name__=='__main__':unittest.main()
