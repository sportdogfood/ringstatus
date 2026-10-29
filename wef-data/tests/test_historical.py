"""Regression against the exact existing 75 Git archive; no network or live writes."""
import copy,datetime,gzip,json,os,pathlib,subprocess,tempfile,unittest
from unittest.mock import patch
from test_scheduled_contract import API,SCHEMA,AUTHORED
from data_runner import Runner,Repository,KEYS
from wef_data_handoff import verify_snapshot

REPO=pathlib.Path(os.environ.get('WEF_HISTORICAL_ARCHIVE_ROOT','missing-read-only-fixture'))
PREFIX='wef-data/shows/15/200000075/4c88a40a2a03efa5d1e48c6be92f528139061f7bbb4d16bee26d3731df323d16'

@unittest.skipUnless((REPO/PREFIX/'manifest.json').exists(),'Pinned historical fixture not configured')
class HistoricalTests(unittest.TestCase):
    def test_all_3903_record_ids_and_closure_preserve_original_receipts(self):
        self.closure()
    def test_airtable_millisecond_readback_closes_without_changing_timestamps(self):
        self.closure(airtable_precision=True)
    def test_true_timestamp_regression_still_blocks_closure(self):
        with self.assertRaisesRegex(ValueError,'Historical shared timestamp regressed'):
            self.closure(airtable_precision=True,true_regression=True)
    def closure(self,airtable_precision=False,true_regression=False):
        manifest,tables=verify_snapshot(REPO/PREFIX);self.assertEqual(manifest['record_count'],3903)
        self.assertEqual(len(manifest['files']),20)
        api=API();mapping={}
        for table in KEYS:
            rows=json.loads(gzip.decompress((REPO/PREFIX/('tables/'+table+'.json.gz')).read_bytes()))
            mapping[table]={r['fields'][KEYS[table]]:r['id'] for r in rows}
        for table in KEYS:
            rows=json.loads(gzip.decompress((REPO/PREFIX/('tables/'+table+'.json.gz')).read_bytes()))
            fields={f['name']:f['id'] for f in SCHEMA['tables'][table]['fields']}
            types={f['name']:f['type'] for f in SCHEMA['tables'][table]['fields']}
            for row in rows:
                for name,value in list(row['fields'].items()):
                    if isinstance(value,dict) and '$links' in value:value=[mapping[t][k] for t,k in value['$links']]
                    if airtable_precision and types[name]=='dateTime' and value:
                        value=datetime.datetime.fromisoformat(value.replace('Z','+00:00')).isoformat(timespec='milliseconds').replace('+00:00','Z')
                    row['fields'][fields[name]]=value;row['fields'].pop(name)
            api.db[SCHEMA['tables'][table]['id']]=rows
        if true_regression:
            field=next(f['id'] for f in SCHEMA['tables']['horses']['fields'] if f['name']=='last-success')
            horse=api.db[SCHEMA['tables']['horses']['id']][0]
            horse['fields'][field]=(datetime.datetime.fromisoformat(horse['fields'][field].replace('Z','+00:00'))-datetime.timedelta(seconds=1)).isoformat()
        timestamps={table:{r['id']:r['fields'].get(next(f['id'] for f in SCHEMA['tables'][table]['fields'] if f['name']=='last-success')) for r in api.db[SCHEMA['tables'][table]['id']]} for table in ('horses','riders','trainers','owners')}
        original=copy.deepcopy(api.db[SCHEMA['tables']['runs']['id']])
        with tempfile.TemporaryDirectory() as folder:
            runner=Runner(Repository(api,SCHEMA,AUTHORED),None,folder,REPO)
            # Suppress only fetch/ancestry network checks. Exact git-show bytes remain real local reads.
            actual_run=subprocess.run
            def offline(command,**options):
                if command[3] in ('fetch','merge-base'):return subprocess.CompletedProcess(command,0)
                return actual_run(command,**options)
            with patch('data_runner.subprocess.run',side_effect=offline):
                runner.close75(api.canonical_context())
            after=Repository(api,SCHEMA,AUTHORED)
            self.assertEqual(after.graph['shows']['15|200000075']['status'],'Archived retained snapshot')
            for row in original:self.assertIn(row,api.db[SCHEMA['tables']['runs']['id']])
            for table,keys in mapping.items():
                for key,identity in keys.items():self.assertEqual(after.inventory[table][key]['id'],identity)
            for table,rows in timestamps.items():
                field=next(f['id'] for f in SCHEMA['tables'][table]['fields'] if f['name']=='last-success')
                self.assertEqual(rows,{r['id']:r['fields'].get(field) for r in api.db[SCHEMA['tables'][table]['id']]})
            count=len(api.creates)
            with patch('data_runner.subprocess.run',side_effect=offline):Runner(after,None,folder,REPO).close75(api.canonical_context())
            self.assertEqual(len(api.creates),count)

if __name__=='__main__':unittest.main()
