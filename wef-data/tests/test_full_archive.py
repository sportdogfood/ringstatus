import pathlib,sys,json,tempfile,unittest,importlib.util,gzip,copy
ROOT=pathlib.Path(__file__).resolve().parents[2];sys.path[:0]=[str(ROOT/'wef-data'),str(ROOT/'wef-schedules/tests')]
spec=importlib.util.spec_from_file_location('archive_test_source',ROOT/'wef-schedules/tests/archive-wef-data.py');archive=importlib.util.module_from_spec(spec);spec.loader.exec_module(archive)
from wef_data_handoff import verify_snapshot

class FullArchiveTests(unittest.TestCase):
    def test_all23_tables_and_legacy19_contract_coexist_with_global_metadata(self):
        with tempfile.TemporaryDirectory() as folder:
            root=pathlib.Path(folder);graph={t:[] for t in archive.TABLE_KEYS};graph['shows']=[{'show-key':'15|75'}];mapping={t:{} for t in graph};mapping['shows']={'15|75':'recOriginal'}
            proof={'status':'PASS','base':'appdl16HJ5ETm8N69','checkedAt':'2026-10-29T14:00:00Z','records':1,'tables':{t:{'status':'PASS','errors':[],'expected':len(rows),'read':len(rows)} for t,rows in graph.items()}}
            ops={'run-logs':[],'err-logs':[],'heartbeat':[],'meta':[{'id':'recMeta','fields':{}}]}
            evidence={'latest-graph.json':graph,'record-map-final.json':mapping,'final-readback.json':proof,'archive-context.json':{'scope':'All retained outgoing records'},'operational-tables.json':ops,'evidence-manifest.json':['record-map-final.json','final-readback.json','operational-tables.json']}
            for name,value in evidence.items():(root/name).write_text(json.dumps(value))
            prefix,files=archive.build(root);manifest=json.loads(files['manifest.json']);self.assertEqual(manifest['all_table_count'],23);self.assertEqual(manifest['operational_record_count'],1)
            target=root/prefix;target.mkdir(parents=True)
            for name,body in files.items():(target/name).parent.mkdir(parents=True,exist_ok=True);(target/name).write_bytes(body)
            verified,facts=verify_snapshot(target);self.assertEqual(facts['shows']['15|75']['show-key'],'15|75');self.assertEqual(set(manifest['counts']),set(archive.TABLE_KEYS));self.assertEqual(gzip.decompress(files['tables/meta.json.gz']),archive.encoded(ops['meta']))

if __name__=='__main__':unittest.main()
