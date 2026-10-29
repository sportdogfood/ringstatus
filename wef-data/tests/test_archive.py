import importlib.util,json,pathlib,tempfile,unittest
ROOT=pathlib.Path(__file__).resolve().parents[2]
spec=importlib.util.spec_from_file_location('archive_source',ROOT/'wef-schedules/tests/archive-wef-data.py')
archive=importlib.util.module_from_spec(spec);spec.loader.exec_module(archive)

class ArchiveTests(unittest.TestCase):
    def test_generic_archive_keeps_keys_and_real_scope_without_trial_files(self):
        with tempfile.TemporaryDirectory() as folder:
            root=pathlib.Path(folder);graph={t:[] for t in archive.TABLE_KEYS};graph['shows']=[{'show-key':'15|200000076'}]
            mapping={t:{} for t in graph};mapping['shows']={'15|200000076':'recExisting'}
            proof={'status':'PASS','base':'appdl16HJ5ETm8N69','checkedAt':'2026-11-01T12:00:00Z','records':1,
                'tables':{t:{'status':'PASS','errors':[],'expected':len(rows),'read':len(rows)} for t,rows in graph.items()}}
            for name,value in [('latest-graph.json',graph),('record-map-final.json',mapping),('final-readback.json',proof),
                ('archive-context.json',{'scope':'Verified retained show76 graph','coverage':'Scheduled class graph; directory incomplete'}),
                ('evidence-manifest.json',['record-map-final.json','final-readback.json'])]:
                (root/name).write_text(json.dumps(value))
            prefix,files=archive.build(root);manifest=json.loads(files['manifest.json'])
            self.assertTrue(str(prefix).replace('\\','/').startswith('wef-data/shows/15/200000076/'))
            self.assertEqual(manifest['scope'],'Verified retained show76 graph')
            self.assertEqual(manifest['counts']['shows'],1)
            self.assertEqual(archive.build(root),(prefix,files))

if __name__=='__main__':unittest.main()
