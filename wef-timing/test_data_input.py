"""Offline fixtures only: real producer validators, temporary retained-cache objects."""
import copy,gzip,hashlib,importlib.util,json,pathlib,tempfile,unittest

HERE=pathlib.Path(__file__).resolve().parent
spec=importlib.util.spec_from_file_location('timing_data_input',HERE/'data-input.py')
adapter=importlib.util.module_from_spec(spec);spec.loader.exec_module(adapter)
from data_contract import freeze,packed,digest
from archive_evidence import FORMAT

CONTEXT={'show-key':'15|76','show-id':76,'customer-id':15,'start-date':'2026-10-29',
         'end-date':'2026-11-01','selected-date':'2026-10-29'}
FENCE=[CONTEXT[k] for k in ('show-key','start-date','end-date','selected-date')]

def unit(scope,source,at='2026-10-29T14:24:00Z'):
    return {'scope':scope,'show-key':'15|76','source':source,'metric':{'completed':at},
            'graph':{},'verification':{'record-map':{}}}

def inventory(at='2026-10-29T14:24:00Z'):
    return unit('dated-inventory-page',{'show':{'show_id':76},'show_date':'2026-10-29',
        'rings':[{'ring_number':6,'ring_id':45,'classes':[{'class_group_id':19,'class_id':30,
            'class_groupxclasses_id':100,'estimated_start_time':'08:06:00','estimated_end_time':'08:39:00',
            'total_trips':11,'class_type':'Jumpers'}]}]},at)

class Cache:
    def __init__(self,root):
        self.root=root;self.objects=root/'objects';self.objects.mkdir();self.runs=[]
    def add(self,units,mode='native',budget=60000,mutate=None):
        name='run-'+str(len(self.runs));refs=[]
        for source in units:
            for row in freeze(source,budget=budget):
                index=len(refs);key=name+'|observation-'+str(index);sha=row['retained-sha256']
                archived=mode=='archive' or (mode=='mixed' and index%2==1)
                if archived:
                    body=gzip.compress(packed(row).encode(),mtime=0)
                    proof={'format':FORMAT,'path':'wef-data/shows/15/76/retained-evidence/parts/'+sha+'.json.gz',
                           'sha256':hashlib.sha256(body).hexdigest(),'bytes':len(body)}
                    ref={'key':key,'sha256':sha,'archive':proof,'scope':source['scope']}
                    (self.objects/('part-archive-'+proof['sha256']+'.json.gz')).write_bytes(body)
                else:
                    rid='rec'+str(len(self.runs)*1000+index).zfill(14)
                    ref={'key':key,'sha256':sha,'record-id':rid}
                    part={**row,'collection-key':key,'status':'Verified retained observation','_record-id':rid}
                    (self.objects/('part-'+rid+'-'+sha+'.json')).write_text(packed(part),encoding='utf-8')
                refs.append(ref)
        state={'phase':'Complete','pending':False,'show-key':'15|76','authority':FENCE,'authority-before':FENCE,
               'observations':refs,'sequence':len(self.runs),
               'predecessor':{'run-key':self.runs[-1]['head'],'state-sha256':self.runs[-1]['run-sha256']} if self.runs else None}
        if mutate:mutate(state)
        body=packed(state);sha=digest(body)
        run={'run-key':name,'status':'Verified completed revision','state-json':body}
        (self.objects/('run-'+sha+'.json')).write_text(packed(run),encoding='utf-8')
        pointer={'head':name,'run-sha256':sha,'show-key':'15|76','authority':FENCE}
        self.runs.append(pointer);return pointer,refs
    def bootstrap(self):
        return self.add([unit('retained-bootstrap',{'retained-projection':{'shows':[{'show-key':'15|76'}]}})])

class ProjectionTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(prefix='timing-data-input-')
        self.addCleanup(self.temp.cleanup);self.cache=Cache(pathlib.Path(self.temp.name));self.cache.bootstrap()
    def project(self,pointer):return adapter.project(self.cache.root,pointer,CONTEXT)
    def test_native_reference_and_values_unchanged(self):
        pointer,refs=self.cache.add([inventory()]);result=self.project(pointer);inv=result['inventories']['2026-10-29']
        self.assertEqual(inv['refs'],[{'base':'appdl16HJ5ETm8N69','table':'collections','key':refs[0]['key'],
            'record':refs[0]['record-id'],'sha256':refs[0]['sha256']}])
        self.assertEqual(inv['observed_at'],'2026-10-29T14:24:00Z')
        self.assertEqual(inv['groups'][0]['rows'][0]['total-trips'],11)
        self.assertEqual(result['proof']['run-key'],pointer['head'])
    def test_archive_exact_proof_no_fabricated_record(self):
        pointer,refs=self.cache.add([inventory()],mode='archive');inv=self.project(pointer)['inventories']['2026-10-29']
        self.assertEqual(inv['refs'][0]['archive'],refs[0]['archive'])
        self.assertEqual(inv['refs'][0]['scope'],'dated-inventory-page')
        self.assertNotIn('record',inv['refs'][0]);self.assertNotIn('record-id',inv['refs'][0])
    def test_mixed_multipart_preserves_all_ordered_references(self):
        pointer,refs=self.cache.add([inventory()],mode='mixed',budget=600)
        inv=self.project(pointer)['inventories']['2026-10-29'];self.assertGreater(len(refs),1)
        self.assertEqual([r['key'] for r in inv['refs']],[r['key'] for r in refs])
        for got,original in zip(inv['refs'],refs):
            self.assertEqual(got['sha256'],original['sha256'])
            if 'archive' in original:self.assertEqual(got['archive'],original['archive']);self.assertNotIn('record',got)
            else:self.assertEqual(got['record'],original['record-id'])
    def test_newest_inventory_wins_without_redating_old_source(self):
        self.cache.add([inventory()],mode='native')
        pointer,refs=self.cache.add([inventory('2026-10-29T14:25:00Z')],mode='archive')
        inv=self.project(pointer)['inventories']['2026-10-29']
        self.assertEqual(inv['observed_at'],'2026-10-29T14:25:00Z');self.assertEqual(inv['refs'][0]['key'],refs[0]['key'])
    def test_archive_corrupt_bytes_fail(self):
        pointer,refs=self.cache.add([inventory()],mode='archive')
        p=self.cache.objects/('part-archive-'+refs[0]['archive']['sha256']+'.json.gz');p.write_bytes(p.read_bytes()+b'x')
        with self.assertRaisesRegex(ValueError,'hash/length'):self.project(pointer)
    def test_archive_with_native_id_fails_producer_provenance_check(self):
        pointer,_=self.cache.add([inventory()],mode='archive',mutate=lambda s:s['observations'][0].update({'record-id':'invented'}))
        with self.assertRaisesRegex(ValueError,'provenance'):self.project(pointer)
    def test_missing_archive_fails_without_native_fallback(self):
        pointer,refs=self.cache.add([inventory()],mode='archive')
        (self.cache.objects/('part-archive-'+refs[0]['archive']['sha256']+'.json.gz')).unlink()
        with self.assertRaises(FileNotFoundError):self.project(pointer)
    def test_incomplete_predecessor_is_rejected_even_with_valid_latest_inventory(self):
        parent=self.cache.runs[0];file=self.cache.objects/('run-'+parent['run-sha256']+'.json')
        body=json.loads(file.read_text());body['status']='Incomplete';file.write_text(packed(body))
        pointer,_=self.cache.add([inventory()],mode='archive')
        with self.assertRaisesRegex(ValueError,'not completed'):self.project(pointer)
    def test_predecessor_hash_corruption_is_rejected(self):
        parent=self.cache.runs[0];file=self.cache.objects/('run-'+parent['run-sha256']+'.json')
        body=json.loads(file.read_text());body['state-json']+=' ';file.write_text(packed(body))
        pointer,_=self.cache.add([inventory()],mode='archive')
        with self.assertRaisesRegex(ValueError,'hash/key'):self.project(pointer)
    def test_head_authority_mismatch_is_rejected(self):
        pointer,_=self.cache.add([inventory()],mode='archive',mutate=lambda s:s.update({'authority-before':['wrong']}))
        with self.assertRaisesRegex(ValueError,'authority'):self.project(pointer)
    def test_native_record_id_mismatch_is_rejected(self):
        pointer,refs=self.cache.add([inventory()]);ref=refs[0]
        file=self.cache.objects/('part-'+ref['record-id']+'-'+ref['sha256']+'.json');body=json.loads(file.read_text())
        body['_record-id']='wrong';file.write_text(packed(body))
        with self.assertRaisesRegex(ValueError,'record ID'):self.project(pointer)

if __name__=='__main__':unittest.main()
