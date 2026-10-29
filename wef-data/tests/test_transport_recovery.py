import io,json,pathlib,sys,tempfile,unittest,urllib.error
from unittest.mock import patch
sys.path.insert(0,str(pathlib.Path(__file__).resolve().parents[1]))
from data_runner import Airtable,run
from wef_data_airtable import TransportDeferred

class RecoveryTests(unittest.TestCase):
    def test_reservation_response_loss_before_server_commit_recovers_without_duplicate(self):
        from test_scheduled_contract import API,SCHEMA,AUTHORED,Source,seed
        class Lost(API):
            lost=False
            def request(self,method,base,table,params=None,body=None):
                if body and body.get('performUpsert') and not self.lost:
                    self.lost=True;raise TransportDeferred(method,base,table,body)
                return super().request(method,base,table,params,body)
        api=Lost();seed(api);source=Source()
        with tempfile.TemporaryDirectory() as folder:
            root=pathlib.Path(folder);schema=root/'schema.json';schema.write_text(json.dumps(SCHEMA));authored=root/'authored.json';authored.write_text(json.dumps(AUTHORED))
            with patch('data_runner.Airtable',return_value=api),patch('data_runner.Source',return_value=source):
                self.assertEqual(run(root,schema,authored,root)['state'],'Deferred')
                reserved=json.loads((root/'active.json').read_text())['run-key']
                data=json.loads((root/'deferred.json').read_text());data['next-due']=0;(root/'deferred.json').write_text(json.dumps(data))
                result=run(root,schema,authored,root)
            self.assertTrue(result['ok']);self.assertEqual(result['run-key'],reserved)
            self.assertEqual(len(api.db[SCHEMA['tables']['runs']['id']]),1)

    def test_legacy_unknown_create_absent_reconciles_exact_reserved_key(self):
        from test_scheduled_contract import API,SCHEMA,AUTHORED,Source,seed,SHOW
        from data_runner import Runner,Repository,atomic
        api=API();seed(api);source=Source()
        with tempfile.TemporaryDirectory() as folder:
            root=pathlib.Path(folder);schema=root/'schema.json';schema.write_text(json.dumps(SCHEMA));authored=root/'authored.json';authored.write_text(json.dumps(AUTHORED))
            runner=Runner(Repository(api,SCHEMA,AUTHORED),source,folder,folder);runner.begin(SHOW,{})
            key=runner.runkey;table=SCHEMA['tables']['runs'];primary=table['primaryFieldId']
            api.db[table['id']]=[]
            atomic(root/'deferred.json',{'operation':{'method':'POST','base':SCHEMA['base'],'table':table['id'],'body':{'records':[{'fields':{primary:key}}]},'write-outcome':'Unknown'},'next-due':0})
            with patch('data_runner.Airtable',return_value=api),patch('data_runner.Source',return_value=source):result=run(root,schema,authored,root)
            self.assertTrue(result['ok']);self.assertEqual(result['run-key'],key)
            self.assertEqual(sum(r['fields'].get(primary)==key for r in api.db[table['id']]),1)
            self.assertFalse((root/'deferred.json').exists())

    def test_explicit_rate_limit_is_distinct_from_unknown_write(self):
        def opener(request,timeout):raise urllib.error.HTTPError('https://fixture',429,'Rate limited',{},None)
        with self.assertRaises(TransportDeferred) as error:Airtable(token='fixture',opener=opener,sleep=lambda _:None).request('POST','appFixture','tblFixture',body={'records':[]})
        self.assertEqual(error.exception.operation['write-outcome'],'NotSent')
    def test_invalid_or_conflicting_class_identity_never_writes_trips(self):
        from test_scheduled_contract import API,SCHEMA,AUTHORED,Source,seed
        from data_runner import Runner,Repository
        for bad in ('boolean','duplicate-uuid'):
            class Invalid(Source):
                def fetch(self,route,params):
                    payload,metric=super().fetch(route,params)
                    if route.startswith('/classes/'):
                        if bad=='boolean':payload['trips'][0]['horse_id']=True
                        else:payload['trips'].append({**payload['trips'][0],'entry_id':22});payload['total_entry_trips']=2
                    return payload,metric
            api=API();seed(api)
            with tempfile.TemporaryDirectory() as folder:
                with self.assertRaises(ValueError):Runner(Repository(api,SCHEMA,AUTHORED),Invalid(),folder,folder).tick()
                self.assertEqual(api.db.get(SCHEMA['tables']['trips']['id'],[]),[])
    def test_proof_sealing_does_not_rewrite_verified_business_graph(self):
        from test_scheduled_contract import API,SCHEMA,AUTHORED,Source,seed
        from data_runner import Runner,Repository
        api=API();seed(api)
        with tempfile.TemporaryDirectory() as folder:
            runner=Runner(Repository(api,SCHEMA,AUTHORED),Source(),folder,folder)
            with patch.object(runner.repo,'stage',wraps=runner.repo.stage) as stage:runner.tick()
            graphs=[call.args[0] for call in stage.call_args_list]
            # Normal observation sealing must use control-only writes after its
            # actual business graph has passed the first exact readback.
            self.assertTrue(any(set(g)=={'runs','collections'} for g in graphs))

    def test_connection_reset_get_is_bounded_and_recovers(self):
        calls=[]
        def opener(request,timeout):
            calls.append(request.method)
            if len(calls)<3:raise urllib.error.URLError(ConnectionResetError(10054,'fixture reset'))
            return io.BytesIO(b'{"records":[]}')
        client=Airtable(token='fixture',opener=opener,sleep=lambda _:None)
        self.assertEqual(client.request('GET','appFixture','tblFixture'),{'records':[]});self.assertEqual(len(calls),3)
    def test_uncertain_create_is_not_retried(self):
        calls=[]
        def opener(request,timeout):calls.append(request.method);raise urllib.error.URLError(ConnectionResetError(10054,'fixture reset'))
        with self.assertRaises(TransportDeferred) as error:Airtable(token='fixture',opener=opener,sleep=lambda _:None).request('POST','appFixture','tblFixture',body={'records':[{'fields':{'key':'same'}}]})
        self.assertEqual(calls,['POST']);self.assertEqual(error.exception.operation['method'],'POST')
    def test_startup_transport_failure_defers_without_stop_or_business_writes(self):
        from data_runner import atomic
        with tempfile.TemporaryDirectory() as folder:
            atomic(pathlib.Path(folder)/'active.json',{'run-key':'existing'})
            # Validate handler at the first constructor boundary with readable config files.
            for name in ['schema','authored']:pathlib.Path(folder,name+'.json').write_text('{}')
            with patch('data_runner.Airtable',return_value=object()),patch('data_runner.Repository',side_effect=TransportDeferred('GET','appFixture','tblFixture')):
                result=run(folder,pathlib.Path(folder)/'schema.json',pathlib.Path(folder)/'authored.json',folder)
            self.assertEqual(result['state'],'Deferred');self.assertFalse(pathlib.Path(folder,'stopped.json').exists());self.assertTrue(pathlib.Path(folder,'active.json').exists())

    def test_lost_run_create_response_reuses_id_and_reserved_run(self):
        from test_scheduled_contract import API,SCHEMA,AUTHORED,Source,seed
        class Lost(API):
            lost=False
            def request(self,method,base,table,params=None,body=None):
                result=super().request(method,base,table,params,body)
                if body and body.get('performUpsert') and not self.lost:
                    self.lost=True;raise TransportDeferred(method,base,table,body)
                return result
        api=Lost();seed(api);source=Source()
        with tempfile.TemporaryDirectory() as folder:
            schema=pathlib.Path(folder,'schema.json');schema.write_text(json.dumps(SCHEMA));authored=pathlib.Path(folder,'authored.json');authored.write_text(json.dumps(AUTHORED))
            with patch('data_runner.Airtable',return_value=api),patch('data_runner.Source',return_value=source):
                result=run(folder,schema,authored,folder)
                self.assertEqual(result['state'],'Deferred');self.assertEqual(len(source.calls),0)
                reserved=json.loads(pathlib.Path(folder,'active.json').read_text())['run-key']
                before=api.db[SCHEMA['tables']['runs']['id']][0]['id']
                deferred=pathlib.Path(folder,'deferred.json');data=json.loads(deferred.read_text());data['next-due']=0;deferred.write_text(json.dumps(data))
                recovered=run(folder,schema,authored,folder)
            self.assertTrue(recovered['ok']);self.assertEqual(recovered['run-key'],reserved)
            self.assertEqual(api.db[SCHEMA['tables']['runs']['id']][0]['id'],before)

if __name__=='__main__':unittest.main()
