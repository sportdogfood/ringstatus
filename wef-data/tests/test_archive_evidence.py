"""Offline archive cutover fixtures; never call production collectors or providers."""
import copy,json,pathlib,sys,tempfile,unittest
from unittest.mock import patch
ROOT=pathlib.Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT))
from test_scheduled_contract import API,SCHEMA,AUTHORED,Source,seed,SHOW
from data_runner import Runner,Repository,latest_state
from retained_feed import publish,load
from archive_evidence import EvidenceStore,ArchiveDeferred

class Publisher:
    def __init__(self):self.calls=[];self.unavailable=False
    def __call__(self,repository,prefix,files):
        if self.unavailable:raise ArchiveDeferred('Fixture remote unavailable')
        self.calls.append((prefix,copy.deepcopy(files)))
        for name,body in files.items():
            target=pathlib.Path(repository)/name;target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(body)
        return {'commit':'fixture-commit','verified_remote_commit':'fixture-commit','path':prefix,'files':len(files)}

class EvidenceTests(unittest.TestCase):
    def test_incomplete_journal_bytes_survive_later_same_show_bundle(self):
        with tempfile.TemporaryDirectory() as folder:
            api,source,root,publisher,store=self.setup_run(folder)
            repo=Repository(api,SCHEMA,AUTHORED);runner=Runner(repo,source,root/'runtime',root/'git',evidence=store)
            runner.begin(SHOW,latest_state(list(repo.graph['runs'].values()),SHOW))
            runner.observation('interrupted-day',{'exact':'old observation'},{'started':runner.at,'completed':runner.at},{})
            incomplete=runner.runkey;reference=copy.deepcopy(runner.state['observations'][0])
            repo.patch('runs',incomplete,{'status':'Retained incomplete; canonical context changed'})
            (root/'runtime/active.json').unlink()
            later=Runner(Repository(api,SCHEMA,AUTHORED),source,root/'runtime',root/'git',evidence=store)
            later.begin(SHOW,latest_state(list(later.repo.graph['runs'].values()),SHOW));later.inventory(api.canonical_context()['records'][0])
            later.finish(api.canonical_context())
            _,name,files=store.bundle(later.repo,later.state,later.runkey)
            self.assertIn(reference['archive']['path'],files)
            manifest=json.loads(files[name]);self.assertIn(incomplete,manifest['records']['incomplete-runs'])
            self.assertNotIn(reference,manifest['records']['observations'])

    def test_outgoing_archive_contains_incomplete_parts_and_pending_source(self):
        import io,tarfile
        from data_runner import archiver
        with tempfile.TemporaryDirectory() as folder:
            api,source,root,publisher,store=self.setup_run(folder)
            runner=Runner(Repository(api,SCHEMA,AUTHORED),source,root/'runtime',root/'git',evidence=store)
            runner.begin(SHOW,latest_state(list(runner.repo.graph['runs'].values()),SHOW))
            runner.observation('interrupted-show',{'saved':'source bytes'},{'started':runner.at,'completed':runner.at},{})
            reference=runner.state['observations'][0]
            pending={'run-key':runner.runkey,'show-key':SHOW,'source':{'not-yet-applied':'exact pending source'},'graph':{},'next-state':{},'start':2,'metric':{'started':runner.at,'completed':runner.at},'scope':'pending','request-start':1}
            (root/'runtime/pending.json').write_text(json.dumps(pending))
            captured={}
            def archive(evidence,repository,publish):
                prefix,files=archiver.build(evidence);captured.update(files)
                target=repository/prefix;target.mkdir(parents=True,exist_ok=True)
                for name,body in files.items():(target/name).parent.mkdir(parents=True,exist_ok=True);(target/name).write_bytes(body)
                return {'path':str(prefix),'verified_remote_commit':'offline-proof'}
            ops={'run-logs':[],'err-logs':[],'heartbeat':[],'meta':[]}
            with patch('full_archive.operational',return_value=(ops,{},{})),patch.object(archiver,'archive',side_effect=archive):
                runner.archive_outgoing(SHOW,api.canonical_context())
            with tarfile.open(fileobj=io.BytesIO(captured['source-and-evidence.tar.gz'])) as tar:
                self.assertEqual(tar.extractfile(reference['archive']['path']).read(),store.local_path(reference).read_bytes())
                journals=json.load(tar.extractfile('retained-evidence-journals.json'))
                self.assertEqual(journals[runner.runkey]['observations'][0],reference)
                checkpoints=json.load(tar.extractfile('retained-checkpoints.json'))
                self.assertEqual(checkpoints[0]['pending']['source'],pending['source'])
            self.assertNotEqual(runner.repo.graph['runs'][runner.runkey]['status'],'Verified completed revision')
            store.local_path(reference).write_bytes(b'corrupt')
            with self.assertRaises(ValueError),patch.object(archiver,'archive') as no_publish,patch('full_archive.operational',return_value=(ops,{},{})):
                runner.archive_outgoing(SHOW,api.canonical_context())
            no_publish.assert_not_called()

    def setup_run(self,folder):
        api=API();seed(api);source=Source();root=pathlib.Path(folder)
        legacy=Runner(Repository(api,SCHEMA,AUTHORED),source,root/'runtime',root/'git');legacy.tick()
        publisher=Publisher();store=EvidenceStore(root/'runtime/retained-evidence',root/'git',publisher)
        return api,source,root,publisher,store
    def test_mixed_chain_stops_collection_writes_and_preserves_legacy_records(self):
        with tempfile.TemporaryDirectory() as folder:
            api,source,root,publisher,store=self.setup_run(folder)
            before=copy.deepcopy(api.db[SCHEMA['tables']['collections']['id']])
            for number in range(2):
                repo=Repository(api,SCHEMA,AUTHORED);runner=Runner(repo,source,root/'runtime',root/'git',evidence=store)
                runner.begin(SHOW,latest_state(list(repo.graph['runs'].values()),SHOW));runner.inventory(api.canonical_context()['records'][0]);runner.day_summaries(api.canonical_context()['records'][0]);result=runner.finish(api.canonical_context())
                self.assertTrue(result['ok']);self.assertEqual(result['evidence-archive']['verified_remote_commit'],'fixture-commit')
                self.assertEqual(before,api.db[SCHEMA['tables']['collections']['id']])
                self.assertTrue(all('record-id' not in r and r['archive'] for r in runner.state['observations']))
                publish(repo,runner.state,root/'feed',runner.state['authority'])
                pointer=json.loads((root/'feed/head-15-200000076.json').read_text())
                facts,proof=load(root/'feed',pointer)
                self.assertIn('200000076|21',facts['entries']);self.assertEqual(proof['run-key'],runner.runkey)
                from day_counts import accepted_summaries
                from retained_feed import collection_rows
                summaries=accepted_summaries(runner.runkey,repo.graph['runs'],collection_rows(repo,runner.state),api.canonical_context()['records'][0])
                self.assertEqual(len(summaries),1)
            self.assertEqual(len(publisher.calls),2)
            for row in runner.repo.graph['requests'].values():
                if row['request-key'].startswith(runner.runkey+'|'):self.assertNotIn('collection',row)
    def test_new_show_without_legacy_collections_keeps_four_season_projections(self):
        from season_projection import project
        from season_store import SeasonStore
        from test_store import MemoryAPI
        with tempfile.TemporaryDirectory() as folder:
            root=pathlib.Path(folder);api=API();seed(api);source=Source();store=EvidenceStore(root/'runtime/retained-evidence',root/'git',Publisher())
            runner=Runner(Repository(api,SCHEMA,AUTHORED),source,root/'runtime',root/'git',evidence=store);runner.tick()
            self.assertEqual(api.db.get(SCHEMA['tables']['collections']['id'],[]),[])
            publish(runner.repo,runner.state,root/'feed',runner.state['authority']);pointer=json.loads((root/'feed/head-15-200000076.json').read_text())
            facts,proof=load(root/'feed',pointer);config=json.loads((ROOT.parent/'wef-season/schema.json').read_text(encoding='utf-8'))
            for entity,item in config['entities'].items():
                target=MemoryAPI();writer=SeasonStore(target,item,root/entity);graph=project(facts,entity)
                self.assertGreater(writer.apply(graph)['records-added'],0);before=copy.deepcopy(target.db)
                self.assertEqual(writer.apply(graph)['records-added'],0);self.assertEqual(before,target.db)
            saved=runner.repo.scoped(SHOW)
            self.assertIn(runner.runkey,{r['run-key'] for r in saved['runs']});self.assertTrue(saved['requests']);self.assertEqual(saved['collections'],[])
    def test_archive_failure_keeps_exact_completion_and_recovers_without_source(self):
        with tempfile.TemporaryDirectory() as folder:
            api,source,root,publisher,store=self.setup_run(folder);before=copy.deepcopy(api.db[SCHEMA['tables']['collections']['id']])
            runner=Runner(Repository(api,SCHEMA,AUTHORED),source,root/'runtime',root/'git',evidence=store)
            runner.begin(SHOW,latest_state(list(runner.repo.graph['runs'].values()),SHOW));runner.inventory(api.canonical_context()['records'][0])
            key=runner.runkey;publisher.unavailable=True
            with self.assertRaises(ArchiveDeferred):runner.finish(api.canonical_context())
            self.assertTrue((root/'runtime/active.json').exists());self.assertNotEqual(runner.repo.graph['runs'][key]['status'],'Verified completed revision')
            completion=runner.state['completion-at'];calls=len(source.calls);publisher.unavailable=False
            recovered=Runner(Repository(api,SCHEMA,AUTHORED),source,root/'runtime',root/'git',evidence=store)
            self.assertTrue(recovered.recover(api.canonical_context()));result=recovered.finish(api.canonical_context())
            self.assertEqual(result['run-key'],key);self.assertEqual(recovered.state['completed'],completion)
            self.assertEqual(len(source.calls),calls);self.assertEqual(before,api.db[SCHEMA['tables']['collections']['id']])
    def test_saved_unit_recovery_never_allocates_collection_rows(self):
        with tempfile.TemporaryDirectory() as folder:
            api,source,root,publisher,store=self.setup_run(folder);before=copy.deepcopy(api.db[SCHEMA['tables']['collections']['id']])
            runner=Runner(Repository(api,SCHEMA,AUTHORED),source,root/'runtime',root/'git',evidence=store);runner.begin(SHOW,latest_state(list(runner.repo.graph['runs'].values()),SHOW))
            actual=store.stage
            def interrupted(*args,**kwargs):actual(*args,**kwargs);raise KeyboardInterrupt()
            with patch.object(store,'stage',side_effect=interrupted):
                with self.assertRaises(KeyboardInterrupt):runner.observation('saved-unit',{'value':'é'}, {'started':runner.at,'completed':runner.at},{})
            recovered=Runner(Repository(api,SCHEMA,AUTHORED),source,root/'runtime',root/'git',evidence=store)
            self.assertTrue(recovered.recover(api.canonical_context()));recovered.finish(api.canonical_context())
            self.assertEqual(before,api.db[SCHEMA['tables']['collections']['id']]);self.assertFalse((root/'runtime/pending.json').exists())
    def test_equal_payloads_share_bytes_without_merging_observation_keys(self):
        with tempfile.TemporaryDirectory() as folder:
            store=EvidenceStore(pathlib.Path(folder)/'stage',pathlib.Path(folder)/'git',Publisher())
            from data_contract import freeze
            rows=freeze({'same':'é'*100000})
            first=store.stage(SHOW,[{'collection-key':'existing-builder|observation-'+str(i),**r} for i,r in enumerate(rows)])
            second=store.stage(SHOW,[{'collection-key':'another-run|observation-'+str(i),**r} for i,r in enumerate(rows)])
            self.assertEqual([r['archive'] for r in first],[r['archive'] for r in second]);self.assertNotEqual([r['key'] for r in first],[r['key'] for r in second])
            self.assertTrue(all('record-id' not in r for r in first));self.assertEqual(store.read(first[0])['collection-key'],first[0]['key'])
            path=store.local_path(first[0]);path.write_bytes(b'bad')
            with self.assertRaises(ValueError):store.read(first[0])
    def test_publication_fence_change_cannot_advance_completed_head(self):
        with tempfile.TemporaryDirectory() as folder:
            api,source,root,publisher,store=self.setup_run(folder)
            runner=Runner(Repository(api,SCHEMA,AUTHORED),source,root/'runtime',root/'git',evidence=store)
            runner.begin(SHOW,latest_state(list(runner.repo.graph['runs'].values()),SHOW));runner.inventory(api.canonical_context()['records'][0])
            def changed(*args):
                result=publisher(*args);api.context['records'][0]['selected-date']='2026-10-30';return result
            store.publisher=changed
            from data_runner import CanonicalTransition
            with self.assertRaises(CanonicalTransition):runner.finish({'records':[{**api.context['records'][0]}]})
            self.assertNotEqual(runner.repo.graph['runs'][runner.runkey]['status'],'Verified completed revision')
            self.assertTrue((root/'runtime/active.json').exists())
    def test_archive_retry_exhaustion_stops_and_does_not_retry_on_next_wake(self):
        from data_runner import run
        with tempfile.TemporaryDirectory() as folder:
            api,source,root,publisher,store=self.setup_run(folder);publisher.unavailable=True
            schema=root/'schema.json';schema.write_text(json.dumps(SCHEMA));authored=root/'authored.json';authored.write_text(json.dumps(AUTHORED))
            with patch('data_runner.Airtable',return_value=api),patch('data_runner.Source',return_value=source),patch('archive_evidence.EvidenceStore',return_value=store):
                outcomes=[]
                for _ in range(4):
                    result=run(root/'runtime',schema,authored,root/'git',archive_storage=True);outcomes.append(result['state'])
                    deferred=root/'runtime/archive-deferred.json'
                    if deferred.exists():
                        saved=json.loads(deferred.read_text());saved['next-due']=0;deferred.write_text(json.dumps(saved))
                self.assertEqual(outcomes,['Deferred','Deferred','Deferred','Stopped']);calls=len(source.calls)
                self.assertTrue((root/'runtime/stopped.json').exists())
                self.assertIn('Stopped',run(root/'runtime',schema,authored,root/'git',archive_storage=True)['state'])
                self.assertEqual(len(source.calls),calls)

if __name__=='__main__':unittest.main()
