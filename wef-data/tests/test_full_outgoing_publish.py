"""Disposable local Git origins only; never invoke production acquisition."""
import json,pathlib,sys,tempfile,subprocess,unittest
from unittest.mock import patch
sys.path[:0]=[str(pathlib.Path(__file__).resolve().parents[1]),str(pathlib.Path(__file__).resolve().parent)]
import test_git_evidence as git_fixture
import test_archive as archive_fixture
from archive_evidence import publish_bundle,ArchiveDeferred
from data_runner import Runner

class FullOutgoingTests(unittest.TestCase):
    def setUp(self):
        # Only the legacy helper's production-remote check is adapted to the
        # disposable origin. Publication/reconciliation still uses real Git.
        def archive_git(repo,*args):
            if args==('remote','get-url','origin'):return 'https://github.com/sportdogfood/ringstatus-data'
            return self.git('-C',repo,*args)
        self.guard=patch.object(archive_fixture.archive,'git',side_effect=archive_git);self.guard.start();self.addCleanup(self.guard.stop)
    def git(self,*args):return git_fixture.GitEvidenceTests.git(self,*args)
    def repositories(self,root):return git_fixture.GitEvidenceTests.repositories(self,root)
    def evidence(self,root):
        root.mkdir();graph={t:[] for t in archive_fixture.archive.TABLE_KEYS}
        graph['shows']=[{'show-key':'15|200000076'}]
        graph['days']=[{'day-key':'15|200000076|2026-10-30','show':{'$links':[['shows','15|200000076']]}}]
        graph['horses']=[{'horse-key':'42','horse-id':42}]
        graph['entries']=[{'entry-key':'200000076|99','show':{'$links':[['shows','15|200000076']]},'horse':{'$links':[['horses','42']]}}]
        mapping={t:{str(r[archive_fixture.archive.TABLE_KEYS[t]]):'rec'+t+str(i) for i,r in enumerate(rows)} for t,rows in graph.items()}
        proof={'status':'PASS','base':'appdl16HJ5ETm8N69','checkedAt':'2026-10-30T12:00:00Z','records':sum(map(len,graph.values())),'tables':{t:{'status':'PASS','errors':[],'expected':len(rows),'read':len(rows)} for t,rows in graph.items()}}
        values={'latest-graph.json':graph,'record-map-final.json':mapping,'final-readback.json':proof,'archive-context.json':{'scope':'Offline retained fixture','coverage':'Fixture only'},'evidence-manifest.json':['record-map-final.json','final-readback.json'],'operational-tables.json':{t:[{'id':'rec'+t,'fields':{'kept':'unchanged'}}] for t in ['heartbeat','run-logs','err-logs','meta']}}
        for name,value in values.items():(root/name).write_text(json.dumps(value),encoding='utf-8')
        return root
    def local_publisher(self,origin):return lambda repo,prefix,files:publish_bundle(repo,prefix,files,{str(origin)})
    def test_remote_advance_after_commit_recovers_repeatedly_without_losing_archive_or_other_history(self):
        with tempfile.TemporaryDirectory() as folder:
            root=pathlib.Path(folder);origin,repo=self.repositories(root);other=root/'other';self.git('clone',origin,other)
            self.git('-C',other,'config','user.email','offline@example.invalid');self.git('-C',other,'config','user.name','Other fixture')
            evidence=self.evidence(root/'evidence');prefix,files=archive_fixture.archive.build(evidence);real=subprocess.check_output;raced=[]
            def racing(args,**kwargs):
                self.assertNotIn('--force',args)
                if len(args)>3 and args[3]=='push' and not raced:
                    raced.append(True);(other/'other-producer').write_text('preserved');self.git('-C',other,'add','other-producer');self.git('-C',other,'commit','-m','Remote advance');self.git('-C',other,'push','origin','main')
                return real(args,**kwargs)
            with patch('archive_evidence.publish_bundle',side_effect=self.local_publisher(origin)):
                with patch('archive_evidence.subprocess.check_output',side_effect=racing),self.assertRaises(ArchiveDeferred):archive_fixture.archive.archive(evidence,repo,publish=True)
                own=self.git('-C',repo,'rev-parse','HEAD');remote=self.git('--git-dir',origin,'rev-parse','main')
                result=archive_fixture.archive.archive(evidence,repo,publish=True)
                for parent in [own,remote]:self.git('-C',repo,'merge-base','--is-ancestor',parent,result['verified_remote_commit'])
                archive_fixture.archive.verify(repo,prefix,files,result['verified_remote_commit'])
                self.assertEqual(self.git('--git-dir',origin,'show','main:other-producer'),'preserved')
                count=self.git('-C',repo,'rev-list','--count','HEAD');again=archive_fixture.archive.archive(evidence,repo,publish=True)
                self.assertEqual(count,self.git('-C',repo,'rev-list','--count','HEAD'));self.assertEqual(again['verified_remote_commit'],result['verified_remote_commit'])
                self.assertEqual(archive_fixture.archive.build(evidence),(prefix,files))
    def test_unrelated_local_history_rejected_without_mutation(self):
        with tempfile.TemporaryDirectory() as folder:
            root=pathlib.Path(folder);origin,repo=self.repositories(root);other=root/'other';self.git('clone',origin,other)
            self.git('-C',other,'config','user.email','offline@example.invalid');self.git('-C',other,'config','user.name','Other fixture')
            (repo/'foreign').write_text('kept');self.git('-C',repo,'add','foreign');self.git('-C',repo,'commit','-m','Foreign local history')
            (other/'remote').write_text('kept');self.git('-C',other,'add','remote');self.git('-C',other,'commit','-m','Remote');self.git('-C',other,'push','origin','main');before=self.git('-C',repo,'rev-parse','HEAD')
            with patch('archive_evidence.publish_bundle',side_effect=self.local_publisher(origin)),self.assertRaisesRegex(ValueError,'unrelated'):archive_fixture.archive.archive(self.evidence(root/'evidence'),repo,publish=True)
            self.assertEqual(self.git('-C',repo,'rev-parse','HEAD'),before);self.assertEqual((repo/'foreign').read_text(),'kept');self.assertFalse((repo/'.git/MERGE_HEAD').exists())
    def test_conflicting_immutable_history_is_integrity_failure_not_readiness(self):
        with tempfile.TemporaryDirectory() as folder:
            root=pathlib.Path(folder);origin,repo=self.repositories(root);other=root/'other';self.git('clone',origin,other)
            self.git('-C',other,'config','user.email','offline@example.invalid');self.git('-C',other,'config','user.name','Other fixture')
            evidence=self.evidence(root/'evidence');prefix,files=archive_fixture.archive.build(evidence);target=(prefix/'manifest.json').as_posix()
            for checkout,body in [(repo,b'local existing evidence'),(other,b'remote conflicting evidence')]:
                f=checkout/target;f.parent.mkdir(parents=True);f.write_bytes(body);self.git('-C',checkout,'add',target);self.git('-C',checkout,'commit','-m','Immutable history')
            self.git('-C',other,'push','origin','main');before=self.git('-C',repo,'rev-parse','HEAD')
            with patch('archive_evidence.publish_bundle',side_effect=self.local_publisher(origin)),self.assertRaisesRegex(ValueError,'Conflicting outgoing archive history'):archive_fixture.archive.archive(evidence,repo,publish=True)
            self.assertEqual(self.git('-C',repo,'rev-parse','HEAD'),before);self.assertFalse((repo/'.git/MERGE_HEAD').exists());self.assertEqual((repo/target).read_bytes(),b'local existing evidence')
    def test_archive_deferral_is_readiness_while_integrity_errors_remain_fatal(self):
        with tempfile.TemporaryDirectory() as folder:
            runner=object.__new__(Runner);runner.runtime=pathlib.Path(folder);runner.retention_waits=[]
            with patch.object(runner,'archive_outgoing',side_effect=ArchiveDeferred('Archive Git push did not complete')):runner.retain_outgoing('15|200000075',{})
            self.assertEqual(runner.retention_waits[0]['phase'],'archive-readiness')
            with patch.object(runner,'archive_outgoing',side_effect=ValueError('Immutable archive collision')),self.assertRaises(ValueError):runner.retain_outgoing('15|200000075',{})
    def test_deferred_outgoing_publication_does_not_stop_actual_current_show_fixture(self):
        import test_scheduled_contract as fixture
        from data_runner import Repository
        for invalid in [False,True]:
            api=fixture.API();fixture.seed(api);source=fixture.Source()
            with self.subTest(invalid=invalid),tempfile.TemporaryDirectory() as folder:
                runner=Runner(Repository(api,fixture.SCHEMA,fixture.AUTHORED),source,folder,folder)
                runner.repo.graph['shows']['15|200000075']={'show-key':'15|200000075'}
                error=ValueError('Immutable archive collision') if invalid else ArchiveDeferred('Archive Git push did not complete')
                with patch.object(runner,'archive_outgoing',side_effect=error):
                    if invalid:
                        with self.assertRaises(ValueError):runner.tick()
                        self.assertFalse(source.calls)
                    else:
                        result=runner.tick();self.assertTrue(result['ok']);self.assertTrue(source.calls)
                        self.assertEqual(result['retention-waits'][0]['phase'],'archive-readiness')
    def test_published_path_keeps_remote_and_branch_guards(self):
        with tempfile.TemporaryDirectory() as folder:
            root=pathlib.Path(folder);origin,repo=self.repositories(root);evidence=self.evidence(root/'evidence')
            with self.assertRaisesRegex(ValueError,'Wrong retained evidence repository'):archive_fixture.archive.archive(evidence,repo,publish=True)
            self.git('-C',repo,'switch','-c','not-main')
            with patch('archive_evidence.publish_bundle',side_effect=self.local_publisher(origin)),self.assertRaisesRegex(ValueError,'existing main'):archive_fixture.archive.archive(evidence,repo,publish=True)
    def test_ahead_only_foreign_commit_is_never_published(self):
        with tempfile.TemporaryDirectory() as folder:
            root=pathlib.Path(folder);origin,repo=self.repositories(root);evidence=self.evidence(root/'evidence')
            (repo/'foreign').write_text('kept local');self.git('-C',repo,'add','foreign');self.git('-C',repo,'commit','-m','Foreign ahead-only')
            local=self.git('-C',repo,'rev-parse','HEAD');remote=self.git('--git-dir',origin,'rev-parse','main')
            with patch('archive_evidence.publish_bundle',side_effect=self.local_publisher(origin)),self.assertRaisesRegex(ValueError,'unrelated'):archive_fixture.archive.archive(evidence,repo,publish=True)
            self.assertEqual(self.git('-C',repo,'rev-parse','HEAD'),local);self.assertEqual(self.git('--git-dir',origin,'rev-parse','main'),remote)
    def test_current_publication_can_reconcile_exact_frozen_outgoing_bundle_only(self):
        with tempfile.TemporaryDirectory() as folder:
            root=pathlib.Path(folder);origin,repo=self.repositories(root);other=root/'other';self.git('clone',origin,other)
            self.git('-C',other,'config','user.email','offline@example.invalid');self.git('-C',other,'config','user.name','Other fixture')
            evidence=self.evidence(root/'evidence');prefix,files=archive_fixture.archive.build(evidence);pending={(prefix/name).as_posix():body for name,body in files.items()}
            for name,body in pending.items():f=repo/name;f.parent.mkdir(parents=True,exist_ok=True);f.write_bytes(body)
            self.git('-C',repo,'add',prefix);self.git('-C',repo,'commit','-m','Pending verified outgoing snapshot');old=self.git('-C',repo,'rev-parse','HEAD')
            (other/'remote').write_text('kept');self.git('-C',other,'add','remote');self.git('-C',other,'commit','-m','Remote advance');self.git('-C',other,'push','origin','main')
            current='wef-data/shows/15/200000077/retained-evidence';current_files={current+'/part':b'verified current'}
            result=publish_bundle(repo,current,current_files,{str(origin)},pending_bundles={prefix.as_posix():pending})
            self.git('-C',repo,'merge-base','--is-ancestor',old,result['verified_remote_commit']);archive_fixture.archive.verify(repo,prefix,files,result['verified_remote_commit'])
            self.assertEqual(self.git('--git-dir',origin,'show','main:'+current+'/part'),'verified current')
            self.assertEqual(result['files'],1)
    def test_actual_runner_reuses_frozen_cutoff_when_clock_and_shared_rows_change(self):
        import test_retirement as retained_fixture
        import test_scheduled_contract as source_fixture
        import data_runner
        from data_runner import Repository
        api,repo,facts,ids,approval=retained_fixture.setup();calls=[]
        with tempfile.TemporaryDirectory() as folder:
            root=pathlib.Path(folder);runtime=root/'runtime';runtime.mkdir();repository=root/'repository';repository.mkdir()
            first=Runner(repo,source_fixture.Source(),runtime,repository)
            def saved_archive(evidence,target,publish,**kwargs):
                prefix,files=data_runner.archiver.build(evidence);calls.append((prefix,files))
                if len(calls)==1:raise ArchiveDeferred('First raced push')
                dest=target/prefix;dest.mkdir(parents=True,exist_ok=True)
                for name,body in files.items():(dest/name).parent.mkdir(parents=True,exist_ok=True);(dest/name).write_bytes(body)
                return {'path':prefix.as_posix(),'files':len(files),'verified_remote_commit':'offline-proof'}
            ops={t:[] for t in ['heartbeat','run-logs','err-logs','meta']}
            with patch('full_archive.operational',return_value=(ops,{},{})),patch.object(data_runner.archiver,'archive',side_effect=saved_archive):
                with patch('data_runner.stamp',return_value='2026-10-30T12:00:00Z'),self.assertRaises(ArchiveDeferred):first.archive_outgoing(retained_fixture.OLD,api.canonical_context())
                fields={f['name']:f['id'] for f in repo.schema['tables']['riders']['fields']}
                api.db[repo.schema['tables']['riders']['id']][0]['fields'][fields['rider-name']]='Changed after saved cutoff'
                second=Runner(Repository(api,repo.schema,repo.authored),source_fixture.Source(),runtime,repository)
                with patch('data_runner.stamp',return_value='2026-10-30T13:00:00Z'):result=second.archive_outgoing(retained_fixture.OLD,api.canonical_context())
            self.assertEqual(calls[0],calls[1]);self.assertEqual(result['path'],calls[0][0].as_posix());self.assertFalse(api.deletes)

if __name__=='__main__':unittest.main()
