"""Git publisher tests use disposable local repositories; no network/provider calls."""
import pathlib,subprocess,sys,tempfile,unittest
from unittest.mock import patch
sys.path.insert(0,str(pathlib.Path(__file__).resolve().parents[1]))
from archive_evidence import publish_bundle,ArchiveDeferred

class GitEvidenceTests(unittest.TestCase):
    def test_divergent_unrelated_local_commit_is_rejected_without_merge(self):
        with tempfile.TemporaryDirectory() as folder:
            root=pathlib.Path(folder);origin,checkout=self.repositories(root);other=root/'other';self.git('clone',origin,other)
            self.git('-C',other,'config','user.email','offline@example.invalid');self.git('-C',other,'config','user.name','Other fixture')
            (checkout/'unrelated').write_text('preserved local');self.git('-C',checkout,'add','unrelated');self.git('-C',checkout,'commit','-m','Unrelated local')
            before=self.git('-C',checkout,'rev-parse','HEAD')
            (other/'remote').write_text('remote');self.git('-C',other,'add','remote');self.git('-C',other,'commit','-m','Other commit');self.git('-C',other,'push','origin','main')
            prefix='wef-data/shows/15/200000076/retained-evidence'
            with self.assertRaisesRegex(ValueError,'unrelated'):publish_bundle(checkout,prefix,{prefix+'/part':b'own'},{str(origin)})
            self.assertEqual(self.git('-C',checkout,'rev-parse','HEAD'),before);self.assertEqual(self.git('-C',checkout,'status','--porcelain'),'')

    def test_conflicting_immutable_object_rejects_without_worktree_merge_state(self):
        with tempfile.TemporaryDirectory() as folder:
            root=pathlib.Path(folder);origin,checkout=self.repositories(root);other=root/'other';self.git('clone',origin,other)
            self.git('-C',other,'config','user.email','offline@example.invalid');self.git('-C',other,'config','user.name','Other fixture')
            prefix='wef-data/shows/15/200000076/retained-evidence';name=prefix+'/parts/same'
            for target,body in ((checkout,b'local'),(other,b'conflict')):
                file=target/name;file.parent.mkdir(parents=True);file.write_bytes(body);self.git('-C',target,'add',name);self.git('-C',target,'commit','-m','Evidence')
            self.git('-C',other,'push','origin','main');before=self.git('-C',checkout,'rev-parse','HEAD')
            with self.assertRaises(ArchiveDeferred):publish_bundle(checkout,prefix,{name:b'local'},{str(origin)})
            self.assertEqual(self.git('-C',checkout,'rev-parse','HEAD'),before);self.assertEqual(self.git('-C',checkout,'status','--porcelain'),'')
            self.assertFalse((checkout/'.git/MERGE_HEAD').exists());self.assertEqual((checkout/name).read_bytes(),b'local')

    def git(self,*args):return subprocess.check_output(['git',*map(str,args)],stderr=subprocess.PIPE,text=True).strip()
    def repositories(self,root):
        origin=root/'origin';checkout=root/'checkout'
        self.git('init','--bare','--initial-branch=main',origin);self.git('clone',origin,checkout)
        self.git('-C',checkout,'config','user.email','offline@example.invalid');self.git('-C',checkout,'config','user.name','Offline fixture')
        (checkout/'README').write_text('fixture');self.git('-C',checkout,'add','README');self.git('-C',checkout,'commit','-m','Fixture baseline');self.git('-C',checkout,'push','origin','main')
        return origin,checkout
    def test_publish_remote_readback_and_retry_after_lost_confirmation(self):
        with tempfile.TemporaryDirectory() as folder:
            origin,checkout=self.repositories(pathlib.Path(folder));prefix='wef-data/shows/15/200000076/retained-evidence'
            files={prefix+'/parts/hash.json.gz':b'fixture bytes'};real=subprocess.check_output
            def interrupted(args,**kwargs):
                if args[3]=='archive':raise subprocess.CalledProcessError(1,args)
                return real(args,**kwargs)
            with patch('archive_evidence.subprocess.check_output',side_effect=interrupted):
                with self.assertRaises(ArchiveDeferred):publish_bundle(checkout,prefix,files,{str(origin)})
            before=self.git('-C',checkout,'rev-list','--count','HEAD')
            receipt=publish_bundle(checkout,prefix,files,{str(origin)})
            self.assertEqual(receipt['commit'],receipt['verified_remote_commit']);self.assertEqual(before,self.git('-C',checkout,'rev-list','--count','HEAD'))
            self.assertEqual((checkout/next(iter(files))).read_bytes(),b'fixture bytes')
    def test_only_own_untracked_interrupted_temporary_is_reconciled(self):
        with tempfile.TemporaryDirectory() as folder:
            origin,checkout=self.repositories(pathlib.Path(folder));prefix='wef-data/shows/15/200000076/retained-evidence'
            name=prefix+'/parts/hash.json.gz';temporary=checkout/(name+'.tmp');temporary.parent.mkdir(parents=True);temporary.write_bytes(b'partial')
            publish_bundle(checkout,prefix,{name:b'complete'}, {str(origin)})
            self.assertFalse(temporary.exists());self.assertEqual((checkout/name).read_bytes(),b'complete')
            (checkout/'unrelated').write_text('must remain')
            with self.assertRaisesRegex(ValueError,'unrelated'):publish_bundle(checkout,prefix,{name:b'complete'}, {str(origin)})
            self.assertEqual((checkout/'unrelated').read_text(),'must remain')
    def test_nonfastforward_push_never_force_overwrites_other_commits(self):
        with tempfile.TemporaryDirectory() as folder:
            root=pathlib.Path(folder);origin,checkout=self.repositories(root);other=root/'other';self.git('clone',origin,other)
            self.git('-C',other,'config','user.email','offline@example.invalid');self.git('-C',other,'config','user.name','Other fixture')
            prefix='wef-data/shows/15/200000076/retained-evidence';real=subprocess.check_output; raced=[]
            def racing(args,**kwargs):
                if args[3]=='push' and not raced:
                    raced.append(True);(other/'other').write_text('preserved');self.git('-C',other,'add','other');self.git('-C',other,'commit','-m','Concurrent archive');self.git('-C',other,'push','origin','main')
                self.assertNotIn('--force',args);return real(args,**kwargs)
            with patch('archive_evidence.subprocess.check_output',side_effect=racing):
                with self.assertRaises(ArchiveDeferred):publish_bundle(checkout,prefix,{prefix+'/part':b'own'}, {str(origin)})
            self.assertEqual(self.git('--git-dir',origin,'show','main:other'),'preserved')
            local=self.git('-C',checkout,'rev-parse','HEAD');remote=self.git('--git-dir',origin,'rev-parse','main')
            receipt=publish_bundle(checkout,prefix,{prefix+'/part':b'own'}, {str(origin)})
            for ancestor in (local,remote):self.git('-C',checkout,'merge-base','--is-ancestor',ancestor,receipt['verified_remote_commit'])
            self.assertEqual(self.git('--git-dir',origin,'show','main:other'),'preserved')
            self.assertEqual(self.git('--git-dir',origin,'show','main:'+prefix+'/part'),'own')

if __name__=='__main__':unittest.main()
