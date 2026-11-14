import importlib.util,json,pathlib,sys,tempfile,unittest
from types import SimpleNamespace
from unittest.mock import patch
sys.path.insert(0,str(pathlib.Path(__file__).resolve().parents[1]))
import fast_workflow as module
class ReaderCapture(unittest.TestCase):
 def test_failure_capture_preserves_both_attempts_and_exposes_capture_pointer(self):
  with tempfile.TemporaryDirectory(prefix='fast-capture-test-') as folder:
   attempts=[{'http':502,'failure':{'reason':'runner ECONNRESET'}},{'http':502,'failure':{'reason':'Config response is missing _t'}}]
   result={'ok':False,'error':'Source HTTP502: Config response is missing _t','attempts':attempts,'metric':dict(attempts[-1])}
   with patch.object(module.subprocess,'run',return_value=SimpleNamespace(returncode=0,stdout=json.dumps(result),stderr='')) as command:
    with self.assertRaises(RuntimeError) as caught:module.Reader(pathlib.Path(folder),2).fetch('/schedule',{'customer_id':15,'show_id':200000078,'date':'2026-11-14'},prepare=True)
   self.assertEqual(command.call_count,1)
   capture=pathlib.Path(caught.exception.metric['capture']);self.assertTrue(capture.exists())
   data=json.loads(capture.read_text(encoding='utf-8'));self.assertEqual(data['attempts'],attempts)
   self.assertIn('missing _t',str(caught.exception));self.assertFalse(data['ok'])
if __name__=='__main__':unittest.main()
