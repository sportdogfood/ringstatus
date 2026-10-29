"""Source bridge receipts at an offline process boundary; no HTTP calls."""
import base64,hashlib,json,pathlib,subprocess,sys,unittest
from unittest.mock import patch
sys.path.insert(0,str(pathlib.Path(__file__).resolve().parents[1]))
from data_runner import Source,SourceFailure

class SourceTransportTests(unittest.TestCase):
    def test_bridge_verifies_exact_bytes_and_sanitizes_retained_source(self):
        body='{"horse":"HÖRSE","token":"private","trips":[]}'.encode()
        metric={'http':200,'bytes':len(body),'sha256':hashlib.sha256(body).hexdigest()}
        response={'ok':True,'body':base64.b64encode(body).decode(),'metric':metric}
        with patch('data_runner.subprocess.run',return_value=subprocess.CompletedProcess([],0,json.dumps(response).encode(),b'')) as boundary:
            payload,got=Source().fetch('/classes/123',{'show_id':76,'customer_id':15})
        self.assertEqual(payload,{'horse':'HÖRSE','trips':[]});self.assertEqual(got,metric)
        self.assertEqual(boundary.call_args.args[0][0],'node');self.assertEqual(boundary.call_args.kwargs['timeout'],28)
    def test_denial_keeps_metadata_without_retrying(self):
        response={'ok':False,'error':'Source HTTP 403','metric':{'route':'/schedule','http':403,'headers':{'cf-ray':'receipt'}}}
        with patch('data_runner.subprocess.run',return_value=subprocess.CompletedProcess([],1,json.dumps(response).encode(),b'')) as boundary:
            with self.assertRaises(SourceFailure) as caught:Source().fetch('/schedule',{'show_id':76,'customer_id':15,'date':'2026-10-29'})
        self.assertEqual(boundary.call_count,1);self.assertEqual(caught.exception.diagnostic,response['metric'])

if __name__=='__main__':unittest.main()
