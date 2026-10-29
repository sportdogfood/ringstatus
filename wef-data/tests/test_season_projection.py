import pathlib,sys,tempfile,unittest,json
sys.path.insert(0,str(pathlib.Path(__file__).resolve().parents[1]))
from test_scheduled_contract import API,SCHEMA,AUTHORED,Source,seed,SHOW
from data_runner import Runner,Repository
from retained_feed import publish,load
from season_projection import project

class SeasonProjectionTests(unittest.TestCase):
    def test_entity_show_day_membership_no_directory_attendance_and_exact_keys(self):
        api=API();seed(api)
        with tempfile.TemporaryDirectory() as folder:
            runner=Runner(Repository(api,SCHEMA,AUTHORED),Source(),folder,folder);runner.tick();root=pathlib.Path(folder,'feed');publish(runner.repo,runner.state,root,api.canonical_context());facts,ref=load(root,json.loads((root/'head-15-200000076.json').read_text()))
            graph=project(facts,'horses');self.assertIn('121',graph['horses']);self.assertIn('122',graph['horses'])
            self.assertEqual(graph['horses']['121']['shows'],[SHOW]);self.assertEqual(graph['horses']['121']['show-days'],[SHOW+'|2026-10-29']);self.assertEqual(graph['horses']['122']['show-days'],[])
            self.assertEqual(project(facts,'rings')['rings'][SHOW+'|2026-10-29|1']['ring-key'],SHOW+'|2026-10-29|1')
            self.assertEqual(graph,project(facts,'horses'))

if __name__=='__main__':unittest.main()
