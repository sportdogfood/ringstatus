import copy,datetime,json,pathlib,sys,tempfile,unittest
from unittest.mock import patch
sys.path.insert(0,str(pathlib.Path(__file__).resolve().parents[1]))
from test_scheduled_contract import API,SCHEMA,AUTHORED,Source,seed,SHOW
from data_runner import Runner,Repository,refs,latest_state,stamp
from retained_feed import publish,load

class RolloverTests(unittest.TestCase):
    def test_new_canonical_show_retires_checkpoint_and_never_fetches_old_show(self):
        class Mapped(Source):
            def fetch(self,route,params):
                p,m=super().fetch(route,params);sid=params.get('show_id',200000076)
                if sid==200000077:
                    if route=='/schedule':p['show'].update(show_id=sid,start_date='2026-11-05',end_date='2026-11-05')
                    elif route=='/entries':
                        p['show_id']=str(sid)
                        for row in p['entries']:row['show_id']=sid
                    else:p['show_id']=sid;p['class_related_data']['date']='2026-11-05'
                return p,m
        api=API();seed(api);source=Mapped()
        # These model separate scheduled invocations, not one Windows clock tick.
        original_begin=Runner.begin;begins=[];start=datetime.datetime.now(datetime.timezone.utc)-datetime.timedelta(seconds=10)
        def separated_begin(runner,*args,**kwargs):
            at=(start+datetime.timedelta(seconds=len(begins))).isoformat();begins.append(at)
            with patch('data_runner.stamp',return_value=at):return original_begin(runner,*args,**kwargs)
        with tempfile.TemporaryDirectory() as folder,patch.object(Runner,'begin',separated_begin):
            runner=Runner(Repository(api,SCHEMA,AUTHORED),source,folder,folder);runner.tick()
            runner.begin(SHOW,latest_state(list(runner.repo.graph['runs'].values()),SHOW));old=runner.runkey;calls=len(source.calls)
            context=api.context['records'][0];context.update({'show-key':'15|200000077','show-id':200000077,'start-date':'2026-11-05','end-date':'2026-11-05','selected-date':'2026-11-05'})
            with patch.object(Runner,'archive_outgoing',return_value={'verified':True}) as archive:
                result=Runner(Repository(api,SCHEMA,AUTHORED),source,folder,folder).tick()
            self.assertEqual(result['show-key'],'15|200000077');self.assertTrue(archive.called)
            self.assertNotEqual(result['run-key'],old)
            self.assertTrue(all(p['show_id']==200000077 for route,p in source.calls[calls:]))
            self.assertTrue((pathlib.Path(folder)/'retired-checkpoints'/old.replace(':','_')/'active.json').exists())
            self.assertEqual(Repository(api,SCHEMA,AUTHORED).graph['runs'][old]['status'],'Retained incomplete; canonical context changed')

    def test_missing_prior_link_fact_is_frozen_with_original_timestamp(self):
        from retained_feed import dependencies
        api=API();seed(api);source=Source()
        with tempfile.TemporaryDirectory() as folder:
            runner=Runner(Repository(api,SCHEMA,AUTHORED),source,folder,folder);runner.tick()
            runner.repo.stage({'runs':[{'run-key':'unaccepted-fixture'}],'horses':[{'horse-key':'300','horse-id':300,'horse-name':'RETAINED','last-success':'2026-10-28T12:00:00Z'}]})
            runner.begin(SHOW,latest_state(list(runner.repo.graph['runs'].values()),SHOW))
            runner.observation('retained-entry',{'coverage':'Previously retained fixture'}, {'started':stamp(),'completed':stamp()}, {'entries':[{'entry-key':'200000076|30','entry-id':30,'show':refs('shows',SHOW),'horse':refs('horses','300'),'riders':refs('riders'),'trainers':refs('trainers'),'owners':refs('owners')}]})
            extra=dependencies(runner.repo,runner.state);self.assertEqual(extra['horses'][0]['last-success'],'2026-10-28T12:00:00Z')
            calls=len(source.calls);runner.observation('retained-dependencies',{}, {'started':stamp(),'completed':stamp()},extra);runner.finish(api.canonical_context())
            root=pathlib.Path(folder,'feed');publish(runner.repo,runner.state,root,runner.state['authority']);facts,proof=load(root,json.loads((root/'head-15-200000076.json').read_text()))
            self.assertEqual(facts['horses']['300']['horse-name'],'RETAINED');self.assertEqual(len(source.calls),calls)

if __name__=='__main__':unittest.main()
