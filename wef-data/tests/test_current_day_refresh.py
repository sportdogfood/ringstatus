"""Offline current-day due selection; no production/source calls."""
import copy,datetime,pathlib,sys,tempfile,threading,time,unittest
from unittest.mock import patch
ROOT=pathlib.Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT))
import data_runner

AT=datetime.datetime.fromisoformat('2026-10-30T16:00:00+00:00').timestamp()
POLICY={k:{'priority':p,'maximum-seconds':m} for k,p,m in [('current',10,180),('today',30,900),('other',50,7200),('completed',60,14400)]}
def state(key,day='2026-10-30',age=900,lane='completed',due=None):
 return {'class_key':key,'day':day,'lane':lane,'last_success':datetime.datetime.fromtimestamp(AT-age,datetime.timezone.utc).isoformat(),'next_due':AT+3000 if due is None else due,'interval':3600}
def select(states,at=AT,slot=0,limit=4):
 fn=getattr(data_runner,'count_due',None)
 return fn(states,POLICY,at,limit=limit,slot=slot) if fn else data_runner.planner.due(states,POLICY,at,limit)

class CurrentDayRefreshTests(unittest.TestCase):
 def test_cohort_keeps_four_concurrent_reads_and_recovers_only_missing_response(self):
  guard=threading.Lock();active=[0];peak=[0];calls=[];fail=[True]
  def fetch(key):
   with guard:active[0]+=1;peak[0]=max(peak[0],active[0]);calls.append(key)
   try:
    time.sleep(0.002)
    if key==17 and fail[0]:raise data_runner.SourceFailure('temporary',{'http':503})
    return {'id':key,'original-observation':'unchanged'}
   finally:
    with guard:active[0]-=1
  keys=list(range(37))
  with tempfile.TemporaryDirectory() as folder:
   path=pathlib.Path(folder)/'responses.json';scope={'keys':[str(k) for k in keys],'show-key':'15|200000076'}
   with self.assertRaises(data_runner.SourceFailure):data_runner.capture_source_batch(path,scope,keys,str,fetch)
   fail[0]=False;calls.clear()
   result=data_runner.capture_source_batch(path,scope,keys,str,fetch)
   self.assertEqual(calls,[17]);self.assertEqual([v['id'] for v in result],keys)
   self.assertLessEqual(peak[0],4)
 def test_completed_today_refreshes_before_hourly_due(self):
  today=state('200030671',age=151);self.assertEqual(select([today]),[today])
 def test_oldest_today_precedes_overdue_future_and_past(self):
  today=state('today',age=7800,due=AT-4200)
  future=state('future','2026-10-31',age=18000,lane='other',due=0)
  past=state('past','2026-10-29',age=24000,due=0)
  self.assertEqual(select([future,past,today],limit=1),[today])
 def test_fresh_completed_today_is_not_polled_every_wake(self):
  self.assertEqual(select([state('fresh',age=60)]),[])
 def test_fair_turn_preserves_other_day_progress_and_four_request_bound(self):
  today=[state(str(i),age=1000+i) for i in range(10)]
  other=state('other','2026-10-31',age=19000,lane='other',due=0)
  chosen=select(today+[other],slot=3);self.assertEqual(len(chosen),4);self.assertIn(other,chosen)
  self.assertNotIn(other,select(today+[other],slot=0))
 def test_eastern_day_instead_of_utc_or_selected_date(self):
  at=datetime.datetime.fromisoformat('2026-10-30T02:00:00+00:00').timestamp()
  yesterday=state('eastern-today','2026-10-29',age=1000);yesterday['last_success']=datetime.datetime.fromtimestamp(at-1000,datetime.timezone.utc).isoformat();yesterday['next_due']=at+3600
  tomorrow=state('utc-today','2026-10-30',age=50000,lane='other',due=0)
  self.assertEqual(select([tomorrow,yesterday],at=at,limit=1),[yesterday])
 def test_unknown_source_time_is_due_but_no_inflight_read(self):
  unknown=state('unknown');unknown['last_success']=None
  inflight=state('inflight');inflight['inflight']=True
  self.assertEqual(select([unknown,inflight]),[unknown])
 def test_selection_never_rewrites_original_observation_or_policy(self):
  rows=[state('old',age=900)];before=copy.deepcopy((rows,POLICY));select(rows)
  self.assertEqual((rows,POLICY),before)
 def test_other_days_keep_existing_planner_behavior(self):
  rows=[state('future','2026-10-31',age=19000,lane='other',due=0),state('past','2026-10-29',age=20000,due=0)]
  self.assertEqual(select(rows),data_runner.planner.due(rows,POLICY,AT,4))
 def test_current_day_cohort_is_bounded_without_expanding_other_day_batch(self):
  today=[state(str(i),age=1000+i) for i in range(60)]
  self.assertEqual(len(select(today,limit=40)),40)
  other=[state(str(i),'2026-10-31',age=19000,lane='other',due=0) for i in range(60)]
  self.assertEqual(len(select(other,limit=40)),4)

class CurrentDayRunnerTests(unittest.TestCase):
 def test_four_short_revisions_preserve_other_day_fair_turn(self):
  from test_scheduled_contract import API,SCHEMA,AUTHORED,Source,seed
  clock={'wall':AT,'mono':0}
  class Mixed(Source):
   def fetch(self,route,params):
    body,metric=super().fetch(route,params)
    if route=='/schedule':
     body['show'].update(start_date='2026-10-30',end_date='2026-10-31')
     template=body['rings'][0]['classes'][0]
     ids=range(2,62) if params['date']=='2026-10-30' else [100]
     body['rings'][0]['classes']=[dict(template,class_id=i,class_groupxclasses_id=i+10) for i in ids]
    elif route.startswith('/classes/'):
     cid=int(route.rsplit('/',1)[1]);body['class']['class_id']=cid
     body['class_related_data']['date']='2026-10-31' if cid==100 else '2026-10-30'
    stamp=datetime.datetime.fromtimestamp(clock['wall'],datetime.timezone.utc).isoformat();metric.update(started=stamp,completed=stamp,sha256=data_runner.digest(data_runner.packed(body)))
    return body,metric
  class Short(data_runner.Runner):
   def class_batch(self,*args,**kwargs):
    super().class_batch(*args,**kwargs);clock['mono']+=181
  api=API();seed(api);api.context['records'][0].update({'start-date':'2026-10-30','end-date':'2026-10-31','selected-date':'2026-10-30'});source=Mixed()
  with tempfile.TemporaryDirectory() as folder,patch('data_runner.time.time',side_effect=lambda:clock['wall']),patch('data_runner.time.monotonic',side_effect=lambda:clock['mono']):
   for _ in range(4):
    clock['mono']=0
    runner=Short(data_runner.Repository(api,SCHEMA,AUTHORED),source,folder,folder)
    self.assertTrue(runner.tick()['ok']);self.assertEqual(runner.state['slots'],1)
    clock['wall']+=200
   self.assertIn('/classes/100',[route for route,_ in source.calls])
   self.assertEqual(runner.state['count-turn'],4)

 def test_two_real_fixture_ticks_refresh_completed_today_preserving_ids_and_original_times(self):
  from test_scheduled_contract import API,SCHEMA,AUTHORED,Source,seed,SHOW
  clock=[AT];day='2026-10-30'
  class CurrentSource(Source):
   def fetch(self,route,params):
    body,metric=super().fetch(route,params)
    if route=='/schedule':body['show'].update(start_date=day,end_date=day)
    elif route.startswith('/classes/'):body['class_related_data']['date']=day
    stamp=datetime.datetime.fromtimestamp(clock[0],datetime.timezone.utc).isoformat()
    metric.update(started=stamp,completed=stamp,sha256=data_runner.digest(data_runner.packed(body)))
    return body,metric
  api=API();seed(api);api.context['records'][0].update({'start-date':day,'end-date':day,'selected-date':day});source=CurrentSource()
  with tempfile.TemporaryDirectory() as folder,patch('data_runner.time.time',side_effect=lambda:clock[0]):
   runner=data_runner.Runner(data_runner.Repository(api,SCHEMA,AUTHORED),source,folder,folder)
   first=runner.tick();self.assertTrue(first['ok'])
   old={t:{k:r['id'] for k,r in rows.items()} for t,rows in runner.repo.inventory.items()}
   work=next(iter(runner.repo.graph['work-items'].values()));original=work['last-success']
   self.assertEqual(work['interval-seconds'],150)
   self.assertEqual(data_runner.planner.epoch(work['next-due']),AT+150)
   clock[0]+=151;source.calls=[]
   again=data_runner.Runner(data_runner.Repository(api,SCHEMA,AUTHORED),source,folder,folder)
   self.assertTrue(again.tick()['ok']);self.assertEqual(sum(route.startswith('/classes/') for route,_ in source.calls),1)
   new_work=next(iter(again.repo.graph['work-items'].values()))
   self.assertEqual(data_runner.planner.epoch(new_work['last-success']),clock[0])
   self.assertNotEqual(new_work['last-success'],original)
   for table,rows in old.items():
    for key,native in rows.items():self.assertEqual(again.repo.store.inventory(table)[key]['id'],native)
   from day_counts import retained_units
   from retained_feed import collection_rows
   units=retained_units(again.state,collection_rows(again.repo,again.state))
   body=next(u['source']['body'] for u,refs in units if u['scope'].startswith('day-count-summary|'))
   self.assertEqual(data_runner.planner.epoch(body['accepted-as-of']),AT)
   self.assertEqual(body['completeness'],'complete')

 def test_measured_infeasible_work_is_reported_without_claiming_freshness(self):
  from test_scheduled_contract import API,SCHEMA,AUTHORED,Source,seed
  day='2026-10-30'
  class Many(Source):
   def fetch(self,route,params):
    body,metric=super().fetch(route,params)
    if route=='/schedule':
     body['show'].update(start_date=day,end_date=day)
     template=body['rings'][0]['classes'][0]
     body['rings'][0]['classes']=[dict(template,class_id=2+i,class_groupxclasses_id=5+i) for i in range(37)]
    elif route.startswith('/classes/'):
     body['class']['class_id']=int(route.rsplit('/',1)[1]);body['class_related_data']['date']=day
    stamp=datetime.datetime.fromtimestamp(AT,datetime.timezone.utc).isoformat();metric.update(started=stamp,completed=stamp,sha256=data_runner.digest(data_runner.packed(body)))
    return body,metric
  class Measured(data_runner.Runner):
   sizes=[]
   def class_batch(self,*args,**kwargs):
    self.sizes.append(len(args[0]))
    super().class_batch(*args,**kwargs);self.state.setdefault('timing',{})['class-batch-seconds-per-class']=10.215
  api=API();seed(api);api.context['records'][0].update({'start-date':day,'end-date':day,'selected-date':day})
  with tempfile.TemporaryDirectory() as folder,patch('data_runner.time.time',return_value=AT):
   runner=Measured(data_runner.Repository(api,SCHEMA,AUTHORED),Many(),folder,folder);self.assertTrue(runner.tick()['ok'])
   capacity=runner.state['capacity'];self.assertEqual(capacity['today-count-scopes'],37)
   self.assertEqual(runner.sizes,[37])
   self.assertAlmostEqual(capacity['today-class-work-estimate-seconds'],377.955)
   self.assertTrue(capacity['today-target-infeasible-before-publication'])
   self.assertEqual(capacity['admission-budget-seconds'],180)

if __name__=='__main__':unittest.main()
