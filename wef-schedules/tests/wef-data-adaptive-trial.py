"""Bounded isolated adaptive lane/slot source trial. No production scheduling.
Consumes the completed baseline graph and emits Airtable deltas for readback.
"""
import argparse,concurrent.futures,copy,datetime,hashlib,json,pathlib,subprocess,time,unittest
ROOT=pathlib.Path(__file__).resolve().parents[1]/'evidence/wef-data-20261024'
def stamp():return datetime.datetime.now(datetime.timezone.utc).isoformat()
def epoch(s):return datetime.datetime.fromisoformat(s.replace('Z','+00:00')).timestamp()
def iso(v):return datetime.datetime.fromtimestamp(v,datetime.timezone.utc).isoformat()
def packed(v):return json.dumps(v,separators=(',',':'),sort_keys=True,ensure_ascii=False)
def fingerprint(v):return hashlib.sha256(packed(v).encode()).hexdigest()
def safe(v):
 if isinstance(v,dict):return {k:safe(x) for k,x in v.items() if k.lower() not in {'_','_t','token','cookie','authorization','access_token','refresh_token'}}
 if isinstance(v,list):return [safe(x) for x in v]
 return v
def decide(state,payload,at,policy,error=None):
 s=copy.deepcopy(state);s['revision']+=1
 if error:
  s['failures']+=1;s['next_due']=at+min(300,30*2**min(s['failures']-1,4));s['error']=error
  # Failure does not advance validated success/change or replace fingerprint.
  return s,'Source failure; last good retained; bounded recovery retry'
 new=fingerprint(payload);changed=new!=s['fingerprint'];s['unchanged']=0 if changed else s['unchanged']+1;s['failures']=0;s.pop('error',None)
 oldstatus=s['status'];s['status']=payload['class_related_data']['status'];today=datetime.datetime.fromtimestamp(at,datetime.timezone.utc).astimezone(__import__('zoneinfo').ZoneInfo('America/New_York')).date().isoformat()
 lane='current' if s['day']==today and s['status']=='Underway' else 'completed' if s['status']=='Completed' else 'today' if s['day']==today else 'other';p=policy[lane]
 if lane!=s['lane']:s['interval']=p['interval-seconds'];reason=f'Status/date reclassification: {oldstatus} → {s["status"]}; lane {s["lane"]} → {lane}'
 elif changed:s['interval']=p['minimum-seconds'];reason='Validated semantic change: shorter trial refresh interval'
 elif s['unchanged']>=2:s['interval']=min(p['maximum-seconds'],max(p['minimum-seconds'],int(s['interval']*1.5)));reason='Two or more unchanged validations: bounded interval expansion'
 else:reason='Validated unchanged once: maintain interval'
 s['lane']=lane;s['last_success']=iso(at);s['next_due']=at+s['interval'];s['fingerprint']=new
 if changed:s['last_change']=iso(at)
 return s,reason
def due(states,policies,at,limit=4):
 candidates=[s for s in states if s.get('next_due',(epoch(s['last_success']) if s.get('last_success') else 0)+s['interval'])<=at and not s.get('inflight')]
 def rank(s):
  maximum=policies[s['lane']]['maximum-seconds'];deadline=(epoch(s['last_success']) if s.get('last_success') else 0)+maximum
  return (0 if deadline<=at else 1,deadline if deadline<=at else policies[s['lane']]['priority'],s.get('next_due',0),s['class_key'])
 return sorted(candidates,key=rank)[:limit]
def get(s):
 customer,sid=map(int,s['class_key'].split('|')[:2])
 if customer!=15:raise ValueError('Unsupported customer namespace')
 start=time.perf_counter();started=stamp();url=f'https://wef-schedules-decoded.gombcg.workers.dev/classes/{s["class_id"]}?show_id={sid}&customer_id={customer}'
 r=subprocess.run(['curl','-sS','--max-time','18','-w','\n%{http_code}',url],capture_output=True);b,_,code=r.stdout.rpartition(b'\n');m={'class_key':s['class_key'],'class_id':s['class_id'],'http':int(code or 0),'bytes':len(b),'elapsed_seconds':time.perf_counter()-start,'started':started,'completed':stamp(),'sha256':hashlib.sha256(b).hexdigest()}
 try:
  if r.returncode or code!=b'200':raise ValueError('HTTP/transport '+str(code))
  p=safe(json.loads(b));related=p['class_related_data']
  if int(p['show_id'])!=sid or int(p['class']['class_id'])!=s['class_id']:raise ValueError('Wrong show/class')
  if str(related['date'])[:10]!=s['day'] or int(related['ring'])!=s['ring']:raise ValueError('Source moved/date mismatch; inventory revalidation required')
  if not isinstance(p.get('trips'),list) or len(p['trips'])!=int(p['total_entry_trips']):raise ValueError('Incomplete trip array')
  keys=[]
  for t in p['trips']:
   if not all(t.get(k) for k in ['entry_id','horse_id','rider_id']):raise ValueError('Missing trip identity')
   keys.append((t['entry_id'],t.get('entryxclasses_uuid') or t.get('trip_id')))
  if len(keys)!=len(set(keys)):raise ValueError('Duplicate trip identity')
  return p,m,None
 except Exception as e:return None,m,str(e)
class PlannerTests(unittest.TestCase):
 def setUp(self):
  self.policy={'current':{'interval-seconds':90,'minimum-seconds':60,'maximum-seconds':180,'priority':10},'today':{'interval-seconds':300,'minimum-seconds':120,'maximum-seconds':900,'priority':30},'other':{'interval-seconds':1800,'minimum-seconds':600,'maximum-seconds':7200,'priority':50},'completed':{'interval-seconds':3600,'minimum-seconds':900,'maximum-seconds':14400,'priority':60}}
  self.at=epoch('2026-10-24T16:00:00+00:00');self.payload={'class_related_data':{'status':'Underway'},'trips':[]};self.s={'day':'2026-10-24','ring':1,'class_id':1,'class_key':'x','last_success':iso(self.at-180),'last_change':iso(self.at-180),'fingerprint':fingerprint(self.payload),'status':'Underway','interval':90,'unchanged':0,'failures':0,'revision':1,'lane':'current','next_due':self.at-90}
 def test_unchanged_expands_only_after_second_success(self):
  a,_=decide(self.s,self.payload,self.at,self.policy);self.assertEqual(a['interval'],90);b,_=decide(a,self.payload,self.at+90,self.policy);self.assertEqual(b['interval'],135);self.assertEqual(b['last_change'],self.s['last_change'])
 def test_change_promotes_interval(self):
  p=copy.deepcopy(self.payload);p['trips']=[{'entry_id':1}];a,_=decide(self.s,p,self.at,self.policy);self.assertEqual(a['interval'],60);self.assertEqual(a['last_change'],iso(self.at))
 def test_failure_preserves_success_and_hash(self):
  a,_=decide(self.s,None,self.at,self.policy,'timeout');self.assertEqual(a['last_success'],self.s['last_success']);self.assertEqual(a['fingerprint'],self.s['fingerprint']);self.assertEqual(a['next_due'],self.at+30)
 def test_completed_still_due(self):
  p=copy.deepcopy(self.payload);p['class_related_data']['status']='Completed';a,_=decide(self.s,p,self.at,self.policy);self.assertEqual(a['lane'],'completed');self.assertEqual(len(due([a],self.policy,self.at+3601)),1)
 def test_bounded_no_inflight_and_fairness(self):
  states=[{**self.s,'class_key':str(i),'inflight':i==0} for i in range(10)];self.assertEqual(len(due(states,self.policy,self.at)),4);self.assertNotIn('0',[s['class_key'] for s in due(states,self.policy,self.at)])
  old={**self.s,'class_key':'old','lane':'other','last_success':iso(self.at-8000),'next_due':self.at-6000};self.assertEqual(due(states+[old],self.policy,self.at,1)[0]['class_key'],'old')
def run_trial(seconds=220,request_limit=40,resume=False):
 graph=json.loads((ROOT/'graph.json').read_text(encoding='utf-8'));policies={p['lane-key']:p for p in graph['lanes']};states={w['work-key']:json.loads(w['state-json']) for w in graph['work-items']}
 for s in states.values():s['next_due']=epoch(s['last_success'])+s['interval']
 result={'run_key':'adaptive-'+stamp(),'started':stamp(),'status':'Running','scope':'Bounded isolated adaptive trial, no background deployment','slots':[],'observations':[],'errors':[],'policies':policies,'initial_due':len(due(list(states.values()),policies,time.time(),10000))};out=ROOT/'adaptive';out.mkdir(exist_ok=True);start=time.perf_counter();requests=0;observed={}
 if resume:
  result=json.loads((out/'trial.json').read_text(encoding='utf-8'));(out/'trial-initial-segment.json').write_text(json.dumps(result,separators=(',',':')));states=result['states'];policies=result['policies'];requests=len(result['observations']);result['status']='Running';result.setdefault('resumed_at',[]).append(stamp())
  for o in result['observations']:observed[o['class_key']]=observed.get(o['class_key'],0)+1
 def save():
  result['states']=states;(out/'trial.json').write_text(json.dumps(result,separators=(',',':')))
 while time.perf_counter()-start<seconds and requests<request_limit:
  selected=due(list(states.values()),policies,time.time(),min(4,request_limit-requests))
  if not selected:
   time.sleep(min(1,max(0,seconds-(time.perf_counter()-start))));continue
  slot={'key':result['run_key']+'|slot-'+str(len(result['slots'])+1),'started':stamp(),'budget_seconds':25,'class_keys':[s['class_key'] for s in selected],'requests':[]};begin=time.perf_counter()
  with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
   for old,(p,m,error) in zip(selected,pool.map(get,selected)):
    at=epoch(m['completed']);new,reason=decide(old,p,at,policies,error);states[old['class_key']]=new;requests+=1;observed[old['class_key']]=observed.get(old['class_key'],0)+1
    item={'class_key':old['class_key'],'old':old,'new':new,'reason':reason,'metric':m,'error':error,'slot':slot['key']}
    if p:
     filename=f'{requests:03}-{old["class_id"]}.json';(out/filename).write_text(packed(p));item['source_file']=filename
    else:result['errors'].append(item)
    result['observations'].append(item);slot['requests'].append(m);save()
  slot['completed']=stamp();slot['elapsed_seconds']=time.perf_counter()-begin;result['slots'].append(slot);save();print('Adaptive slot',len(result['slots']),'requests',requests,'repeated_classes',sum(n>=2 for n in observed.values()),flush=True)
  # At least two real due observations of one class, plus a plan revision.
  if any(n>=2 for n in observed.values()) and any(o['new']['interval']!=o['old']['interval'] or o['new']['lane']!=o['old']['lane'] for o in result['observations']):break
 result['completed']=stamp();result['elapsed_seconds']=epoch(result['completed'])-epoch(result['started']);result['request_count']=requests;result['repeated_classes']=sum(n>=2 for n in observed.values());result['replans']=sum(o['new']['interval']!=o['old']['interval'] or o['new']['lane']!=o['old']['lane'] for o in result['observations']);result['status']='PASS' if not result['errors'] and result['repeated_classes'] and result['replans'] else 'FAIL';save();print('Adaptive trial',result['status'],requests,'requests',result['repeated_classes'],'repeated classes',result['replans'],'replans',flush=True)
if __name__=='__main__':
 parser=argparse.ArgumentParser();parser.add_argument('--test',action='store_true');parser.add_argument('--resume',action='store_true');parser.add_argument('--evidence-root',type=pathlib.Path);args=parser.parse_args()
 if args.test:unittest.main(argv=['planner-tests'])
 else:
  if args.evidence_root is None:parser.error('--evidence-root is required; saved trial is never a runtime default')
  ROOT=args.evidence_root.resolve()
  if (ROOT/'adaptive/trial.json').exists() and not args.resume:raise ValueError('Existing trial requires explicit resume')
  run_trial(resume=args.resume)
