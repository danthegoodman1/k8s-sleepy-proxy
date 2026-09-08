"""Bounded read-only metadata capture for one newly owned diagnostic namespace."""
from pathlib import Path
import concurrent.futures,json,os,signal,subprocess,threading,time
ROOT=Path('/Users/dangoodman/code/sleepy-pods')
OUT=ROOT/'docs/review-evidence/lifecycle-membership-diagnostic'
NS='sleepypods-e2e-membership-wire-20260908'
BASE=['kubectl','--kubeconfig',str(ROOT/'.generated/implementation-evidence/kubeconfig'),'--context','kind-sleepypods-review-remediation','--request-timeout=5s','-n',NS]
STOP=threading.Event()
for sig in [signal.SIGTERM,signal.SIGINT]:signal.signal(sig,lambda *_:STOP.set())
SQL="""SELECT json_build_object(
'observed_unix_ms',(extract(epoch from clock_timestamp())*1000)::bigint,
'instances',(SELECT coalesce(json_agg(x),'[]'::json) FROM (SELECT instance_id,state,generation,updated_at_unix_millis FROM instances WHERE instance_id='lifecycle-membership')x),
'materializations',(SELECT coalesce(json_agg(x),'[]'::json) FROM (SELECT materialization_id,instance_id,instance_generation,projection_generation,state,state_entered_at_unix_millis,operation_deadline_unix_millis,next_attempt_at_unix_millis,drain_not_before_unix_millis,failure_requires_cleanup,failure_count,failure_kind,failure_message,wake_failure_message,reconcile_owner,reconcile_attempt,reconcile_lease_expires_at_unix_millis,updated_at_unix_millis,backend_uri,rendered_objects FROM materializations WHERE instance_id='lifecycle-membership')x),
'effects',(SELECT coalesce(json_agg(x),'[]'::json) FROM (SELECT e.materialization_id,e.effect_id,e.lease_owner,e.lease_attempt,e.instance_generation,e.operation,e.object_ref,e.expected_uid,e.expected_resource_version,e.started_at_unix_millis FROM materialization_effects e JOIN materializations m USING(materialization_id) WHERE m.instance_id='lifecycle-membership')x));"""

def run(args):
 now=time.time_ns()//1000000
 try:
  result=subprocess.run(BASE+args,text=True,capture_output=True,timeout=7)
  return {'observed_wall_ms':now,'exit_code':result.returncode,'stdout':result.stdout,'stderr':result.stderr[:2048]}
 except subprocess.TimeoutExpired as error:return {'observed_wall_ms':now,'error':'command exceeded7s','stderr':str(error)[:2048]}

def db():
 row=run(['exec','deployment/sleepypods-postgres','-c','postgres','--','env','PGOPTIONS=-c statement_timeout=3000','psql','-X','-qAt','-U','sleepypods','-d','sleepypods','-v','ON_ERROR_STOP=1','-c',SQL])
 if row.get('exit_code')==0:
  try:row['metadata']=json.loads(row.pop('stdout'))
  except ValueError:row['parse_error']='invalid JSON'
 return row

def kube():
 row=run(['get','deployment,replicaset,pod,service','-l','sleepypods.io/instance-id=lifecycle-membership','-o','json'])
 if row.get('exit_code')==0:
  obj=json.loads(row.pop('stdout'));items=[]
  for item in obj.get('items',[]):
   m=item.get('metadata',{});spec=item.get('spec',{});status=item.get('status',{})
   items.append({'kind':item['kind'],'name':m.get('name'),'uid':m.get('uid'),'resourceVersion':m.get('resourceVersion'),'generation':m.get('generation'),'ownerReferences':m.get('ownerReferences'),'labels':m.get('labels'),'deletionTimestamp':m.get('deletionTimestamp'),'deletionGracePeriodSeconds':m.get('deletionGracePeriodSeconds'),'terminationGracePeriodSeconds':spec.get('terminationGracePeriodSeconds'),'replicas':spec.get('replicas'),'clusterIP':spec.get('clusterIP'),'observedGeneration':status.get('observedGeneration'),'readyReplicas':status.get('readyReplicas'),'availableReplicas':status.get('availableReplicas'),'phase':status.get('phase'),'podIP':status.get('podIP'),'conditions':status.get('conditions'),'containerStates':[{'name':v.get('name'),'ready':v.get('ready'),'restartCount':v.get('restartCount'),'state':v.get('state'),'lastState':v.get('lastState')} for v in status.get('containerStatuses',[])]})
  row['metadata']=items
 return row

def capture(name,fn):
 with (OUT/name).open('w') as f:
  started=time.monotonic()
  while not STOP.is_set() and time.monotonic()-started<360:
   try:result=fn()
   except Exception as error:result={'observed_wall_ms':time.time_ns()//1000000,'error':repr(error)}
   f.write(json.dumps(result,separators=(',',':'))+'\n');f.flush()
   STOP.wait(1)
with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
 futures=[pool.submit(capture,'database-timeline.jsonl',db),pool.submit(capture,'kube-timeline.jsonl',kube)]
 for future in futures:future.result()
