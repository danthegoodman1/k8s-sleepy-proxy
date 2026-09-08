import concurrent.futures,datetime,hashlib,json,os,subprocess,time
from pathlib import Path
root=Path('/Users/dangoodman/code/sleepy-pods');out=Path(__file__).resolve().parent
ns='sp-6m-draining-'+str(int(time.time()))
env=os.environ.copy();env.pop('DOCKER_CONTEXT',None)
env.update(DOCKER_CONFIG=str(root/'.generated/docker-public-config'),DOCKER_HOST='unix:///Users/dangoodman/.docker/run/docker.sock',SLEEPYPODS_KIND_CLUSTER='sleepypods-review-remediation',SLEEPYPODS_KIND_KEEP_CLUSTER='1',SLEEPYPODS_KIND_E2E_KEEP_NAMESPACE='1',SLEEPYPODS_KIND_E2E_NAMESPACE=ns,SLEEPYPODS_IMAGE_TAG='review-remediation',SLEEPYPODS_KIND_E2E_OPERATOR_PORT='19951',SLEEPYPODS_KIND_E2E_FRONTLINE_PORT='19980',KUBECONFIG=str(root/'.generated/implementation-evidence/kubeconfig'))
production=[]
for path in [root/'Cargo.toml',root/'Cargo.lock',root/'Dockerfile']:
 production.append(path)
for crate in (root/'crates').iterdir():
 if not crate.is_dir():continue
 for name in ['Cargo.toml','build.rs']:
  if (crate/name).is_file():production.append(crate/name)
 for directory in ['src','migrations','proto']:
  production.extend(p for p in (crate/directory).rglob('*') if p.is_file())
def digest():
 h=hashlib.sha256()
 for p in sorted(set(production)):h.update(str(p.relative_to(root)).encode()+b'\0'+p.read_bytes())
 return h.hexdigest()
record={'namespace':ns,'production_before':digest(),'started_utc':datetime.datetime.now(datetime.timezone.utc).isoformat()}
assert record['production_before']=='799aa84679078f48050f9f44d8a3eb2f0facd3e442479a8319878d81c04e0989',record
(out/'run.json').write_text(json.dumps(record,indent=2)+'\n')
print('Starting retained frozen-image diagnostic in '+ns,flush=True)
log=(out/'driver.log').open('w');started=time.monotonic();process=subprocess.Popen(['/bin/bash',str(out/'runner.sh')],cwd=root,env=env,stdout=log,stderr=subprocess.STDOUT)
sql="SELECT json_build_object('clock',clock_timestamp(),'instances',(SELECT json_agg(i) FROM instances i),'materializations',(SELECT json_agg(m) FROM materializations m),'effects',(SELECT json_agg(e) FROM materialization_effects e),'activity',(SELECT json_agg(a) FROM (SELECT pid,state,wait_event_type,wait_event,pg_blocking_pids(pid) AS blockers,left(query,700) AS query FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid() LIMIT 16) a))::text"
commands={'db':['kubectl','-n',ns,'exec','deployment/sleepypods-postgres','--','psql','-U','sleepypods','-d','sleepypods','-XAt','-c',sql], 'objects':['kubectl','-n',ns,'get','pods,statefulsets,deployments,services,pvc,endpointslices','-o','json'], 'pv':['kubectl','get','pv','-l','sleepypods.io/instance-id=lifecycle-delete-draining','-o','json'], 'control-plane':['kubectl','-n',ns,'logs','deployment/sleepypods-control-plane','--timestamps','--tail=300'], 'events':['kubectl','-n',ns,'get','events','-o','json']}
def capture(item,stamp):
 name,command=item
 try:
  result=subprocess.run(command,cwd=root,env=env,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,timeout=10)
  data=result.stdout;code=result.returncode
 except subprocess.TimeoutExpired as e:data=(e.stdout or b'')+b'\nCAPTURE TIMEOUT';code=124
 (out/(stamp+'-'+name+'.log')).write_bytes(data)
 return {'name':name,'exit':code}
with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
 while process.poll() is None:
  stamp=datetime.datetime.now(datetime.timezone.utc).strftime('%H%M%S')
  results=list(executor.map(lambda item:capture(item,stamp),commands.items()))
  if time.monotonic()-started>360:
   record['overrun']=True;break
  time.sleep(3)
 if process.poll() is None:process.terminate()
 try:code=process.wait(timeout=20)
 except subprocess.TimeoutExpired:process.kill();code=process.wait()
 stamp=datetime.datetime.now(datetime.timezone.utc).strftime('%H%M%S-final')
 list(executor.map(lambda item:capture(item,stamp),commands.items()))
log.close();record.update(exit_code=code,elapsed_seconds=time.monotonic()-started,production_after=digest())
(out/'run.json').write_text(json.dumps(record,indent=2)+'\n');print(json.dumps(record),flush=True)
