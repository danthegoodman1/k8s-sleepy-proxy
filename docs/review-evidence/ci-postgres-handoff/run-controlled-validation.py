from pathlib import Path
import subprocess,os,json,hashlib,time,datetime
root=Path.cwd()
base=root/'.generated/ci-fix/supersession'
base.mkdir(exist_ok=True)
source=root/'crates/control-plane/tests/postgres_store/runtime_work.rs'
green=source.read_bytes()
old=b'let completion_budget = if definite_apply {\n            Duration::from_secs(5)'
new=b'let completion_budget = if definite_apply {\n            Duration::from_secs(1)'
assert green.count(old)==1
red=green.replace(old,new)
(base/'fixture-green.rs').write_bytes(green)
(base/'fixture-red.rs').write_bytes(red)
env=os.environ.copy()
env.pop('DOCKER_CONTEXT',None)
env.pop('SLEEPYPODS_POSTGRES_URL',None)
env['DOCKER_CONFIG']=str(root/'.generated/docker-public-config')
env['DOCKER_HOST']='unix:///Users/dangoodman/.docker/run/docker.sock'
def run(args,**kwargs):
    return subprocess.run(args,env=env,cwd=root,check=True,text=True,capture_output=True,timeout=kwargs.pop('timeout',20),**kwargs)
before=run(['docker','ps','-aq','--no-trunc']).stdout.splitlines()
(base/'containers-before.json').write_text(json.dumps(before,indent=2)+'\n')
container=None
results=[]
try:
    container=run(['docker','run','--detach','--label','sleepypods.task=ci-supersession','--env','POSTGRES_DB=sleepypods','--env','POSTGRES_USER=sleepypods','--env','POSTGRES_PASSWORD=sleepypods','--publish','127.0.0.1::5432','postgres:17-alpine']).stdout.strip()
    assert container not in before and len(container)==64
    (base/'owned-container.txt').write_text(container+'\n')
    ready=time.monotonic()+30
    while True:
        result=subprocess.run(['docker','exec',container,'pg_isready','-U','sleepypods','-d','sleepypods'],env=env,capture_output=True,text=True,timeout=5)
        if result.returncode==0: break
        if time.monotonic()>ready: raise RuntimeError('owned Postgres readiness exceeded30s')
        time.sleep(.2)
    binding=run(['docker','port',container,'5432/tcp']).stdout.strip()
    port=int(binding.rsplit(':',1)[1])
    testenv=env|{'SLEEPYPODS_POSTGRES_URL':f'postgres://sleepypods:sleepypods@127.0.0.1:{port}/sleepypods'}
    for label,contents in [('controlled-red',red),('candidate-green',green)]:
        source.write_bytes(contents)
        started=time.monotonic()
        with (base/f'{label}.log').open('w') as log:
            result=subprocess.run(['cargo','test','--locked','--offline','-p','control-plane','--test','postgres_store','runtime_work::postgres_runtime_delete_supersedes_pending_attempt','--','--exact','--nocapture'],cwd=root,env=testenv,stdout=log,stderr=subprocess.STDOUT,timeout=180)
        text=(base/f'{label}.log').read_text()
        if label=='controlled-red':
            assert result.returncode!=0 and 'accepted Delete did not finish within 1s' in text,text[-6000:]
            assert '6L definite handoff apply-transient:' in text,text[-6000:]
        else:
            assert result.returncode==0 and '1 passed; 0 failed; 0 ignored' in text,text[-6000:]
        results.append({'label':label,'exit':result.returncode,'duration_seconds':time.monotonic()-started,'sha256':hashlib.sha256(contents).hexdigest(),'log':f'{label}.log'})
finally:
    source.write_bytes(green)
    if container:
        cleanup=subprocess.run(['docker','rm','-f',container],env=env,capture_output=True,text=True,timeout=20)
        (base/'cleanup.log').write_text(cleanup.stdout+cleanup.stderr)
        if cleanup.returncode: raise RuntimeError('owned container cleanup failed')
    after=run(['docker','ps','-aq','--no-trunc']).stdout.splitlines()
    (base/'containers-after.json').write_text(json.dumps(after,indent=2)+'\n')
    (base/'results.json').write_text(json.dumps({'ran_at_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'results':results,'before':before,'after':after,'preexisting_inventory_unchanged':set(before)==set(after),'source_restored':source.read_bytes()==green},indent=2)+'\n')
print(json.dumps(results,indent=2))
