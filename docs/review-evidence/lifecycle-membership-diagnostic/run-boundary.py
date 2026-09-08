#!/usr/bin/env python3
"""Run only the synthetic scheduler boundary against an owned disposable PostgreSQL."""
import hashlib, json, os, pathlib, re, shutil, signal, subprocess, time
root = pathlib.Path('/Users/dangoodman/code/sleepy-pods')
out = root / 'docs/review-evidence/lifecycle-membership-diagnostic'
built = re.search(r'Executable.*\(([^)]+)\)', (out/'boundary-build.log').read_text()).group(1)
binary = root/'.generated/implementation-evidence/cleanup-boundary-driver'
shutil.copy2(built, binary)
scratch = pathlib.Path('/private/tmp/sleepypods-cleanup-boundary-ro13u8e8')
sources = {}
for path in ['crates/control-plane/tests/postgres_store.rs','crates/control-plane/tests/postgres_store/cleanup_boundary.rs']:
    sources[path] = hashlib.sha256((scratch/path).read_bytes()).hexdigest()
meta = {'scratch':str(scratch),'source_sha256':sources,'executable_sha256':hashlib.sha256(binary.read_bytes()).hexdigest(),'scope':'one test; new disposable PostgreSQL container only; no kind operations'}
env = os.environ.copy()
env.pop('DOCKER_CONTEXT', None)
env['DOCKER_CONFIG'] = str(root/'.generated/docker-public-config')
env['DOCKER_HOST'] = 'unix:///Users/dangoodman/.docker/run/docker.sock'
def docker(*args):
    return subprocess.run(['docker',*args],env=env,check=True,text=True,capture_output=True,timeout=30).stdout.strip()
cid = None
started = time.monotonic()
try:
    cid = docker('run','--detach','--label','sleepypods.test=cleanup-boundary','--env','POSTGRES_DB=sleepypods','--env','POSTGRES_USER=sleepypods','--env','POSTGRES_PASSWORD=sleepypods','--publish','127.0.0.1::5432','postgres:17-alpine')
    meta['owned_container'] = cid
    ready_by = time.monotonic()+60
    while True:
        try:
            docker('exec',cid,'pg_isready','-U','sleepypods','-d','sleepypods')
            break
        except subprocess.CalledProcessError:
            if time.monotonic() >= ready_by: raise
            time.sleep(0.25)
    port = docker('port',cid,'5432/tcp').split(':')[-1]
    if not port.isdecimal(): raise RuntimeError('invalid owned PostgreSQL port')
    env['SLEEPYPODS_POSTGRES_URL'] = f'postgres://sleepypods:sleepypods@127.0.0.1:{port}/sleepypods'
    cmd = [str(binary),'cleanup_boundary::healthy_cleanup_retry_can_outlast_sixty_second_observation','--exact','--nocapture']
    meta['command'] = cmd
    with (out/'boundary-postgres.log').open('w') as log:
        proc = subprocess.Popen(cmd,env=env,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
        try: meta['exit_code'] = proc.wait(timeout=150)
        except subprocess.TimeoutExpired:
            os.killpg(proc.pid,signal.SIGKILL)
            proc.wait()
            raise
finally:
    if cid:
        docker('rm','-f',cid)
        meta['owned_container_removed'] = True
    meta['seconds'] = time.monotonic()-started
    (out/'boundary-postgres.json').write_text(json.dumps(meta,indent=2)+'\n')
print(json.dumps(meta,indent=2))
raise SystemExit(meta['exit_code'])
