import subprocess, os, time, json, threading, socket, http.client
from pathlib import Path
out=Path(os.environ.get('CAPTURE_OUT','.generated/phase6j-rewake-capture2'));out.mkdir(exist_ok=True)
ns='sleepypods-e2e-stateless-hot-cache'
k=['kubectl','--kubeconfig','.generated/implementation-evidence/kubeconfig','-n',ns]
procs=[]; files=[]; log_procs={}
def launch(cmd,name,env=None):
 f=(out/name).open('w');files.append(f)
 p=subprocess.Popen(cmd,stdout=f,stderr=subprocess.STDOUT,env=env);procs.append(p);return p
trace=r'''
import socket,struct,time,json,datetime
s=socket.socket(socket.AF_PACKET,socket.SOCK_RAW,socket.htons(3));s.settimeout(.5)
end=time.monotonic()+35
print('capture ready: connection headers and DNS only',flush=True)
while time.monotonic()<end:
 try: data,addr=s.recvfrom(65535)
 except socket.timeout: continue
 if len(data)<34 or data[12:14]!=b'\x08\x00': continue
 ip=data[14:];ihl=(ip[0]&15)*4
 if len(ip)<ihl+8:continue
 src=socket.inet_ntoa(ip[12:16]);dst=socket.inet_ntoa(ip[16:20]);proto=ip[9]
 if '10.244.0.98' not in (src,dst):continue
 pkt={'utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'iface':addr[0],'src':src,'dst':dst}
 payload=ip[ihl:]
 if proto==6 and len(payload)>=20:
  sp,dp,seq,ack=struct.unpack('!HHII',payload[:12]);off=(payload[12]>>4)*4
  if sp==50051 or dp==50051:continue
  body=payload[off:];pkt.update(proto='tcp',sport=sp,dport=dp,seq=seq,ack=ack,flags=payload[13],bytes=len(body))
  if body.startswith(b'HTTP/'):pkt['http_status']=body.split(b'\r\n',1)[0].decode(errors='replace')[:80]
  elif body.startswith((b'GET ',b'POST ',b'HEAD ')):pkt['request_method']=body.split(b' ',1)[0].decode()
  print(json.dumps(pkt),flush=True)
 elif proto==17 and len(payload)>20:
  sp,dp=struct.unpack('!HH',payload[:4])
  if 53 not in (sp,dp):continue
  dns=payload[8:];ident,flags,qd,an,ns,ar=struct.unpack('!HHHHHH',dns[:12]);pos=12;labels=[]
  if qd:
   while pos<len(dns) and dns[pos] and dns[pos]<64:
    n=dns[pos];pos+=1;labels.append(dns[pos:pos+n].decode(errors='replace'));pos+=n
   pos+=1
  pkt.update(proto='dns',sport=sp,dport=dp,id=ident,reply=bool(flags&0x8000),rcode=flags&15,qname='.'.join(labels),answers=an)
  if pos+4<=len(dns):pkt['qtype']=struct.unpack('!H',dns[pos:pos+2])[0]
  print(json.dumps(pkt),flush=True)
'''
env=os.environ.copy();env['DOCKER_CONFIG']=str(Path('.generated/docker-public-config').resolve());env['DOCKER_HOST']='unix:///Users/dangoodman/.docker/run/docker.sock'
stop=threading.Event()
try:
 launch(['docker','exec','sleepypods-review-remediation-control-plane','python3','-u','-c',trace],'network.log',env)
 launch(k+['logs','-f','deployment/sleepypods-frontline','--timestamps','--since=2s'],'frontline.log')
 launch(k+['logs','-f','deployment/sleepypods-control-plane','--timestamps','--since=2s'],'control-plane.log')
 def watch(watcher, resource):
  decoder=json.JSONDecoder();buf=''
  with (out/(resource+'.jsonl')).open('w') as f:
   while not stop.is_set():
    c=watcher.stdout.read(1)
    if not c:break
    buf+=c
    try:x,end=decoder.raw_decode(buf.lstrip())
    except json.JSONDecodeError:continue
    buf=buf.lstrip()[end:];o=x.get('object',{});m=o.get('metadata',{});kind=o.get('kind','');name=m.get('name','')
    if not name.startswith('e2e-app-'):continue
    row={'observed_utc':time.time(),'event':x.get('type'),'kind':kind,'name':name,'uid':m.get('uid'),'created':m.get('creationTimestamp'),'deleting':m.get('deletionTimestamp'),'owner':m.get('ownerReferences')}
    if kind=='Service':row.update(spec=o.get('spec'))
    elif kind=='EndpointSlice':row.update(endpoints=o.get('endpoints'),ports=o.get('ports'))
    elif kind=='Pod':
     row.update(status=o.get('status'))
     for container in o.get('status',{}).get('containerStatuses',[]):
      key=(name,container['name'])
      if key not in log_procs and 'running' in container.get('state',{}):
       log_procs[key]=launch(k+['logs','-f',name,'-c',container['name'],'--timestamps'],name+'-'+container['name']+'.log')
    print(json.dumps(row),file=f,flush=True)
 threads=[]
 for resource in ['pods','services','endpointslices']:
  watcher=subprocess.Popen(k+['get',resource,'--watch','--output-watch-events','-o','json'],stdout=subprocess.PIPE,stderr=(out/(resource+'-watch-errors.log')).open('w'),text=True,bufsize=1);procs.append(watcher)
  t=threading.Thread(target=watch,args=(watcher,resource));t.start();threads.append(t)
 launch(k+['port-forward','svc/sleepypods-frontline','19180:8080'],'port-forward.log')
 deadline=time.monotonic()+10
 while 'Forwarding from' not in (out/'port-forward.log').read_text() or 'capture ready' not in (out/'network.log').read_text():
  if time.monotonic()>deadline:raise RuntimeError('capture or port-forward not ready')
  time.sleep(.05)
 start=time.monotonic(); print('one request started',time.time(),flush=True)
 with socket.create_connection(('127.0.0.1',19180),timeout=35) as sock:
  sock.sendall(b'GET / HTTP/1.1\r\nHost: e2e.sleepypods.test\r\nConnection: close\r\n\r\n')
  response=http.client.HTTPResponse(sock);response.begin();body=response.read()
  record={'started_utc_epoch':time.time()-(time.monotonic()-start),'completed_utc_epoch':time.time(),'duration':time.monotonic()-start,'status':response.status,'headers':response.getheaders(),'body_length':len(body)}
  (out/'response.json').write_text(json.dumps(record,indent=2));print(json.dumps(record),flush=True)
 time.sleep(4)
finally:
 stop.set()
 for p in procs:
  if p.poll() is None:p.terminate()
 for p in procs:
  try:p.wait(timeout=2)
  except subprocess.TimeoutExpired:p.kill();p.wait()
 for f in files:f.close()
 for t in globals().get('threads',[]):t.join(timeout=2)
