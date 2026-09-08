from pathlib import Path
import os,socket,subprocess,sys,time,threading,json,hashlib,urllib.request
root=Path('/Users/dangoodman/code/sleepy-pods');out=root/'docs/review-evidence/reassignment-causal-diagnostic';owned=[];files=[]
def port():
 with socket.socket() as s:s.bind(('127.0.0.1',0));return s.getsockname()[1]
def launch(name,args,vars):
 f=(out/name).open('w');files.append(f);p=subprocess.Popen(args,cwd=root,env=dict(os.environ,**vars),stdout=f,stderr=subprocess.STDOUT);owned.append(p);return p
def ready(p):
 end=time.monotonic()+10
 while time.monotonic()<end:
  try:
   with socket.create_connection(('127.0.0.1',p),.2):return
  except OSError:time.sleep(.05)
 raise RuntimeError(f'listener {p} did not start')
def client(label,p,path):
 r=subprocess.run(['/private/tmp/reassignment-wire-client',f'127.0.0.1:{p}','app.example.test',path],text=True,capture_output=True,timeout=5)
 (out/(label+'.log')).write_text(r.stdout+r.stderr);return {'case':label,'exit_code':r.returncode,'trace':r.stderr}
try:
 app,backend,grpc,cp,front=[port() for _ in range(5)]
 launch('local-python-app.log',[sys.executable,'-c',"import runpy,http.server,socketserver,os; m=runpy.run_path('scripts/kind-routing-app.py'); http.server.HTTPServer.server_bind=lambda self:(socketserver.TCPServer.server_bind(self),setattr(self,'server_name','localhost'),setattr(self,'server_port',self.server_address[1])); http.server.ThreadingHTTPServer(('127.0.0.1',int(os.environ['PORT'])),m['Handler']).serve_forever()"],{'PORT':str(app),'SLEEPYPODS_E2E_INSTANCE':'local-old'})
 launch('local-helper.log',['target/debug/examples/frontline_load_smoke','server'],{'SLEEPYPODS_LOAD_SMOKE_BACKEND_ADDR':f'127.0.0.1:{backend}','SLEEPYPODS_LOAD_SMOKE_GRPC_BACKEND_ADDR':f'127.0.0.1:{grpc}','SLEEPYPODS_LOAD_SMOKE_CONTROL_PLANE_ADDR':f'127.0.0.1:{cp}','SLEEPYPODS_LOAD_SMOKE_BACKEND_URI':f'http://127.0.0.1:{app}','SLEEPYPODS_LOAD_SMOKE_ROUTE_PATH':'/'})
 ready(app);ready(cp)
 launch('local-frontline.log',['target/debug/frontline'],{'SLEEPYPODS_CONTROL_PLANE_ENDPOINT':f'http://127.0.0.1:{cp}','SLEEPYPODS_FRONTLINE_LISTEN_ADDR':f'127.0.0.1:{front}'})
 ready(front)
 results=[client('direct-python',app,'/direct')]
 results += [client(f'frontline-fresh-{i}',front,f'/fresh-{i}') for i in range(3)]
 with urllib.request.urlopen(f'http://127.0.0.1:{backend}/__sleepypods_load_smoke_stats',timeout=2) as response:stats=response.read().decode()
 listener=socket.socket();listener.bind(('127.0.0.1',0));listener.listen();lateport=listener.getsockname()[1]
 def delayed_eof():
  with listener:
   c,_=listener.accept()
   with c:
    c.recv(4096);c.sendall(b'HTTP/1.1 200 OK\r\nContent-Length: 3\r\nConnection: close\r\n\r\nok\n');time.sleep(1.5)
 t=threading.Thread(target=delayed_eof);t.start();results.append(client('controlled-complete-body-late-eof',lateport,'/'));t.join()
 meta={'scope':'Native existing frontend binary + existing fakeCP helper + existing Python HTTP/1.0 routing Handler with diagnostic loopback-only binding and getfqdn bypass (native host reverse-DNS blocked before listen); no Kubernetes port-forward. Controlled lateEOF is synthetic, not attribution of deployed failure.','binaries':{p:hashlib.sha256((root/p).read_bytes()).hexdigest() for p in ['target/debug/frontline','target/debug/examples/frontline_load_smoke']},'results':results,'helper_stats':stats}
 (out/'local-wire-results.json').write_text(json.dumps(meta,indent=2)+'\n');print(json.dumps(meta,indent=2))
finally:
 for p in reversed(owned):
  p.terminate()
  try:p.wait(timeout=5)
  except subprocess.TimeoutExpired:p.kill();p.wait()
 for f in files:f.close()
