import os

import socket,struct,time,json,datetime
s=socket.socket(socket.AF_PACKET,socket.SOCK_RAW,socket.htons(3));s.settimeout(.5)
end=time.monotonic()+45
print('capture ready: connection headers and DNS only',flush=True)
while time.monotonic()<end:
 try: data,addr=s.recvfrom(65535)
 except socket.timeout: continue
 if len(data)<34 or data[12:14]!=b'\x08\x00': continue
 ip=data[14:];ihl=(ip[0]&15)*4
 if len(ip)<ihl+8:continue
 src=socket.inet_ntoa(ip[12:16]);dst=socket.inet_ntoa(ip[16:20]);proto=ip[9]
 if os.environ['SNI_CAPTURE_FRONTLINE_IP'] not in (src,dst):continue
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
