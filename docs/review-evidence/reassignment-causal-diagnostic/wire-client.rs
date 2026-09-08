use std::{io::{Read,Write},net::{SocketAddr,TcpStream},time::{Duration,Instant}};
struct Meter { stream: TcpStream, start: Instant, bytes: Vec<u8>, headers:bool, framed:bool }
impl Read for Meter {
 fn read(&mut self, dst:&mut [u8])->std::io::Result<usize>{
  match self.stream.read(dst) {
   Ok(n)=>{
    if self.bytes.is_empty() && n>0 { eprintln!("first_byte_ms={:.3}",self.start.elapsed().as_secs_f64()*1000.); }
    self.bytes.extend_from_slice(&dst[..n]);
    if let Some(end)=self.bytes.windows(4).position(|x|x==b"\r\n\r\n") {
     let head=String::from_utf8_lossy(&self.bytes[..end]);
     if !self.headers { eprintln!("headers_ms={:.3} head={:?}",self.start.elapsed().as_secs_f64()*1000.,head); self.headers=true; }
     let size=head.lines().find_map(|line|{let (k,v)=line.split_once(':')?; k.eq_ignore_ascii_case("content-length").then(||v.trim().parse::<usize>().ok()).flatten()});
     if !self.framed && size.is_some_and(|n|self.bytes.len()>=end+4+n){eprintln!("framed_complete_ms={:.3}",self.start.elapsed().as_secs_f64()*1000.);self.framed=true;}
    }
    if n==0 { eprintln!("eof_ms={:.3}",self.start.elapsed().as_secs_f64()*1000.); }
    Ok(n)
   }
   Err(e)=>{eprintln!("read_error_ms={:.3} kind={:?} captured_bytes={} framed_complete={}",self.start.elapsed().as_secs_f64()*1000.,e.kind(),self.bytes.len(),self.framed);Err(e)}
  }
 }
}
fn main(){
 let a=std::env::args().collect::<Vec<_>>();let addr=a[1].parse::<SocketAddr>().unwrap();let host=&a[2];let path=&a[3];let start=Instant::now();
 let mut stream=TcpStream::connect_timeout(&addr,Duration::from_secs(5)).unwrap();eprintln!("connect_ms={:.3}",start.elapsed().as_secs_f64()*1000.);
 stream.set_read_timeout(Some(Duration::from_secs(1))).unwrap();stream.set_write_timeout(Some(Duration::from_secs(1))).unwrap();
 write!(stream,"GET {path} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\r\n").unwrap();stream.flush().unwrap();eprintln!("write_ms={:.3}",start.elapsed().as_secs_f64()*1000.);
 let mut measured=Meter{stream,start,bytes:Vec::new(),headers:false,framed:false};let mut bytes=Vec::new();let result=measured.read_to_end(&mut bytes);
 eprintln!("read_to_end_result={result:?} elapsed_ms={:.3} bytes={}",start.elapsed().as_secs_f64()*1000.,bytes.len());
 if result.is_err(){std::process::exit(1)}
}
