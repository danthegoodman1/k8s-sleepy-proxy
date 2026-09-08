#![cfg(unix)]

use std::{
    io::{BufRead, BufReader},
    net::{SocketAddr, TcpListener, TcpStream},
    process::{Child, Command, Stdio},
    sync::mpsc,
    thread,
    time::{Duration, Instant},
};

extern "C" {
    fn kill(pid: i32, sig: i32) -> i32;
}

struct ChildGuard(Child);
impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn unused_addr() -> SocketAddr {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
}

#[test]
fn sigterm_cleanly_stops_frontline_during_initial_cp_retry() {
    startup_signal(15);
}

#[test]
fn sigint_cleanly_stops_frontline_during_initial_cp_retry() {
    startup_signal(2);
}

fn startup_signal(signal: i32) {
    let addr = unused_addr();
    let cp = unused_addr();
    let mut command = Command::new(env!("CARGO_BIN_EXE_frontline"));
    // Keep this process fixture independent of developer/runtime configuration.
    for (name, _) in std::env::vars_os() {
        if name.to_string_lossy().starts_with("SLEEPYPODS_") {
            command.env_remove(name);
        }
    }
    let mut child = ChildGuard(
        command
            .env("SLEEPYPODS_FRONTLINE_LISTEN_ADDR", addr.to_string())
            .env("SLEEPYPODS_CONTROL_PLANE_ENDPOINT", format!("http://{cp}"))
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap(),
    );
    let stderr = child.0.stderr.take().unwrap();
    let (started_tx, started_rx) = mpsc::sync_channel(1);
    let reader = thread::spawn(move || {
        let mut started_tx = Some(started_tx);
        let mut log = String::new();
        for line in BufReader::new(stderr).lines() {
            let line = line.unwrap();
            if line.contains("frontline waiting for control-plane endpoint") {
                if let Some(tx) = started_tx.take() {
                    let _ = tx.send(());
                }
            }
            log.push_str(&line);
            log.push('\n');
        }
        log
    });
    started_rx
        .recv_timeout(Duration::from_secs(3))
        .expect("actual startup entered CP retry loop");
    assert!(
        TcpStream::connect(addr).is_err(),
        "frontline must not publish its listener before initial CP setup"
    );
    // The child has installed its handler before its first real failed connect.
    assert_eq!(unsafe { kill(child.0.id() as i32, signal) }, 0);
    let deadline = Instant::now() + Duration::from_secs(3);
    let status = loop {
        if let Some(status) = child.0.try_wait().unwrap() {
            break status;
        }
        assert!(
            Instant::now() < deadline,
            "startup signal failed to stop frontline promptly"
        );
        thread::sleep(Duration::from_millis(5));
    };
    let log = reader.join().unwrap();
    assert!(
        status.success(),
        "signal must follow clean cancellation, not default signal termination: {status}\n{log}"
    );
    assert!(!log.contains("frontline runtime failed"), "{log}");
    assert!(TcpStream::connect(addr).is_err());
}
