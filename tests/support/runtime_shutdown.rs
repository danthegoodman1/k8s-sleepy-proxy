//! Test-only child-process fixture included by all three binary entrypoints.
//! Each invocation calls that binary's actual private run_with_runtime helper.
use super::{run_with_runtime, RUNTIME_SHUTDOWN_TIMEOUT};
use std::{error::Error, time::Duration};

#[test]
fn blocking_worker_cannot_pin_final_process_teardown() {
    const MODE: &str = "SLEEPYPODS_TEST_BLOCKING_RUNTIME_TEARDOWN";
    if let Ok(mode) = std::env::var(MODE) {
        let failed = mode == "bounded-error";
        let finished_async_cleanup = async {
            let (started, ready) = tokio::sync::oneshot::channel();
            tokio::task::spawn_blocking(move || {
                started.send(()).unwrap();
                loop {
                    std::thread::park();
                }
            });
            ready.await.unwrap();
            // Model a DNS worker left behind after its calling async task is
            // canceled and joined. The real startup signal tests are separate.
            let task = tokio::spawn(std::future::pending::<()>());
            tokio::task::yield_now().await;
            task.abort();
            assert!(task.await.unwrap_err().is_cancelled());
            eprintln!("blocking worker remains after owned async cleanup");
            if failed {
                Err::<(), Box<dyn Error + Send + Sync>>("controlled shutdown failure".into())
            } else {
                Ok(())
            }
        };
        if mode == "old" {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap();
            runtime.block_on(finished_async_cleanup).unwrap();
            // Reproduce the old #[tokio::main] implicit unbounded Runtime drop.
            drop(runtime);
        } else if let Err(error) = run_with_runtime(finished_async_cleanup) {
            assert!(failed);
            eprintln!("entrypoint returned error: {error}");
            std::process::exit(1);
        } else {
            assert!(!failed);
        }
        return;
    }
    use std::{
        io::{BufRead, BufReader},
        process::{Child, Command, Stdio},
        sync::mpsc,
        thread,
    };
    struct ChildGuard(Child);
    impl Drop for ChildGuard {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    for mode in ["old", "bounded-ok", "bounded-error"] {
        let mut child = ChildGuard(
            Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "runtime_shutdown_tests::blocking_worker_cannot_pin_final_process_teardown",
                    "--nocapture",
                ])
                .env(MODE, mode)
                .stdout(Stdio::null())
                .stderr(Stdio::piped())
                .spawn()
                .unwrap(),
        );
        let stderr = child.0.stderr.take().unwrap();
        let (ready, waiting) = mpsc::sync_channel(1);
        let reader = thread::spawn(move || {
            let mut ready = Some(ready);
            let mut log = String::new();
            for line in BufReader::new(stderr).lines() {
                let line = line.unwrap();
                if line.contains("blocking worker remains after owned async cleanup") {
                    if let Some(ready) = ready.take() {
                        let _ = ready.send(());
                    }
                }
                log.push_str(&line);
                log.push('\n');
            }
            log
        });
        waiting
            .recv_timeout(Duration::from_secs(3))
            .expect("child reached runtime teardown with a blocked worker");
        let started = std::time::Instant::now();
        if mode == "old" {
            thread::sleep(RUNTIME_SHUTDOWN_TIMEOUT + Duration::from_millis(250));
            assert!(
                child.0.try_wait().unwrap().is_none(),
                "old runtime must remain pinned after async cleanup"
            );
            child.0.kill().unwrap();
            child.0.wait().unwrap();
            println!(
                "old runtime remained pinned past {:?}; parent terminated the controlled child",
                started.elapsed()
            );
        } else {
            let status = loop {
                if let Some(status) = child.0.try_wait().unwrap() {
                    break status;
                }
                assert!(
                    started.elapsed() < Duration::from_secs(3),
                    "bounded runtime teardown did not finish"
                );
                thread::sleep(Duration::from_millis(5));
            };
            assert_eq!(
                status.code(),
                Some(i32::from(mode == "bounded-error")),
                "{status}"
            );
            println!(
                "{mode} runtime exited after {:?} with {status}",
                started.elapsed()
            );
        }
        let log = reader.join().unwrap();
        assert!(!log.contains("panicked"), "{log}");
        if mode == "bounded-error" {
            assert!(log.contains("controlled shutdown failure"), "{log}");
        }
    }
}
