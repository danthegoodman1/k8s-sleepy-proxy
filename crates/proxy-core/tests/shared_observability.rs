use std::sync::Arc;

use sleepypods_observability::recorder::{
    InMemoryObservability, LifecycleLogEvent, ObservabilityEvent, ObservabilityRecorder,
};

#[test]
fn compatibility_reexport_and_shared_crate_use_one_global_recorder() {
    let sink = Arc::new(InMemoryObservability::default());
    assert!(ObservabilityRecorder::install_global(sink.clone()));
    proxy_core::observability::recorder::ObservabilityRecorder::global()
        .record_log(LifecycleLogEvent::new("through.proxy.reexport", Vec::new()));
    ObservabilityRecorder::global()
        .record_log(LifecycleLogEvent::new("through.shared.crate", Vec::new()));
    let names = sink
        .events()
        .into_iter()
        .map(|event| match event {
            ObservabilityEvent::Log(event) => event.name().to_owned(),
            _ => panic!("expected log event"),
        })
        .collect::<Vec<_>>();
    assert_eq!(names, ["through.proxy.reexport", "through.shared.crate"]);
}
