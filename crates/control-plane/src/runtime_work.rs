//! Durable scheduling and coalesced notification contracts.
use crate::{
    ids::{Generation, MaterializationId},
    materialization::RenderedObjectRef,
};

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MaterializationWorkStatus {
    /// Advisory age measured with the database clock. Only present while Ready;
    /// automatic sleep still rechecks eligibility atomically.
    pub ready_age: Option<std::time::Duration>,
    pub next_attempt_at_unix_millis: i64,
    pub operation_deadline_unix_millis: i64,
    /// Remaining duration sampled with the persistence clock, never computed
    /// by subtracting a process wall clock from the persisted timestamp.
    /// Callers deduct the entire status-request elapsed time conservatively.
    pub operation_remaining: Option<std::time::Duration>,
    pub failure_count: u32,
    pub failure_kind: Option<String>,
    pub failure_message: String,
    pub wake_failure_message: String,
    pub uncertain_effect: Option<UncertainMaterializationEffect>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UncertainMaterializationEffect {
    pub generation: Generation,
    pub owner: String,
    pub attempt: u64,
    pub effect_id: u64,
    pub operation: String,
    pub object: RenderedObjectRef,
    pub expected_uid: Option<String>,
    pub expected_resource_version: Option<String>,
    pub started_at_unix_millis: i64,
}

#[derive(Clone, Debug)]
pub struct RecordMaterializationFailure {
    pub expected_state: crate::materialization::MaterializationState,
    pub materialization_id: MaterializationId,
    pub owner: String,
    pub attempt: u64,
    pub generation: Generation,
    pub permanent: bool,
    pub message: String,
}

#[derive(Clone, Debug)]
pub struct DurableRouteChanges {
    pub cursor: u64,
    pub reset: bool,
    pub events: Vec<serde_json::Value>,
}

/// Cancellation is owned by the supervised caller; no Drop task is spawned.
#[derive(Clone, Debug)]
pub(crate) struct Cancellation(tokio::sync::watch::Sender<bool>);
impl Cancellation {
    pub fn new() -> Self {
        Self(tokio::sync::watch::channel(false).0)
    }
    pub fn cancel(&self) {
        self.0.send_replace(true);
    }
    pub fn is_cancelled(&self) -> bool {
        *self.0.borrow()
    }
    pub async fn cancelled(&self) {
        let mut receiver = self.0.subscribe();
        if *receiver.borrow_and_update() {
            return;
        }
        while receiver.changed().await.is_ok() {
            if *receiver.borrow_and_update() {
                return;
            }
        }
    }
}
