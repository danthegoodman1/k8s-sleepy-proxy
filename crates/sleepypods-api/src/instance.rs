#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum InstanceState {
    Cold,
    Waking,
    Running,
    Draining,
    Failed,
    Deleting,
    Deleted,
}
