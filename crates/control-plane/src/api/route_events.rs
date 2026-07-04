use tokio::sync::broadcast;

use crate::{ProtocolRoute, RouteBindingId, RouteBindingRecord, RouteIdentity};

const ROUTE_EVENT_BUFFER: usize = 1024;

#[derive(Clone, Debug)]
pub struct RouteSubscriptionBroker {
    sender: broadcast::Sender<RouteBindingChange>,
}

#[derive(Clone, Debug)]
pub struct RouteBindingChange {
    pub route_binding_id: RouteBindingId,
    pub reason: RouteBindingChangeReason,
    pub identity: Option<RouteIdentity>,
    pub protocol: Option<ProtocolRoute>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RouteBindingChangeReason {
    Removed,
    Changed,
}

impl RouteSubscriptionBroker {
    pub fn new() -> Self {
        let (sender, _) = broadcast::channel(ROUTE_EVENT_BUFFER);
        Self { sender }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<RouteBindingChange> {
        self.sender.subscribe()
    }

    pub fn notify_route_removed(&self, route_binding_id: RouteBindingId) {
        self.notify(
            route_binding_id,
            RouteBindingChangeReason::Removed,
            None,
            None,
        );
    }

    pub fn notify_routes_removed(&self, route_bindings: &[RouteBindingRecord]) {
        for route_binding in route_bindings {
            self.notify_route_removed(route_binding.id.clone());
        }
    }

    pub fn notify_route_changed(
        &self,
        route_binding_id: RouteBindingId,
        identity: RouteIdentity,
        protocol: ProtocolRoute,
    ) {
        self.notify(
            route_binding_id,
            RouteBindingChangeReason::Changed,
            Some(identity),
            Some(protocol),
        );
    }

    pub fn notify_routes_changed(&self, route_bindings: &[RouteBindingRecord]) {
        for route_binding in route_bindings {
            self.notify_route_changed(
                route_binding.id.clone(),
                route_binding.identity.clone(),
                route_binding.protocol,
            );
        }
    }

    fn notify(
        &self,
        route_binding_id: RouteBindingId,
        reason: RouteBindingChangeReason,
        identity: Option<RouteIdentity>,
        protocol: Option<ProtocolRoute>,
    ) {
        let _ = self.sender.send(RouteBindingChange {
            route_binding_id,
            reason,
            identity,
            protocol,
        });
    }
}

impl Default for RouteSubscriptionBroker {
    fn default() -> Self {
        Self::new()
    }
}
