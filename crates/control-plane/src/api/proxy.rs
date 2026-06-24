use std::{collections::HashMap, pin::Pin, sync::Arc, time::Duration};

use proxy_core::observability::recorder::ObservabilityRecorder;
use tokio::sync::broadcast;
use tonic::{
    codegen::tokio_stream::{self, wrappers::ReceiverStream},
    Request, Response, Status,
};

use crate::{
    api::{
        pb::{self, proxy_control_plane_server::ProxyControlPlaneServer},
        route_events::{RouteBindingChange, RouteBindingChangeReason, RouteSubscriptionBroker},
    },
    ids::{BackendGeneration, Generation, InstanceId},
    instance::{self as domain_instance, InstanceState},
    materialization::{
        LoadReadyMaterializationRequest, MaterializationRecord, MaterializationTarget,
    },
    materializer::{KubernetesMaterializer, KubernetesMaterializerClient},
    route as domain_route,
    store::{ControlPlaneStore, StoreError},
    wake::{self, WakeInstanceError, WakeInstanceResult, WakeUnavailableReason},
};

pub const PROXY_SERVICE_NAME: &str = "sleepypods.controlplane.v1.ProxyControlPlane";
const POSITIVE_ROUTE_CACHE_TTL: Duration = Duration::from_secs(10);
const SUBSCRIBE_RESPONSE_BUFFER: usize = 16;

type ProxySubscribeResponseStream = Pin<
    Box<
        dyn tokio_stream::Stream<Item = Result<pb::ProxySubscribeResponse, Status>>
            + Send
            + 'static,
    >,
>;

#[derive(Clone, Debug)]
struct ActiveRouteSubscription {
    route_binding_id: crate::ids::RouteBindingId,
    request_identity: domain_route::RouteIdentity,
    matched_identity: domain_route::RouteIdentity,
    protocol: domain_route::ProtocolRoute,
}

#[derive(Clone)]
pub struct StoreBackedProxyApi<C> {
    store: Arc<dyn ControlPlaneStore>,
    materializer: KubernetesMaterializer<C>,
    target: MaterializationTarget,
    observability: ObservabilityRecorder,
    route_events: RouteSubscriptionBroker,
}

impl<C> StoreBackedProxyApi<C> {
    pub fn new(
        store: Arc<dyn ControlPlaneStore>,
        materializer: KubernetesMaterializer<C>,
        target: MaterializationTarget,
    ) -> Self {
        Self::with_observability(
            store,
            materializer,
            target,
            ObservabilityRecorder::default(),
        )
    }

    pub fn with_observability(
        store: Arc<dyn ControlPlaneStore>,
        materializer: KubernetesMaterializer<C>,
        target: MaterializationTarget,
        observability: ObservabilityRecorder,
    ) -> Self {
        Self::with_observability_and_route_events(
            store,
            materializer,
            target,
            observability,
            RouteSubscriptionBroker::new(),
        )
    }

    pub fn with_route_events(
        store: Arc<dyn ControlPlaneStore>,
        materializer: KubernetesMaterializer<C>,
        target: MaterializationTarget,
        route_events: RouteSubscriptionBroker,
    ) -> Self {
        Self::with_observability_and_route_events(
            store,
            materializer,
            target,
            ObservabilityRecorder::default(),
            route_events,
        )
    }

    pub fn with_observability_and_route_events(
        store: Arc<dyn ControlPlaneStore>,
        materializer: KubernetesMaterializer<C>,
        target: MaterializationTarget,
        observability: ObservabilityRecorder,
        route_events: RouteSubscriptionBroker,
    ) -> Self {
        Self {
            store,
            materializer,
            target,
            observability,
            route_events,
        }
    }
}

pub type StoreBackedProxyGrpcService<C> = ProxyControlPlaneServer<StoreBackedProxyApi<C>>;

pub fn proxy_grpc_service_with_store<C>(
    store: Arc<dyn ControlPlaneStore>,
    materializer: KubernetesMaterializer<C>,
    target: MaterializationTarget,
) -> StoreBackedProxyGrpcService<C>
where
    C: KubernetesMaterializerClient + Clone + 'static,
{
    proxy_grpc_service_with_store_and_route_events(
        store,
        materializer,
        target,
        RouteSubscriptionBroker::new(),
    )
}

pub fn proxy_grpc_service_with_store_and_route_events<C>(
    store: Arc<dyn ControlPlaneStore>,
    materializer: KubernetesMaterializer<C>,
    target: MaterializationTarget,
    route_events: RouteSubscriptionBroker,
) -> StoreBackedProxyGrpcService<C>
where
    C: KubernetesMaterializerClient + Clone + 'static,
{
    ProxyControlPlaneServer::new(StoreBackedProxyApi::with_observability_and_route_events(
        store,
        materializer,
        target,
        ObservabilityRecorder::global(),
        route_events,
    ))
}

#[tonic::async_trait]
impl<C> pb::proxy_control_plane_server::ProxyControlPlane for StoreBackedProxyApi<C>
where
    C: KubernetesMaterializerClient + Clone + 'static,
{
    type SubscribeStream = ProxySubscribeResponseStream;

    async fn wake_instance(
        &self,
        request: Request<pb::ProxyWakeInstanceRequest>,
    ) -> Result<Response<pb::ProxyWakeInstanceResponse>, Status> {
        let request = proxy_wake_request_from_proto(request.into_inner(), self.target.clone())?;
        let instance_id = request.instance_id.as_str().to_owned();

        let result = wake::wake_instance_with_observability(
            self.store.as_ref(),
            &self.materializer,
            request,
            self.observability.clone(),
        )
        .await;
        let response = match result {
            Ok(WakeInstanceResult::Completed { result }) => {
                proxy_ready_response(&result.instance, &result.materialization)?
            }
            Ok(WakeInstanceResult::AlreadyRunning {
                instance,
                materialization,
            }) => proxy_ready_response(&instance, &materialization)?,
            Ok(WakeInstanceResult::AlreadyWaking { instance }) => pb::ProxyWakeInstanceResponse {
                outcome: Some(pb::proxy_wake_instance_response::Outcome::StillWaking(
                    pb::ProxyWakeStillWakingResult {
                        instance_id: instance.id.as_str().to_owned(),
                        instance_generation: instance.generation.get(),
                    },
                )),
            },
            Err(error) => proxy_wake_error_response(instance_id, error)?,
        };

        Ok(Response::new(response))
    }

    async fn subscribe(
        &self,
        request: Request<tonic::Streaming<pb::ProxySubscribeRequest>>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let mut requests = request.into_inner();
        let store = Arc::clone(&self.store);
        let target = self.target.clone();
        let mut route_events = self.route_events.subscribe();
        let (responses, response_stream) = tokio::sync::mpsc::channel(SUBSCRIBE_RESPONSE_BUFFER);

        tokio::spawn(async move {
            let mut subscriptions = HashMap::new();
            let mut next_subscription_number = 0_u64;

            loop {
                tokio::select! {
                    request = requests.message() => {
                        let Some(request) = (match request {
                            Ok(request) => request,
                            Err(status) => {
                                let _ = responses.send(Err(status)).await;
                                return;
                            }
                        }) else {
                            return;
                        };

                        let response = handle_subscribe_request(
                            store.as_ref(),
                            target.clone(),
                            request,
                            &mut subscriptions,
                            &mut next_subscription_number,
                        )
                        .await;

                        match response {
                            Ok(Some(response)) => {
                                if responses.send(Ok(response)).await.is_err() {
                                    return;
                                }
                            }
                            Ok(None) => {}
                            Err(status) => {
                                let _ = responses.send(Err(status)).await;
                                return;
                            }
                        }
                    }
                    event = route_events.recv() => {
                        match event {
                            Ok(event) => {
                                for response in invalidations_for_route_event(
                                    &mut subscriptions,
                                    &event,
                                ) {
                                    if responses.send(Ok(response)).await.is_err() {
                                        return;
                                    }
                                }
                            }
                            Err(broadcast::error::RecvError::Lagged(_)) => {
                                let _ = responses
                                    .send(Err(Status::unavailable("route update stream lagged")))
                                    .await;
                                return;
                            }
                            Err(broadcast::error::RecvError::Closed) => {}
                        }
                    }
                }
            }
        });

        Ok(Response::new(Box::pin(ReceiverStream::new(
            response_stream,
        ))))
    }
}

fn invalidations_for_route_event(
    subscriptions: &mut HashMap<String, ActiveRouteSubscription>,
    event: &RouteBindingChange,
) -> Vec<pb::ProxySubscribeResponse> {
    let mut invalidated = Vec::new();
    subscriptions.retain(|subscription_id, dependency| {
        if subscription_invalidated_by_event(dependency, event) {
            invalidated.push(subscription_id.clone());
            false
        } else {
            true
        }
    });

    invalidated
        .into_iter()
        .map(|subscription_id| pb::ProxySubscribeResponse {
            output: Some(pb::proxy_subscribe_response::Output::RouteInvalidated(
                pb::ProxyRouteInvalidatedResponse {
                    subscription_id,
                    reason: route_change_reason_to_proto(event.reason) as i32,
                },
            )),
        })
        .collect()
}

fn subscription_invalidated_by_event(
    subscription: &ActiveRouteSubscription,
    event: &RouteBindingChange,
) -> bool {
    match event.reason {
        RouteBindingChangeReason::Removed => {
            subscription.route_binding_id == event.route_binding_id
        }
        RouteBindingChangeReason::Changed => {
            if subscription.route_binding_id == event.route_binding_id {
                return true;
            }

            let (Some(identity), Some(protocol)) = (&event.identity, event.protocol) else {
                return false;
            };
            if subscription.protocol != protocol {
                return false;
            }

            let Some(new_score) =
                domain_route::route_match_score(identity, &subscription.request_identity)
            else {
                return false;
            };
            let Some(cached_score) = domain_route::route_match_score(
                &subscription.matched_identity,
                &subscription.request_identity,
            ) else {
                return true;
            };

            new_score > cached_score
        }
    }
}

fn route_change_reason_to_proto(
    reason: RouteBindingChangeReason,
) -> pb::ProxyRouteInvalidationReason {
    match reason {
        RouteBindingChangeReason::Removed => pb::ProxyRouteInvalidationReason::RouteRemoved,
        RouteBindingChangeReason::Changed => pb::ProxyRouteInvalidationReason::RouteChanged,
    }
}

async fn handle_subscribe_request(
    store: &dyn ControlPlaneStore,
    target: MaterializationTarget,
    request: pb::ProxySubscribeRequest,
    subscriptions: &mut HashMap<String, ActiveRouteSubscription>,
    next_subscription_number: &mut u64,
) -> Result<Option<pb::ProxySubscribeResponse>, Status> {
    match request
        .input
        .ok_or_else(|| Status::invalid_argument("subscribe request input is required"))?
    {
        pb::proxy_subscribe_request::Input::SubscribeRoute(request) => subscribe_route(
            store,
            target,
            request,
            subscriptions,
            next_subscription_number,
        )
        .await
        .map(Some),
        pb::proxy_subscribe_request::Input::Unsubscribe(request) => {
            let subscription_id = non_empty_field(request.subscription_id, "subscription_id")?;
            subscriptions.remove(&subscription_id);
            // Unsubscribe is idempotent and does not acknowledge; later update sources will
            // consult this per-stream map before sending subscription-targeted messages.
            Ok(None)
        }
    }
}

async fn subscribe_route(
    store: &dyn ControlPlaneStore,
    target: MaterializationTarget,
    request: pb::ProxySubscribeRouteRequest,
    subscriptions: &mut HashMap<String, ActiveRouteSubscription>,
    next_subscription_number: &mut u64,
) -> Result<pb::ProxySubscribeResponse, Status> {
    let request_id = non_empty_field(request.request_id, "request_id")?;
    let request_identity = request
        .identity
        .ok_or_else(|| Status::invalid_argument("identity is required"))
        .and_then(route_identity_from_proto)?;

    match store
        .resolve_route(request_identity.clone())
        .await
        .map_err(store_error_to_status)?
    {
        domain_route::RouteResolution::Resolved {
            matched_identity,
            mut entry,
        } => {
            publish_ready_backend(store, &mut entry, target).await?;
            let subscription_id = next_subscription_id(next_subscription_number);
            subscriptions.insert(
                subscription_id.clone(),
                ActiveRouteSubscription {
                    route_binding_id: entry.route_binding_id.clone(),
                    request_identity: request_identity.clone(),
                    matched_identity: matched_identity.clone(),
                    protocol: protocol_for_route_identity(&matched_identity),
                },
            );

            Ok(pb::ProxySubscribeResponse {
                output: Some(pb::proxy_subscribe_response::Output::RouteResolved(
                    pb::ProxyRouteResolvedResponse {
                        request_id,
                        subscription_id,
                        matched_identity: Some(route_identity_to_proto(matched_identity)),
                        route: Some(route_entry_to_proto(entry)),
                        cache_policy: Some(cache_policy_to_proto(domain_route::CachePolicy::new(
                            POSITIVE_ROUTE_CACHE_TTL,
                        ))),
                    },
                )),
            })
        }
        domain_route::RouteResolution::Miss { negative_cache } => Ok(pb::ProxySubscribeResponse {
            output: Some(pb::proxy_subscribe_response::Output::RouteMiss(
                pb::ProxyRouteMissResponse {
                    request_id,
                    request_identity: Some(route_identity_to_proto(request_identity)),
                    negative_cache_policy: Some(cache_policy_to_proto(negative_cache)),
                },
            )),
        }),
    }
}

fn protocol_for_route_identity(
    identity: &domain_route::RouteIdentity,
) -> domain_route::ProtocolRoute {
    match identity {
        domain_route::RouteIdentity::Http { .. } => domain_route::ProtocolRoute::Http,
        domain_route::RouteIdentity::Sni { .. } => domain_route::ProtocolRoute::TlsSni,
    }
}

async fn publish_ready_backend(
    store: &dyn ControlPlaneStore,
    entry: &mut domain_route::RouteEntry,
    target: MaterializationTarget,
) -> Result<(), Status> {
    entry.backend = None;
    entry.backend_generation = None;

    let materialization = store
        .load_ready_materialization(LoadReadyMaterializationRequest::new(
            entry.instance_id.clone(),
            entry.instance_generation,
            target,
        ))
        .await
        .map_err(store_error_to_status)?;

    if let Some(materialization) = materialization {
        if let Some(backend) = materialization.backend {
            entry.backend = Some(backend);
            entry.backend_generation = Some(materialization.backend_generation);
        }
    }

    Ok(())
}

fn next_subscription_id(next_subscription_number: &mut u64) -> String {
    *next_subscription_number += 1;
    format!("sub:{next_subscription_number}")
}

fn non_empty_field(value: String, field: &'static str) -> Result<String, Status> {
    if value.trim().is_empty() {
        return Err(Status::invalid_argument(format!(
            "{field} must not be empty"
        )));
    }

    Ok(value)
}

fn proxy_wake_request_from_proto(
    request: pb::ProxyWakeInstanceRequest,
    target: MaterializationTarget,
) -> Result<wake::WakeInstanceRequest, Status> {
    let mut wake_request = wake::WakeInstanceRequest::new(
        InstanceId::new(request.instance_id).map_err(invalid_argument_status)?,
        Generation::new(request.expected_generation),
        target,
    );
    if let Some(backend_generation) = request.backend_generation {
        wake_request =
            wake_request.with_backend_generation(BackendGeneration::new(backend_generation));
    }

    Ok(wake_request)
}

fn proxy_ready_response(
    instance: &domain_instance::InstanceRecord,
    materialization: &MaterializationRecord,
) -> Result<pb::ProxyWakeInstanceResponse, Status> {
    match materialization.backend.as_ref() {
        Some(backend) => Ok(pb::ProxyWakeInstanceResponse {
            outcome: Some(pb::proxy_wake_instance_response::Outcome::Ready(
                pb::ProxyWakeReadyResult {
                    instance_id: instance.id.as_str().to_owned(),
                    instance_generation: instance.generation.get(),
                    backend_uri: backend.uri().to_owned(),
                    backend_generation: materialization.backend_generation.get(),
                },
            )),
        }),
        None => Err(Status::failed_precondition(format!(
            "ready materialization for instance {} has no backend endpoint",
            instance.id.as_str()
        ))),
    }
}

fn proxy_wake_error_response(
    request_instance_id: String,
    error: WakeInstanceError,
) -> Result<pb::ProxyWakeInstanceResponse, Status> {
    match error {
        WakeInstanceError::NotFound => Err(Status::not_found("instance not found")),
        WakeInstanceError::GenerationConflict { expected, actual } => {
            Ok(pb::ProxyWakeInstanceResponse {
                outcome: Some(
                    pb::proxy_wake_instance_response::Outcome::GenerationConflict(
                        pb::ProxyWakeGenerationConflictResult {
                            instance_id: request_instance_id,
                            expected_generation: expected.get(),
                            actual_generation: actual.get(),
                        },
                    ),
                ),
            })
        }
        WakeInstanceError::Unavailable { instance, reason } => Ok(pb::ProxyWakeInstanceResponse {
            outcome: Some(pb::proxy_wake_instance_response::Outcome::Unavailable(
                pb::ProxyWakeUnavailableResult {
                    instance_id: instance.id.as_str().to_owned(),
                    instance_generation: instance.generation.get(),
                    reason: proxy_unavailable_reason_to_proto(reason) as i32,
                },
            )),
        }),
        WakeInstanceError::ReadyMaterializationNotFound { instance, .. } => {
            Err(Status::failed_precondition(format!(
                "ready materialization was not found for instance {} and configured target",
                instance.id.as_str()
            )))
        }
        WakeInstanceError::WorkloadClassNotFound { instance } => {
            Err(Status::failed_precondition(format!(
                "workload class version was not found for instance {}",
                instance.id.as_str()
            )))
        }
        WakeInstanceError::Render { instance, source } => Err(Status::internal(format!(
            "manifest render failed for instance {}: {source}",
            instance.id.as_str()
        ))),
        WakeInstanceError::SleepPolicy { instance, source } => Err(Status::internal(format!(
            "sleep policy resolution failed for instance {}: {source}",
            instance.id.as_str()
        ))),
        WakeInstanceError::Materializer { instance, source } => Err(Status::unavailable(format!(
            "materialization failed for instance {}: {source}",
            instance.id.as_str()
        ))),
        WakeInstanceError::Store(error) => Err(store_error_to_status(error)),
    }
}

fn proxy_unavailable_reason_to_proto(
    reason: WakeUnavailableReason,
) -> pb::ProxyWakeUnavailableReason {
    match reason {
        WakeUnavailableReason::Deleting => pb::ProxyWakeUnavailableReason::Deleting,
        WakeUnavailableReason::Deleted => pb::ProxyWakeUnavailableReason::Deleted,
    }
}

fn route_identity_from_proto(
    identity: pb::RouteIdentity,
) -> Result<domain_route::RouteIdentity, Status> {
    match identity
        .kind
        .ok_or_else(|| Status::invalid_argument("route identity kind is required"))?
    {
        pb::route_identity::Kind::Http(http) => Ok(domain_route::RouteIdentity::Http {
            host: http
                .host
                .ok_or_else(|| Status::invalid_argument("HTTP route host is required"))
                .and_then(route_host_from_proto)?,
            path: http
                .path_prefix
                .map(domain_route::PathPrefix::new)
                .transpose()
                .map_err(invalid_argument_status)?,
        }),
        pb::route_identity::Kind::Sni(sni) => Ok(domain_route::RouteIdentity::Sni {
            host: sni
                .host
                .ok_or_else(|| Status::invalid_argument("SNI route host is required"))
                .and_then(route_host_from_proto)?,
        }),
    }
}

fn route_host_from_proto(host: pb::RouteHost) -> Result<domain_route::RouteHost, Status> {
    match pb::RouteHostKind::try_from(host.kind)
        .map_err(|_| Status::invalid_argument("route host kind is invalid"))?
    {
        pb::RouteHostKind::Exact => {
            domain_route::RouteHost::exact(host.host).map_err(invalid_argument_status)
        }
        pb::RouteHostKind::WildcardSuffix => {
            domain_route::RouteHost::wildcard_suffix(host.host).map_err(invalid_argument_status)
        }
        pb::RouteHostKind::Unspecified => {
            Err(Status::invalid_argument("route host kind is required"))
        }
    }
}

fn route_identity_to_proto(identity: domain_route::RouteIdentity) -> pb::RouteIdentity {
    let kind = match identity {
        domain_route::RouteIdentity::Http { host, path } => {
            pb::route_identity::Kind::Http(pb::HttpRouteIdentity {
                host: Some(route_host_to_proto(host)),
                path_prefix: path.map(|path| path.as_str().to_owned()),
            })
        }
        domain_route::RouteIdentity::Sni { host } => {
            pb::route_identity::Kind::Sni(pb::SniRouteIdentity {
                host: Some(route_host_to_proto(host)),
            })
        }
    };

    pb::RouteIdentity { kind: Some(kind) }
}

fn route_host_to_proto(host: domain_route::RouteHost) -> pb::RouteHost {
    pb::RouteHost {
        kind: route_host_kind_to_proto(host.kind()) as i32,
        host: host.as_str().to_owned(),
    }
}

fn route_host_kind_to_proto(kind: domain_route::RouteHostKind) -> pb::RouteHostKind {
    match kind {
        domain_route::RouteHostKind::Exact => pb::RouteHostKind::Exact,
        domain_route::RouteHostKind::WildcardSuffix => pb::RouteHostKind::WildcardSuffix,
    }
}

fn route_entry_to_proto(entry: domain_route::RouteEntry) -> pb::ProxyRouteEntry {
    pb::ProxyRouteEntry {
        route_binding_id: entry.route_binding_id.as_str().to_owned(),
        instance_id: entry.instance_id.as_str().to_owned(),
        instance_state: instance_state_to_proto(entry.instance_state) as i32,
        instance_generation: entry.instance_generation.get(),
        backend_uri: entry.backend.map(|backend| backend.uri().to_owned()),
        backend_generation: entry
            .backend_generation
            .map(|backend_generation| backend_generation.get()),
    }
}

fn instance_state_to_proto(state: InstanceState) -> pb::InstanceState {
    match state {
        InstanceState::Cold => pb::InstanceState::Cold,
        InstanceState::Waking => pb::InstanceState::Waking,
        InstanceState::Running => pb::InstanceState::Running,
        InstanceState::Draining => pb::InstanceState::Draining,
        InstanceState::Failed => pb::InstanceState::Failed,
        InstanceState::Deleting => pb::InstanceState::Deleting,
        InstanceState::Deleted => pb::InstanceState::Deleted,
    }
}

fn cache_policy_to_proto(policy: domain_route::CachePolicy) -> pb::ProxyCachePolicy {
    pb::ProxyCachePolicy {
        ttl_millis: policy.ttl().as_millis().try_into().unwrap_or(u64::MAX),
    }
}

fn invalid_argument_status(error: impl std::fmt::Display) -> Status {
    Status::invalid_argument(error.to_string())
}

fn store_error_to_status(error: StoreError) -> Status {
    match error {
        StoreError::InvalidArgument { message } => Status::invalid_argument(message),
        StoreError::NotFound { resource } => Status::not_found(format!("{resource} not found")),
        StoreError::AlreadyExists { resource } => {
            Status::already_exists(format!("{resource} already exists"))
        }
        StoreError::GenerationConflict { expected, actual } => Status::failed_precondition(
            format!("generation conflict: expected generation {expected}, found {actual}"),
        ),
        StoreError::IdempotencyConflict => {
            Status::already_exists("idempotency key was already used for a different request")
        }
        StoreError::Unavailable { message } => Status::unavailable(message),
        StoreError::Internal { message } => Status::internal(message),
    }
}
