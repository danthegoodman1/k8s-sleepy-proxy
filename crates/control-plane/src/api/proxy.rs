use std::{collections::HashMap, pin::Pin, sync::Arc, time::Duration};

use tonic::{
    codegen::tokio_stream::{self, wrappers::ReceiverStream},
    Request, Response, Status,
};

use crate::{
    api::pb::{self, proxy_control_plane_server::ProxyControlPlaneServer},
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

#[derive(Clone)]
pub struct StoreBackedProxyApi<C> {
    store: Arc<dyn ControlPlaneStore>,
    materializer: KubernetesMaterializer<C>,
    target: MaterializationTarget,
}

impl<C> StoreBackedProxyApi<C> {
    pub fn new(
        store: Arc<dyn ControlPlaneStore>,
        materializer: KubernetesMaterializer<C>,
        target: MaterializationTarget,
    ) -> Self {
        Self {
            store,
            materializer,
            target,
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
    ProxyControlPlaneServer::new(StoreBackedProxyApi::new(store, materializer, target))
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

        let result = wake::wake_instance(self.store.as_ref(), &self.materializer, request).await;
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
        let (responses, response_stream) = tokio::sync::mpsc::channel(SUBSCRIBE_RESPONSE_BUFFER);

        tokio::spawn(async move {
            let mut subscriptions = HashMap::new();
            let mut next_subscription_number = 0_u64;

            while let Some(request) = match requests.message().await {
                Ok(request) => request,
                Err(status) => {
                    let _ = responses.send(Err(status)).await;
                    return;
                }
            } {
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
        });

        Ok(Response::new(Box::pin(ReceiverStream::new(
            response_stream,
        ))))
    }
}

async fn handle_subscribe_request(
    store: &dyn ControlPlaneStore,
    target: MaterializationTarget,
    request: pb::ProxySubscribeRequest,
    subscriptions: &mut HashMap<String, domain_route::RouteDependencyLookup>,
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
    subscriptions: &mut HashMap<String, domain_route::RouteDependencyLookup>,
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
                domain_route::RouteDependencyLookup::from_route_entry(&entry),
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
