use std::{
    collections::HashMap,
    error::Error,
    fmt,
    net::{AddrParseError, SocketAddr},
    sync::Arc,
};

use proxy_core::observability::{
    metrics::{
        EXCLUSIVITY_KEYS_HELD, MATERIALIZATIONS_NONTERMINAL,
        MATERIALIZATION_OLDEST_NONTERMINAL_AGE_SECONDS,
    },
    prometheus::{serve_prometheus_metrics_with_collector, PrometheusMetricsSink},
    recorder::{
        CompositeObservabilitySink, MetricObservation, ObservabilityRecorder,
        StderrObservabilitySink,
    },
};
use tokio::{sync::watch, task::JoinSet};
use tonic::transport::server::Router;
use tower::layer::util::{Identity, Stack};
use tower_http::cors::CorsLayer;

use crate::{
    api::{
        operator_grpc_service_with_store_and_route_events, operator_grpc_web_server_builder,
        proxy_grpc_service_with_store_and_route_events, sidecar_grpc_service_with_store,
        RouteSubscriptionBroker,
    },
    auth::{AuthConfig, ControlPlaneAuth, InvalidStaticBearerTokens, StaticBearerTokens},
    config::{ControlPlaneConfig, PostgresStoreConfig, StoreProviderConfig, StoreProviderName},
    materialization::{
        InvalidMaterializationTarget, LoadMaterializationOperationalMetricsRequest,
        MaterializationState, MaterializationTarget,
    },
    materializer::{
        KubernetesMaterializer, KubernetesMaterializerClient, RetryingKubernetesMaterializerClient,
    },
    postgres::PostgresStore,
    reconciler::{MaterializationReconciler, MaterializationReconcilerConfig},
    store::{ControlPlaneStore, RetryingControlPlaneStore},
    KubeMaterializerClient,
};

pub const CONTROL_PLANE_LISTEN_ADDR_ENV: &str = "SLEEPYPODS_CONTROL_PLANE_LISTEN_ADDR";
pub const OPERATOR_GRPC_WEB_LISTEN_ADDR_ENV: &str = "SLEEPYPODS_OPERATOR_GRPC_WEB_LISTEN_ADDR";
pub const STORE_PROVIDER_ENV: &str = "SLEEPYPODS_STORE_PROVIDER";
pub const POSTGRES_URL_ENV: &str = "SLEEPYPODS_POSTGRES_URL";
pub const CLUSTER_ID_ENV: &str = "SLEEPYPODS_CLUSTER_ID";
pub const NAMESPACE_ENV: &str = "SLEEPYPODS_NAMESPACE";
pub const AUTH_MODE_ENV: &str = "SLEEPYPODS_CONTROL_PLANE_AUTH_MODE";
pub const AUTH_OPERATOR_TOKEN_ENV: &str = "SLEEPYPODS_CONTROL_PLANE_OPERATOR_TOKEN";
pub const AUTH_PROXY_TOKEN_ENV: &str = "SLEEPYPODS_CONTROL_PLANE_PROXY_TOKEN";
pub const AUTH_SIDECAR_TOKEN_ENV: &str = "SLEEPYPODS_CONTROL_PLANE_SIDECAR_TOKEN";
pub const METRICS_LISTEN_ADDR_ENV: &str = "SLEEPYPODS_CONTROL_PLANE_METRICS_LISTEN_ADDR";

pub type NativeControlPlaneRouter = Router<Identity>;
pub type OperatorGrpcWebRouter = Router<Stack<tonic_web::GrpcWebLayer, Stack<CorsLayer, Identity>>>;
pub type RuntimeResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RuntimeConfig {
    pub listen_addr: SocketAddr,
    pub operator_grpc_web_listen_addr: Option<SocketAddr>,
    pub metrics_listen_addr: Option<SocketAddr>,
    pub control_plane: ControlPlaneConfig,
    pub target: MaterializationTarget,
}

#[derive(Debug)]
pub enum RuntimeConfigError {
    MissingEnv {
        name: &'static str,
    },
    InvalidListenAddr {
        name: &'static str,
        value: String,
        source: AddrParseError,
    },
    InvalidStoreProvider {
        value: String,
    },
    InvalidPostgresConfig(crate::ids::EmptyStringError),
    InvalidMaterializationTarget(InvalidMaterializationTarget),
    InvalidAuthMode {
        value: String,
    },
    InvalidAuthConfig(InvalidStaticBearerTokens),
}

impl RuntimeConfig {
    pub fn from_env() -> Result<Self, RuntimeConfigError> {
        Self::from_key_values(std::env::vars())
    }

    pub fn from_key_values<I, K, V>(values: I) -> Result<Self, RuntimeConfigError>
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        let values = values
            .into_iter()
            .map(|(key, value)| (key.into(), value.into()))
            .collect::<HashMap<_, _>>();

        let listen_addr = parse_required_socket_addr(&values, CONTROL_PLANE_LISTEN_ADDR_ENV)?;
        let operator_grpc_web_listen_addr =
            parse_optional_socket_addr(&values, OPERATOR_GRPC_WEB_LISTEN_ADDR_ENV)?;
        let metrics_listen_addr = parse_optional_socket_addr(&values, METRICS_LISTEN_ADDR_ENV)?;
        let provider = required_value(&values, STORE_PROVIDER_ENV)?;
        let store = match provider.parse::<StoreProviderName>().map_err(|_| {
            RuntimeConfigError::InvalidStoreProvider {
                value: provider.to_owned(),
            }
        })? {
            StoreProviderName::Postgres => {
                let url = required_value(&values, POSTGRES_URL_ENV)?;
                StoreProviderConfig::Postgres(
                    PostgresStoreConfig::new(url)
                        .map_err(RuntimeConfigError::InvalidPostgresConfig)?,
                )
            }
        };
        let auth = parse_auth_config(&values)?;
        let target = MaterializationTarget::new(
            required_value(&values, CLUSTER_ID_ENV)?,
            required_value(&values, NAMESPACE_ENV)?,
        )
        .map_err(RuntimeConfigError::InvalidMaterializationTarget)?;

        Ok(Self {
            listen_addr,
            operator_grpc_web_listen_addr,
            metrics_listen_addr,
            control_plane: ControlPlaneConfig::new(store, auth),
            target,
        })
    }
}

pub fn native_control_plane_router<C>(
    store: Arc<dyn ControlPlaneStore>,
    materializer: KubernetesMaterializer<C>,
    target: MaterializationTarget,
) -> NativeControlPlaneRouter
where
    C: KubernetesMaterializerClient + Clone + 'static,
{
    let route_events = RouteSubscriptionBroker::new();
    native_control_plane_router_with_route_events(
        store,
        materializer,
        target,
        AuthConfig::NoAuth,
        route_events,
    )
}

fn native_control_plane_router_with_route_events<C>(
    store: Arc<dyn ControlPlaneStore>,
    materializer: KubernetesMaterializer<C>,
    target: MaterializationTarget,
    auth_config: AuthConfig,
    route_events: RouteSubscriptionBroker,
) -> NativeControlPlaneRouter
where
    C: KubernetesMaterializerClient + Clone + 'static,
{
    let auth = ControlPlaneAuth::from_config(auth_config, ObservabilityRecorder::global());
    tonic::transport::Server::builder()
        .add_service(tonic::service::interceptor::InterceptedService::new(
            operator_grpc_service_with_store_and_route_events(
                Arc::clone(&store),
                materializer.clone(),
                target.clone(),
                route_events.clone(),
            ),
            auth.interceptor(
                crate::api::OPERATOR_SERVICE_NAME,
                crate::auth::CallerRole::Operator,
            ),
        ))
        .add_service(tonic::service::interceptor::InterceptedService::new(
            proxy_grpc_service_with_store_and_route_events(
                Arc::clone(&store),
                materializer.clone(),
                target.clone(),
                route_events,
            ),
            auth.interceptor(
                crate::api::PROXY_SERVICE_NAME,
                crate::auth::CallerRole::Proxy,
            ),
        ))
        .add_service(tonic::service::interceptor::InterceptedService::new(
            sidecar_grpc_service_with_store(store, materializer, target),
            auth.interceptor(
                crate::api::SIDECAR_SERVICE_NAME,
                crate::auth::CallerRole::Sidecar,
            ),
        ))
}

pub fn operator_grpc_web_router<C>(
    store: Arc<dyn ControlPlaneStore>,
    materializer: KubernetesMaterializer<C>,
    target: MaterializationTarget,
) -> OperatorGrpcWebRouter
where
    C: KubernetesMaterializerClient + Clone + 'static,
{
    operator_grpc_web_router_with_route_events(
        store,
        materializer,
        target,
        AuthConfig::NoAuth,
        RouteSubscriptionBroker::new(),
    )
}

fn operator_grpc_web_router_with_route_events<C>(
    store: Arc<dyn ControlPlaneStore>,
    materializer: KubernetesMaterializer<C>,
    target: MaterializationTarget,
    auth_config: AuthConfig,
    route_events: RouteSubscriptionBroker,
) -> OperatorGrpcWebRouter
where
    C: KubernetesMaterializerClient + Clone + 'static,
{
    let auth = ControlPlaneAuth::from_config(auth_config, ObservabilityRecorder::global());
    operator_grpc_web_server_builder().add_service(
        tonic::service::interceptor::InterceptedService::new(
            operator_grpc_service_with_store_and_route_events(
                store,
                materializer,
                target,
                route_events,
            ),
            auth.interceptor(
                crate::api::OPERATOR_SERVICE_NAME,
                crate::auth::CallerRole::Operator,
            ),
        ),
    )
}

pub async fn run_from_env() -> RuntimeResult<()> {
    let config = RuntimeConfig::from_env()?;
    let prometheus = install_runtime_observability(config.metrics_listen_addr.is_some());
    let store = connect_store(&config.control_plane.store).await?;
    let kube_client = KubeMaterializerClient::try_default().await?;
    let materializer = KubernetesMaterializer::new(
        RetryingKubernetesMaterializerClient::with_default_policy(kube_client),
    );

    serve(config, store, materializer, prometheus).await?;

    Ok(())
}

pub async fn serve<C>(
    config: RuntimeConfig,
    store: Arc<dyn ControlPlaneStore>,
    materializer: KubernetesMaterializer<C>,
    prometheus: Option<PrometheusMetricsSink>,
) -> RuntimeResult<()>
where
    C: KubernetesMaterializerClient + Clone + 'static,
{
    let materializer = materializer_with_runtime_auth(materializer, &config.control_plane.auth);
    let route_events = RouteSubscriptionBroker::new();
    let native_router = native_control_plane_router_with_route_events(
        Arc::clone(&store),
        materializer.clone(),
        config.target.clone(),
        config.control_plane.auth.clone(),
        route_events.clone(),
    );
    let (shutdown_tx, _) = watch::channel(false);
    let native_shutdown = shutdown_tx.subscribe();
    let reconciler_shutdown = shutdown_tx.subscribe();
    let reconciler = MaterializationReconciler::new(
        Arc::clone(&store),
        materializer.clone(),
        MaterializationReconcilerConfig::default(),
        ObservabilityRecorder::global(),
    );
    tokio::spawn(async move {
        reconciler.run_until_shutdown(reconciler_shutdown).await;
    });

    let shutdown_for_signal = shutdown_tx.clone();
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        let _ = shutdown_for_signal.send(true);
    });

    let mut listeners = JoinSet::new();
    listeners.spawn({
        let native_shutdown = native_shutdown.clone();
        async move {
            native_router
                .serve_with_shutdown(config.listen_addr, wait_for_shutdown(native_shutdown))
                .await
                .map_err(|error| Box::new(error) as Box<dyn Error + Send + Sync>)
        }
    });

    if let Some(operator_grpc_web_addr) = config.operator_grpc_web_listen_addr {
        let operator_grpc_web_router = operator_grpc_web_router_with_route_events(
            Arc::clone(&store),
            materializer.clone(),
            config.target.clone(),
            config.control_plane.auth.clone(),
            route_events,
        );
        let operator_grpc_web_shutdown = native_shutdown.clone();
        listeners.spawn(async move {
            operator_grpc_web_router
                .serve_with_shutdown(
                    operator_grpc_web_addr,
                    wait_for_shutdown(operator_grpc_web_shutdown),
                )
                .await
                .map_err(|error| Box::new(error) as Box<dyn Error + Send + Sync>)
        });
    }

    if let Some(metrics_addr) = config.metrics_listen_addr {
        let prometheus = prometheus.unwrap_or_else(PrometheusMetricsSink::new);
        let metrics_shutdown = native_shutdown.clone();
        let store = Arc::clone(&store);
        listeners.spawn(async move {
            serve_prometheus_metrics_with_collector(
                metrics_addr,
                prometheus,
                wait_for_shutdown(metrics_shutdown),
                move |sink| {
                    let store = Arc::clone(&store);
                    async move {
                        record_materialization_operational_metrics(store, sink).await;
                    }
                },
            )
            .await
            .map_err(|error| Box::new(error) as Box<dyn Error + Send + Sync>)
        });
    }

    let mut first_error = None;
    while let Some(result) = listeners.join_next().await {
        match result {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                let _ = shutdown_tx.send(true);
                if first_error.is_none() {
                    first_error = Some(error);
                }
            }
            Err(error) => {
                let _ = shutdown_tx.send(true);
                if first_error.is_none() {
                    first_error = Some(Box::new(error) as Box<dyn Error + Send + Sync>);
                }
            }
        }
    }

    first_error.map_or(Ok(()), Err)
}

fn install_runtime_observability(metrics_enabled: bool) -> Option<PrometheusMetricsSink> {
    if !metrics_enabled {
        let _ = ObservabilityRecorder::install_stderr_global();
        return None;
    }

    let prometheus = PrometheusMetricsSink::new();
    let _ = ObservabilityRecorder::install_global(Arc::new(CompositeObservabilitySink::new(vec![
        Arc::new(StderrObservabilitySink),
        Arc::new(prometheus.clone()),
    ])));
    Some(prometheus)
}

async fn record_materialization_operational_metrics(
    store: Arc<dyn ControlPlaneStore>,
    sink: PrometheusMetricsSink,
) {
    for state in MaterializationState::BACKLOG_STATES {
        let state = state.metric_label();
        sink.record_observation(MetricObservation::new(
            MATERIALIZATIONS_NONTERMINAL,
            vec![state],
            0.0,
        ));
        sink.record_observation(MetricObservation::new(
            MATERIALIZATION_OLDEST_NONTERMINAL_AGE_SECONDS,
            vec![state],
            0.0,
        ));
    }

    for state in MaterializationState::HELD_KEY_STATES {
        let state = state.metric_label();
        sink.record_observation(MetricObservation::new(
            EXCLUSIVITY_KEYS_HELD,
            vec![state],
            0.0,
        ));
    }

    let Ok(metrics) = store
        .load_materialization_operational_metrics(
            LoadMaterializationOperationalMetricsRequest::new(std::time::SystemTime::now()),
        )
        .await
    else {
        return;
    };

    for state in metrics.backlog_states {
        let label = state.state.metric_label();
        sink.record_observation(MetricObservation::new(
            MATERIALIZATIONS_NONTERMINAL,
            vec![label],
            state.count as f64,
        ));
        sink.record_observation(MetricObservation::new(
            MATERIALIZATION_OLDEST_NONTERMINAL_AGE_SECONDS,
            vec![label],
            state
                .oldest_age
                .map(|age| age.as_secs_f64())
                .unwrap_or_default(),
        ));
    }

    for state in metrics.held_key_states {
        let label = state.state.metric_label();
        sink.record_observation(MetricObservation::new(
            EXCLUSIVITY_KEYS_HELD,
            vec![label],
            state.exclusivity_keys_held as f64,
        ));
    }
}

fn materializer_with_runtime_auth<C>(
    materializer: KubernetesMaterializer<C>,
    auth_config: &AuthConfig,
) -> KubernetesMaterializer<C>
where
    C: KubernetesMaterializerClient,
{
    materializer.with_sidecar_control_plane_token(auth_config.sidecar_bearer_token().cloned())
}

async fn connect_store(
    config: &StoreProviderConfig,
) -> Result<Arc<dyn ControlPlaneStore>, crate::StoreError> {
    match config {
        StoreProviderConfig::Postgres(config) => {
            let store = PostgresStore::connect(config).await?;
            let store: Arc<dyn ControlPlaneStore> = Arc::new(store);
            Ok(Arc::new(RetryingControlPlaneStore::with_default_policy(
                store,
            )))
        }
    }
}

async fn wait_for_shutdown(mut shutdown: watch::Receiver<bool>) {
    while !*shutdown.borrow() {
        if shutdown.changed().await.is_err() {
            return;
        }
    }
}

fn required_value<'a>(
    values: &'a HashMap<String, String>,
    name: &'static str,
) -> Result<&'a str, RuntimeConfigError> {
    values
        .get(name)
        .map(String::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or(RuntimeConfigError::MissingEnv { name })
}

fn parse_required_socket_addr(
    values: &HashMap<String, String>,
    name: &'static str,
) -> Result<SocketAddr, RuntimeConfigError> {
    let value = required_value(values, name)?;
    parse_socket_addr(name, value)
}

fn parse_optional_socket_addr(
    values: &HashMap<String, String>,
    name: &'static str,
) -> Result<Option<SocketAddr>, RuntimeConfigError> {
    let Some(value) = values
        .get(name)
        .map(String::as_str)
        .filter(|value| !value.trim().is_empty())
    else {
        return Ok(None);
    };

    parse_socket_addr(name, value).map(Some)
}

fn parse_socket_addr(name: &'static str, value: &str) -> Result<SocketAddr, RuntimeConfigError> {
    value
        .parse()
        .map_err(|source| RuntimeConfigError::InvalidListenAddr {
            name,
            value: value.to_owned(),
            source,
        })
}

fn parse_auth_config(values: &HashMap<String, String>) -> Result<AuthConfig, RuntimeConfigError> {
    let mode = required_value(values, AUTH_MODE_ENV)?;
    match mode.trim().to_ascii_lowercase().as_str() {
        "no-auth" => Ok(AuthConfig::NoAuth),
        "static-bearer-token" | "static-bearer-tokens" => {
            let tokens = StaticBearerTokens::new(
                required_value(values, AUTH_OPERATOR_TOKEN_ENV)?,
                required_value(values, AUTH_PROXY_TOKEN_ENV)?,
                required_value(values, AUTH_SIDECAR_TOKEN_ENV)?,
            )
            .map_err(RuntimeConfigError::InvalidAuthConfig)?;
            Ok(AuthConfig::static_bearer_tokens(tokens))
        }
        _ => Err(RuntimeConfigError::InvalidAuthMode {
            value: mode.to_owned(),
        }),
    }
}

impl fmt::Display for RuntimeConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingEnv { name } => write!(f, "{name} is required"),
            Self::InvalidListenAddr {
                name,
                value,
                source,
            } => write!(
                f,
                "{name} value {value:?} is not a valid listen address: {source}"
            ),
            Self::InvalidStoreProvider { value } => {
                write!(f, "{STORE_PROVIDER_ENV} value {value:?} is not supported")
            }
            Self::InvalidPostgresConfig(source) => source.fmt(f),
            Self::InvalidMaterializationTarget(source) => source.fmt(f),
            Self::InvalidAuthMode { value } => {
                write!(f, "{AUTH_MODE_ENV} value {value:?} is not supported")
            }
            Self::InvalidAuthConfig(source) => source.fmt(f),
        }
    }
}

impl Error for RuntimeConfigError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidListenAddr { source, .. } => Some(source),
            Self::InvalidPostgresConfig(source) => Some(source),
            Self::InvalidMaterializationTarget(source) => Some(source),
            Self::InvalidAuthConfig(source) => Some(source),
            Self::MissingEnv { .. }
            | Self::InvalidStoreProvider { .. }
            | Self::InvalidAuthMode { .. } => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::{
        http01::{
            DeleteHttp01ChallengeRequest, ExpireHttp01ChallengesRequest, Http01ChallengeKey,
            Http01ChallengeRecord, PutHttp01ChallengeRequest,
        },
        instance::{
            CompareAndSwapInstanceStateRequest, CreateInstanceRequest, CreateInstanceResult,
            DeleteInstanceRequest, GetInstanceRequest, InstanceRecord,
        },
        manifest::KubernetesObject,
        materialization::{
            BackendEndpoint, CompleteWakeRequest, CompleteWakeResult,
            LoadMaterializationOperationalMetricsRequest, LoadReadyMaterializationRequest,
            MaterializationBacklogOperationalMetrics, MaterializationHeldKeysOperationalMetrics,
            MaterializationOperationalMetrics, MaterializationRecord, MaterializationState,
            RecordMaterializationRequest, RenderedObjectRef,
        },
        materializer::{KubernetesClientFuture, KubernetesClientResult, KubernetesMaterializer},
        route::{
            CreateRouteBindingRequest, DeleteRouteBindingRequest, GetRouteBindingRequest,
            RouteBindingRecord, RouteDependencyLookup, RouteDependencySet, RouteIdentity,
            RouteResolution,
        },
        store::{StoreError, StoreFuture, StoreResult},
        workload::{
            CreateWorkloadClassVersionRequest, LoadWorkloadClassVersionRequest,
            WorkloadClassVersion,
        },
        ControlPlaneStore, KubernetesMaterializerClient,
    };

    use super::*;

    #[test]
    fn env_config_requires_listen_addr() {
        let error =
            RuntimeConfig::from_key_values(valid_env_without(CONTROL_PLANE_LISTEN_ADDR_ENV))
                .expect_err("listen address is required");

        assert!(matches!(
            error,
            RuntimeConfigError::MissingEnv {
                name: CONTROL_PLANE_LISTEN_ADDR_ENV
            }
        ));
    }

    #[test]
    fn env_config_rejects_unknown_store_provider() {
        let error = RuntimeConfig::from_key_values(
            valid_env()
                .into_iter()
                .map(|(key, value)| {
                    if key == STORE_PROVIDER_ENV {
                        (key, "sqlite")
                    } else {
                        (key, value)
                    }
                })
                .collect::<Vec<_>>(),
        )
        .expect_err("unknown store provider is invalid");

        assert!(matches!(
            error,
            RuntimeConfigError::InvalidStoreProvider { value } if value == "sqlite"
        ));
    }

    #[test]
    fn env_config_rejects_invalid_native_listen_addr() {
        let error = RuntimeConfig::from_key_values(
            valid_env()
                .into_iter()
                .map(|(key, value)| {
                    if key == CONTROL_PLANE_LISTEN_ADDR_ENV {
                        (key, "localhost")
                    } else {
                        (key, value)
                    }
                })
                .collect::<Vec<_>>(),
        )
        .expect_err("invalid listen address is rejected");

        assert!(matches!(
            error,
            RuntimeConfigError::InvalidListenAddr {
                name: CONTROL_PLANE_LISTEN_ADDR_ENV,
                value,
                ..
            } if value == "localhost"
        ));
    }

    #[test]
    fn env_config_defaults_grpc_web_listener_to_absent() {
        let config = RuntimeConfig::from_key_values(valid_env())
            .expect("valid config without grpc-web listener");

        assert_eq!(config.operator_grpc_web_listen_addr, None);
    }

    #[test]
    fn env_config_accepts_optional_grpc_web_listener() {
        let mut values = valid_env();
        values.push((OPERATOR_GRPC_WEB_LISTEN_ADDR_ENV, "127.0.0.1:50052"));

        let config = RuntimeConfig::from_key_values(values).expect("valid config");

        assert_eq!(
            config.operator_grpc_web_listen_addr,
            Some("127.0.0.1:50052".parse().expect("socket address"))
        );
    }

    #[test]
    fn env_config_defaults_metrics_listener_to_absent() {
        let config = RuntimeConfig::from_key_values(valid_env())
            .expect("valid config without metrics listener");

        assert_eq!(config.metrics_listen_addr, None);
    }

    #[test]
    fn env_config_accepts_optional_metrics_listener() {
        let mut values = valid_env();
        values.push((METRICS_LISTEN_ADDR_ENV, "127.0.0.1:19090"));

        let config = RuntimeConfig::from_key_values(values).expect("valid config");

        assert_eq!(
            config.metrics_listen_addr,
            Some("127.0.0.1:19090".parse().expect("socket address"))
        );
    }

    #[test]
    fn env_config_parses_valid_config() {
        let config = RuntimeConfig::from_key_values(valid_env()).expect("valid config");

        assert_eq!(
            config.listen_addr,
            "127.0.0.1:50051".parse().expect("socket address")
        );
        assert_eq!(config.target.cluster_id(), "cluster-a");
        assert_eq!(config.target.namespace(), "apps");
        assert_eq!(
            config.control_plane.store.provider_name(),
            StoreProviderName::Postgres
        );
        assert_eq!(config.control_plane.auth, AuthConfig::NoAuth);
    }

    #[test]
    fn env_config_requires_explicit_auth_mode() {
        let error = RuntimeConfig::from_key_values(valid_env_without(AUTH_MODE_ENV))
            .expect_err("auth mode is required");

        assert!(matches!(
            error,
            RuntimeConfigError::MissingEnv {
                name: AUTH_MODE_ENV
            }
        ));
    }

    #[test]
    fn env_config_parses_static_bearer_token_auth() {
        let config = RuntimeConfig::from_key_values(static_auth_env()).expect("valid config");
        let AuthConfig::StaticBearerTokens(tokens) = config.control_plane.auth else {
            panic!("expected static bearer token config");
        };

        assert_eq!(
            tokens
                .token_for(crate::auth::CallerRole::Operator)
                .authorization_header_value()
                .expect("header value")
                .to_str()
                .expect("ascii"),
            "Bearer operator-secret"
        );
        assert_eq!(
            tokens
                .token_for(crate::auth::CallerRole::Proxy)
                .authorization_header_value()
                .expect("header value")
                .to_str()
                .expect("ascii"),
            "Bearer proxy-secret"
        );
        assert_eq!(
            tokens
                .token_for(crate::auth::CallerRole::Sidecar)
                .authorization_header_value()
                .expect("header value")
                .to_str()
                .expect("ascii"),
            "Bearer sidecar-secret"
        );
    }

    #[test]
    fn serve_materializer_gets_sidecar_token_from_runtime_auth_config() {
        let config = RuntimeConfig::from_key_values(static_auth_env()).expect("valid config");
        let materializer = materializer_with_runtime_auth(
            KubernetesMaterializer::new(NoopKubernetesClient),
            &config.control_plane.auth,
        );

        let token = materializer
            .sidecar_control_plane_token()
            .expect("static auth sidecar token is attached to materializer");

        assert_eq!(
            token
                .authorization_header_value()
                .expect("header")
                .to_str()
                .expect("ascii"),
            "Bearer sidecar-secret"
        );
    }

    #[test]
    fn env_config_static_auth_fails_closed_when_credentials_are_missing() {
        let error = RuntimeConfig::from_key_values(
            static_auth_env()
                .into_iter()
                .filter(|(key, _)| *key != AUTH_PROXY_TOKEN_ENV)
                .collect::<Vec<_>>(),
        )
        .expect_err("missing proxy token is rejected");

        assert!(matches!(
            error,
            RuntimeConfigError::MissingEnv {
                name: AUTH_PROXY_TOKEN_ENV
            }
        ));
    }

    #[test]
    fn env_config_static_auth_rejects_malformed_env_tokens() {
        let error = RuntimeConfig::from_key_values(
            static_auth_env()
                .into_iter()
                .map(|(key, value)| {
                    if key == AUTH_OPERATOR_TOKEN_ENV {
                        (key, "has space")
                    } else {
                        (key, value)
                    }
                })
                .collect::<Vec<_>>(),
        )
        .expect_err("malformed token is rejected");

        assert!(matches!(
            error,
            RuntimeConfigError::InvalidAuthConfig(InvalidStaticBearerTokens::InvalidToken {
                role: crate::auth::CallerRole::Operator,
                ..
            })
        ));
    }

    #[test]
    fn env_config_static_auth_requires_distinct_role_tokens() {
        let error = RuntimeConfig::from_key_values(
            static_auth_env()
                .into_iter()
                .map(|(key, value)| {
                    if key == AUTH_SIDECAR_TOKEN_ENV {
                        (key, "proxy-secret")
                    } else {
                        (key, value)
                    }
                })
                .collect::<Vec<_>>(),
        )
        .expect_err("duplicate role tokens are rejected");

        assert!(matches!(
            error,
            RuntimeConfigError::InvalidAuthConfig(InvalidStaticBearerTokens::DuplicateToken {
                first: crate::auth::CallerRole::Proxy,
                second: crate::auth::CallerRole::Sidecar
            })
        ));
    }

    #[test]
    fn constructs_native_and_operator_grpc_web_routers() {
        let store: Arc<dyn ControlPlaneStore> = Arc::new(NoopStore);
        let materializer = KubernetesMaterializer::new(NoopKubernetesClient);
        let target = MaterializationTarget::new("cluster-a", "apps").expect("target");

        let _native =
            native_control_plane_router(Arc::clone(&store), materializer.clone(), target.clone());
        let _operator_grpc_web = operator_grpc_web_router(store, materializer, target);
    }

    #[tokio::test]
    async fn materialization_backlog_metrics_exclude_ready_and_failed_states() {
        let sink = PrometheusMetricsSink::new();
        let store: Arc<dyn ControlPlaneStore> = Arc::new(OperationalMetricsStore);

        record_materialization_operational_metrics(store, sink.clone()).await;

        let rendered = sink.render();
        assert!(rendered.contains("sleepypods_materializations_nonterminal{state=\"pending\"} 2\n"));
        assert!(rendered.contains(
            "sleepypods_materialization_oldest_nonterminal_age_seconds{state=\"deleting\"}"
        ));
        assert!(!rendered.contains("sleepypods_materializations_nonterminal{state=\"ready\"}"));
        assert!(!rendered.contains(
            "sleepypods_materialization_oldest_nonterminal_age_seconds{state=\"ready\"}"
        ));
        assert!(rendered.contains("sleepypods_exclusivity_keys_held{state=\"ready\"} 3\n"));
        assert!(rendered.contains("sleepypods_exclusivity_keys_held{state=\"failed\"} 1\n"));
    }

    fn valid_env() -> Vec<(&'static str, &'static str)> {
        vec![
            (CONTROL_PLANE_LISTEN_ADDR_ENV, "127.0.0.1:50051"),
            (STORE_PROVIDER_ENV, "postgres"),
            (
                POSTGRES_URL_ENV,
                "postgres://sleepypods@example.com/sleepypods",
            ),
            (CLUSTER_ID_ENV, "cluster-a"),
            (NAMESPACE_ENV, "apps"),
            (AUTH_MODE_ENV, "no-auth"),
        ]
    }

    fn static_auth_env() -> Vec<(&'static str, &'static str)> {
        valid_env()
            .into_iter()
            .map(|(key, value)| {
                if key == AUTH_MODE_ENV {
                    (key, "static-bearer-token")
                } else {
                    (key, value)
                }
            })
            .chain([
                (AUTH_OPERATOR_TOKEN_ENV, "operator-secret"),
                (AUTH_PROXY_TOKEN_ENV, "proxy-secret"),
                (AUTH_SIDECAR_TOKEN_ENV, "sidecar-secret"),
            ])
            .collect()
    }

    fn valid_env_without(name: &str) -> Vec<(&'static str, &'static str)> {
        valid_env()
            .into_iter()
            .filter(|(key, _)| *key != name)
            .collect()
    }

    #[derive(Clone, Debug)]
    struct NoopKubernetesClient;

    impl KubernetesMaterializerClient for NoopKubernetesClient {
        fn apply_object<'a>(
            &'a self,
            _object: &'a KubernetesObject,
        ) -> KubernetesClientFuture<'a, KubernetesClientResult<()>> {
            Box::pin(async { Ok(()) })
        }

        fn delete_object<'a>(
            &'a self,
            _object: &'a RenderedObjectRef,
        ) -> KubernetesClientFuture<'a, KubernetesClientResult<()>> {
            Box::pin(async { Ok(()) })
        }

        fn wait_for_pvc_bound<'a>(
            &'a self,
            _namespace: &'a str,
            _name: &'a str,
        ) -> KubernetesClientFuture<'a, KubernetesClientResult<()>> {
            Box::pin(async { Ok(()) })
        }

        fn wait_for_readiness<'a>(
            &'a self,
            _objects: &'a [RenderedObjectRef],
        ) -> KubernetesClientFuture<'a, KubernetesClientResult<BackendEndpoint>> {
            Box::pin(async {
                BackendEndpoint::new("http://example")
                    .map_err(|error| crate::KubernetesClientError::new(error.to_string()))
            })
        }
    }

    #[derive(Debug)]
    struct NoopStore;

    #[derive(Debug)]
    struct OperationalMetricsStore;

    impl ControlPlaneStore for OperationalMetricsStore {
        fn load_materialization_operational_metrics<'a>(
            &'a self,
            _request: LoadMaterializationOperationalMetricsRequest,
        ) -> StoreFuture<'a, StoreResult<MaterializationOperationalMetrics>> {
            Box::pin(async {
                Ok(MaterializationOperationalMetrics::new(
                    vec![
                        MaterializationBacklogOperationalMetrics::new(
                            MaterializationState::Pending,
                            2,
                            Some(std::time::Duration::from_secs(11)),
                        ),
                        MaterializationBacklogOperationalMetrics::new(
                            MaterializationState::Deleting,
                            1,
                            Some(std::time::Duration::from_secs(7)),
                        ),
                    ],
                    vec![
                        MaterializationHeldKeysOperationalMetrics::new(
                            MaterializationState::Ready,
                            3,
                        ),
                        MaterializationHeldKeysOperationalMetrics::new(
                            MaterializationState::Failed,
                            1,
                        ),
                    ],
                ))
            })
        }
    }

    impl ControlPlaneStore for NoopStore {
        fn create_instance<'a>(
            &'a self,
            _request: CreateInstanceRequest,
        ) -> StoreFuture<'a, StoreResult<CreateInstanceResult>> {
            not_implemented()
        }

        fn get_instance<'a>(
            &'a self,
            _request: GetInstanceRequest,
        ) -> StoreFuture<'a, StoreResult<Option<InstanceRecord>>> {
            not_implemented()
        }

        fn delete_instance<'a>(
            &'a self,
            _request: DeleteInstanceRequest,
        ) -> StoreFuture<'a, StoreResult<bool>> {
            not_implemented()
        }

        fn create_workload_class_version<'a>(
            &'a self,
            _request: CreateWorkloadClassVersionRequest,
        ) -> StoreFuture<'a, StoreResult<WorkloadClassVersion>> {
            not_implemented()
        }

        fn load_workload_class_version<'a>(
            &'a self,
            _request: LoadWorkloadClassVersionRequest,
        ) -> StoreFuture<'a, StoreResult<Option<WorkloadClassVersion>>> {
            not_implemented()
        }

        fn create_route_binding<'a>(
            &'a self,
            _request: CreateRouteBindingRequest,
        ) -> StoreFuture<'a, StoreResult<RouteBindingRecord>> {
            not_implemented()
        }

        fn get_route_binding<'a>(
            &'a self,
            _request: GetRouteBindingRequest,
        ) -> StoreFuture<'a, StoreResult<Option<RouteBindingRecord>>> {
            not_implemented()
        }

        fn delete_route_binding<'a>(
            &'a self,
            _request: DeleteRouteBindingRequest,
        ) -> StoreFuture<'a, StoreResult<bool>> {
            not_implemented()
        }

        fn resolve_route<'a>(
            &'a self,
            _identity: RouteIdentity,
        ) -> StoreFuture<'a, StoreResult<RouteResolution>> {
            not_implemented()
        }

        fn compare_and_swap_instance_state<'a>(
            &'a self,
            _request: CompareAndSwapInstanceStateRequest,
        ) -> StoreFuture<'a, StoreResult<InstanceRecord>> {
            not_implemented()
        }

        fn record_materialization<'a>(
            &'a self,
            _request: RecordMaterializationRequest,
        ) -> StoreFuture<'a, StoreResult<MaterializationRecord>> {
            not_implemented()
        }

        fn load_ready_materialization<'a>(
            &'a self,
            _request: LoadReadyMaterializationRequest,
        ) -> StoreFuture<'a, StoreResult<Option<MaterializationRecord>>> {
            not_implemented()
        }

        fn load_active_materialization<'a>(
            &'a self,
            _request: crate::LoadActiveMaterializationRequest,
        ) -> StoreFuture<'a, StoreResult<Option<MaterializationRecord>>> {
            not_implemented()
        }

        fn complete_wake<'a>(
            &'a self,
            _request: CompleteWakeRequest,
        ) -> StoreFuture<'a, StoreResult<CompleteWakeResult>> {
            not_implemented()
        }

        fn begin_sleep<'a>(
            &'a self,
            _request: crate::BeginSleepRequest,
        ) -> StoreFuture<'a, StoreResult<crate::BeginSleepResult>> {
            not_implemented()
        }

        fn finalize_sleep<'a>(
            &'a self,
            _request: crate::FinalizeSleepRequest,
        ) -> StoreFuture<'a, StoreResult<crate::FinalizeSleepResult>> {
            not_implemented()
        }

        fn lookup_route_dependencies<'a>(
            &'a self,
            _request: RouteDependencyLookup,
        ) -> StoreFuture<'a, StoreResult<Option<RouteDependencySet>>> {
            not_implemented()
        }

        fn put_http01_challenge<'a>(
            &'a self,
            _request: PutHttp01ChallengeRequest,
        ) -> StoreFuture<'a, StoreResult<Http01ChallengeRecord>> {
            not_implemented()
        }

        fn resolve_http01_challenge<'a>(
            &'a self,
            _key: Http01ChallengeKey,
        ) -> StoreFuture<'a, StoreResult<Option<Http01ChallengeRecord>>> {
            not_implemented()
        }

        fn delete_http01_challenge<'a>(
            &'a self,
            _request: DeleteHttp01ChallengeRequest,
        ) -> StoreFuture<'a, StoreResult<bool>> {
            not_implemented()
        }

        fn expire_http01_challenges<'a>(
            &'a self,
            _request: ExpireHttp01ChallengesRequest,
        ) -> StoreFuture<'a, StoreResult<usize>> {
            not_implemented()
        }
    }

    fn not_implemented<'a, T>() -> StoreFuture<'a, StoreResult<T>> {
        Box::pin(async { Err(StoreError::internal("not implemented")) })
    }
}
