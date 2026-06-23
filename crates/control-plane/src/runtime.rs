use std::{
    collections::HashMap,
    error::Error,
    fmt,
    net::{AddrParseError, SocketAddr},
    sync::Arc,
};

use tokio::sync::watch;
use tonic::transport::server::Router;
use tower::layer::util::{Identity, Stack};
use tower_http::cors::CorsLayer;

use crate::{
    api::{
        operator_grpc_service_with_store, operator_grpc_web_server_builder,
        proxy_grpc_service_with_store, sidecar_grpc_service_with_store,
    },
    config::{ControlPlaneConfig, PostgresStoreConfig, StoreProviderConfig, StoreProviderName},
    materialization::{InvalidMaterializationTarget, MaterializationTarget},
    materializer::{KubernetesMaterializer, KubernetesMaterializerClient},
    postgres::PostgresStore,
    store::ControlPlaneStore,
    KubeMaterializerClient,
};

pub const CONTROL_PLANE_LISTEN_ADDR_ENV: &str = "SLEEPYPODS_CONTROL_PLANE_LISTEN_ADDR";
pub const OPERATOR_GRPC_WEB_LISTEN_ADDR_ENV: &str = "SLEEPYPODS_OPERATOR_GRPC_WEB_LISTEN_ADDR";
pub const STORE_PROVIDER_ENV: &str = "SLEEPYPODS_STORE_PROVIDER";
pub const POSTGRES_URL_ENV: &str = "SLEEPYPODS_POSTGRES_URL";
pub const CLUSTER_ID_ENV: &str = "SLEEPYPODS_CLUSTER_ID";
pub const NAMESPACE_ENV: &str = "SLEEPYPODS_NAMESPACE";

pub type NativeControlPlaneRouter = Router<Identity>;
pub type OperatorGrpcWebRouter = Router<Stack<CorsLayer, Stack<tonic_web::GrpcWebLayer, Identity>>>;
pub type RuntimeResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RuntimeConfig {
    pub listen_addr: SocketAddr,
    pub operator_grpc_web_listen_addr: Option<SocketAddr>,
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
        let target = MaterializationTarget::new(
            required_value(&values, CLUSTER_ID_ENV)?,
            required_value(&values, NAMESPACE_ENV)?,
        )
        .map_err(RuntimeConfigError::InvalidMaterializationTarget)?;

        Ok(Self {
            listen_addr,
            operator_grpc_web_listen_addr,
            control_plane: ControlPlaneConfig::new(store),
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
    tonic::transport::Server::builder()
        .add_service(operator_grpc_service_with_store(
            Arc::clone(&store),
            materializer.clone(),
            target.clone(),
        ))
        .add_service(proxy_grpc_service_with_store(
            Arc::clone(&store),
            materializer.clone(),
            target.clone(),
        ))
        .add_service(sidecar_grpc_service_with_store(store, materializer, target))
}

pub fn operator_grpc_web_router<C>(
    store: Arc<dyn ControlPlaneStore>,
    materializer: KubernetesMaterializer<C>,
    target: MaterializationTarget,
) -> OperatorGrpcWebRouter
where
    C: KubernetesMaterializerClient + Clone + 'static,
{
    operator_grpc_web_server_builder().add_service(operator_grpc_service_with_store(
        store,
        materializer,
        target,
    ))
}

pub async fn run_from_env() -> RuntimeResult<()> {
    let config = RuntimeConfig::from_env()?;
    let store = connect_store(&config.control_plane.store).await?;
    let kube_client = KubeMaterializerClient::try_default().await?;
    let materializer = KubernetesMaterializer::new(kube_client);

    serve(config, store, materializer).await?;

    Ok(())
}

pub async fn serve<C>(
    config: RuntimeConfig,
    store: Arc<dyn ControlPlaneStore>,
    materializer: KubernetesMaterializer<C>,
) -> Result<(), tonic::transport::Error>
where
    C: KubernetesMaterializerClient + Clone + 'static,
{
    let native_router = native_control_plane_router(
        Arc::clone(&store),
        materializer.clone(),
        config.target.clone(),
    );
    let (shutdown_tx, _) = watch::channel(false);
    let native_shutdown = shutdown_tx.subscribe();

    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        let _ = shutdown_tx.send(true);
    });

    if let Some(operator_grpc_web_addr) = config.operator_grpc_web_listen_addr {
        let operator_grpc_web_router =
            operator_grpc_web_router(store, materializer.clone(), config.target.clone());
        let operator_grpc_web_shutdown = native_shutdown.clone();

        tokio::try_join!(
            native_router
                .serve_with_shutdown(config.listen_addr, wait_for_shutdown(native_shutdown),),
            operator_grpc_web_router.serve_with_shutdown(
                operator_grpc_web_addr,
                wait_for_shutdown(operator_grpc_web_shutdown),
            )
        )?;
    } else {
        native_router
            .serve_with_shutdown(config.listen_addr, wait_for_shutdown(native_shutdown))
            .await?;
    }

    Ok(())
}

async fn connect_store(
    config: &StoreProviderConfig,
) -> Result<Arc<dyn ControlPlaneStore>, crate::StoreError> {
    match config {
        StoreProviderConfig::Postgres(config) => {
            let store = PostgresStore::connect(config).await?;
            Ok(Arc::new(store))
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
        }
    }
}

impl Error for RuntimeConfigError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidListenAddr { source, .. } => Some(source),
            Self::InvalidPostgresConfig(source) => Some(source),
            Self::InvalidMaterializationTarget(source) => Some(source),
            Self::MissingEnv { .. } | Self::InvalidStoreProvider { .. } => None,
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
            LoadReadyMaterializationRequest, MaterializationRecord, RecordMaterializationRequest,
            RenderedObjectRef,
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
        ]
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
