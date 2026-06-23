//! Protobuf-defined control-plane API and transport scaffolding.

mod proxy;
pub mod server;
mod sidecar;
mod template;

pub mod pb {
    tonic::include_proto!("sleepypods.controlplane.v1");
}

pub use proxy::{
    proxy_grpc_service_with_store, StoreBackedProxyApi, StoreBackedProxyGrpcService,
    PROXY_SERVICE_NAME,
};
pub use server::{
    operator_grpc_server_builder, operator_grpc_service, operator_grpc_service_with_store,
    operator_grpc_web_cors_layer, operator_grpc_web_server_builder, OperatorApiPlaceholder,
    StoreBackedOperatorApi, StoreBackedOperatorGrpcService, OPERATOR_SERVICE_NAME,
    OPERATOR_UNARY_METHODS,
};
pub use sidecar::{
    sidecar_grpc_service_with_store, StoreBackedSidecarApi, StoreBackedSidecarGrpcService,
    SIDECAR_SERVICE_NAME,
};
