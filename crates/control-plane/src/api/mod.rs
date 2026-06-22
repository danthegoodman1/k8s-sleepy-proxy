//! Protobuf-defined control-plane API and transport scaffolding.

pub mod server;
mod template;

pub mod pb {
    tonic::include_proto!("sleepypods.controlplane.v1");
}

pub use server::{
    operator_grpc_server_builder, operator_grpc_service, operator_grpc_service_with_store,
    operator_grpc_web_server_builder, OperatorApiPlaceholder, StoreBackedOperatorApi,
    StoreBackedOperatorGrpcService, OPERATOR_SERVICE_NAME, OPERATOR_UNARY_METHODS,
};
