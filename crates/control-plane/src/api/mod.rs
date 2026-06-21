//! Protobuf-defined control-plane API and transport scaffolding.

pub mod server;

pub mod pb {
    tonic::include_proto!("sleepypods.controlplane.v1");
}

pub use server::{
    operator_grpc_server_builder, operator_grpc_service, operator_grpc_web_server_builder,
    OperatorApiPlaceholder, OPERATOR_SERVICE_NAME, OPERATOR_UNARY_METHODS,
};
