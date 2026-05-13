pub mod cli_validation;
pub mod crypto;
pub mod deployer;
pub mod node;
pub mod service;
pub mod types;
pub mod watcher;

/// Generated protobuf/Connect RPC types.
pub mod proto {
    include!(concat!(env!("OUT_DIR"), "/_connectrpc.rs"));
}

// Re-export the leaf module for convenience (proto::GetStatusResponse etc.)
pub use proto::peat::sidecar::v1 as pb;
