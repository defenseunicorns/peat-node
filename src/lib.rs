pub mod attachments;
pub mod crypto;
pub mod node;
pub mod query;
pub mod service;
pub mod watcher;

/// Generated protobuf/Connect RPC types.
pub mod proto {
    include!(concat!(env!("OUT_DIR"), "/_connectrpc.rs"));
}

// Re-export the leaf module for convenience (proto::GetStatusResponse etc.)
pub use proto::peat::sidecar::v1 as pb;
