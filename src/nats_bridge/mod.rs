//! Native Core NATS bridge.
//!
//! This module begins with the typed, credential-safe configuration boundary.
//! Runtime connection, subscription, ingestion, and egress behavior are added
//! only by later phases once they can depend on validated configuration.

pub mod config;
pub mod readiness;
pub mod runtime;
