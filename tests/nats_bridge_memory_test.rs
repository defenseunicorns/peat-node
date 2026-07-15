//! Scoped allocator regression for one active maximum-size bridge item.
//!
//! The measurement is a current-thread, no-yield window. It is deliberately
//! narrower than RSS or whole-process memory: mmap, native-library and kernel
//! buffers, async-nats transport retention, and allocations on other OS
//! threads are outside this test's accounting domain.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

use base64::Engine;
use futures::FutureExt;
use peat_node::nats_bridge::envelope::BridgeEnvelope;
use peat_node::nats_bridge::ingress::MAX_INGRESS_PAYLOAD_BYTES;
use peat_node::node::{SidecarConfig, SidecarNode};

// Calibrated on 2026-07-15 with the ignored calibration test below. The
// observed maximum was 32,863,033 bytes (encrypted high-escape fixture); the
// fixed 40 MiB threshold adds 9,080,007 bytes / 27.6% conservative headroom
// for allocator/platform variation. Ordinary assertions never derive it.
const MAX_SCOPED_ALLOCATOR_DELTA_BYTES: i128 = 41_943_040;

thread_local! {
    static ACCOUNTING_ENABLED: Cell<bool> = const { Cell::new(false) };
    static CURRENT_DELTA: Cell<i128> = const { Cell::new(0) };
    static HIGH_WATER_DELTA: Cell<i128> = const { Cell::new(0) };
    static ACCOUNTING_OVERFLOWED: Cell<bool> = const { Cell::new(false) };
}

struct ScopedAllocator;

fn adjust_current_delta(delta: i128) {
    ACCOUNTING_ENABLED.with(|enabled| {
        if !enabled.get() {
            return;
        }
        CURRENT_DELTA.with(|current| {
            let old = current.get();
            let next = match old.checked_add(delta) {
                Some(next) => next,
                None => {
                    ACCOUNTING_OVERFLOWED.with(|overflowed| overflowed.set(true));
                    old.saturating_add(delta)
                }
            };
            current.set(next);
            HIGH_WATER_DELTA.with(|high_water| high_water.set(high_water.get().max(next)));
        });
    });
}

unsafe impl GlobalAlloc for ScopedAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            adjust_current_delta(layout.size() as i128);
        }
        pointer
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc_zeroed(layout) };
        if !pointer.is_null() {
            adjust_current_delta(layout.size() as i128);
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        unsafe { System.dealloc(pointer, layout) };
        adjust_current_delta(-(layout.size() as i128));
    }

    unsafe fn realloc(&self, pointer: *mut u8, old_layout: Layout, new_size: usize) -> *mut u8 {
        let replacement = unsafe { System.realloc(pointer, old_layout, new_size) };
        if !replacement.is_null() {
            adjust_current_delta(new_size as i128 - old_layout.size() as i128);
        }
        replacement
    }
}

#[global_allocator]
static GLOBAL_ALLOCATOR: ScopedAllocator = ScopedAllocator;

#[derive(Clone, Copy, Debug)]
enum FixtureKind {
    HighEscapedString,
    HighNodeCount,
    MaximumNesting,
}

impl FixtureKind {
    fn name(self) -> &'static str {
        match self {
            Self::HighEscapedString => "high_escaped_string",
            Self::HighNodeCount => "high_node_count",
            Self::MaximumNesting => "maximum_nesting",
        }
    }

    fn build(self) -> String {
        match self {
            Self::HighEscapedString => {
                let escaped_quote_count = (MAX_INGRESS_PAYLOAD_BYTES - 2) / 2;
                format!("\"{}\"", r#"\""#.repeat(escaped_quote_count))
            }
            Self::HighNodeCount => {
                let value_count = (MAX_INGRESS_PAYLOAD_BYTES - 1) / 2;
                let mut payload = String::with_capacity(MAX_INGRESS_PAYLOAD_BYTES - 1);
                payload.push('[');
                for index in 0..value_count {
                    if index > 0 {
                        payload.push(',');
                    }
                    payload.push('0');
                }
                payload.push(']');
                payload
            }
            Self::MaximumNesting => {
                const DEPTH: usize = 127;
                let content_len = MAX_INGRESS_PAYLOAD_BYTES - (DEPTH * 2) - 2;
                format!(
                    "{}\"{}\"{}",
                    "[".repeat(DEPTH),
                    "a".repeat(content_len),
                    "]".repeat(DEPTH)
                )
            }
        }
    }
}

#[derive(Debug)]
struct CaseMeasurement {
    fixture: &'static str,
    encrypted: bool,
    scoped_rust_global_allocator_live_byte_delta: i128,
}

fn begin_accounting() {
    ACCOUNTING_ENABLED.with(|enabled| enabled.set(false));
    CURRENT_DELTA.with(|current| current.set(0));
    HIGH_WATER_DELTA.with(|high_water| high_water.set(0));
    ACCOUNTING_OVERFLOWED.with(|overflowed| overflowed.set(false));
    ACCOUNTING_ENABLED.with(|enabled| enabled.set(true));
}

fn finish_accounting() -> i128 {
    ACCOUNTING_ENABLED.with(|enabled| enabled.set(false));
    let overflowed = ACCOUNTING_OVERFLOWED.with(Cell::get);
    assert!(!overflowed, "scoped allocator bookkeeping overflowed");
    HIGH_WATER_DELTA.with(Cell::get)
}

fn encryption_key() -> String {
    base64::engine::general_purpose::STANDARD.encode([0x42_u8; 32])
}

async fn measure_case(kind: FixtureKind, encrypted: bool) -> CaseMeasurement {
    let payload = kind.build();
    assert!(payload.len() <= MAX_INGRESS_PAYLOAD_BYTES);
    let temp = tempfile::tempdir().expect("temporary allocator test directory");
    let node = SidecarNode::new(SidecarConfig {
        node_id: format!("memory-{}-{encrypted}", kind.name()),
        app_id: "nats-bridge-memory-test".to_owned(),
        data_dir: temp.path().to_path_buf(),
        disable_mdns: true,
        encryption_key: encrypted.then(encryption_key),
        ..Default::default()
    })
    .await
    .expect("create allocator test node");

    // Warm this exact fixture/configuration with accounting disabled. Setup and
    // warm-up may schedule Tokio background work on this current OS thread.
    let warm_envelope =
        BridgeEnvelope::from_payload("vision.summary", "source", payload.as_bytes())
            .expect("warm fixture should validate");
    let warm_json = serde_json::to_string(&warm_envelope).expect("warm envelope serialization");
    node.create_bridge_document("warm", "warm", &warm_json)
        .await
        .expect("warm bridge write");

    // No await, spawn, blocking-pool call, or scheduler yield is permitted from
    // begin_accounting through finish_accounting. TLS selects the current OS
    // thread; the no-yield window prevents same-thread background task work.
    begin_accounting();
    let envelope = BridgeEnvelope::from_payload("vision.summary", "source", payload.as_bytes())
        .expect("measured fixture should validate");
    let envelope_json = serde_json::to_string(&envelope).expect("measured envelope serialization");
    let immediate = node
        .create_bridge_document("measured", "measured", &envelope_json)
        .now_or_never();
    assert_eq!(
        immediate,
        Some(Ok(())),
        "create_bridge_document yielded or failed inside the no-yield window"
    );
    let scoped_rust_global_allocator_live_byte_delta = finish_accounting();

    node.shutdown().await.expect("shutdown allocator test node");
    CaseMeasurement {
        fixture: kind.name(),
        encrypted,
        scoped_rust_global_allocator_live_byte_delta,
    }
}

async fn measure_all_cases() -> Vec<CaseMeasurement> {
    let mut measurements = Vec::new();
    for kind in [
        FixtureKind::HighEscapedString,
        FixtureKind::HighNodeCount,
        FixtureKind::MaximumNesting,
    ] {
        for encrypted in [false, true] {
            measurements.push(measure_case(kind, encrypted).await);
        }
    }
    measurements
}

#[tokio::test(flavor = "current_thread")]
async fn maximum_ingress_fixtures_stay_within_fixed_scoped_allocator_delta() {
    for measurement in measure_all_cases().await {
        assert!(
            measurement.scoped_rust_global_allocator_live_byte_delta
                <= MAX_SCOPED_ALLOCATOR_DELTA_BYTES,
            "case {measurement:?} exceeded fixed scoped Rust-global-allocator live-byte delta threshold {MAX_SCOPED_ALLOCATOR_DELTA_BYTES}"
        );
    }
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "calibration mode: prints independently warmed case deltas"]
async fn calibrate_scoped_allocator_delta() {
    let measurements = measure_all_cases().await;
    for measurement in &measurements {
        println!(
            "fixture={} encrypted={} scoped Rust-global-allocator live-byte delta={}",
            measurement.fixture,
            measurement.encrypted,
            measurement.scoped_rust_global_allocator_live_byte_delta
        );
    }
    let maximum = measurements
        .iter()
        .map(|measurement| measurement.scoped_rust_global_allocator_live_byte_delta)
        .max()
        .expect("calibration has cases");
    println!("maximum scoped Rust-global-allocator live-byte delta={maximum}");
}
