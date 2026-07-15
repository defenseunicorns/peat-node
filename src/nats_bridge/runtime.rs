//! Non-blocking Core NATS connection lifecycle and readiness events.
//!
//! async-nats 0.49.1 treats `max_reconnects(0)` as unlimited. This runtime
//! therefore uses the client's reconnect callback as the single retry owner
//! instead of layering a competing outer dial loop over it.

use std::future::pending;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_nats::{
    Client, ConnectOptions, Event, HeaderValue, Request, RequestErrorKind, Subscriber,
};
use futures::stream::{FuturesUnordered, StreamExt};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tracing::{debug, info, warn};

use super::config::{validate_bridge_node_identity, EnabledBridgeConfig};
use super::egress::{
    run_bridge_event_router, run_egress_worker, DeliveryCoordinator, EgressDiagnostics,
    EgressStats, NatsBridgePublisher, BRIDGE_ORIGIN_HEADER,
};
use super::ingress::{
    ingress_channel, is_payload_oversized, run_ingress_processor, IngressDiagnostics, IngressItem,
    IngressSender, IngressStats,
};
use super::readiness::{BridgeReadiness, BridgeStatus};
use crate::node::SidecarNode;

const RETRY_MIN: Duration = Duration::from_secs(1);
const RETRY_MAX: Duration = Duration::from_secs(30);
const OUTAGE_WARNING_INTERVAL: Duration = Duration::from_secs(5 * 60);
const SLOW_CONSUMER_WARNING_INTERVAL: Duration = Duration::from_secs(60);
const SUPERVISOR_SIGNAL_CAPACITY: usize = 64;
const READINESS_BARRIER_SUBJECT: &str = "_PEAT.NATS_BRIDGE.READINESS";
const READINESS_BARRIER_TIMEOUT: Duration = Duration::from_secs(2);

/// Last lifecycle event delivered by async-nats to the bridge callback.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeliveredLifecycleEvent {
    Initial,
    Connected,
    Disconnected,
    Error,
}

/// Payload-safe, monotonic callback-delivery diagnostic.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LifecycleSnapshot {
    pub sequence: u64,
    pub connection_epoch: u64,
    pub invalidation_epoch: u64,
    pub connected: bool,
    pub last_event: DeliveredLifecycleEvent,
    connected_sequence: u64,
    invalidation_sequence: u64,
}

impl Default for LifecycleSnapshot {
    fn default() -> Self {
        Self {
            sequence: 0,
            connection_epoch: 0,
            invalidation_epoch: 0,
            connected: false,
            last_event: DeliveredLifecycleEvent::Initial,
            connected_sequence: 0,
            invalidation_sequence: 0,
        }
    }
}

#[derive(Clone)]
struct LifecycleControl {
    tx: watch::Sender<LifecycleSnapshot>,
    readiness: BridgeReadiness,
    // Callback invalidation and barrier validation/commit must be one transition.
    transition_lock: Arc<Mutex<()>>,
}

impl LifecycleControl {
    fn new(readiness: BridgeReadiness) -> Self {
        Self {
            tx: watch::channel(LifecycleSnapshot::default()).0,
            readiness,
            transition_lock: Arc::new(Mutex::new(())),
        }
    }

    fn subscribe(&self) -> watch::Receiver<LifecycleSnapshot> {
        self.tx.subscribe()
    }

    fn snapshot(&self) -> LifecycleSnapshot {
        *self.tx.borrow()
    }

    fn delivered(&self, event: DeliveredLifecycleEvent) {
        let _transition = self
            .transition_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match event {
            DeliveredLifecycleEvent::Connected => {
                self.readiness.set_connected();
            }
            DeliveredLifecycleEvent::Disconnected => {
                self.readiness.mark_disconnected();
            }
            DeliveredLifecycleEvent::Error => {
                self.readiness.invalidate_subscription_generation();
            }
            DeliveredLifecycleEvent::Initial => return,
        }
        self.tx.send_modify(|state| {
            state.sequence = state.sequence.saturating_add(1);
            state.last_event = event;
            match event {
                DeliveredLifecycleEvent::Connected => {
                    state.connection_epoch = state.connection_epoch.saturating_add(1);
                    state.connected = true;
                    state.connected_sequence = state.sequence;
                }
                DeliveredLifecycleEvent::Disconnected => {
                    state.invalidation_epoch = state.invalidation_epoch.saturating_add(1);
                    state.connected = false;
                    state.invalidation_sequence = state.sequence;
                }
                DeliveredLifecycleEvent::Error => {
                    state.invalidation_epoch = state.invalidation_epoch.saturating_add(1);
                    state.invalidation_sequence = state.sequence;
                }
                DeliveredLifecycleEvent::Initial => {}
            }
        });
    }

    fn commit_barrier(&self, tags: BarrierTags, generation_id: Option<u64>) -> bool {
        let _transition = self
            .transition_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if !barrier_tags_match(tags, self.snapshot(), generation_id) {
            return false;
        }
        self.readiness
            .mark_all_subscriptions_established()
            .became_ready()
    }
}

/// Stable reason values for credential-safe lifecycle events.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LifecycleReason {
    Starting,
    BrokerUnavailable,
    SubscriptionsPending,
    Disconnected,
    Ready,
    RetryScheduled,
}

impl LifecycleReason {
    fn as_str(self) -> &'static str {
        match self {
            Self::Starting => "starting",
            Self::BrokerUnavailable => "broker_unavailable",
            Self::SubscriptionsPending => "subscriptions_pending",
            Self::Disconnected => "disconnected",
            Self::Ready => "ready",
            Self::RetryScheduled => "retry_scheduled",
        }
    }
}

/// Typed data behind every production bridge lifecycle log.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LifecycleAction {
    pub bridge_ready: bool,
    pub reason: LifecycleReason,
    pub nats_host: String,
    pub nats_port: u16,
    pub connected: bool,
    pub established_subscriptions: usize,
    pub expected_subscriptions: usize,
    pub retry_attempt: Option<usize>,
    pub retry_delay_ms: Option<u64>,
    pub error_kind: Option<&'static str>,
}

impl LifecycleAction {
    fn status(reason: LifecycleReason, host: &str, port: u16, status: &BridgeStatus) -> Self {
        Self {
            bridge_ready: status.is_ready(),
            reason,
            nats_host: host.to_owned(),
            nats_port: port,
            connected: status.connected,
            established_subscriptions: status.established_subjects.len(),
            expected_subscriptions: status.expected_subscriptions,
            retry_attempt: None,
            retry_delay_ms: None,
            error_kind: None,
        }
    }

    fn unavailable(host: &str, port: u16, status: &BridgeStatus) -> Self {
        let mut action = Self::status(LifecycleReason::BrokerUnavailable, host, port, status);
        action.error_kind = Some("connection_unavailable");
        action
    }

    fn retry(host: &str, port: u16, attempt: usize, delay: Duration) -> Self {
        Self {
            bridge_ready: false,
            reason: LifecycleReason::RetryScheduled,
            nats_host: host.to_owned(),
            nats_port: port,
            connected: false,
            established_subscriptions: 0,
            expected_subscriptions: 0,
            retry_attempt: Some(attempt),
            retry_delay_ms: Some(delay.as_millis().min(u128::from(u64::MAX)) as u64),
            error_kind: None,
        }
    }

    fn emit(&self) {
        let reason = self.reason.as_str();
        match self.reason {
            LifecycleReason::Starting | LifecycleReason::SubscriptionsPending => info!(
                bridge_ready = self.bridge_ready,
                nats_host = %self.nats_host,
                nats_port = self.nats_port,
                connected = self.connected,
                established_subscriptions = self.established_subscriptions,
                expected_subscriptions = self.expected_subscriptions,
                reason,
                "NATS bridge readiness"
            ),
            LifecycleReason::BrokerUnavailable | LifecycleReason::Disconnected => warn!(
                bridge_ready = self.bridge_ready,
                nats_host = %self.nats_host,
                nats_port = self.nats_port,
                connected = self.connected,
                established_subscriptions = self.established_subscriptions,
                expected_subscriptions = self.expected_subscriptions,
                reason,
                error_kind = self.error_kind.unwrap_or("connection_lost"),
                "NATS bridge unavailable"
            ),
            LifecycleReason::Ready => info!(
                bridge_ready = self.bridge_ready,
                nats_host = %self.nats_host,
                nats_port = self.nats_port,
                connected = self.connected,
                established_subscriptions = self.established_subscriptions,
                expected_subscriptions = self.expected_subscriptions,
                reason,
                "NATS bridge ready"
            ),
            LifecycleReason::RetryScheduled => debug!(
                bridge_ready = self.bridge_ready,
                nats_host = %self.nats_host,
                nats_port = self.nats_port,
                retry_attempt = self.retry_attempt.unwrap_or_default(),
                retry_delay_ms = self.retry_delay_ms.unwrap_or_default(),
                reason,
                "NATS bridge retry scheduled"
            ),
        }
    }
}

/// Process-lifetime handle to the single bridge supervisor task.
pub struct BridgeRuntimeHandle {
    readiness: BridgeReadiness,
    lifecycle: LifecycleControl,
    task: JoinHandle<()>,
    support_tasks: Vec<JoinHandle<()>>,
    stats: IngressStats,
    _egress_stats: EgressStats,
}

/// Public, label-free subset of remote publication outcomes.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct BridgeEgressSnapshot {
    pub published: u64,
    pub unavailable: u64,
    pub publish_failed: u64,
    pub max_payload_exceeded: u64,
}

impl BridgeRuntimeHandle {
    /// Watch internal readiness without changing the public sidecar status RPC.
    pub fn readiness(&self) -> &BridgeReadiness {
        &self.readiness
    }

    /// Label-free ingress counters for operational and integration evidence.
    pub fn stats(&self) -> &IngressStats {
        &self.stats
    }

    /// Observe only lifecycle events that reached the bridge callback.
    pub fn lifecycle_snapshot(&self) -> LifecycleSnapshot {
        self.lifecycle.snapshot()
    }

    /// Observe terminal remote publication outcomes without dynamic labels.
    pub fn egress_snapshot(&self) -> BridgeEgressSnapshot {
        let snapshot = self._egress_stats.snapshot();
        BridgeEgressSnapshot {
            published: snapshot.published,
            unavailable: snapshot.unavailable,
            publish_failed: snapshot.publish_failed,
            max_payload_exceeded: snapshot.max_payload_exceeded,
        }
    }

    /// Whether the supervisor has unexpectedly terminated.
    pub fn is_finished(&self) -> bool {
        self.task.is_finished() || self.support_tasks.iter().any(JoinHandle::is_finished)
    }
}

/// Enabled-only runtime constructor.
pub struct BridgeRuntime;

impl BridgeRuntime {
    /// Phase 1 compatibility constructor used until process wiring supplies a node.
    ///
    /// This keeps the connection lifecycle non-blocking; [`Self::spawn`]
    /// owns the Phase 2 subscription and ingress generation.
    pub fn spawn_connection_only(config: EnabledBridgeConfig) -> BridgeRuntimeHandle {
        let stats = IngressStats::default();
        Self::spawn_supervisor(config, stats, None, None, Vec::new())
    }

    /// Spawn the complete subscription-aware ingress runtime.
    pub fn spawn(
        config: EnabledBridgeConfig,
        source_node_id: String,
        node: Arc<SidecarNode>,
    ) -> BridgeRuntimeHandle {
        Self::try_spawn(config, source_node_id, node)
            .expect("bridge runtime requires startup-validated node identity")
    }

    /// Fallible startup boundary used by the process before any NATS task starts.
    pub fn try_spawn(
        config: EnabledBridgeConfig,
        source_node_id: String,
        node: Arc<SidecarNode>,
    ) -> anyhow::Result<BridgeRuntimeHandle> {
        let local_node_id = node.node_id().to_owned();
        let origin_header_value = validate_bridge_node_identity(&local_node_id)
            .map_err(|_| anyhow::anyhow!("invalid effective NATS bridge node identity"))?;
        if source_node_id != local_node_id {
            warn!(
                error_kind = "node_identity_mismatch",
                "NATS bridge ignored divergent caller node identity"
            );
        }
        let ledger_health = node
            .install_bridge_ledger(
                config
                    .mappings()
                    .iter()
                    .map(|mapping| mapping.collection().to_owned()),
            )
            .map_err(|_| anyhow::anyhow!("NATS bridge local-exclusion ledger unavailable"))?;
        let stats = IngressStats::default();
        let diagnostics = IngressDiagnostics::new(
            config
                .mappings()
                .iter()
                .map(|mapping| (mapping.subject().clone(), mapping.collection().to_owned())),
        );
        let (ingress_tx, ingress_rx) = ingress_channel();
        let egress_stats = EgressStats::default();
        let (_exclusion, delivery) = node
            .bridge_ledgers()
            .expect("successful installation retains journal facades");
        let configured_subjects = config
            .mappings()
            .iter()
            .map(|mapping| mapping.subject().clone())
            .collect::<Vec<_>>();
        let ingress_task = tokio::spawn(run_ingress_processor(
            ingress_rx,
            local_node_id.clone(),
            Arc::clone(&node),
            stats.clone(),
            configured_subjects,
        ));
        let handle = Self::spawn_supervisor(
            config,
            stats,
            Some((ingress_tx, diagnostics, local_node_id.clone())),
            delivery.map(|delivery| EgressSetup {
                delivery,
                origin_header_value,
                stats: egress_stats,
                events: node.subscribe_bridge_changes(),
                local_node_id: local_node_id.clone(),
            }),
            vec![ingress_task],
        );
        handle
            .readiness
            .set_exclusion_healthy(ledger_health.exclusion_healthy);
        handle
            .readiness
            .set_delivery_healthy(ledger_health.delivery_healthy);
        Ok(handle)
    }

    fn spawn_supervisor(
        config: EnabledBridgeConfig,
        stats: IngressStats,
        ingress: Option<(IngressSender, IngressDiagnostics, String)>,
        egress: Option<EgressSetup>,
        mut support_tasks: Vec<JoinHandle<()>>,
    ) -> BridgeRuntimeHandle {
        let server_addr = config.server_addr().clone();
        let nats_host = server_addr.host().to_owned();
        let nats_port = server_addr.port();
        let readiness = BridgeReadiness::new(
            config
                .mappings()
                .iter()
                .map(|mapping| mapping.subject().clone()),
        );
        let lifecycle = LifecycleControl::new(readiness.clone());

        LifecycleAction::status(
            LifecycleReason::Starting,
            &nats_host,
            nats_port,
            &readiness.snapshot(),
        )
        .emit();

        let task_readiness = readiness.clone();
        let task_lifecycle = lifecycle.clone();
        let task_host = nats_host.clone();
        let task_stats = stats.clone();
        let handle_egress_stats = egress
            .as_ref()
            .map(|state| state.stats.clone())
            .unwrap_or_default();
        let egress = egress.map(|setup| {
            let (coordinator, rx) = DeliveryCoordinator::new(
                config.mappings(),
                &setup.local_node_id,
                setup.stats.clone(),
                setup.delivery.clone(),
                readiness.clone(),
            );
            let diagnostics = coordinator.diagnostics();
            support_tasks.push(tokio::spawn(run_bridge_event_router(
                setup.events,
                coordinator,
                setup.stats.clone(),
                diagnostics.clone(),
            )));
            EgressSupervisor {
                rx,
                delivery: setup.delivery,
                origin_header_value: setup.origin_header_value,
                stats: setup.stats,
                diagnostics,
            }
        });
        let task = tokio::spawn(async move {
            run_client_supervisor(
                server_addr,
                task_host,
                nats_port,
                SupervisorRuntime {
                    readiness: task_readiness,
                    lifecycle: task_lifecycle,
                    stats: task_stats,
                },
                config,
                ingress,
                egress,
            )
            .await;
        });

        BridgeRuntimeHandle {
            readiness,
            lifecycle,
            task,
            support_tasks,
            stats,
            _egress_stats: handle_egress_stats,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum ClientSignal {
    GenerationEnded { generation_id: u64 },
}

struct SubscriptionGeneration {
    id: u64,
    task: JoinHandle<()>,
}

#[derive(Clone)]
struct SubscriptionInputs {
    ingress_tx: IngressSender,
    stats: IngressStats,
    diagnostics: IngressDiagnostics,
    local_node_id: String,
    signal_tx: mpsc::Sender<ClientSignal>,
}

struct SupervisorRuntime {
    readiness: BridgeReadiness,
    lifecycle: LifecycleControl,
    stats: IngressStats,
}

struct EgressSupervisor {
    rx: mpsc::Receiver<super::egress::EgressItem>,
    delivery: super::ledger::DeliveryLedger,
    origin_header_value: HeaderValue,
    stats: EgressStats,
    diagnostics: EgressDiagnostics,
}

struct EgressSetup {
    delivery: super::ledger::DeliveryLedger,
    origin_header_value: HeaderValue,
    stats: EgressStats,
    events: tokio::sync::broadcast::Receiver<crate::node::BridgeChangeEvent>,
    local_node_id: String,
}

struct AbortTaskOnDrop(JoinHandle<()>);

impl Drop for AbortTaskOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct BarrierTags {
    connection_epoch: u64,
    invalidation_epoch: u64,
    generation_id: u64,
}

struct BarrierAttempt {
    tags: BarrierTags,
    task: JoinHandle<bool>,
}

impl BarrierAttempt {
    async fn stop(self) {
        self.task.abort();
        let _ = self.task.await;
    }
}

fn barrier_tags_match(
    tags: BarrierTags,
    lifecycle: LifecycleSnapshot,
    generation_id: Option<u64>,
) -> bool {
    lifecycle.connected
        && lifecycle.connection_epoch == tags.connection_epoch
        && lifecycle.invalidation_epoch == tags.invalidation_epoch
        && generation_id == Some(tags.generation_id)
}

fn barrier_allowed_after_latest_delivery(lifecycle: LifecycleSnapshot) -> bool {
    lifecycle.connected && lifecycle.connected_sequence > lifecycle.invalidation_sequence
}

fn disconnected_reason(lifecycle: LifecycleSnapshot) -> LifecycleReason {
    if lifecycle.connection_epoch == 0 {
        LifecycleReason::BrokerUnavailable
    } else {
        LifecycleReason::Disconnected
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum GenerationAction {
    BuildAll {
        subject_count: usize,
    },
    FlushRetained {
        generation_id: u64,
    },
    RemoveOnly {
        generation_id: u64,
    },
    RebuildAll {
        generation_id: u64,
        subject_count: usize,
    },
}

fn connected_generation_action(
    generation: Option<&SubscriptionGeneration>,
    subject_count: usize,
) -> GenerationAction {
    match generation {
        Some(generation) => GenerationAction::FlushRetained {
            generation_id: generation.id,
        },
        None => GenerationAction::BuildAll { subject_count },
    }
}

fn ended_generation_action(
    generation: Option<&SubscriptionGeneration>,
    ended_generation_id: u64,
    connected: bool,
    subject_count: usize,
) -> Option<GenerationAction> {
    let generation = generation?;
    if generation.id != ended_generation_id {
        return None;
    }
    if connected {
        Some(GenerationAction::RebuildAll {
            generation_id: generation.id,
            subject_count,
        })
    } else {
        Some(GenerationAction::RemoveOnly {
            generation_id: generation.id,
        })
    }
}

impl SubscriptionGeneration {
    async fn stop(self) {
        self.task.abort();
        let _ = self.task.await;
    }
}

async fn run_client_supervisor(
    server_addr: async_nats::ServerAddr,
    nats_host: String,
    nats_port: u16,
    runtime: SupervisorRuntime,
    config: EnabledBridgeConfig,
    ingress: Option<(IngressSender, IngressDiagnostics, String)>,
    egress: Option<EgressSupervisor>,
) {
    let SupervisorRuntime {
        readiness,
        lifecycle,
        stats,
    } = runtime;
    let (signal_tx, mut signal_rx) = mpsc::channel(SUPERVISOR_SIGNAL_CAPACITY);
    let (slow_consumer_tx, mut slow_consumer_rx) = mpsc::channel(SUPERVISOR_SIGNAL_CAPACITY);
    let mut lifecycle_rx = lifecycle.subscribe();
    let retry_host = nats_host.clone();
    let event_lifecycle = lifecycle.clone();
    let options = ConnectOptions::new()
        .no_echo()
        .retry_on_initial_connect()
        .max_reconnects(None)
        .subscription_capacity(1)
        .reconnect_delay_callback(move |attempt| {
            let jitter_percent = rand::random::<u8>() % 21;
            let delay = retry_delay(attempt, jitter_percent);
            LifecycleAction::retry(&retry_host, nats_port, attempt, delay).emit();
            delay
        })
        .event_callback(move |event| {
            let lifecycle = event_lifecycle.clone();
            let slow_consumer_tx = slow_consumer_tx.clone();
            async move {
                match event {
                    Event::Connected => {
                        lifecycle.delivered(DeliveredLifecycleEvent::Connected);
                    }
                    Event::Disconnected | Event::Closed => {
                        lifecycle.delivered(DeliveredLifecycleEvent::Disconnected);
                    }
                    Event::ClientError(_) | Event::ServerError(_) => {
                        lifecycle.delivered(DeliveredLifecycleEvent::Error);
                    }
                    Event::SlowConsumer(_) => {
                        let _ = slow_consumer_tx.try_send(());
                    }
                    _ => {}
                }
            }
        });

    // retry_on_initial_connect makes this return a client before the first
    // network attempt completes. Retaining it here keeps exactly one internal
    // connector/reconnect owner alive for the supervisor lifetime.
    let client = match options.connect(server_addr).await {
        Ok(client) => client,
        Err(_) => {
            let status = readiness.snapshot();
            LifecycleAction::unavailable(&nats_host, nats_port, &status).emit();
            return;
        }
    };

    let _egress_task = egress.map(|state| {
        let publisher = NatsBridgePublisher::new(client.clone(), readiness.clone());
        AbortTaskOnDrop(tokio::spawn(run_egress_worker(
            state.rx,
            state.origin_header_value,
            publisher,
            state.delivery,
            readiness.clone(),
            state.stats,
            state.diagnostics,
        )))
    });

    let mut outage = OutageLogState::default();
    let mut slow_consumers = SlowConsumerLogState::default();
    let mut generation = None;
    let mut next_generation_id = 1_u64;
    let mut handled_connection_epoch = 0_u64;
    let mut handled_invalidation_epoch = 0_u64;
    let mut barrier: Option<BarrierAttempt> = None;
    loop {
        let warning_deadline = outage.deadline();
        tokio::select! {
            changed = lifecycle_rx.changed() => {
                if changed.is_err() {
                    return;
                }
                let delivered = *lifecycle_rx.borrow_and_update();

                if delivered.invalidation_epoch != handled_invalidation_epoch {
                    handled_invalidation_epoch = delivered.invalidation_epoch;
                    if let Some(attempt) = barrier.take() {
                        attempt.stop().await;
                    }
                    if delivered.connected {
                        if outage.begin(Instant::now()) {
                            LifecycleAction::unavailable(
                                &nats_host,
                                nats_port,
                                &readiness.snapshot(),
                            ).emit();
                        }
                    } else {
                        outage.begin(Instant::now());
                        match disconnected_reason(delivered) {
                            LifecycleReason::BrokerUnavailable => LifecycleAction::unavailable(
                                &nats_host,
                                nats_port,
                                &readiness.snapshot(),
                            ).emit(),
                            LifecycleReason::Disconnected => LifecycleAction::status(
                                LifecycleReason::Disconnected,
                                &nats_host,
                                nats_port,
                                &readiness.snapshot(),
                            ).emit(),
                            _ => unreachable!("disconnect classification has two outcomes"),
                        }
                    }
                }

                if delivered.connection_epoch != handled_connection_epoch {
                    handled_connection_epoch = delivered.connection_epoch;
                    outage.recovered();
                    LifecycleAction::status(
                        LifecycleReason::SubscriptionsPending,
                        &nats_host,
                        nats_port,
                        &readiness.snapshot(),
                    ).emit();
                    if let Some((ingress_tx, diagnostics, local_node_id)) = ingress.as_ref() {
                        let action = connected_generation_action(
                            generation.as_ref(),
                            config.mappings().len(),
                        );
                        match action {
                            GenerationAction::BuildAll { subject_count } => {
                                debug_assert_eq!(subject_count, config.mappings().len());
                                generation = build_subscription_generation(
                                    &client,
                                    &config,
                                    SubscriptionInputs {
                                        ingress_tx: ingress_tx.clone(),
                                        stats: stats.clone(),
                                        diagnostics: diagnostics.clone(),
                                        local_node_id: local_node_id.clone(),
                                        signal_tx: signal_tx.clone(),
                                    },
                                    next_generation_id,
                                ).await;
                                next_generation_id = next_generation_id.saturating_add(1);
                            }
                            GenerationAction::FlushRetained { generation_id } => {
                                debug_assert_eq!(
                                    generation.as_ref().map(|current| current.id),
                                    Some(generation_id),
                                );
                            }
                            GenerationAction::RebuildAll { .. } => unreachable!(
                                "connected transition cannot request generation rebuild"
                            ),
                            GenerationAction::RemoveOnly { .. } => unreachable!(
                                "connected transition cannot request generation removal"
                            ),
                        }
                        if barrier_allowed_after_latest_delivery(delivered) {
                            barrier = start_barrier(&client, generation.as_ref(), delivered);
                        }
                    }
                }
            }
            signal = signal_rx.recv() => {
                let Some(signal) = signal else { return; };
                match signal {
                    ClientSignal::GenerationEnded { generation_id } => {
                        let Some(action) = ended_generation_action(
                            generation.as_ref(),
                            generation_id,
                            readiness.snapshot().connected,
                            config.mappings().len(),
                        ) else {
                            continue;
                        };
                        let current_generation_id = match action {
                            GenerationAction::RemoveOnly { generation_id }
                            | GenerationAction::RebuildAll { generation_id, .. } => generation_id,
                            GenerationAction::BuildAll { .. }
                            | GenerationAction::FlushRetained { .. } => unreachable!(
                                "generation end must remove the matching generation"
                            ),
                        };
                        debug_assert_eq!(current_generation_id, generation_id);
                        readiness.invalidate_subscription_generation();
                        if let Some(attempt) = barrier.take() {
                            attempt.stop().await;
                        }
                        if let Some(old_generation) = generation.take() {
                            old_generation.stop().await;
                        }
                        if let GenerationAction::RebuildAll { subject_count, .. } = action {
                            debug_assert_eq!(subject_count, config.mappings().len());
                            let Some((ingress_tx, diagnostics, local_node_id)) = ingress.as_ref() else {
                                continue;
                            };
                            generation = build_subscription_generation(
                                &client,
                                &config,
                                SubscriptionInputs {
                                    ingress_tx: ingress_tx.clone(),
                                    stats: stats.clone(),
                                    diagnostics: diagnostics.clone(),
                                    local_node_id: local_node_id.clone(),
                                    signal_tx: signal_tx.clone(),
                                },
                                next_generation_id,
                            ).await;
                            next_generation_id = next_generation_id.saturating_add(1);
                            let current_lifecycle = lifecycle.snapshot();
                            if barrier_allowed_after_latest_delivery(current_lifecycle) {
                                barrier = start_barrier(
                                    &client,
                                    generation.as_ref(),
                                    current_lifecycle,
                                );
                            }
                        }
                    }
                }
            }
            slow_consumer = slow_consumer_rx.recv() => {
                if slow_consumer.is_none() {
                    return;
                }
                if let Some(action) = handle_slow_consumer(
                    &stats,
                    &mut slow_consumers,
                    Instant::now(),
                    &nats_host,
                    nats_port,
                ) {
                    action.emit();
                }
            }
            result = wait_for_barrier(&mut barrier) => {
                let Some(result) = result else { continue; };
                let attempt = barrier.take().expect("completed barrier remains present");
                let succeeded = result.unwrap_or(false);
                if succeeded
                    && lifecycle.commit_barrier(
                        attempt.tags,
                        generation.as_ref().map(|current| current.id),
                    )
                {
                    LifecycleAction::status(
                        LifecycleReason::Ready,
                        &nats_host,
                        nats_port,
                        &readiness.snapshot(),
                    ).emit();
                }
            }
            _ = wait_for_outage_deadline(warning_deadline) => {
                if outage.periodic_due(Instant::now()) {
                    LifecycleAction::unavailable(
                        &nats_host,
                        nats_port,
                        &readiness.snapshot(),
                    ).emit();
                }
            }
        }
    }
}

async fn build_subscription_generation(
    client: &Client,
    config: &EnabledBridgeConfig,
    inputs: SubscriptionInputs,
    generation_id: u64,
) -> Option<SubscriptionGeneration> {
    let SubscriptionInputs {
        ingress_tx,
        stats,
        diagnostics,
        local_node_id,
        signal_tx,
    } = inputs;
    let mut subscribers = Vec::with_capacity(config.mappings().len());
    for mapping in config.mappings() {
        let Ok(subscriber) = client.subscribe(mapping.subject().clone()).await else {
            return None;
        };
        subscribers.push((subscriber, mapping.collection().to_owned()));
    }

    let task = tokio::spawn(async move {
        let mut readers = FuturesUnordered::new();
        for (subscriber, collection) in subscribers {
            readers.push(read_subscription(
                subscriber,
                collection,
                ingress_tx.clone(),
                stats.clone(),
                diagnostics.clone(),
                local_node_id.clone(),
            ));
        }
        if readers.next().await.is_some() {
            let _ = signal_tx
                .send(ClientSignal::GenerationEnded { generation_id })
                .await;
        }
    });
    Some(SubscriptionGeneration {
        id: generation_id,
        task,
    })
}

async fn read_subscription(
    mut subscriber: Subscriber,
    collection: String,
    ingress_tx: IngressSender,
    stats: IngressStats,
    diagnostics: IngressDiagnostics,
    local_node_id: String,
) {
    while let Some(message) = subscriber.next().await {
        if has_exact_own_origin(&message.headers, &local_node_id) {
            stats.record_self_suppressed();
            continue;
        }
        let payload_bytes = message.payload.len();
        if is_payload_oversized(payload_bytes) {
            stats.record_oversized_payload();
            diagnostics.record_oversized(&message.subject, &collection, payload_bytes);
            drop(message);
            continue;
        }
        let item = IngressItem::new(
            message.subject.clone(),
            collection.clone(),
            message.payload.to_vec(),
        );
        // Release async-nats' original Bytes before queue backpressure can await.
        drop(message);
        if ingress_tx.send(item).await.is_err() {
            return;
        }
    }
}

fn has_exact_own_origin(headers: &Option<async_nats::HeaderMap>, local_node_id: &str) -> bool {
    let Some(headers) = headers else {
        return false;
    };
    let mut values = headers.get_all(BRIDGE_ORIGIN_HEADER);
    let Some(value) = values.next() else {
        return false;
    };
    value.as_str() == local_node_id && values.next().is_none()
}

fn start_barrier(
    client: &Client,
    generation: Option<&SubscriptionGeneration>,
    lifecycle: LifecycleSnapshot,
) -> Option<BarrierAttempt> {
    let generation_id = generation?.id;
    let client = client.clone();
    Some(BarrierAttempt {
        tags: BarrierTags {
            connection_epoch: lifecycle.connection_epoch,
            invalidation_epoch: lifecycle.invalidation_epoch,
            generation_id,
        },
        task: tokio::spawn(async move { broker_round_trip(&client).await }),
    })
}

async fn wait_for_barrier(
    barrier: &mut Option<BarrierAttempt>,
) -> Option<Result<bool, tokio::task::JoinError>> {
    match barrier {
        Some(attempt) => Some((&mut attempt.task).await),
        None => pending().await,
    }
}

async fn broker_round_trip(client: &Client) -> bool {
    let request = Request::new().timeout(Some(READINESS_BARRIER_TIMEOUT));
    match client
        .send_request(READINESS_BARRIER_SUBJECT, request)
        .await
    {
        Ok(_) => true,
        Err(error) => error.kind() == RequestErrorKind::NoResponders,
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct SlowConsumerAction {
    nats_host: String,
    nats_port: u16,
    event_count: u64,
}

impl SlowConsumerAction {
    fn new(nats_host: &str, nats_port: u16, event_count: u64) -> Self {
        Self {
            nats_host: nats_host.to_owned(),
            nats_port,
            event_count,
        }
    }

    fn emit(&self) {
        warn!(
            nats_host = %self.nats_host,
            nats_port = self.nats_port,
            slow_consumer_events = self.event_count,
            error_kind = "slow_consumer",
            "NATS bridge slow consumer"
        );
    }
}

#[derive(Default)]
struct SlowConsumerLogState {
    next_warning: Option<Instant>,
}

impl SlowConsumerLogState {
    fn should_warn(&mut self, now: Instant) -> bool {
        if self.next_warning.is_some_and(|deadline| now < deadline) {
            return false;
        }
        self.next_warning = Some(now + SLOW_CONSUMER_WARNING_INTERVAL);
        true
    }
}

fn handle_slow_consumer(
    stats: &IngressStats,
    warning_state: &mut SlowConsumerLogState,
    now: Instant,
    nats_host: &str,
    nats_port: u16,
) -> Option<SlowConsumerAction> {
    stats.record_slow_consumer();
    warning_state.should_warn(now).then(|| {
        SlowConsumerAction::new(nats_host, nats_port, stats.snapshot().slow_consumer_events)
    })
}

async fn wait_for_outage_deadline(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => pending().await,
    }
}

fn nominal_retry_delay(attempt: usize) -> Duration {
    const SECONDS: [u64; 6] = [1, 2, 4, 8, 16, 30];
    Duration::from_secs(SECONDS[attempt.saturating_sub(1).min(SECONDS.len() - 1)])
}

fn retry_delay(attempt: usize, jitter_percent: u8) -> Duration {
    let nominal = nominal_retry_delay(attempt);
    let base_ms = nominal.as_millis().min(u128::from(u64::MAX)) as u64;
    let extra_ms = base_ms.saturating_mul(u64::from(jitter_percent.min(20))) / 100;
    Duration::from_millis(base_ms.saturating_add(extra_ms)).clamp(RETRY_MIN, RETRY_MAX)
}

#[derive(Default)]
struct OutageLogState {
    next_warning: Option<Instant>,
}

impl OutageLogState {
    /// Returns true only for the first failure in an outage.
    fn begin(&mut self, now: Instant) -> bool {
        if self.next_warning.is_some() {
            return false;
        }
        self.next_warning = Some(now + OUTAGE_WARNING_INTERVAL);
        true
    }

    fn periodic_due(&mut self, now: Instant) -> bool {
        match self.next_warning {
            Some(deadline) if now >= deadline => {
                self.next_warning = Some(now + OUTAGE_WARNING_INTERVAL);
                true
            }
            _ => false,
        }
    }

    fn deadline(&self) -> Option<Instant> {
        self.next_warning
    }

    fn recovered(&mut self) {
        self.next_warning = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nats_bridge::config::BridgeConfig;
    use crate::nats_bridge::ingress::INGRESS_QUEUE_CAPACITY;
    use async_nats::HeaderMap;

    #[tokio::test]
    async fn runtime_try_spawn_rejects_invalid_origin_identity_before_nats_tasks() {
        let dir = tempfile::tempdir().unwrap();
        let node = Arc::new(
            SidecarNode::new(crate::node::SidecarConfig {
                node_id: "invalid\norigin".to_owned(),
                app_id: "runtime-identity-test".to_owned(),
                data_dir: dir.path().to_path_buf(),
                disable_mdns: true,
                ..Default::default()
            })
            .await
            .unwrap(),
        );
        let result = BridgeRuntime::try_spawn(
            enabled_config(),
            "invalid\norigin".to_owned(),
            Arc::clone(&node),
        );
        let Err(error) = result else {
            panic!("invalid header identity must reject runtime startup");
        };
        assert_eq!(
            error.to_string(),
            "invalid effective NATS bridge node identity"
        );
    }

    fn enabled_config() -> EnabledBridgeConfig {
        let mappings = vec![
            "vision.summary=frames".to_owned(),
            "node.health=health".to_owned(),
        ];
        let BridgeConfig::Enabled(config) =
            BridgeConfig::from_raw(Some("nats://127.0.0.1:9"), &mappings)
                .expect("test config should be valid")
        else {
            panic!("mappings should enable config");
        };
        config
    }

    #[test]
    fn origin_marker_predicate_requires_one_byte_exact_value() {
        let local = "effective-test-node";
        assert!(!has_exact_own_origin(&None, local));

        for value in [
            "",
            "foreign-node",
            "%malformed%",
            " effective-test-node ",
            "EFFECTIVE-TEST-NODE",
        ] {
            let mut headers = HeaderMap::new();
            headers.insert(BRIDGE_ORIGIN_HEADER, value);
            assert!(!has_exact_own_origin(&Some(headers), local));
        }

        let mut exact = HeaderMap::new();
        exact.insert(BRIDGE_ORIGIN_HEADER, local);
        assert!(has_exact_own_origin(&Some(exact), local));

        let mut repeated = HeaderMap::new();
        repeated.append(BRIDGE_ORIGIN_HEADER, local);
        repeated.append(BRIDGE_ORIGIN_HEADER, local);
        assert!(!has_exact_own_origin(&Some(repeated), local));
    }

    fn generation(id: u64) -> SubscriptionGeneration {
        SubscriptionGeneration {
            id,
            task: tokio::spawn(pending()),
        }
    }

    #[test]
    fn nominal_backoff_sequence_caps_and_resets_by_attempt_generation() {
        let delays: Vec<_> = (1..=7)
            .map(|attempt| nominal_retry_delay(attempt).as_secs())
            .collect();
        assert_eq!(delays, [1, 2, 4, 8, 16, 30, 30]);
        assert_eq!(nominal_retry_delay(1), Duration::from_secs(1));
    }

    #[test]
    fn bounded_jitter_never_crosses_hard_limits() {
        for attempt in 1..=12 {
            for percent in 0..=100 {
                let delay = retry_delay(attempt, percent);
                assert!((RETRY_MIN..=RETRY_MAX).contains(&delay));
            }
        }
        assert_eq!(retry_delay(1, 0), Duration::from_secs(1));
        assert_eq!(retry_delay(6, 20), Duration::from_secs(30));
    }

    #[tokio::test(start_paused = true)]
    async fn outage_warning_occurs_initially_then_each_five_minutes() {
        let now = Instant::now();
        let mut outage = OutageLogState::default();
        assert!(outage.begin(now));
        assert!(!outage.begin(now));
        assert!(!outage.periodic_due(now + Duration::from_secs(299)));
        assert!(outage.periodic_due(now + Duration::from_secs(300)));
        assert!(!outage.periodic_due(now + Duration::from_secs(599)));
        assert!(outage.periodic_due(now + Duration::from_secs(600)));
        outage.recovered();
        assert!(outage.begin(now + Duration::from_secs(601)));
    }

    #[tokio::test(start_paused = true)]
    async fn outage_warning_wait_is_anchored_to_outage_start() {
        tokio::time::advance(Duration::from_secs(299)).await;
        let mut outage = OutageLogState::default();
        assert!(outage.begin(Instant::now()));

        let waiter = tokio::spawn(wait_for_outage_deadline(outage.deadline()));
        tokio::time::advance(Duration::from_secs(299)).await;
        assert!(!waiter.is_finished());
        tokio::time::advance(Duration::from_secs(1)).await;
        waiter.await.expect("deadline waiter should finish");
    }

    #[test]
    fn lifecycle_actions_keep_exact_boolean_readiness_transitions() {
        let config = enabled_config();
        let host = config.server_addr().host().to_owned();
        let port = config.server_addr().port();
        let readiness = BridgeReadiness::new(
            config
                .mappings()
                .iter()
                .map(|mapping| mapping.subject().clone()),
        );

        let starting = LifecycleAction::status(
            LifecycleReason::Starting,
            &host,
            port,
            &readiness.snapshot(),
        );
        let unavailable = LifecycleAction::unavailable(&host, port, &readiness.snapshot());
        assert!(!starting.bridge_ready);
        assert!(!unavailable.bridge_ready);

        readiness.set_connected();
        let pending = LifecycleAction::status(
            LifecycleReason::SubscriptionsPending,
            &host,
            port,
            &readiness.snapshot(),
        );
        assert!(pending.connected);
        assert!(!pending.bridge_ready);

        let transition = readiness.mark_all_subscriptions_established();
        assert!(transition.became_ready());
        let ready =
            LifecycleAction::status(LifecycleReason::Ready, &host, port, &readiness.snapshot());
        assert!(ready.bridge_ready);
        assert!(!readiness
            .mark_all_subscriptions_established()
            .became_ready());

        readiness.mark_disconnected();
        let disconnected = LifecycleAction::status(
            LifecycleReason::Disconnected,
            &host,
            port,
            &readiness.snapshot(),
        );
        assert!(!disconnected.bridge_ready);
        assert!(!disconnected.connected);
        assert!(disconnected.established_subscriptions == 0);
        readiness.set_connected();
        assert!(!readiness.snapshot().is_ready());
    }

    #[tokio::test]
    async fn barrier_success_requires_exact_live_epoch_and_generation_tags() {
        let config = enabled_config();
        let readiness = BridgeReadiness::new(
            config
                .mappings()
                .iter()
                .map(|mapping| mapping.subject().clone()),
        );
        let lifecycle = LifecycleControl::new(readiness);
        lifecycle.delivered(DeliveredLifecycleEvent::Connected);
        let current = lifecycle.snapshot();
        let generation = generation(7);
        let tags = BarrierTags {
            connection_epoch: current.connection_epoch,
            invalidation_epoch: current.invalidation_epoch,
            generation_id: generation.id,
        };

        assert!(barrier_tags_match(tags, current, Some(generation.id)));
        assert!(!barrier_tags_match(
            BarrierTags {
                connection_epoch: tags.connection_epoch.saturating_add(1),
                ..tags
            },
            current,
            Some(generation.id),
        ));
        lifecycle.delivered(DeliveredLifecycleEvent::Error);
        assert!(!barrier_tags_match(
            tags,
            lifecycle.snapshot(),
            Some(generation.id),
        ));
        lifecycle.delivered(DeliveredLifecycleEvent::Disconnected);
        assert!(!barrier_tags_match(
            tags,
            lifecycle.snapshot(),
            Some(generation.id),
        ));
        generation.stop().await;
    }

    #[test]
    fn delivered_error_and_stale_barrier_commit_share_one_transition_boundary() {
        let config = enabled_config();
        let readiness = BridgeReadiness::new(
            config
                .mappings()
                .iter()
                .map(|mapping| mapping.subject().clone()),
        );
        let lifecycle = LifecycleControl::new(readiness.clone());

        for generation_id in 1..=64 {
            lifecycle.delivered(DeliveredLifecycleEvent::Connected);
            let current = lifecycle.snapshot();
            let tags = BarrierTags {
                connection_epoch: current.connection_epoch,
                invalidation_epoch: current.invalidation_epoch,
                generation_id,
            };
            let start = Arc::new(std::sync::Barrier::new(3));

            let error_lifecycle = lifecycle.clone();
            let error_start = Arc::clone(&start);
            let error = std::thread::spawn(move || {
                error_start.wait();
                error_lifecycle.delivered(DeliveredLifecycleEvent::Error);
            });

            let barrier_lifecycle = lifecycle.clone();
            let barrier_start = Arc::clone(&start);
            let barrier = std::thread::spawn(move || {
                barrier_start.wait();
                barrier_lifecycle.commit_barrier(tags, Some(generation_id));
            });

            start.wait();
            error.join().expect("delivered error thread should finish");
            barrier.join().expect("barrier commit thread should finish");

            let status = readiness.snapshot();
            assert!(status.connected);
            assert!(status.established_subjects.is_empty());
            assert!(!status.is_ready());
        }
    }

    #[tokio::test]
    async fn ingress_accepts_messages_before_barrier_confirmation() {
        let config = enabled_config();
        let readiness = BridgeReadiness::new(
            config
                .mappings()
                .iter()
                .map(|mapping| mapping.subject().clone()),
        );
        readiness.set_connected();
        let (sender, mut rx) = ingress_channel();

        sender
            .send(IngressItem::new(
                "vision.summary".into(),
                "frames".to_owned(),
                br#"{"pre_barrier":true}"#.to_vec(),
            ))
            .await
            .expect("pre-barrier ingress should not be readiness-gated");

        let _item = rx.recv().await.expect("message should be queued");
        assert!(!readiness.snapshot().is_ready());
    }

    #[tokio::test]
    async fn full_ingress_queue_cannot_delay_disconnect_readiness() {
        let config = enabled_config();
        let readiness = BridgeReadiness::new(
            config
                .mappings()
                .iter()
                .map(|mapping| mapping.subject().clone()),
        );
        readiness.set_connected();
        readiness.mark_all_subscriptions_established();
        let (sender, _rx) = ingress_channel();
        for sequence in 0..INGRESS_QUEUE_CAPACITY {
            sender
                .send(IngressItem::new(
                    "vision.summary".into(),
                    "frames".to_owned(),
                    sequence.to_string().into_bytes(),
                ))
                .await
                .expect("queue should accept its bounded capacity");
        }
        let blocked = tokio::spawn(async move {
            sender
                .send(IngressItem::new(
                    "vision.summary".into(),
                    "frames".to_owned(),
                    Vec::new(),
                ))
                .await
        });
        tokio::task::yield_now().await;
        assert!(!blocked.is_finished());

        let lifecycle = LifecycleControl::new(readiness.clone());
        lifecycle.delivered(DeliveredLifecycleEvent::Disconnected);
        assert!(!readiness.snapshot().connected);
        assert!(!readiness.snapshot().is_ready());
        blocked.abort();
    }

    #[test]
    fn delivered_control_invalidation_is_non_blocking_under_callback_saturation() {
        let readiness = BridgeReadiness::new(["vision.summary".into()]);
        let lifecycle = LifecycleControl::new(readiness.clone());
        lifecycle.delivered(DeliveredLifecycleEvent::Connected);
        readiness.mark_all_subscriptions_established();
        let (_slow_tx, slow_rx) = mpsc::channel::<()>(1);

        for _ in 0..(SUPERVISOR_SIGNAL_CAPACITY * 4) {
            lifecycle.delivered(DeliveredLifecycleEvent::Error);
        }

        let snapshot = lifecycle.snapshot();
        assert_eq!(
            snapshot.invalidation_epoch,
            (SUPERVISOR_SIGNAL_CAPACITY * 4) as u64
        );
        assert!(snapshot.connected);
        assert_eq!(snapshot.last_event, DeliveredLifecycleEvent::Error);
        assert!(readiness.snapshot().established_subjects.is_empty());
        assert!(
            slow_rx.is_empty(),
            "lifecycle control is separate from telemetry"
        );
    }

    #[test]
    fn disconnect_reason_distinguishes_initial_unavailability_from_connection_loss() {
        let readiness = BridgeReadiness::new(["vision.summary".into()]);
        let lifecycle = LifecycleControl::new(readiness);

        lifecycle.delivered(DeliveredLifecycleEvent::Disconnected);
        assert_eq!(
            disconnected_reason(lifecycle.snapshot()),
            LifecycleReason::BrokerUnavailable
        );

        lifecycle.delivered(DeliveredLifecycleEvent::Connected);
        lifecycle.delivered(DeliveredLifecycleEvent::Disconnected);
        assert_eq!(
            disconnected_reason(lifecycle.snapshot()),
            LifecycleReason::Disconnected
        );
    }

    #[tokio::test]
    async fn ended_generation_disconnect_then_end_removes_before_next_connect() {
        let generation = generation(41);
        assert_eq!(
            ended_generation_action(Some(&generation), 41, false, 2),
            Some(GenerationAction::RemoveOnly { generation_id: 41 })
        );
        generation.stop().await;
        assert_eq!(
            connected_generation_action(None, 2),
            GenerationAction::BuildAll { subject_count: 2 },
            "the next connection must build a replacement generation"
        );
        let replacement_id = 42_u64;
        assert!(replacement_id > 41);
    }

    #[tokio::test]
    async fn ended_generation_end_then_disconnect_retains_only_the_replacement() {
        let ended = generation(41);
        assert_eq!(
            ended_generation_action(Some(&ended), 41, true, 2),
            Some(GenerationAction::RebuildAll {
                generation_id: 41,
                subject_count: 2,
            })
        );
        ended.stop().await;

        let replacement = generation(42);
        assert_eq!(
            connected_generation_action(Some(&replacement), 2),
            GenerationAction::FlushRetained { generation_id: 42 },
            "ordinary disconnect retains only the live replacement"
        );
        assert!(replacement.id > 41);
        replacement.stop().await;
    }

    #[tokio::test]
    async fn ended_generation_stale_signal_cannot_remove_current_generation() {
        let generation = generation(42);
        assert_eq!(
            ended_generation_action(Some(&generation), 41, false, 2),
            None,
            "a stale sentinel cannot replace the live generation"
        );
        assert_eq!(
            connected_generation_action(Some(&generation), 2),
            GenerationAction::FlushRetained { generation_id: 42 }
        );
        generation.stop().await;
    }

    #[tokio::test(start_paused = true)]
    async fn slow_consumers_count_every_event_and_warn_once_per_sixty_seconds() {
        let stats = IngressStats::default();
        let mut warnings = SlowConsumerLogState::default();
        let config = enabled_config();
        let readiness = BridgeReadiness::new(
            config
                .mappings()
                .iter()
                .map(|mapping| mapping.subject().clone()),
        );
        readiness.set_connected();
        readiness.mark_all_subscriptions_established();
        let before = readiness.snapshot();
        let generation_id = 19_u64;
        let now = Instant::now();

        assert!(handle_slow_consumer(&stats, &mut warnings, now, "127.0.0.1", 4222).is_some());
        assert!(handle_slow_consumer(
            &stats,
            &mut warnings,
            now + Duration::from_secs(59),
            "127.0.0.1",
            4222,
        )
        .is_none());
        assert!(handle_slow_consumer(
            &stats,
            &mut warnings,
            now + SLOW_CONSUMER_WARNING_INTERVAL,
            "127.0.0.1",
            4222,
        )
        .is_some());
        assert_eq!(stats.snapshot().slow_consumer_events, 3);
        assert_eq!(readiness.snapshot(), before);
        assert_eq!(generation_id, 19);
    }

    #[test]
    fn lifecycle_and_slow_consumer_actions_cannot_retain_unsafe_sources() {
        let mappings = vec!["vision.summary=frames".to_owned()];
        let BridgeConfig::Enabled(config) = BridgeConfig::from_raw(
            Some("nats://raw-user:raw-pass%65ncoded@broker.internal:4222"),
            &mappings,
        )
        .expect("credential-bearing config should be valid") else {
            panic!("mapping should enable config");
        };
        let readiness = BridgeReadiness::new(["vision.summary".into()]);
        let lifecycle = LifecycleAction::unavailable(
            config.server_addr().host(),
            config.server_addr().port(),
            &readiness.snapshot(),
        );
        let slow =
            SlowConsumerAction::new(config.server_addr().host(), config.server_addr().port(), 7);
        let rendered = format!("{lifecycle:?} {slow:?}");
        for forbidden in [
            "raw-user",
            "raw-pass%65ncoded",
            "nats://",
            "subscription 923",
            "server source error",
            r#"{"private":"payload"}"#,
        ] {
            assert!(!rendered.contains(forbidden));
        }
        assert!(rendered.contains("broker.internal"));
        assert!(rendered.contains("SlowConsumerAction"));
    }

    #[tokio::test]
    async fn spawn_returns_before_unavailable_connector_can_resolve() {
        let handle = BridgeRuntime::spawn_connection_only(enabled_config());
        assert!(!handle.readiness().snapshot().is_ready());
        assert!(!handle.is_finished());
    }

    #[test]
    fn lifecycle_actions_contain_no_credential_bearing_address() {
        let mappings = vec!["vision.summary=frames".to_owned()];
        let BridgeConfig::Enabled(config) = BridgeConfig::from_raw(
            Some("nats://raw-user:raw-pass%65ncoded@127.0.0.1:9"),
            &mappings,
        )
        .expect("test config should be valid") else {
            panic!("mapping should enable config");
        };
        let readiness = BridgeReadiness::new(
            config
                .mappings()
                .iter()
                .map(|mapping| mapping.subject().clone()),
        );
        let rendered = format!(
            "{:?}",
            LifecycleAction::unavailable(
                config.server_addr().host(),
                config.server_addr().port(),
                &readiness.snapshot(),
            )
        );
        for secret in ["raw-user", "raw-pass%65ncoded", "raw-passencoded"] {
            assert!(!rendered.contains(secret));
        }
    }
}
