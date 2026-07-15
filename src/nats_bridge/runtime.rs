//! Non-blocking Core NATS connection lifecycle and readiness events.
//!
//! async-nats 0.49.1 treats `max_reconnects(0)` as unlimited. This runtime
//! therefore uses the client's reconnect callback as the single retry owner
//! instead of layering a competing outer dial loop over it.

use std::future::pending;
use std::sync::Arc;
use std::time::Duration;

use async_nats::{Client, ConnectOptions, Event, Subscriber};
use futures::stream::{FuturesUnordered, StreamExt};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tracing::{debug, info, warn};

use super::config::EnabledBridgeConfig;
use super::ingress::{
    ingress_channel, run_ingress_processor, IngressItem, IngressSender, IngressStats,
};
use super::readiness::{BridgeReadiness, BridgeStatus};
use crate::node::SidecarNode;

const RETRY_MIN: Duration = Duration::from_secs(1);
const RETRY_MAX: Duration = Duration::from_secs(30);
const OUTAGE_WARNING_INTERVAL: Duration = Duration::from_secs(5 * 60);
const SLOW_CONSUMER_WARNING_INTERVAL: Duration = Duration::from_secs(60);
const SUPERVISOR_SIGNAL_CAPACITY: usize = 64;

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
    task: JoinHandle<()>,
    stats: IngressStats,
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

    /// Whether the supervisor has unexpectedly terminated.
    pub fn is_finished(&self) -> bool {
        self.task.is_finished()
    }
}

/// Enabled-only runtime constructor.
pub struct BridgeRuntime;

impl BridgeRuntime {
    /// Phase 1 compatibility constructor used until process wiring supplies a node.
    ///
    /// This keeps the connection lifecycle non-blocking; [`Self::spawn_with_node`]
    /// owns the Phase 2 subscription and ingress generation.
    pub fn spawn(config: EnabledBridgeConfig) -> BridgeRuntimeHandle {
        let stats = IngressStats::default();
        Self::spawn_supervisor(config, stats, None)
    }

    /// Spawn the complete subscription-aware ingress runtime.
    pub fn spawn_with_node(
        config: EnabledBridgeConfig,
        source_node_id: String,
        node: Arc<SidecarNode>,
    ) -> BridgeRuntimeHandle {
        let stats = IngressStats::default();
        let (ingress_tx, ingress_rx) = ingress_channel();
        let configured_subjects = config
            .mappings()
            .iter()
            .map(|mapping| mapping.subject().clone())
            .collect::<Vec<_>>();
        tokio::spawn(run_ingress_processor(
            ingress_rx,
            source_node_id,
            node,
            stats.clone(),
            configured_subjects,
        ));
        Self::spawn_supervisor(config, stats, Some(ingress_tx))
    }

    fn spawn_supervisor(
        config: EnabledBridgeConfig,
        stats: IngressStats,
        ingress_tx: Option<IngressSender>,
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

        LifecycleAction::status(
            LifecycleReason::Starting,
            &nats_host,
            nats_port,
            &readiness.snapshot(),
        )
        .emit();

        let task_readiness = readiness.clone();
        let task_host = nats_host.clone();
        let task_stats = stats.clone();
        let task = tokio::spawn(async move {
            run_client_supervisor(
                server_addr,
                task_host,
                nats_port,
                task_readiness,
                config,
                ingress_tx,
                task_stats,
            )
            .await;
        });

        BridgeRuntimeHandle {
            readiness,
            task,
            stats,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum ClientSignal {
    Connected,
    Disconnected,
    Unavailable,
    SlowConsumer,
    GenerationEnded { generation_id: u64 },
}

struct SubscriptionGeneration {
    id: u64,
    task: JoinHandle<()>,
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
    readiness: BridgeReadiness,
    config: EnabledBridgeConfig,
    ingress_tx: Option<IngressSender>,
    stats: IngressStats,
) {
    let (signal_tx, mut signal_rx) = mpsc::channel(SUPERVISOR_SIGNAL_CAPACITY);
    let retry_host = nats_host.clone();
    let event_signal_tx = signal_tx.clone();
    let options = ConnectOptions::new()
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
            let signal_tx = event_signal_tx.clone();
            async move {
                let signal = match event {
                    Event::Connected => Some(ClientSignal::Connected),
                    Event::Disconnected | Event::Closed => Some(ClientSignal::Disconnected),
                    Event::ClientError(_) | Event::ServerError(_) => {
                        Some(ClientSignal::Unavailable)
                    }
                    Event::SlowConsumer(_) => Some(ClientSignal::SlowConsumer),
                    _ => None,
                };
                if let Some(signal) = signal {
                    let _ = signal_tx.send(signal).await;
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

    let mut outage = OutageLogState::default();
    let mut slow_consumers = SlowConsumerLogState::default();
    let mut generation = None;
    let mut next_generation_id = 1_u64;
    loop {
        let warning_deadline = outage.deadline();
        tokio::select! {
            signal = signal_rx.recv() => {
                let Some(signal) = signal else { return; };
                match signal {
                    ClientSignal::Connected => {
                        outage.recovered();
                        readiness.set_connected();
                        LifecycleAction::status(
                            LifecycleReason::SubscriptionsPending,
                            &nats_host,
                            nats_port,
                            &readiness.snapshot(),
                        ).emit();
                        if let Some(ingress_tx) = ingress_tx.as_ref() {
                            if generation.is_none() {
                                generation = build_subscription_generation(
                                    &client,
                                    &config,
                                    ingress_tx.clone(),
                                    signal_tx.clone(),
                                    next_generation_id,
                                ).await;
                                next_generation_id = next_generation_id.saturating_add(1);
                            }
                            establish_generation(
                                &client,
                                generation.as_ref(),
                                &readiness,
                                &nats_host,
                                nats_port,
                            ).await;
                        }
                    }
                    ClientSignal::Disconnected => {
                        readiness.mark_disconnected();
                        outage.begin(Instant::now());
                        LifecycleAction::status(
                            LifecycleReason::Disconnected,
                            &nats_host,
                            nats_port,
                            &readiness.snapshot(),
                        ).emit();
                    }
                    ClientSignal::Unavailable => {
                        if outage.begin(Instant::now()) {
                            LifecycleAction::unavailable(
                                &nats_host,
                                nats_port,
                                &readiness.snapshot(),
                            ).emit();
                        }
                    }
                    ClientSignal::SlowConsumer => {
                        stats.record_slow_consumer();
                        if slow_consumers.should_warn(Instant::now()) {
                            SlowConsumerAction::new(&nats_host, nats_port, stats.snapshot().slow_consumer_events).emit();
                        }
                    }
                    ClientSignal::GenerationEnded { generation_id } => {
                        if generation.as_ref().map(|current| current.id) != Some(generation_id)
                            || !readiness.snapshot().connected
                        {
                            continue;
                        }
                        readiness.invalidate_subscription_generation();
                        if let Some(old_generation) = generation.take() {
                            old_generation.stop().await;
                        }
                        if let Some(ingress_tx) = ingress_tx.as_ref() {
                            generation = build_subscription_generation(
                                &client,
                                &config,
                                ingress_tx.clone(),
                                signal_tx.clone(),
                                next_generation_id,
                            ).await;
                            next_generation_id = next_generation_id.saturating_add(1);
                            establish_generation(
                                &client,
                                generation.as_ref(),
                                &readiness,
                                &nats_host,
                                nats_port,
                            ).await;
                        }
                    }
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
    ingress_tx: IngressSender,
    signal_tx: mpsc::Sender<ClientSignal>,
    generation_id: u64,
) -> Option<SubscriptionGeneration> {
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
) {
    while let Some(message) = subscriber.next().await {
        let item = IngressItem::new(
            message.subject,
            collection.clone(),
            message.payload.to_vec(),
        );
        if ingress_tx.send(item).await.is_err() {
            return;
        }
    }
}

async fn establish_generation(
    client: &Client,
    generation: Option<&SubscriptionGeneration>,
    readiness: &BridgeReadiness,
    nats_host: &str,
    nats_port: u16,
) {
    if generation.is_none() || client.flush().await.is_err() {
        return;
    }
    let transition = readiness.mark_all_subscriptions_established();
    if transition.became_ready() {
        LifecycleAction::status(
            LifecycleReason::Ready,
            nats_host,
            nats_port,
            transition.status(),
        )
        .emit();
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
    async fn spawn_returns_before_unavailable_connector_can_resolve() {
        let handle = BridgeRuntime::spawn(enabled_config());
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
