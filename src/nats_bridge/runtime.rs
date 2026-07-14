//! Non-blocking Core NATS connection lifecycle and readiness events.
//!
//! async-nats 0.49.1 treats `max_reconnects(0)` as unlimited. This runtime
//! therefore uses the client's reconnect callback as the single retry owner
//! instead of layering a competing outer dial loop over it.

use std::time::Duration;

use async_nats::{ConnectOptions, Event, Subject};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::{Instant, MissedTickBehavior};
use tracing::{debug, info, warn};

use super::config::EnabledBridgeConfig;
use super::readiness::{BridgeReadiness, BridgeStatus};

const RETRY_MIN: Duration = Duration::from_secs(1);
const RETRY_MAX: Duration = Duration::from_secs(30);
const OUTAGE_WARNING_INTERVAL: Duration = Duration::from_secs(5 * 60);

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
    nats_host: String,
    nats_port: u16,
}

impl BridgeRuntimeHandle {
    /// Watch internal readiness without changing the public sidecar status RPC.
    pub fn readiness(&self) -> &BridgeReadiness {
        &self.readiness
    }

    /// Stable Phase 2 seam, called only after a real subscribe plus flush.
    pub fn mark_subscription_established(&self, subject: &Subject) {
        let transition = self.readiness.mark_subscription_established(subject);
        if transition.became_ready() {
            LifecycleAction::status(
                LifecycleReason::Ready,
                &self.nats_host,
                self.nats_port,
                &self.readiness.snapshot(),
            )
            .emit();
        }
    }

    /// Whether the supervisor has unexpectedly terminated.
    pub fn is_finished(&self) -> bool {
        self.task.is_finished()
    }
}

/// Enabled-only runtime constructor.
pub struct BridgeRuntime;

impl BridgeRuntime {
    /// Spawn one supervisor and return immediately without awaiting a broker dial.
    pub fn spawn(config: EnabledBridgeConfig) -> BridgeRuntimeHandle {
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
        let task = tokio::spawn(async move {
            run_client_supervisor(server_addr, task_host, nats_port, task_readiness).await;
        });

        BridgeRuntimeHandle {
            readiness,
            task,
            nats_host,
            nats_port,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ClientSignal {
    Connected,
    Disconnected,
    Unavailable,
}

async fn run_client_supervisor(
    server_addr: async_nats::ServerAddr,
    nats_host: String,
    nats_port: u16,
    readiness: BridgeReadiness,
) {
    let (signal_tx, mut signal_rx) = mpsc::unbounded_channel();
    let retry_host = nats_host.clone();
    let options = ConnectOptions::new()
        .retry_on_initial_connect()
        .max_reconnects(None)
        .reconnect_delay_callback(move |attempt| {
            let jitter_percent = rand::random::<u8>() % 21;
            let delay = retry_delay(attempt, jitter_percent);
            LifecycleAction::retry(&retry_host, nats_port, attempt, delay).emit();
            delay
        })
        .event_callback(move |event| {
            let signal_tx = signal_tx.clone();
            async move {
                let signal = match event {
                    Event::Connected => Some(ClientSignal::Connected),
                    Event::Disconnected | Event::Closed => Some(ClientSignal::Disconnected),
                    Event::ClientError(_) | Event::ServerError(_) => {
                        Some(ClientSignal::Unavailable)
                    }
                    _ => None,
                };
                if let Some(signal) = signal {
                    let _ = signal_tx.send(signal);
                }
            }
        });

    // retry_on_initial_connect makes this return a client before the first
    // network attempt completes. Retaining it here keeps exactly one internal
    // connector/reconnect owner alive for the supervisor lifetime.
    let _client = match options.connect(server_addr).await {
        Ok(client) => client,
        Err(_) => {
            let status = readiness.snapshot();
            LifecycleAction::unavailable(&nats_host, nats_port, &status).emit();
            return;
        }
    };

    let mut outage = OutageLogState::default();
    let mut warning_tick = tokio::time::interval_at(
        Instant::now() + OUTAGE_WARNING_INTERVAL,
        OUTAGE_WARNING_INTERVAL,
    );
    warning_tick.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
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
                }
            }
            _ = warning_tick.tick() => {
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

        readiness.mark_subscription_established(&Subject::from("vision.summary"));
        assert!(!readiness.snapshot().is_ready());
        let transition = readiness.mark_subscription_established(&Subject::from("node.health"));
        assert!(transition.became_ready());
        let ready =
            LifecycleAction::status(LifecycleReason::Ready, &host, port, &readiness.snapshot());
        assert!(ready.bridge_ready);
        assert!(!readiness
            .mark_subscription_established(&Subject::from("node.health"))
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
