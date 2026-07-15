//! Bounded, serial persistence pipeline for messages received from Core NATS.
//!
//! All configured subjects share one FIFO. Senders await capacity rather than
//! deliberately dropping at this boundary, and exactly one receiver validates
//! messages and performs create-only Peat writes. A message's UUID and encoded
//! envelope are created once before bounded retries so one accepted message
//! cannot become multiple documents.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use async_nats::Subject;
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::nats_bridge::envelope::BridgeEnvelope;
use crate::node::{CreateBridgeDocumentError, SidecarNode};

/// Process-wide number of raw NATS messages allowed to await persistence.
pub const INGRESS_QUEUE_CAPACITY: usize = 256;

/// Maximum create-only storage calls made for one accepted NATS message.
pub const STORE_MAX_ATTEMPTS: usize = 3;

/// Delay after the first transient storage failure.
pub const STORE_RETRY_DELAY_FIRST: Duration = Duration::from_millis(50);

/// Delay after the second transient storage failure.
pub const STORE_RETRY_DELAY_SECOND: Duration = Duration::from_millis(200);

const STORE_RETRY_DELAYS: [Duration; STORE_MAX_ATTEMPTS - 1] =
    [STORE_RETRY_DELAY_FIRST, STORE_RETRY_DELAY_SECOND];

/// One routed message awaiting validation and persistence.
#[derive(Debug)]
pub struct IngressItem {
    subject: Subject,
    collection: String,
    payload: Vec<u8>,
}

impl IngressItem {
    /// Construct an item from validated literal route metadata and raw bytes.
    pub fn new(subject: Subject, collection: String, payload: Vec<u8>) -> Self {
        Self {
            subject,
            collection,
            payload,
        }
    }
}

/// Cloneable producer for the one process-wide ingress FIFO.
#[derive(Clone)]
pub struct IngressSender {
    tx: mpsc::Sender<IngressItem>,
}

impl IngressSender {
    /// Await shared queue capacity and enqueue one raw message in FIFO order.
    pub async fn send(&self, item: IngressItem) -> Result<(), mpsc::error::SendError<IngressItem>> {
        self.tx.send(item).await
    }
}

/// Create the sole bounded ingress channel used by all subject readers.
pub fn ingress_channel() -> (IngressSender, mpsc::Receiver<IngressItem>) {
    let (tx, rx) = mpsc::channel(INGRESS_QUEUE_CAPACITY);
    (IngressSender { tx }, rx)
}

/// Narrow create-only persistence seam used by the serial processor.
pub trait BridgeDocumentWriter: Send + Sync + 'static {
    fn create_bridge_document<'a>(
        &'a self,
        collection: &'a str,
        doc_id: &'a str,
        envelope_json: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), CreateBridgeDocumentError>> + Send + 'a>>;
}

impl BridgeDocumentWriter for Arc<SidecarNode> {
    fn create_bridge_document<'a>(
        &'a self,
        collection: &'a str,
        doc_id: &'a str,
        envelope_json: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), CreateBridgeDocumentError>> + Send + 'a>> {
        Box::pin(SidecarNode::create_bridge_document(
            self,
            collection,
            doc_id,
            envelope_json,
        ))
    }
}

/// Drain one ingress receiver serially until every sender has been dropped.
pub async fn run_ingress_processor<W>(
    mut rx: mpsc::Receiver<IngressItem>,
    source_node_id: String,
    writer: W,
) where
    W: BridgeDocumentWriter,
{
    while let Some(item) = rx.recv().await {
        process_item(&source_node_id, &writer, item).await;
    }
}

async fn process_item<W>(source_node_id: &str, writer: &W, item: IngressItem)
where
    W: BridgeDocumentWriter,
{
    let envelope =
        match BridgeEnvelope::from_payload(item.subject.as_str(), source_node_id, &item.payload) {
            Ok(envelope) => envelope,
            Err(_) => return,
        };

    let doc_id = Uuid::new_v4().to_string();
    let Ok(envelope_json) = serde_json::to_string(&envelope) else {
        return;
    };

    for attempt in 0..STORE_MAX_ATTEMPTS {
        match writer
            .create_bridge_document(&item.collection, &doc_id, &envelope_json)
            .await
        {
            Ok(()) => return,
            Err(error) if is_transient(error) && attempt < STORE_RETRY_DELAYS.len() => {
                tokio::time::sleep(STORE_RETRY_DELAYS[attempt]).await;
            }
            Err(_) => return,
        }
    }
}

fn is_transient(error: CreateBridgeDocumentError) -> bool {
    matches!(
        error,
        CreateBridgeDocumentError::StoreRead | CreateBridgeDocumentError::StoreWrite
    )
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::Mutex;

    use tokio::sync::{mpsc, Notify};

    use super::*;

    #[derive(Clone, Debug)]
    struct WriteCall {
        collection: String,
        doc_id: String,
        envelope_json: String,
        at: tokio::time::Instant,
    }

    #[derive(Clone, Default)]
    struct FakeWriter {
        calls: Arc<Mutex<Vec<WriteCall>>>,
        results: Arc<Mutex<VecDeque<Result<(), CreateBridgeDocumentError>>>>,
        call_tx: Arc<Mutex<Option<mpsc::Sender<WriteCall>>>>,
        blocked: Arc<Mutex<bool>>,
        release: Arc<Notify>,
    }

    impl FakeWriter {
        fn with_results(
            results: impl IntoIterator<Item = Result<(), CreateBridgeDocumentError>>,
        ) -> Self {
            Self {
                results: Arc::new(Mutex::new(results.into_iter().collect())),
                ..Self::default()
            }
        }

        fn calls(&self) -> Vec<WriteCall> {
            self.calls.lock().expect("calls lock").clone()
        }

        fn observe_calls(&self) -> mpsc::Receiver<WriteCall> {
            let (tx, rx) = mpsc::channel(16);
            *self.call_tx.lock().expect("call sender lock") = Some(tx);
            rx
        }

        fn block(&self) {
            *self.blocked.lock().expect("blocked lock") = true;
        }

        fn release(&self) {
            *self.blocked.lock().expect("blocked lock") = false;
            self.release.notify_waiters();
        }
    }

    impl BridgeDocumentWriter for FakeWriter {
        fn create_bridge_document<'a>(
            &'a self,
            collection: &'a str,
            doc_id: &'a str,
            envelope_json: &'a str,
        ) -> Pin<Box<dyn Future<Output = Result<(), CreateBridgeDocumentError>> + Send + 'a>>
        {
            Box::pin(async move {
                let call = WriteCall {
                    collection: collection.to_owned(),
                    doc_id: doc_id.to_owned(),
                    envelope_json: envelope_json.to_owned(),
                    at: tokio::time::Instant::now(),
                };
                self.calls.lock().expect("calls lock").push(call.clone());
                let call_tx = self.call_tx.lock().expect("call sender lock").clone();
                if let Some(tx) = call_tx {
                    let _ = tx.send(call).await;
                }
                while *self.blocked.lock().expect("blocked lock") {
                    self.release.notified().await;
                }
                self.results
                    .lock()
                    .expect("results lock")
                    .pop_front()
                    .unwrap_or(Ok(()))
            })
        }
    }

    fn item(subject: &str, collection: &str, payload: &[u8]) -> IngressItem {
        IngressItem::new(subject.into(), collection.to_owned(), payload.to_vec())
    }

    #[tokio::test]
    async fn queue_capacity_blocks_the_257th_send() {
        let (sender, _rx) = ingress_channel();
        for sequence in 0..INGRESS_QUEUE_CAPACITY {
            sender
                .send(item(
                    "vision.summary",
                    "frames",
                    sequence.to_string().as_bytes(),
                ))
                .await
                .expect("queue should have capacity");
        }

        let blocked = tokio::spawn({
            let sender = sender.clone();
            async move { sender.send(item("vision.summary", "frames", b"257")).await }
        });
        tokio::task::yield_now().await;
        assert!(!blocked.is_finished(), "the 257th send must await capacity");
        blocked.abort();
    }

    #[tokio::test]
    async fn blocked_writer_preserves_global_fifo_across_subjects() {
        let writer = FakeWriter::default();
        writer.block();
        let mut observed = writer.observe_calls();
        let (sender, rx) = ingress_channel();
        let worker = tokio::spawn(run_ingress_processor(
            rx,
            "source-a".to_owned(),
            writer.clone(),
        ));

        sender
            .send(item("alpha", "collection-a", br#"{"sequence":1}"#))
            .await
            .expect("first send");
        let first = observed.recv().await.expect("first write call");
        sender
            .send(item("beta", "collection-b", br#"{"sequence":2}"#))
            .await
            .expect("second send");
        sender
            .send(item("alpha", "collection-a", br#"{"sequence":3}"#))
            .await
            .expect("third send");
        tokio::task::yield_now().await;
        assert!(
            observed.try_recv().is_err(),
            "one blocked writer must serialize work"
        );

        writer.release();
        let second = observed.recv().await.expect("second write call");
        let third = observed.recv().await.expect("third write call");
        assert_eq!(
            [first.collection, second.collection, third.collection],
            ["collection-a", "collection-b", "collection-a"]
        );

        drop(sender);
        worker.await.expect("worker should finish");
    }

    #[tokio::test]
    async fn invalid_utf8_and_json_never_reach_writer_and_processing_continues() {
        let writer = FakeWriter::default();
        let (sender, rx) = ingress_channel();
        let worker = tokio::spawn(run_ingress_processor(
            rx,
            "source-a".to_owned(),
            writer.clone(),
        ));

        sender
            .send(item("alpha", "frames", &[0xff, 0xfe]))
            .await
            .expect("invalid UTF-8 send");
        sender
            .send(item("alpha", "frames", br#"{"broken":"#))
            .await
            .expect("invalid JSON send");
        sender
            .send(item("alpha", "frames", br#"{"valid":true}"#))
            .await
            .expect("valid send");
        drop(sender);
        worker.await.expect("worker should finish");

        assert_eq!(writer.calls().len(), 1);
    }

    #[tokio::test]
    async fn identical_messages_receive_distinct_uuid_v4_ids() {
        let writer = FakeWriter::default();
        let (sender, rx) = ingress_channel();
        let worker = tokio::spawn(run_ingress_processor(
            rx,
            "source-a".to_owned(),
            writer.clone(),
        ));
        let payload = br#"{"same":true}"#;

        sender
            .send(item("alpha", "frames", payload))
            .await
            .expect("first send");
        sender
            .send(item("alpha", "frames", payload))
            .await
            .expect("second send");
        drop(sender);
        worker.await.expect("worker should finish");

        let calls = writer.calls();
        assert_eq!(calls.len(), 2);
        assert_ne!(calls[0].doc_id, calls[1].doc_id);
        for call in calls {
            let parsed = Uuid::parse_str(&call.doc_id).expect("document ID should be UUID");
            assert_eq!(parsed.get_version_num(), 4);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn transient_retries_reuse_id_and_envelope_with_exact_bounded_schedule() {
        let writer = FakeWriter::with_results([
            Err(CreateBridgeDocumentError::StoreRead),
            Err(CreateBridgeDocumentError::StoreWrite),
            Ok(()),
        ]);
        let (sender, rx) = ingress_channel();
        let worker = tokio::spawn(run_ingress_processor(
            rx,
            "source-a".to_owned(),
            writer.clone(),
        ));
        sender
            .send(item("alpha", "frames", br#"{"valid":true}"#))
            .await
            .expect("send");
        drop(sender);

        tokio::task::yield_now().await;
        assert_eq!(writer.calls().len(), 1);
        tokio::time::advance(STORE_RETRY_DELAY_FIRST - Duration::from_millis(1)).await;
        tokio::task::yield_now().await;
        assert_eq!(writer.calls().len(), 1);
        tokio::time::advance(Duration::from_millis(1)).await;
        tokio::task::yield_now().await;
        assert_eq!(writer.calls().len(), 2);
        tokio::time::advance(STORE_RETRY_DELAY_SECOND).await;
        tokio::task::yield_now().await;
        worker.await.expect("worker should finish");

        let calls = writer.calls();
        assert_eq!(calls.len(), STORE_MAX_ATTEMPTS);
        assert!(calls.iter().all(|call| call.doc_id == calls[0].doc_id));
        assert!(calls
            .iter()
            .all(|call| call.envelope_json == calls[0].envelope_json));
        assert_eq!(calls[1].at - calls[0].at, STORE_RETRY_DELAY_FIRST);
        assert_eq!(calls[2].at - calls[1].at, STORE_RETRY_DELAY_SECOND);
    }

    #[tokio::test(start_paused = true)]
    async fn transient_failures_stop_after_three_attempts() {
        let writer = FakeWriter::with_results(std::iter::repeat_n(
            Err(CreateBridgeDocumentError::StoreWrite),
            STORE_MAX_ATTEMPTS,
        ));
        let (sender, rx) = ingress_channel();
        let worker = tokio::spawn(run_ingress_processor(
            rx,
            "source-a".to_owned(),
            writer.clone(),
        ));
        sender
            .send(item("alpha", "frames", br#"{"valid":true}"#))
            .await
            .expect("send");
        drop(sender);
        tokio::time::advance(STORE_RETRY_DELAY_FIRST + STORE_RETRY_DELAY_SECOND).await;
        tokio::task::yield_now().await;
        worker.await.expect("worker should finish");
        assert_eq!(writer.calls().len(), STORE_MAX_ATTEMPTS);
    }

    #[tokio::test(start_paused = true)]
    async fn permanent_failure_does_not_retry_or_sleep() {
        let writer = FakeWriter::with_results([Err(CreateBridgeDocumentError::AlreadyExists)]);
        let (sender, rx) = ingress_channel();
        let worker = tokio::spawn(run_ingress_processor(
            rx,
            "source-a".to_owned(),
            writer.clone(),
        ));
        sender
            .send(item("alpha", "frames", br#"{"valid":true}"#))
            .await
            .expect("send");
        drop(sender);
        worker.await.expect("worker should finish");
        assert_eq!(writer.calls().len(), 1);
        assert_eq!(tokio::time::Instant::now(), writer.calls()[0].at);
    }
}
