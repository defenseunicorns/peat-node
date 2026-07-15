//! Crash-consistent, fixed-width suppression journals for the NATS bridge.
//!
//! Local authorship and remote delivery are intentionally separate files and
//! workers. A damaged or blocked delivery journal therefore cannot prevent a
//! healthy local-exclusion writer from protecting mapped Peat mutations.

use std::collections::HashMap;
use std::fmt;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc as std_mpsc, Arc, Mutex};
use std::thread::JoinHandle;

use sha2::{Digest, Sha256};
use tokio::sync::oneshot;

const LEDGER_DIR: &str = "nats-bridge-ledger-v1";
const EXCLUSION_FILE: &str = "local-exclusion-v1";
const DELIVERY_FILE: &str = "remote-delivery-v1";
const MAGIC: &[u8; 8] = b"PNATSJ01";
const VERSION: u32 = 1;
const HEADER_LEN: usize = 32;
const RECORD_LEN: usize = 80;
const COMMAND_CAPACITY: usize = 64;
pub(crate) const MAX_UNIQUE_ENTRIES: usize = 262_144;
const MAX_RECORDS: usize = MAX_UNIQUE_ENTRIES * 2;
const MAX_FILE_BYTES: u64 = (HEADER_LEN + MAX_RECORDS * RECORD_LEN) as u64;
const DIGEST_DOMAIN: &[u8] = b"peat-node:nats-bridge-ledger-key:v1";
const CHECKSUM_DOMAIN: &[u8] = b"peat-node:nats-bridge-ledger-record:v1";

/// Fixed identity retained by both journals. No raw collection or document ID
/// crosses the journal boundary.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) struct LedgerDigest(pub(crate) [u8; 32]);

/// Domain-separated, length-framed digest for a Peat document identity.
pub(crate) fn document_digest(collection: &str, doc_id: &str) -> LedgerDigest {
    let mut digest = Sha256::new();
    digest.update((DIGEST_DOMAIN.len() as u64).to_be_bytes());
    digest.update(DIGEST_DOMAIN);
    digest.update((collection.len() as u64).to_be_bytes());
    digest.update(collection.as_bytes());
    digest.update((doc_id.len() as u64).to_be_bytes());
    digest.update(doc_id.as_bytes());
    LedgerDigest(digest.finalize().into())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
enum RecordState {
    LocalExcluded = 1,
    Reserved = 2,
    Completed = 3,
}

impl RecordState {
    fn decode(value: u8) -> Result<Self, LedgerError> {
        match value {
            1 => Ok(Self::LocalExcluded),
            2 => Ok(Self::Reserved),
            3 => Ok(Self::Completed),
            _ => Err(LedgerError::Corrupt),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
enum JournalKind {
    Exclusion = 1,
    Delivery = 2,
}

impl JournalKind {
    fn file_name(self) -> &'static str {
        match self {
            Self::Exclusion => EXCLUSION_FILE,
            Self::Delivery => DELIVERY_FILE,
        }
    }

    fn permits(self, state: RecordState) -> bool {
        matches!(
            (self, state),
            (Self::Exclusion, RecordState::LocalExcluded)
                | (
                    Self::Delivery,
                    RecordState::Reserved | RecordState::Completed
                )
        )
    }
}

/// Payload-safe journal failure classification.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum LedgerError {
    Unavailable,
    Corrupt,
    Capacity,
    QueueFull,
    Stopped,
    IoUnjoined,
}

impl fmt::Display for LedgerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Unavailable => "bridge ledger unavailable",
            Self::Corrupt => "bridge ledger corrupt",
            Self::Capacity => "bridge ledger capacity exhausted",
            Self::QueueFull => "bridge ledger command queue full",
            Self::Stopped => "bridge ledger stopped",
            Self::IoUnjoined => "bridge ledger I/O worker did not join",
        })
    }
}

impl std::error::Error for LedgerError {}

/// Result of atomically checking and durably reserving a delivery key.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ReserveResult {
    Reserved,
    Suppressed,
}

enum Command {
    Record {
        digest: LedgerDigest,
        state: RecordState,
        response: oneshot::Sender<Result<bool, LedgerError>>,
    },
    Lookup {
        digest: LedgerDigest,
        response: oneshot::Sender<Result<Option<RecordState>, LedgerError>>,
    },
    Stop,
}

struct Worker {
    sender: std_mpsc::SyncSender<Command>,
    healthy: Arc<AtomicBool>,
    stopped: Arc<AtomicBool>,
    join: Mutex<Option<JoinHandle<()>>>,
}

impl Worker {
    fn spawn(data_dir: &Path, kind: JournalKind) -> Result<Arc<Self>, LedgerError> {
        let (sender, receiver) = std_mpsc::sync_channel(COMMAND_CAPACITY);
        let (open_tx, open_rx) = std_mpsc::sync_channel(1);
        let healthy = Arc::new(AtomicBool::new(false));
        let stopped = Arc::new(AtomicBool::new(false));
        let worker_healthy = Arc::clone(&healthy);
        let worker_stopped = Arc::clone(&stopped);
        let path = data_dir.to_path_buf();
        let join = std::thread::Builder::new()
            .name(
                match kind {
                    JournalKind::Exclusion => "peat-nats-exclusion-ledger",
                    JournalKind::Delivery => "peat-nats-delivery-ledger",
                }
                .to_owned(),
            )
            .spawn(move || {
                let opened = Journal::open(&path, kind);
                match opened {
                    Ok(mut journal) => {
                        worker_healthy.store(true, Ordering::Release);
                        let _ = open_tx.send(Ok(()));
                        while let Ok(command) = receiver.recv() {
                            if worker_stopped.load(Ordering::Acquire) {
                                break;
                            }
                            match command {
                                Command::Record {
                                    digest,
                                    state,
                                    response,
                                } => {
                                    let result = journal.record(digest, state);
                                    if result.is_err() {
                                        worker_healthy.store(false, Ordering::Release);
                                    }
                                    let _ = response.send(result);
                                }
                                Command::Lookup { digest, response } => {
                                    let result = if worker_healthy.load(Ordering::Acquire) {
                                        Ok(journal.index.get(&digest).copied())
                                    } else {
                                        Err(LedgerError::Unavailable)
                                    };
                                    let _ = response.send(result);
                                }
                                Command::Stop => break,
                            }
                        }
                    }
                    Err(error) => {
                        let _ = open_tx.send(Err(error));
                    }
                }
                worker_healthy.store(false, Ordering::Release);
                worker_stopped.store(true, Ordering::Release);
            })
            .map_err(|_| LedgerError::Unavailable)?;
        open_rx.recv().map_err(|_| LedgerError::Unavailable)??;
        Ok(Arc::new(Self {
            sender,
            healthy,
            stopped,
            join: Mutex::new(Some(join)),
        }))
    }

    fn is_healthy(&self) -> bool {
        self.healthy.load(Ordering::Acquire)
    }

    async fn record(&self, digest: LedgerDigest, state: RecordState) -> Result<bool, LedgerError> {
        if !self.is_healthy() {
            return Err(LedgerError::Unavailable);
        }
        let (response, receive) = oneshot::channel();
        self.sender
            .try_send(Command::Record {
                digest,
                state,
                response,
            })
            .map_err(map_send_error)?;
        receive.await.map_err(|_| LedgerError::Stopped)?
    }

    async fn lookup(&self, digest: LedgerDigest) -> Result<Option<RecordState>, LedgerError> {
        if !self.is_healthy() {
            return Err(LedgerError::Unavailable);
        }
        let (response, receive) = oneshot::channel();
        self.sender
            .try_send(Command::Lookup { digest, response })
            .map_err(map_send_error)?;
        receive.await.map_err(|_| LedgerError::Stopped)?
    }

    fn request_stop(&self) {
        self.stopped.store(true, Ordering::Release);
        let _ = self.sender.try_send(Command::Stop);
    }

    fn join(&self) -> Result<(), LedgerError> {
        self.request_stop();
        let join = self.join.lock().unwrap_or_else(|e| e.into_inner()).take();
        join.map_or(Ok(()), |join| {
            join.join().map_err(|_| LedgerError::IoUnjoined)
        })
    }
}

fn map_send_error(error: std_mpsc::TrySendError<Command>) -> LedgerError {
    match error {
        std_mpsc::TrySendError::Full(_) => LedgerError::QueueFull,
        std_mpsc::TrySendError::Disconnected(_) => LedgerError::Stopped,
    }
}

/// Independently owned local-exclusion journal facade.
#[derive(Clone)]
pub(crate) struct LocalExclusionLedger {
    worker: Arc<Worker>,
}

impl LocalExclusionLedger {
    pub(crate) fn is_healthy(&self) -> bool {
        self.worker.is_healthy()
    }

    pub(crate) async fn record_local_excluded(
        &self,
        digest: LedgerDigest,
    ) -> Result<(), LedgerError> {
        self.worker
            .record(digest, RecordState::LocalExcluded)
            .await
            .map(|_| ())
    }

    #[cfg(test)]
    async fn contains(&self, digest: LedgerDigest) -> Result<bool, LedgerError> {
        Ok(matches!(
            self.worker.lookup(digest).await?,
            Some(RecordState::LocalExcluded)
        ))
    }
}

/// Independently owned remote-delivery journal facade.
#[derive(Clone)]
pub(crate) struct DeliveryLedger {
    worker: Arc<Worker>,
}

impl DeliveryLedger {
    pub(crate) fn is_healthy(&self) -> bool {
        self.worker.is_healthy()
    }

    pub(crate) async fn check_and_reserve(
        &self,
        digest: LedgerDigest,
    ) -> Result<ReserveResult, LedgerError> {
        self.worker
            .record(digest, RecordState::Reserved)
            .await
            .map(|inserted| {
                if inserted {
                    ReserveResult::Reserved
                } else {
                    ReserveResult::Suppressed
                }
            })
    }

    pub(crate) async fn mark_completed(&self, digest: LedgerDigest) -> Result<(), LedgerError> {
        self.worker
            .record(digest, RecordState::Completed)
            .await
            .map(|_| ())
    }

    pub(crate) async fn is_suppressed(&self, digest: LedgerDigest) -> Result<bool, LedgerError> {
        Ok(matches!(
            self.worker.lookup(digest).await?,
            Some(RecordState::Reserved | RecordState::Completed)
        ))
    }
}

/// Root owner for both journal workers.
pub(crate) struct BridgeLedger {
    exclusion: LocalExclusionLedger,
    delivery: DeliveryLedger,
}

impl BridgeLedger {
    pub(crate) fn open(data_dir: &Path) -> Result<Self, LedgerOpenError> {
        let exclusion = Worker::spawn(data_dir, JournalKind::Exclusion)
            .map_err(|error| LedgerOpenError::Exclusion(error))?;
        let delivery = match Worker::spawn(data_dir, JournalKind::Delivery) {
            Ok(worker) => worker,
            Err(error) => {
                // A bad delivery artifact must not poison the independently
                // valid exclusion writer. Return both facts to startup.
                return Err(LedgerOpenError::Delivery {
                    error,
                    exclusion: LocalExclusionLedger { worker: exclusion },
                });
            }
        };
        Ok(Self {
            exclusion: LocalExclusionLedger { worker: exclusion },
            delivery: DeliveryLedger { worker: delivery },
        })
    }

    pub(crate) fn exclusion(&self) -> LocalExclusionLedger {
        self.exclusion.clone()
    }

    pub(crate) fn delivery(&self) -> DeliveryLedger {
        self.delivery.clone()
    }

    pub(crate) fn request_stop(&self) {
        self.exclusion.worker.request_stop();
        self.delivery.worker.request_stop();
    }

    pub(crate) fn join(&self) -> Result<(), LedgerError> {
        self.exclusion.worker.join()?;
        self.delivery.worker.join()
    }
}

/// Startup preserves the usable exclusion writer when only delivery is bad.
pub(crate) enum LedgerOpenError {
    Exclusion(LedgerError),
    Delivery {
        error: LedgerError,
        exclusion: LocalExclusionLedger,
    },
}

impl fmt::Debug for LedgerOpenError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Exclusion(error) => f.debug_tuple("Exclusion").field(error).finish(),
            Self::Delivery { error, .. } => f
                .debug_struct("Delivery")
                .field("error", error)
                .field("exclusion", &"healthy writer preserved")
                .finish(),
        }
    }
}

struct Journal {
    kind: JournalKind,
    file: File,
    dir: AnchoredDir,
    sequence: u64,
    records: usize,
    index: HashMap<LedgerDigest, RecordState>,
}

impl Journal {
    fn open(data_dir: &Path, kind: JournalKind) -> Result<Self, LedgerError> {
        let dir = AnchoredDir::open(data_dir)?;
        let mut file = dir.open_active(kind.file_name())?;
        let len = file.metadata().map_err(|_| LedgerError::Unavailable)?.len();
        if len == 0 {
            write_header(&mut file, kind)?;
        } else if len > MAX_FILE_BYTES {
            return Err(LedgerError::Capacity);
        }
        let (sequence, records, index) = recover(&mut file, kind)?;
        Ok(Self {
            kind,
            file,
            dir,
            sequence,
            records,
            index,
        })
    }

    fn record(&mut self, digest: LedgerDigest, state: RecordState) -> Result<bool, LedgerError> {
        if !self.kind.permits(state) {
            return Err(LedgerError::Corrupt);
        }
        match (self.index.get(&digest).copied(), state) {
            (Some(RecordState::LocalExcluded), RecordState::LocalExcluded)
            | (Some(RecordState::Reserved | RecordState::Completed), RecordState::Reserved)
            | (Some(RecordState::Completed), RecordState::Completed) => return Ok(false),
            (Some(RecordState::Reserved), RecordState::Completed) | (None, _) => {}
            _ => return Err(LedgerError::Corrupt),
        }
        if self.index.len() >= MAX_UNIQUE_ENTRIES && !self.index.contains_key(&digest) {
            return Err(LedgerError::Capacity);
        }
        if self.records >= MAX_RECORDS {
            self.compact()?;
        }
        self.sequence = self.sequence.checked_add(1).ok_or(LedgerError::Capacity)?;
        let record = encode_record(self.sequence, state, digest);
        self.file
            .seek(SeekFrom::End(0))
            .and_then(|_| self.file.write_all(&record))
            .and_then(|_| self.file.sync_all())
            .map_err(|_| LedgerError::Unavailable)?;
        self.index.insert(digest, state);
        self.records += 1;
        Ok(true)
    }

    fn compact(&mut self) -> Result<(), LedgerError> {
        let mut temp = self.dir.create_temp(self.kind.file_name())?;
        write_header(&mut temp, self.kind)?;
        let mut entries: Vec<_> = self.index.iter().map(|(k, v)| (*k, *v)).collect();
        entries.sort_unstable_by_key(|(digest, _)| digest.0);
        let mut sequence = 0_u64;
        for (digest, state) in entries {
            sequence += 1;
            temp.write_all(&encode_record(sequence, state, digest))
                .map_err(|_| LedgerError::Unavailable)?;
        }
        temp.sync_all().map_err(|_| LedgerError::Unavailable)?;
        drop(temp);
        self.dir.replace_from_temp(self.kind.file_name())?;
        self.file = self.dir.open_active(self.kind.file_name())?;
        self.sequence = sequence;
        self.records = self.index.len();
        Ok(())
    }
}

fn write_header(file: &mut File, kind: JournalKind) -> Result<(), LedgerError> {
    let mut header = [0_u8; HEADER_LEN];
    header[..8].copy_from_slice(MAGIC);
    header[8..12].copy_from_slice(&VERSION.to_be_bytes());
    header[12..16].copy_from_slice(&(RECORD_LEN as u32).to_be_bytes());
    header[16] = kind as u8;
    file.write_all(&header)
        .and_then(|_| file.sync_all())
        .map_err(|_| LedgerError::Unavailable)
}

fn recover(
    file: &mut File,
    kind: JournalKind,
) -> Result<(u64, usize, HashMap<LedgerDigest, RecordState>), LedgerError> {
    file.seek(SeekFrom::Start(0))
        .map_err(|_| LedgerError::Unavailable)?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)
        .map_err(|_| LedgerError::Unavailable)?;
    if bytes.len() < HEADER_LEN
        || &bytes[..8] != MAGIC
        || u32::from_be_bytes(bytes[8..12].try_into().unwrap()) != VERSION
        || u32::from_be_bytes(bytes[12..16].try_into().unwrap()) != RECORD_LEN as u32
        || bytes[16] != kind as u8
        || bytes[17..HEADER_LEN].iter().any(|byte| *byte != 0)
    {
        return Err(LedgerError::Corrupt);
    }
    let body_len = bytes.len() - HEADER_LEN;
    let complete_len = body_len / RECORD_LEN * RECORD_LEN;
    let mut index = HashMap::new();
    let mut expected_sequence = 1_u64;
    for chunk in bytes[HEADER_LEN..HEADER_LEN + complete_len].chunks_exact(RECORD_LEN) {
        let (sequence, state, digest) = decode_record(chunk)?;
        if sequence != expected_sequence || !kind.permits(state) {
            return Err(LedgerError::Corrupt);
        }
        expected_sequence = expected_sequence
            .checked_add(1)
            .ok_or(LedgerError::Corrupt)?;
        match (index.get(&digest).copied(), state) {
            (None, RecordState::LocalExcluded | RecordState::Reserved)
            | (Some(RecordState::Reserved), RecordState::Completed) => {
                index.insert(digest, state);
            }
            _ => return Err(LedgerError::Corrupt),
        }
        if index.len() > MAX_UNIQUE_ENTRIES {
            return Err(LedgerError::Capacity);
        }
    }
    if complete_len != body_len {
        file.set_len((HEADER_LEN + complete_len) as u64)
            .and_then(|_| file.sync_all())
            .map_err(|_| LedgerError::Unavailable)?;
    }
    Ok((expected_sequence - 1, complete_len / RECORD_LEN, index))
}

fn encode_record(sequence: u64, state: RecordState, digest: LedgerDigest) -> [u8; RECORD_LEN] {
    let mut record = [0_u8; RECORD_LEN];
    record[..8].copy_from_slice(&sequence.to_be_bytes());
    record[8] = state as u8;
    record[16..48].copy_from_slice(&digest.0);
    let checksum = record_checksum(&record[..48]);
    record[48..].copy_from_slice(&checksum);
    record
}

fn decode_record(record: &[u8]) -> Result<(u64, RecordState, LedgerDigest), LedgerError> {
    if record.len() != RECORD_LEN || record[9..16].iter().any(|byte| *byte != 0) {
        return Err(LedgerError::Corrupt);
    }
    if record_checksum(&record[..48]).as_slice() != &record[48..] {
        return Err(LedgerError::Corrupt);
    }
    Ok((
        u64::from_be_bytes(record[..8].try_into().unwrap()),
        RecordState::decode(record[8])?,
        LedgerDigest(record[16..48].try_into().unwrap()),
    ))
}

fn record_checksum(fields: &[u8]) -> [u8; 32] {
    let mut checksum = Sha256::new();
    checksum.update((CHECKSUM_DOMAIN.len() as u64).to_be_bytes());
    checksum.update(CHECKSUM_DOMAIN);
    checksum.update(fields);
    checksum.finalize().into()
}

#[cfg(unix)]
struct AnchoredDir {
    fd: File,
}

#[cfg(unix)]
impl AnchoredDir {
    fn open(data_dir: &Path) -> Result<Self, LedgerError> {
        use std::ffi::CString;
        use std::os::fd::{AsRawFd, FromRawFd};
        use std::os::unix::ffi::OsStrExt;

        let mut absolute = if data_dir.is_absolute() {
            data_dir.to_path_buf()
        } else {
            std::env::current_dir()
                .map_err(|_| LedgerError::Unavailable)?
                .join(data_dir)
        };
        // macOS exposes `/var` as the platform-owned `/private/var` symlink.
        // Resolve that fixed alias without permitting a configured, mutable
        // symlink component to enter the anchored walk.
        #[cfg(target_os = "macos")]
        if let Ok(suffix) = absolute.strip_prefix("/var") {
            absolute = Path::new("/private/var").join(suffix);
        }
        let root = unsafe {
            libc::open(
                c"/".as_ptr(),
                libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC,
            )
        };
        if root < 0 {
            return Err(LedgerError::Unavailable);
        }
        let mut current = unsafe { File::from_raw_fd(root) };
        for component in absolute.components() {
            use std::path::Component;
            let Component::Normal(component) = component else {
                continue;
            };
            let component =
                CString::new(component.as_bytes()).map_err(|_| LedgerError::Unavailable)?;
            let next = unsafe {
                libc::openat(
                    current.as_raw_fd(),
                    component.as_ptr(),
                    libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
                )
            };
            if next < 0 {
                return Err(LedgerError::Unavailable);
            }
            current = unsafe { File::from_raw_fd(next) };
        }
        let child = CString::new(LEDGER_DIR).unwrap();
        let mut child_fd = unsafe {
            libc::openat(
                current.as_raw_fd(),
                child.as_ptr(),
                libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
            )
        };
        if child_fd < 0 {
            let made = unsafe { libc::mkdirat(current.as_raw_fd(), child.as_ptr(), 0o700) };
            if made < 0
                && std::io::Error::last_os_error().kind() != std::io::ErrorKind::AlreadyExists
            {
                return Err(LedgerError::Unavailable);
            }
            child_fd = unsafe {
                libc::openat(
                    current.as_raw_fd(),
                    child.as_ptr(),
                    libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
                )
            };
        }
        if child_fd < 0 {
            return Err(LedgerError::Unavailable);
        }
        Ok(Self {
            fd: unsafe { File::from_raw_fd(child_fd) },
        })
    }

    fn open_active(&self, name: &str) -> Result<File, LedgerError> {
        use std::ffi::CString;
        use std::os::fd::{AsRawFd, FromRawFd};
        let name = CString::new(name).unwrap();
        let fd = unsafe {
            libc::openat(
                self.fd.as_raw_fd(),
                name.as_ptr(),
                libc::O_RDWR | libc::O_CREAT | libc::O_NOFOLLOW | libc::O_CLOEXEC,
                0o600,
            )
        };
        if fd < 0 {
            return Err(LedgerError::Unavailable);
        }
        let file = unsafe { File::from_raw_fd(fd) };
        if !file
            .metadata()
            .map_err(|_| LedgerError::Unavailable)?
            .is_file()
        {
            return Err(LedgerError::Unavailable);
        }
        Ok(file)
    }

    fn create_temp(&self, active: &str) -> Result<File, LedgerError> {
        use std::ffi::CString;
        use std::os::fd::{AsRawFd, FromRawFd};
        let name = CString::new(format!(".{active}.compact")).unwrap();
        let fd = unsafe {
            libc::openat(
                self.fd.as_raw_fd(),
                name.as_ptr(),
                libc::O_RDWR | libc::O_CREAT | libc::O_EXCL | libc::O_NOFOLLOW | libc::O_CLOEXEC,
                0o600,
            )
        };
        if fd < 0 {
            return Err(LedgerError::Unavailable);
        }
        Ok(unsafe { File::from_raw_fd(fd) })
    }

    fn replace_from_temp(&self, active: &str) -> Result<(), LedgerError> {
        use std::ffi::CString;
        use std::os::fd::AsRawFd;
        let temp = CString::new(format!(".{active}.compact")).unwrap();
        let active = CString::new(active).unwrap();
        if unsafe {
            libc::renameat(
                self.fd.as_raw_fd(),
                temp.as_ptr(),
                self.fd.as_raw_fd(),
                active.as_ptr(),
            )
        } < 0
        {
            return Err(LedgerError::Unavailable);
        }
        self.fd.sync_all().map_err(|_| LedgerError::Unavailable)
    }
}

#[cfg(not(unix))]
struct AnchoredDir {
    path: PathBuf,
}

#[cfg(not(unix))]
impl AnchoredDir {
    fn open(data_dir: &Path) -> Result<Self, LedgerError> {
        let data_meta =
            std::fs::symlink_metadata(data_dir).map_err(|_| LedgerError::Unavailable)?;
        if !data_meta.is_dir() || data_meta.file_type().is_symlink() {
            return Err(LedgerError::Unavailable);
        }
        let path = data_dir.join(LEDGER_DIR);
        match std::fs::create_dir(&path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(_) => return Err(LedgerError::Unavailable),
        }
        let meta = std::fs::symlink_metadata(&path).map_err(|_| LedgerError::Unavailable)?;
        if !meta.is_dir() || meta.file_type().is_symlink() {
            return Err(LedgerError::Unavailable);
        }
        Ok(Self { path })
    }

    fn open_active(&self, name: &str) -> Result<File, LedgerError> {
        let path = self.path.join(name);
        if std::fs::symlink_metadata(&path)
            .is_ok_and(|meta| meta.file_type().is_symlink() || !meta.is_file())
        {
            return Err(LedgerError::Unavailable);
        }
        std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)
            .map_err(|_| LedgerError::Unavailable)
    }

    fn create_temp(&self, active: &str) -> Result<File, LedgerError> {
        std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(self.path.join(format!(".{active}.compact")))
            .map_err(|_| LedgerError::Unavailable)
    }

    fn replace_from_temp(&self, active: &str) -> Result<(), LedgerError> {
        std::fs::rename(
            self.path.join(format!(".{active}.compact")),
            self.path.join(active),
        )
        .map_err(|_| LedgerError::Unavailable)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ledger_path(dir: &tempfile::TempDir, name: &str) -> PathBuf {
        dir.path().join(LEDGER_DIR).join(name)
    }

    #[test]
    fn digest_is_stable_length_framed_and_fixed_width() {
        assert_eq!(document_digest("ab", "c"), document_digest("ab", "c"));
        assert_ne!(document_digest("ab", "c"), document_digest("a", "bc"));
        assert_eq!(std::mem::size_of::<LedgerDigest>(), 32);
    }

    #[tokio::test]
    async fn journals_are_independent_and_recover_terminal_states() {
        let dir = tempfile::tempdir().unwrap();
        let ledger = BridgeLedger::open(dir.path()).unwrap();
        let local = document_digest("frames", "local");
        let remote = document_digest("frames", "remote");
        ledger
            .exclusion()
            .record_local_excluded(local)
            .await
            .unwrap();
        assert_eq!(
            ledger.delivery().check_and_reserve(remote).await.unwrap(),
            ReserveResult::Reserved
        );
        ledger.delivery().mark_completed(remote).await.unwrap();
        ledger.request_stop();
        ledger.join().unwrap();

        let reopened = BridgeLedger::open(dir.path()).unwrap();
        assert!(reopened.exclusion().contains(local).await.unwrap());
        assert!(reopened.delivery().is_suppressed(remote).await.unwrap());
        assert_eq!(
            reopened.delivery().check_and_reserve(remote).await.unwrap(),
            ReserveResult::Suppressed
        );
        reopened.join().unwrap();
    }

    #[tokio::test]
    async fn torn_final_record_is_truncated_but_complete_corruption_is_preserved() {
        let dir = tempfile::tempdir().unwrap();
        let ledger = BridgeLedger::open(dir.path()).unwrap();
        ledger
            .exclusion()
            .record_local_excluded(document_digest("frames", "one"))
            .await
            .unwrap();
        ledger.join().unwrap();
        let path = ledger_path(&dir, EXCLUSION_FILE);
        let good_len = std::fs::metadata(&path).unwrap().len();
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap();
        file.write_all(&[7_u8; 17]).unwrap();
        file.sync_all().unwrap();
        let reopened = BridgeLedger::open(dir.path()).unwrap();
        assert_eq!(std::fs::metadata(&path).unwrap().len(), good_len);
        reopened.join().unwrap();

        let mut bytes = std::fs::read(&path).unwrap();
        bytes[HEADER_LEN + 20] ^= 0xff;
        std::fs::write(&path, &bytes).unwrap();
        let before = std::fs::read(&path).unwrap();
        assert!(matches!(
            BridgeLedger::open(dir.path()),
            Err(LedgerOpenError::Exclusion(LedgerError::Corrupt))
        ));
        assert_eq!(std::fs::read(&path).unwrap(), before);
    }

    #[tokio::test]
    async fn corrupt_delivery_preserves_a_usable_exclusion_worker() {
        let dir = tempfile::tempdir().unwrap();
        let ledger = BridgeLedger::open(dir.path()).unwrap();
        ledger.join().unwrap();
        let path = ledger_path(&dir, DELIVERY_FILE);
        let mut bytes = std::fs::read(&path).unwrap();
        bytes[8] ^= 1;
        std::fs::write(&path, bytes).unwrap();
        let Err(LedgerOpenError::Delivery { error, exclusion }) = BridgeLedger::open(dir.path())
        else {
            panic!("delivery corruption should preserve exclusion");
        };
        assert_eq!(error, LedgerError::Corrupt);
        exclusion
            .record_local_excluded(document_digest("frames", "still-local"))
            .await
            .unwrap();
        exclusion.worker.join().unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn symlinked_ledger_directory_and_active_file_are_rejected() {
        use std::os::unix::fs::symlink;
        let dir = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        symlink(outside.path(), dir.path().join(LEDGER_DIR)).unwrap();
        assert!(matches!(
            BridgeLedger::open(dir.path()),
            Err(LedgerOpenError::Exclusion(LedgerError::Unavailable))
        ));

        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join(LEDGER_DIR)).unwrap();
        symlink(
            outside.path().join("victim"),
            ledger_path(&dir, EXCLUSION_FILE),
        )
        .unwrap();
        assert!(matches!(
            BridgeLedger::open(dir.path()),
            Err(LedgerOpenError::Exclusion(LedgerError::Unavailable))
        ));
    }
}
