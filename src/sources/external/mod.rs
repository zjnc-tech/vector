//! In-process ingress for native `LogEvent`s. Typed struct conversion belongs to
//! the embedding API, not to this source. No extra forwarding queue is created.
use std::{
    collections::HashMap,
    fmt,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
};

use chrono::Utc;
use tokio::sync::Notify;
use tracing::{Instrument, Span};
use vector_lib::{
    buffers::topology::channel::LimitedTrySendError,
    codecs::NativeDeserializerConfig,
    config::LogNamespace,
    configurable::configurable_component,
    event::{EventArray, LogEvent},
    internal_event::{CountByteSize, EventsReceived, InternalEventHandle as _, Registered},
    EstimatedJsonEncodedSizeOf,
};

use crate::{
    config::{DataType, SourceConfig, SourceContext, SourceOutput},
    source_sender::CHUNK_SIZE,
    SourceSender,
};

/// Maximum events per submission. Larger batches must be split by the API layer.
/// This bounds batch count, not bytes; the embedding API must bound payload sizes.
pub const MAX_BATCH_EVENTS: usize = CHUNK_SIZE;

/// Configuration for a source supplied with logs by an embedding application.
#[configurable_component(source(
    "external",
    "Receive native log events from an embedding application."
))]
#[derive(Clone, Debug, Default)]
#[serde(deny_unknown_fields)]
pub struct ExternalSourceConfig {}

impl_generate_config_from_default!(ExternalSourceConfig);

/// Per-runtime registry. Pass a clone through `ExtraContext::single_value` before
/// building the topology, then look up handles after a successful build/start.
#[derive(Clone, Default)]
pub struct ExternalSourceRegistry {
    entries: Arc<Mutex<HashMap<String, ExternalSourceHandle>>>,
}

impl ExternalSourceRegistry {
    /// Stop admitting submissions to every registered source. Already queued
    /// events are drained by the topology's normal shutdown procedure.
    pub fn close(&self) {
        for handle in self
            .entries
            .lock()
            .expect("external registry lock poisoned")
            .values()
        {
            handle.state.begin_close();
        }
    }

    /// Resolve a source once; subsequent writes do not lock or consult this map.
    pub fn get(&self, name: &str) -> Option<ExternalSourceHandle> {
        self.entries
            .lock()
            .expect("external registry lock poisoned")
            .get(name)
            .cloned()
    }
}

/// A producer-local handle. Clone once per producer rather than locking a shared
/// handle across `.await`. Methods accept only native logs and log batches.
#[derive(Clone)]
pub struct ExternalSourceHandle {
    sender: SourceSender,
    state: Arc<IngressState>,
    log_namespace: LogNamespace,
    events_received: Registered<EventsReceived>,
    span: Span,
}

/// Errors from an asynchronous submission. Success means enqueued, not delivered
/// to a sink. Cancelling the future may discard a batch that has not been queued.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExternalSendError {
    Closed,
    TooLarge,
}

impl fmt::Display for ExternalSendError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Closed => "external source is closed",
            Self::TooLarge => "external source batch exceeds MAX_BATCH_EVENTS",
        })
    }
}

impl std::error::Error for ExternalSendError {}

/// Nonblocking failures retain ownership for retry. Metadata may already have
/// been attached; no events from a failed submission were queued.
#[derive(Debug)]
pub enum ExternalTrySendError<T> {
    Full(T),
    Closed(T),
    TooLarge(T),
}

impl<T> ExternalTrySendError<T> {
    pub fn into_inner(self) -> T {
        match self {
            Self::Full(value) | Self::Closed(value) | Self::TooLarge(value) => value,
        }
    }

    fn map<U>(self, f: impl FnOnce(T) -> U) -> ExternalTrySendError<U> {
        match self {
            Self::Full(value) => ExternalTrySendError::Full(f(value)),
            Self::Closed(value) => ExternalTrySendError::Closed(f(value)),
            Self::TooLarge(value) => ExternalTrySendError::TooLarge(f(value)),
        }
    }
}

impl<T> fmt::Display for ExternalTrySendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Full(_) => "external source queue is full",
            Self::Closed(_) => "external source is closed",
            Self::TooLarge(_) => "external source batch exceeds MAX_BATCH_EVENTS",
        })
    }
}

impl<T: fmt::Debug> std::error::Error for ExternalTrySendError<T> {}

impl ExternalSourceHandle {
    pub async fn emit(&mut self, log: LogEvent) -> Result<(), ExternalSendError> {
        self.emit_batch(vec![log]).await
    }

    /// Submit a homogeneous batch without rebuilding it through `Vec<Event>`.
    pub async fn emit_batch(&mut self, mut logs: Vec<LogEvent>) -> Result<(), ExternalSendError> {
        let span = self.span.clone();
        async {
            let _flight = self.state.enter()?;
            if logs.len() > MAX_BATCH_EVENTS {
                return Err(ExternalSendError::TooLarge);
            }
            if logs.is_empty() {
                return Ok(());
            }
            let count_byte_size = self.prepare(&mut logs);
            // Closing cancels sends still waiting for capacity. Already enqueued
            // batches remain in the source pump queue and are drained normally.
            tokio::select! {
                biased;
                _ = self.state.wait_closing() => Err(ExternalSendError::Closed),
                result = self.sender.send_event(EventArray::Logs(logs)) => {
                    result.map_err(|_| {
                        emit!(crate::internal_events::StreamClosedError { count: count_byte_size.0 });
                        ExternalSendError::Closed
                    })?;
                    self.events_received.emit(count_byte_size);
                    Ok(())
                }
            }
        }
        .instrument(span)
        .await
    }

    pub fn try_emit(&mut self, log: LogEvent) -> Result<(), ExternalTrySendError<LogEvent>> {
        self.try_emit_batch(vec![log])
            .map_err(|error| error.map(|mut logs| logs.pop().expect("one submitted log")))
    }

    /// Atomically submit a batch, or return the entire batch without waiting.
    pub fn try_emit_batch(
        &mut self,
        mut logs: Vec<LogEvent>,
    ) -> Result<(), ExternalTrySendError<Vec<LogEvent>>> {
        let span = self.span.clone();
        let _entered = span.enter();
        let _flight = match self.state.enter() {
            Ok(flight) => flight,
            Err(_) => return Err(ExternalTrySendError::Closed(logs)),
        };
        if logs.len() > MAX_BATCH_EVENTS {
            return Err(ExternalTrySendError::TooLarge(logs));
        }
        if logs.is_empty() {
            return Ok(());
        }
        let count_byte_size = self.prepare(&mut logs);
        self.sender
            .try_send_array(EventArray::Logs(logs))
            .map_err(|error| match error {
                LimitedTrySendError::InsufficientCapacity(events) => {
                    ExternalTrySendError::Full(unwrap_logs(events))
                }
                LimitedTrySendError::Disconnected(events) => {
                    ExternalTrySendError::Closed(unwrap_logs(events))
                }
            })?;
        self.events_received.emit(count_byte_size);
        Ok(())
    }

    fn prepare(&self, logs: &mut [LogEvent]) -> CountByteSize {
        let now = Utc::now();
        for log in logs.iter_mut() {
            self.log_namespace.insert_standard_vector_source_metadata(
                log,
                ExternalSourceConfig::NAME,
                now,
            );
        }
        CountByteSize(
            logs.len(),
            logs.iter()
                .map(LogEvent::estimated_json_encoded_size_of)
                .sum(),
        )
    }
}

fn unwrap_logs(events: EventArray) -> Vec<LogEvent> {
    match events {
        EventArray::Logs(logs) => logs,
        _ => unreachable!("external source submits only logs"),
    }
}

const CLOSING: usize = 1 << (usize::BITS - 1);
const ACTIVE_MASK: usize = CLOSING - 1;

struct IngressState {
    // Admission and in-flight count share one atomic to avoid a check/close race.
    status: AtomicUsize,
    finished: AtomicBool,
    changed: Notify,
    sender: SourceSender,
}

impl IngressState {
    fn enter(self: &Arc<Self>) -> Result<InFlight, ExternalSendError> {
        self.status
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |status| {
                if status & CLOSING != 0 || status & ACTIVE_MASK == ACTIVE_MASK {
                    None
                } else {
                    Some(status + 1)
                }
            })
            .map_err(|_| ExternalSendError::Closed)?;
        Ok(InFlight(Arc::clone(self)))
    }

    fn begin_close(&self) {
        let old = self.status.fetch_or(CLOSING, Ordering::AcqRel);
        self.changed.notify_waiters();
        if old & ACTIVE_MASK == 0 {
            self.finish_close();
        }
    }

    fn finish_close(&self) {
        // All sends are gone and admission is closed. Retained host handles no
        // longer keep the pump alive, and no send can enqueue after it exits.
        self.sender.close_when_idle();
        self.finished.store(true, Ordering::Release);
        self.changed.notify_waiters();
    }

    async fn wait_closing(&self) {
        loop {
            let notified = self.changed.notified();
            if self.status.load(Ordering::Acquire) & CLOSING != 0 {
                return;
            }
            notified.await;
        }
    }

    async fn wait_finished(&self) {
        loop {
            let notified = self.changed.notified();
            if self.finished.load(Ordering::Acquire) {
                return;
            }
            notified.await;
        }
    }
}

struct InFlight(Arc<IngressState>);

impl Drop for InFlight {
    fn drop(&mut self) {
        let old = self.0.status.fetch_sub(1, Ordering::AcqRel);
        if old & CLOSING != 0 && old & ACTIVE_MASK == 1 {
            self.0.finish_close();
        }
    }
}

struct Registration {
    registry: ExternalSourceRegistry,
    key: String,
    state: Arc<IngressState>,
}

impl Drop for Registration {
    fn drop(&mut self) {
        self.state.begin_close();
        let mut entries = self
            .registry
            .entries
            .lock()
            .expect("external registry lock poisoned");
        if entries
            .get(&self.key)
            .is_some_and(|entry| Arc::ptr_eq(&entry.state, &self.state))
        {
            entries.remove(&self.key);
        }
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "external")]
impl SourceConfig for ExternalSourceConfig {
    async fn build(&self, cx: SourceContext) -> crate::Result<super::Source> {
        let registry = cx
            .extra_context
            .get::<ExternalSourceRegistry>()
            .ok_or("external source requires ExternalSourceRegistry in ExtraContext")?
            .clone();
        let log_namespace = cx.log_namespace(None);
        let key = cx.key.id().to_owned();
        let state = Arc::new(IngressState {
            status: AtomicUsize::new(0),
            finished: AtomicBool::new(false),
            changed: Notify::new(),
            sender: cx.out.clone(),
        });
        let handle = ExternalSourceHandle {
            sender: cx.out,
            state: Arc::clone(&state),
            log_namespace,
            events_received: register!(EventsReceived),
            span: Span::current(),
        };
        {
            let mut entries = registry
                .entries
                .lock()
                .expect("external registry lock poisoned");
            if entries.contains_key(&key) {
                return Err(format!("external source {key:?} is already registered").into());
            }
            entries.insert(key.clone(), handle);
        }
        // Capture the guard before constructing the future so an unpolled future
        // dropped during a failed topology build also unregisters and closes.
        let registration = Registration {
            registry,
            key,
            state,
        };
        Ok(Box::pin(async move {
            let _shutdown_complete = cx.shutdown.await;
            registration.state.begin_close();
            registration.state.wait_finished().await;
            drop(registration);
            Ok(())
        }))
    }

    fn outputs(&self, log_namespace: LogNamespace) -> Vec<SourceOutput> {
        vec![SourceOutput::new_maybe_logs(
            DataType::Log,
            NativeDeserializerConfig
                .schema_definition(log_namespace)
                .with_standard_vector_source_metadata(),
        )]
    }

    fn can_acknowledge(&self) -> bool {
        // The embedding API attaches finalizers and exposes completion via flush.
        // Direct submissions still promise enqueue completion only.
        true
    }
}
