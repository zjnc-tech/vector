//! Embedded Vector pipelines with a host Tokio runtime or the `owned-runtime` feature.
//!
//! `emit` acknowledges admission; `flush` waits for Vector event finalization.
//! Install a tracing subscriber in the host if component diagnostics are needed.
extern crate self as vector_runtime;

#[cfg(feature = "owned-runtime")]
mod owned;
mod tracker;
#[cfg(feature = "owned-runtime")]
pub use owned::{BlockingSourceHandle, OwnedRuntime};

use std::{
    collections::HashMap,
    fmt::Display,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{sync::oneshot, task::JoinHandle};
use vector::{
    config::{self, ConfigBuilder, ConfigDiff},
    extra_context::ExtraContext,
    sources::external::{ExternalSourceHandle, ExternalSourceRegistry},
    topology::{RunningTopology, TopologyPieces},
};

pub use event::external::{EncodeError, EncodeErrorKind, IntoExternalValue};
pub use event::{Event, LogEvent, Value};
use tracker::Tracker;
pub use vector::event;
pub use vector::sources::external::{ExternalSendError, ExternalTrySendError, MAX_BATCH_EVENTS};
pub use vector_external_macros::ExternalSourceEvent;

/// Nonblocking admission errors retain the native payload for retry.
#[derive(Debug, thiserror::Error)]
pub enum TryEmitError<T: std::fmt::Debug> {
    #[error("unknown external source: {0}")]
    UnknownSource(String, T),
    #[error("external source queue is full")]
    Full(T),
    #[error("external source is closed")]
    Closed(T),
    #[error("batch exceeds MAX_BATCH_EVENTS")]
    TooLarge(T),
}

impl<T: std::fmt::Debug> TryEmitError<T> {
    pub fn into_inner(self) -> T {
        match self {
            Self::UnknownSource(_, value)
            | Self::Full(value)
            | Self::Closed(value)
            | Self::TooLarge(value) => value,
        }
    }
}

/// Errors from configuration, encoding, admission, or lifecycle operations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("blocking API called inside Tokio; use the async API or call from a plain OS thread")]
    BlockingInAsyncContext,
    #[error("invalid configuration: {0}")]
    Config(String),
    #[error("could not start pipeline: {0}")]
    Start(String),
    #[error("unknown external source: {0}")]
    UnknownSource(String),
    #[error("event encoding failed: {0}")]
    Encode(String),
    #[error(transparent)]
    Send(#[from] ExternalSendError),
    #[error("at least one submitted batch was rejected or errored")]
    Delivery,
    #[error("{0} timed out")]
    Timeout(&'static str),
    #[error("pipeline failed: {0}")]
    Failed(String),
}

/// Lifecycle state, shared by the runtime and its producer handles.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Status {
    Running,
    Stopping,
    Stopped,
    Failed(String),
}

/// Parsed pipeline configuration. Starting consumes this value.
pub struct Pipeline {
    builder: ConfigBuilder,
    shutdown_timeout: Duration,
    flush_timeout: Duration,
}

impl Pipeline {
    /// Parse a single YAML document. Environment interpolation and config
    /// providers are deliberately left to the host application.
    pub fn from_yaml(yaml: &str) -> Result<Self, Error> {
        let value: serde_yaml::Value =
            serde_yaml::from_str(yaml).map_err(|e| Error::Config(e.to_string()))?;
        validate_components(&value)?;
        let builder: ConfigBuilder =
            serde_yaml::from_value(value).map_err(|e| Error::Config(e.to_string()))?;
        Ok(Self {
            builder,
            shutdown_timeout: Duration::from_secs(60),
            flush_timeout: Duration::from_secs(30),
        })
    }

    /// Maximum duration for draining components before aborting their tasks.
    pub fn with_shutdown_timeout(mut self, timeout: Duration) -> Self {
        self.shutdown_timeout = timeout;
        self
    }

    /// Maximum duration of each `RuntimeHandle::flush` call.
    pub fn with_flush_timeout(mut self, timeout: Duration) -> Self {
        self.flush_timeout = timeout;
        self
    }

    /// Build and start the topology. Requires a Tokio runtime with I/O and time enabled.
    /// Does not install signal handlers, a tracing subscriber, or a Tokio runtime.
    pub async fn start(self) -> Result<RuntimeHandle, Error> {
        tokio::runtime::Handle::try_current().map_err(|e| Error::Start(e.to_string()))?;
        initialize_globals(&self.builder)?;
        let (mut config, warnings) = self
            .builder
            .build_with_warnings()
            .map_err(|errors| Error::Config(errors.join("\n")))?;
        // Our supervisor owns the deadline and explicitly aborts overdue tasks.
        config.graceful_shutdown_duration = None;
        let registry = ExternalSourceRegistry::default();
        let diff = ConfigDiff::initial(&config);
        let pieces = TopologyPieces::build(
            &config,
            &diff,
            HashMap::new(),
            ExtraContext::single_value(registry.clone()),
        )
        .await
        .map_err(|errors| Error::Start(errors.join("\n")))?;
        let (topology, mut errors) = RunningTopology::start_validated(config, diff, pieces)
            .await
            .ok_or_else(|| {
                Error::Start(
                    "healthcheck or metrics configuration failed; see tracing diagnostics".into(),
                )
            })?;
        let aborts = AbortTasks(topology.abort_handles());
        let shared = Arc::new(Shared {
            status: Mutex::new(Status::Running),
            tracker: Arc::default(),
        });
        let (stop_tx, stop_rx) = oneshot::channel();
        let worker_shared = shared.clone();
        let worker_registry = registry.clone();
        let supervisor = tokio::spawn(async move {
            let _aborts = aborts;
            let failure = tokio::select! {
                _ = stop_rx => None,
                error = errors.recv() => Some(error.map_or_else(|| "component monitor closed".to_owned(), |e| e.to_string())),
            };
            worker_registry.close();
            *worker_shared.status.lock().unwrap() = Status::Stopping;
            let result = match tokio::time::timeout(self.shutdown_timeout, topology.stop()).await {
                Err(_) => Err(Error::Timeout("shutdown")),
                Ok(()) => match failure.or_else(|| errors.try_recv().ok().map(|e| e.to_string())) {
                    Some(error) => Err(Error::Failed(error)),
                    None => Ok(()),
                },
            };
            *worker_shared.status.lock().unwrap() = match &result {
                Ok(()) => Status::Stopped,
                Err(error) => Status::Failed(error.to_string()),
            };
            result
        });
        Ok(RuntimeHandle {
            registry,
            shared,
            stop_tx: Some(stop_tx),
            supervisor: Some(supervisor),
            flush_timeout: self.flush_timeout,
            warnings,
        })
    }
}

struct AbortTasks(Vec<tokio::task::AbortHandle>);
impl Drop for AbortTasks {
    fn drop(&mut self) {
        for handle in &self.0 {
            handle.abort();
        }
    }
}

struct Shared {
    status: Mutex<Status>,
    tracker: Arc<Tracker>,
}

/// Owns the pipeline lifecycle. Dropping requests shutdown in the background;
/// call `stop().await` to wait for completion and observe errors.
pub struct RuntimeHandle {
    registry: ExternalSourceRegistry,
    shared: Arc<Shared>,
    stop_tx: Option<oneshot::Sender<()>>,
    supervisor: Option<JoinHandle<Result<(), Error>>>,
    flush_timeout: Duration,
    warnings: Vec<String>,
}

impl RuntimeHandle {
    /// Resolve once per producer to avoid registry lookup on the hot path.
    pub fn source(&self, name: &str) -> Result<SourceHandle, Error> {
        self.registry
            .get(name)
            .map(|inner| SourceHandle {
                inner,
                shared: self.shared.clone(),
            })
            .ok_or_else(|| Error::UnknownSource(name.to_owned()))
    }

    pub fn status(&self) -> Status {
        self.shared.status.lock().unwrap().clone()
    }
    pub fn warnings(&self) -> &[String] {
        &self.warnings
    }

    /// Accept a native log/Event or a type deriving `ExternalSourceEvent`.
    pub async fn emit<E>(&self, source: &str, event: E) -> Result<(), Error>
    where
        E: TryInto<Event>,
        E::Error: Display,
    {
        self.source(source)?.emit(event).await
    }

    /// Submit a native batch without serializing or rebuilding its Vec.
    pub async fn emit_batch(&self, source: &str, logs: Vec<LogEvent>) -> Result<(), Error> {
        self.source(source)?.emit_batch(logs).await
    }

    pub fn try_emit(&self, source: &str, log: LogEvent) -> Result<(), TryEmitError<LogEvent>> {
        match self.source(source) {
            Ok(mut handle) => handle.try_emit(log),
            Err(_) => Err(TryEmitError::UnknownSource(source.to_owned(), log)),
        }
    }

    pub fn try_emit_batch(
        &self,
        source: &str,
        logs: Vec<LogEvent>,
    ) -> Result<(), TryEmitError<Vec<LogEvent>>> {
        match self.source(source) {
            Ok(mut handle) => handle.try_emit_batch(logs),
            Err(_) => Err(TryEmitError::UnknownSource(source.to_owned(), logs)),
        }
    }

    pub async fn emit_serializable<T: serde::Serialize>(
        &self,
        source: &str,
        value: &T,
    ) -> Result<(), Error> {
        self.source(source)?.emit_serializable(value).await
    }

    pub async fn emit_value(&self, source: &str, value: Value) -> Result<(), Error> {
        self.source(source)?.emit_value(value).await
    }

    pub async fn emit_json(&self, source: &str, json: &str) -> Result<(), Error> {
        self.source(source)?.emit_json(json).await
    }

    /// Wait for finalization of submissions registered before this call is polled.
    /// Filtered events count as complete. This does not force aggregate timers,
    /// remote durability, or a Prometheus scrape. Delivery errors remain sticky.
    pub async fn flush(&self) -> Result<(), Error> {
        tokio::time::timeout(self.flush_timeout, self.shared.tracker.flush())
            .await
            .map_err(|_| Error::Timeout("flush"))??;
        match self.status() {
            Status::Failed(error) => Err(Error::Failed(error)),
            _ => Ok(()),
        }
    }

    fn request_stop(&mut self) {
        self.registry.close();
        let mut status = self.shared.status.lock().unwrap();
        if *status == Status::Running {
            *status = Status::Stopping;
        }
        if let Some(tx) = self.stop_tx.take() {
            let _ = tx.send(());
        }
    }

    /// Reject new submissions, drain the topology, and wait for shutdown.
    pub async fn stop(mut self) -> Result<(), Error> {
        self.request_stop();
        self.supervisor
            .take()
            .unwrap()
            .await
            .map_err(|e| Error::Failed(e.to_string()))??;
        tokio::time::timeout(self.flush_timeout, self.shared.tracker.flush())
            .await
            .map_err(|_| Error::Timeout("finalization after shutdown"))?
    }
}

impl Drop for RuntimeHandle {
    fn drop(&mut self) {
        self.request_stop();
    }
}

/// Clone once per producer; methods use `&mut self` without a shared async mutex.
#[derive(Clone)]
pub struct SourceHandle {
    inner: ExternalSourceHandle,
    shared: Arc<Shared>,
}

impl SourceHandle {
    pub async fn emit<E>(&mut self, event: E) -> Result<(), Error>
    where
        E: TryInto<Event>,
        E::Error: Display,
    {
        let event = event.try_into().map_err(|e| Error::Encode(e.to_string()))?;
        match event {
            Event::Log(log) => self.emit_batch(vec![log]).await,
            _ => Err(Error::Encode(
                "external sources accept only log events".into(),
            )),
        }
    }

    pub async fn emit_batch(&mut self, mut logs: Vec<LogEvent>) -> Result<(), Error> {
        if logs.len() > MAX_BATCH_EVENTS {
            return Err(ExternalSendError::TooLarge.into());
        }
        self.shared.tracker.attach(&mut logs);
        self.inner.emit_batch(logs).await.map_err(Into::into)
    }

    /// On backpressure, return the original native log for retry.
    pub fn try_emit(&mut self, log: LogEvent) -> Result<(), TryEmitError<LogEvent>> {
        self.try_emit_batch(vec![log]).map_err(|error| match error {
            TryEmitError::Full(mut logs) => TryEmitError::Full(logs.remove(0)),
            TryEmitError::Closed(mut logs) => TryEmitError::Closed(logs.remove(0)),
            TryEmitError::TooLarge(mut logs) => TryEmitError::TooLarge(logs.remove(0)),
            TryEmitError::UnknownSource(name, mut logs) => {
                TryEmitError::UnknownSource(name, logs.remove(0))
            }
        })
    }

    pub fn try_emit_batch(
        &mut self,
        mut logs: Vec<LogEvent>,
    ) -> Result<(), TryEmitError<Vec<LogEvent>>> {
        if logs.len() > MAX_BATCH_EVENTS {
            return Err(TryEmitError::TooLarge(logs));
        }
        let original: Vec<_> = logs
            .iter()
            .map(|log| log.metadata().finalizers().clone())
            .collect();
        self.shared.tracker.attach(&mut logs);
        self.inner.try_emit_batch(logs).map_err(|error| {
            let restore = |mut logs: Vec<LogEvent>| {
                for (log, finalizers) in logs.iter_mut().zip(original) {
                    drop(log.metadata_mut().take_finalizers());
                    log.metadata_mut().merge_finalizers(finalizers);
                }
                logs
            };
            match error {
                ExternalTrySendError::Full(logs) => TryEmitError::Full(restore(logs)),
                ExternalTrySendError::Closed(logs) => TryEmitError::Closed(restore(logs)),
                ExternalTrySendError::TooLarge(logs) => TryEmitError::TooLarge(restore(logs)),
            }
        })
    }

    pub async fn emit_value(&mut self, value: Value) -> Result<(), Error> {
        if !matches!(value, Value::Object(_)) {
            return Err(Error::Encode("event root must be an object".into()));
        }
        self.emit(LogEvent::from(value)).await
    }

    /// JSON is a convenience path; prefer native logs or the derive for hot paths.
    pub async fn emit_json(&mut self, json: &str) -> Result<(), Error> {
        let value = serde_json::from_str(json).map_err(|e| Error::Encode(e.to_string()))?;
        self.emit_value(json_value(value)?).await
    }

    /// Convenience adapter using an intermediate JSON value. The derive avoids it.
    pub async fn emit_serializable<T: serde::Serialize>(&mut self, value: &T) -> Result<(), Error> {
        let value = serde_json::to_value(value).map_err(|e| Error::Encode(e.to_string()))?;
        self.emit_value(json_value(value)?).await
    }
}

fn json_value(value: serde_json::Value) -> Result<Value, Error> {
    use serde_json::Value as J;
    Ok(match value {
        J::Null => Value::Null,
        J::Bool(value) => Value::Boolean(value),
        J::String(value) => Value::from(value),
        J::Number(value) => {
            if let Some(value) = value.as_i64() {
                Value::Integer(value)
            } else if value.is_u64() {
                return Err(Error::Encode("integer exceeds i64::MAX".into()));
            } else {
                Value::from(
                    value
                        .as_f64()
                        .ok_or_else(|| Error::Encode("invalid float".into()))?,
                )
            }
        }
        J::Array(values) => Value::Array(
            values
                .into_iter()
                .map(json_value)
                .collect::<Result<_, _>>()?,
        ),
        J::Object(values) => Value::Object(
            values
                .into_iter()
                .map(|(key, value)| Ok((key.into(), json_value(value)?)))
                .collect::<Result<_, Error>>()?,
        ),
    })
}

fn validate_components(value: &serde_yaml::Value) -> Result<(), Error> {
    for (section, allowed) in [
        ("sources", &["external"][..]),
        (
            "sinks",
            &[
                "console",
                "clickhouse",
                "kafka",
                "vector",
                "prometheus_exporter",
                "prometheus_remote_write",
            ][..],
        ),
    ] {
        if let Some(components) = value.get(section).and_then(serde_yaml::Value::as_mapping) {
            for (name, component) in components {
                let kind = component
                    .get("type")
                    .and_then(serde_yaml::Value::as_str)
                    .unwrap_or("");
                if !allowed.contains(&kind) {
                    return Err(Error::Config(format!(
                        "unsupported {section} component {name:?}: {kind}"
                    )));
                }
            }
        }
    }
    for section in ["provider", "secret", "enrichment_tables", "api"] {
        if value.get(section).is_some() {
            return Err(Error::Config(format!(
                "{section} is not supported by the embedded runtime"
            )));
        }
    }
    Ok(())
}

fn initialize_globals(builder: &ConfigBuilder) -> Result<(), Error> {
    static LOCK: Mutex<()> = Mutex::new(());
    let _lock = LOCK.lock().unwrap();
    // These settings belong to Vector's process-wide singletons. Keep every
    // embedded pipeline on the default schema/telemetry/expiry policy.
    let defaults = ConfigBuilder::default();
    if builder.global.log_schema != defaults.global.log_schema
        || builder.global.telemetry != defaults.global.telemetry
        || builder.global.expire_metrics.is_some()
        || builder.global.expire_metrics_secs.is_some()
        || builder.global.expire_metrics_per_metric_set.is_some()
    {
        return Err(Error::Config(
            "embedded pipelines require default log_schema, telemetry and metrics expiry settings"
                .into(),
        ));
    }
    config::init_log_schema(defaults.global.log_schema.clone(), false);
    config::init_telemetry(defaults.global.telemetry.clone(), false);
    if config::log_schema() != &defaults.global.log_schema
        || config::telemetry().to_owned() != defaults.global.telemetry
    {
        return Err(Error::Start(
            "process-wide Vector schema or telemetry is incompatible".into(),
        ));
    }
    if vector::metrics::Controller::get().is_err() {
        vector::metrics::init_global()
            .map_err(|e| Error::Start(format!("metrics initialization: {e}")))?;
    }
    vector::metrics::Controller::get()
        .map_err(|e| Error::Start(format!("metrics controller unavailable: {e}")))?;
    Ok(())
}
