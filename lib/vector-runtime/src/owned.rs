//! Synchronous facade backed by a privately owned Tokio runtime.
use std::{fmt::Display, future::Future};

use crate::{
    Error, Event, LogEvent, Pipeline, RuntimeHandle, SourceHandle, Status, TryEmitError, Value,
};

// Runtime::drop blocks and panics in an async context. Always use the explicit
// nonblocking shutdown operation, including startup errors and unwinding.
struct Executor(Option<tokio::runtime::Runtime>);

impl Executor {
    fn runtime(&self) -> &tokio::runtime::Runtime {
        self.0.as_ref().unwrap()
    }
}

impl Drop for Executor {
    fn drop(&mut self) {
        if let Some(runtime) = self.0.take() {
            runtime.shutdown_background();
        }
    }
}

fn check_blocking_context() -> Result<(), Error> {
    if tokio::runtime::Handle::try_current().is_ok() {
        Err(Error::BlockingInAsyncContext)
    } else {
        Ok(())
    }
}

impl Pipeline {
    /// Start on a private multithreaded Tokio runtime from synchronous code.
    /// Requires the `owned-runtime` feature. Inside async code use `start().await`.
    pub fn start_owned(self) -> Result<OwnedRuntime, Error> {
        self.start_owned_with_workers(2)
    }

    /// Like `start_owned`, with an explicit positive worker thread count.
    pub fn start_owned_with_workers(self, worker_threads: usize) -> Result<OwnedRuntime, Error> {
        check_blocking_context()?;
        if worker_threads == 0 {
            return Err(Error::Config(
                "worker_threads must be greater than zero".into(),
            ));
        }
        let executor = Executor(Some(
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(worker_threads)
                .thread_name("vector-runtime")
                .enable_all()
                .build()
                .map_err(|e| Error::Start(e.to_string()))?,
        ));
        let inner = executor.runtime().block_on(self.start())?;
        Ok(OwnedRuntime {
            inner: Some(inner),
            executor: Some(executor),
        })
    }
}

/// A synchronous pipeline with its own Tokio worker threads.
///
/// Blocking operations must run outside an entered Tokio runtime. `stop` waits for draining. Drop requests shutdown and
/// drains on a cleanup thread; it does not block the caller. The process must
/// stay alive for background cleanup, so prefer explicit `stop` before exit.
pub struct OwnedRuntime {
    inner: Option<RuntimeHandle>,
    executor: Option<Executor>,
}

impl OwnedRuntime {
    fn inner(&self) -> &RuntimeHandle {
        self.inner.as_ref().unwrap()
    }

    fn block_on<T>(&self, future: impl Future<Output = Result<T, Error>>) -> Result<T, Error> {
        check_blocking_context()?;
        self.executor.as_ref().unwrap().runtime().block_on(future)
    }

    /// Resolve a producer handle. It does not keep the pipeline alive after stop/drop.
    pub fn source(&self, name: &str) -> Result<BlockingSourceHandle, Error> {
        Ok(BlockingSourceHandle {
            inner: self.inner().source(name)?,
            executor: self.executor.as_ref().unwrap().runtime().handle().clone(),
        })
    }

    pub fn status(&self) -> Status {
        self.inner().status()
    }
    pub fn warnings(&self) -> &[String] {
        self.inner().warnings()
    }

    pub fn emit<E>(&self, source: &str, event: E) -> Result<(), Error>
    where
        E: TryInto<Event>,
        E::Error: Display,
    {
        self.block_on(self.inner().emit(source, event))
    }

    pub fn emit_batch(&self, source: &str, logs: Vec<LogEvent>) -> Result<(), Error> {
        self.block_on(self.inner().emit_batch(source, logs))
    }

    pub fn emit_value(&self, source: &str, value: Value) -> Result<(), Error> {
        self.block_on(self.inner().emit_value(source, value))
    }

    pub fn emit_json(&self, source: &str, json: &str) -> Result<(), Error> {
        self.block_on(self.inner().emit_json(source, json))
    }

    pub fn emit_serializable<T: serde::Serialize>(
        &self,
        source: &str,
        value: &T,
    ) -> Result<(), Error> {
        self.block_on(self.inner().emit_serializable(source, value))
    }

    /// Nonblocking methods may also be called from async code.
    pub fn try_emit(&self, source: &str, log: LogEvent) -> Result<(), TryEmitError<LogEvent>> {
        self.inner().try_emit(source, log)
    }

    pub fn try_emit_batch(
        &self,
        source: &str,
        logs: Vec<LogEvent>,
    ) -> Result<(), TryEmitError<Vec<LogEvent>>> {
        self.inner().try_emit_batch(source, logs)
    }

    pub fn flush(&self) -> Result<(), Error> {
        self.block_on(self.inner().flush())
    }

    /// Drain the pipeline before shutting down its private executor.
    /// Calling from an async context returns an error and requests background shutdown.
    pub fn stop(mut self) -> Result<(), Error> {
        check_blocking_context()?;
        let inner = self.inner.take().unwrap();
        self.executor
            .as_ref()
            .unwrap()
            .runtime()
            .block_on(inner.stop())
    }
}

impl Drop for OwnedRuntime {
    fn drop(&mut self) {
        if let Some(mut inner) = self.inner.take() {
            inner.request_stop();
            let executor = self.executor.take().unwrap();
            // Executor's drop is safe even if spawning fails and the closure is dropped.
            let _ = std::thread::Builder::new()
                .name("vector-runtime-shutdown".into())
                .spawn(move || {
                    let _ = executor.runtime().block_on(inner.stop());
                });
        }
    }
}

/// Producer-local synchronous handle. Clone once for each producer thread.
#[derive(Clone)]
pub struct BlockingSourceHandle {
    inner: SourceHandle,
    executor: tokio::runtime::Handle,
}

impl BlockingSourceHandle {
    pub fn emit<E>(&mut self, event: E) -> Result<(), Error>
    where
        E: TryInto<Event>,
        E::Error: Display,
    {
        check_blocking_context()?;
        self.executor.block_on(self.inner.emit(event))
    }

    pub fn emit_batch(&mut self, logs: Vec<LogEvent>) -> Result<(), Error> {
        check_blocking_context()?;
        self.executor.block_on(self.inner.emit_batch(logs))
    }

    pub fn emit_value(&mut self, value: Value) -> Result<(), Error> {
        check_blocking_context()?;
        self.executor.block_on(self.inner.emit_value(value))
    }

    pub fn emit_json(&mut self, json: &str) -> Result<(), Error> {
        check_blocking_context()?;
        self.executor.block_on(self.inner.emit_json(json))
    }

    pub fn emit_serializable<T: serde::Serialize>(&mut self, value: &T) -> Result<(), Error> {
        check_blocking_context()?;
        self.executor.block_on(self.inner.emit_serializable(value))
    }

    pub fn try_emit(&mut self, log: LogEvent) -> Result<(), TryEmitError<LogEvent>> {
        self.inner.try_emit(log)
    }

    pub fn try_emit_batch(
        &mut self,
        logs: Vec<LogEvent>,
    ) -> Result<(), TryEmitError<Vec<LogEvent>>> {
        self.inner.try_emit_batch(logs)
    }
}
