use std::{sync::Mutex, time::Duration};

use pyo3::{
    create_exception,
    exceptions::{
        PyAttributeError, PyKeyError, PyOverflowError, PyRuntimeError, PyTimeoutError, PyTypeError,
        PyValueError,
    },
    prelude::*,
    types::{PyBool, PyBytes, PyDict, PyFloat, PyInt, PyList, PyString, PyTuple},
};
use vector_runtime::{
    BlockingSourceHandle, Error, ExternalSendError, LogEvent, OwnedRuntime,
    Pipeline as RustPipeline, Status, TryEmitError, Value, MAX_BATCH_EVENTS,
};

create_exception!(vector_runtime, VectorRuntimeError, PyRuntimeError);
create_exception!(vector_runtime, ClosedError, VectorRuntimeError);
create_exception!(vector_runtime, DeliveryError, VectorRuntimeError);
create_exception!(vector_runtime, QueueFullError, VectorRuntimeError);

fn error(error: Error) -> PyErr {
    let message = error.to_string();
    match error {
        Error::Config(_) | Error::Encode(_) => PyValueError::new_err(message),
        Error::UnknownSource(_) => PyKeyError::new_err(message),
        Error::Timeout(_) => PyTimeoutError::new_err(message),
        Error::Delivery => DeliveryError::new_err(message),
        Error::Send(ExternalSendError::Closed) => ClosedError::new_err(message),
        Error::Send(ExternalSendError::TooLarge) => PyValueError::new_err(message),
        _ => VectorRuntimeError::new_err(message),
    }
}

fn try_error<T: std::fmt::Debug>(error: TryEmitError<T>) -> PyErr {
    match error {
        TryEmitError::Full(_) => QueueFullError::new_err(
            "external source queue is full; retry the original Python input",
        ),
        TryEmitError::Closed(_) => ClosedError::new_err("external source is closed"),
        TryEmitError::TooLarge(_) => PyValueError::new_err("batch exceeds MAX_BATCH_EVENTS"),
        TryEmitError::UnknownSource(name, _) => PyKeyError::new_err(name),
    }
}

fn duration(value: f64, name: &str) -> PyResult<Duration> {
    Duration::try_from_secs_f64(value).map_err(|_| {
        PyValueError::new_err(format!(
            "{name} must be finite, nonnegative and representable"
        ))
    })
}

fn value(input: &Bound<'_, PyAny>, depth: usize) -> PyResult<Value> {
    if depth > 64 {
        return Err(PyValueError::new_err(
            "event nesting exceeds 64 levels (possibly a cycle)",
        ));
    }
    if input.is_none() {
        return Ok(Value::Null);
    }
    // bool is a subclass of int in Python.
    if input.is_instance_of::<PyBool>() {
        return Ok(Value::Boolean(input.extract()?));
    }
    if input.is_instance_of::<PyInt>() {
        return input
            .extract::<i64>()
            .map(Value::Integer)
            .map_err(|_| PyOverflowError::new_err("event integers must fit signed 64 bits"));
    }
    if input.is_instance_of::<PyFloat>() {
        let number: f64 = input.extract()?;
        if !number.is_finite() {
            return Err(PyValueError::new_err("event floats must be finite"));
        }
        return Ok(Value::from(number));
    }
    if let Ok(string) = input.downcast::<PyString>() {
        return Ok(Value::from(string.extract::<String>()?));
    }
    if let Ok(bytes) = input.downcast::<PyBytes>() {
        return Ok(Value::Bytes(bytes.as_bytes().to_vec().into()));
    }
    if let Ok(dict) = input.downcast::<PyDict>() {
        let mut fields = vector_runtime::event::ObjectMap::new();
        for (key, item) in dict.iter() {
            let key = key
                .downcast::<PyString>()
                .map_err(|_| PyTypeError::new_err("event dictionary keys must be strings"))?;
            fields.insert(key.extract::<String>()?.into(), value(&item, depth + 1)?);
        }
        return Ok(Value::Object(fields));
    }
    if let Ok(items) = input.downcast::<PyList>() {
        return Ok(Value::Array(
            items
                .iter()
                .map(|item| value(&item, depth + 1))
                .collect::<PyResult<_>>()?,
        ));
    }
    if let Ok(items) = input.downcast::<PyTuple>() {
        return Ok(Value::Array(
            items
                .iter()
                .map(|item| value(&item, depth + 1))
                .collect::<PyResult<_>>()?,
        ));
    }
    Err(PyTypeError::new_err(
        "supported event values: None, bool, int, float, str, bytes, list, tuple, dict",
    ))
}

fn log(input: &Bound<'_, PyAny>) -> PyResult<LogEvent> {
    if input.is_instance_of::<PyDict>() {
        return Ok(LogEvent::from(value(input, 0)?));
    }
    let descriptor = input.getattr("__vector_event_fields__").map_err(|e| {
        if e.is_instance_of::<PyAttributeError>(input.py()) {
            PyTypeError::new_err("event must be a dict or an @external_event instance")
        } else {
            e
        }
    })?;
    let fields: Vec<(String, String)> = descriptor.extract()?;
    let mut object = vector_runtime::event::ObjectMap::new();
    for (attribute, key) in fields {
        let item = input.getattr(attribute.as_str())?;
        if object.insert(key.into(), value(&item, 1)?).is_some() {
            return Err(PyValueError::new_err("duplicate external event field name"));
        }
    }
    Ok(LogEvent::from(Value::Object(object)))
}

fn batch(input: &Bound<'_, PyAny>) -> PyResult<Vec<LogEvent>> {
    let len = if let Ok(items) = input.downcast::<PyList>() {
        items.len()
    } else if let Ok(items) = input.downcast::<PyTuple>() {
        items.len()
    } else {
        return Err(PyTypeError::new_err(
            "batch must be a list or tuple of events",
        ));
    };
    if len > MAX_BATCH_EVENTS {
        return Err(PyValueError::new_err("batch exceeds MAX_BATCH_EVENTS"));
    }
    input.try_iter()?.map(|item| log(&item?)).collect()
}

#[pyclass(name = "Pipeline", module = "vector_runtime", frozen)]
struct Pipeline {
    inner: Mutex<Option<RustPipeline>>,
    workers: usize,
}

#[pymethods]
impl Pipeline {
    #[new]
    #[pyo3(signature = (yaml, *, worker_threads=2, shutdown_timeout=60.0, flush_timeout=30.0))]
    fn new(
        py: Python<'_>,
        yaml: &str,
        worker_threads: usize,
        shutdown_timeout: f64,
        flush_timeout: f64,
    ) -> PyResult<Self> {
        if worker_threads == 0 {
            return Err(PyValueError::new_err("worker_threads must be positive"));
        }
        let shutdown = duration(shutdown_timeout, "shutdown_timeout")?;
        let flush = duration(flush_timeout, "flush_timeout")?;
        let pipeline = py
            .allow_threads(|| RustPipeline::from_yaml(yaml))
            .map_err(error)?
            .with_shutdown_timeout(shutdown)
            .with_flush_timeout(flush);
        Ok(Self {
            inner: Mutex::new(Some(pipeline)),
            workers: worker_threads,
        })
    }

    #[staticmethod]
    #[pyo3(signature = (yaml, *, worker_threads=2, shutdown_timeout=60.0, flush_timeout=30.0))]
    fn from_yaml(
        py: Python<'_>,
        yaml: &str,
        worker_threads: usize,
        shutdown_timeout: f64,
        flush_timeout: f64,
    ) -> PyResult<Self> {
        Self::new(py, yaml, worker_threads, shutdown_timeout, flush_timeout)
    }

    fn start(&self, py: Python<'_>) -> PyResult<Runtime> {
        py.allow_threads(|| {
            let pipeline = self
                .inner
                .lock()
                .unwrap()
                .take()
                .ok_or_else(|| ClosedError::new_err("pipeline has already been started"))?;
            let runtime = pipeline
                .start_owned_with_workers(self.workers)
                .map_err(error)?;
            Ok(Runtime {
                state: Mutex::new(RuntimeState {
                    inner: Some(runtime),
                    terminal: "running".into(),
                }),
            })
        })
    }
}

struct RuntimeState {
    inner: Option<OwnedRuntime>,
    terminal: String,
}

#[pyclass(module = "vector_runtime", frozen)]
struct Runtime {
    state: Mutex<RuntimeState>,
}

impl Runtime {
    fn get_source(&self, name: &str) -> PyResult<BlockingSourceHandle> {
        self.state
            .lock()
            .unwrap()
            .inner
            .as_ref()
            .ok_or_else(|| ClosedError::new_err("runtime is closed"))?
            .source(name)
            .map_err(error)
    }
}

#[pymethods]
impl Runtime {
    fn source(&self, py: Python<'_>, name: &str) -> PyResult<SourceHandle> {
        py.allow_threads(|| self.get_source(name).map(|inner| SourceHandle { inner }))
    }

    fn emit(&self, py: Python<'_>, source: &str, event: &Bound<'_, PyAny>) -> PyResult<()> {
        let log = log(event)?;
        py.allow_threads(|| self.get_source(source)?.emit(log).map_err(error))
    }

    fn emit_batch(&self, py: Python<'_>, source: &str, events: &Bound<'_, PyAny>) -> PyResult<()> {
        let logs = batch(events)?;
        py.allow_threads(|| self.get_source(source)?.emit_batch(logs).map_err(error))
    }

    fn emit_json(&self, py: Python<'_>, source: &str, json: &str) -> PyResult<()> {
        py.allow_threads(|| self.get_source(source)?.emit_json(json).map_err(error))
    }

    fn try_emit(&self, py: Python<'_>, source: &str, event: &Bound<'_, PyAny>) -> PyResult<()> {
        let log = log(event)?;
        py.allow_threads(|| self.get_source(source)?.try_emit(log).map_err(try_error))
    }

    fn try_emit_batch(
        &self,
        py: Python<'_>,
        source: &str,
        events: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let logs = batch(events)?;
        py.allow_threads(|| {
            self.get_source(source)?
                .try_emit_batch(logs)
                .map_err(try_error)
        })
    }

    fn flush(&self, py: Python<'_>) -> PyResult<()> {
        py.allow_threads(|| {
            self.state
                .lock()
                .unwrap()
                .inner
                .as_ref()
                .ok_or_else(|| ClosedError::new_err("runtime is closed"))?
                .flush()
                .map_err(error)
        })
    }

    fn stop(&self, py: Python<'_>) -> PyResult<()> {
        py.allow_threads(|| {
            let runtime = {
                let mut state = self.state.lock().unwrap();
                let Some(runtime) = state.inner.take() else {
                    return if state.terminal == "stopped" {
                        Ok(())
                    } else {
                        Err(ClosedError::new_err(format!(
                            "runtime is {}",
                            state.terminal
                        )))
                    };
                };
                state.terminal = "stopping".into();
                runtime
            };
            let result = runtime.stop().map_err(error);
            self.state.lock().unwrap().terminal =
                if result.is_ok() { "stopped" } else { "failed" }.into();
            result
        })
    }

    fn close(&self, py: Python<'_>) -> PyResult<()> {
        self.stop(py)
    }

    #[getter]
    fn status(&self, py: Python<'_>) -> String {
        py.allow_threads(|| {
            let state = self.state.lock().unwrap();
            match state.inner.as_ref().map(OwnedRuntime::status) {
                Some(Status::Running) => "running".into(),
                Some(Status::Stopping) => "stopping".into(),
                Some(Status::Stopped) => "stopped".into(),
                Some(Status::Failed(message)) => format!("failed: {message}"),
                None => state.terminal.clone(),
            }
        })
    }

    #[getter]
    fn warnings(&self, py: Python<'_>) -> Vec<String> {
        py.allow_threads(|| {
            self.state
                .lock()
                .unwrap()
                .inner
                .as_ref()
                .map(|runtime| runtime.warnings().to_vec())
                .unwrap_or_default()
        })
    }

    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __exit__(
        &self,
        py: Python<'_>,
        exc_type: &Bound<'_, PyAny>,
        _exc: &Bound<'_, PyAny>,
        _tb: &Bound<'_, PyAny>,
    ) -> PyResult<bool> {
        let result = self.stop(py);
        // Preserve an exception already raised in the with block.
        if exc_type.is_none() {
            result?;
        }
        Ok(false)
    }
}

#[pyclass(module = "vector_runtime", frozen)]
struct SourceHandle {
    inner: BlockingSourceHandle,
}

#[pymethods]
impl SourceHandle {
    fn emit(&self, py: Python<'_>, event: &Bound<'_, PyAny>) -> PyResult<()> {
        let log = log(event)?;
        let mut source = self.inner.clone();
        py.allow_threads(move || source.emit(log).map_err(error))
    }

    fn emit_batch(&self, py: Python<'_>, events: &Bound<'_, PyAny>) -> PyResult<()> {
        let logs = batch(events)?;
        let mut source = self.inner.clone();
        py.allow_threads(move || source.emit_batch(logs).map_err(error))
    }

    fn emit_json(&self, py: Python<'_>, json: &str) -> PyResult<()> {
        let mut source = self.inner.clone();
        py.allow_threads(move || source.emit_json(json).map_err(error))
    }

    fn try_emit(&self, py: Python<'_>, event: &Bound<'_, PyAny>) -> PyResult<()> {
        let log = log(event)?;
        let mut source = self.inner.clone();
        py.allow_threads(move || source.try_emit(log).map_err(try_error))
    }

    fn try_emit_batch(&self, py: Python<'_>, events: &Bound<'_, PyAny>) -> PyResult<()> {
        let logs = batch(events)?;
        let mut source = self.inner.clone();
        py.allow_threads(move || source.try_emit_batch(logs).map_err(try_error))
    }
}

#[pymodule]
fn _native(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<Pipeline>()?;
    module.add_class::<Runtime>()?;
    module.add_class::<SourceHandle>()?;
    let py = module.py();
    module.add("VectorRuntimeError", py.get_type::<VectorRuntimeError>())?;
    module.add("ClosedError", py.get_type::<ClosedError>())?;
    module.add("DeliveryError", py.get_type::<DeliveryError>())?;
    module.add("QueueFullError", py.get_type::<QueueFullError>())?;
    module.add("MAX_BATCH_EVENTS", MAX_BATCH_EVENTS)?;
    module.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}
