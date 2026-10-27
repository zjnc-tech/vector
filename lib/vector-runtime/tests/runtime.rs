#![cfg(feature = "clickhouse")]

use hyper::{
    service::{make_service_fn, service_fn},
    Body, Response, Server, StatusCode,
};
use std::{convert::Infallible, sync::Arc, time::Duration};
use tokio::sync::{mpsc, Semaphore};
use vector_runtime::{
    event::{BatchNotifier, BatchStatus, EventFinalizer},
    Error, ExternalSourceEvent, LogEvent, Pipeline, Status, TryEmitError, Value,
};

struct Mock {
    endpoint: String,
    received: mpsc::UnboundedReceiver<Vec<u8>>,
    gate: Arc<Semaphore>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for Mock {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn mock(status: StatusCode) -> Mock {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let (tx, received) = mpsc::unbounded_channel();
    let gate = Arc::new(Semaphore::new(0));
    let server_gate = gate.clone();
    let service = make_service_fn(move |_| {
        let (tx, gate) = (tx.clone(), server_gate.clone());
        async move {
            Ok::<_, Infallible>(service_fn(move |request| {
                let (tx, gate) = (tx.clone(), gate.clone());
                async move {
                    let body = hyper::body::to_bytes(request.into_body()).await.unwrap();
                    let _ = tx.send(body.to_vec());
                    gate.acquire().await.unwrap().forget();
                    Ok::<_, Infallible>(
                        Response::builder()
                            .status(status)
                            .body(Body::empty())
                            .unwrap(),
                    )
                }
            }))
        }
    });
    let server = Server::from_tcp(listener).unwrap().serve(service);
    let task = tokio::spawn(async move {
        server.await.unwrap();
    });
    Mock {
        endpoint,
        received,
        gate,
        task,
    }
}

fn config(mock: &Mock) -> String {
    format!(
        r#"
healthchecks:
  enabled: false
sources:
  input:
    type: external
transforms:
  decorate:
    type: remap
    inputs: [input]
    source: '.processed = true'
sinks:
  output:
    type: clickhouse
    inputs: [decorate]
    endpoint: "{}"
    table: events
    compression: none
    batch:
      max_events: 1
      timeout_secs: 0.01
"#,
        mock.endpoint
    )
}

#[derive(ExternalSourceEvent)]
struct MusaMallocEvent {
    pid: u32,
    size: u64,
}

#[tokio::test]
async fn typed_native_json_and_batches_reach_sink_and_flush_waits_for_response() {
    let mut mock = mock(StatusCode::OK);
    let runtime = Pipeline::from_yaml(&config(&mock))
        .unwrap()
        .start()
        .await
        .unwrap();
    assert_eq!(runtime.status(), Status::Running);
    runtime
        .emit(
            "input",
            MusaMallocEvent {
                pid: 12,
                size: 4096,
            },
        )
        .await
        .unwrap();
    let body = tokio::time::timeout(Duration::from_secs(5), mock.received.recv())
        .await
        .unwrap()
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["pid"], 12);
    assert_eq!(json["size"], 4096);
    assert_eq!(json["processed"], true);
    assert!(
        tokio::time::timeout(Duration::from_millis(50), runtime.flush())
            .await
            .is_err()
    );
    mock.gate.add_permits(100);
    runtime.flush().await.unwrap();
    runtime
        .emit_json("input", r#"{"kind":"json"}"#)
        .await
        .unwrap();
    runtime
        .emit_value(
            "input",
            Value::Object(
                [("kind".into(), Value::from("value"))]
                    .into_iter()
                    .collect(),
            ),
        )
        .await
        .unwrap();
    runtime
        .emit_batch("input", vec![new_log("one"), new_log("two")])
        .await
        .unwrap();
    runtime.try_emit("input", new_log("try")).unwrap();
    runtime.flush().await.unwrap();
    for _ in 0..5 {
        tokio::time::timeout(Duration::from_secs(5), mock.received.recv())
            .await
            .unwrap()
            .unwrap();
    }
    let mut retained = runtime.source("input").unwrap();
    runtime.stop().await.unwrap();
    assert!(matches!(
        retained.try_emit(new_log("late")),
        Err(TryEmitError::Closed(_))
    ));
    assert!(retained.emit(new_log("late")).await.is_err());
}

#[tokio::test]
async fn rejected_batches_are_reported_by_flush_and_stop() {
    let mock = mock(StatusCode::BAD_REQUEST);
    mock.gate.add_permits(10);
    let runtime = Pipeline::from_yaml(&config(&mock))
        .unwrap()
        .start()
        .await
        .unwrap();
    runtime.emit("input", new_log("reject")).await.unwrap();
    assert!(matches!(runtime.flush().await, Err(Error::Delivery)));
    assert!(matches!(runtime.flush().await, Err(Error::Delivery)));
    assert!(matches!(runtime.stop().await, Err(Error::Delivery)));
}

#[tokio::test]
async fn deadline_closes_retained_handles() {
    let mut mock = mock(StatusCode::OK);
    let runtime = Pipeline::from_yaml(&config(&mock))
        .unwrap()
        .with_shutdown_timeout(Duration::from_millis(50))
        .with_flush_timeout(Duration::from_millis(50))
        .start()
        .await
        .unwrap();
    let mut source = runtime.source("input").unwrap();
    source.emit(new_log("blocked response")).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), mock.received.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        runtime.flush().await,
        Err(Error::Timeout("flush"))
    ));
    assert!(matches!(
        runtime.stop().await,
        Err(Error::Timeout("shutdown"))
    ));
    assert!(matches!(
        source.try_emit(new_log("late")),
        Err(TryEmitError::Closed(_))
    ));
}

#[tokio::test]
async fn dropping_runtime_closes_ingress_and_drains_in_background() {
    let mut mock = mock(StatusCode::OK);
    mock.gate.add_permits(10);
    let runtime = Pipeline::from_yaml(&config(&mock))
        .unwrap()
        .start()
        .await
        .unwrap();
    let mut source = runtime.source("input").unwrap();
    let (batch, receipt) = BatchNotifier::new_with_receiver();
    let mut log = new_log("drain on drop");
    log.add_finalizer(EventFinalizer::new(batch));
    source.emit(log).await.unwrap();
    drop(runtime);
    assert!(matches!(
        source.try_emit(new_log("late")),
        Err(TryEmitError::Closed(_))
    ));
    tokio::time::timeout(Duration::from_secs(5), mock.received.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), receipt)
            .await
            .unwrap(),
        BatchStatus::Delivered
    );
}

#[tokio::test]
async fn separate_runtimes_do_not_share_source_registry() {
    let mock_a = mock(StatusCode::OK);
    let mock_b = mock(StatusCode::OK);
    mock_a.gate.add_permits(10);
    mock_b.gate.add_permits(10);
    let a = Pipeline::from_yaml(&config(&mock_a))
        .unwrap()
        .start()
        .await
        .unwrap();
    let b = Pipeline::from_yaml(&config(&mock_b))
        .unwrap()
        .start()
        .await
        .unwrap();
    a.stop().await.unwrap();
    b.emit("input", new_log("still running")).await.unwrap();
    b.flush().await.unwrap();
    b.stop().await.unwrap();
}

#[tokio::test]
async fn errors_preserve_ownership_and_do_not_leave_pending_receipts() {
    let mock = mock(StatusCode::OK);
    let runtime = Pipeline::from_yaml(&config(&mock))
        .unwrap()
        .start()
        .await
        .unwrap();
    let log = new_log("retry");
    assert!(matches!(
        runtime.try_emit("missing", log),
        Err(TryEmitError::UnknownSource(_, _))
    ));
    assert!(matches!(
        runtime
            .emit(
                "input",
                MusaMallocEvent {
                    pid: 1,
                    size: u64::MAX
                }
            )
            .await,
        Err(Error::Encode(_))
    ));
    assert!(matches!(
        runtime
            .emit_json("input", r#"{"n":18446744073709551615}"#)
            .await,
        Err(Error::Encode(_))
    ));
    assert!(matches!(
        runtime.emit_json("input", "[]").await,
        Err(Error::Encode(_))
    ));
    let mut source = runtime.source("input").unwrap();
    let logs = (0..=vector_runtime::MAX_BATCH_EVENTS)
        .map(|_| new_log("too large"))
        .collect();
    assert!(matches!(
        source.try_emit_batch(logs),
        Err(TryEmitError::TooLarge(_))
    ));
    runtime.flush().await.unwrap();
    runtime.stop().await.unwrap();
    let (batch, mut receipt) = BatchNotifier::new_with_receiver();
    let mut log = new_log("returned");
    log.add_finalizer(EventFinalizer::new(batch));
    let returned = source.try_emit(log).unwrap_err().into_inner();
    assert!(receipt.try_recv().is_err());
    drop(returned);
    assert_eq!(receipt.await, BatchStatus::Delivered);
}

#[tokio::test]
async fn invalid_configuration_is_reported() {
    assert!(Pipeline::from_yaml("sources: {in: {type: stdin}}").is_err());
    assert!(Pipeline::from_yaml("sinks: {out: {type: file}}").is_err());
    assert!(Pipeline::from_yaml("provider: {type: http}").is_err());
    let mock = mock(StatusCode::OK);
    let missing = config(&mock).replace("inputs: [input]", "inputs: [missing]");
    assert!(matches!(
        Pipeline::from_yaml(&missing).unwrap().start().await,
        Err(Error::Config(_))
    ));
    let bad_vrl = config(&mock).replace(".processed = true", "this is invalid vrl !!");
    assert!(matches!(
        Pipeline::from_yaml(&bad_vrl).unwrap().start().await,
        Err(Error::Start(_))
    ));
    // Failed builds release registrations and do not poison a later start.
    Pipeline::from_yaml(&config(&mock))
        .unwrap()
        .start()
        .await
        .unwrap()
        .stop()
        .await
        .unwrap();
}

fn new_log(message: &str) -> LogEvent {
    LogEvent::from(Value::Object(
        [("message".into(), Value::from(message))]
            .into_iter()
            .collect(),
    ))
}

#[cfg(feature = "owned-runtime")]
#[test]
fn owned_runtime_works_without_host_executor_and_across_threads() {
    // Only the mock server uses this separate executor; client calls below are
    // plain synchronous calls with no entered Tokio context.
    let server_runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let mock = {
        let _guard = server_runtime.enter();
        mock(StatusCode::OK)
    };
    mock.gate.add_permits(100);
    let runtime = Pipeline::from_yaml(&config(&mock))
        .unwrap()
        .start_owned()
        .unwrap();
    runtime
        .emit("input", MusaMallocEvent { pid: 2, size: 8 })
        .unwrap();
    let mut retained = runtime.source("input").unwrap();
    let mut producer = retained.clone();
    std::thread::spawn(move || producer.emit_json(r#"{"pid":3,"size":9}"#).unwrap())
        .join()
        .unwrap();
    runtime.flush().unwrap();
    runtime.stop().unwrap();
    assert!(matches!(
        retained.try_emit(new_log("closed")),
        Err(TryEmitError::Closed(_))
    ));
    assert!(matches!(
        retained.emit(new_log("closed")),
        Err(Error::Send(_))
    ));
}

#[cfg(feature = "owned-runtime")]
#[tokio::test]
async fn owned_runtime_rejects_nested_blocking_and_can_drop_in_async_context() {
    let mock = mock(StatusCode::OK);
    let yaml = config(&mock);
    assert!(matches!(
        Pipeline::from_yaml(&yaml).unwrap().start_owned(),
        Err(Error::BlockingInAsyncContext)
    ));
    let runtime =
        std::thread::spawn(move || Pipeline::from_yaml(&yaml).unwrap().start_owned().unwrap())
            .join()
            .unwrap();
    assert!(matches!(
        runtime.flush(),
        Err(Error::BlockingInAsyncContext)
    ));
    let mut source = runtime.source("input").unwrap();
    drop(runtime); // Must not panic with Tokio's "cannot drop a runtime" error.
    assert!(matches!(
        source.try_emit(new_log("closed")),
        Err(TryEmitError::Closed(_))
    ));
}

#[cfg(feature = "owned-runtime")]
#[test]
fn owned_runtime_shutdown_deadline_is_enforced() {
    let server_runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let mut mock = {
        let _guard = server_runtime.enter();
        mock(StatusCode::OK)
    };
    let runtime = Pipeline::from_yaml(&config(&mock))
        .unwrap()
        .with_shutdown_timeout(Duration::from_millis(50))
        .start_owned()
        .unwrap();
    runtime.emit("input", new_log("no response")).unwrap();
    server_runtime.block_on(async {
        tokio::time::timeout(Duration::from_secs(5), mock.received.recv())
            .await
            .unwrap()
            .unwrap();
    });
    assert!(matches!(runtime.stop(), Err(Error::Timeout("shutdown"))));
}

#[cfg(feature = "owned-runtime")]
#[test]
fn owned_drop_keeps_executor_alive_until_response_and_rejects_invalid_workers() {
    let server_runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let mut mock = {
        let _guard = server_runtime.enter();
        mock(StatusCode::OK)
    };
    let yaml = config(&mock);
    assert!(matches!(
        Pipeline::from_yaml(&yaml)
            .unwrap()
            .start_owned_with_workers(0),
        Err(Error::Config(_))
    ));
    let runtime = Pipeline::from_yaml(&yaml)
        .unwrap()
        .start_owned_with_workers(1)
        .unwrap();
    let (tx, rx) = std::sync::mpsc::channel();
    let batch = BatchNotifier::new_with_callback(move |status| {
        let _ = tx.send(status);
    });
    let mut log = new_log("drain with private executor");
    log.add_finalizer(EventFinalizer::new(batch));
    runtime.emit("input", log).unwrap();
    drop(runtime);
    let body = server_runtime.block_on(async {
        tokio::time::timeout(Duration::from_secs(5), mock.received.recv())
            .await
            .unwrap()
            .unwrap()
    });
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&body).unwrap()["message"],
        "drain with private executor"
    );
    assert!(matches!(
        rx.try_recv(),
        Err(std::sync::mpsc::TryRecvError::Empty)
    ));
    mock.gate.add_permits(1);
    assert_eq!(
        rx.recv_timeout(Duration::from_secs(5)).unwrap(),
        BatchStatus::Delivered
    );
}
