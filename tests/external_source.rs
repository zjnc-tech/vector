#![cfg(all(feature = "sources-external", feature = "test-utils"))]

use std::time::Duration;

use futures::poll;
use tokio::time::timeout;
use vector::{
    config::{ComponentKey, SourceConfig, SourceContext},
    event::{EventArray, LogEvent, Value},
    extra_context::ExtraContext,
    shutdown::SourceShutdownCoordinator,
    source_sender::SourceSenderItem,
    sources::{
        external::{
            ExternalSendError, ExternalSourceConfig, ExternalSourceRegistry, ExternalTrySendError,
            MAX_BATCH_EVENTS,
        },
        Source,
    },
    SourceSender,
};
use vector_lib::buffers::topology::channel::LimitedReceiver;

const DEADLINE: Duration = Duration::from_secs(5);

#[test]
fn external_config_is_registered_and_rejects_unknown_options() {
    let config: Box<dyn SourceConfig> =
        serde_json::from_value(serde_json::json!({"type": "external"})).unwrap();
    assert!(config.can_acknowledge());
    assert!(serde_json::from_value::<Box<dyn SourceConfig>>(
        serde_json::json!({"type": "external", "unexpected": true})
    )
    .is_err());
}

fn log(id: i64) -> LogEvent {
    let mut log = LogEvent::default();
    log.insert("id", id);
    log
}

async fn build(
    name: &str,
    capacity: usize,
    registry: &ExternalSourceRegistry,
    vector_namespace: bool,
) -> (
    Source,
    SourceShutdownCoordinator,
    LimitedReceiver<SourceSenderItem>,
) {
    let (sender, receiver) = SourceSender::new_test_sender_with_buffer(capacity);
    let (mut cx, shutdown) = SourceContext::new_shutdown(&ComponentKey::from(name), sender);
    cx.extra_context = ExtraContext::single_value(registry.clone());
    cx.schema.log_namespace = Some(vector_namespace);
    let source = ExternalSourceConfig {}.build(cx).await.unwrap();
    (source, shutdown, receiver)
}

#[tokio::test]
async fn registers_native_logs_and_preserves_batch_storage() {
    let registry = ExternalSourceRegistry::default();
    let (source, _, mut rx) = build("malloc", 8, &registry, true).await;
    assert!(registry.get("missing").is_none());
    let mut handle = registry.get("malloc").unwrap();
    let batch = vec![log(1), log(2)];
    let storage = batch.as_ptr();
    handle.emit_batch(batch).await.unwrap();
    let EventArray::Logs(logs) = rx.next().await.unwrap().events else {
        panic!("expected logs")
    };
    assert_eq!(logs.as_ptr(), storage);
    assert_eq!(logs[0].get("id"), Some(&Value::Integer(1)));
    assert_eq!(logs[1].get("id"), Some(&Value::Integer(2)));
    assert_eq!(
        logs[0].get(vrl::metadata_path!("vector", "source_type")),
        Some(&Value::from("external"))
    );
    assert!(matches!(
        logs[0].get(vrl::metadata_path!("vector", "ingest_timestamp")),
        Some(Value::Timestamp(_))
    ));
    assert!(logs[0].get("source_type").is_none());

    // Dropping an unpolled source also cleans up registration and closes handles.
    drop(source);
    assert!(registry.get("malloc").is_none());
    assert!(matches!(
        handle.try_emit(log(3)),
        Err(ExternalTrySendError::Closed(_))
    ));
    assert!(timeout(DEADLINE, rx.next()).await.unwrap().is_none());
}

#[tokio::test]
async fn full_queue_returns_whole_batch_and_empty_batch_uses_no_capacity() {
    let registry = ExternalSourceRegistry::default();
    let (_source, _, mut rx) = build("malloc", 2, &registry, false).await;
    let mut handle = registry.get("malloc").unwrap();
    handle.try_emit(log(1)).unwrap();

    let error = handle.try_emit_batch(vec![log(2), log(3)]).unwrap_err();
    assert!(matches!(error, ExternalTrySendError::Full(_)));
    let logs = error.into_inner();
    assert_eq!(logs.len(), 2);
    assert_eq!(logs[0].get("id"), Some(&Value::Integer(2)));

    for _ in 0..10 {
        handle.try_emit_batch(vec![]).unwrap();
    }
    assert_eq!(rx.available_capacity(), 1);
    rx.next().await.unwrap();
    handle.try_emit_batch(logs).unwrap();
    let EventArray::Logs(logs) = rx.next().await.unwrap().events else {
        panic!("expected logs")
    };
    assert_eq!(logs.len(), 2);
    assert_eq!(logs[0].get("source_type"), Some(&Value::from("external")));

    let oversized = vec![log(0); MAX_BATCH_EVENTS + 1];
    assert!(matches!(
        handle.try_emit_batch(oversized),
        Err(ExternalTrySendError::TooLarge(_))
    ));
    assert_eq!(rx.available_capacity(), 2);
}

#[tokio::test]
async fn asynchronous_send_obeys_backpressure() {
    let registry = ExternalSourceRegistry::default();
    let (_source, _, mut rx) = build("malloc", 1, &registry, false).await;
    let mut handle = registry.get("malloc").unwrap();
    handle.emit(log(1)).await.unwrap();
    let send = handle.emit(log(2));
    tokio::pin!(send);
    assert!(poll!(send.as_mut()).is_pending());
    rx.next().await.unwrap();
    timeout(DEADLINE, send).await.unwrap().unwrap();
    let EventArray::Logs(logs) = rx.next().await.unwrap().events else {
        panic!("expected logs")
    };
    assert_eq!(logs[0].get("id"), Some(&Value::Integer(2)));
}

#[tokio::test]
async fn shutdown_cancels_blocked_sends_and_drains_with_handles_retained() {
    let registry = ExternalSourceRegistry::default();
    let (source, shutdown, mut rx) = build("malloc", 1, &registry, false).await;
    let mut handle = registry.get("malloc").unwrap();
    handle.emit(log(1)).await.unwrap();
    let mut producer = handle.clone();
    let send = producer.emit(log(2));
    tokio::pin!(send);
    assert!(poll!(send.as_mut()).is_pending());
    let task = tokio::spawn(source);
    let (sent, ()) = timeout(DEADLINE, async {
        tokio::join!(send, shutdown.shutdown_all(None))
    })
    .await
    .unwrap();
    assert_eq!(sent, Err(ExternalSendError::Closed));
    task.await.unwrap().unwrap();
    assert!(registry.get("malloc").is_none());
    assert!(matches!(
        handle.try_emit(log(3)),
        Err(ExternalTrySendError::Closed(_))
    ));
    let EventArray::Logs(logs) = rx.next().await.unwrap().events else {
        panic!("expected logs")
    };
    assert_eq!(logs[0].get("id"), Some(&Value::Integer(1)));
    assert!(timeout(DEADLINE, rx.next()).await.unwrap().is_none());
}

#[tokio::test]
async fn missing_registry_is_a_build_error() {
    let (sender, _rx) = SourceSender::new_test_sender_with_buffer(2);
    let cx = SourceContext::new_test(sender, None);
    let error = ExternalSourceConfig {}.build(cx).await.err().unwrap();
    assert!(error.to_string().contains("ExternalSourceRegistry"));
}

#[tokio::test]
async fn duplicate_registration_does_not_replace_existing_source() {
    let registry = ExternalSourceRegistry::default();
    let (_source, _, mut rx) = build("same", 2, &registry, false).await;
    let (sender, _other_rx) = SourceSender::new_test_sender_with_buffer(2);
    let mut cx = SourceContext::new_test(sender, None);
    cx.key = ComponentKey::from("same");
    cx.extra_context = ExtraContext::single_value(registry.clone());
    let error = ExternalSourceConfig {}.build(cx).await.err().unwrap();
    assert!(error.to_string().contains("already registered"));
    registry.get("same").unwrap().try_emit(log(1)).unwrap();
    assert!(rx.next().await.is_some());
}

#[tokio::test]
async fn dropping_source_cancels_pending_send_and_closes_queue() {
    let registry = ExternalSourceRegistry::default();
    let (source, _, mut rx) = build("malloc", 1, &registry, false).await;
    let mut handle = registry.get("malloc").unwrap();
    handle.try_emit(log(1)).unwrap();
    let send = handle.emit(log(2));
    tokio::pin!(send);
    assert!(poll!(send.as_mut()).is_pending());
    drop(source);
    assert_eq!(
        timeout(DEADLINE, send).await.unwrap(),
        Err(ExternalSendError::Closed)
    );
    assert!(rx.next().await.is_some());
    assert!(timeout(DEADLINE, rx.next()).await.unwrap().is_none());
    assert!(registry.get("malloc").is_none());
}

#[tokio::test]
async fn disconnected_receiver_returns_logs_to_nonblocking_caller() {
    let registry = ExternalSourceRegistry::default();
    let (_source, _, rx) = build("malloc", 1, &registry, false).await;
    let mut handle = registry.get("malloc").unwrap();
    drop(rx);
    let error = handle.try_emit(log(42)).unwrap_err();
    assert!(matches!(error, ExternalTrySendError::Closed(_)));
    assert_eq!(error.into_inner().get("id"), Some(&Value::Integer(42)));
    assert_eq!(handle.emit(log(43)).await, Err(ExternalSendError::Closed));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_wakes_all_producers_on_a_full_queue() {
    let registry = ExternalSourceRegistry::default();
    let (source, shutdown, mut rx) = build("malloc", 8, &registry, false).await;
    let task = tokio::spawn(source);
    let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(9));
    let mut producers = Vec::new();
    for id in 0..8 {
        let mut handle = registry.get("malloc").unwrap();
        let barrier = std::sync::Arc::clone(&barrier);
        producers.push(tokio::spawn(async move {
            handle.emit(log(id)).await.unwrap();
            barrier.wait().await;
            handle.emit(log(id + 8)).await
        }));
    }
    timeout(DEADLINE, barrier.wait()).await.unwrap();
    // No receiver is polled until shutdown finishes: all second submissions
    // must either be cancelled while blocked or rejected at admission.
    timeout(DEADLINE, shutdown.shutdown_all(None))
        .await
        .unwrap();
    task.await.unwrap().unwrap();
    for producer in producers {
        assert_eq!(producer.await.unwrap(), Err(ExternalSendError::Closed));
    }
    let mut count = 0;
    while let Some(item) = timeout(DEADLINE, rx.next()).await.unwrap() {
        let EventArray::Logs(logs) = item.events else {
            panic!("expected logs")
        };
        count += logs.len();
    }
    assert_eq!(count, 8);
}

#[tokio::test]
async fn registries_are_isolated_and_rebuild_does_not_reopen_old_handles() {
    let first = ExternalSourceRegistry::default();
    let second = ExternalSourceRegistry::default();
    let (source, _, mut rx) = build("same", 2, &first, false).await;
    let (_other_source, _, mut other_rx) = build("same", 2, &second, false).await;
    let mut stale = first.get("same").unwrap();
    let mut other = second.get("same").unwrap();
    stale.try_emit(log(1)).unwrap();
    other.try_emit(log(2)).unwrap();
    let EventArray::Logs(logs) = rx.next().await.unwrap().events else {
        panic!("expected logs")
    };
    assert_eq!(logs[0].get("id"), Some(&Value::Integer(1)));
    let EventArray::Logs(logs) = other_rx.next().await.unwrap().events else {
        panic!("expected logs")
    };
    assert_eq!(logs[0].get("id"), Some(&Value::Integer(2)));

    drop(source);
    let (_new_source, _, mut new_rx) = build("same", 2, &first, false).await;
    assert!(matches!(
        stale.try_emit(log(3)),
        Err(ExternalTrySendError::Closed(_))
    ));
    first.get("same").unwrap().try_emit(log(4)).unwrap();
    assert!(new_rx.next().await.is_some());
}
