use crate::{
    event::{BatchNotifier, BatchStatus, EventFinalizer},
    Error, LogEvent,
};
use std::{
    collections::BTreeSet,
    sync::{Arc, Mutex},
};
use tokio::sync::Notify;

#[derive(Default)]
pub(crate) struct Tracker {
    state: Mutex<State>,
    changed: Notify,
}

#[derive(Default)]
struct State {
    next: u64,
    pending: BTreeSet<u64>,
    failed: Option<u64>,
}

impl Tracker {
    pub(crate) fn attach(self: &Arc<Self>, logs: &mut [LogEvent]) {
        if logs.is_empty() {
            return;
        }
        let id = {
            let mut state = self.state.lock().unwrap();
            state.next += 1;
            let id = state.next;
            state.pending.insert(id);
            id
        };
        let tracker = self.clone();
        let batch = BatchNotifier::new_with_callback(move |status| {
            let mut state = tracker.state.lock().unwrap();
            state.pending.remove(&id);
            if status != BatchStatus::Delivered {
                state.failed = Some(state.failed.map_or(id, |old| old.min(id)));
            }
            drop(state);
            tracker.changed.notify_waiters();
        });
        for log in logs {
            log.add_finalizer(EventFinalizer::new(batch.clone()));
        }
    }

    pub(crate) async fn flush(&self) -> Result<(), Error> {
        let target = self.state.lock().unwrap().next;
        loop {
            // Register before checking state, so completion cannot lose a wakeup.
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                let state = self.state.lock().unwrap();
                if state.pending.first().is_none_or(|id| *id > target) {
                    return if state.failed.is_some_and(|id| id <= target) {
                        Err(Error::Delivery)
                    } else {
                        Ok(())
                    };
                }
            }
            notified.await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::EventStatus;

    #[tokio::test]
    async fn snapshot_does_not_wait_for_later_submissions() {
        let tracker = Arc::new(Tracker::default());
        let mut first = vec![LogEvent::default()];
        tracker.attach(&mut first);
        let flush = tracker.flush();
        tokio::pin!(flush);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(10), &mut flush)
                .await
                .is_err()
        );
        let mut later = vec![LogEvent::default()];
        tracker.attach(&mut later);
        drop(first);
        tokio::time::timeout(std::time::Duration::from_secs(1), &mut flush)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(tracker.state.lock().unwrap().pending.len(), 1);
        drop(later);
        tracker.flush().await.unwrap();
    }

    #[tokio::test]
    async fn callback_waits_for_all_event_copies_and_keeps_failure() {
        let tracker = Arc::new(Tracker::default());
        let mut logs = vec![LogEvent::default(), LogEvent::default()];
        tracker.attach(&mut logs);
        let retained = logs[0].clone();
        logs[1]
            .metadata()
            .finalizers()
            .update_status(EventStatus::Rejected);
        drop(logs);
        assert_eq!(tracker.state.lock().unwrap().pending.len(), 1);
        drop(retained);
        assert!(matches!(tracker.flush().await, Err(Error::Delivery)));
        assert!(tracker.state.lock().unwrap().pending.is_empty());
    }
}
