use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use serde_json::Value;
use tokio::sync::{watch, Semaphore};

type Outcome = Arc<Result<Value, String>>;
type Completion = Option<(Instant, Outcome)>;

// Expensive admin reads survive HTTP cancellation. Retries join the same job;
// only one computation may run, and completed results expire after five seconds.
pub(crate) struct BackgroundReads {
    entries: Mutex<HashMap<u64, watch::Receiver<Completion>>>,
    capacity: Arc<Semaphore>,
}

impl Default for BackgroundReads {
    fn default() -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
            capacity: Arc::new(Semaphore::new(1)),
        }
    }
}

pub(crate) enum ReadResult {
    Ready(Outcome),
    Pending,
    Busy,
}

impl BackgroundReads {
    pub(crate) async fn read<F>(&self, key: u64, work: F) -> ReadResult
    where
        F: FnOnce() -> anyhow::Result<Value> + Send + 'static,
    {
        let mut receiver = {
            let mut entries = self.entries.lock();
            entries.retain(|_, rx| {
                rx.borrow()
                    .as_ref()
                    .is_none_or(|(at, _)| at.elapsed() < Duration::from_secs(5))
            });
            if let Some(rx) = entries.get(&key) {
                rx.clone()
            } else {
                let Ok(permit) = self.capacity.clone().try_acquire_owned() else {
                    return ReadResult::Busy;
                };
                if entries.len() >= 32 {
                    return ReadResult::Busy;
                }
                let (tx, rx) = watch::channel(None);
                entries.insert(key, rx.clone());
                tokio::spawn(async move {
                    let result = tokio::task::spawn_blocking(move || {
                        let _permit = permit;
                        work()
                    })
                    .await
                    .map_err(|err| err.to_string())
                    .and_then(|result| result.map_err(|err| err.to_string()));
                    tx.send_replace(Some((Instant::now(), Arc::new(result))));
                });
                rx
            }
        };
        let completed = tokio::time::timeout(Duration::from_millis(150), async {
            loop {
                if let Some((_, result)) = receiver.borrow_and_update().as_ref() {
                    return result.clone();
                }
                if receiver.changed().await.is_err() {
                    return Arc::new(Err("background read stopped".to_string()));
                }
            }
        })
        .await;
        match completed {
            Ok(result) => ReadResult::Ready(result),
            Err(_) => ReadResult::Pending,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn slow_reads_survive_cancellation_and_retries_do_not_duplicate_work() {
        let reads = Arc::new(BackgroundReads::default());
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (finish_tx, finish_rx) = std::sync::mpsc::channel();
        let task_reads = reads.clone();
        let task = tokio::spawn(async move {
            task_reads
                .read(1, move || {
                    started_tx.send(()).unwrap();
                    finish_rx.recv().unwrap();
                    Ok(serde_json::json!({"done": true}))
                })
                .await
        });
        started_rx.await.unwrap();
        task.abort();
        assert!(matches!(
            reads.read(2, || panic!("must not start")).await,
            ReadResult::Busy
        ));
        assert!(matches!(
            reads.read(1, || panic!("must join existing job")).await,
            ReadResult::Pending
        ));
        finish_tx.send(()).unwrap();
        let ReadResult::Ready(result) = reads.read(1, || panic!("must reuse result")).await else {
            panic!("not ready")
        };
        assert_eq!(result.as_ref().as_ref().unwrap()["done"], true);
    }

    #[tokio::test]
    async fn failures_and_panics_release_capacity() {
        let reads = BackgroundReads::default();
        let ReadResult::Ready(result) = reads.read(1, || panic!("test worker panic")).await else {
            panic!("not ready")
        };
        assert!(result.is_err());
        let ReadResult::Ready(result) = reads.read(2, || anyhow::bail!("not found")).await else {
            panic!("not ready")
        };
        assert_eq!(result.as_ref().as_ref().unwrap_err(), "not found");
    }
}
