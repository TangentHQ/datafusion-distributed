use super::encode::Encoded;
use arrow_flight::{FlightData, error::FlightError};
use datafusion::common::runtime::SpawnedTask;

use futures::{Stream, StreamExt};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::{sync::CancellationToken, task::TaskTracker};

/// Merge Flight streams under bounded queue slots and fallible byte admission.
/// Returned FlightData retains its charge until its final byte slice drops;
/// freeing a queue slot does not release that byte ownership.
pub(super) fn spawn_select_all<T>(
    inner: Vec<T>,
    queue_size: usize,
    tracker: TaskTracker,
    closed: CancellationToken,
) -> impl Stream<Item = Result<FlightData, FlightError>>
where
    T: Stream<Item = Result<Encoded, FlightError>> + Send + Unpin + 'static,
{
    let mut tasks = Vec::with_capacity(inner.len());
    let mut receivers = Vec::with_capacity(inner.len());
    for mut input in inner {
        let (sender, receiver) = tokio::sync::mpsc::channel(queue_size);
        receivers.push(ReceiverStream::new(receiver));
        let closed = closed.clone();
        tasks.push(SpawnedTask::spawn(tracker.track_future(async move {
            loop {
                let permit = tokio::select! {
                    biased;
                    _ = closed.cancelled() => return,
                    permit = sender.reserve() => match permit {
                        Ok(permit) => permit,
                        Err(_) => return,
                    },
                };
                let message = tokio::select! {
                    biased;
                    _ = closed.cancelled() => return,
                    _ = sender.closed() => return,
                    message = input.next() => message,
                };
                let Some(message) = message else {
                    return;
                };
                let message = message.map(Encoded::into_data);
                let failed = message.is_err();
                permit.send(message);
                if failed {
                    return;
                }
            }
        })));
    }
    futures::stream::select_all(receivers).map(move |message| {
        let _ = &tasks;
        message
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};
    use std::sync::Arc;

    #[tokio::test]
    async fn shutdown_joins_pending_and_backpressured_flight_producers() {
        use crate::Worker;
        use datafusion::execution::memory_pool::MemoryConsumer;
        use futures::stream::BoxStream;

        for mode in 0..3 {
            let blocked = mode != 0;
            let worker = Worker::default();
            let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(16));
            let memory = MemoryConsumer::new("Flight producer fixture").register(&pool);
            memory.try_grow(8).unwrap();
            let ready = Arc::new(tokio::sync::Notify::new());
            let started = Arc::clone(&ready);
            let input: BoxStream<'static, Result<Encoded, FlightError>> = if blocked {
                futures::stream::once(async move {
                    let _memory = memory;
                    started.notify_one();
                    futures::future::pending().await
                })
                .boxed()
            } else {
                let pool = Arc::clone(&pool);
                futures::stream::repeat_with(move || {
                    let _memory = &memory;
                    started.notify_one();
                    Encoded::admitted(FlightData::new(), &pool)
                })
                .boxed()
            };
            let producer = worker.producer().await.unwrap();
            let mut shutdown = Box::pin(worker.shutdown());
            if mode == 2 {
                assert!(futures::poll!(&mut shutdown).is_pending());
            }
            let stream = spawn_select_all(
                vec![input],
                1,
                producer.tracker.clone(),
                producer.closed.clone(),
            );
            drop(producer);
            if mode != 2 {
                ready.notified().await;
            }
            tokio::time::timeout(std::time::Duration::from_secs(5), shutdown)
                .await
                .expect("worker shutdown must join its Flight producers");
            assert_eq!(pool.reserved(), 0, "mode: {mode}");
            assert!(worker.producer().await.is_err());
            drop(stream);
        }
    }

    #[tokio::test]
    async fn returned_flight_bytes_retain_memory_after_queue_drop() {
        let worker = crate::Worker::default();
        let producer = worker.producer().await.unwrap();
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(16));
        let data = FlightData::new().with_data_body(bytes::Bytes::from_static(b"payload"));
        let mut stream = spawn_select_all(
            vec![futures::stream::iter([Encoded::admitted(data, &pool)])],
            1,
            producer.tracker.clone(),
            producer.closed.clone(),
        );
        drop(producer);
        let data = stream.next().await.unwrap().unwrap();
        let child = data.data_body.slice(1..4);
        drop(data);
        drop(stream);
        worker.shutdown().await;
        assert_eq!(pool.reserved(), 7);
        assert_eq!(child.as_ref(), b"ayl");
        drop(child);
        assert_eq!(pool.reserved(), 0);
    }

    #[tokio::test]
    async fn over_budget_message_fails_instead_of_bypassing_admission() {
        let worker = crate::Worker::default();
        let producer = worker.producer().await.unwrap();
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(4));
        let data = FlightData::new().with_data_body(bytes::Bytes::from_static(b"payload"));
        let mut stream = spawn_select_all(
            vec![futures::stream::iter([Encoded::admitted(data, &pool)])],
            1,
            producer.tracker.clone(),
            producer.closed.clone(),
        );
        drop(producer);
        assert!(stream.next().await.unwrap().is_err());
        assert!(stream.next().await.is_none());
        drop(stream);
        worker.shutdown().await;
        assert_eq!(pool.reserved(), 0);
    }
}
