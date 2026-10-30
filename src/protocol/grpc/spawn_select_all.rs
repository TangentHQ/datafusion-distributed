use super::encode::Encoded;
use arrow_flight::{FlightData, error::FlightError};
use datafusion::common::runtime::SpawnedTask;

use futures::{Stream, StreamExt};
use tokio_stream::wrappers::ReceiverStream;

/// Merge Flight streams under bounded queue slots and fallible byte admission.
/// Returned FlightData retains its charge until its final byte slice drops;
/// freeing a queue slot does not release that byte ownership.
pub(super) fn spawn_select_all<T>(
    inner: Vec<T>,
    queue_size: usize,
) -> impl Stream<Item = Result<FlightData, FlightError>>
where
    T: Stream<Item = Result<Encoded, FlightError>> + Send + Unpin + 'static,
{
    let mut tasks = Vec::with_capacity(inner.len());
    let mut receivers = Vec::with_capacity(inner.len());
    for mut input in inner {
        let (sender, receiver) = tokio::sync::mpsc::channel(queue_size);
        receivers.push(ReceiverStream::new(receiver));
        tasks.push(SpawnedTask::spawn(async move {
            loop {
                let Ok(permit) = sender.reserve().await else {
                    return;
                };
                let message = tokio::select! {
                    biased;
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
        }));
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
    async fn returned_flight_bytes_retain_memory_after_queue_drop() {
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(16));
        let data = FlightData::new().with_data_body(bytes::Bytes::from_static(b"payload"));
        let mut stream = spawn_select_all(
            vec![futures::stream::iter([Encoded::admitted(data, &pool)])],
            1,
        );
        let data = stream.next().await.unwrap().unwrap();
        let child = data.data_body.slice(1..4);
        drop(data);
        drop(stream);
        assert_eq!(pool.reserved(), 7);
        assert_eq!(child.as_ref(), b"ayl");
        drop(child);
        assert_eq!(pool.reserved(), 0);
    }

    #[tokio::test]
    async fn over_budget_message_fails_instead_of_bypassing_admission() {
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(4));
        let data = FlightData::new().with_data_body(bytes::Bytes::from_static(b"payload"));
        let mut stream = spawn_select_all(
            vec![futures::stream::iter([Encoded::admitted(data, &pool)])],
            1,
        );
        assert!(stream.next().await.unwrap().is_err());
        assert!(stream.next().await.is_none());
        drop(stream);
        assert_eq!(pool.reserved(), 0);
    }
}
