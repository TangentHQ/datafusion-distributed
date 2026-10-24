use super::{Credit, Requester};
use crate::common::now_ns;
use crate::{
    BytesMetricExt, CoordinatorToWorkerMsg, LatencyMetricExt, MaybeEncoded, WorkUnit,
    WorkUnitBatch, WorkUnitMsg,
};
use datafusion::common::{HashMap, Result, exec_err, internal_datafusion_err};
use datafusion::execution::TaskContext;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation};
use datafusion::physical_expr_common::metrics::MetricBuilder;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion_proto::protobuf::proto_error;
use futures::StreamExt;
use futures::stream::BoxStream;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio_stream::wrappers::UnboundedReceiverStream;
use uuid::Uuid;

pub(crate) struct HeldWorkUnit {
    unit: WorkUnitMsg,
    memory: MemoryReservation,
}

/// A decoded work unit and its independently retained admission. Polling the
/// next credit, cancelling a feed, or dropping its task cannot release this
/// charge while the consumer still owns the value.
pub struct OwnedWorkUnit<T> {
    value: T,
    memory: Arc<MemoryReservation>,
}

impl<T> std::ops::Deref for OwnedWorkUnit<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.value
    }
}

impl<T> OwnedWorkUnit<T> {
    pub(crate) fn new(value: T, memory: MemoryReservation) -> Self {
        Self {
            value,
            memory: Arc::new(memory),
        }
    }
    pub fn memory(&self) -> Arc<MemoryReservation> {
        Arc::clone(&self.memory)
    }
    pub(crate) fn into_inner(self) -> T {
        self.value
    }
}

pub(crate) type WorkUnitTx = UnboundedSender<Result<HeldWorkUnit>>;
pub(crate) type WorkUnitRx = UnboundedReceiver<Result<HeldWorkUnit>>;
pub(crate) struct RemoteRx {
    receiver: Mutex<Option<WorkUnitRx>>,
    credit: Option<Arc<Mutex<Credit>>>,
}
pub(crate) struct RemoteTx {
    pub(crate) sender: WorkUnitTx,
    pub(crate) credit: Option<Arc<Mutex<Credit>>>,
    pool: Arc<dyn MemoryPool>,
}

impl RemoteTx {
    pub(crate) fn send(&self, unit: WorkUnitMsg) -> Result<()> {
        let size = match &unit.body {
            MaybeEncoded::Encoded(bytes) => bytes.capacity(),
            MaybeEncoded::Decoded(value) => value.encoded_len(),
        };
        let memory = MemoryConsumer::new("QueuedWorkUnit").register(&self.pool);
        memory.try_grow(
            size.checked_add(512)
                .ok_or_else(|| internal_datafusion_err!("Work unit size overflow"))?,
        )?;
        self.sender
            .send(Ok(HeldWorkUnit { unit, memory }))
            .map_err(|_| internal_datafusion_err!("Work unit receiver closed"))
    }
}
pub(crate) type RemoteWorkUnitFeedRxs = HashMap<(Uuid, usize), RemoteRx>;
pub(crate) type RemoteWorkUnitFeedTxs = HashMap<(Uuid, usize), RemoteTx>;

/// Bridge between the worker's gRPC layer and the remote-variant
/// [`crate::WorkUnitFeed`]s installed in the deserialized plan.
///
/// One (sender, receiver) pair is created per `(feed id, partition)` when a new plan is
/// set on the worker:
/// - The **senders** are used by the [`crate::Worker`] gRPC handler to push the serialized
///   [`crate::WorkUnit`]s that arrive over the coordinator channel into the right queue.
/// - The **receivers** are consumed by the worker-side [`RemoteFeedProvider`] (the remote
///   variant of [`crate::WorkUnitFeed`]), which decodes the bytes back into the leaf plan's
///   concrete `T::WorkUnit` type so the leaf sees the same typed stream as it would in a
///   single-node execution.
#[derive(Default)]
pub(crate) struct RemoteWorkUnitFeedRegistry {
    pub(crate) receivers: RemoteWorkUnitFeedRxs,
    pub(crate) senders: RemoteWorkUnitFeedTxs,
}

impl RemoteWorkUnitFeedRegistry {
    /// Creates all the receivers and senders for a specific [WorkUnitMsg] Feed id. One feed per
    /// partition is created.
    ///
    /// Calling this twice with the same `id` is a coordinator bug — duplicate declarations
    /// mean two plan nodes share a UUID, which would cause "already consumed" when both
    /// nodes call `feed()`. We skip rather than overwrite so the coordinator-side duplicate
    /// detection in `task_specialized_plan` surfaces the real error first.
    pub(crate) fn add(
        &mut self,
        id: Uuid,
        partitions: usize,
        demand: bool,
        pool: &Arc<dyn MemoryPool>,
    ) {
        for partition in 0..partitions {
            // Skip if already registered; overwriting would silently drop the existing
            // receiver and cause a confusing "already consumed" error at execution time.
            if self.receivers.contains_key(&(id, partition)) {
                continue;
            }
            let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
            let credit = demand.then(|| Arc::new(Mutex::new(Credit::default())));
            self.receivers.insert(
                (id, partition),
                RemoteRx {
                    receiver: Mutex::new(Some(rx)),
                    credit: credit.clone(),
                },
            );
            self.senders.insert(
                (id, partition),
                RemoteTx {
                    sender: tx,
                    credit,
                    pool: Arc::clone(pool),
                },
            );
        }
    }
}

pub(crate) fn build_work_unit_batch_msg(
    id: &Uuid,
    work_unit_batch: Vec<(usize, Result<Box<dyn WorkUnit>>)>,
) -> Result<CoordinatorToWorkerMsg> {
    Ok(CoordinatorToWorkerMsg::WorkUnitBatch(WorkUnitBatch {
        batch: work_unit_batch
            .into_iter()
            .map(|(partition, work_unit)| {
                Ok(WorkUnitMsg {
                    id: *id,
                    partition,
                    body: MaybeEncoded::Decoded(work_unit?),
                    created_timestamp_unix_nanos: now_ns(),
                    sent_timestamp_unix_nanos: 0,
                    received_timestamp_unix_nanos: 0,
                    processed_timestamp_unix_nanos: 0,
                })
            })
            .collect::<Result<_>>()?,
    }))
}

pub(crate) fn set_work_unit_send_time(mut msg: CoordinatorToWorkerMsg) -> CoordinatorToWorkerMsg {
    match &mut msg {
        CoordinatorToWorkerMsg::WorkUnitBatch(batch) => {
            for unit in &mut batch.batch {
                unit.sent_timestamp_unix_nanos = now_ns();
            }
        }
        CoordinatorToWorkerMsg::WorkUnitResponse(response) => {
            if let Some(unit) = &mut response.unit {
                unit.sent_timestamp_unix_nanos = now_ns();
            }
        }
        _ => {}
    }
    msg
}

pub(crate) fn set_work_unit_received_time(
    mut msg: CoordinatorToWorkerMsg,
) -> CoordinatorToWorkerMsg {
    match &mut msg {
        CoordinatorToWorkerMsg::WorkUnitBatch(batch) => {
            for unit in &mut batch.batch {
                unit.received_timestamp_unix_nanos = now_ns();
            }
        }
        CoordinatorToWorkerMsg::WorkUnitResponse(response) => {
            if let Some(unit) = &mut response.unit {
                unit.received_timestamp_unix_nanos = now_ns();
            }
        }
        _ => {}
    }
    msg
}

/// Remove implementation of a [WorkUnitFeedProvider] that pulls [crate::WorkUnitMsg]s coming over
/// the wire from a [RemoteWorkUnitFeedRegistry].
///
/// Deserializing a [crate::WorkUnitFeed] with [crate::WorkUnitFeed::from_proto] always returns a
/// [crate::WorkUnitFeed<RemoteFeedProvider>] that will receive messages over the network, rather
/// than executing the original [WorkUnitFeedProvider] locally.
///
/// There's a diagram about how this works in [crate::WorkUnitFeed].
#[derive(Debug, Clone)]
pub(crate) struct RemoteFeedProvider {
    pub(crate) id: Uuid,
    pub(crate) metrics: ExecutionPlanMetricsSet,
}

impl RemoteFeedProvider {
    pub(crate) fn feed<T: WorkUnit + Default + 'static>(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
        feedback: bool,
        bound: fn(&[u8]) -> Result<usize>,
    ) -> Result<BoxStream<'static, Result<OwnedWorkUnit<T>>>> {
        let bdr = || MetricBuilder::new(&self.metrics);

        let bytes_transferred = bdr().bytes_counter("work_unit_bytes");
        let in_memory_transferred = bdr().global_counter("work_unit_in_memory_count");
        let msg_count = bdr().global_counter("work_unit_count");
        // Track end-to-end network latency distribution for all work units.
        let send_latency_max = bdr().max_latency("work_unit_send_latency_max");
        let send_latency_p50 = bdr().p50_latency("work_unit_send_latency_p50");

        let received_latency_max = bdr().max_latency("work_unit_received_latency_max");
        let received_latency_p50 = bdr().p50_latency("work_unit_received_latency_p50");

        let processed_latency_max = bdr().max_latency("work_unit_processed_latency_max");
        let processed_latency_p50 = bdr().p50_latency("work_unit_processed_latency_p50");

        let elapsed_compute = bdr().elapsed_compute(partition);

        let Some(rxs) = ctx
            .session_config()
            .get_extension::<RemoteWorkUnitFeedRxs>()
        else {
            return exec_err!("Missing RemoteWorkUnitFeedRegistry in context");
        };

        let id = self.id;
        let Some(remote_feed) = rxs.get(&(id, partition)) else {
            return exec_err!(
                "Missing WorkUnit feed for id {id} and partition {partition}. Was the WorkUnitFeed registered with DistributedExt::with_distributed_work_unit_feed?"
            );
        };

        let Some(receiver) = std::mem::take(
            &mut *remote_feed
                .receiver
                .lock()
                .map_err(|_| internal_datafusion_err!("Work unit receiver is poisoned"))?,
        ) else {
            return exec_err!(
                "WorkUnit feed for id {id} and partition {partition} was already consumed"
            );
        };

        let stream = match &remote_feed.credit {
            None => UnboundedReceiverStream::new(receiver).boxed(),
            Some(credit) => {
                let credit = Arc::clone(credit);
                {
                    let mut credit = credit
                        .lock()
                        .map_err(|_| internal_datafusion_err!("Work unit credit is poisoned"))?;
                    credit.required = feedback;
                    credit.memory =
                        Some(MemoryConsumer::new("WorkUnitFeedback").register(ctx.memory_pool()));
                }
                let requester = ctx
                    .session_config()
                    .get_extension::<Requester>()
                    .ok_or_else(|| internal_datafusion_err!("Missing work unit demand channel"))?;
                futures::stream::try_unfold(receiver, move |mut receiver| {
                    let credit = Arc::clone(&credit);
                    let requester = Arc::clone(&requester);
                    async move {
                        let demand = credit
                            .lock()
                            .map_err(|_| internal_datafusion_err!("Work unit credit is poisoned"))?
                            .request(id, partition)?;
                        requester
                            .0
                            .send(crate::WorkerToCoordinatorMsg::WorkUnitDemand(demand))
                            .map_err(|_| {
                                internal_datafusion_err!("Work unit demand channel closed")
                            })?;
                        match receiver.recv().await {
                            Some(result) => Ok(Some((result?, receiver))),
                            None => Ok(None),
                        }
                    }
                })
                .boxed()
            }
        };
        Ok(stream
            .map(move |work_unit_msg_or_err| {
                let HeldWorkUnit {
                    unit: mut work_unit_msg,
                    memory,
                } = work_unit_msg_or_err?;
                let reserved = memory.size();
                let timer = elapsed_compute.timer();
                let work_unit = match work_unit_msg.body {
                    MaybeEncoded::Encoded(bytes) => {
                        bytes_transferred.add_bytes(bytes.len());
                        // The leaf supplies a schema-specific bound. Admission
                        // happens before prost allocates the decoded message.
                        let decoded = bound(&bytes)?;
                        memory.try_grow(decoded)?;
                        let value = T::decode(bytes.as_slice())
                            .map_err(|err| proto_error(format!("{err}")))?;
                        drop(bytes);
                        memory.shrink(reserved);
                        value
                    }
                    MaybeEncoded::Decoded(work_unit) => {
                        let work_unit = work_unit.into_any();
                        let Ok(work_unit) = work_unit.downcast::<T>() else {
                            return exec_err!(
                                "Expected WorkUnit of type {}",
                                std::any::type_name::<T>()
                            );
                        };
                        let size = work_unit.encoded_len();
                        memory.try_grow(size)?;
                        let encoded = work_unit.encode_to_bytes();
                        let decoded = bound(&encoded)?;
                        memory.try_resize(reserved.checked_add(decoded).ok_or_else(|| {
                            internal_datafusion_err!("Work unit size overflow")
                        })?)?;
                        drop(encoded);
                        in_memory_transferred.add(1);
                        *work_unit
                    }
                };

                timer.done();
                work_unit_msg.processed_timestamp_unix_nanos = now_ns();

                let WorkUnitMsg {
                    created_timestamp_unix_nanos: base,
                    sent_timestamp_unix_nanos,
                    received_timestamp_unix_nanos,
                    processed_timestamp_unix_nanos,
                    ..
                } = work_unit_msg;

                msg_count.add(1);

                send_latency_max.add_nanos(sent_timestamp_unix_nanos.saturating_sub(base));
                send_latency_p50.add_nanos(sent_timestamp_unix_nanos.saturating_sub(base));

                received_latency_max.add_nanos(received_timestamp_unix_nanos.saturating_sub(base));
                received_latency_p50.add_nanos(received_timestamp_unix_nanos.saturating_sub(base));

                processed_latency_max
                    .add_nanos(processed_timestamp_unix_nanos.saturating_sub(base));
                processed_latency_p50
                    .add_nanos(processed_timestamp_unix_nanos.saturating_sub(base));

                Ok(OwnedWorkUnit {
                    value: work_unit,
                    memory: Arc::new(memory),
                })
            })
            .boxed())
    }

    pub(crate) fn complete(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
        body: bytes::Bytes,
    ) -> Result<()> {
        let feeds = ctx
            .session_config()
            .get_extension::<RemoteWorkUnitFeedRxs>()
            .ok_or_else(|| internal_datafusion_err!("Missing remote work unit feeds"))?;
        let credit = feeds
            .get(&(self.id, partition))
            .and_then(|feed| feed.credit.as_ref())
            .ok_or_else(|| internal_datafusion_err!("Completion requires demand feed"))?;
        credit
            .lock()
            .map_err(|_| internal_datafusion_err!("Work unit credit is poisoned"))?
            .complete(body)
    }

    pub(crate) fn metrics(&self) -> ExecutionPlanMetricsSet {
        self.metrics.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::execution::{
        context::SessionContext, memory_pool::GreedyMemoryPool, runtime_env::RuntimeEnvBuilder,
    };
    use datafusion::prelude::SessionConfig;
    use futures::TryStreamExt;
    use prost::Message;

    #[derive(Clone, PartialEq, Message)]
    struct Piece {
        #[prost(bytes = "vec", tag = "1")]
        body: Vec<u8>,
    }

    fn bound(bytes: &[u8]) -> Result<usize> {
        bytes
            .len()
            .checked_add(128)
            .ok_or_else(|| internal_datafusion_err!("piece too large"))
    }

    // Advancing or dropping the feed cannot release the prior piece's bytes.
    // A failed predecode admission must drop the queued allocation as well.
    #[tokio::test]
    async fn returned_pieces_keep_independent_charges_after_eof_and_cancellation() -> Result<()> {
        for (budget, cancelled) in [(1600, false), (64 * 1024, false), (64 * 1024, true)] {
            let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(budget));
            let runtime = RuntimeEnvBuilder::new()
                .with_memory_pool(pool.clone())
                .build_arc()?;
            let id = Uuid::new_v4();
            let mut registry = RemoteWorkUnitFeedRegistry::default();
            registry.add(id, 1, false, &pool);
            let tx = registry.senders.remove(&(id, 0)).unwrap();
            let encoded = Piece {
                body: vec![17; 800],
            }
            .encode_to_vec();
            tx.send(WorkUnitMsg {
                id,
                partition: 0,
                body: MaybeEncoded::Encoded(encoded),
                created_timestamp_unix_nanos: 0,
                sent_timestamp_unix_nanos: 0,
                received_timestamp_unix_nanos: 0,
                processed_timestamp_unix_nanos: 0,
            })?;
            drop(tx);
            let config = SessionConfig::new().with_extension(Arc::new(registry.receivers));
            let ctx = SessionContext::new_with_config_rt(config, runtime).task_ctx();
            let provider = RemoteFeedProvider {
                id,
                metrics: ExecutionPlanMetricsSet::new(),
            };
            let mut stream = provider.feed::<Piece>(0, ctx, false, bound)?;
            if budget == 1600 {
                // Encoded queue fits, but the additional decoded body does not.
                assert!(stream.try_next().await.is_err());
                drop(stream);
                assert_eq!(pool.reserved(), 0);
                continue;
            }
            let piece = stream.try_next().await?.unwrap();
            assert_eq!(piece.body.as_slice(), vec![17; 800]);
            let charged = pool.reserved();
            assert!(charged >= piece.body.capacity());
            if !cancelled {
                assert!(stream.try_next().await?.is_none());
            }
            drop(stream);
            assert_eq!(
                pool.reserved(),
                charged,
                "EOF cannot release the returned piece"
            );
            drop(piece);
            assert_eq!(pool.reserved(), 0);
        }
        Ok(())
    }
}
