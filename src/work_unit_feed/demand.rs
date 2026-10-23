//! One outstanding descriptor per exact query task/feed/partition. A response
//! is never retried here: channel loss belongs to the enclosing query attempt.
use crate::work_unit_feed::build_work_unit_batch_msg;
use crate::{CoordinatorToWorkerMsg, MaybeEncoded, TaskKey, WorkUnitDemand, WorkUnitResponse};
use datafusion::common::{HashMap, Result, exec_err, internal_datafusion_err};
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation};
use futures::{StreamExt, stream::BoxStream};
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc::UnboundedSender;
use uuid::Uuid;

use super::WorkUnit;

type Feed = BoxStream<'static, Result<Box<dyn WorkUnit>>>;
type Key = (TaskKey, Uuid, usize);

#[derive(Default)]
pub(crate) struct Demands {
    feeds: Mutex<HashMap<Key, Arc<Mutex<State>>>>,
    senders: Mutex<HashMap<TaskKey, UnboundedSender<CoordinatorToWorkerMsg>>>,
}

struct State {
    sequence: u64,
    feed: Option<Feed>,
    ended: bool,
    memory: Arc<MemoryReservation>,
}

impl Demands {
    pub(crate) fn add(
        &self,
        task: TaskKey,
        id: Uuid,
        partition: usize,
        feed: Feed,
        pool: &Arc<dyn MemoryPool>,
    ) -> Result<()> {
        let mut feeds = self
            .feeds
            .lock()
            .map_err(|_| internal_datafusion_err!("Demand registry is poisoned"))?;
        if feeds
            .insert(
                (task, id, partition),
                Arc::new(Mutex::new(State {
                    sequence: 0,
                    feed: Some(feed),
                    ended: false,
                    memory: Arc::new(MemoryConsumer::new("DemandDescriptor").register(pool)),
                })),
            )
            .is_some()
        {
            return exec_err!(
                "Duplicate demand feed for task {task:?}, feed {id}, partition {partition}"
            );
        }
        Ok(())
    }

    pub(crate) fn sender(
        &self,
        task: TaskKey,
        tx: UnboundedSender<CoordinatorToWorkerMsg>,
    ) -> Result<()> {
        self.senders
            .lock()
            .map_err(|_| internal_datafusion_err!("Demand senders are poisoned"))?
            .insert(task, tx);
        Ok(())
    }

    /// Validate and take the single credit synchronously before spawning any
    /// work. Duplicate, unsolicited, stale and concurrent demands fail closed.
    pub(crate) fn request(
        &self,
        task: TaskKey,
        demand: WorkUnitDemand,
    ) -> Result<impl Future<Output = Result<()>> + Send + 'static> {
        let tx = self
            .senders
            .lock()
            .map_err(|_| internal_datafusion_err!("Demand senders are poisoned"))?
            .get(&task)
            .cloned()
            .ok_or_else(|| internal_datafusion_err!("Missing demand channel for {task:?}"))?;
        let state = self
            .feeds
            .lock()
            .map_err(|_| internal_datafusion_err!("Demand registry is poisoned"))?
            .get(&(task, demand.id, demand.partition))
            .cloned()
            .ok_or_else(|| internal_datafusion_err!("Undeclared demand feed for {task:?}"))?;
        let (mut feed, memory) = {
            let mut state = state
                .lock()
                .map_err(|_| internal_datafusion_err!("Demand state is poisoned"))?;
            if state.ended || state.sequence != demand.sequence {
                return exec_err!("Stale demand sequence for {task:?}");
            }
            let feed = state.feed.take().ok_or_else(|| {
                internal_datafusion_err!("Concurrent work unit demand for {task:?}")
            })?;
            state.memory.free();
            (feed, Arc::clone(&state.memory))
        };
        Ok(async move {
            let unit = match feed.next().await {
                Some(result) => {
                    let batch =
                        build_work_unit_batch_msg(&demand.id, vec![(demand.partition, result)])?;
                    let CoordinatorToWorkerMsg::WorkUnitBatch(mut batch) = batch else {
                        return exec_err!("Invalid work unit response");
                    };
                    batch.batch.pop()
                }
                None => None,
            };
            if let Some(unit) = &unit {
                let size = match &unit.body {
                    MaybeEncoded::Encoded(bytes) => bytes.capacity(),
                    MaybeEncoded::Decoded(value) => value.encoded_len(),
                };
                memory.try_resize(
                    size.checked_mul(3)
                        .and_then(|bytes| bytes.checked_add(512))
                        .ok_or_else(|| internal_datafusion_err!("Descriptor size overflow"))?,
                )?;
            }
            {
                let mut state = state
                    .lock()
                    .map_err(|_| internal_datafusion_err!("Demand state is poisoned"))?;
                state.sequence = state
                    .sequence
                    .checked_add(1)
                    .ok_or_else(|| internal_datafusion_err!("Work unit sequence overflow"))?;
                state.ended = unit.is_none();
                state.feed = if state.ended { None } else { Some(feed) };
            }
            tx.send(CoordinatorToWorkerMsg::WorkUnitResponse(WorkUnitResponse {
                demand,
                unit,
            }))
            .map_err(|_| internal_datafusion_err!("Demand response channel closed for {task:?}"))
        })
    }
}

/// Worker channel extension; the TaskKey is implicit in this channel, so a leaf
/// cannot request a different query or worker's descriptor.
#[derive(Clone)]
pub(crate) struct Requester(pub(crate) UnboundedSender<crate::WorkerToCoordinatorMsg>);

#[derive(Default)]
pub(crate) struct Credit {
    pub(crate) sequence: u64,
    pub(crate) waiting: bool,
}

impl Credit {
    pub(crate) fn request(&mut self, id: Uuid, partition: usize) -> Result<WorkUnitDemand> {
        if self.waiting {
            return exec_err!("Concurrent demand for feed {id}");
        }
        self.waiting = true;
        Ok(WorkUnitDemand {
            id,
            partition,
            sequence: self.sequence,
        })
    }
    pub(crate) fn respond(&mut self, demand: WorkUnitDemand) -> Result<()> {
        if !self.waiting || demand.sequence != self.sequence {
            return exec_err!("Unsolicited or stale work unit response for {}", demand.id);
        }
        self.sequence = self
            .sequence
            .checked_add(1)
            .ok_or_else(|| internal_datafusion_err!("Work unit sequence overflow"))?;
        self.waiting = false;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MaybeEncoded;
    use futures::TryStreamExt;
    use prost::Message;

    #[derive(Clone, PartialEq, prost::Message)]
    struct Unit {
        #[prost(uint64, tag = "1")]
        id: u64,
    }

    #[tokio::test]
    async fn demand_sequences_reject_duplicates_foreign_tasks_and_concurrent_reads() {
        let feeds = Demands::default();
        let task = TaskKey {
            query_id: Uuid::new_v4(),
            stage_id: 1,
            task_number: 0,
        };
        let id = Uuid::new_v4();
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        feeds.sender(task, tx).unwrap();
        let pool: Arc<dyn MemoryPool> =
            Arc::new(datafusion::execution::memory_pool::UnboundedMemoryPool::default());
        feeds
            .add(
                task,
                id,
                0,
                futures::stream::iter(vec![Ok(Box::new(Unit { id: 3 }) as Box<dyn WorkUnit>)])
                    .boxed(),
                &pool,
            )
            .unwrap();
        let demand = WorkUnitDemand {
            id,
            partition: 0,
            sequence: 0,
        };
        let request = feeds.request(task, demand).unwrap();
        assert!(feeds.request(task, demand).is_err());
        assert!(
            feeds
                .request(
                    TaskKey {
                        task_number: 1,
                        ..task
                    },
                    demand
                )
                .is_err()
        );
        request.await.unwrap();
        let CoordinatorToWorkerMsg::WorkUnitResponse(response) = rx.recv().await.unwrap() else {
            panic!("wrong response")
        };
        let unit = response.unit.unwrap();
        let bytes = match unit.body {
            MaybeEncoded::Encoded(bytes) => bytes,
            MaybeEncoded::Decoded(unit) => unit.encode_to_bytes(),
        };
        assert_eq!(Unit::decode(bytes.as_slice()).unwrap().id, 3);
        assert!(feeds.request(task, demand).is_err());
        feeds
            .request(
                task,
                WorkUnitDemand {
                    sequence: 1,
                    ..demand
                },
            )
            .unwrap()
            .await
            .unwrap();
        let CoordinatorToWorkerMsg::WorkUnitResponse(response) = rx.recv().await.unwrap() else {
            panic!("wrong response")
        };
        assert!(response.unit.is_none());
        assert!(
            feeds
                .request(
                    task,
                    WorkUnitDemand {
                        sequence: 2,
                        ..demand
                    }
                )
                .is_err()
        );
    }

    #[cfg(feature = "integration")]
    #[tokio::test]
    async fn two_http_workers_pull_remaining_frozen_ids_after_fast_completion() -> Result<()> {
        use crate::test_utils::test_work_unit_feed::{
            RowGeneratorExec, RowGeneratorFeedProvider, ScanGate, TestWorkUnitFeedExecCodec,
        };
        use crate::{
            ChannelResolver, DistributedExt, ExecuteTaskRequest, ProducerHead, SetPlanRequest,
            WorkUnitFeed, WorkUnitFeedDeclaration, Worker, WorkerQueryContext,
            WorkerToCoordinatorMsg,
        };
        use datafusion::arrow::array::{Int64Array, StringArray};
        use datafusion::execution::{
            SessionStateBuilder, memory_pool::GreedyMemoryPool, runtime_env::RuntimeEnvBuilder,
        };
        use datafusion::physical_plan::{ExecutionPlan, metrics::ExecutionPlanMetricsSet};
        use http::HeaderMap;
        use tokio_stream::wrappers::{TcpListenerStream, UnboundedReceiverStream};
        use tonic::transport::Server;
        let (started, mut observed) = tokio::sync::mpsc::unbounded_channel();
        let gate = Arc::new(ScanGate {
            started,
            release: tokio::sync::Semaphore::new(0),
        });
        let feeds = Arc::new(Demands::default());
        let provider = RowGeneratorFeedProvider::shared(2, 1..=8);
        let feed = WorkUnitFeed::new(provider).with_demand();
        let id = feed.id;
        let mut state = SessionStateBuilder::new()
            .with_default_features()
            .with_distributed_option_extension(crate::DistributedConfig::default())
            .with_distributed_user_codec(TestWorkUnitFeedExecCodec);
        state = state.with_runtime_env(Arc::new(
            RuntimeEnvBuilder::new()
                .with_memory_pool(Arc::new(GreedyMemoryPool::new(64 * 1024 * 1024)))
                .build()?,
        ));
        let ctx = state.build().task_ctx();
        let query_id = Uuid::new_v4();
        let mut servers = tokio::task::JoinSet::new();
        let mut forwarders = tokio::task::JoinSet::new();
        let mut consumers = tokio::task::JoinSet::new();
        let mut senders = vec![];
        let mut workers = vec![];
        for rank in 0..2 {
            let gate = Arc::clone(&gate);
            let worker = Worker::from_session_builder(move |mut query: WorkerQueryContext| {
                let gate = Arc::clone(&gate);
                async move {
                    let cfg = query
                        .builder
                        .config()
                        .clone()
                        .unwrap_or_default()
                        .with_extension(gate);
                    Ok(query
                        .builder
                        .with_config(cfg)
                        .with_distributed_user_codec(TestWorkUnitFeedExecCodec)
                        .build())
                }
            })
            .with_max_message_size(1024);
            workers.push(worker.clone());
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
            let url: url::Url = format!("http://{}", listener.local_addr()?)
                .parse()
                .unwrap();
            servers.spawn(async move {
                Server::builder()
                    .add_service(worker.into_worker_server())
                    .serve_with_incoming(TcpListenerStream::new(listener))
                    .await
            });
            let task = TaskKey {
                query_id,
                stage_id: 1,
                task_number: rank,
            };
            feeds.add(
                task,
                id,
                0,
                feed.feed(rank, Arc::clone(&ctx))?
                    .map(|value| value.map(|value| Box::new(value) as Box<dyn WorkUnit>))
                    .boxed(),
                ctx.memory_pool(),
            )?;
            let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
            feeds.sender(task, tx.clone())?;
            senders.push(tx);
            let plan: Arc<dyn ExecutionPlan> = Arc::new(RowGeneratorExec::new(
                WorkUnitFeed::from_proto(feed.to_proto())?,
                "frozen".into(),
                1,
                None,
                8,
            ));
            let resolver = crate::grpc::DefaultChannelResolver::default();
            let mut client = resolver.get_worker_client_for_url(&url).await?;
            let mut messages = client
                .coordinator_channel(
                    HeaderMap::new(),
                    SetPlanRequest {
                        task_key: task,
                        task_count: 2,
                        plan: MaybeEncoded::Decoded(plan),
                        dynamic_filter_remote_producer_ids: vec![],
                        work_unit_feed_declarations: vec![WorkUnitFeedDeclaration {
                            id,
                            partitions: 1,
                            demand: true,
                        }],
                        target_worker_url: url,
                        query_start_time_ns: 0,
                    },
                    UnboundedReceiverStream::new(rx)
                        .map(crate::work_unit_feed::set_work_unit_send_time)
                        .boxed(),
                    ExecutionPlanMetricsSet::new(),
                    &ctx,
                )
                .await?;
            let demands = Arc::clone(&feeds);
            forwarders.spawn(async move {
                while let Some(message) = messages.try_next().await? {
                    if let WorkerToCoordinatorMsg::WorkUnitDemand(demand) = message {
                        demands.request(task, demand)?.await?;
                    }
                }
                Ok::<_, datafusion::common::DataFusionError>(())
            });
            let mut outputs = client
                .execute_task(
                    HeaderMap::new(),
                    ExecuteTaskRequest {
                        task_key: task,
                        target_partition_start: 0,
                        target_partition_end: 1,
                        producer_head: ProducerHead::None,
                    },
                    ExecutionPlanMetricsSet::new(),
                    &ctx,
                )
                .await?;
            let stream = outputs.remove(0);
            consumers.spawn(async move { stream.try_collect::<Vec<_>>().await });
        }
        // ID1 is held in an executing worker, never in the coordinator feed.
        // The other worker must finish all remaining IDs before it is released.
        let mut events = vec![];
        tokio::time::timeout(std::time::Duration::from_secs(20), async {
            for _ in 0..8 {
                events.push(observed.recv().await.unwrap());
            }
        })
        .await
        .expect("demand feed deadlocked");
        let slow = events.iter().find(|(_, id)| *id == 1).unwrap().0;
        assert!(
            events
                .iter()
                .filter(|(_, id)| *id > 1)
                .all(|(rank, _)| *rank != slow)
        );
        gate.release.add_permits(1);
        let mut ids = vec![];
        let mut retained = None;
        while let Some(result) = consumers.join_next().await {
            for batch in result.unwrap()? {
                retained = Some(batch.column(0).slice(0,1));
                let values = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                let ranks = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                for i in 0..batch.num_rows() {
                    ids.push((values.value(i).to_owned(), ranks.value(i)));
                }
            }
        }
        ids.sort();
        assert_eq!(
            ids.iter().map(|(id, _)| id.as_str()).collect::<Vec<_>>(),
            (1..=8).map(|id| format!("frozen:{id}")).collect::<Vec<_>>()
        );
        for worker in &workers { worker.shutdown().await; }
        drop(senders); drop(feeds); drop(feed);
        forwarders.abort_all(); while forwarders.join_next().await.is_some() {}
        servers.abort_all(); while servers.join_next().await.is_some() {}
        drop(workers);
        let pool = Arc::clone(ctx.memory_pool());
        drop(ctx);
        assert!(pool.reserved()>0,"projected child lost its byte owner");
        drop(retained);
        assert_eq!(pool.reserved(),0,"shutdown retained query buffers");
        Ok(())
    }
}
