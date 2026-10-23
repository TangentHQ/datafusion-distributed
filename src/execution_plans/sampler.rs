use crate::common::{TreeNodeExt, require_one_child, vec_cast};
use crate::{
    BytesCounterMetric, BytesMetricExt, GaugeMetricExt, LatencyMetricExt, LoadInfo, MaxGaugeMetric,
    MaxLatencyMetric, P50LatencyMetric,
};
use datafusion::arrow::array::Array;
use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::HashSet;
use datafusion::common::hash_utils::create_hashes_with_hasher;
use datafusion::common::runtime::SpawnedTask;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{DataFusionError, Result, exec_datafusion_err, exec_err};
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr_common::metrics::{Gauge, MetricValue, MetricsSet};
use datafusion::physical_plan::metrics::MetricValue::PeakMemoryUsage;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricBuilder, Time};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
};
use futures::stream::FusedStream;
use futures::{FutureExt, Stream, StreamExt, TryFutureExt};
use std::collections::VecDeque;
use std::fmt::{Debug, Formatter};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock, Mutex, OnceLock};
use std::task::{Context, Poll};
use std::time::Instant;
use tokio::sync::{oneshot, watch};

/// How many [RecordBatch]s to allow the input stream to yield synchronously (without yielding back
/// to tokio) before short-circuiting buffering.
const READY_CHUNK_LIMIT: usize = 64;
/// Sampling is a bounded prefix, not a second materialization of the query.
const READY_BYTES: usize = 16 * 1024 * 1024;
/// Maximum number of rows sampled from the peek queue when estimating per-column NDV.
const NDV_MAX_ROWS_SAMPLE: usize = 1000;

#[derive(Debug)]
pub struct SamplerExec {
    pub(crate) input: Arc<dyn ExecutionPlan>,
    pub(crate) metric_set: ExecutionPlanMetricsSet,
    pub(crate) partition_samplers: Vec<PartitionSampler>,
    pub(crate) execution_started: Arc<AtomicBool>,
}

/// Metrics that quantify how long the sampler held data in memory before the consumer
/// (real execution) attached, plus the peak accumulated size reached. All metrics are shared
/// across the partition samplers; the latency metrics aggregate per-partition observations.
#[derive(Debug, Clone)]
pub(crate) struct SamplerExecMetrics {
    /// Time since [SamplerExec::gate_for_first_sampler] was called until the first batch from
    /// the input arrived
    kick_off_to_fist_batch_p50: P50LatencyMetric,
    kick_off_to_fist_batch_max: MaxLatencyMetric,
    /// Time since [SamplerExec::gate_for_first_sampler] was called until the [LoadInfo] message
    /// was sent.
    kick_off_to_load_info_sent_p50: P50LatencyMetric,
    kick_off_to_load_info_sent_max: MaxLatencyMetric,
    /// Time since [SamplerExec::gate_for_first_sampler] was called until the node was properly
    /// executed with [SamplerExec::execute].
    kick_off_to_execution_p50: P50LatencyMetric,
    kick_off_to_execution_max: MaxLatencyMetric,
    /// Maximum number of record batches peeked by a sampler.
    max_batches_peeked: MaxGaugeMetric,
    /// Peak memory accumulated by any partition sampler during the sampling phase.
    max_mem_used: Gauge,
    /// Bytes ready at the moment of reporting load info.
    bytes_ready: BytesCounterMetric,
    /// Elapsed compute while sampling.
    elapsed_compute: Time,
}

impl SamplerExecMetrics {
    fn new(metric_set: &ExecutionPlanMetricsSet) -> Self {
        let bdr = || MetricBuilder::new(metric_set);
        Self {
            kick_off_to_fist_batch_p50: bdr().p50_latency("kick_off_to_first_batch_p50"),
            kick_off_to_fist_batch_max: bdr().max_latency("kick_off_to_first_batch_max"),
            kick_off_to_load_info_sent_p50: bdr().p50_latency("kick_off_to_load_info_sent_p50"),
            kick_off_to_load_info_sent_max: bdr().max_latency("kick_off_to_load_info_sent_max"),
            kick_off_to_execution_p50: bdr().p50_latency("kick_off_to_execution_p50"),
            kick_off_to_execution_max: bdr().max_latency("kick_off_to_execution_max"),
            max_batches_peeked: bdr().max_gauge("max_batches_peeked"),
            max_mem_used: {
                let gauge = Gauge::new();
                bdr().build(PeakMemoryUsage {
                    name: "max_mem_used".into(),
                    gauge: gauge.clone(),
                });
                gauge
            },
            bytes_ready: bdr().bytes_counter("bytes_ready"),
            elapsed_compute: {
                let time = Time::new();
                bdr().build(MetricValue::ElapsedCompute(time.clone()));
                time
            },
        }
    }
}

/// Gates the sampling until [SamplerGate::kick_off] is called. Sampling has irreversible side
/// effects in a query's lifetime, and starting it renders the query non-retryable. Only call
/// [SamplerGate::kick_off] after a worker has fully committed to being part of a query.
pub(crate) struct SamplerGate {
    sampling_start_tx_opt: Option<watch::Sender<bool>>,
    receivers: Vec<oneshot::Receiver<LoadInfo>>,
}

impl SamplerGate {
    /// Takes the [LoadInfo] receivers. This function can only be called once, as subsequent calls
    /// will always return an empty [Vec].
    pub(crate) fn take_receivers(&mut self) -> Vec<oneshot::Receiver<LoadInfo>> {
        std::mem::take(&mut self.receivers)
    }

    /// Kicks off the sampling. Once this is called, the query is no longer retryable.
    pub(crate) fn kick_off(&self) {
        if let Some(watch) = &self.sampling_start_tx_opt {
            let _ = watch.send(true);
        }
    }
}

impl SamplerExec {
    pub(crate) fn new(input: Arc<dyn ExecutionPlan>) -> Self {
        let metric_set = ExecutionPlanMetricsSet::new();
        let metric_set_clone = metric_set.clone();
        // Metrics need to be lazily initialized, otherwise the coordinator side will register
        // them when they are never relevant there, they are just relevant in workers.
        //
        // If we don't do this, the [SamplerExec] constructed during planning will register its
        // own zeroed SamplerExecMetrics in the ExecutionPlanMetricsSet, even if the metrics we care
        // about are just the ones collected in workers during execution.
        let metrics: Arc<LazyLock<_, Box<dyn FnOnce() -> SamplerExecMetrics + Send>>> =
            Arc::new(LazyLock::new(Box::new(move || {
                SamplerExecMetrics::new(&metric_set_clone)
            })));
        let partitions = input.properties().partitioning.partition_count();
        let execution_started = Arc::new(AtomicBool::new(false));
        let mut samplers = Vec::with_capacity(partitions);
        for i in 0..partitions {
            samplers.push(PartitionSampler {
                partition_idx: i,
                input: Arc::clone(&input),
                stream: Mutex::new(None),
                metrics: Arc::clone(&metrics),
                kick_off_at: Arc::new(OnceLock::new()),
                first_batch_at: Arc::new(OnceLock::new()),
                load_info_sent_at: Arc::new(OnceLock::new()),
                execution_started: Arc::clone(&execution_started),
            });
        }
        Self {
            input,
            metric_set,
            partition_samplers: samplers,
            execution_started,
        }
    }

    pub(crate) fn gate_for_first_sampler(
        plan: Arc<dyn ExecutionPlan>,
        ctx: Arc<TaskContext>,
    ) -> Result<SamplerGate> {
        let mut sampling_start_tx_opt = None;
        let mut receivers = vec![];
        plan.apply(|plan| {
            let Some(sampler) = plan.downcast_ref::<SamplerExec>() else {
                return Ok(TreeNodeRecursion::Continue);
            };
            let (sampling_start_tx, sampling_start_rx) = watch::channel(false);
            for partition_sampler in &sampler.partition_samplers {
                let rx =
                    partition_sampler.spawn_sampler(Arc::clone(&ctx), sampling_start_rx.clone())?;
                receivers.push(rx);
            }
            sampling_start_tx_opt = Some(sampling_start_tx);
            Ok(TreeNodeRecursion::Stop)
        })?;

        Ok(SamplerGate {
            sampling_start_tx_opt,
            receivers,
        })
    }
}

pub(crate) struct PartitionSampler {
    partition_idx: usize,
    input: Arc<dyn ExecutionPlan>,
    stream: Mutex<Option<SendableRecordBatchStream>>,
    execution_started: Arc<AtomicBool>,

    // Metrics state.
    metrics: Arc<LazyLock<SamplerExecMetrics, Box<dyn FnOnce() -> SamplerExecMetrics + Send>>>,
    /// Set when `kick_off` is invoked. Used at `execute()` time to record how long the
    /// sampler sampled data before the consumer attached.
    kick_off_at: Arc<OnceLock<Instant>>,
    /// Set the first time the producer task emits a `LoadInfo`. Used at `execute()` time
    /// to record the gap between the first sample and the consumer starting.
    first_batch_at: Arc<OnceLock<Instant>>,
    /// Set immediately after `sampling_tx.send()` succeeds. Used to measure the full
    /// round-trip: LoadInfo sent → coordinator collects votes → downstream plan dispatched
    /// → consumer calls execute().
    load_info_sent_at: Arc<OnceLock<Instant>>,
}

impl Debug for PartitionSampler {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PartitionSampler").finish()
    }
}

impl PartitionSampler {
    fn start_stream(&self) -> Option<SendableRecordBatchStream> {
        let Some(kick_off_at) = self.kick_off_at.get() else {
            return self.stream.lock().unwrap().take();
        };

        // Time since this sampler was kicked off until the first batch arrived.
        if let Some(t) = self.first_batch_at.get() {
            let delay = t.saturating_duration_since(*kick_off_at);
            self.metrics.kick_off_to_fist_batch_p50.add_duration(delay);
            self.metrics.kick_off_to_fist_batch_max.add_duration(delay);
        }

        // Time since the sampler was kicked off until the LoadInfo message was sent.
        if let Some(t) = self.load_info_sent_at.get() {
            let delay = t.saturating_duration_since(*kick_off_at);
            self.metrics
                .kick_off_to_load_info_sent_p50
                .add_duration(delay);
            self.metrics
                .kick_off_to_load_info_sent_max
                .add_duration(delay);
        }

        // Time since the sampler was kicked off until it started executing.
        let delay = kick_off_at.elapsed();
        self.metrics.kick_off_to_execution_p50.add_duration(delay);
        self.metrics.kick_off_to_execution_max.add_duration(delay);

        self.stream.lock().unwrap().take()
    }

    fn spawn_sampler(
        &self,
        ctx: Arc<TaskContext>,
        mut sampling_start_rx: watch::Receiver<bool>,
    ) -> Result<oneshot::Receiver<LoadInfo>> {
        let _ = self.kick_off_at.set(Instant::now());
        let (sampling_tx, sampling_rx) = oneshot::channel();

        let input = Arc::clone(&self.input);
        let partition_idx = self.partition_idx;
        let schema = input.schema();
        let elapsed_compute = self.metrics.elapsed_compute.clone();
        let first_batch_at = Arc::clone(&self.first_batch_at);
        let n_cols = self.input.schema().fields.len();

        let mut reporter = LoadInfoDropHandler {
            plan: Arc::clone(&self.input),
            load_info: zero_load_info(partition_idx, n_cols),
            sampling_tx: Some(sampling_tx),
            load_info_sent_at: Arc::clone(&self.load_info_sent_at),
            bytes_ready_metric: self.metrics.bytes_ready.clone(),
            omit: Arc::clone(&self.execution_started),
        };

        let mut peek = RecordBatchPeek {
            peek: VecDeque::new(),
            n_cols,
            max_mem_used: self.metrics.max_mem_used.clone(),
            max_batches_peeked: self.metrics.max_batches_peeked.clone(),
            memory_reservation: Arc::new(
                MemoryConsumer::new(format!("PartitionSampler[{partition_idx}]"))
                    .register(ctx.memory_pool()),
            ),
            first_batch_at: Arc::clone(&self.first_batch_at),
        };

        let task = SpawnedTask::spawn(async move {
            sampling_start_rx
                .wait_for(|started| *started)
                .await
                .map_err(|_| exec_datafusion_err!("Sampling start gate closed before kickoff"))?;

            // Instantiating an input stream may itself start executing upstream work. Do not do
            // that until this worker has committed to sampling: a failed coordinator-channel
            // attempt must remain retryable without consuming upstream resources.
            let mut input_stream = input.execute(partition_idx, ctx)?.fuse();

            // Admit one batch before retaining it. `ready_chunks` first
            // accumulated an uncharged Vec of every synchronously ready batch.
            let Some(first) = input_stream.next().await else {
                reporter.load_info.reached_eos = true;
                reporter.report(&peek);
                return Ok(peek.chain(input_stream).boxed());
            };
            let _guard = elapsed_compute.timer();
            first_batch_at.get_or_init(Instant::now);
            peek.push(first?)?;
            while peek.peek.len() < READY_CHUNK_LIMIT
                && peek.memory_reservation.size() < READY_BYTES
            {
                match input_stream.next().now_or_never() {
                    Some(Some(batch)) => peek.push(batch?)?,
                    Some(None) | None => break,
                }
            }
            // If the input stream is already terminated, every batch it will ever produce is now in
            // `peek`, so this partition has reached EOS and its `rows_ready` / `bytes_ready` are
            // final rather than a partial snapshot.
            reporter.load_info.reached_eos = input_stream.is_terminated();
            reporter.report(&peek);

            Ok(peek.chain(input_stream).boxed())
        });

        let stream = async move {
            task.await
                .map_err(|err| DataFusionError::Internal(err.to_string()))?
        }
        .try_flatten_stream();

        self.stream
            .lock()
            .expect("poisoned lock")
            .replace(Box::pin(RecordBatchStreamAdapter::new(schema, stream)));

        Ok(sampling_rx)
    }
}

/// Wraps a [LoadInfo] and emits it on [Drop] through the provided [oneshot] channel.
///
/// Emitting on drop ensures that it's always emitted.
struct LoadInfoDropHandler {
    omit: Arc<AtomicBool>,
    plan: Arc<dyn ExecutionPlan>,

    load_info: LoadInfo,
    bytes_ready_metric: BytesCounterMetric,
    sampling_tx: Option<oneshot::Sender<LoadInfo>>,
    load_info_sent_at: Arc<OnceLock<Instant>>,
}

impl LoadInfoDropHandler {
    fn report(mut self, peek: &RecordBatchPeek) {
        if self.omit.load(Ordering::Relaxed) {
            return;
        }

        self.set_per_col_bytes_ready(peek.per_col_bytes_ready());
        self.set_rows_ready(peek.rows_ready());
        self.set_per_col_ndv(peek.per_col_ndv());
        self.set_per_col_null_pct(peek.per_col_null_pct());
    }

    fn set_per_col_bytes_ready(&mut self, bytes_ready: Vec<usize>) {
        self.load_info.per_column_bytes_ready = vec_cast(&bytes_ready);
        self.bytes_ready_metric.add_bytes(bytes_ready.iter().sum());
    }

    fn set_rows_ready(&mut self, rows_ready: usize) {
        self.load_info.rows_ready = rows_ready;
    }

    fn set_per_col_ndv(&mut self, per_column_ndv: Vec<f32>) {
        self.load_info.per_column_ndv_percentage = per_column_ndv;
    }

    fn set_per_col_null_pct(&mut self, per_column_null_pct: Vec<f32>) {
        self.load_info.per_column_null_percentage = per_column_null_pct;
    }

    fn set_rows_pulled_from_leaf(&mut self) {
        let mut rows_pulled_from_leaf = 0;
        let _ = self.plan.apply_driver_path(|plan| {
            if plan.children().is_empty()
                && let Some(metrics) = plan.metrics()
                && let Some(output_rows) = metrics.output_rows()
            {
                let partition_count = self.plan.output_partitioning().partition_count();
                // Here, we need to divide by `partition_count` because the `output_rows` returned
                // by the leaf node metrics is not per-partition, is global to the whole node.
                //
                // In a perfect scenario, we might be able to map the current SamplerExec partition
                // in scope to the specific partition of the leaf node, but several things can
                // happen in between these two nodes that mangle the partitions. For example, the
                // presence of a RepartitionExec or an InterleaveExec will mess with partition
                // mapping from SamplerExec to leaf node.
                //
                // Just dividing by `partition_count` gives a good approximation, but it's still
                // not perfect as it will not account for data skews.
                rows_pulled_from_leaf += output_rows / partition_count;
            }
            Ok(TreeNodeRecursion::Continue)
        });
        self.load_info.rows_pulled_from_leaf = rows_pulled_from_leaf;
    }
}

impl Drop for LoadInfoDropHandler {
    fn drop(&mut self) {
        if self.omit.load(Ordering::Relaxed) {
            return;
        }
        self.set_rows_pulled_from_leaf();
        if let Some(sampling_tx) = self.sampling_tx.take() {
            let _ = sampling_tx.send(std::mem::take(&mut self.load_info));
            let _ = self.load_info_sent_at.set(Instant::now());
        }
    }
}

fn zero_load_info(partition_idx: usize, n_cols: usize) -> LoadInfo {
    LoadInfo {
        partition: partition_idx,
        rows_ready: 0,
        per_column_bytes_ready: vec![0; n_cols],
        per_column_ndv_percentage: vec![0.0; n_cols],
        per_column_null_percentage: vec![0.0; n_cols],
        rows_pulled_from_leaf: 0,
        reached_eos: false,
    }
}

struct RecordBatchPeek {
    peek: VecDeque<RecordBatch>,
    n_cols: usize,
    max_batches_peeked: MaxGaugeMetric,
    max_mem_used: Gauge,
    memory_reservation: Arc<MemoryReservation>,
    first_batch_at: Arc<OnceLock<Instant>>,
}

impl RecordBatchPeek {
    fn push(&mut self, batch: RecordBatch) -> Result<()> {
        let batch_size = record_batch_size(&batch);
        if self.peek.is_empty() {
            let _ = self.first_batch_at.set(Instant::now());
        }
        self.memory_reservation.try_grow(batch_size)?;
        self.max_mem_used.add(batch_size);
        self.peek.push_back(batch);
        self.max_batches_peeked.set_max(self.peek.len());
        Ok(())
    }

    fn per_col_bytes_ready(&self) -> Vec<usize> {
        let mut result = vec![0; self.n_cols];
        for batch in self.peek.iter() {
            for (i, col) in batch.columns().iter().enumerate() {
                result[i] += column_size(col)
            }
        }
        result
    }

    fn rows_ready(&self) -> usize {
        self.peek.iter().map(|batch| batch.num_rows()).sum()
    }

    fn per_col_ndv(&self) -> Vec<f32> {
        let total_rows: usize = self.peek.iter().map(|b| b.num_rows()).sum();
        if total_rows == 0 {
            return vec![0.0; self.n_cols];
        }

        // Hash borrowed primitive values instead of cloning ScalarValue
        // payloads. One hash set is reused per column, including very wide
        // strings; NDV is a hint and can conservatively stay unknown when its
        // scratch reservation cannot be admitted.
        let scratch = self.memory_reservation.new_empty();
        let bytes = NDV_MAX_ROWS_SAMPLE * (size_of::<usize>() + 4 * size_of::<u64>())
            + self.n_cols * size_of::<f32>()
            + 4096;
        if scratch.try_grow(bytes).is_err() {
            return vec![1.0; self.n_cols];
        }
        let mut indices = if total_rows <= NDV_MAX_ROWS_SAMPLE {
            (0..total_rows).collect::<Vec<_>>()
        } else {
            rand::seq::index::sample(&mut rand::rng(), total_rows, NDV_MAX_ROWS_SAMPLE).into_vec()
        };
        indices.sort_unstable();
        let state = std::collections::hash_map::RandomState::new();
        let mut set = HashSet::<u64>::with_capacity(indices.len());
        (0..self.n_cols)
            .map(|column| {
                set.clear();
                let mut base = 0;
                let mut position = 0;
                for batch in &self.peek {
                    let array = batch.column(column);
                    // Nested hash kernels can allocate proportional to a child
                    // array. Keep their NDV unknown rather than allocate scratch
                    // outside the admitted fixed sample budget.
                    if array.data_type().is_nested()
                        || matches!(
                            array.data_type(),
                            datafusion::arrow::datatypes::DataType::Dictionary(_, _)
                        )
                    {
                        return 1.0;
                    }
                    while position < indices.len() && indices[position] < base + batch.num_rows() {
                        let row = indices[position] - base;
                        if !array.is_null(row) {
                            let mut hash = [0];
                            if create_hashes_with_hasher([array.slice(row, 1)], &state, &mut hash)
                                .is_err()
                            {
                                return 1.0;
                            }
                            set.insert(hash[0]);
                        }
                        position += 1;
                    }
                    base += batch.num_rows();
                }
                set.len() as f32 / indices.len() as f32
            })
            .collect()
    }

    fn per_col_null_pct(&self) -> Vec<f32> {
        let total_rows: usize = self.peek.iter().map(|b| b.num_rows()).sum();
        if total_rows == 0 {
            return vec![0.0; self.n_cols];
        }
        let mut counts = vec![0usize; self.n_cols];
        for batch in &self.peek {
            for (col_idx, count) in counts.iter_mut().enumerate() {
                *count += batch.column(col_idx).null_count();
            }
        }
        counts
            .into_iter()
            .map(|c| c as f32 / total_rows as f32)
            .collect()
    }
}

impl Stream for RecordBatchPeek {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.as_mut().peek.pop_front() {
            None => Poll::Ready(None),
            Some(batch) => {
                self.memory_reservation.shrink(record_batch_size(&batch));
                Poll::Ready(Some(Ok(batch)))
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.peek.len(), Some(self.peek.len()))
    }
}

fn column_size(arr: &ArrayRef) -> usize {
    arr.to_data().get_slice_memory_size().unwrap_or(0)
}

fn record_batch_size(batch: &RecordBatch) -> usize {
    batch.columns().iter().map(column_size).sum()
}

impl DisplayAs for SamplerExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "SamplerExec: partitions={}",
            self.partition_samplers.len()
        )
    }
}

impl ExecutionPlan for SamplerExec {
    fn name(&self) -> &str {
        "SamplerExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.input.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::new(require_one_child(children)?)))
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        self.execution_started.store(true, Ordering::Relaxed);
        let Some(partition_sampler) = self.partition_samplers.get(partition) else {
            return exec_err!("Partition {partition} not available in SamplerExec");
        };
        let Some(stream) = partition_sampler.start_stream() else {
            return exec_err!("SamplerExec[{partition}] was not kicked off");
        };
        Ok(stream)
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metric_set.clone_inner())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int64Array, StringArray};
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};
    use datafusion::execution::runtime_env::RuntimeEnvBuilder;
    use datafusion::prelude::{SessionConfig, SessionContext};
    use futures::TryStreamExt;

    fn peek(pool: &Arc<dyn MemoryPool>) -> RecordBatchPeek {
        RecordBatchPeek {
            peek: VecDeque::new(),
            n_cols: 1,
            max_batches_peeked: MaxGaugeMetric::default(),
            max_mem_used: Gauge::new(),
            memory_reservation: Arc::new(MemoryConsumer::new("test sampler").register(pool)),
            first_batch_at: Arc::new(OnceLock::new()),
        }
    }

    #[test]
    fn retention_rejects_pressure_and_releases_all_admitted_bytes() {
        let batch = RecordBatch::try_from_iter([(
            "value",
            Arc::new(Int64Array::from(vec![1; 1024])) as ArrayRef,
        )])
        .unwrap();
        let bytes = record_batch_size(&batch);
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(bytes));
        let mut queue = peek(&pool);
        queue.push(batch.clone()).unwrap();
        assert!(queue.push(batch).is_err());
        assert_eq!(queue.rows_ready(), 1024);
        assert_eq!(pool.reserved(), bytes);
        drop(queue);
        assert_eq!(pool.reserved(), 0);
    }

    #[test]
    fn wide_ndv_hashes_borrow_values_and_pressure_keeps_a_conservative_hint() {
        let value = "wide".repeat(64 * 1024);
        let batch = RecordBatch::try_from_iter([(
            "value",
            Arc::new(StringArray::from(vec![
                Some(value.as_str()),
                Some(value.as_str()),
                None,
            ])) as ArrayRef,
        )])
        .unwrap();
        let bytes = record_batch_size(&batch);
        for room in [0, 128 * 1024] {
            let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(bytes + room));
            let mut queue = peek(&pool);
            queue.push(batch.clone()).unwrap();
            assert_eq!(
                queue.per_col_ndv(),
                vec![if room == 0 { 1.0 } else { 1.0 / 3.0 }]
            );
            assert_eq!(
                pool.reserved(),
                bytes,
                "NDV scratch is released after reporting"
            );
            drop(queue);
            assert_eq!(pool.reserved(), 0);
        }
    }

    #[tokio::test]
    async fn ready_input_samples_a_bounded_prefix_then_preserves_every_row_in_order() {
        let batches = (0..512)
            .map(|index| {
                RecordBatch::try_from_iter([(
                    "value",
                    Arc::new(Int64Array::from(vec![index])) as ArrayRef,
                )])
                .unwrap()
            })
            .collect::<Vec<_>>();
        let source =
            MemorySourceConfig::try_new_exec(&[batches.clone()], batches[0].schema(), None)
                .unwrap();
        let plan: Arc<dyn ExecutionPlan> = Arc::new(SamplerExec::new(source));
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(128 * 1024));
        let runtime = Arc::new(
            RuntimeEnvBuilder::new()
                .with_memory_pool(pool.clone())
                .build()
                .unwrap(),
        );
        let ctx = SessionContext::new_with_config_rt(SessionConfig::new(), runtime).task_ctx();
        let mut gate = SamplerExec::gate_for_first_sampler(plan.clone(), ctx.clone()).unwrap();
        let report = gate.take_receivers().pop().unwrap();
        gate.kick_off();
        let report = report.await.unwrap();
        assert!(
            report.rows_ready > 0 && report.rows_ready < 512,
            "sampling cannot drain all ready input"
        );
        let output = plan
            .execute(0, ctx)
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(output, batches);
        drop(plan);
        assert_eq!(pool.reserved(), 0);
    }
}
