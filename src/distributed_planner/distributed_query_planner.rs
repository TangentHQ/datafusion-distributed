use crate::common::TreeNodeExt;
use crate::distributed_planner::inject_network_boundaries::{
    CardinalityBasedNetworkBoundaryBuilder, inject_network_boundaries,
};
use crate::distributed_planner::insert_broadcast::insert_broadcast_execs;
use crate::distributed_planner::insert_children_isolator_union::insert_children_isolator_unions;
use crate::distributed_planner::normalize_collect_joins::normalize_collect_joins;
use crate::distributed_planner::partial_reduce_below_network_shuffles::partial_reduce_below_network_shuffles;
use crate::distributed_planner::prepare_network_boundaries::prepare_network_boundaries;
use crate::distributed_planner::push_fetch_into_network_coalesce::push_fetch_into_network_coalesce;
use crate::events::{ScaleUpLeafNodeEvent, ScaleUpLeafNodeHandlers};
use crate::explain_analyze::DistributedAnalyzeExec;
use crate::{DistributedConfig, DistributedExec, NetworkBoundaryExt};
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::execution::context::QueryPlanner;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::analyze::AnalyzeExec;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::{
    ChildrenPropertiesMode, ExecutionPlan, ExecutionPlanProperties, ReplaceChildrenOptions,
};
use datafusion::physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner};
use futures::future::BoxFuture;
use std::sync::Arc;

/// Upper bound for simultaneous HTTP/2 streams from one member to a peer.
/// Call this on the prepared dynamic physical plan, before execution. `resident`
/// is the maximum tasks of *each* stage routed to one member; callers must
/// disable route failover or include its placement skew in that bound.
///
/// Boundary injection creates at most one stage per shaped node, one per
/// input edge for an otherwise elided native distribution, and a final root
/// gather. AQE changes task counts, not this shaped operator inventory. Each
/// resident producer/consumer pair opens one ExecuteTask request: Direct
/// and Broadcast batch all of that consumer task's partitions in one range;
/// TwoPhase uses one range per producer task. WorkerConnectionPool caches
/// those exact ranges. Control streams remain live across the entire DAG.
/// This bounds channel metadata, not protobuf body allocation.
pub fn peer_streams(
    plan: &Arc<dyn ExecutionPlan>,
    resident: usize,
) -> datafusion::common::Result<u32> {
    use datafusion::common::{plan_datafusion_err, tree_node::TreeNodeRecursion};
    if resident == 0 {
        return datafusion::common::plan_err!("Distributed member has no admitted task slots");
    }
    let overflow = || plan_datafusion_err!("Distributed channel envelope overflow");
    let mut stages = 1usize;
    plan.apply(|plan| {
        stages = stages
            .checked_add(1 + plan.children().len())
            .ok_or_else(overflow)?;
        Ok(TreeNodeRecursion::Continue)
    })?;
    let streams = resident
        .checked_mul(resident)
        .and_then(|data| data.checked_add(resident))
        .and_then(|per_stage| per_stage.checked_mul(stages))
        .and_then(|all| all.checked_add(2))
        .ok_or_else(overflow)?;
    u32::try_from(streams).map_err(|_| overflow())
}

/// Transforms a single-node physical plan into a distributed plan by injecting network
/// boundaries between stages.
///
/// The pipeline runs four passes in order:
///
/// 1. **Pre-distribution shaping.** A [CoalescePartitionsExec] is wrapped on top of the plan
///    when it has more than one output partition (so [inject_network_boundaries] later sees a
///    partition-collecting parent and injects a `NetworkCoalesceExec` above its child). Then
///    [insert_broadcast_execs] adds `BroadcastExec` nodes on the build side of `CollectLeft`
///    hash joins so those build sides can later be wrapped in `NetworkBroadcastExec`.
///    [insert_children_isolator_unions] replaces `UnionExec` nodes with single-node-correct
///    `ChildrenIsolatorUnionExec` placeholders whose task maps are finalized during boundary
///    injection.
///
/// 2. **Boundary injection.** [inject_network_boundaries] walks the plan, computes a task count
///    for each node, and inserts `NetworkShuffleExec` / `NetworkBroadcastExec` /
///    `NetworkCoalesceExec` above the nodes that delimit a stage (hash `RepartitionExec`s,
///    build-side `BroadcastExec`s, and any node sitting under a `CoalescePartitionsExec` /
///    `SortPreservingMergeExec`).
///
/// 3. **Boundary preparation.** [prepare_network_boundaries] readies each injected boundary
///    for execution: elides ones that aren't actually needed and finalises the survivors. If
///    no boundary survives, this function returns `None`.
///
/// 4. **Shuffle-volume optimization.** [partial_reduce_below_network_shuffles] inserts partial
///    aggregation nodes underneath hash shuffles where it can, so less data crosses the network.
#[derive(Debug)]
pub(crate) struct DistributedQueryPlanner {
    pub(crate) prev: Option<Arc<dyn QueryPlanner + Send + Sync>>,
}

#[async_trait]
impl QueryPlanner for DistributedQueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session: &dyn Session,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let original_plan = match &self.prev {
            None => {
                // Use the default physical planner.
                let planner = DefaultPhysicalPlanner::default();
                planner.create_physical_plan(logical_plan, session).await?
            }
            Some(prev) => prev.create_physical_plan(logical_plan, session).await?,
        };

        create_distributed_plan(original_plan, session).await
    }
}

/// Applies distributed planning to an already-created physical plan.
///
/// Inputs to [`AnalyzeExec`] are planned recursively. When one becomes distributed, the native
/// node is replaced by [`DistributedAnalyzeExec`]. The future is boxed because recursive `async
/// fn`s would otherwise have an infinitely-sized return type.
fn create_distributed_plan(
    original_plan: Arc<dyn ExecutionPlan>,
    session: &dyn Session,
) -> BoxFuture<'_, datafusion::common::Result<Arc<dyn ExecutionPlan>>> {
    Box::pin(async move {
        if original_plan.is::<DistributedExec>() {
            return Ok(original_plan);
        }

        if let Some(analyze) = original_plan.downcast_ref::<AnalyzeExec>() {
            // Recursively distribute the query being analyzed. Replace `AnalyzeExec` only when its
            // input became distributed; otherwise retain DataFusion's native implementation.
            let input = create_distributed_plan(Arc::clone(analyze.input()), session).await?;
            return match input.is::<DistributedExec>() {
                true => Ok(Arc::new(DistributedAnalyzeExec::new(analyze, input))),
                false => Arc::new(analyze.clone()).replace_children(
                    vec![input],
                    ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
                ),
            };
        }

        let session_cfg = session.config();
        let cfg = session_cfg.options();
        let d_cfg = DistributedConfig::from_config_options(cfg)?;

        // The plan already contains network boundaries set by the user. Just ensure they have nice
        // unique identifiers for each stage, and move forward with it.
        if original_plan.exists(|plan| Ok(plan.is_network_boundary()))? {
            // Ensure the leaves are appropriately scaled up.
            let scaled = original_plan.transform_down_with_task_count(1, |plan, task_count| {
                if !plan.children().is_empty() {
                    return Ok(Transformed::no(plan));
                }
                let ev = ScaleUpLeafNodeEvent {
                    plan: &plan,
                    task_count,
                    session_config: session_cfg,
                };
                match ScaleUpLeafNodeHandlers::handle(ev) {
                    None => Ok(Transformed::no(plan)),
                    Some(response) => Ok(Transformed::yes(response?.plan)),
                }
            })?;
            // Ensure the stages in the plan have nice unique identifiers.
            let plan = prepare_network_boundaries(scaled.data)?;
            if !plan.exists(|plan| Ok(plan.is_network_boundary()))? {
                return Ok(plan);
            }
            let plan = push_fetch_into_network_coalesce(plan)?;
            return Ok(create_distributed_exec(Arc::clone(&plan), d_cfg));
        }

        let mut plan = Arc::clone(&original_plan);

        let cfg = session.config_options();
        plan = normalize_collect_joins(plan, cfg)?;

        if plan.output_partitioning().partition_count() > 1 {
            plan = Arc::new(CoalescePartitionsExec::new(plan));
        }

        plan = insert_broadcast_execs(plan, cfg)?;
        plan = insert_children_isolator_unions(plan, cfg)?;

        if d_cfg.dynamic_task_count {
            // The task count will be decided dynamically at execution time.
            return Ok(create_distributed_exec(Arc::clone(&plan), d_cfg));
        }

        // Compute per-node task counts and inject `Network*Exec` nodes at the stage boundaries.
        plan = inject_network_boundaries(plan, CardinalityBasedNetworkBoundaryBuilder, session_cfg)
            .await?;

        plan = prepare_network_boundaries(plan)?;
        if !plan.exists(|plan| Ok(plan.is_network_boundary()))? {
            return Ok(original_plan);
        }

        let plan = partial_reduce_below_network_shuffles(plan, cfg)?;
        let plan = push_fetch_into_network_coalesce(plan)?;

        Ok(create_distributed_exec(Arc::clone(&plan), d_cfg))
    })
}

fn create_distributed_exec(
    plan: Arc<dyn ExecutionPlan>,
    d_cfg: &DistributedConfig,
) -> Arc<dyn ExecutionPlan> {
    Arc::new(
        DistributedExec::new(plan)
            .with_metrics_collection(d_cfg.collect_metrics)
            .with_dynamic_filter_collection(d_cfg.collect_dynamic_filters),
    )
}

#[cfg(test)]
mod tests {
    use crate::assert_snapshot;
    use crate::test_utils::plans::{TestPlanBuilder, build_side_one_desired_task_count_handler};
    /* schema for the "weather" table

     MinTemp [type=DOUBLE] [repetitiontype=OPTIONAL]
     MaxTemp [type=DOUBLE] [repetitiontype=OPTIONAL]
     Rainfall [type=DOUBLE] [repetitiontype=OPTIONAL]
     Evaporation [type=DOUBLE] [repetitiontype=OPTIONAL]
     Sunshine [type=BYTE_ARRAY] [convertedtype=UTF8] [repetitiontype=OPTIONAL]
     WindGustDir [type=BYTE_ARRAY] [convertedtype=UTF8] [repetitiontype=OPTIONAL]
     WindGustSpeed [type=BYTE_ARRAY] [convertedtype=UTF8] [repetitiontype=OPTIONAL]
     WindDir9am [type=BYTE_ARRAY] [convertedtype=UTF8] [repetitiontype=OPTIONAL]
     WindDir3pm [type=BYTE_ARRAY] [convertedtype=UTF8] [repetitiontype=OPTIONAL]
     WindSpeed9am [type=BYTE_ARRAY] [convertedtype=UTF8] [repetitiontype=OPTIONAL]
     WindSpeed3pm [type=INT64] [convertedtype=INT_64] [repetitiontype=OPTIONAL]
     Humidity9am [type=INT64] [convertedtype=INT_64] [repetitiontype=OPTIONAL]
     Humidity3pm [type=INT64] [convertedtype=INT_64] [repetitiontype=OPTIONAL]
     Pressure9am [type=DOUBLE] [repetitiontype=OPTIONAL]
     Pressure3pm [type=DOUBLE] [repetitiontype=OPTIONAL]
     Cloud9am [type=INT64] [convertedtype=INT_64] [repetitiontype=OPTIONAL]
     Cloud3pm [type=INT64] [convertedtype=INT_64] [repetitiontype=OPTIONAL]
     Temp9am [type=DOUBLE] [repetitiontype=OPTIONAL]
     Temp3pm [type=DOUBLE] [repetitiontype=OPTIONAL]
     RainToday [type=BYTE_ARRAY] [convertedtype=UTF8] [repetitiontype=OPTIONAL]
     RISK_MM [type=DOUBLE] [repetitiontype=OPTIONAL]
     RainTomorrow [type=BYTE_ARRAY] [convertedtype=UTF8] [repetitiontype=OPTIONAL]
    */

    #[tokio::test]
    async fn test_select_all() {
        let query = r#"
        SELECT * FROM weather
        "#;
        let physical_plan_ascii = TestPlanBuilder::default()
            .physical_plan_as_ascii(query, false)
            .await;
        assert_snapshot!(physical_plan_ascii, @r"
        ┌───── DistributedExec
        │ CoalescePartitionsExec
        │   [Stage 1] => NetworkCoalesceExec: output_partitions=9, input_tasks=3
        └──────────────────────────────────────────────────
          ┌───── Stage 1 ── tasks=3, partitions=9
          │ DistributedLeafExec:
          │   t0: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000000.parquet:<int>..<int>, /testdata/weather/result-000001.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[MinTemp, MaxTemp, Rainfall, Evaporation, Sunshine, WindGustDir, WindGustSpeed, WindDir9am, WindDir3pm, WindSpeed9am, WindSpeed3pm, Humidity9am, Humidity3pm, Pressure9am, Pressure3pm, Cloud9am, Cloud3pm, Temp9am, Temp3pm, RainToday, RISK_MM, RainTomorrow], file_type=parquet
          │   t1: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000001.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[MinTemp, MaxTemp, Rainfall, Evaporation, Sunshine, WindGustDir, WindGustSpeed, WindDir9am, WindDir3pm, WindSpeed9am, WindSpeed3pm, Humidity9am, Humidity3pm, Pressure9am, Pressure3pm, Cloud9am, Cloud3pm, Temp9am, Temp3pm, RainToday, RISK_MM, RainTomorrow], file_type=parquet
          │   t2: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000001.parquet:<int>..<int>, /testdata/weather/result-000002.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[MinTemp, MaxTemp, Rainfall, Evaporation, Sunshine, WindGustDir, WindGustSpeed, WindDir9am, WindDir3pm, WindSpeed9am, WindSpeed3pm, Humidity9am, Humidity3pm, Pressure9am, Pressure3pm, Cloud9am, Cloud3pm, Temp9am, Temp3pm, RainToday, RISK_MM, RainTomorrow], file_type=parquet
          └──────────────────────────────────────────────────
        ");
    }

    #[tokio::test]
    async fn test_aggregation() {
        let query = r#"
        SELECT count(*), "RainToday" FROM weather GROUP BY "RainToday" ORDER BY count(*)
        "#;
        let physical_plan_ascii = TestPlanBuilder::default()
            .distributed_cardinality_effect_task_scale_factor(1.5)
            .physical_plan_as_ascii(query, false)
            .await;
        assert_snapshot!(physical_plan_ascii, @"
        ┌───── DistributedExec
        │ SortPreservingMergeExec: [count(*)@0 ASC NULLS LAST]
        │   [Stage 2] => NetworkCoalesceExec: output_partitions=8, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 2 ── tasks=2, partitions=4
          │ ProjectionExec: expr=[count(Int64(1))@1 as count(*), RainToday@0 as RainToday]
          │   SortExec: expr=[count(Int64(1))@1 ASC NULLS LAST], preserve_partitioning=[true]
          │     AggregateExec: mode=FinalPartitioned, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
          │       [Stage 1] => NetworkShuffleExec: output_partitions=4, input_tasks=3
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── tasks=3, partitions=8
            │ RepartitionExec: partitioning=Hash([RainToday@0], 8), input_partitions=3
            │   AggregateExec: mode=Partial, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
            │     DistributedLeafExec:
            │       t0: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000000.parquet:<int>..<int>, /testdata/weather/result-000001.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[RainToday], file_type=parquet
            │       t1: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000001.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[RainToday], file_type=parquet
            │       t2: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000001.parquet:<int>..<int>, /testdata/weather/result-000002.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[RainToday], file_type=parquet
            └──────────────────────────────────────────────────
        ");
    }

    #[tokio::test]
    async fn test_aggregation_with_fewer_workers_than_files() {
        let query = r#"
        SELECT count(*), "RainToday" FROM weather GROUP BY "RainToday" ORDER BY count(*)
        "#;
        let physical_plan_ascii = TestPlanBuilder::default()
            .num_workers(2)
            .physical_plan_as_ascii(query, false)
            .await;
        assert_snapshot!(physical_plan_ascii, @"
        ┌───── DistributedExec
        │ SortPreservingMergeExec: [count(*)@0 ASC NULLS LAST]
        │   [Stage 2] => NetworkCoalesceExec: output_partitions=8, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 2 ── tasks=2, partitions=4
          │ ProjectionExec: expr=[count(Int64(1))@1 as count(*), RainToday@0 as RainToday]
          │   SortExec: expr=[count(Int64(1))@1 ASC NULLS LAST], preserve_partitioning=[true]
          │     AggregateExec: mode=FinalPartitioned, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
          │       [Stage 1] => NetworkShuffleExec: output_partitions=4, input_tasks=2
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── tasks=2, partitions=8
            │ RepartitionExec: partitioning=Hash([RainToday@0], 8), input_partitions=3
            │   AggregateExec: mode=Partial, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
            │     DistributedLeafExec:
            │       t0: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000000.parquet:<int>..<int>, /testdata/weather/result-000001.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[RainToday], file_type=parquet
            │       t1: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000001.parquet:<int>..<int>, /testdata/weather/result-000002.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[RainToday], file_type=parquet
            └──────────────────────────────────────────────────
        ");
    }

    #[tokio::test]
    async fn test_aggregation_with_0_workers() {
        let query = r#"
        SELECT count(*), "RainToday" FROM weather GROUP BY "RainToday" ORDER BY count(*)
        "#;
        let physical_plan_ascii = TestPlanBuilder::default()
            .num_workers(0)
            .physical_plan_as_ascii(query, false)
            .await;
        assert_snapshot!(physical_plan_ascii, @"
        SortPreservingMergeExec: [count(*)@0 ASC NULLS LAST]
          ProjectionExec: expr=[count(Int64(1))@1 as count(*), RainToday@0 as RainToday]
            SortExec: expr=[count(Int64(1))@1 ASC NULLS LAST], preserve_partitioning=[true]
              AggregateExec: mode=FinalPartitioned, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
                RepartitionExec: partitioning=Hash([RainToday@0], 4), input_partitions=3
                  AggregateExec: mode=Partial, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
                    DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[RainToday], file_type=parquet
        ");
    }

    #[tokio::test]
    async fn test_aggregation_with_high_cardinality_factor() {
        let query = r#"
        SELECT count(*), "RainToday" FROM weather GROUP BY "RainToday" ORDER BY count(*)
        "#;
        let physical_plan_ascii = TestPlanBuilder::default()
            .distributed_cardinality_effect_task_scale_factor(3.0)
            .physical_plan_as_ascii(query, false)
            .await;
        assert_snapshot!(physical_plan_ascii, @"
        ┌───── DistributedExec
        │ SortPreservingMergeExec: [count(*)@0 ASC NULLS LAST]
        │   ProjectionExec: expr=[count(Int64(1))@1 as count(*), RainToday@0 as RainToday]
        │     SortExec: expr=[count(Int64(1))@1 ASC NULLS LAST], preserve_partitioning=[true]
        │       AggregateExec: mode=FinalPartitioned, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
        │         [Stage 1] => NetworkShuffleExec: output_partitions=4, input_tasks=3
        └──────────────────────────────────────────────────
          ┌───── Stage 1 ── tasks=3, partitions=4
          │ RepartitionExec: partitioning=Hash([RainToday@0], 4), input_partitions=3
          │   AggregateExec: mode=Partial, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
          │     DistributedLeafExec:
          │       t0: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000000.parquet:<int>..<int>, /testdata/weather/result-000001.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[RainToday], file_type=parquet
          │       t1: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000001.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[RainToday], file_type=parquet
          │       t2: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000001.parquet:<int>..<int>, /testdata/weather/result-000002.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[RainToday], file_type=parquet
          └──────────────────────────────────────────────────
        ");
    }

    #[tokio::test]
    async fn test_aggregation_with_high_file_scan_config_bytes_per_task() {
        let query = r#"
        SELECT count(*), "RainToday" FROM weather GROUP BY "RainToday" ORDER BY count(*)
        "#;
        let physical_plan_ascii = TestPlanBuilder::default()
            .distributed_file_scan_config_bytes_per_partition(128 * 1024 * 1024)
            .physical_plan_as_ascii(query, false)
            .await;
        assert_snapshot!(physical_plan_ascii, @"
        SortPreservingMergeExec: [count(*)@0 ASC NULLS LAST]
          ProjectionExec: expr=[count(Int64(1))@1 as count(*), RainToday@0 as RainToday]
            SortExec: expr=[count(Int64(1))@1 ASC NULLS LAST], preserve_partitioning=[true]
              AggregateExec: mode=FinalPartitioned, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
                RepartitionExec: partitioning=Hash([RainToday@0], 4), input_partitions=3
                  AggregateExec: mode=Partial, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
                    DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[RainToday], file_type=parquet
        ");
    }

    #[tokio::test]
    async fn test_aggregation_with_partitions_per_task() {
        let query = r#"
        SELECT count(*), "RainToday" FROM weather GROUP BY "RainToday" ORDER BY count(*)
        "#;
        let physical_plan_ascii = TestPlanBuilder::default()
            .distributed_cardinality_effect_task_scale_factor(1.5)
            .physical_plan_as_ascii(query, false)
            .await;
        assert_snapshot!(physical_plan_ascii, @"
        ┌───── DistributedExec
        │ SortPreservingMergeExec: [count(*)@0 ASC NULLS LAST]
        │   [Stage 2] => NetworkCoalesceExec: output_partitions=8, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 2 ── tasks=2, partitions=4
          │ ProjectionExec: expr=[count(Int64(1))@1 as count(*), RainToday@0 as RainToday]
          │   SortExec: expr=[count(Int64(1))@1 ASC NULLS LAST], preserve_partitioning=[true]
          │     AggregateExec: mode=FinalPartitioned, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
          │       [Stage 1] => NetworkShuffleExec: output_partitions=4, input_tasks=3
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── tasks=3, partitions=8
            │ RepartitionExec: partitioning=Hash([RainToday@0], 8), input_partitions=3
            │   AggregateExec: mode=Partial, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
            │     DistributedLeafExec:
            │       t0: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000000.parquet:<int>..<int>, /testdata/weather/result-000001.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[RainToday], file_type=parquet
            │       t1: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000001.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[RainToday], file_type=parquet
            │       t2: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000001.parquet:<int>..<int>, /testdata/weather/result-000002.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[RainToday], file_type=parquet
            └──────────────────────────────────────────────────
        ");
    }

    #[tokio::test]
    async fn test_left_join() {
        let query = r#"
        SELECT a."MinTemp", b."MaxTemp" FROM weather a LEFT JOIN weather b ON a."RainToday" = b."RainToday"
        "#;
        let physical_plan_ascii = TestPlanBuilder::default()
            .physical_plan_as_ascii(query, false)
            .await;
        assert_snapshot!(physical_plan_ascii, @"
        ┌───── DistributedExec
        │ CoalescePartitionsExec
        │   [Stage 3] => NetworkCoalesceExec: output_partitions=12, input_tasks=3
        └──────────────────────────────────────────────────
          ┌───── Stage 3 ── tasks=3, partitions=4
          │ HashJoinExec: mode=Partitioned, join_type=Left, on=[(RainToday@1, RainToday@1)], projection=[MinTemp@0, MaxTemp@2]
          │   [Stage 1] => NetworkShuffleExec: output_partitions=4, input_tasks=3
          │   [Stage 2] => NetworkShuffleExec: output_partitions=4, input_tasks=3
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── tasks=3, partitions=12
            │ RepartitionExec: partitioning=Hash([RainToday@1], 12), input_partitions=3
            │   DistributedLeafExec:
            │     t0: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000000.parquet:<int>..<int>, /testdata/weather/result-000001.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[MinTemp, RainToday], file_type=parquet
            │     t1: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000001.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[MinTemp, RainToday], file_type=parquet
            │     t2: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000001.parquet:<int>..<int>, /testdata/weather/result-000002.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[MinTemp, RainToday], file_type=parquet
            └──────────────────────────────────────────────────
            ┌───── Stage 2 ── tasks=3, partitions=12
            │ RepartitionExec: partitioning=Hash([RainToday@1], 12), input_partitions=3
            │   DistributedLeafExec:
            │     t0: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000000.parquet:<int>..<int>, /testdata/weather/result-000001.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[MaxTemp, RainToday], file_type=parquet, predicate=DynamicFilter [ empty ], dynamic_rg_pruning=eligible
            │     t1: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000001.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[MaxTemp, RainToday], file_type=parquet, predicate=DynamicFilter [ empty ], dynamic_rg_pruning=eligible
            │     t2: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000001.parquet:<int>..<int>, /testdata/weather/result-000002.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[MaxTemp, RainToday], file_type=parquet, predicate=DynamicFilter [ empty ], dynamic_rg_pruning=eligible
            └──────────────────────────────────────────────────
        ");
    }

    #[tokio::test]
    async fn test_left_join_distributed() {
        let query = r#"
        WITH a AS (
            SELECT
                AVG("MinTemp") as "MinTemp",
                "RainTomorrow"
            FROM weather
            WHERE "RainToday" = 'yes'
            GROUP BY "RainTomorrow"
        ), b AS (
            SELECT
                AVG("MaxTemp") as "MaxTemp",
                "RainTomorrow"
            FROM weather
            WHERE "RainToday" = 'no'
            GROUP BY "RainTomorrow"
        )
        SELECT
            a."MinTemp",
            b."MaxTemp"
        FROM a
        LEFT JOIN b
        ON a."RainTomorrow" = b."RainTomorrow"
        "#;
        let physical_plan_ascii = TestPlanBuilder::default()
            .distributed_cardinality_effect_task_scale_factor(1.5)
            .physical_plan_as_ascii(query, false)
            .await;
        assert_snapshot!(physical_plan_ascii, @"
        ┌───── DistributedExec
        │ CoalescePartitionsExec
        │   [Stage 5] => NetworkCoalesceExec: output_partitions=8, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 5 ── tasks=2, partitions=4
          │ HashJoinExec: mode=Partitioned, join_type=Left, on=[(RainTomorrow@1, RainTomorrow@1)], projection=[MinTemp@0, MaxTemp@2]
          │   [Stage 2] => NetworkShuffleExec: output_partitions=4, input_tasks=2
          │   [Stage 4] => NetworkShuffleExec: output_partitions=4, input_tasks=2
          └──────────────────────────────────────────────────
            ┌───── Stage 2 ── tasks=2, partitions=8
            │ RepartitionExec: partitioning=Hash([RainTomorrow@1], 8), input_partitions=4
            │   ProjectionExec: expr=[avg(weather.MinTemp)@1 as MinTemp, RainTomorrow@0 as RainTomorrow]
            │     AggregateExec: mode=FinalPartitioned, gby=[RainTomorrow@0 as RainTomorrow], aggr=[avg(weather.MinTemp)]
            │       [Stage 1] => NetworkShuffleExec: output_partitions=4, input_tasks=3
            └──────────────────────────────────────────────────
              ┌───── Stage 1 ── tasks=3, partitions=8
              │ RepartitionExec: partitioning=Hash([RainTomorrow@0], 8), input_partitions=4
              │   AggregateExec: mode=Partial, gby=[RainTomorrow@1 as RainTomorrow], aggr=[avg(weather.MinTemp)]
              │     FilterExec: RainToday@1 = yes, projection=[MinTemp@0, RainTomorrow@2]
              │       RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=3
              │         DistributedLeafExec:
              │           t0: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000000.parquet:<int>..<int>, /testdata/weather/result-000001.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[MinTemp, RainToday, RainTomorrow], file_type=parquet, predicate=RainToday@19 = yes, pruning_predicate=RainToday_null_count@2 != row_count@3 AND RainToday_min@0 <= yes AND yes <= RainToday_max@1, required_guarantees=[RainToday in (yes)]
              │           t1: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000001.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[MinTemp, RainToday, RainTomorrow], file_type=parquet, predicate=RainToday@19 = yes, pruning_predicate=RainToday_null_count@2 != row_count@3 AND RainToday_min@0 <= yes AND yes <= RainToday_max@1, required_guarantees=[RainToday in (yes)]
              │           t2: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000001.parquet:<int>..<int>, /testdata/weather/result-000002.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[MinTemp, RainToday, RainTomorrow], file_type=parquet, predicate=RainToday@19 = yes, pruning_predicate=RainToday_null_count@2 != row_count@3 AND RainToday_min@0 <= yes AND yes <= RainToday_max@1, required_guarantees=[RainToday in (yes)]
              └──────────────────────────────────────────────────
            ┌───── Stage 4 ── tasks=2, partitions=8
            │ RepartitionExec: partitioning=Hash([RainTomorrow@1], 8), input_partitions=4
            │   ProjectionExec: expr=[avg(weather.MaxTemp)@1 as MaxTemp, RainTomorrow@0 as RainTomorrow]
            │     AggregateExec: mode=FinalPartitioned, gby=[RainTomorrow@0 as RainTomorrow], aggr=[avg(weather.MaxTemp)]
            │       [Stage 3] => NetworkShuffleExec: output_partitions=4, input_tasks=3
            └──────────────────────────────────────────────────
              ┌───── Stage 3 ── tasks=3, partitions=8
              │ RepartitionExec: partitioning=Hash([RainTomorrow@0], 8), input_partitions=4
              │   AggregateExec: mode=Partial, gby=[RainTomorrow@1 as RainTomorrow], aggr=[avg(weather.MaxTemp)]
              │     FilterExec: RainToday@1 = no, projection=[MaxTemp@0, RainTomorrow@2]
              │       RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=3
              │         DistributedLeafExec:
              │           t0: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000000.parquet:<int>..<int>, /testdata/weather/result-000001.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[MaxTemp, RainToday, RainTomorrow], file_type=parquet, predicate=RainToday@19 = no AND DynamicFilter [ empty ], dynamic_rg_pruning=eligible, pruning_predicate=RainToday_null_count@2 != row_count@3 AND RainToday_min@0 <= no AND no <= RainToday_max@1, required_guarantees=[RainToday in (no)]
              │           t1: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000001.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[MaxTemp, RainToday, RainTomorrow], file_type=parquet, predicate=RainToday@19 = no AND DynamicFilter [ empty ], dynamic_rg_pruning=eligible, pruning_predicate=RainToday_null_count@2 != row_count@3 AND RainToday_min@0 <= no AND no <= RainToday_max@1, required_guarantees=[RainToday in (no)]
              │           t2: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000001.parquet:<int>..<int>, /testdata/weather/result-000002.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[MaxTemp, RainToday, RainTomorrow], file_type=parquet, predicate=RainToday@19 = no AND DynamicFilter [ empty ], dynamic_rg_pruning=eligible, pruning_predicate=RainToday_null_count@2 != row_count@3 AND RainToday_min@0 <= no AND no <= RainToday_max@1, required_guarantees=[RainToday in (no)]
              └──────────────────────────────────────────────────
        ");
    }

    #[tokio::test]
    async fn test_sort() {
        let query = r#"
        SELECT * FROM weather ORDER BY "MinTemp" DESC
        "#;
        let physical_plan_ascii = TestPlanBuilder::default()
            .physical_plan_as_ascii(query, false)
            .await;
        assert_snapshot!(physical_plan_ascii, @r"
        ┌───── DistributedExec
        │ SortPreservingMergeExec: [MinTemp@0 DESC]
        │   [Stage 1] => NetworkCoalesceExec: output_partitions=9, input_tasks=3
        └──────────────────────────────────────────────────
          ┌───── Stage 1 ── tasks=3, partitions=9
          │ SortExec: expr=[MinTemp@0 DESC], preserve_partitioning=[true]
          │   DistributedLeafExec:
          │     t0: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000000.parquet:<int>..<int>, /testdata/weather/result-000001.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[MinTemp, MaxTemp, Rainfall, Evaporation, Sunshine, WindGustDir, WindGustSpeed, WindDir9am, WindDir3pm, WindSpeed9am, WindSpeed3pm, Humidity9am, Humidity3pm, Pressure9am, Pressure3pm, Cloud9am, Cloud3pm, Temp9am, Temp3pm, RainToday, RISK_MM, RainTomorrow], file_type=parquet, sort_order_for_reorder=[MinTemp@0 DESC], reverse_row_groups=true
          │     t1: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000001.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[MinTemp, MaxTemp, Rainfall, Evaporation, Sunshine, WindGustDir, WindGustSpeed, WindDir9am, WindDir3pm, WindSpeed9am, WindSpeed3pm, Humidity9am, Humidity3pm, Pressure9am, Pressure3pm, Cloud9am, Cloud3pm, Temp9am, Temp3pm, RainToday, RISK_MM, RainTomorrow], file_type=parquet, sort_order_for_reorder=[MinTemp@0 DESC], reverse_row_groups=true
          │     t2: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000001.parquet:<int>..<int>, /testdata/weather/result-000002.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[MinTemp, MaxTemp, Rainfall, Evaporation, Sunshine, WindGustDir, WindGustSpeed, WindDir9am, WindDir3pm, WindSpeed9am, WindSpeed3pm, Humidity9am, Humidity3pm, Pressure9am, Pressure3pm, Cloud9am, Cloud3pm, Temp9am, Temp3pm, RainToday, RISK_MM, RainTomorrow], file_type=parquet, sort_order_for_reorder=[MinTemp@0 DESC], reverse_row_groups=true
          └──────────────────────────────────────────────────
        ");
    }

    #[tokio::test]
    async fn test_limit_fetch_pushes_into_network_coalesce_input_stage() {
        let query = r#"
        SELECT "RainToday", count(*) FROM weather GROUP BY "RainToday" LIMIT 10
        "#;
        let physical_plan_ascii = TestPlanBuilder::default()
            .distributed_cardinality_effect_task_scale_factor(1.5)
            .physical_plan_as_ascii(query, false)
            .await;
        assert_snapshot!(physical_plan_ascii, @"
        ┌───── DistributedExec
        │ ProjectionExec: expr=[RainToday@0 as RainToday, count(Int64(1))@1 as count(*)]
        │   CoalescePartitionsExec: fetch=10
        │     [Stage 2] => NetworkCoalesceExec: output_partitions=8, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 2 ── tasks=2, partitions=4
          │ LocalLimitExec: fetch=10
          │   AggregateExec: mode=FinalPartitioned, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
          │     [Stage 1] => NetworkShuffleExec: output_partitions=4, input_tasks=3
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── tasks=3, partitions=8
            │ RepartitionExec: partitioning=Hash([RainToday@0], 8), input_partitions=3
            │   AggregateExec: mode=Partial, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
            │     DistributedLeafExec:
            │       t0: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000000.parquet:<int>..<int>, /testdata/weather/result-000001.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[RainToday], file_type=parquet
            │       t1: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000001.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[RainToday], file_type=parquet
            │       t2: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000001.parquet:<int>..<int>, /testdata/weather/result-000002.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[RainToday], file_type=parquet
            └──────────────────────────────────────────────────
        ");
    }

    #[tokio::test]
    async fn test_distinct() {
        let query = r#"
        SELECT DISTINCT "RainToday", "WindGustDir" FROM weather
        "#;
        let physical_plan_ascii = TestPlanBuilder::default()
            .distributed_cardinality_effect_task_scale_factor(1.5)
            .physical_plan_as_ascii(query, false)
            .await;
        assert_snapshot!(physical_plan_ascii, @"
        ┌───── DistributedExec
        │ CoalescePartitionsExec
        │   [Stage 2] => NetworkCoalesceExec: output_partitions=8, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 2 ── tasks=2, partitions=4
          │ AggregateExec: mode=FinalPartitioned, gby=[RainToday@0 as RainToday, WindGustDir@1 as WindGustDir], aggr=[]
          │   [Stage 1] => NetworkShuffleExec: output_partitions=4, input_tasks=3
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── tasks=3, partitions=8
            │ RepartitionExec: partitioning=Hash([RainToday@0, WindGustDir@1], 8), input_partitions=3
            │   AggregateExec: mode=Partial, gby=[RainToday@0 as RainToday, WindGustDir@1 as WindGustDir], aggr=[]
            │     DistributedLeafExec:
            │       t0: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000000.parquet:<int>..<int>, /testdata/weather/result-000001.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[RainToday, WindGustDir], file_type=parquet
            │       t1: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000001.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[RainToday, WindGustDir], file_type=parquet
            │       t2: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000001.parquet:<int>..<int>, /testdata/weather/result-000002.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[RainToday, WindGustDir], file_type=parquet
            └──────────────────────────────────────────────────
        ");
    }

    #[tokio::test]
    async fn test_show_columns() {
        let query = r#"
        SHOW COLUMNS from weather
        "#;
        let physical_plan_ascii = TestPlanBuilder::default()
            .information_schema(true)
            .physical_plan_as_ascii(query, false)
            .await;
        assert_snapshot!(physical_plan_ascii, @r"
        FilterExec: table_name@2 = weather, projection=[table_catalog@0, table_schema@1, table_name@2, column_name@3, data_type@5, is_nullable@4]
          RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=1
            StreamingTableExec: partition_sizes=1, projection=[table_catalog, table_schema, table_name, column_name, is_nullable, data_type]
        ");
    }

    #[tokio::test]
    async fn test_limited_by_worker() {
        let query = r#"
        SELECT 1 FROM weather
        UNION ALL
        SELECT 1 FROM flights_1m
        "#;
        let physical_plan_ascii = TestPlanBuilder::default()
            .target_partitions(2)
            .num_workers(2)
            .physical_plan_as_ascii(query, false)
            .await;
        assert_snapshot!(physical_plan_ascii, @"
        ┌───── DistributedExec
        │ CoalescePartitionsExec
        │   [Stage 1] => NetworkCoalesceExec: output_partitions=4, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 1 ── tasks=2, partitions=4
          │ DistributedUnionExec: t0:[c0] t1:[c1]
          │   DistributedLeafExec:
          │     t0: DataSourceExec: file_groups={2 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>, /testdata/weather/result-000001.parquet:<int>..<int>], [/testdata/weather/result-000001.parquet:<int>..<int>, /testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[1 as Int64(1)], file_type=parquet
          │   DistributedLeafExec:
          │     t0: DataSourceExec: file_groups={2 groups: [[/testdata/flights-1m.parquet:<int>..<int>], [/testdata/flights-1m.parquet:<int>..<int>]]}, projection=[1 as Int64(1)], file_type=parquet
          └──────────────────────────────────────────────────
        ");
    }

    #[tokio::test]
    async fn test_limited_by_config() {
        let query = r#"
        SELECT 1 FROM weather
        UNION ALL
        SELECT 1 FROM flights_1m
        "#;
        let physical_plan_ascii = TestPlanBuilder::default()
            .distributed_max_tasks_per_stage(2)
            .physical_plan_as_ascii(query, false)
            .await;
        assert_snapshot!(physical_plan_ascii, @"
        ┌───── DistributedExec
        │ CoalescePartitionsExec
        │   [Stage 1] => NetworkCoalesceExec: output_partitions=8, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 1 ── tasks=2, partitions=8
          │ DistributedUnionExec: t0:[c0] t1:[c1]
          │   DistributedLeafExec:
          │     t0: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000000.parquet:<int>..<int>, /testdata/weather/result-000001.parquet:<int>..<int>, /testdata/weather/result-000002.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[1 as Int64(1)], file_type=parquet
          │   DistributedLeafExec:
          │     t0: DataSourceExec: file_groups={4 groups: [[/testdata/flights-1m.parquet:<int>..<int>], [/testdata/flights-1m.parquet:<int>..<int>], [/testdata/flights-1m.parquet:<int>..<int>], [/testdata/flights-1m.parquet:<int>..<int>]]}, projection=[1 as Int64(1)], file_type=parquet
          └──────────────────────────────────────────────────
        ");
    }

    #[tokio::test]
    async fn test_unioning_2_tables() {
        let query = r#"
        SELECT "MinTemp", "RainToday" FROM weather WHERE "MinTemp" > 10.0
        UNION ALL
        SELECT "MaxTemp", "RainToday" FROM weather WHERE "MaxTemp" < 30.0
        "#;
        let physical_plan_ascii = TestPlanBuilder::default()
            .num_workers(6)
            .physical_plan_as_ascii(query, false)
            .await;
        assert_snapshot!(physical_plan_ascii, @r"
        ┌───── DistributedExec
        │ CoalescePartitionsExec
        │   [Stage 1] => NetworkCoalesceExec: output_partitions=24, input_tasks=6
        └──────────────────────────────────────────────────
          ┌───── Stage 1 ── tasks=6, partitions=24
          │ DistributedUnionExec: t0:[c0(0/3)] t1:[c0(1/3)] t2:[c0(2/3)] t3:[c1(0/3)] t4:[c1(1/3)] t5:[c1(2/3)]
          │   FilterExec: MinTemp@0 > 10
          │     RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=3
          │       DistributedLeafExec:
          │         t0: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000000.parquet:<int>..<int>, /testdata/weather/result-000001.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[MinTemp, RainToday], file_type=parquet, predicate=MinTemp@0 > 10, pruning_predicate=MinTemp_null_count@1 != row_count@2 AND MinTemp_max@0 > 10, required_guarantees=[]
          │         t1: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000001.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[MinTemp, RainToday], file_type=parquet, predicate=MinTemp@0 > 10, pruning_predicate=MinTemp_null_count@1 != row_count@2 AND MinTemp_max@0 > 10, required_guarantees=[]
          │         t2: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000001.parquet:<int>..<int>, /testdata/weather/result-000002.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[MinTemp, RainToday], file_type=parquet, predicate=MinTemp@0 > 10, pruning_predicate=MinTemp_null_count@1 != row_count@2 AND MinTemp_max@0 > 10, required_guarantees=[]
          │   ProjectionExec: expr=[MaxTemp@0 as MinTemp, RainToday@1 as RainToday]
          │     FilterExec: MaxTemp@0 < 30
          │       RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=3
          │         DistributedLeafExec:
          │           t0: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000000.parquet:<int>..<int>, /testdata/weather/result-000001.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[MaxTemp, RainToday], file_type=parquet, predicate=MaxTemp@1 < 30, pruning_predicate=MaxTemp_null_count@1 != row_count@2 AND MaxTemp_min@0 < 30, required_guarantees=[]
          │           t1: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000001.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[MaxTemp, RainToday], file_type=parquet, predicate=MaxTemp@1 < 30, pruning_predicate=MaxTemp_null_count@1 != row_count@2 AND MaxTemp_min@0 < 30, required_guarantees=[]
          │           t2: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000001.parquet:<int>..<int>, /testdata/weather/result-000002.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[MaxTemp, RainToday], file_type=parquet, predicate=MaxTemp@1 < 30, pruning_predicate=MaxTemp_null_count@1 != row_count@2 AND MaxTemp_min@0 < 30, required_guarantees=[]
          └──────────────────────────────────────────────────
        ");
    }

    #[tokio::test]
    async fn test_unioning_2_tables_limited_workers() {
        let query = r#"
        SELECT "MinTemp", "RainToday" FROM weather WHERE "MinTemp" > 10.0
        UNION ALL
        SELECT "MaxTemp", "RainToday" FROM weather WHERE "MaxTemp" < 30.0
        "#;
        let physical_plan_ascii = TestPlanBuilder::default()
            .physical_plan_as_ascii(query, false)
            .await;
        assert_snapshot!(physical_plan_ascii, @r"
        ┌───── DistributedExec
        │ CoalescePartitionsExec
        │   [Stage 1] => NetworkCoalesceExec: output_partitions=12, input_tasks=3
        └──────────────────────────────────────────────────
          ┌───── Stage 1 ── tasks=3, partitions=12
          │ DistributedUnionExec: t0:[c0(0/2)] t1:[c0(1/2)] t2:[c1]
          │   FilterExec: MinTemp@0 > 10
          │     RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=3
          │       DistributedLeafExec:
          │         t0: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000000.parquet:<int>..<int>, /testdata/weather/result-000001.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[MinTemp, RainToday], file_type=parquet, predicate=MinTemp@0 > 10, pruning_predicate=MinTemp_null_count@1 != row_count@2 AND MinTemp_max@0 > 10, required_guarantees=[]
          │         t1: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000001.parquet:<int>..<int>, /testdata/weather/result-000002.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[MinTemp, RainToday], file_type=parquet, predicate=MinTemp@0 > 10, pruning_predicate=MinTemp_null_count@1 != row_count@2 AND MinTemp_max@0 > 10, required_guarantees=[]
          │   ProjectionExec: expr=[MaxTemp@0 as MinTemp, RainToday@1 as RainToday]
          │     FilterExec: MaxTemp@0 < 30
          │       RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=3
          │         DistributedLeafExec:
          │           t0: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000000.parquet:<int>..<int>, /testdata/weather/result-000001.parquet:<int>..<int>, /testdata/weather/result-000002.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[MaxTemp, RainToday], file_type=parquet, predicate=MaxTemp@1 < 30, pruning_predicate=MaxTemp_null_count@1 != row_count@2 AND MaxTemp_min@0 < 30, required_guarantees=[]
          └──────────────────────────────────────────────────
        ");
    }

    #[tokio::test]
    async fn test_unioning_3_tables() {
        let query = r#"
        SELECT "MinTemp", "RainToday" FROM weather WHERE "MinTemp" > 10.0
        UNION ALL
        SELECT "MaxTemp", "RainToday" FROM weather WHERE "MaxTemp" < 30.0
        UNION ALL
        SELECT "Temp9am", "RainToday" FROM weather WHERE "Temp9am" > 15.0
        "#;
        let physical_plan_ascii = TestPlanBuilder::default()
            .physical_plan_as_ascii(query, false)
            .await;
        assert_snapshot!(physical_plan_ascii, @r"
        ┌───── DistributedExec
        │ CoalescePartitionsExec
        │   [Stage 1] => NetworkCoalesceExec: output_partitions=12, input_tasks=3
        └──────────────────────────────────────────────────
          ┌───── Stage 1 ── tasks=3, partitions=12
          │ DistributedUnionExec: t0:[c0] t1:[c1] t2:[c2]
          │   FilterExec: MinTemp@0 > 10
          │     RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=3
          │       DistributedLeafExec:
          │         t0: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000000.parquet:<int>..<int>, /testdata/weather/result-000001.parquet:<int>..<int>, /testdata/weather/result-000002.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[MinTemp, RainToday], file_type=parquet, predicate=MinTemp@0 > 10, pruning_predicate=MinTemp_null_count@1 != row_count@2 AND MinTemp_max@0 > 10, required_guarantees=[]
          │   ProjectionExec: expr=[MaxTemp@0 as MinTemp, RainToday@1 as RainToday]
          │     FilterExec: MaxTemp@0 < 30
          │       RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=3
          │         DistributedLeafExec:
          │           t0: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000000.parquet:<int>..<int>, /testdata/weather/result-000001.parquet:<int>..<int>, /testdata/weather/result-000002.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[MaxTemp, RainToday], file_type=parquet, predicate=MaxTemp@1 < 30, pruning_predicate=MaxTemp_null_count@1 != row_count@2 AND MaxTemp_min@0 < 30, required_guarantees=[]
          │   ProjectionExec: expr=[Temp9am@0 as MinTemp, RainToday@1 as RainToday]
          │     FilterExec: Temp9am@0 > 15
          │       RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=3
          │         DistributedLeafExec:
          │           t0: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000000.parquet:<int>..<int>, /testdata/weather/result-000001.parquet:<int>..<int>, /testdata/weather/result-000002.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[Temp9am, RainToday], file_type=parquet, predicate=Temp9am@17 > 15, pruning_predicate=Temp9am_null_count@1 != row_count@2 AND Temp9am_max@0 > 15, required_guarantees=[]
          └──────────────────────────────────────────────────
        ");
    }

    #[tokio::test]
    async fn test_unioning_5_tables() {
        let query = r#"
        SELECT "MinTemp", "RainToday" FROM weather WHERE "MinTemp" > 10.0
        UNION ALL
        SELECT "MaxTemp", "RainToday" FROM weather WHERE "MaxTemp" < 30.0
        UNION ALL
        SELECT "Temp9am", "RainToday" FROM weather WHERE "Temp9am" > 15.0
        UNION ALL
        SELECT "Temp3pm", "RainToday" FROM weather WHERE "Temp3pm" < 25.0
        UNION ALL
        SELECT "Rainfall", "RainToday" FROM weather WHERE "Rainfall" > 5.0
        "#;
        let physical_plan_ascii = TestPlanBuilder::default()
            .physical_plan_as_ascii(query, false)
            .await;
        assert_snapshot!(physical_plan_ascii, @r"
        ┌───── DistributedExec
        │ CoalescePartitionsExec
        │   [Stage 1] => NetworkCoalesceExec: output_partitions=24, input_tasks=3
        └──────────────────────────────────────────────────
          ┌───── Stage 1 ── tasks=3, partitions=24
          │ DistributedUnionExec: t0:[c0, c3] t1:[c1, c4] t2:[c2]
          │   FilterExec: MinTemp@0 > 10
          │     RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=3
          │       DistributedLeafExec:
          │         t0: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000000.parquet:<int>..<int>, /testdata/weather/result-000001.parquet:<int>..<int>, /testdata/weather/result-000002.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[MinTemp, RainToday], file_type=parquet, predicate=MinTemp@0 > 10, pruning_predicate=MinTemp_null_count@1 != row_count@2 AND MinTemp_max@0 > 10, required_guarantees=[]
          │   ProjectionExec: expr=[MaxTemp@0 as MinTemp, RainToday@1 as RainToday]
          │     FilterExec: MaxTemp@0 < 30
          │       RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=3
          │         DistributedLeafExec:
          │           t0: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000000.parquet:<int>..<int>, /testdata/weather/result-000001.parquet:<int>..<int>, /testdata/weather/result-000002.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[MaxTemp, RainToday], file_type=parquet, predicate=MaxTemp@1 < 30, pruning_predicate=MaxTemp_null_count@1 != row_count@2 AND MaxTemp_min@0 < 30, required_guarantees=[]
          │   ProjectionExec: expr=[Temp9am@0 as MinTemp, RainToday@1 as RainToday]
          │     FilterExec: Temp9am@0 > 15
          │       RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=3
          │         DistributedLeafExec:
          │           t0: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000000.parquet:<int>..<int>, /testdata/weather/result-000001.parquet:<int>..<int>, /testdata/weather/result-000002.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[Temp9am, RainToday], file_type=parquet, predicate=Temp9am@17 > 15, pruning_predicate=Temp9am_null_count@1 != row_count@2 AND Temp9am_max@0 > 15, required_guarantees=[]
          │   ProjectionExec: expr=[Temp3pm@0 as MinTemp, RainToday@1 as RainToday]
          │     FilterExec: Temp3pm@0 < 25
          │       RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=3
          │         DistributedLeafExec:
          │           t0: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000000.parquet:<int>..<int>, /testdata/weather/result-000001.parquet:<int>..<int>, /testdata/weather/result-000002.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[Temp3pm, RainToday], file_type=parquet, predicate=Temp3pm@18 < 25, pruning_predicate=Temp3pm_null_count@1 != row_count@2 AND Temp3pm_min@0 < 25, required_guarantees=[]
          │   ProjectionExec: expr=[Rainfall@0 as MinTemp, RainToday@1 as RainToday]
          │     FilterExec: Rainfall@0 > 5
          │       RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=3
          │         DistributedLeafExec:
          │           t0: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000000.parquet:<int>..<int>, /testdata/weather/result-000001.parquet:<int>..<int>, /testdata/weather/result-000002.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[Rainfall, RainToday], file_type=parquet, predicate=Rainfall@2 > 5, pruning_predicate=Rainfall_null_count@1 != row_count@2 AND Rainfall_max@0 > 5, required_guarantees=[]
          └──────────────────────────────────────────────────
        ");
    }

    #[tokio::test]
    async fn test_broadcast_join() {
        let query = r#"
        SELECT a."MinTemp", b."MaxTemp"
        FROM weather a INNER JOIN weather b
        ON a."RainToday" = b."RainToday"
        "#;
        let physical_plan_ascii = TestPlanBuilder::default()
            .broadcast_joins(true)
            .physical_plan_as_ascii(query, false)
            .await;
        assert_snapshot!(physical_plan_ascii, @"
        ┌───── DistributedExec
        │ CoalescePartitionsExec
        │   [Stage 2] => NetworkCoalesceExec: output_partitions=9, input_tasks=3
        └──────────────────────────────────────────────────
          ┌───── Stage 2 ── tasks=3, partitions=9
          │ HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(RainToday@1, RainToday@1)], projection=[MinTemp@0, MaxTemp@2]
          │   CoalescePartitionsExec
          │     [Stage 1] => NetworkBroadcastExec: partitions_per_consumer=3, stage_partitions=9, input_tasks=3
          │   DistributedLeafExec:
          │     t0: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000000.parquet:<int>..<int>, /testdata/weather/result-000001.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[MaxTemp, RainToday], file_type=parquet, predicate=DynamicFilter [ empty ], dynamic_rg_pruning=eligible
          │     t1: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000001.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[MaxTemp, RainToday], file_type=parquet, predicate=DynamicFilter [ empty ], dynamic_rg_pruning=eligible
          │     t2: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000001.parquet:<int>..<int>, /testdata/weather/result-000002.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[MaxTemp, RainToday], file_type=parquet, predicate=DynamicFilter [ empty ], dynamic_rg_pruning=eligible
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── tasks=3, partitions=27
            │ BroadcastExec: input_partitions=3, consumer_tasks=3, output_partitions=9
            │   DistributedLeafExec:
            │     t0: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000000.parquet:<int>..<int>, /testdata/weather/result-000001.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[MinTemp, RainToday], file_type=parquet
            │     t1: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000001.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[MinTemp, RainToday], file_type=parquet
            │     t2: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000001.parquet:<int>..<int>, /testdata/weather/result-000002.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[MinTemp, RainToday], file_type=parquet
            └──────────────────────────────────────────────────
        ")
    }

    #[tokio::test]
    async fn test_broadcast_nested_joins() {
        let query = r#"
        SELECT a."MinTemp", b."MaxTemp", c."Rainfall"
        FROM weather a
        INNER JOIN weather b ON a."RainToday" = b."RainToday"
        INNER JOIN weather c ON b."RainToday" = c."RainToday"
        "#;
        let physical_plan_ascii = TestPlanBuilder::default()
            .broadcast_joins(true)
            .physical_plan_as_ascii(query, false)
            .await;
        assert_snapshot!(physical_plan_ascii, @"
        ┌───── DistributedExec
        │ CoalescePartitionsExec
        │   [Stage 3] => NetworkCoalesceExec: output_partitions=9, input_tasks=3
        └──────────────────────────────────────────────────
          ┌───── Stage 3 ── tasks=3, partitions=9
          │ HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(RainToday@2, RainToday@1)], projection=[MinTemp@0, MaxTemp@1, Rainfall@3]
          │   CoalescePartitionsExec
          │     [Stage 2] => NetworkBroadcastExec: partitions_per_consumer=3, stage_partitions=9, input_tasks=3
          │   DistributedLeafExec:
          │     t0: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000000.parquet:<int>..<int>, /testdata/weather/result-000001.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[Rainfall, RainToday], file_type=parquet, predicate=DynamicFilter [ empty ], dynamic_rg_pruning=eligible
          │     t1: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000001.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[Rainfall, RainToday], file_type=parquet, predicate=DynamicFilter [ empty ], dynamic_rg_pruning=eligible
          │     t2: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000001.parquet:<int>..<int>, /testdata/weather/result-000002.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[Rainfall, RainToday], file_type=parquet, predicate=DynamicFilter [ empty ], dynamic_rg_pruning=eligible
          └──────────────────────────────────────────────────
            ┌───── Stage 2 ── tasks=3, partitions=27
            │ BroadcastExec: input_partitions=3, consumer_tasks=3, output_partitions=9
            │   HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(RainToday@1, RainToday@1)], projection=[MinTemp@0, MaxTemp@2, RainToday@3]
            │     CoalescePartitionsExec
            │       [Stage 1] => NetworkBroadcastExec: partitions_per_consumer=3, stage_partitions=9, input_tasks=3
            │     DistributedLeafExec:
            │       t0: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000000.parquet:<int>..<int>, /testdata/weather/result-000001.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[MaxTemp, RainToday], file_type=parquet, predicate=DynamicFilter [ empty ], dynamic_rg_pruning=eligible
            │       t1: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000001.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[MaxTemp, RainToday], file_type=parquet, predicate=DynamicFilter [ empty ], dynamic_rg_pruning=eligible
            │       t2: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000001.parquet:<int>..<int>, /testdata/weather/result-000002.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[MaxTemp, RainToday], file_type=parquet, predicate=DynamicFilter [ empty ], dynamic_rg_pruning=eligible
            └──────────────────────────────────────────────────
              ┌───── Stage 1 ── tasks=3, partitions=27
              │ BroadcastExec: input_partitions=3, consumer_tasks=3, output_partitions=9
              │   DistributedLeafExec:
              │     t0: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000000.parquet:<int>..<int>, /testdata/weather/result-000001.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[MinTemp, RainToday], file_type=parquet
              │     t1: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000001.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[MinTemp, RainToday], file_type=parquet
              │     t2: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000001.parquet:<int>..<int>, /testdata/weather/result-000002.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[MinTemp, RainToday], file_type=parquet
              └──────────────────────────────────────────────────
        ")
    }

    #[tokio::test]
    async fn test_broadcast_datasource_as_build_child() {
        let query = r#"
        SELECT a."MinTemp", b."MaxTemp"
        FROM weather a INNER JOIN weather b
        ON a."RainToday" = b."RainToday"
        "#;
        let physical_plan_ascii = TestPlanBuilder::default()
            .physical_plan_as_ascii(query, false)
            .await;
        assert_snapshot!(physical_plan_ascii, @r"
        ┌───── DistributedExec
        │ CoalescePartitionsExec
        │   HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(RainToday@1, RainToday@1)], projection=[MinTemp@0, MaxTemp@2]
        │     CoalescePartitionsExec
        │       [Stage 1] => NetworkCoalesceExec: output_partitions=9, input_tasks=3
        │     DistributedLeafExec:
        │       t0: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000000.parquet:<int>..<int>, /testdata/weather/result-000001.parquet:<int>..<int>, /testdata/weather/result-000002.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[MaxTemp, RainToday], file_type=parquet, predicate=DynamicFilter [ empty ], dynamic_rg_pruning=eligible
        └──────────────────────────────────────────────────
          ┌───── Stage 1 ── tasks=3, partitions=9
          │ DistributedLeafExec:
          │   t0: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000000.parquet:<int>..<int>, /testdata/weather/result-000001.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[MinTemp, RainToday], file_type=parquet
          │   t1: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000001.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[MinTemp, RainToday], file_type=parquet
          │   t2: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000001.parquet:<int>..<int>, /testdata/weather/result-000002.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[MinTemp, RainToday], file_type=parquet
          └──────────────────────────────────────────────────
        ");

        let physical_plan_ascii = TestPlanBuilder::default()
            .broadcast_joins(true)
            .physical_plan_as_ascii(query, false)
            .await;
        assert_snapshot!(physical_plan_ascii, @"
        ┌───── DistributedExec
        │ CoalescePartitionsExec
        │   [Stage 2] => NetworkCoalesceExec: output_partitions=9, input_tasks=3
        └──────────────────────────────────────────────────
          ┌───── Stage 2 ── tasks=3, partitions=9
          │ HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(RainToday@1, RainToday@1)], projection=[MinTemp@0, MaxTemp@2]
          │   CoalescePartitionsExec
          │     [Stage 1] => NetworkBroadcastExec: partitions_per_consumer=3, stage_partitions=9, input_tasks=3
          │   DistributedLeafExec:
          │     t0: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000000.parquet:<int>..<int>, /testdata/weather/result-000001.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[MaxTemp, RainToday], file_type=parquet, predicate=DynamicFilter [ empty ], dynamic_rg_pruning=eligible
          │     t1: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000001.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[MaxTemp, RainToday], file_type=parquet, predicate=DynamicFilter [ empty ], dynamic_rg_pruning=eligible
          │     t2: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000001.parquet:<int>..<int>, /testdata/weather/result-000002.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[MaxTemp, RainToday], file_type=parquet, predicate=DynamicFilter [ empty ], dynamic_rg_pruning=eligible
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── tasks=3, partitions=27
            │ BroadcastExec: input_partitions=3, consumer_tasks=3, output_partitions=9
            │   DistributedLeafExec:
            │     t0: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000000.parquet:<int>..<int>, /testdata/weather/result-000001.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[MinTemp, RainToday], file_type=parquet
            │     t1: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000001.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[MinTemp, RainToday], file_type=parquet
            │     t2: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000001.parquet:<int>..<int>, /testdata/weather/result-000002.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[MinTemp, RainToday], file_type=parquet
            └──────────────────────────────────────────────────
        ")
    }

    #[tokio::test]
    async fn test_broadcast_union_children_isolator_plan() {
        let query = r#"
        SELECT a."MinTemp", b."MaxTemp"
        FROM weather a INNER JOIN weather b
        ON a."RainToday" = b."RainToday"
        UNION ALL
        SELECT a."MinTemp", b."MaxTemp"
        FROM weather a INNER JOIN weather b
        ON a."RainToday" = b."RainToday"
        UNION ALL
        SELECT a."MinTemp", b."MaxTemp"
        FROM weather a INNER JOIN weather b
        ON a."RainToday" = b."RainToday"
        "#;
        let physical_plan_ascii = TestPlanBuilder::default()
            .broadcast_joins(true)
            .physical_plan_as_ascii(query, false)
            .await;
        assert_snapshot!(physical_plan_ascii, @"
        ┌───── DistributedExec
        │ CoalescePartitionsExec
        │   [Stage 4] => NetworkCoalesceExec: output_partitions=9, input_tasks=3
        └──────────────────────────────────────────────────
          ┌───── Stage 4 ── tasks=3, partitions=9
          │ DistributedUnionExec: t0:[c0] t1:[c1] t2:[c2]
          │   HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(RainToday@1, RainToday@1)], projection=[MinTemp@0, MaxTemp@2]
          │     CoalescePartitionsExec
          │       [Stage 1] => NetworkBroadcastExec: partitions_per_consumer=3, stage_partitions=3, input_tasks=3
          │     DistributedLeafExec:
          │       t0: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000000.parquet:<int>..<int>, /testdata/weather/result-000001.parquet:<int>..<int>, /testdata/weather/result-000002.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[MaxTemp, RainToday], file_type=parquet, predicate=DynamicFilter [ empty ], dynamic_rg_pruning=eligible
          │   HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(RainToday@1, RainToday@1)], projection=[MinTemp@0, MaxTemp@2]
          │     CoalescePartitionsExec
          │       [Stage 2] => NetworkBroadcastExec: partitions_per_consumer=3, stage_partitions=3, input_tasks=3
          │     DistributedLeafExec:
          │       t0: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000000.parquet:<int>..<int>, /testdata/weather/result-000001.parquet:<int>..<int>, /testdata/weather/result-000002.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[MaxTemp, RainToday], file_type=parquet, predicate=DynamicFilter [ empty ], dynamic_rg_pruning=eligible
          │   HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(RainToday@1, RainToday@1)], projection=[MinTemp@0, MaxTemp@2]
          │     CoalescePartitionsExec
          │       [Stage 3] => NetworkBroadcastExec: partitions_per_consumer=3, stage_partitions=3, input_tasks=3
          │     DistributedLeafExec:
          │       t0: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000000.parquet:<int>..<int>, /testdata/weather/result-000001.parquet:<int>..<int>, /testdata/weather/result-000002.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[MaxTemp, RainToday], file_type=parquet, predicate=DynamicFilter [ empty ], dynamic_rg_pruning=eligible
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── tasks=3, partitions=9
            │ BroadcastExec: input_partitions=3, consumer_tasks=1, output_partitions=3
            │   DistributedLeafExec:
            │     t0: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000000.parquet:<int>..<int>, /testdata/weather/result-000001.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[MinTemp, RainToday], file_type=parquet
            │     t1: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000001.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[MinTemp, RainToday], file_type=parquet
            │     t2: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000001.parquet:<int>..<int>, /testdata/weather/result-000002.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[MinTemp, RainToday], file_type=parquet
            └──────────────────────────────────────────────────
            ┌───── Stage 2 ── tasks=3, partitions=9
            │ BroadcastExec: input_partitions=3, consumer_tasks=1, output_partitions=3
            │   DistributedLeafExec:
            │     t0: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000000.parquet:<int>..<int>, /testdata/weather/result-000001.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[MinTemp, RainToday], file_type=parquet
            │     t1: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000001.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[MinTemp, RainToday], file_type=parquet
            │     t2: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000001.parquet:<int>..<int>, /testdata/weather/result-000002.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[MinTemp, RainToday], file_type=parquet
            └──────────────────────────────────────────────────
            ┌───── Stage 3 ── tasks=3, partitions=9
            │ BroadcastExec: input_partitions=3, consumer_tasks=1, output_partitions=3
            │   DistributedLeafExec:
            │     t0: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000000.parquet:<int>..<int>, /testdata/weather/result-000001.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[MinTemp, RainToday], file_type=parquet
            │     t1: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000001.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[MinTemp, RainToday], file_type=parquet
            │     t2: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000001.parquet:<int>..<int>, /testdata/weather/result-000002.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[MinTemp, RainToday], file_type=parquet
            └──────────────────────────────────────────────────
        ");
    }

    #[tokio::test]
    async fn test_broadcast_one_to_many_plan() {
        let query = r#"
        SELECT a."MinTemp", b."MaxTemp"
        FROM weather a INNER JOIN weather b
        ON a."RainToday" = b."RainToday"
        "#;
        let physical_plan_ascii = TestPlanBuilder::default()
            .broadcast_joins(true)
            .desired_task_count_handler(build_side_one_desired_task_count_handler)
            .physical_plan_as_ascii(query, false)
            .await;
        assert_snapshot!(physical_plan_ascii, @"
        ┌───── DistributedExec
        │ CoalescePartitionsExec
        │   [Stage 2] => NetworkCoalesceExec: output_partitions=9, input_tasks=3
        └──────────────────────────────────────────────────
          ┌───── Stage 2 ── tasks=3, partitions=9
          │ HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(RainToday@1, RainToday@1)], projection=[MinTemp@0, MaxTemp@2]
          │   CoalescePartitionsExec
          │     [Stage 1] => NetworkBroadcastExec: partitions_per_consumer=3, stage_partitions=9, input_tasks=1
          │   DistributedLeafExec:
          │     t0: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000000.parquet:<int>..<int>, /testdata/weather/result-000001.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[MaxTemp, RainToday], file_type=parquet, predicate=DynamicFilter [ empty ], dynamic_rg_pruning=eligible
          │     t1: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000001.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[MaxTemp, RainToday], file_type=parquet, predicate=DynamicFilter [ empty ], dynamic_rg_pruning=eligible
          │     t2: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000001.parquet:<int>..<int>, /testdata/weather/result-000002.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[MaxTemp, RainToday], file_type=parquet, predicate=DynamicFilter [ empty ], dynamic_rg_pruning=eligible
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── tasks=1, partitions=9
            │ BroadcastExec: input_partitions=3, consumer_tasks=3, output_partitions=9
            │   DistributedLeafExec:
            │     t0: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000000.parquet:<int>..<int>, /testdata/weather/result-000001.parquet:<int>..<int>, /testdata/weather/result-000002.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[MinTemp, RainToday], file_type=parquet
            └──────────────────────────────────────────────────
        ");
    }
}

#[cfg(test)]
mod single_partition_tests {
    use crate::test_utils::{
        in_memory_channel_resolver::{InMemoryChannelResolver, InMemoryWorkerResolver},
        test_work_unit_feed::{
            RowGeneratorExec, RowGeneratorFeedProvider, TestWorkUnitFeedExecCodec, WorkUnitOp,
            row_generator_desired_task_count_handler, row_generator_scale_up_leaf_node_handler,
        },
    };
    use crate::{DistributedConfig, DistributedExt, SessionStateBuilderExt, WorkerQueryContext};
    use datafusion::{
        arrow::{
            array::{Int64Array, StringArray},
            datatypes::SchemaRef,
            record_batch::RecordBatch,
            util::pretty::pretty_format_batches,
        },
        catalog::{Session, TableProvider},
        common::Result,
        datasource::{MemTable, TableType},
        execution::SessionStateBuilder,
        logical_expr::Expr,
        physical_expr::expressions::Column,
        physical_plan::{ExecutionPlan, projection::ProjectionExec},
        prelude::{SessionConfig, SessionContext},
    };
    use std::sync::Arc;

    #[derive(Debug)]
    struct Input(Arc<dyn ExecutionPlan>);
    #[async_trait::async_trait]
    impl TableProvider for Input {
        fn schema(&self) -> SchemaRef {
            self.0.schema()
        }
        fn table_type(&self) -> TableType {
            TableType::Base
        }
        async fn scan(
            &self,
            _: &dyn Session,
            projection: Option<&Vec<usize>>,
            _: &[Expr],
            _: Option<usize>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            match projection {
                None => Ok(self.0.clone()),
                Some(indices) => Ok(Arc::new(ProjectionExec::try_new(
                    indices
                        .iter()
                        .map(|&index| {
                            (
                                Arc::new(Column::new(self.0.schema().field(index).name(), index))
                                    as Arc<dyn datafusion::physical_expr::PhysicalExpr>,
                                self.0.schema().field(index).name().clone(),
                            )
                        })
                        .collect::<Vec<_>>(),
                    self.0.clone(),
                )?)),
            }
        }
    }
    /// A native one-partition plan may legitimately omit coalesces and hash
    /// repartitions. Scaling its leaf must restore those semantic boundaries,
    /// and the result collector must consume every task rather than variant0.
    #[tokio::test]
    async fn scaled_single_partition_queries_match_native_sql() -> Result<()> {
        let mut observed = Vec::new();
        for dynamic in [false, true] {
            let channels = InMemoryChannelResolver::from_session_builder(
                |ctx: WorkerQueryContext| async move {
                    Ok(ctx
                        .builder
                        .with_distributed_user_codec(TestWorkUnitFeedExecCodec)
                        .with_distributed_work_unit_feed(|leaf: &RowGeneratorExec| Some(&leaf.feed))
                        .build())
                },
            );
            let mut config = SessionConfig::new().with_target_partitions(1);
            let distributed = DistributedConfig {
                dynamic_task_count: dynamic,
                max_tasks_per_stage: 2,
                ..Default::default()
            };
            config.set_distributed_option_extension(distributed);
            let state = SessionStateBuilder::new()
                .with_default_features()
                .with_config(config)
                .with_distributed_channel_resolver(channels)
                .with_distributed_worker_resolver(InMemoryWorkerResolver::new(2))
                .with_distributed_user_codec(TestWorkUnitFeedExecCodec)
                .with_distributed_work_unit_feed(|leaf: &RowGeneratorExec| Some(&leaf.feed))
                .with_distributed_desired_task_count_handler(
                    row_generator_desired_task_count_handler,
                )
                .with_distributed_scale_up_leaf_node_handler(
                    row_generator_scale_up_leaf_node_handler,
                )
                .with_distributed_planner()
                .build();
            let ctx = SessionContext::new_with_state(state);
            let provider = RowGeneratorFeedProvider::new(
                2,
                vec![vec![WorkUnitOp::Rows(4)], vec![WorkUnitOp::Rows(4)]],
            );
            let leaf = Arc::new(RowGeneratorExec::new(
                crate::WorkUnitFeed::new(provider),
                "input".into(),
                1,
                None,
                8,
            )) as Arc<dyn ExecutionPlan>;
            ctx.register_table("input", Arc::new(Input(leaf.clone())))?;
            let batch = RecordBatch::try_new(
                leaf.schema(),
                vec![
                    Arc::new(StringArray::from(vec!["input"; 8])),
                    Arc::new(Int64Array::from(vec![0, 0, 0, 0, 1, 1, 1, 1])),
                    Arc::new(Int64Array::from(vec![0; 8])),
                    Arc::new(StringArray::from(vec![
                        "a", "b", "c", "d", "a", "b", "c", "d",
                    ])),
                ],
            )?;
            let native =
                SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
            native.register_table(
                "input",
                Arc::new(MemTable::try_new(batch.schema(), vec![vec![batch]])?),
            )?;
            for sql in [
                "SELECT task,letter FROM input ORDER BY task,letter",
                "SELECT COUNT(DISTINCT task) AS c FROM input",
                "SELECT task FROM input ORDER BY task DESC LIMIT 3",
                "SELECT letter,COUNT(*) AS c FROM input GROUP BY letter ORDER BY letter",
                "SELECT ROW_NUMBER() OVER (ORDER BY task,letter) AS n FROM input ORDER BY n",
                "SELECT letter,COUNT(*) OVER (PARTITION BY letter) AS n FROM input ORDER BY letter,n",
            ] {
                let actual = tokio::time::timeout(
                    std::time::Duration::from_secs(20),
                    ctx.sql(sql).await?.collect(),
                )
                .await
                .expect("single-partition query deadlocked")?;
                let expected = native.sql(sql).await?.collect().await?;
                assert_eq!(
                    pretty_format_batches(&actual)?.to_string(),
                    pretty_format_batches(&expected)?.to_string(),
                    "dynamic={dynamic}; {sql}"
                );
                observed.push(format!(
                    "{dynamic}\t{}\t{sql}",
                    actual.iter().map(|batch| batch.num_rows()).sum::<usize>()
                ));
            }
        }
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("target/replays/single-partition.tsv");
        std::fs::create_dir_all(path.parent().unwrap())?;
        std::fs::write(
            path,
            format!(
                "command: cargo +1.97.1 test --offline --no-default-features --lib scaled_single_partition_queries_match_native_sql\ninputs: nativePartitions=1 desiredTasks=2 sourceRows=8\nexpected: every task contributes once; sorted, limited, grouped and windowed results equal independent native SQL\nobserved: dynamic\trows\tsql\n{}\n",
                observed.join("\n")
            ),
        )?;
        Ok(())
    }
    /// All branch setup channels remain open until the query ends. The old
    /// per-partition cap deadlocks before the final branches can be prepared.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn whole_dag_peer_capacity_prepares_every_branch_and_cancels_cleanly() -> Result<()> {
        use crate::test_utils::test_work_unit_feed::ScanGate;
        use crate::{ChannelResolver, Worker, WorkerChannel, WorkerResolver, grpc};
        use datafusion::common::runtime::SpawnedTask;
        use datafusion::execution::{
            memory_pool::{GreedyMemoryPool, MemoryPool},
            runtime_env::RuntimeEnvBuilder,
        };
        use datafusion::physical_plan::{common::collect, execute_stream};
        use std::sync::atomic::{AtomicUsize, Ordering};
        #[derive(Clone)]
        struct Http {
            url: url::Url,
            channel: tonic::transport::Channel,
        }
        impl WorkerResolver for Http {
            fn get_urls(&self) -> Result<Vec<url::Url>> {
                Ok(vec![self.url.clone()])
            }
        }
        #[async_trait::async_trait]
        impl ChannelResolver for Http {
            async fn get_worker_client_for_url(
                &self,
                url: &url::Url,
            ) -> Result<Box<dyn WorkerChannel>> {
                assert_eq!(url, &self.url);
                Ok(grpc::create_worker_client_with_limit(
                    grpc::BoxCloneSyncChannel::new(self.channel.clone()),
                    2 * 1024 * 1024,
                ))
            }
        }
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let url = url::Url::parse(&format!("http://{}", listener.local_addr()?)).unwrap();
        let http = Http {
            channel: tonic::transport::Channel::from_shared(url.to_string())
                .unwrap()
                .connect_lazy(),
            url,
        };
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(64 * 1024 * 1024));
        let runtime = Arc::new(
            RuntimeEnvBuilder::new()
                .with_memory_pool(pool.clone())
                .build()?,
        );
        let (started, mut starts) = tokio::sync::mpsc::unbounded_channel();
        let gate = Arc::new(ScanGate {
            started,
            release: tokio::sync::Semaphore::new(0),
        });
        let setups = Arc::new(AtomicUsize::new(0));
        let (changed, mut setup_changes) = tokio::sync::watch::channel(0usize);
        let worker = Worker::from_session_builder({
            let gate = gate.clone();
            let http = http.clone();
            let setups = setups.clone();
            move |ctx: WorkerQueryContext| {
                let gate = gate.clone();
                let http = http.clone();
                let setups = setups.clone();
                let changed = changed.clone();
                async move {
                    let mut builder = ctx.builder;
                    let config = builder.config().as_mut().unwrap();
                    *config = config.clone().with_extension(gate);
                    let state = builder
                        .with_distributed_channel_resolver(http)
                        .with_distributed_user_codec(TestWorkUnitFeedExecCodec)
                        .with_distributed_work_unit_feed(|leaf: &RowGeneratorExec| Some(&leaf.feed))
                        .build();
                    let count = setups.fetch_add(1, Ordering::SeqCst) + 1;
                    changed.send_replace(count);
                    Ok(state)
                }
            }
        })
        .with_runtime_env(runtime.clone());
        let mut config = SessionConfig::new().with_target_partitions(2);
        let distributed = DistributedConfig {
            dynamic_task_count: true,
            max_tasks_per_stage: 2,
            max_coordinator_channel_retries: 0,
            ..Default::default()
        };
        config.set_distributed_option_extension(distributed);
        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_runtime_env(runtime.clone())
            .with_config(config)
            .with_distributed_channel_resolver(http.clone())
            .with_distributed_worker_resolver(http.clone())
            .with_distributed_user_codec(TestWorkUnitFeedExecCodec)
            .with_distributed_work_unit_feed(|leaf: &RowGeneratorExec| Some(&leaf.feed))
            .with_distributed_desired_task_count_handler(row_generator_desired_task_count_handler)
            .with_distributed_scale_up_leaf_node_handler(row_generator_scale_up_leaf_node_handler)
            .with_distributed_planner()
            .build();
        let ctx = SessionContext::new_with_state(state);
        let input = || {
            Arc::new(Input(Arc::new(RowGeneratorExec::new(
                crate::WorkUnitFeed::new(RowGeneratorFeedProvider::shared(2, 1..=64)).with_demand(),
                "input".into(),
                1,
                None,
                64,
            )))) as Arc<dyn TableProvider>
        };
        ctx.register_table("input", input())?;
        let sql = format!(
            "SELECT SUM(n) AS total FROM ({})",
            (0..8)
                .map(|_| "SELECT SUM(CASE WHEN task >= 0 THEN 1 ELSE 0 END) AS n FROM input")
                .collect::<Vec<_>>()
                .join(" UNION ALL ")
        );
        let physical = ctx.sql(&sql).await?.create_physical_plan().await?;
        let streams = super::peer_streams(&physical, 2)?;
        assert!(
            starts.try_recv().is_err(),
            "physical planning must not read input"
        );
        let stop = tokio_util::sync::CancellationToken::new();
        let stopping = stop.clone();
        let service = worker
            .clone()
            .into_worker_server()
            .max_decoding_message_size(2 * 1024 * 1024)
            .max_encoding_message_size(2 * 1024 * 1024);
        let server = SpawnedTask::spawn(async move {
            tonic::transport::Server::builder()
                .max_concurrent_streams(Some(streams))
                .add_service(service)
                .serve_with_incoming_shutdown(
                    tokio_stream::wrappers::TcpListenerStream::new(listener),
                    stopping.cancelled_owned(),
                )
                .await
        });
        let task_ctx = ctx.task_ctx();
        let mut query =
            SpawnedTask::spawn(async move { collect(execute_stream(physical, task_ctx)?).await });
        tokio::time::timeout(std::time::Duration::from_secs(20), async {
            while setups.load(Ordering::SeqCst) < 16 {
                setup_changes.changed().await.unwrap();
            }
            while starts.recv().await.unwrap().1 != 1 {}
        })
        .await
        .expect("whole DAG setup exhausted HTTP/2 streams");
        assert!(
            futures::poll!(&mut query).is_pending(),
            "the required input is still held at its barrier"
        );
        gate.release.add_permits(1);
        let actual = tokio::time::timeout(std::time::Duration::from_secs(20), &mut query)
            .await
            .unwrap()
            .unwrap()?;
        let native = SessionContext::new();
        let batch = RecordBatch::try_from_iter([(
            "value",
            Arc::new(Int64Array::from_iter_values(0..64)) as datafusion::arrow::array::ArrayRef,
        )])?;
        native.register_table(
            "input",
            Arc::new(MemTable::try_new(batch.schema(), vec![vec![batch]])?),
        )?;
        let expected = native
            .sql("SELECT COUNT(*) AS total FROM input")
            .await?
            .collect()
            .await?;
        assert_eq!(
            pretty_format_batches(&actual)?.to_string(),
            pretty_format_batches(&expected)?.to_string()
        );
        drop(actual);
        drop(expected);
        drop(native);
        let first_setups = setups.load(Ordering::SeqCst);
        ctx.deregister_table("input")?;
        ctx.register_table("input", input())?;
        let physical = ctx.sql(&sql).await?.create_physical_plan().await?;
        let task_ctx = ctx.task_ctx();
        let cancel = tokio_util::sync::CancellationToken::new();
        let cancelling = cancel.clone();
        let query = SpawnedTask::spawn(async move {
            let stream = execute_stream(physical, task_ctx)?;
            tokio::select! {
                () = cancelling.cancelled() => Ok(Vec::new()),
                result = collect(stream) => result,
            }
        });
        tokio::time::timeout(std::time::Duration::from_secs(20), async {
            while setups.load(Ordering::SeqCst) < first_setups + 16 {
                setup_changes.changed().await.unwrap();
            }
            while starts.recv().await.unwrap().1 != 1 {}
        })
        .await
        .expect("second DAG setup blocked");
        cancel.cancel();
        tokio::time::timeout(std::time::Duration::from_secs(5), query)
            .await
            .unwrap()
            .unwrap()?;
        drop(ctx);
        drop(http);
        tokio::time::timeout(std::time::Duration::from_secs(5), worker.shutdown())
            .await
            .unwrap();
        drop(worker);
        stop.cancel();
        tokio::time::timeout(std::time::Duration::from_secs(5), server)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        drop(runtime);
        drop(gate);
        assert_eq!(
            pool.reserved(),
            0,
            "EOF and cancellation release every stage/Flight/sampler owner"
        );
        let path =
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("target/replays/whole-dag.tsv");
        std::fs::create_dir_all(path.parent().unwrap())?;
        std::fs::write(
            path,
            format!(
                "command: cargo +1.97.1 test --offline --lib whole_dag_peer_capacity\ninput: 8 COUNT branches, 2 tasks, one TCP/H2 peer, ID1 barrier\nexpected: all branches prepare before release; native SQL total64; cancellation joins while ID1 remains held; final pool0\nobserved: streamEnvelope={streams} setups={} finalReserved={}\n",
                setups.load(Ordering::SeqCst),
                pool.reserved()
            ),
        )?;
        Ok(())
    }
}
