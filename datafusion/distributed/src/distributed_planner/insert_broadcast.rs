use std::sync::Arc;

use datafusion::common::JoinType;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::config::ConfigOptions;
use datafusion::error::DataFusionError;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::joins::{
    CrossJoinExec, HashJoinExec, NestedLoopJoinExec, PartitionMode,
};
use datafusion::physical_plan::{ChildrenPropertiesMode, ExecutionPlan, ReplaceChildrenOptions};

use crate::BroadcastExec;

use super::DistributedConfig;

/// This is a top-down traversal of a [ExecutionPlan] that inserts [BroadcastExec] operators where
/// appropriate.
///
/// # What is it doing?
/// The pass searches for joins whose left input can be broadcast without duplicating output rows:
/// CollectLeft [HashJoinExec]s, [NestedLoopJoinExec]s, and [CrossJoinExec]s. Then it does one of
/// two things:
///     1. If the build child is a fetch-less [CoalescePartitionsExec] -> Insert a
///        [BroadcastExec] directly below it. A fetch-bearing coalesce stays below the broadcast
///        so its global limit is applied before the rows are replicated to consumers.
///     2. Otherwise -> Insert a [BroadcastExec] below the join but above its
///        original build child, coalescing only when it has multiple partitions.
/// ```text
///                  ┌──────────────────────┐                                                    ┌──────────────────────┐
///                  │   CoalesceBatches    │                                                    │   CoalesceBatches    │
///                  └───────────▲──────────┘                                                    └───────────▲──────────┘
///                              │                                                                           │
///                  ┌───────────┴──────────┐                                                    ┌───────────┴──────────┐
///                  │       HashJoin       │                                                    │       HashJoin       │
///                  │    (CollectLeft)     │                                                    │    (CollectLeft)     │
///                  └────▲────────────▲────┘                                                    └────▲────────────▲────┘
///                       │            │                                                              │            │
///             ┌─────────┘            └──────────┐                                         ┌─────────┘            └──────────┐
///        Build Side                        Probe Side                                Build Side                        Probe Side
///             │                                 │                                         │                                 │
/// ┌───────────┴──────────┐          ┌───────────┴──────────┐                  ┌───────────┴──────────┐          ┌───────────┴──────────┐
/// │  CoalescePartitions  │          │      Projection      │                  │  CoalescePartitions  │          │      Projection      │
/// └───▲────▲────▲────▲───┘          └───────────▲──────────┘                  └───────────▲──────────┘          └───────────▲──────────┘
///     │    │    │    │                          │                                         │                                 │
/// ┌───┴────┴────┴────┴───┐          ┌───────────┴──────────┐                  ┌───────────┴──────────┐          ┌───────────┴──────────┐
/// │      DataSource      │          │     Aggregation      │ ───────────────▶ │    BroadcastExec     │          │     Aggregation      │
/// └──────────────────────┘          └───────────▲──────────┘                  └──▲────▲────▲────▲────┘          └───────────▲──────────┘
///                                               │                                │    │    │    │                           │
///                                   ┌───────────┴──────────┐                  ┌──┴────┴────┴────┴────┐          ┌───────────┴──────────┐
///                                   │     Repartition      │                  │      DataSource      │          │     Repartition      │
///                                   └───────────▲──────────┘                  └──────────────────────┘          └───────────▲──────────┘
///                                               │                                                                           │
///                                   ┌───────────┴──────────┐                                                    ┌───────────┴──────────┐
///                                   │     Aggregation      │                                                    │     Aggregation      │
///                                   │      (Partial)       │                                                    │      (Partial)       │
///                                   └───────────▲──────────┘                                                    └───────────▲──────────┘
///                                               │                                                                           │
///                                   ┌───────────┴──────────┐                                                    ┌───────────┴──────────┐
///                                   │      DataSource      │                                                    │      DataSource      │
///                                   └──────────────────────┘                                                    └──────────────────────┘
/// ```
///
/// # Why a Right or Inner join type?
/// Left and Full join types allow build side rows to be emitted. This creates complicatioins when
/// broadcasting your build side to all workers as it can cause incorrect and duplicate data. Here
/// is an example:
///
/// Say there is arbitrary tables containing information on customers and their orders.
/// ```text
///                                         Probe Side
///                                 ┌──────────┬─────────────┐
///         Build Side              │ order_id │ customer_id │
/// ┌─────────────┬─────────┐       ├──────────┼─────────────┤
/// │ customer_id │  Name   │       │   100    │      1      │
/// ├─────────────┼─────────┤       ├──────────┼─────────────┤
/// │      1      │  John   │       │   200    │      1      │
/// ├─────────────┼─────────┤       ├──────────┼─────────────┤
/// │      2      │  Alice  │       │   300    │      1      │
/// ├─────────────┼─────────┤       ├──────────┼─────────────┤
/// │      3      │   Bob   │       │   400    │      2      │
/// └─────────────┴─────────┘       ├──────────┼─────────────┤
///                                 │   500    │      2      │
///                                 └──────────┴─────────────┘
/// ```
/// Then want to execute the query:
/// SELECT * FROM customers c
/// WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.customer_id);
///
/// This query is selecting all customers that have not made an order. It does this by using a
/// LeftAnti join which will emit all the rows from our build side which do not have a matching
/// join key (in this case customer_id) on the probe side.
///
/// In a single node this would produce the correct result: (3, Bob)
///
/// In a distributed context broadcasting the build table would create incorrect results:
/// ```text
/// ┌──────────────────────────────────────────────────────────┐    ┌──────────────────────────────────────────────────────────┐
/// │                         Worker 1                         │    │                         Worker 1                         │
/// │  ┌─────────────┬─────────┐   ┌──────────┬─────────────┐  │    │  ┌─────────────┬─────────┐                               │
/// │  │ customer_id │  Name   │   │ order_id │ customer_id │  │    │  │ customer_id │  Name   │   ┌──────────┬─────────────┐  │
/// │  ├─────────────┼─────────┤   ├──────────┼─────────────┤  │    │  ├─────────────┼─────────┤   │ order_id │ customer_id │  │
/// │  │      1      │  John   │   │   100    │      1      │  │    │  │      1      │  John   │   ├──────────┼─────────────┤  │
/// │  ├─────────────┼─────────┤   ├──────────┼─────────────┤  │    │  ├─────────────┼─────────┤   │   400    │      2      │  │
/// │  │      2      │  Alice  │   │   200    │      1      │  │    │  │      2      │  Alice  │   ├──────────┼─────────────┤  │
/// │  ├─────────────┼─────────┤   ├──────────┼─────────────┤  │    │  ├─────────────┼─────────┤   │   500    │      2      │  │
/// │  │      3      │   Bob   │   │   300    │      1      │  │    │  │      3      │   Bob   │   └──────────┴─────────────┘  │
/// │  └─────────────┴─────────┘   └──────────┴─────────────┘  │    │  └─────────────┴─────────┘                               │
/// └──────────────────────────────────────────────────────────┘    └──────────────────────────────────────────────────────────┘
/// ```
/// Worker 1 would emit: (2, Alice), (3, Bob)
/// Worker 2 would emit: (1, John), (3, Bob)
/// Thus when unioning results: (2, Alice), (3, Bob), (1, John), (3, Bob)
/// ```
///
/// ```
pub(super) fn insert_broadcast_execs(
    plan: Arc<dyn ExecutionPlan>,
    cfg: &ConfigOptions,
) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
    let d_cfg = DistributedConfig::from_config_options(cfg)?;
    if !d_cfg.broadcast_joins {
        return Ok(plan);
    }

    plan.transform_down(|node| {
        if !can_broadcast_left_input(node.as_ref()) {
            return Ok(Transformed::no(node));
        }

        let children = node.children();
        let Some(build_child) = children.first() else {
            return Ok(Transformed::no(node));
        };

        let new_build_child: Arc<dyn ExecutionPlan> = if let Some(coalesce) = build_child
            .downcast_ref::<CoalescePartitionsExec>()
            .filter(|coalesce| coalesce.fetch().is_none())
        {
            if coalesce.input().is::<BroadcastExec>() {
                return Ok(Transformed::no(node));
            }
            coalesced_broadcast(Arc::clone(coalesce.input()))
        } else if build_child.is::<BroadcastExec>() {
            coalesce(Arc::clone(build_child))
        } else {
            // A fetch-bearing coalesce remains below the broadcast so the limit is global.
            coalesced_broadcast(Arc::clone(build_child))
        };

        let mut new_children: Vec<Arc<dyn ExecutionPlan>> = children.into_iter().cloned().collect();
        new_children[0] = new_build_child;
        Ok(Transformed::yes(node.replace_children(
            new_children,
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )?))
    })
    .map(|transformed| transformed.data)
}

fn coalesced_broadcast(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    // consumer_task_count=1 is a placeholder and will be corrected during optimizer rule.
    let broadcast: Arc<dyn ExecutionPlan> = Arc::new(BroadcastExec::new(input, 1));
    coalesce(broadcast)
}

fn coalesce(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    // Coalescing even one partition discards its constant evidence. Existing
    // sorts may have omitted those constants, so preserve the valid input.
    if input.properties().partitioning.partition_count() == 1 {
        input
    } else {
        Arc::new(CoalescePartitionsExec::new(input))
    }
}

fn can_broadcast_left_input(plan: &dyn ExecutionPlan) -> bool {
    if let Some(hash_join) = plan.downcast_ref::<HashJoinExec>() {
        return hash_join.partition_mode() == &PartitionMode::CollectLeft
            && is_left_broadcast_safe(hash_join.join_type());
    }

    if let Some(nested_loop_join) = plan.downcast_ref::<NestedLoopJoinExec>() {
        return is_left_broadcast_safe(nested_loop_join.join_type());
    }

    plan.downcast_ref::<CrossJoinExec>().is_some()
}

pub(super) fn is_left_broadcast_safe(join_type: &JoinType) -> bool {
    matches!(
        join_type,
        JoinType::Inner
            | JoinType::Right
            | JoinType::RightSemi
            | JoinType::RightAnti
            | JoinType::RightMark
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::assert_snapshot;
    use crate::test_utils::plans::{TestPlan, TestPlanBuilder};
    use datafusion::arrow::{
        array::{ArrayRef, Int64Array, StringArray},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use datafusion::common::{NullEquality, ScalarValue};
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::functions_window::row_number::row_number_udwf;
    use datafusion::logical_expr::{
        Operator, WindowFrame, WindowFrameBound, WindowFrameUnits, WindowFunctionDefinition,
    };
    use datafusion::physical_expr::{
        PhysicalExpr, PhysicalSortExpr,
        expressions::{BinaryExpr, CastExpr, Column, Literal},
    };
    use datafusion::physical_optimizer::{
        PhysicalOptimizerRule, ensure_requirements::EnsureRequirements,
    };
    use datafusion::physical_plan::displayable;
    use datafusion::physical_plan::{
        InputOrderMode, collect,
        execution_plan::reset_plan_states,
        filter::FilterExec,
        projection::ProjectionExec,
        union::UnionExec,
        windows::{BoundedWindowAggExec, create_window_expr},
    };
    use datafusion::prelude::SessionContext;

    #[tokio::test]
    async fn broadcast_preparation_preserves_window_partition_evidence()
    -> datafusion::common::Result<()> {
        let ctx = SessionContext::new_with_config(
            datafusion::prelude::SessionConfig::new().with_target_partitions(1),
        );
        let mut options = ctx.copied_config().options().as_ref().clone();
        options.extensions.insert(DistributedConfig {
            broadcast_joins: true,
            ..Default::default()
        });
        // Two physical partitions have different request constants. They still
        // need coalescing; neither request may become a global constant.
        for (parts, broadcast) in [(1, false), (1, true), (2, false), (2, true)] {
            let native = window(parts, broadcast, &ctx, &options)?;
            let once = insert_broadcast_execs(Arc::clone(&native), &options)?;
            let twice = insert_broadcast_execs(Arc::clone(&once), &options)?;
            let expected = ["first", "second"][..parts]
                .iter()
                .flat_map(|request| {
                    [
                        [request.to_string(), "2023".into(), "2".into()].to_vec(),
                        [request.to_string(), "2024".into(), "1".into()].to_vec(),
                    ]
                })
                .collect::<Vec<_>>();
            for plan in [native, once, twice] {
                let actual = rows(&collect(reset_plan_states(plan)?, ctx.task_ctx()).await?)?;
                assert_eq!(
                    actual, expected,
                    "parts={parts}, initially broadcast={broadcast}"
                );
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_insert_broadcast_with_existing_coalesce_build_child() {
        let query = r#"
        SELECT a."MinTemp", b."MaxTemp"
        FROM weather a INNER JOIN weather b
        ON a."RainToday" = b."RainToday"
        "#;
        let physical_plan_string = TestPlanBuilder::default()
            .num_workers(4)
            .physical_plan_as_string(query)
            .await;
        assert_snapshot!(physical_plan_string, @r"
        DistributedExec
          CoalescePartitionsExec
            HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(RainToday@1, RainToday@1)], projection=[MinTemp@0, MaxTemp@2]
              CoalescePartitionsExec
                [Stage 1] => NetworkCoalesceExec: output_partitions=12, input_tasks=4
                  DistributedLeafExec: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp, RainToday], file_type=parquet
              DistributedLeafExec: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MaxTemp, RainToday], file_type=parquet, predicate=DynamicFilter [ empty ], dynamic_rg_pruning=eligible
        ");
        let test_plan = TestPlanBuilder::new()
            .target_partitions(4)
            .broadcast_joins(true)
            .build()
            .await;
        let plan = apply_broadcasts(&test_plan, query, 1).await;
        let plan = displayable(plan.as_ref()).indent(true).to_string();
        assert_snapshot!(plan, @"
        HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(RainToday@1, RainToday@1)], projection=[MinTemp@0, MaxTemp@2]
          CoalescePartitionsExec
            BroadcastExec: input_partitions=3, consumer_tasks=1, output_partitions=3
              DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp, RainToday], file_type=parquet
          DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MaxTemp, RainToday], file_type=parquet, predicate=DynamicFilter [ empty ], dynamic_rg_pruning=eligible
        ");
    }

    #[tokio::test]
    async fn test_insert_broadcast_without_existing_coalesce_build_child() {
        let query = r#"
        SELECT a."MinTemp", b."MaxTemp"
        FROM weather a INNER JOIN weather b
        ON a."RainToday" = b."RainToday"
        "#;
        let physical_plan_string = TestPlanBuilder::new()
            .target_partitions(1)
            .num_workers(4)
            .physical_plan_as_string(query)
            .await;
        assert_snapshot!(physical_plan_string, @"
        HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(RainToday@1, RainToday@1)], projection=[MinTemp@0, MaxTemp@2]
          DataSourceExec: file_groups={1 group: [[/testdata/weather/result-000000.parquet, /testdata/weather/result-000001.parquet, /testdata/weather/result-000002.parquet]]}, projection=[MinTemp, RainToday], file_type=parquet
          DataSourceExec: file_groups={1 group: [[/testdata/weather/result-000000.parquet, /testdata/weather/result-000001.parquet, /testdata/weather/result-000002.parquet]]}, projection=[MaxTemp, RainToday], file_type=parquet, predicate=DynamicFilter [ empty ], dynamic_rg_pruning=eligible
        ");
        let test_plan = TestPlanBuilder::new()
            .target_partitions(1)
            .broadcast_joins(true)
            .build()
            .await;
        let plan = apply_broadcasts(&test_plan, query, 1).await;
        let plan = displayable(plan.as_ref()).indent(true).to_string();
        assert_snapshot!(plan, @"
        HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(RainToday@1, RainToday@1)], projection=[MinTemp@0, MaxTemp@2]
          BroadcastExec: input_partitions=1, consumer_tasks=1, output_partitions=1
            DataSourceExec: file_groups={1 group: [[/testdata/weather/result-000000.parquet, /testdata/weather/result-000001.parquet, /testdata/weather/result-000002.parquet]]}, projection=[MinTemp, RainToday], file_type=parquet
          DataSourceExec: file_groups={1 group: [[/testdata/weather/result-000000.parquet, /testdata/weather/result-000001.parquet, /testdata/weather/result-000002.parquet]]}, projection=[MaxTemp, RainToday], file_type=parquet, predicate=DynamicFilter [ empty ], dynamic_rg_pruning=eligible
        ");
    }

    #[tokio::test]
    async fn test_no_broadcast_left_join() {
        let query = r#"
        SELECT a."MinTemp", b."MaxTemp"
        FROM weather a LEFT JOIN weather b
        ON a."RainToday" = b."RainToday"
        "#;
        let test_plan = TestPlanBuilder::new()
            .target_partitions(4)
            .broadcast_joins(true)
            .build()
            .await;
        let plan = apply_broadcasts(&test_plan, query, 1).await;
        let plan = displayable(plan.as_ref()).indent(true).to_string();
        assert_snapshot!(plan, @"
        HashJoinExec: mode=CollectLeft, join_type=Left, on=[(RainToday@1, RainToday@1)], projection=[MinTemp@0, MaxTemp@2]
          CoalescePartitionsExec
            DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp, RainToday], file_type=parquet
          DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MaxTemp, RainToday], file_type=parquet, predicate=DynamicFilter [ empty ], dynamic_rg_pruning=eligible
        ");
    }

    #[tokio::test]
    async fn test_no_broadcast_when_disabled() {
        let query = r#"
        SELECT a."MinTemp", b."MaxTemp"
        FROM weather a INNER JOIN weather b
        ON a."RainToday" = b."RainToday"
        "#;
        let test_plan = TestPlanBuilder::new()
            .target_partitions(4)
            .broadcast_joins(false)
            .build()
            .await;
        let plan = apply_broadcasts(&test_plan, query, 1).await;
        let plan = displayable(plan.as_ref()).indent(true).to_string();
        assert_snapshot!(plan, @"
        HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(RainToday@1, RainToday@1)], projection=[MinTemp@0, MaxTemp@2]
          CoalescePartitionsExec
            DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp, RainToday], file_type=parquet
          DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MaxTemp, RainToday], file_type=parquet, predicate=DynamicFilter [ empty ], dynamic_rg_pruning=eligible
        ");
    }

    #[tokio::test]
    async fn test_insert_broadcast_keeps_fetch_bearing_coalesce_below_broadcast() {
        let query = r#"
        SELECT a."MinTemp", b."MaxTemp"
        FROM (SELECT "MinTemp", "RainToday" FROM weather OFFSET 0 LIMIT 50) a
        INNER JOIN weather b
        ON a."RainToday" = b."RainToday"
        "#;

        let test_plan = TestPlanBuilder::new()
            .target_partitions(4)
            .broadcast_joins(true)
            .build()
            .await;
        let plan = apply_broadcasts(&test_plan, query, 1).await;
        let plan = displayable(plan.as_ref()).indent(true).to_string();
        assert_snapshot!(plan, @r"
        HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(RainToday@1, RainToday@1)], projection=[MinTemp@0, MaxTemp@2]
          BroadcastExec: input_partitions=1, consumer_tasks=1, output_partitions=1
            DataSourceExec: file_groups={1 group: [[/testdata/weather/result-<shard>.parquet]]}, projection=[MinTemp, RainToday], limit=50, file_type=parquet
          DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MaxTemp, RainToday], file_type=parquet, predicate=DynamicFilter [ empty ], dynamic_rg_pruning=eligible
        ");
    }

    #[tokio::test]
    async fn test_insert_broadcast_cross_join() {
        let query = r#"
        SELECT a."MinTemp", b."MaxTemp"
        FROM weather a CROSS JOIN weather b
        "#;
        let test_plan = TestPlanBuilder::new()
            .target_partitions(4)
            .broadcast_joins(true)
            .build()
            .await;
        let plan = apply_broadcasts(&test_plan, query, 1).await;
        let plan = displayable(plan.as_ref()).indent(true).to_string();
        assert_snapshot!(plan, @"
        CrossJoinExec
          CoalescePartitionsExec
            BroadcastExec: input_partitions=3, consumer_tasks=1, output_partitions=3
              DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp], file_type=parquet
          DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MaxTemp], file_type=parquet
        ");
    }

    #[tokio::test]
    async fn test_insert_broadcast_nested_loop_inner_join() {
        let query = r#"
        SELECT a."MinTemp", b."MaxTemp"
        FROM weather a JOIN weather b ON a."MinTemp" > b."MaxTemp"
        "#;
        let test_plan = TestPlanBuilder::new()
            .target_partitions(4)
            .broadcast_joins(true)
            .build()
            .await;
        let plan = apply_broadcasts(&test_plan, query, 1).await;
        let plan = displayable(plan.as_ref()).indent(true).to_string();
        assert_snapshot!(plan, @"
        NestedLoopJoinExec: join_type=Inner, filter=MinTemp@0 > MaxTemp@1
          CoalescePartitionsExec
            BroadcastExec: input_partitions=3, consumer_tasks=1, output_partitions=3
              DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp], file_type=parquet
          DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MaxTemp], file_type=parquet
        ");
    }

    #[tokio::test]
    async fn test_insert_broadcast_is_idempotent_for_supported_joins() {
        let queries = [
            r#"
            SELECT a."MinTemp", b."MaxTemp"
            FROM weather a INNER JOIN weather b
            ON a."RainToday" = b."RainToday"
            "#,
            r#"
            SELECT a."MinTemp", b."MaxTemp"
            FROM weather a JOIN weather b ON a."MinTemp" > b."MaxTemp"
            "#,
            r#"
            SELECT a."MinTemp", b."MaxTemp"
            FROM weather a CROSS JOIN weather b
            "#,
        ];

        let test_plan = TestPlanBuilder::new()
            .target_partitions(4)
            .broadcast_joins(true)
            .build()
            .await;

        for query in queries {
            let once = apply_broadcasts(&test_plan, query, 1).await;
            let twice = apply_broadcasts(&test_plan, query, 2).await;
            let once = displayable(once.as_ref()).indent(true).to_string();
            let twice = displayable(twice.as_ref()).indent(true).to_string();

            assert_eq!(once, twice);
            assert_eq!(once.matches("BroadcastExec").count(), 1);
        }
    }

    #[tokio::test]
    async fn test_no_broadcast_nested_loop_left_join() {
        let query = r#"
        SELECT a."MinTemp", b."MaxTemp"
        FROM weather a LEFT JOIN weather b ON a."MinTemp" > b."MaxTemp"
        "#;
        let test_plan = TestPlanBuilder::new()
            .target_partitions(4)
            .broadcast_joins(true)
            .build()
            .await;
        let plan = apply_broadcasts(&test_plan, query, 1).await;
        let plan = displayable(plan.as_ref()).indent(true).to_string();
        assert_snapshot!(plan, @"
        NestedLoopJoinExec: join_type=Left, filter=MinTemp@0 > MaxTemp@1
          CoalescePartitionsExec
            DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp], file_type=parquet
          DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MaxTemp], file_type=parquet
        ");
    }

    async fn apply_broadcasts(
        test_plan: &TestPlan,
        query: &str,
        applications: usize,
    ) -> Arc<dyn ExecutionPlan> {
        let ctx = test_plan.get_ctx();
        let mut plan = test_plan.physical_plan(query).await;
        for _ in 0..applications {
            plan = insert_broadcast_execs(plan, ctx.state_ref().read().config_options().as_ref())
                .expect("failed to insert broadcasts");
        }
        plan
    }

    // Metadata casts conservatively describe a filtered key as constant only
    // within each partition. Use real operators to derive that property.
    fn request(value: &str) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("company", DataType::Int64, false),
            Field::new("request", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
                Arc::new(StringArray::from(vec![value, "unselected"])),
            ],
        )?;
        let source = MemorySourceConfig::try_new_exec(&[vec![batch]], schema.clone(), None)?;
        let predicate = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("request", 1)),
            Operator::Eq,
            Arc::new(Literal::new(ScalarValue::Utf8(Some(value.into())))),
        ));
        let filtered = Arc::new(FilterExec::try_new(predicate, source)?);
        let field = Arc::new(
            Field::new("request", DataType::Utf8, false)
                .with_metadata([("logical.contract".to_string(), "request".to_string())]),
        );
        Ok(Arc::new(ProjectionExec::try_new(
            vec![
                (
                    Arc::new(Column::new("company", 0)) as Arc<dyn PhysicalExpr>,
                    "company".into(),
                ),
                (
                    Arc::new(CastExpr::new_with_target_field(
                        Arc::new(Column::new("request", 1)),
                        field,
                        None,
                    )),
                    "request".into(),
                ),
            ],
            filtered,
        )?))
    }

    fn window(
        parts: usize,
        broadcast: bool,
        ctx: &SessionContext,
        options: &ConfigOptions,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let mut left = if parts == 1 {
            request("first")?
        } else {
            UnionExec::try_new(vec![request("first")?, request("second")?])?
        };
        if broadcast {
            left = Arc::new(BroadcastExec::new(left, 1));
        }
        let schema = Arc::new(Schema::new(vec![
            Field::new("company", DataType::Int64, false),
            Field::new("period", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 1])) as ArrayRef,
                Arc::new(Int64Array::from(vec![2023, 2024])),
            ],
        )?;
        let right = MemorySourceConfig::try_new_exec(&[vec![batch]], schema, None)?;
        let on = vec![(
            Arc::new(Column::new("company", 0)) as Arc<dyn PhysicalExpr>,
            Arc::new(Column::new("company", 0)) as Arc<dyn PhysicalExpr>,
        )];
        let joined = Arc::new(HashJoinExec::try_new(
            left,
            right,
            on,
            None,
            &JoinType::Inner,
            Some(vec![1, 3]),
            PartitionMode::CollectLeft,
            NullEquality::NullEqualsNothing,
            false,
        )?) as Arc<dyn ExecutionPlan>;
        let partition = vec![Arc::new(Column::new("request", 0)) as Arc<dyn PhysicalExpr>];
        let order = vec![PhysicalSortExpr::new(
            Arc::new(Column::new("period", 1)),
            datafusion::arrow::compute::SortOptions {
                descending: true,
                nulls_first: true,
            },
        )];
        let frame = Arc::new(WindowFrame::new_bounds(
            WindowFrameUnits::Rows,
            WindowFrameBound::Preceding(ScalarValue::UInt64(None)),
            WindowFrameBound::CurrentRow,
        ));
        let window = create_window_expr(
            &WindowFunctionDefinition::WindowUDF(row_number_udwf()),
            "ordinal".into(),
            &[],
            &partition,
            &order,
            frame,
            joined.schema(),
            false,
            false,
            None,
        )?;
        let plan = Arc::new(BoundedWindowAggExec::try_new(
            vec![window],
            joined,
            InputOrderMode::Sorted,
            true,
        )?) as Arc<dyn ExecutionPlan>;
        let native = EnsureRequirements::new().optimize(plan, options)?;
        datafusion::physical_planner::DefaultPhysicalPlanner::default().optimize_physical_plan(
            native,
            &ctx.state(),
            |_, _| {},
        )
    }

    fn rows(batches: &[RecordBatch]) -> datafusion::common::Result<Vec<Vec<String>>> {
        let mut rows = batches
            .iter()
            .flat_map(|batch| {
                (0..batch.num_rows()).map(move |row| {
                    batch
                        .columns()
                        .iter()
                        .map(|column| {
                            ScalarValue::try_from_array(column, row).map(|value| value.to_string())
                        })
                        .collect::<datafusion::common::Result<Vec<_>>>()
                })
            })
            .collect::<datafusion::common::Result<Vec<_>>>()?;
        rows.sort_unstable();
        Ok(rows)
    }
}
