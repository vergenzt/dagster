import time

from dagster import (
    AutomationCondition,
    DailyPartitionsDefinition,
    HourlyPartitionsDefinition,
    evaluate_automation_conditions,
)
from dagster._core.instance import DagsterInstance
from dagster_test.toys.auto_materializing.large_graph import AssetLayerConfig, build_assets


def test_eager_perf() -> None:
    hourly_partitions_def = HourlyPartitionsDefinition("2020-01-01-00:00")
    daily_partitions_def = DailyPartitionsDefinition("2020-01-01")
    assets = build_assets(
        id="perf_test",
        layer_configs=[
            AssetLayerConfig(100, 0, hourly_partitions_def),
            AssetLayerConfig(200, 2, hourly_partitions_def),
            AssetLayerConfig(200, 4, hourly_partitions_def),
            AssetLayerConfig(200, 4, daily_partitions_def),
            AssetLayerConfig(200, 2, daily_partitions_def),
            AssetLayerConfig(100, 2, daily_partitions_def),
        ],
        automation_condition=AutomationCondition.eager(),
    )

    instance = DagsterInstance.ephemeral()
    cursor = None
    start = time.time()
    for _ in range(2):
        cursor = evaluate_automation_conditions(
            defs=assets, instance=instance, cursor=cursor
        ).cursor
        end = time.time()
        duration = end - start
        # all iterations should take less than 20 seconds on this graph
        assert duration < 20.0
        start = time.time()
