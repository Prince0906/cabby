"""Tests for the S1 pipeline pieces: schema check, raw_trips, quality checks and the sensor.

They use the fakes (no AWS). The blocking test runs the real Dagster job: a file whose schema moved
must stop the run before raw_trips issues its COPY.
"""

import json

import dagster as dg
import pytest
from fakes import LOG, fake_bucket, fake_redshift, schema_2023, schema_2025

from labpipes.contracts import Column, trips_schema
from labpipes.defs import quality
from labpipes.defs.raw_trips import load_statements, raw_trips
from labpipes.defs.schema_check import schema_contract, schema_mismatches
from labpipes.defs.sensor import trip_files_sensor
from labpipes.defs.trip_files import trip_files

URI = "s3://data/trips/yellow_tripdata_2024-03.parquet"


# --- schema_contract: what counts as a mismatch -------------------------------------------------------


def test_the_contract_matches_itself():
    assert schema_mismatches(trips_schema(), trips_schema()) == []


def test_names_compare_case_insensitively():
    renamed = [Column(c.name.lower(), c.type) for c in trips_schema()]
    assert schema_mismatches(trips_schema(), renamed) == []


def test_a_missing_column_blocks():
    assert any("missing column(s): airport_fee" in p for p in schema_mismatches(trips_schema(), trips_schema()[:-1]))


def test_an_extra_trailing_column_blocks():
    assert any("unexpected column(s): cbd_congestion_fee" in p for p in schema_mismatches(trips_schema(), schema_2025()))


def test_a_reordered_column_blocks():
    cols = trips_schema()
    swapped = [cols[1], cols[0], *cols[2:]]
    assert any("reordered" in p for p in schema_mismatches(cols, swapped))


def test_a_changed_type_blocks():
    problems = schema_mismatches(trips_schema(), schema_2023())
    assert any("type of vendorid: expected INT32, found INT64" in p for p in problems)


# --- raw_trips: idempotent, positional-safe load ------------------------------------------------------


def test_load_is_delete_then_copy_then_insert_in_that_order():
    sqls = load_statements("2024-03", URI)
    kinds = [s.split()[0] for s in sqls]
    assert kinds == ["create", "create", "delete", "create", "copy", "insert"]
    assert sqls[2] == "delete from ingest.raw_trips where source_file = 'yellow_tripdata_2024-03.parquet'"
    assert f"copy raw_trips_load from '{URI}' iam_role default format as parquet" == sqls[4]
    assert sqls[5] == "insert into ingest.raw_trips select 'yellow_tripdata_2024-03.parquet', * from raw_trips_load"
    assert not any("{" in s for s in sqls)


def test_load_uses_the_contract_columns_in_order():
    sqls = load_statements("2024-03", URI)
    temp = next(s for s in sqls if s.startswith("create temp table"))
    assert f"({'vendorid INTEGER, tpep_pickup_datetime TIMESTAMP'}" in temp
    assert temp.count(",") == len(trips_schema()) - 1
    assert "source_file varchar(64) not null" in sqls[1]


def test_load_refuses_a_month_that_could_inject_sql():
    with pytest.raises(ValueError):
        load_statements("2024-03'; drop table x; --", URI)


# --- quality checks: block when unusable, warn when odd -------------------------------------------------


def test_share_below_the_warn_limit_passes():
    r = quality.judge_share(0.0046, "dropped", warn=0.01, block=0.03)
    assert r.passed


def test_share_between_the_limits_warns_without_blocking():
    r = quality.judge_share(0.02, "dropped", warn=0.01, block=0.03)
    assert not r.passed and r.severity == dg.AssetCheckSeverity.WARN


def test_share_above_the_block_limit_blocks():
    r = quality.judge_share(0.08, "dropped", warn=0.01, block=0.03)
    assert not r.passed and r.severity == dg.AssetCheckSeverity.ERROR


def test_a_warn_only_check_never_blocks():
    r = quality.judge_share(0.9, "null passengers", warn=0.25)
    assert not r.passed and r.severity == dg.AssetCheckSeverity.WARN


def test_the_observed_months_all_pass_the_measured_thresholds():
    # 2024-01..04, as measured in Redshift (see the table in quality.py)
    for dropped in (0.00496, 0.00457, 0.00466, 0.00457):
        assert quality.judge_share(dropped, "dropped", warn=quality.MART_DROP_WARN, block=quality.MART_DROP_BLOCK).passed
    # March against the median of January and February (+20%): odd, not blocked
    r = quality.judge_deviation(3_582_628, (2_964_624 + 3_007_526) / 2, "rows", warn=quality.ROWS_VS_HISTORY_WARN,
                                block=quality.ROWS_VS_HISTORY_BLOCK)
    assert r.passed
    # and every month against the median of the other three, whatever order they loaded in
    counts = {"2024-01": 2_964_624, "2024-02": 3_007_526, "2024-03": 3_582_628, "2024-04": 3_514_289}
    for m, n in counts.items():
        r = quality.judge_deviation(n, quality.baseline_of_others(counts, m), "rows", warn=quality.ROWS_VS_HISTORY_WARN,
                                    block=quality.ROWS_VS_HISTORY_BLOCK)
        assert r.passed, m


def test_the_first_month_has_nothing_to_compare_with():
    assert quality.baseline_of_others({"2024-01": 2_964_624}, "2024-01") is None
    assert quality.judge_deviation(2_964_624, None, "rows", warn=0.30, block=0.50).passed


def test_the_baseline_ignores_load_order_and_the_month_itself():
    only_later = {"2024-01": 1.0, "2024-03": 3.0, "2024-04": 5.0}
    assert quality.baseline_of_others(only_later, "2024-01") == 4.0  # the later months count too
    assert quality.baseline_of_others(only_later, "2024-04") == 2.0  # the month itself never does


def test_a_collapsed_month_blocks():
    r = quality.judge_deviation(300_000, 3_000_000, "rows", warn=0.30, block=0.50)
    assert not r.passed and r.severity == dg.AssetCheckSeverity.ERROR


def test_a_month_with_just_over_half_its_trips_missing_blocks():
    r = quality.judge_deviation(1_350_000, 3_000_000, "rows", warn=quality.ROWS_VS_HISTORY_WARN, block=quality.ROWS_VS_HISTORY_BLOCK)
    assert not r.passed and r.severity == dg.AssetCheckSeverity.ERROR  # -55%: the case a 60% limit would have let through


def test_a_moderately_small_month_only_warns():
    r = quality.judge_deviation(1_800_000, 3_000_000, "rows", warn=0.30, block=0.50)
    assert not r.passed and r.severity == dg.AssetCheckSeverity.WARN


def test_the_sensor_is_deployed_stopped():
    assert trip_files_sensor.default_status == dg.DefaultSensorStatus.STOPPED


def _spec(check):
    return next(iter(check.check_specs))


def test_blocking_flags_match_the_decisions():
    for check in (quality.rows_match_footer, quality.times_present, quality.pickup_in_file_month,
                  quality.mart_filter_drop_share, quality.rows_vs_history):
        assert _spec(check).blocking, _spec(check).name
    for check in (quality.null_passengers, quality.duplicate_rows, quality.median_duration_shift):
        assert not _spec(check).blocking, _spec(check).name
    assert _spec(schema_contract).blocking  # contracts/platform.yaml lists it as blocking


# --- the sensor ---------------------------------------------------------------------------------------


def _tick(bucket, instance, cursor=None):
    ctx = dg.build_sensor_context(instance=instance, cursor=cursor, resources={"data_bucket": bucket})
    return trip_files_sensor(ctx)


def _bucket(**etags):
    return fake_bucket({m.replace("_", "-"): (e, trips_schema()) for m, e in etags.items()})


def test_first_tick_adds_every_month_and_requests_one_run_each():
    instance = dg.DagsterInstance.ephemeral()
    result = _tick(_bucket(**{"2024_01": "a", "2024_02": "b"}), instance)
    assert [r.partition_key for r in result.run_requests] == ["2024-01", "2024-02"]
    assert [r.run_key for r in result.run_requests] == ["2024-01:a", "2024-02:b"]
    assert result.dynamic_partitions_requests[0].partition_keys == ["2024-01", "2024-02"]
    assert json.loads(result.cursor) == {"2024-01": "a", "2024-02": "b"}


def test_nothing_new_means_no_run():
    instance = dg.DagsterInstance.ephemeral()
    result = _tick(_bucket(**{"2024_01": "a"}), instance, cursor=json.dumps({"2024-01": "a"}))
    assert isinstance(result, dg.SkipReason)


def test_a_changed_file_gets_a_new_run_and_no_new_partition():
    instance = dg.DagsterInstance.ephemeral()
    instance.add_dynamic_partitions("trip_months", ["2024-01"])
    result = _tick(_bucket(**{"2024_01": "a2"}), instance, cursor=json.dumps({"2024-01": "a"}))
    assert [r.run_key for r in result.run_requests] == ["2024-01:a2"]
    assert result.dynamic_partitions_requests == []


# --- blocking, end to end -----------------------------------------------------------------------------


def _run_month(columns, month="2024-01"):
    bucket = fake_bucket({month: ("etag", columns)})
    redshift = fake_redshift()
    defs = dg.Definitions(
        assets=[trip_files, raw_trips],
        asset_checks=[schema_contract],
        resources={"redshift": redshift, "data_bucket": bucket},
    )
    instance = dg.DagsterInstance.ephemeral()
    instance.add_dynamic_partitions("trip_months", [month])
    job = defs.resolve_implicit_global_asset_job_def()
    result = job.execute_in_process(partition_key=month, instance=instance, raise_on_error=False)
    loaded = {e.asset_key.to_user_string() for e in result.get_asset_materialization_events()}
    return loaded, list(LOG)


def test_a_file_that_matches_the_contract_is_loaded():
    loaded, sql = _run_month(trips_schema())
    assert "raw_trips" in loaded
    assert any(s.startswith("copy raw_trips_load") for s in sql)


@pytest.mark.parametrize("columns", [schema_2025(), schema_2023(), trips_schema()[:-1]])
def test_a_file_whose_schema_moved_is_never_copied(columns):
    loaded, sql = _run_month(columns)
    assert "trip_files" in loaded  # the footer is read and recorded...
    assert "raw_trips" not in loaded  # ...but the blocking check stops the load
    assert not any(s.startswith("copy ") for s in sql)
