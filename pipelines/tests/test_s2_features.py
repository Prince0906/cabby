"""Tests for the S2 feature guard: feature_contract (blocking) and features_keep_share (advisory).

The contract is the real contracts/features.yaml. The Redshift answers are scripted (no AWS).
"""

import random

import dagster as dg
import pytest
from fakes import LOG, FakeRedshift

from labpipes.contracts import features_contract
from labpipes.defs.feature_check import (
    column_problems,
    expected_columns,
    feature_contract,
    features_keep_share,
    judge_keep_share,
    value_problems,
    value_sql,
)
from labpipes.defs.features import trip_features
from labpipes.defs.sensor import ingest_month

CONTRACT = features_contract()


def table_columns(extra=(), drop=(), retype=None):
    """What svv_columns would report for a table that follows the contract, with changes."""
    rows = []
    for name, (type_, length) in expected_columns(CONTRACT).items():
        if name in drop:
            continue
        type_ = (retype or {}).get(name, type_)
        rows.append({"column_name": name, "data_type": type_, "character_maximum_length": length})
    rows += [{"column_name": n, "data_type": t, "character_maximum_length": None} for n, t in extra]
    return rows


def good_values(rows=1000):
    row = {"rows": rows}
    for col in [*CONTRACT["keys"], *CONTRACT["inputs"], CONTRACT["label"]]:
        row[f"nulls_{col['name']}"] = 0
    for col in [*CONTRACT["inputs"], CONTRACT["label"]]:
        row[f"outside_{col['name']}"] = 0
    return row


# --- columns: an allowlist first, forbidden names as a backup ------------------------------------------


def test_a_table_that_follows_the_contract_has_no_problems():
    assert column_problems(CONTRACT, table_columns()) == []


def test_column_order_does_not_matter():
    shuffled = table_columns()
    random.Random(1).shuffle(shuffled)
    assert column_problems(CONTRACT, shuffled) == []


def test_a_forbidden_column_is_reported_as_leaky():
    problems = column_problems(CONTRACT, table_columns(extra=[("trip_distance", "double precision")]))
    assert any("LEAKY" in p and "trip_distance" in p for p in problems)


def test_a_renamed_leaky_column_is_still_stopped_by_the_allowlist():
    problems = column_problems(CONTRACT, table_columns(extra=[("dist", "double precision")]))
    assert problems and any("unexpected column(s)" in p and "dist" in p for p in problems)
    assert not any("LEAKY" in p for p in problems)  # the name list alone would have missed it


def test_every_forbidden_name_in_the_contract_is_caught():
    for name in CONTRACT["forbidden"]:
        assert any("LEAKY" in p for p in column_problems(CONTRACT, table_columns(extra=[(name.upper(), "double precision")]))), name


def test_a_missing_column_blocks():
    problems = column_problems(CONTRACT, table_columns(drop={"pickup_hour"}))
    assert any("missing column(s): pickup_hour" in p for p in problems)


def test_a_wrong_type_blocks():
    problems = column_problems(CONTRACT, table_columns(retype={"pickup_zone_id": "double precision"}))
    assert any("type of pickup_zone_id" in p for p in problems)


def test_a_wrong_length_of_the_month_key_blocks():
    cols = table_columns()
    next(c for c in cols if c["column_name"] == "trip_month")["character_maximum_length"] = 10
    assert any("type of trip_month" in p for p in column_problems(CONTRACT, cols))


# --- values ---------------------------------------------------------------------------------------------


def test_the_value_query_is_built_from_the_contract_and_validates_the_month():
    sql = value_sql(CONTRACT, "2024-01")
    assert "from staging.trip_features where trip_month = '2024-01'" in sql
    assert "duration_min <= 0 or duration_min > 180" in sql  # the label is above 0, up to 180
    assert "pickup_zone_id < 1 or pickup_zone_id > 263" in sql
    with pytest.raises(ValueError):
        value_sql(CONTRACT, "2024-01'; drop table x; --")


def test_good_values_have_no_problems():
    assert value_problems(CONTRACT, good_values()) == []


def test_an_empty_month_blocks():
    assert value_problems(CONTRACT, {"rows": 0}) == ["no rows for this month"]


def test_nulls_and_out_of_range_values_block():
    row = {**good_values(), "nulls_pickup_hour": 3, "outside_duration_min": 7, "outside_pickup_dow": 2}
    problems = value_problems(CONTRACT, row)
    assert any("3 null value(s) in pickup_hour" in p for p in problems)
    assert any("7 value(s) of duration_min outside 0 < duration_min <= 180" in p for p in problems)
    assert any("2 value(s) of pickup_dow outside 0 <= pickup_dow <= 6" in p for p in problems)


# --- keep share: advisory --------------------------------------------------------------------------------


def test_the_measured_keep_rates_pass():
    for kept, staged in ((2_931_052, 2_964_624), (2_976_570, 3_007_526), (3_544_940, 3_582_628), (3_476_146, 3_514_289)):
        assert judge_keep_share(kept, staged).passed


def test_a_big_drop_only_warns_it_never_blocks():
    r = judge_keep_share(900_000, 1_000_000)
    assert not r.passed and r.severity == dg.AssetCheckSeverity.WARN


def test_blocking_flags():
    assert next(iter(feature_contract.check_specs)).blocking  # contracts/mlflow.yaml: blocking
    assert not next(iter(features_keep_share.check_specs)).blocking


# --- the real Dagster check, end to end -------------------------------------------------------------------


class ScriptedRedshift(FakeRedshift):
    """Answers the two feature-contract queries from scripted data; everything else as FakeRedshift."""

    columns: list = []
    value_row: dict = {}

    def query(self, sql: str) -> list[dict]:
        if "from svv_columns" in sql:
            LOG.append(sql)
            return self.columns
        if "as nulls_" in sql or "nulls_trip_month" in sql:
            LOG.append(sql)
            return [self.value_row]
        return super().query(sql)


def run_checks(columns, value_row, month="2024-01"):
    LOG.clear()
    redshift = ScriptedRedshift(cluster_id="lab", database="lab", db_user="labadmin", columns=columns, value_row=value_row)
    defs = dg.Definitions(assets=[trip_features], asset_checks=[feature_contract, features_keep_share], resources={"redshift": redshift})
    instance = dg.DagsterInstance.ephemeral()
    instance.add_dynamic_partitions("trip_months", [month])
    result = defs.resolve_implicit_global_asset_job_def().execute_in_process(partition_key=month, instance=instance, raise_on_error=False)
    return {e.check_name: e for e in result.get_asset_check_evaluations()}


def test_the_check_passes_for_a_clean_table():
    evaluations = run_checks(table_columns(), good_values())
    assert evaluations["feature_contract"].passed


def test_the_check_fails_with_error_severity_for_a_leaky_table():
    evaluations = run_checks(table_columns(extra=[("trip_distance", "double precision")]), good_values())
    ev = evaluations["feature_contract"]
    assert not ev.passed and ev.severity == dg.AssetCheckSeverity.ERROR
    assert "LEAKY" in ev.description


def test_values_are_not_queried_when_the_columns_are_already_wrong():
    run_checks(table_columns(drop={"pickup_hour"}), good_values())
    assert not any("nulls_pickup_zone_id" in s for s in LOG)


# --- the ingest job now builds the features --------------------------------------------------------------


def test_the_ingest_job_includes_trip_features():
    assert "trip_features" in repr(ingest_month.selection)
