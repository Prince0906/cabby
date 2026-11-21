"""Tests for S2 training: the window, the UNLOAD SQL, the gate, and train_model end to end.

End to end runs on the fakes (UNLOADs write synthetic trips to a local folder) and a local MLflow (SQLite):
no AWS. The real contracts/mlflow.yaml and features.yaml are used.
"""

import dagster as dg
import mlflow
import pytest
from fakes import LOG, LocalArtifacts, UnloadingRedshift
from mlflow import MlflowClient
from test_s2_features import good_values, table_columns

from labpipes.contracts import mlflow_contract
from labpipes.defs.training import (
    LATENCY_BUDGET_MS,
    MAE_MARGIN,
    alias_refusal,
    beats_champion,
    challenger,
    choose_window,
    distances_select,
    eval_report,
    judge,
    months_in,
    previous_months,
    train_model,
    trained_model,
    training_set,
    unload_sql,
)
from labpipes.resources import MlflowTracking

MONTHS = ["2024-01", "2024-02", "2024-03", "2024-04"]
NAME = mlflow_contract()["registered_model"]


# --- the window (record 009) --------------------------------------------------------------------------


def test_previous_months_cross_a_year():
    assert previous_months("2024-04", 3) == ["2024-01", "2024-02", "2024-03"]
    assert previous_months("2024-02", 3) == ["2023-11", "2023-12", "2024-01"]
    assert months_in("2023-11..2024-01") == ["2023-11", "2023-12", "2024-01"]


def test_the_default_window_is_three_months_before_the_latest():
    assert choose_window(MONTHS) == (["2024-01", "2024-02", "2024-03"], "2024-04")


def test_a_named_holdout_month():
    assert choose_window(MONTHS, "2024-03", 2) == (["2024-01", "2024-02"], "2024-03")


def test_a_gap_in_the_window_fails_instead_of_training_on_less():
    with pytest.raises(ValueError, match="2024-02"):
        choose_window(["2024-01", "2024-03", "2024-04"])
    with pytest.raises(ValueError, match="2023-12"):
        choose_window(MONTHS, "2024-02")  # only one month before it


def test_a_bad_month_never_reaches_sql():
    with pytest.raises(ValueError):
        choose_window(MONTHS, "2024-04'; drop table x; --")


# --- UNLOAD -------------------------------------------------------------------------------------------


def test_unload_doubles_the_quotes_of_the_select():
    sql = unload_sql("select 1 from t where trip_month in ('2024-01')", "s3://b/training/r/train/")
    assert sql.startswith("unload ('select 1 from t where trip_month in (''2024-01'')') to 's3://b/training/r/train/'")
    assert "iam_role default" in sql and "format as parquet" in sql


def test_distances_come_from_the_window_only():
    sql = distances_select(["2024-01", "2024-02", "2024-03"])
    assert "median(trip_distance)" in sql and "'2024-03'" in sql and "'2024-04'" not in sql


# --- the gate (record 008) --------------------------------------------------------------------------------


def report(new_mae=3.70, ref_mae=3.956, slices=None, ref_slices=None, p95=5.0, champion=None):
    base = {"airport": 6.72, "manhattan": 3.34, "outer": 9.19, "night": 2.87}
    new = {"mae_min": new_mae, "holdout_rows": 1000.0, "p95_latency_ms": p95, **{f"mae_min.{k}": v for k, v in (slices or base).items()}}
    ref = {"mae_min": ref_mae, "holdout_rows": 1000.0, "p95_latency_ms": 1.0, **{f"mae_min.{k}": v for k, v in (ref_slices or base).items()}}
    return {"version": "2", "reference": "champion v1" if champion else "v1 baseline", "champion_version": champion,
            "reference_saw_holdout": False, "new": new, "reference_metrics": ref}


def test_a_clearly_better_model_passes():
    passed, lines = judge(report())
    assert passed and all(line.startswith("PASS") for line in lines)


def test_better_by_less_than_the_margin_fails():
    assert not judge(report(new_mae=3.956 * (1 - MAE_MARGIN / 2)))[0]
    assert judge(report(new_mae=3.956 * (1 - MAE_MARGIN)))[0]  # exactly at the margin passes


def test_an_airport_regression_blocks_even_when_overall_mae_improves():
    passed, lines = judge(report(slices={"airport": 7.2, "manhattan": 3.0, "outer": 9.0, "night": 2.8}))
    assert not passed and any(line.startswith("FAIL airport") for line in lines)


def test_outer_and_night_get_a_wider_tolerance():
    ref = {"airport": 6.72, "manhattan": 3.34, "outer": 9.19, "night": 2.87}
    assert judge(report(slices={**ref, "outer": 9.19 * 1.04}, ref_slices=ref))[0]
    assert not judge(report(slices={**ref, "manhattan": 3.34 * 1.04}, ref_slices=ref))[0]


def test_over_the_latency_budget_fails():
    passed, lines = judge(report(p95=LATENCY_BUDGET_MS + 1))
    assert not passed and any(line.startswith("FAIL p95") for line in lines)


def test_different_holdouts_fail():
    r = report()
    r["reference_metrics"]["holdout_rows"] = 999.0
    assert not judge(r)[0]


def test_challenger_refuses_a_failing_report_or_a_stale_one():
    assert alias_refusal(report(), {}) is None
    assert "fails the gate" in alias_refusal(report(new_mae=4.0), {})
    assert "champion is v3 now" in alias_refusal(report(), {"champion": "3"})  # compared with v1, champion moved
    assert alias_refusal(report(champion="3"), {"champion": "3"}) is None


def test_the_gate_blocks():
    assert next(iter(beats_champion.check_specs)).blocking  # contracts/mlflow.yaml: blocking


def test_the_job_holds_exactly_the_training_assets():
    keys = {k.to_user_string() for k in train_model.selection.resolve([training_set, trained_model, eval_report, challenger])}
    assert keys == {"training_set", "trained_model", "eval_report", "challenger"}


# --- train_model end to end ------------------------------------------------------------------------------


def aliases(client) -> dict:
    return {k: str(v) for k, v in client.get_registered_model(NAME).aliases.items()}  # MLflow returns ints


class ContractRedshift(UnloadingRedshift):
    """UnloadingRedshift that also answers the feature-contract queries training_set re-runs."""

    columns: list = []

    def query(self, sql: str) -> list[dict]:
        if "from svv_columns" in sql:
            LOG.append(sql)
            return self.columns
        if "as nulls_trip_month" in sql:
            LOG.append(sql)
            return [good_values(4000)]
        return super().query(sql)


@pytest.fixture
def lab(tmp_path, monkeypatch):
    monkeypatch.setenv("GIT_SHA", "abc1234")
    uri = f"sqlite:///{tmp_path}/mlflow.db"
    mlflow.set_tracking_uri(uri)
    MlflowClient(uri).create_experiment(mlflow_contract()["experiment"], artifact_location=str(tmp_path / "mlartifacts"))
    root = tmp_path / "s3"
    root.mkdir()
    LOG.clear()

    def run(columns=None):
        resources = {
            "redshift": ContractRedshift(cluster_id="lab", database="lab", db_user="labadmin", root=str(root),
                                         columns=columns if columns is not None else table_columns()),
            "artifacts": LocalArtifacts(bucket="art", root=str(root)),
            "tracking": MlflowTracking(tracking_uri=uri),
        }
        defs = dg.Definitions(assets=[training_set, trained_model, eval_report, challenger], asset_checks=[beats_champion],
                              jobs=[train_model], resources=resources)
        return defs.resolve_job_def("train_model").execute_in_process(instance=dg.DagsterInstance.ephemeral(), raise_on_error=False,
                                                                  run_config={"ops": {"trained_model": {"config": {"num_rounds": 60}}}})

    return run, MlflowClient(uri)


def test_first_run_registers_a_tagged_version_and_sets_challenger(lab):
    run, client = lab
    result = run()
    assert result.success
    assert next(e for e in result.get_asset_check_evaluations() if e.check_name == "beats_champion").passed

    mv = client.get_model_version(NAME, "1")
    assert set(mlflow_contract()["version_tags"]) <= set(mv.tags)
    assert mv.tags["training_window"] == "2024-01..2024-03" and mv.tags["holdout_month"] == "2024-04"
    assert mv.tags["git_sha"] == "abc1234" and mv.tags["features_version"] == "1"
    assert float(mv.tags["mae_min"]) < float(mv.tags["champion_mae_min"])  # v2 beat v1 on the synthetic trips

    metrics = client.get_run(mv.run_id).data.metrics
    assert set(mlflow_contract()["metrics"]) <= set(metrics)

    assert aliases(client) == {"challenger": "1"}  # never champion

    unloads = [s for s in LOG if s.startswith("unload")]
    assert len(unloads) == 3 and not any("''2024-04''" in s for s in unloads if "median(trip_distance)" in s)


def test_against_a_champion_that_is_no_worse_the_gate_blocks_and_challenger_stays(lab):
    run, client = lab
    assert run().success
    client.set_registered_model_alias(NAME, "champion", "1")  # what S3's approval workflow would do

    result = run()  # same data and params: v2 equals the champion, so it is not 1% better
    gate = next(e for e in result.get_asset_check_evaluations() if e.check_name == "beats_champion")
    assert not gate.passed and gate.severity == dg.AssetCheckSeverity.ERROR
    assert "champion v1" in gate.description
    assert client.get_model_version(NAME, "2").tags["compared_with"] == "champion v1"
    assert aliases(client) == {"champion": "1", "challenger": "1"}  # not moved to v2
    assert not any(m.asset_key.to_user_string() == "challenger" for m in result.asset_materializations_for_node("challenger"))


def test_a_leaky_feature_table_stops_training_before_any_unload(lab):
    run, client = lab
    result = run(columns=table_columns(extra=[("trip_distance", "double precision")]))
    assert not result.success
    assert not any(s.startswith("unload") for s in LOG)
    assert client.search_registered_models() == []
