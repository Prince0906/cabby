"""S2: training_set, trained_model, eval_report, the challenger gate and the challenger asset.

- training_set: UNLOAD a window of staging.trip_features (and the holdout month) to the artifact bucket.
  The zone-pair distance lookup (contracts/features.yaml, est_distance_miles) comes from the window only.
- trained_model: fit tripmodel.LightGBMModel (the given v2), log it to MLflow as a pyfunc with
  tripmodel.model_signature() and the tripmodel package bundled, register a version of the model named
  in contracts/mlflow.yaml, and tag it as that contract says.
- eval_report: tripmodel.evaluate the new version and the champion (or the v1 baseline before there is
  one) on the same holdout; log the metrics.
- A blocking check that guards a `challenger` asset, which sets the alias. The pipeline never moves
  `champion`.
Decision points: what stays in the notebook (007); the promotion metric and margin (008); the training
window (009).

How the pieces fit:
- Window (record 009): the holdout is the latest month in staging.trip_features, unless the run's config
  names one; the window is the TRAIN_MONTHS months just before it, all present. Train on the past, test
  on the month after: that is what serving does with next month's trips.
- training_set re-checks the feature contract for every month it reads. feature_contract blocks only
  inside the ingest run, and the month's rows are already in the table by then, so a month that failed
  it would otherwise be trained on.
- Every UNLOAD goes under training/<dagster run id>/, so two runs never mix files. After the UNLOADs the
  row counts in S3 must equal the table's counts.
- eval_report loads the new version back from the registry (as serving will) and evaluates it, and the
  reference (the champion, or v1 fitted on the same window), on the same holdout rows.
- beats_champion (record 008) blocks the challenger step. The challenger asset judges the report again
  before it sets the alias, because the check only blocks steps of its own run: materializing
  `challenger` alone would otherwise reuse whatever eval_report was stored last.
"""

import os
from pathlib import Path

import dagster as dg
import mlflow
import pyarrow.dataset as pads

import tripmodel
from tripmodel import BaselineModel, LightGBMModel, evaluate, model_signature

from ..contracts import features_contract, mlflow_contract
from ..partitions import check_month
from ..resources import ArtifactBucket, MlflowTracking, RedshiftData
from .feature_check import months_problems
from .features import trip_features

# Record 009: three months train, the month after tests.
TRAIN_MONTHS = 3

# Record 008: what a new version must do on the holdout to become challenger.
MAE_MARGIN = 0.01  # overall MAE at least 1% below the reference's
SLICE_TOLERANCE = {"airport": 0.02, "manhattan": 0.02, "outer": 0.05, "night": 0.05}  # how much worse a slice may get
LATENCY_BUDGET_MS = 50.0  # p95 single-row predict; the app gives serving 300 ms in all

INPUT_COLUMNS = "pickup_zone_id, dropoff_zone_id, pickup_hour, pickup_dow, duration_min"


class TrainingWindow(dg.Config):
    holdout_month: str = ""  # YYYY-MM; empty means the latest month in staging.trip_features
    train_months: int = TRAIN_MONTHS


class TrainingParams(dg.Config):
    num_rounds: int = 300  # LightGBMModel's boosting rounds


# --- the window ---------------------------------------------------------------------------------------


def previous_months(month: str, n: int) -> list[str]:
    """The n months before `month`, oldest first: ('2024-04', 3) -> ['2024-01', '2024-02', '2024-03']."""
    y, m = map(int, check_month(month).split("-"))
    out = []
    for _ in range(n):
        y, m = (y - 1, 12) if m == 1 else (y, m - 1)
        out.append(f"{y:04d}-{m:02d}")
    return out[::-1]


def choose_window(available: list[str], holdout_month: str = "", train_months: int = TRAIN_MONTHS) -> tuple[list[str], str]:
    """(window, holdout). Every month must be in staging.trip_features: a gap would silently train on less."""
    if train_months < 1:
        raise ValueError("train_months must be at least 1")
    available = sorted(check_month(m) for m in available)
    if not available:
        raise ValueError("staging.trip_features has no months")
    holdout = check_month(holdout_month) if holdout_month else available[-1]
    window = previous_months(holdout, train_months)
    missing = [m for m in [*window, holdout] if m not in available]
    if missing:
        raise ValueError(f"window {window[0]}..{window[-1]} with holdout {holdout} needs months not in trip_features: {', '.join(missing)}")
    return window, holdout


def window_tag(window: list[str]) -> str:
    return f"{window[0]}..{window[-1]}"


def months_in(window: str) -> list[str]:
    """'2024-01..2024-03' -> ['2024-01', '2024-02', '2024-03'] (the training_window tag)."""
    first, last = (check_month(m) for m in window.split(".."))
    months = [last]
    while months[0] > first:
        months.insert(0, previous_months(months[0], 1)[0])
    return months


# --- UNLOAD ---------------------------------------------------------------------------------------------


def _in_list(months: list[str]) -> str:
    return ", ".join(f"'{check_month(m)}'" for m in months)


def unload_sql(select: str, uri: str) -> str:
    """UNLOAD a SELECT to a prefix as Parquet. The SELECT is a quoted string, so its quotes are doubled."""
    if "'" in uri:
        raise ValueError(f"bad S3 uri: {uri!r}")
    return f"unload ('{select.replace(chr(39), chr(39) * 2)}') to '{uri}' iam_role default format as parquet allowoverwrite"


def features_select(months: list[str]) -> str:
    return f"select {INPUT_COLUMNS} from staging.trip_features where trip_month in ({_in_list(months)})"


def distances_select(window: list[str]) -> str:
    """Median trip distance per zone pair, over the window's valid trips only. The trip's own distance is
    known only afterwards, so the model gets this estimate (features.yaml: est_distance_miles). Built from
    the window and never the holdout, or the evaluation would peek at the month it tests on."""
    return (
        "select pickup_zone_id, dropoff_zone_id, median(trip_distance) as est_distance_miles from staging.stg_trips "
        f"where trip_month in ({_in_list(window)}) and duration_min > 0 and duration_min <= 180 and trip_distance > 0 "
        "and pickup_zone_id between 1 and 263 and dropoff_zone_id between 1 and 263 group by 1, 2"
    )


def count_rows(artifacts: ArtifactBucket, prefix: str) -> int:
    """Rows under a prefix, from the Parquet footers only."""
    return pads.dataset(f"{artifacts.bucket}/{prefix}", filesystem=artifacts.filesystem(), format="parquet").count_rows()


@dg.asset(deps=[trip_features], group_name="model", kinds={"redshift", "s3"})
def training_set(context: dg.AssetExecutionContext, config: TrainingWindow, redshift: RedshiftData, artifacts: ArtifactBucket) -> dg.Output[dict]:
    available = [r["trip_month"] for r in redshift.query("select distinct trip_month from staging.trip_features order by 1")]
    window, holdout = choose_window(available, config.holdout_month, config.train_months)
    problems = months_problems(redshift, features_contract(), [*window, holdout])
    if problems:
        raise dg.Failure("trip_features does not match contracts/features.yaml, so nothing is trained: " + "; ".join(problems))

    prefix = f"training/{context.run.run_id}"
    for part, select in (("train", features_select(window)), ("holdout", features_select([holdout])), ("distances", distances_select(window))):
        redshift.execute(unload_sql(select, artifacts.uri(f"{prefix}/{part}/")))

    expected = redshift.query(
        f"select sum(case when trip_month in ({_in_list(window)}) then 1 else 0 end) as train, "
        f"sum(case when trip_month = '{holdout}' then 1 else 0 end) as holdout from staging.trip_features"
    )[0]
    rows = {part: count_rows(artifacts, f"{prefix}/{part}") for part in ("train", "holdout", "distances")}
    for part in ("train", "holdout"):
        if rows[part] != int(expected[part]):
            raise dg.Failure(f"{part}: UNLOAD wrote {rows[part]:,} rows, trip_features has {int(expected[part]):,} (did a month change mid-run?)")

    out = {"window": window, "holdout": holdout, "prefix": prefix, "train_rows": rows["train"], "holdout_rows": rows["holdout"],
           "distance_pairs": rows["distances"]}
    context.log.info(f"train {window_tag(window)} ({rows['train']:,} rows), holdout {holdout} ({rows['holdout']:,}), {rows['distances']:,} zone pairs")
    return dg.Output(out, metadata={"training_window": window_tag(window), "holdout_month": holdout, "train_rows": rows["train"],
                                    "holdout_rows": rows["holdout"], "distance_pairs": rows["distances"], "prefix": artifacts.uri(prefix)})


# --- train and register -------------------------------------------------------------------------------


@dg.asset(group_name="model", kinds={"lightgbm", "mlflow"})
def trained_model(context: dg.AssetExecutionContext, config: TrainingParams, training_set: dict, artifacts: ArtifactBucket,
                  tracking: MlflowTracking) -> dg.Output[dict]:
    prefix, window, holdout = training_set["prefix"], training_set["window"], training_set["holdout"]
    train = artifacts.read_parquet(f"{prefix}/train")
    distances = artifacts.read_parquet(f"{prefix}/distances")
    model = LightGBMModel().fit(train, distances, num_rounds=config.num_rounds)
    del train

    contract = mlflow_contract()
    name = contract["registered_model"]
    client = tracking.client()  # also points mlflow at the server and the experiment
    tags = {
        "training_window": window_tag(window),
        "holdout_month": holdout,
        "features_version": str(features_contract()["version"]),
        "git_sha": os.environ.get("GIT_SHA", "unknown"),
        "dagster_run_id": context.run.run_id,  # record 007: an alias with no matching run is a bypass
    }
    with mlflow.start_run(run_name=f"v2 {tags['training_window']} -> {holdout}", tags=tags) as run:
        mlflow.log_params({"model": "LightGBMModel", "num_rounds": config.num_rounds, "train_rows": training_set["train_rows"],
                           "distance_pairs": training_set["distance_pairs"], **{f"lgb.{k}": v for k, v in model.params.items()}})
        info = mlflow.pyfunc.log_model(name="model", python_model=model, signature=model_signature(),
                                       code_paths=[str(Path(tripmodel.__file__).parent)], registered_model_name=name)
    version = str(info.registered_model_version)
    for k, v in tags.items():
        client.set_model_version_tag(name, version, k, v)
    context.log.info(f"registered {name} v{version} from run {run.info.run_id}")
    return dg.Output({"version": version, "run_id": run.info.run_id, **tags},
                     metadata={"version": version, "mlflow_run_id": run.info.run_id, **tags})


# --- evaluate against what is serving ----------------------------------------------------------------


@dg.asset(group_name="model", kinds={"mlflow"})
def eval_report(context: dg.AssetExecutionContext, training_set: dict, trained_model: dict, redshift: RedshiftData,
                artifacts: ArtifactBucket, tracking: MlflowTracking) -> dg.Output[dict]:
    name = mlflow_contract()["registered_model"]
    client = tracking.client()
    holdout = artifacts.read_parquet(f"{training_set['prefix']}/holdout")
    boroughs = {int(r["location_id"]): r["borough"] for r in redshift.query("select location_id, borough from ingest.taxi_zones")}

    new = mlflow.pyfunc.load_model(f"models:/{name}/{trained_model['version']}")  # what serving would load
    new_metrics = evaluate(new.predict, holdout, boroughs)

    champion = client.get_registered_model(name).aliases.get("champion")
    if champion:
        ref = mlflow.pyfunc.load_model(f"models:/{name}/{champion}")
        ref_metrics = evaluate(ref.predict, holdout, boroughs)
        ref_window = client.get_model_version(name, str(champion)).tags.get("training_window", "")
        reference = f"champion v{champion}"
        # A champion that trained on the holdout month has an unfair edge; it makes the gate stricter, not looser.
        saw_holdout = bool(ref_window) and training_set["holdout"] in months_in(ref_window)
    else:
        train = artifacts.read_parquet(f"{training_set['prefix']}/train")
        baseline = BaselineModel().fit(train, artifacts.read_parquet(f"{training_set['prefix']}/distances"))
        del train
        ref_metrics = evaluate(lambda df: baseline.predict(None, df), holdout, boroughs)
        reference, saw_holdout = "v1 baseline (no champion yet)", False

    run_id = trained_model["run_id"]
    for k, v in new_metrics.items():
        client.log_metric(run_id, k, v)
    client.log_metric(run_id, "champion_mae_min", ref_metrics["mae_min"])
    for k, v in ref_metrics.items():
        if k.startswith("mae_min."):
            client.log_metric(run_id, f"champion_{k}", v)
    version = trained_model["version"]
    for k, v in (("mae_min", new_metrics["mae_min"]), ("p95_latency_ms", new_metrics["p95_latency_ms"]),
                 ("champion_mae_min", ref_metrics["mae_min"])):
        client.set_model_version_tag(name, version, k, f"{v:.4f}")
    client.set_model_version_tag(name, version, "compared_with", reference)

    report = {"version": version, "run_id": run_id, "training_window": trained_model["training_window"],
              "holdout_month": training_set["holdout"], "reference": reference, "champion_version": str(champion) if champion else None,
              "reference_saw_holdout": saw_holdout, "new": new_metrics, "reference_metrics": ref_metrics}
    client.log_dict(run_id, report, "eval_report.json")
    passed, lines = judge(report)
    context.log.info(f"v{version} vs {reference} on {training_set['holdout']}: " + "; ".join(lines))
    return dg.Output(report, metadata={"version": version, "reference": reference, "mae_min": new_metrics["mae_min"],
                                       "champion_mae_min": ref_metrics["mae_min"], "p95_latency_ms": new_metrics["p95_latency_ms"],
                                       "holdout_rows": int(new_metrics["holdout_rows"]), "gate_would_pass": passed})


# --- the gate ----------------------------------------------------------------------------------------------


def judge(report: dict) -> tuple[bool, list[str]]:
    """Record 008, on one eval_report: (passed, one readable line per rule)."""
    new, ref = report["new"], report["reference_metrics"]
    rules = []
    limit = ref["mae_min"] * (1 - MAE_MARGIN)
    rules.append((new["mae_min"] <= limit, f"MAE {new['mae_min']:.3f} vs {ref['mae_min']:.3f} (must be <= {limit:.3f}, {MAE_MARGIN:.0%} better)"))
    for s, tol in SLICE_TOLERANCE.items():
        k = f"mae_min.{s}"
        if k not in new or k not in ref:
            rules.append((True, f"{s}: no holdout rows, not judged"))
            continue
        cap = ref[k] * (1 + tol)
        rules.append((new[k] <= cap, f"{s} {new[k]:.3f} vs {ref[k]:.3f} (may be up to {cap:.3f}, {tol:.0%} worse)"))
    p95 = new["p95_latency_ms"]
    rules.append((p95 <= LATENCY_BUDGET_MS, f"p95 latency {p95:.1f} ms (budget {LATENCY_BUDGET_MS:.0f} ms)"))
    if int(new.get("holdout_rows", 0)) == 0 or new.get("holdout_rows") != ref.get("holdout_rows"):
        rules.append((False, "the two models were not evaluated on the same, non-empty holdout"))
    return all(ok for ok, _ in rules), [("PASS " if ok else "FAIL ") + line for ok, line in rules]


@dg.asset_check(asset=eval_report, blocking=True,
                description=f"MAE at least {MAE_MARGIN:.0%} below the champion's (or v1's), no slice much worse, p95 within {LATENCY_BUDGET_MS:.0f} ms.")
def beats_champion(eval_report: dict) -> dg.AssetCheckResult:
    passed, lines = judge(eval_report)
    return dg.AssetCheckResult(
        passed=passed,
        severity=dg.AssetCheckSeverity.ERROR,
        description=f"v{eval_report['version']} vs {eval_report['reference']}: " + "; ".join(lines),
        metadata={"version": eval_report["version"], "reference": eval_report["reference"], "rules": dg.MetadataValue.json(lines),
                  "reference_saw_holdout": eval_report["reference_saw_holdout"]},
    )


def alias_refusal(report: dict, aliases: dict) -> str | None:
    """Why `challenger` must not move to this report's version, or None. `aliases`: the registered model's."""
    passed, lines = judge(report)
    if not passed:
        return "the report fails the gate: " + "; ".join(line for line in lines if line.startswith("FAIL"))
    current = aliases.get("champion")
    if (str(current) if current else None) != report["champion_version"]:
        return f"champion is v{current} now, the report compared with {report['reference']}: run train_model again"
    if str(current) == report["version"]:
        return f"v{report['version']} is already champion"
    return None


@dg.asset(group_name="model", kinds={"mlflow"})
def challenger(context: dg.AssetExecutionContext, eval_report: dict, tracking: MlflowTracking) -> dg.MaterializeResult:
    """Point `challenger` at the version that passed. Never touches `champion` (S3's workflow moves it)."""
    name = mlflow_contract()["registered_model"]
    client = tracking.client()
    aliases = client.get_registered_model(name).aliases
    refusal = alias_refusal(eval_report, aliases)
    if refusal:
        raise dg.Failure(f"challenger not moved: {refusal}")
    previous = aliases.get("challenger")
    client.set_registered_model_alias(name, "challenger", eval_report["version"])
    context.log.info(f"challenger: v{previous or '-'} -> v{eval_report['version']}")
    return dg.MaterializeResult(metadata={"version": eval_report["version"], "previous": str(previous or ""), "reference": eval_report["reference"]})


train_model = dg.define_asset_job(
    "train_model",
    selection=dg.AssetSelection.assets(training_set, trained_model, eval_report, challenger),
    description="UNLOAD the window and holdout, train v2, register it, compare with the champion (or v1), and set challenger if it passes.",
)
