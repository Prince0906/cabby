"""S1: the S3 sensor (and the job it starts).

Watch trips/ in the data bucket (DataBucket.trip_files). When a month's file appears or changes, add
the month to the trip_months partitions and request a run that goes from trip_files to zone_hour_stats.
Patterns: pipelines/reference/patterns.py.

Design:
- ONE run per month covers trip_files, the schema check, raw_trips, stg_trips, the quality checks,
  zone_hour_stats and (S2) trip_features with its feature_contract check. Features come after the
  staging checks, so a month that fails a blocking check never reaches the model's training data. A blocking check only stops steps of its own run, so splitting the chain into several
  runs would make "blocking" block nothing.
- "Appears or changes": the cursor remembers each month's ETag. A new month has no entry; a re-uploaded
  month has a different ETag. The run_key is month:etag, so Dagster ignores a repeated request for the
  same file content, and a corrected file gets a run. (A month-only key would ignore the correction.)
  An ETag can also change when identical bytes are re-uploaded in different parts; the extra run is
  harmless because the load is idempotent.
- A run that fails (a blocking check, say) is NOT retried by the sensor: the cursor already has that
  ETag. Fix the data and upload again, or start the job by hand (scripts/dagster-run.sh ingest_month).
- The job is called ingest_month: scripts/dagster-run.sh and scripts/catch-up.sh use that name.
- The sensor is deployed STOPPED. It marks a month as seen the moment it requests its run, so a bug in
  the load SQL would fail every month and nothing would retry them. Prove one month by hand first
  (scripts/dagster-run.sh ingest_month --partition 2024-01 --wait), then turn the sensor on.
"""

import json

import dagster as dg

from ..partitions import trip_months
from ..resources import DataBucket

ingest_month = dg.define_asset_job(
    "ingest_month",
    selection=dg.AssetSelection.assets("taxi_zones", "trip_files", "raw_trips", "stg_trips", "zone_hour_stats", "trip_features"),
    description="One month, end to end: footer, schema check, COPY, staging, quality checks, the mart, then the model features (S2).",
)


@dg.sensor(name="trip_files_sensor", job=ingest_month, minimum_interval_seconds=60, default_status=dg.DefaultSensorStatus.STOPPED)
def trip_files_sensor(context: dg.SensorEvaluationContext, data_bucket: DataBucket):
    files = data_bucket.trip_files()  # oldest month first
    seen: dict[str, str] = json.loads(context.cursor or "{}")  # month -> ETag already requested
    changed = [f for f in files if seen.get(f.month) != f.etag]
    if not changed:
        return dg.SkipReason(f"{len(files)} trip file(s) under trips/, none new or changed")

    known = set(context.instance.get_dynamic_partitions(trip_months.name))
    new_months = [f.month for f in changed if f.month not in known]
    return dg.SensorResult(
        run_requests=[dg.RunRequest(run_key=f"{f.month}:{f.etag}", partition_key=f.month) for f in changed],
        dynamic_partitions_requests=[trip_months.build_add_request(new_months)] if new_months else [],
        cursor=json.dumps({**seen, **{f.month: f.etag for f in changed}}, sort_keys=True),
    )
