"""S1: the raw_trips asset.

One partition per month (partitions.trip_months), depending on trip_files. COPY the month's file into
Redshift with the LabRole attached to the cluster (`iam_role default`), so that re-running a month never
duplicates its rows. The landing columns come from contracts/trips.schema.yaml
(contracts.redshift_columns). Decision points: COPY vs Spectrum; dist and sort keys.

What it does, and why (sql/raw_trips.sql has the statements):
- COPY, not Spectrum: the mart is rebuilt from every month on every run, so a local table is cheaper
  and faster than scanning S3 each time, and the given stg_trips reads a real ingest.raw_trips table.
- The Parquet file has no source_file column, so it is COPY'd into a temp table and inserted with the
  file's name. COPY maps columns by position: the order guarantee comes from the blocking
  schema_contract check on trip_files, which stops the run before this asset starts.
- Idempotent: delete the file's rows, COPY, insert, all in one transaction. A repeated run (a sensor
  firing twice, a manual re-run) gives the same rows once, and a failed run changes nothing.
- Layout: diststyle AUTO and a sort key on the pickup time (one small node, about four months of data).
"""

import dagster as dg

from ..contracts import redshift_columns, trips_schema
from ..partitions import source_file, trip_months
from ..resources import DataBucket, RedshiftData
from ..sql import statements


def load_statements(month: str, uri: str) -> list[str]:
    """The statements that (re)load one month, in order. Run them as one transaction."""
    return statements("raw_trips", month=month, file=source_file(month), uri=uri, columns=redshift_columns(trips_schema()))


@dg.asset(partitions_def=trip_months, deps=["trip_files"], group_name="ingest", kinds={"s3", "redshift"}, pool="redshift")
def raw_trips(context: dg.AssetExecutionContext, redshift: RedshiftData, data_bucket: DataBucket) -> dg.MaterializeResult:
    month = context.partition_key
    file = source_file(month)
    redshift.execute(*load_statements(month, data_bucket.trip_uri(month)))
    rows = redshift.scalar(f"select count(*) from ingest.raw_trips where source_file = '{file}'")
    return dg.MaterializeResult(metadata={"rows": dg.MetadataValue.int(int(rows)), "source_file": file})
