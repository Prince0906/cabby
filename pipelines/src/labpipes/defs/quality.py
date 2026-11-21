"""S1: data-quality checks on stg_trips (nulls, ranges, duplicates, row counts).

Decision points: which checks block downstream and which only warn, and every threshold. Base them on
what real months look like (scripts/verify/results/tlc-stats-2024-01-04.md is the instructor's
baseline; measure your own in Redshift) and on what gameday's corrupt month would look like.

Rule: BLOCK when the data is unusable, WARN when it is merely odd. In Dagster a blocking check stops the
assets downstream of stg_trips in the same run (zone_hour_stats) only when it fails with severity ERROR;
a WARN result is recorded and the run goes on. Checks with `blocking=False` can only warn.

Thresholds come from loading 2024-01..04 into Redshift and measuring each month (shares are of the
month's rows):

  measure                                   observed (4 months)        warn      block
  rows loaded vs the file's footer          equal                      -         any difference
  null pickup or dropoff time               0%                         -         > 0.01%
  pickup date outside the file's month      <= 0.0006%                 -         > 0.01%
  dropped by the mart filter (see below)    0.46% to 0.50%             > 1%      > 3%
  rows vs median of the OTHER months        -2% to +19%                > 30%     > 50%
  null passenger_count                      4.7% to 11.9%              > 25%     -
  exact duplicate rows                      ~0 (1 row in 02)           > 0.1%    -
  median duration vs the other months       about +3% a month          > 15%     -

"Other months" means every month already in stg_trips except this one, not just the earlier ones: the
sensor starts all months at once and Dagster doesn't guarantee which loads first, so "earlier" could
be empty by accident. The first month to load has nothing to compare with and passes; the footer,
null, date and drop-share checks don't need history, so they still guard it. A block at 50% still
leaves 2.5 times the largest swing seen (+19%) as headroom and catches a month with half its trips.

The mart filter (sql/zone_hour_stats.sql) drops trips with duration <= 0 or > 180 minutes and pickup
zones outside 1..263. It does so silently, so the drop share is what makes bad rows visible. The share
here adds the three groups, so a row in two groups counts twice: an upper bound, tiny in practice.
Not measured on the stg_trips column subset: the duplicate share (it warns only, until measured).
"""

import statistics

import dagster as dg

from ..partitions import check_month, trip_months
from ..resources import DataBucket, RedshiftData
from .staging import stg_trips

# Thresholds (fractions). Change them here, with the data that supports the change.
NULL_TIME_BLOCK = 0.0001
OFF_MONTH_BLOCK = 0.0001
MART_DROP_WARN, MART_DROP_BLOCK = 0.01, 0.03
ROWS_VS_HISTORY_WARN, ROWS_VS_HISTORY_BLOCK = 0.30, 0.50
NULL_PASSENGERS_WARN = 0.25
DUPLICATES_WARN = 0.001
MEDIAN_SHIFT_WARN = 0.15

ERROR, WARN = dg.AssetCheckSeverity.ERROR, dg.AssetCheckSeverity.WARN


def _pct(x: float) -> str:
    return f"{100 * x:.4f}%"


def judge_share(share: float, what: str, *, warn: float | None = None, block: float | None = None, **meta) -> dg.AssetCheckResult:
    """pass below `warn`, WARN above it, ERROR above `block`. Either limit may be absent."""
    if block is not None and share > block:
        passed, severity, limit = False, ERROR, block
    elif warn is not None and share > warn:
        passed, severity, limit = False, WARN, warn
    else:
        passed, severity, limit = True, WARN, None
    note = f"{what}: {_pct(share)}" + ("" if limit is None else f" (limit {_pct(limit)})")
    return dg.AssetCheckResult(
        passed=passed, severity=severity, description=note, metadata={"share": share, "warn_at": warn, "block_at": block, **meta}
    )


def judge_deviation(
    value: float, baseline: float | None, what: str, *, warn: float, block: float | None = None
) -> dg.AssetCheckResult:
    """`value` against the baseline of the other months. No baseline (the first month loaded) passes."""
    if not baseline:
        return dg.AssetCheckResult(passed=True, description=f"{what}: no other month loaded to compare with", metadata={"value": value})
    deviation = value / baseline - 1
    result = judge_share(abs(deviation), f"{what} vs the other months ({value:,.2f} against {baseline:,.2f}; {deviation:+.1%})",
                         warn=warn, block=block)
    return dg.AssetCheckResult(
        passed=result.passed,
        severity=result.severity,
        description=result.description,
        metadata={"value": value, "baseline": baseline, "deviation": deviation, "warn_at": warn, "block_at": block},
    )


def _scalar_row(redshift: RedshiftData, sql: str) -> dict:
    return redshift.query(sql)[0]


def _per_month(redshift: RedshiftData, sql: str, column: str) -> dict[str, float]:
    return {r["trip_month"]: float(r[column]) for r in redshift.query(sql)}


def baseline_of_others(per_month: dict[str, float], month: str) -> float | None:
    """Median over every OTHER month that is loaded, or None when this is the only one."""
    others = [v for m, v in per_month.items() if m != month]
    return statistics.median(others) if others else None


@dg.asset_check(
    asset=stg_trips,
    partitions_def=trip_months,
    blocking=True,
    description="Rows loaded for the month equal the Parquet footer's row count. Catches partial and doubled loads.",
)
def rows_match_footer(context: dg.AssetCheckExecutionContext, redshift: RedshiftData, data_bucket: DataBucket) -> dg.AssetCheckResult:
    month = check_month(context.partition_key)
    loaded = int(redshift.scalar(f"select count(*) from staging.stg_trips where trip_month = '{month}'"))
    expected = data_bucket.footer(month).num_rows
    return dg.AssetCheckResult(
        passed=loaded == expected,
        severity=ERROR,
        description=f"{loaded:,} rows loaded, {expected:,} in the file's footer",
        metadata={"loaded": loaded, "footer": expected},
    )


@dg.asset_check(
    asset=stg_trips,
    partitions_def=trip_months,
    blocking=True,
    description="Pickup and dropoff times are present for (nearly) every trip.",
)
def times_present(context: dg.AssetCheckExecutionContext, redshift: RedshiftData) -> dg.AssetCheckResult:
    month = check_month(context.partition_key)
    r = _scalar_row(
        redshift,
        "select count(*) as rows, sum(case when pickup_at is null or dropoff_at is null then 1 else 0 end) as null_times "
        f"from staging.stg_trips where trip_month = '{month}'",
    )
    rows = int(r["rows"]) or 1
    return judge_share(int(r["null_times"] or 0) / rows, "null pickup or dropoff time", block=NULL_TIME_BLOCK, rows=rows)


@dg.asset_check(
    asset=stg_trips,
    partitions_def=trip_months,
    blocking=True,
    description="Pickup dates fall inside the file's month (a handful of stray dates, like 2002, are normal).",
)
def pickup_in_file_month(context: dg.AssetCheckExecutionContext, redshift: RedshiftData) -> dg.AssetCheckResult:
    month = check_month(context.partition_key)
    r = _scalar_row(
        redshift,
        "select count(*) as rows, "
        "sum(case when pickup_at is not null and to_char(pickup_at, 'YYYY-MM') <> trip_month then 1 else 0 end) as off_month "
        f"from staging.stg_trips where trip_month = '{month}'",
    )
    rows = int(r["rows"]) or 1
    return judge_share(int(r["off_month"] or 0) / rows, "pickup outside the file's month", block=OFF_MONTH_BLOCK, rows=rows)


_STATS = (
    "select count(*) as rows, "
    "sum(case when duration_min <= 0 then 1 else 0 end) as nonpositive_duration, "
    "sum(case when duration_min > 180 then 1 else 0 end) as over_3h, "
    "sum(case when passenger_count is null then 1 else 0 end) as null_passengers, "
    "sum(case when pickup_zone_id is null or pickup_zone_id not between 1 and 263 then 1 else 0 end) as unknown_zone "
    "from staging.stg_trips where trip_month = '{month}'"
)


@dg.asset_check(
    asset=stg_trips,
    partitions_def=trip_months,
    blocking=True,
    description="Share of the month that zone_hour_stats would silently drop (duration <= 0 or > 180 min, unknown pickup zone).",
)
def mart_filter_drop_share(context: dg.AssetCheckExecutionContext, redshift: RedshiftData) -> dg.AssetCheckResult:
    month = check_month(context.partition_key)
    r = _scalar_row(redshift, _STATS.format(month=month))
    rows = int(r["rows"]) or 1
    parts = {k: int(r[k] or 0) for k in ("nonpositive_duration", "over_3h", "unknown_zone")}
    return judge_share(sum(parts.values()) / rows, "dropped by the mart filter", warn=MART_DROP_WARN, block=MART_DROP_BLOCK,
                       rows=rows, **parts)


@dg.asset_check(
    asset=stg_trips,
    partitions_def=trip_months,
    blocking=True,
    description="The month's row count is within a wide band around the median of the other loaded months.",
)
def rows_vs_history(context: dg.AssetCheckExecutionContext, redshift: RedshiftData) -> dg.AssetCheckResult:
    month = check_month(context.partition_key)
    counts = _per_month(redshift, "select trip_month, count(*) as n from staging.stg_trips group by trip_month order by trip_month", "n")
    return judge_deviation(
        counts.get(month, 0.0), baseline_of_others(counts, month), "row count", warn=ROWS_VS_HISTORY_WARN, block=ROWS_VS_HISTORY_BLOCK
    )


@dg.asset_check(
    asset=stg_trips,
    partitions_def=trip_months,
    blocking=False,
    description="Missing passenger counts are common and vary by month (5% to 12% observed); only a jump is odd.",
)
def null_passengers(context: dg.AssetCheckExecutionContext, redshift: RedshiftData) -> dg.AssetCheckResult:
    month = check_month(context.partition_key)
    r = _scalar_row(redshift, _STATS.format(month=month))
    rows = int(r["rows"]) or 1
    return judge_share(int(r["null_passengers"] or 0) / rows, "null passenger_count", warn=NULL_PASSENGERS_WARN, rows=rows)


@dg.asset_check(
    asset=stg_trips,
    partitions_def=trip_months,
    blocking=False,
    description="Exact duplicate trips (same times, zones, passengers, distance, rate, payment and amounts).",
)
def duplicate_rows(context: dg.AssetCheckExecutionContext, redshift: RedshiftData) -> dg.AssetCheckResult:
    month = check_month(context.partition_key)
    r = _scalar_row(
        redshift,
        f"select (select count(*) from staging.stg_trips where trip_month = '{month}') as rows, coalesce(sum(c - 1), 0) as extra "
        "from (select count(*) as c from staging.stg_trips "
        f"where trip_month = '{month}' "
        "group by vendor_id, pickup_at, dropoff_at, pickup_zone_id, dropoff_zone_id, passenger_count, trip_distance, "
        "ratecode_id, payment_type, fare_amount, total_amount having count(*) > 1)",
    )
    rows = int(r["rows"]) or 1
    return judge_share(int(r["extra"] or 0) / rows, "exact duplicate rows", warn=DUPLICATES_WARN, rows=rows)


@dg.asset_check(
    asset=stg_trips,
    partitions_def=trip_months,
    blocking=False,
    description="Median trip duration is close to the median of the other months' medians (it drifts about 3% a month).",
)
def median_duration_shift(context: dg.AssetCheckExecutionContext, redshift: RedshiftData) -> dg.AssetCheckResult:
    month = check_month(context.partition_key)
    medians = _per_month(
        redshift,
        "select trip_month, median(duration_min) as med from staging.stg_trips "
        "where duration_min > 0 and duration_min <= 180 group by trip_month order by trip_month",
        "med",
    )
    return judge_deviation(medians.get(month, 0.0), baseline_of_others(medians, month), "median duration (min)", warn=MEDIAN_SHIFT_WARN)
