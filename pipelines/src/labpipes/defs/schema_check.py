"""S1: the schema_contract check on trip_files.

Compare the landed file's Parquet footer (the DataBucket resource reads it) with
contracts/trips.schema.yaml, before raw_trips copies anything. contracts/platform.yaml says this check
is blocking. Patterns: pipelines/reference/patterns.py. Decision point: what counts as a mismatch?

Our answer: ANY structural difference blocks. Parquet COPY maps columns by position, so
- a missing column, or an extra one (even at the end), changes the column count and COPY would fail late;
- a reordered column would load without an error but put values in the wrong columns;
- a changed type would be cast, truncated or rejected by COPY depending on the pair.
Names compare case-insensitively (TLC renamed airport_fee to Airport_fee; positional COPY never sees
names). A harmless-looking change is still a decision: evolving contracts/trips.schema.yaml is
reviewed and recorded, not tolerated by this check.
"""

import dagster as dg

from ..contracts import Column, trips_schema
from ..partitions import trip_months
from ..resources import DataBucket
from .trip_files import trip_files


def schema_mismatches(expected: list[Column], actual: list[Column]) -> list[str]:
    """Every way `actual` differs from `expected`, as readable lines. Empty means the file matches."""
    want = [(c.name.lower(), c.type) for c in expected]
    got = [(c.name.lower(), c.type) for c in actual]
    want_names, got_names = [n for n, _ in want], [n for n, _ in got]
    problems = []
    missing = [n for n in want_names if n not in got_names]
    extra = [n for n in got_names if n not in want_names]
    if missing:
        problems.append(f"missing column(s): {', '.join(missing)}")
    if extra:
        problems.append(f"unexpected column(s): {', '.join(extra)}")
    if not missing and not extra and want_names != got_names:
        problems.append(f"columns reordered: expected {', '.join(want_names)}; found {', '.join(got_names)}")
    want_types = dict(want)
    for name, type_ in got:
        if name in want_types and want_types[name] != type_:
            problems.append(f"type of {name}: expected {want_types[name]}, found {type_}")
    return problems


@dg.asset_check(
    asset=trip_files,
    partitions_def=trip_months,
    blocking=True,
    description="The file's Parquet footer matches contracts/trips.schema.yaml: same columns, order and types.",
)
def schema_contract(context: dg.AssetCheckExecutionContext, data_bucket: DataBucket) -> dg.AssetCheckResult:
    month = context.partition_key
    footer = data_bucket.footer(month)
    problems = schema_mismatches(trips_schema(), footer.columns)
    return dg.AssetCheckResult(
        passed=not problems,
        severity=dg.AssetCheckSeverity.ERROR,
        description="matches the contract" if not problems else "; ".join(problems),
        metadata={
            "num_columns": dg.MetadataValue.int(len(footer.columns)),
            "expected_columns": dg.MetadataValue.int(len(trips_schema())),
            "mismatches": dg.MetadataValue.json(problems),
        },
    )
