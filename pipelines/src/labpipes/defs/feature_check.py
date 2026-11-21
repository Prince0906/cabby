"""S2: the feature_contract check on trip_features.

contracts/features.yaml lists the model's inputs, label and the post-trip columns that must never be
inputs; contracts/mlflow.yaml says this check blocks. Decide what "matches" means: column names,
types, value ranges. Gameday E1 will test it.

Our answer, strongest rule first:
1. EXACT COLUMNS (an allowlist): the table has the contract's keys, inputs and label, and nothing else.
   This is the real defence against leakage: a leaky column is stopped whatever it is called. A name
   list alone is dodged by a rename (trip_distance -> dist); an allowlist is not.
2. FORBIDDEN NAMES (a backup): a column named in the contract's `forbidden` list is reported separately
   as "leaky", so the failure message says what went wrong, not just "unexpected column".
3. TYPES: each column has the contract's type (a float where an integer zone id belongs is wrong).
4. VALUES, for the month being checked: at least one row, no nulls, every input and the label inside
   the contract's range (the label must be above 0 and at most 180 minutes).
Column ORDER is not checked: every consumer reads these columns by name, unlike the positional Parquet
COPY of S1.

Limit, stated honestly: this guards the table. A change that puts leaked values into an allowed
column (say, a distance written into pickup_hour) passes the names and types and can only be caught by
the ranges and by reading the SQL, which is what a code review is for.

A second, advisory check, features_keep_share, watches how many of the month's trips the feature table
keeps. It drops trips with a bad dropoff zone as well as the mart's filter (about 1.0% to 1.1% of each
month measured in 2024-01..04, against 0.46% to 0.50% for the mart). It warns, and never blocks.
"""

import re

import dagster as dg

from ..contracts import features_contract
from ..partitions import check_month, trip_months
from ..resources import RedshiftData
from .features import trip_features

ERROR, WARN = dg.AssetCheckSeverity.ERROR, dg.AssetCheckSeverity.WARN

IDENTIFIER = re.compile(r"[a-z_][a-z0-9_]*")

# features_keep_share: warn when more than this share of a month's staged trips is not kept.
KEEP_WARN_DROP = 0.025  # measured: 1.03% to 1.13%
KEEP_BLOCK_DROP = 0.10  # not used to block: reported in the message only (see the module docstring)


def _ident(name: str) -> str:
    """Contract names end up inside SQL text, so they must look like plain identifiers."""
    if not IDENTIFIER.fullmatch(name):
        raise ValueError(f"not a plain column name: {name!r}")
    return name


def expected_columns(contract: dict) -> dict[str, tuple[str, int | None]]:
    """name -> (Redshift data_type, character length) for the keys, inputs and label, from features.yaml."""
    out = {}
    for col in [*contract["keys"], *contract["inputs"], contract["label"]]:
        m = re.fullmatch(r"char\((\d+)\)", col["type"])
        out[_ident(col["name"])] = ("character", int(m[1])) if m else (col["type"], None)
    return out


def column_problems(contract: dict, actual: list[dict]) -> list[str]:
    """Every way the table's columns differ from the contract, as readable lines. Empty means it matches."""
    want = expected_columns(contract)
    forbidden = {f.lower() for f in contract["forbidden"]}
    got = {a["column_name"].lower(): (a["data_type"], int(a["character_maximum_length"]) if a.get("character_maximum_length") else None)
           for a in actual}
    problems = []
    leaky = sorted(set(got) & forbidden)
    if leaky:
        problems.append(f"LEAKY column(s), known only after the trip: {', '.join(leaky)}")
    extra = sorted(set(got) - set(want) - forbidden)
    if extra:
        problems.append(f"unexpected column(s) not in the contract: {', '.join(extra)}")
    missing = sorted(set(want) - set(got))
    if missing:
        problems.append(f"missing column(s): {', '.join(missing)}")
    for name in sorted(set(want) & set(got)):
        if got[name] != want[name]:
            problems.append(f"type of {name}: expected {want[name][0]}{want[name][1] or ''}, found {got[name][0]}{got[name][1] or ''}")
    return problems


def _range_terms(contract: dict) -> list[tuple[str, str, str]]:
    """(column, label, SQL condition that is TRUE for a value OUTSIDE its range) for every ranged column."""
    terms = []
    for col in [*contract["inputs"], contract["label"]]:
        lo, hi = col["range"]
        name = _ident(col["name"])
        low = f"{name} <= {lo}" if col.get("exclusive_min") else f"{name} < {lo}"
        terms.append((name, f"{lo} < {name} <= {hi}" if col.get("exclusive_min") else f"{lo} <= {name} <= {hi}", f"({low} or {name} > {hi})"))
    return terms


def value_sql(contract: dict, month: str) -> str:
    """One query: the month's row count, and per column the nulls and the values outside the range."""
    table = contract["table"]
    parts = ["count(*) as rows"]
    for col in [*contract["keys"], *contract["inputs"], contract["label"]]:
        name = _ident(col["name"])
        parts.append(f"sum(case when {name} is null then 1 else 0 end) as nulls_{name}")
    for name, _, outside in _range_terms(contract):
        parts.append(f"sum(case when {outside} then 1 else 0 end) as outside_{name}")
    return f"select {', '.join(parts)} from {table} where trip_month = '{check_month(month)}'"


def value_problems(contract: dict, row: dict) -> list[str]:
    """The value rules, judged on the single result row of value_sql."""
    rows = int(row["rows"])
    if rows == 0:
        return ["no rows for this month"]
    problems = []
    for col in [*contract["keys"], *contract["inputs"], contract["label"]]:
        n = int(row[f"nulls_{col['name']}"] or 0)
        if n:
            problems.append(f"{n:,} null value(s) in {col['name']}")
    for name, rule, _ in _range_terms(contract):
        n = int(row[f"outside_{name}"] or 0)
        if n:
            problems.append(f"{n:,} value(s) of {name} outside {rule}")
    return problems


@dg.asset_check(
    asset=trip_features,
    partitions_def=trip_months,
    blocking=True,
    description="The feature table has exactly the contract's columns and types, no post-trip column, and values in range.",
)
def feature_contract(context: dg.AssetCheckExecutionContext, redshift: RedshiftData) -> dg.AssetCheckResult:
    month = check_month(context.partition_key)
    contract = features_contract()
    schema, table = contract["table"].split(".")
    actual = redshift.query(
        "select column_name, data_type, character_maximum_length from svv_columns "
        f"where table_schema = '{_ident(schema)}' and table_name = '{_ident(table)}' order by ordinal_position"
    )
    problems = column_problems(contract, actual)
    if not problems:  # values only make sense once the columns are right
        problems = value_problems(contract, redshift.query(value_sql(contract, month))[0])
    return dg.AssetCheckResult(
        passed=not problems,
        severity=ERROR,
        description="matches contracts/features.yaml" if not problems else "; ".join(problems),
        metadata={"columns": dg.MetadataValue.json([a["column_name"] for a in actual]), "problems": dg.MetadataValue.json(problems)},
    )


def judge_keep_share(kept: int, staged: int) -> dg.AssetCheckResult:
    """Pass while the feature table keeps nearly every staged trip; WARN (never block) when it keeps noticeably fewer."""
    drop = 1 - kept / staged if staged else 1.0
    passed = drop <= KEEP_WARN_DROP
    return dg.AssetCheckResult(
        passed=passed,
        severity=WARN,
        description=f"{kept:,} of {staged:,} staged trips kept; {drop:.2%} dropped (warn above {KEEP_WARN_DROP:.1%}"
        f", a drop above {KEEP_BLOCK_DROP:.0%} would point at stg_trips)",
        metadata={"kept": kept, "staged": staged, "dropped_share": drop},
    )


@dg.asset_check(
    asset=trip_features,
    partitions_def=trip_months,
    blocking=False,
    description="How many of the month's staged trips the feature table keeps (about 98.9% to 99.0% measured).",
)
def features_keep_share(context: dg.AssetCheckExecutionContext, redshift: RedshiftData) -> dg.AssetCheckResult:
    month = check_month(context.partition_key)
    kept = int(redshift.scalar(f"select count(*) from staging.trip_features where trip_month = '{month}'"))
    staged = int(redshift.scalar(f"select count(*) from staging.stg_trips where trip_month = '{month}'"))
    return judge_keep_share(kept, staged)
