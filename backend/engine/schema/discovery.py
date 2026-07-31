"""
CUIN v2 - Source Schema Discovery + Profiling

Powers the banker-facing Fields step of the Settings UI: "show
available fields in parquet" (the literal ask), each annotated with
enough to make an informed blocking/matching decision without reading
any code -- fill rate, distinct-value count, a sample of common
values, a guessed semantic type, and a "would this explode?" verdict
computed with the EXACT same closed-form pair-counting math
engine.rules.precheck already uses for real blocking rules
(SUM(n*(n-1)/2) over group sizes), not a fuzzy cardinality heuristic --
so "BRANCH_CODE" reads as "would produce 3.1B pairs", a number a
banker can actually reason about.

Read-only: this profiles the raw source Parquet directly via DuckDB's
read_parquet() (fast, engine-agnostic analysis -- it doesn't touch
whichever engine will actually run the pipeline). It does not require
a completed run.
"""

from dataclasses import dataclass, field
from typing import Any, List, Optional

import duckdb

from engine.rules.precheck import (
    FanoutSeverity, DEFAULT_WARN_PAIRS, DEFAULT_BLOCK_PAIRS,
    DEFAULT_WARN_BLOCK_SIZE, DEFAULT_BLOCK_BLOCK_SIZE, _severity,
)
from engine.schema.semantic_types import guess_semantic_type, get_semantic_type

# Profiling reads every row of every column -- on the full 1.5M-row
# dataset this is a few seconds, not the tens of minutes a full
# pipeline run costs, since it's a handful of GROUP BYs over columns
# DuckDB reads directly off Parquet's columnar layout (no join, no
# blocking, no scoring). No sampling needed at this scale; if a much
# larger source ever made full profiling too slow, this is the single
# place a LIMIT/SAMPLE would go.


@dataclass
class BlockingVerdict:
    would_explode: bool
    severity: str
    n_distinct_values: int
    n_pairs_if_blocked: int
    largest_group: int
    message: str

    def to_dict(self) -> dict:
        return {
            "would_explode": self.would_explode,
            "severity": self.severity,
            "n_distinct_values": self.n_distinct_values,
            "n_pairs_if_blocked": self.n_pairs_if_blocked,
            "largest_group": self.largest_group,
            "message": self.message,
        }


@dataclass
class ColumnProfile:
    name: str
    parquet_type: str
    is_array: bool
    fill_rate: float
    distinct_count: int
    avg_length: Optional[float]
    top_values: List[dict]
    semantic_type: str
    semantic_label: str
    blocking_safe: bool
    default_comparators: List[str]
    blocking_verdict: BlockingVerdict
    good_for: List[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "parquet_type": self.parquet_type,
            "is_array": self.is_array,
            "fill_rate": self.fill_rate,
            "distinct_count": self.distinct_count,
            "avg_length": self.avg_length,
            "top_values": self.top_values,
            "semantic_type": self.semantic_type,
            "semantic_label": self.semantic_label,
            "blocking_safe": self.blocking_safe,
            "default_comparators": self.default_comparators,
            "blocking_verdict": self.blocking_verdict.to_dict(),
            "good_for": self.good_for,
        }


def _blocking_verdict(con, source_expr: str, is_array: bool,
                       warn_pairs: int, block_pairs: int) -> BlockingVerdict:
    """
    Treats this column's raw value as a hypothetical EXACT blocking
    key (one CUSTOMER_CODE per group, grouped by value) and computes
    the exact pair count that key would produce -- the same
    SUM(n*(n-1)/2) formula engine.rules.precheck.estimate_fanout uses
    for real rules, just against the raw column instead of a
    normalized/rule-compiled one. Array columns are unnested first
    (a customer contributes one row per element, matching how blocking
    rules actually treat multi-value fields).
    """
    if is_array:
        rows = con.execute(f"""
            SELECT v, COUNT(DISTINCT CUSTOMER_CODE) AS n
            FROM (SELECT CUSTOMER_CODE, UNNEST({source_expr}) AS v FROM raw) t
            WHERE v IS NOT NULL AND v != ''
            GROUP BY v
        """).fetchall()
    else:
        rows = con.execute(f"""
            SELECT {source_expr} AS v, COUNT(DISTINCT CUSTOMER_CODE) AS n
            FROM raw WHERE {source_expr} IS NOT NULL
            GROUP BY {source_expr}
        """).fetchall()

    multi = [(v, n) for v, n in rows if n >= 2]
    n_pairs = sum((n * (n - 1)) // 2 for _, n in multi)
    largest_group = max((n for _, n in rows), default=0)
    severity = _severity(
        n_pairs, largest_group, warn_pairs, block_pairs,
        DEFAULT_WARN_BLOCK_SIZE, DEFAULT_BLOCK_BLOCK_SIZE,
    )

    if severity == FanoutSeverity.SAFE:
        message = f"Safe to block on directly -- would produce {n_pairs:,} candidate pairs."
    elif severity == FanoutSeverity.WARN:
        message = f"Risky alone -- would produce {n_pairs:,} candidate pairs (largest group: {largest_group:,} records). Combine with another field or cap block size."
    else:
        message = f"Would explode -- {n_pairs:,} candidate pairs (largest group: {largest_group:,} records). Do not block on this field alone."

    return BlockingVerdict(
        would_explode=(severity == FanoutSeverity.BLOCK),
        severity=severity,
        n_distinct_values=len(rows),
        n_pairs_if_blocked=n_pairs,
        largest_group=largest_group,
        message=message,
    )


def _top_values(con, source_expr: str, is_array: bool, limit: int = 5) -> List[dict]:
    if is_array:
        rows = con.execute(f"""
            SELECT v, COUNT(*) AS n
            FROM (SELECT UNNEST({source_expr}) AS v FROM raw) t
            WHERE v IS NOT NULL AND v != ''
            GROUP BY v ORDER BY n DESC LIMIT {limit}
        """).fetchall()
    else:
        rows = con.execute(f"""
            SELECT {source_expr} AS v, COUNT(*) AS n
            FROM raw WHERE {source_expr} IS NOT NULL
            GROUP BY {source_expr} ORDER BY n DESC LIMIT {limit}
        """).fetchall()
    return [{"value": v, "count": n} for v, n in rows]


def profile_source(
    parquet_path: str,
    warn_pairs: int = DEFAULT_WARN_PAIRS,
    block_pairs: int = DEFAULT_BLOCK_PAIRS,
) -> List[ColumnProfile]:
    con = duckdb.connect()
    try:
        con.execute(f"CREATE VIEW raw AS SELECT * FROM read_parquet('{parquet_path}')")
        total_rows = con.execute("SELECT COUNT(*) FROM raw").fetchone()[0]
        columns = con.execute("DESCRIBE raw").fetchall()

        profiles = []
        for col_name, col_type, *_ in columns:
            is_array = col_type.upper().endswith("[]")
            source_expr = col_name

            if is_array:
                non_null = con.execute(f"""
                    SELECT COUNT(*) FROM raw WHERE {source_expr} IS NOT NULL AND len({source_expr}) > 0
                """).fetchone()[0]
                distinct_count = con.execute(f"""
                    SELECT COUNT(DISTINCT v) FROM (SELECT UNNEST({source_expr}) AS v FROM raw) t WHERE v IS NOT NULL
                """).fetchone()[0]
                avg_length = con.execute(f"""
                    SELECT AVG(len(v)) FROM (SELECT UNNEST({source_expr}) AS v FROM raw) t WHERE v IS NOT NULL
                """).fetchone()[0]
            else:
                non_null = con.execute(f"SELECT COUNT(*) FROM raw WHERE {source_expr} IS NOT NULL").fetchone()[0]
                distinct_count = con.execute(f"SELECT COUNT(DISTINCT {source_expr}) FROM raw").fetchone()[0]
                avg_length = con.execute(
                    f"SELECT AVG(LENGTH(CAST({source_expr} AS VARCHAR))) FROM raw WHERE {source_expr} IS NOT NULL"
                ).fetchone()[0]

            fill_rate = round(non_null / total_rows, 4) if total_rows else 0.0
            semantic_id = guess_semantic_type(col_name, col_type)
            semantic = get_semantic_type(semantic_id)

            verdict = _blocking_verdict(con, source_expr, is_array, warn_pairs, block_pairs)
            top_vals = _top_values(con, source_expr, is_array)

            good_for = []
            if semantic.blocking_safe and verdict.severity != FanoutSeverity.BLOCK:
                good_for.append("blocking")
            if semantic_id not in ("customer_id",):
                good_for.append("matching")
            if verdict.severity == FanoutSeverity.BLOCK:
                good_for.append("would_explode")

            profiles.append(ColumnProfile(
                name=col_name,
                parquet_type=col_type,
                is_array=is_array,
                fill_rate=fill_rate,
                distinct_count=int(distinct_count),
                avg_length=round(avg_length, 1) if avg_length is not None else None,
                top_values=top_vals,
                semantic_type=semantic_id,
                semantic_label=semantic.label,
                blocking_safe=semantic.blocking_safe,
                default_comparators=semantic.default_comparators,
                blocking_verdict=verdict,
                good_for=good_for,
            ))
        return profiles
    finally:
        con.close()
