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

Read-only: this profiles the raw source Parquet directly via pyarrow's
native compute functions (fast, engine-agnostic analysis -- it doesn't
touch whichever engine will actually run the pipeline). It does not
require a completed run.

Stays in Arrow-native columnar form throughout (list_flatten/
list_parent_indices/value_counts/group_by+count_distinct) rather than
converting to pandas -- measured on the full 1.5M-row dataset:
pandas' .explode()/.groupby() on Python-object string columns took
~50s end-to-end (vs ~10s for the DuckDB SQL this replaced), because
those pandas operations fall back to per-element Python-object
comparisons; pyarrow's compute kernels are vectorized C++ and bring
this back in line with (and on some columns faster than) the original
DuckDB implementation.
"""

from dataclasses import dataclass, field
from typing import List, Optional

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from engine.rules.precheck import (
    FanoutSeverity, DEFAULT_WARN_PAIRS, DEFAULT_BLOCK_PAIRS,
    DEFAULT_WARN_BLOCK_SIZE, DEFAULT_BLOCK_BLOCK_SIZE, _severity,
)
from engine.schema.semantic_types import guess_semantic_type, get_semantic_type

CUSTOMER_CODE_COL = "CUSTOMER_CODE"


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


def _duckdb_style_type_name(pa_type: pa.DataType) -> str:
    """
    Maps a pyarrow type to the DuckDB type-name string this module used
    to return (e.g. "VARCHAR", "VARCHAR[]") -- kept for API/frontend
    display compatibility and because engine.schema.semantic_types.
    guess_semantic_type() pattern-matches specific DuckDB numeric type
    names (BIGINT/INTEGER/DOUBLE/FLOAT/DECIMAL/HUGEINT/SMALLINT).
    """
    if pa.types.is_list(pa_type) or pa.types.is_large_list(pa_type):
        return _duckdb_style_type_name(pa_type.value_type) + "[]"
    if pa.types.is_string(pa_type) or pa.types.is_large_string(pa_type):
        return "VARCHAR"
    if pa.types.is_int64(pa_type):
        return "BIGINT"
    if pa.types.is_int32(pa_type):
        return "INTEGER"
    if pa.types.is_int16(pa_type):
        return "SMALLINT"
    if pa.types.is_float64(pa_type):
        return "DOUBLE"
    if pa.types.is_float32(pa_type):
        return "FLOAT"
    if pa.types.is_decimal(pa_type):
        return "DECIMAL"
    if pa.types.is_boolean(pa_type):
        return "BOOLEAN"
    if pa.types.is_date(pa_type):
        return "DATE"
    if pa.types.is_timestamp(pa_type):
        return "TIMESTAMP"
    return str(pa_type).upper()


def _flattened(values: pa.Array, codes: pa.Array, is_array: bool):
    """
    (values, codes) as flat, position-aligned arrays -- for array
    columns, one row per (customer_code, element); for scalar columns,
    unchanged. Exploded/flattened exactly ONCE per column and reused by
    every stat that needs it.
    """
    if not is_array:
        return values, codes
    flat_values = pc.list_flatten(values)
    parent_indices = pc.list_parent_indices(values)
    flat_codes = codes.take(parent_indices)
    return flat_values, flat_codes


def _grouped_distinct_customer_counts(values: pa.Array, codes: pa.Array) -> pa.Table:
    """value -> COUNT(DISTINCT customer_code), as a 2-column table."""
    tbl = pa.table({"_value": values, "_code": codes})
    return tbl.group_by("_value").aggregate([("_code", "count_distinct")])


def _blocking_verdict(values: pa.Array, codes: pa.Array, warn_pairs: int, block_pairs: int) -> BlockingVerdict:
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
    grouped = _grouped_distinct_customer_counts(values, codes)
    counts = grouped.column("_code_count_distinct").to_pylist()
    n_pairs = sum((n * (n - 1)) // 2 for n in counts if n >= 2)
    largest_group = max(counts) if counts else 0
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
        n_distinct_values=len(counts),
        n_pairs_if_blocked=n_pairs,
        largest_group=largest_group,
        message=message,
    )


def _top_values(values: pa.Array, limit: int = 5) -> List[dict]:
    vc = pc.value_counts(values)
    order = pc.array_sort_indices(vc.field("counts"), order="descending")
    top = vc.take(order).slice(0, limit)
    return [
        {"value": v, "count": int(n)}
        for v, n in zip(top.field("values").to_pylist(), top.field("counts").to_pylist())
    ]


def profile_source(
    parquet_path: str,
    warn_pairs: int = DEFAULT_WARN_PAIRS,
    block_pairs: int = DEFAULT_BLOCK_PAIRS,
) -> List[ColumnProfile]:
    table = pq.read_table(parquet_path)
    total_rows = table.num_rows
    codes_col = table.column(CUSTOMER_CODE_COL)

    profiles = []
    for field_ in table.schema:
        col_name = field_.name
        is_array = pa.types.is_list(field_.type) or pa.types.is_large_list(field_.type)
        col_type = _duckdb_style_type_name(field_.type)
        raw_col = table.column(col_name)

        if is_array:
            lengths = pc.list_value_length(raw_col)
            non_null = int(pc.sum(pc.and_(pc.is_valid(lengths), pc.greater(lengths, 0))).as_py() or 0)
        else:
            non_null = int(pc.sum(pc.is_valid(raw_col)).as_py() or 0)
        fill_rate = round(non_null / total_rows, 4) if total_rows else 0.0

        # null_only: matches the original DuckDB distinct_count/
        # avg_length queries (`v IS NOT NULL` alone). valid: matches
        # blocking_verdict/top_values' additional `v != ''` exclusion
        # for array columns -- an intentional-looking asymmetry in the
        # source queries (an empty telephone/document element is a
        # real distinct VALUE worth counting but not a useful blocking
        # key or a value worth surfacing as "top"), preserved here
        # rather than "fixed" during this migration. Scalars never
        # filtered empty strings anywhere in the original, so
        # null_only == valid for them.
        flat_values, flat_codes = _flattened(raw_col, codes_col, is_array)
        null_only_mask = pc.is_valid(flat_values)
        null_only_values = flat_values.filter(null_only_mask)
        if is_array:
            valid_mask = pc.and_(null_only_mask, pc.not_equal(flat_values, ""))
        else:
            valid_mask = null_only_mask
        valid_values = flat_values.filter(valid_mask)
        valid_codes = flat_codes.filter(valid_mask)

        distinct_count = int(pc.count_distinct(null_only_values).as_py())
        avg_length = pc.mean(pc.utf8_length(null_only_values)).as_py() if len(null_only_values) else None

        semantic_id = guess_semantic_type(col_name, col_type)
        semantic = get_semantic_type(semantic_id)

        verdict = _blocking_verdict(valid_values, valid_codes, warn_pairs, block_pairs)
        top_vals = _top_values(valid_values)

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
            distinct_count=distinct_count,
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
