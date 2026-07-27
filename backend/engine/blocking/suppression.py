"""
CUIN v2 - Frequency-Based Junk Suppression (Ruleset v2, Layer 2)

Any normalized identifier value occurring in more than K distinct
records is non-discriminative and excluded from both blocking and
STRONG evidence. K is an ABSOLUTE count, not a percentage — the
existing engine.blocking.BlockingConfig.suppress_frequency_pct=90.0
is a percent of total rows, so at 1.57M rows that threshold is
~1.4M records and never fires in practice. This module fixes that.

Data-driven: this neutralizes today's junk (01711000000 x750,
"EI:Enterprise Info" x21,700) and tomorrow's junk with no hardcoded
blacklist — anything common enough to be non-discriminative gets
suppressed automatically.
"""

import duckdb
from typing import Dict


# Per-identifier-type suppression thresholds, matching policies/ruleset_v2.yaml.
DEFAULT_THRESHOLDS: Dict[str, int] = {
    "mobile": 20,
    "email": 20,
    "document": 20,
    "name_token": 1500,
    "address": 20,
}


def build_frequency_table(con: duckdb.DuckDBPyConnection, thresholds: Dict[str, int] = None) -> None:
    """
    Given an `identifiers` table (customer_code, id_type, value_norm, is_valid, ...)
    already registered on `con`, build `identifier_frequency` and mark
    `is_suppressed` per identifier row.

    Persisted per run so an auditor can see exactly why a given value
    was ignored (e.g. "01711000000 appeared in 750 records > threshold 20").
    """
    thresholds = thresholds or DEFAULT_THRESHOLDS

    con.execute("""
        CREATE OR REPLACE TABLE identifier_frequency AS
        SELECT
            id_type,
            value_norm,
            COUNT(DISTINCT customer_code) AS n_records
        FROM identifiers
        WHERE is_valid AND value_norm IS NOT NULL
        GROUP BY id_type, value_norm
    """)

    # Per-type threshold via a CASE expression (DuckDB has no per-row
    # parameterized join for a small dict — a VALUES list is simplest
    # and keeps the mapping visible in the query plan for audit).
    case_expr = " ".join(
        f"WHEN '{id_type}' THEN {threshold}" for id_type, threshold in thresholds.items()
    )

    con.execute(f"""
        CREATE OR REPLACE TABLE identifier_frequency AS
        SELECT
            id_type,
            value_norm,
            n_records,
            n_records > (CASE id_type {case_expr} ELSE 20 END) AS is_suppressed
        FROM identifier_frequency
    """)

    con.execute("""
        ALTER TABLE identifiers ADD COLUMN IF NOT EXISTS is_suppressed BOOLEAN;
    """)
    con.execute("""
        CREATE OR REPLACE TABLE identifiers AS
        SELECT
            i.* EXCLUDE (is_suppressed),
            COALESCE(f.is_suppressed, FALSE) AS is_suppressed
        FROM identifiers i
        LEFT JOIN identifier_frequency f
            ON i.id_type = f.id_type AND i.value_norm = f.value_norm
    """)


def suppression_summary(con: duckdb.DuckDBPyConnection) -> list:
    """Top suppressed values per type, for the audit trail / run summary."""
    return con.execute("""
        SELECT id_type, value_norm, n_records
        FROM identifier_frequency
        WHERE is_suppressed
        ORDER BY n_records DESC
        LIMIT 50
    """).fetchall()
