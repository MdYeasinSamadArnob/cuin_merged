"""
CUIN v2 - Frequency-Based Junk Suppression, dialect-driven (Ruleset v2, Layer 2)

Dialect-portable twin of engine.blocking.suppression -- same logic
(any normalized identifier value in more than K distinct records is
non-discriminative and suppressed from blocking/STRONG evidence, K an
absolute count per identifier type), translated through
engine.ports.dialect.SqlDialect. engine.blocking.suppression itself is
untouched; see engine.normalize.explode_dialect's docstring for why
this exists as a sibling module rather than a modification.

Two Doris-specific adaptations, both because DorisDialect.
create_or_replace_table compiles to DROP+CREATE (see that module):

1. The original rebuilds `identifier_frequency` FROM `identifier_frequency`
   (adding the is_suppressed column in a second pass) -- a self-reference
   that fails on Doris. Split into two distinctly-named tables instead.
2. The original uses `ALTER TABLE identifiers ADD COLUMN IF NOT EXISTS
   is_suppressed` followed by `... i.* EXCLUDE (is_suppressed) ...` to
   patch the column in place. Doris's ALTER TABLE ADD COLUMN is an async
   schema change (not safe to query immediately after), and EXCLUDE
   requires the column to already exist. Instead this selects the
   explicit column list into a NEW table (`identifiers_staged`), then
   points `identifiers` at it via create_or_replace_table -- the same
   safe, source-name-differs-from-target-name pattern used throughout
   engine.rules.compiler.
"""

from typing import Dict

DEFAULT_THRESHOLDS: Dict[str, int] = {
    "mobile": 20,
    "email": 20,
    "document": 20,
    "name_token": 1500,
    "address": 20,
}

_IDENTIFIER_COLUMNS = ("customer_code", "id_type", "value_raw", "value_norm", "doc_type", "is_valid")


def build_frequency_table(con, dialect, thresholds: Dict[str, int] = None) -> None:
    thresholds = thresholds or DEFAULT_THRESHOLDS

    raw_freq_sql = """
        SELECT
            id_type,
            value_norm,
            COUNT(DISTINCT customer_code) AS n_records
        FROM identifiers
        WHERE is_valid AND value_norm IS NOT NULL
        GROUP BY id_type, value_norm
    """
    for stmt in dialect.create_or_replace_table("identifier_frequency_raw", raw_freq_sql).split(";\n"):
        stmt = stmt.strip()
        if stmt:
            con.execute(stmt)

    case_expr = " ".join(f"WHEN '{id_type}' THEN {threshold}" for id_type, threshold in thresholds.items())
    freq_sql = f"""
        SELECT
            id_type, value_norm, n_records,
            n_records > (CASE id_type {case_expr} ELSE 20 END) AS is_suppressed
        FROM identifier_frequency_raw
    """
    for stmt in dialect.create_or_replace_table("identifier_frequency", freq_sql).split(";\n"):
        stmt = stmt.strip()
        if stmt:
            con.execute(stmt)

    cols = ", ".join(f"i.{c}" for c in _IDENTIFIER_COLUMNS)
    staged_sql = f"""
        SELECT {cols}, COALESCE(f.is_suppressed, FALSE) AS is_suppressed
        FROM identifiers i
        LEFT JOIN identifier_frequency f
            ON i.id_type = f.id_type AND i.value_norm = f.value_norm
    """
    for stmt in dialect.create_or_replace_table("identifiers_staged", staged_sql).split(";\n"):
        stmt = stmt.strip()
        if stmt:
            con.execute(stmt)

    for stmt in dialect.create_or_replace_table("identifiers", "SELECT * FROM identifiers_staged").split(";\n"):
        stmt = stmt.strip()
        if stmt:
            con.execute(stmt)
    con.execute("DROP TABLE IF EXISTS identifiers_staged")
    con.execute("DROP TABLE IF EXISTS identifier_frequency_raw")


def suppression_summary(con) -> list:
    return con.execute("""
        SELECT id_type, value_norm, n_records
        FROM identifier_frequency
        WHERE is_suppressed
        ORDER BY n_records DESC
        LIMIT 50
    """).fetchall()
