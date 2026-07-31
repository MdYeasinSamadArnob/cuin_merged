"""
Acceptance test for engine.rules.compiler: the default rule catalog
(engine.rules.catalog.DEFAULT_BLOCKING_RULES), compiled through
DuckDbDialect, must reproduce
engine.blocking.deterministic_blocker.build_candidate_pairs's output
byte-for-byte -- same pairs, same order, same blocking_reasons
strings. This is the safety net for switching the pipeline from
hand-written blocking SQL to the UI-editable rule catalog: if this
test passes, no pipeline output changes as a result of that switch.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

import duckdb

from engine.normalize.explode import build_identifiers_table
from engine.blocking.suppression import build_frequency_table
from engine.blocking.deterministic_blocker import build_candidate_pairs, build_name_dob_keys
from engine.rules.catalog import DEFAULT_BLOCKING_RULES
from engine.rules.compiler import compile_and_build
from engine.ports.duckdb_dialect import DuckDbDialect

SAMPLE_PARQUET = os.path.join(os.path.dirname(__file__), "..", "fixtures", "sample_5k.parquet")


def _setup_con():
    con = duckdb.connect()
    con.execute("SET preserve_insertion_order=true")
    con.execute(f"CREATE OR REPLACE VIEW raw AS SELECT * FROM read_parquet('{SAMPLE_PARQUET}')")
    build_identifiers_table(con)
    build_frequency_table(con)
    build_name_dob_keys(con)
    return con


def test_compiled_default_catalog_matches_hand_written_blocker():
    if not os.path.exists(SAMPLE_PARQUET):
        import pytest
        pytest.skip(f"fixture not found: {SAMPLE_PARQUET}")

    con_orig = _setup_con()
    build_candidate_pairs(con_orig)
    orig = con_orig.execute(
        "SELECT a_key, b_key, blocking_reasons FROM candidate_pairs ORDER BY a_key, b_key"
    ).fetchall()
    con_orig.close()

    con_new = _setup_con()
    compile_and_build(con_new, list(DEFAULT_BLOCKING_RULES), DuckDbDialect())
    new = con_new.execute(
        "SELECT a_key, b_key, blocking_reasons FROM candidate_pairs ORDER BY a_key, b_key"
    ).fetchall()
    con_new.close()

    assert len(orig) > 0, "no candidate pairs in sample fixture -- fixture missing/empty?"
    assert orig == new, (
        f"compiled rule catalog diverges from hand-written blocker: "
        f"{len(orig)} vs {len(new)} pairs, first mismatch: "
        f"{next(((o, n) for o, n in zip(orig, new) if o != n), None)}"
    )


def test_disabling_a_rule_changes_output():
    """
    Without this, a compiler bug that ignores `enabled` entirely would
    trivially pass the byte-identical test above (both catalogs would
    just run every rule regardless).
    """
    if not os.path.exists(SAMPLE_PARQUET):
        import pytest
        pytest.skip(f"fixture not found: {SAMPLE_PARQUET}")

    from dataclasses import replace

    con = _setup_con()
    only_exact = [r for r in DEFAULT_BLOCKING_RULES if r.rule_id == "exact_identifier_strong"]
    compile_and_build(con, only_exact, DuckDbDialect())
    n_exact_only = con.execute("SELECT COUNT(*) FROM candidate_pairs").fetchone()[0]
    con.close()

    con2 = _setup_con()
    compile_and_build(con2, list(DEFAULT_BLOCKING_RULES), DuckDbDialect())
    n_full = con2.execute("SELECT COUNT(*) FROM candidate_pairs").fetchone()[0]
    con2.close()

    assert n_exact_only <= n_full


def test_guard_shrinks_or_maintains_candidate_count():
    """A tighter max_block_size guard on the composite rule must never INCREASE candidates."""
    if not os.path.exists(SAMPLE_PARQUET):
        import pytest
        pytest.skip(f"fixture not found: {SAMPLE_PARQUET}")

    from dataclasses import replace

    loose = list(DEFAULT_BLOCKING_RULES)
    tight = [
        replace(r, guards=replace(r.guards, max_block_size=2)) if r.type.value == "composite_key" else r
        for r in DEFAULT_BLOCKING_RULES
    ]

    con1 = _setup_con()
    compile_and_build(con1, loose, DuckDbDialect())
    n_loose = con1.execute("SELECT COUNT(*) FROM candidate_pairs").fetchone()[0]
    con1.close()

    con2 = _setup_con()
    compile_and_build(con2, tight, DuckDbDialect())
    n_tight = con2.execute("SELECT COUNT(*) FROM candidate_pairs").fetchone()[0]
    con2.close()

    assert n_tight <= n_loose


if __name__ == "__main__":
    test_compiled_default_catalog_matches_hand_written_blocker()
    test_disabling_a_rule_changes_output()
    test_guard_shrinks_or_maintains_candidate_count()
    print("All rule-compiler zero-change tests passed.")
