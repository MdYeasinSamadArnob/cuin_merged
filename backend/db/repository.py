"""
CUIN v2 - Postgres Persistence Repository (Ruleset v2)

Bulk-persists DuckDB pipeline results into Postgres, closing the gap
the audit found: the Spark datasource pipeline wrote NOTHING to
Postgres or Neo4j -- verified empirically as 0 rows across all 8
tables despite a "completed" run reporting thousands of links. Every
run's evidence, decisions, and clusters are now durable and queryable
via the schema already defined in db/schema.sql (candidate_pairs,
match_scores, match_decisions, clusters) plus the ruleset/fingerprint
columns added in db/migrations/002_identity_sets_and_ruleset.sql.

Only customers that appear in at least one candidate pair are
persisted to customers_norm -- not the full 1.5M-row source dataset.
This keeps the persisted footprint proportional to "records with
found evidence" rather than a blind full copy, which is both faster
and more semantically correct for a table whose purpose is supporting
matching, not warehousing raw source data.

customers_norm.customer_key is a Postgres UUID (schema-defined,
auto-generated), while the pipeline's identity key is the string
CUSTOMER_CODE. upsert_customers() is the mapping layer between the
two, keyed by (source_customer_id, source_system) per the schema's
UNIQUE constraint.
"""

import csv
import io
import json
import logging
from typing import Dict, List, Tuple
from uuid import uuid4

import duckdb
import psycopg2
from psycopg2.extras import execute_values, Json

logger = logging.getLogger(__name__)


def _pg_text_array_literal(values: List[str]) -> str:
    """
    Postgres COPY's TEXT[] representation: `{"a","b"}`. Every element is
    always double-quoted (technically only needed for elements
    containing `,{}"\\` or whitespace, but always-quote is simpler and
    unambiguous) with `\\` and `"` backslash-escaped inside. Used by
    persist_candidate_pairs_and_decisions' COPY path -- execute_values
    left this to psycopg2's list adapter; COPY is a raw text protocol
    with no adapters, so array literals have to be built by hand.
    """
    if not values:
        return "{}"
    escaped = [v.replace("\\", "\\\\").replace('"', '\\"') for v in values]
    return "{" + ",".join(f'"{v}"' for v in escaped) + "}"


def _cluster_id_to_uuid(cluster_id: str) -> str:
    """
    engine.clustering.cluster_manager generates deterministic cluster IDs
    as "CL_<32 hex chars>" (sha256-derived, see
    ClusterManager._get_or_create_cluster_id) -- readable/sliceable for
    the /graph/* API (which does `f"Cluster-{cluster_id[:8]}"`) and JSON
    snapshots. db/schema.sql's `clusters.cluster_id` column is UUID,
    though, so this reformats the SAME 32 hex chars into standard
    8-4-4-4-12 UUID dash placement -- deterministic and lossless (it's
    the identical hash, just re-punctuated), not a new random ID.
    """
    hex_part = cluster_id[3:] if cluster_id.startswith("CL_") else cluster_id
    hex_part = hex_part.ljust(32, "0")[:32]
    return f"{hex_part[0:8]}-{hex_part[8:12]}-{hex_part[12:16]}-{hex_part[16:20]}-{hex_part[20:32]}"


def upsert_customers(pg_conn, con: duckdb.DuckDBPyConnection, customer_codes: List[str]) -> Dict[str, str]:
    """
    Upserts customers_norm for the given CUSTOMER_CODEs and returns
    {customer_code: customer_key_uuid}.
    """
    if not customer_codes:
        return {}

    placeholders = ",".join(["?"] * len(customer_codes))
    rows = con.execute(f"""
        SELECT s.customer_code, s.name_norm, s.dob_iso
        FROM customer_scalars s
        WHERE s.customer_code IN ({placeholders})
    """, customer_codes).fetchall()

    values = [
        (code, name_norm, dob_iso, f"duckdb-pipeline:{code}", "ORACLE_DATASOURCE")
        for code, name_norm, dob_iso in rows
    ]

    with pg_conn.cursor() as cur:
        inserted = execute_values(cur, """
            INSERT INTO customers_norm
                (source_customer_id, name_norm, dob_norm, record_hash, source_system)
            VALUES %s
            ON CONFLICT (source_customer_id, source_system) DO UPDATE SET
                name_norm = EXCLUDED.name_norm,
                dob_norm = EXCLUDED.dob_norm,
                updated_at = NOW()
            RETURNING source_customer_id, customer_key
        """, values, fetch=True, page_size=5000)
        mapping = {row[0]: str(row[1]) for row in inserted}
    pg_conn.commit()
    return mapping


def persist_identifier_frequency(pg_conn, con: duckdb.DuckDBPyConnection, run_id: str) -> int:
    rows = con.execute("""
        SELECT id_type, value_norm, n_records, is_suppressed
        FROM identifier_frequency
        WHERE is_suppressed
    """).fetchall()

    if not rows:
        return 0

    with pg_conn.cursor() as cur:
        # bool(r[3]): DuckDB's connector returns a native Python bool
        # for a BOOLEAN column, but pymysql (Doris) returns a plain int
        # (0/1) -- psycopg2's execute_values infers that int's SQL type
        # as `integer`, which Postgres's boolean column then rejects
        # ("column is of type boolean but expression is of type
        # integer"). Verified live against a real Doris-backed run.
        execute_values(cur, """
            INSERT INTO identifier_frequency (run_id, id_type, value_norm, n_records, is_suppressed)
            VALUES %s
        """, [(run_id, r[0], r[1], r[2], bool(r[3])) for r in rows], page_size=5000)
    pg_conn.commit()
    return len(rows)


# (index_name, create_statement) -- exact mirrors of db/schema.sql's
# definitions for candidate_pairs/match_scores/match_decisions' non-PK
# indexes. Deliberately excludes their FK constraints: those are cheap
# point-lookups against an already-indexed PK on the referenced table
# (customers_norm.customer_key, runs.run_id, candidate_pairs.pair_id),
# not where bulk-insert cost concentrates -- the expensive part is
# building NEW B-tree entries for THIS table's own indexes on every
# inserted row, which is what dropping and rebuilding these avoids.
BULK_WRITE_INDEXES = [
    ("idx_candidates_run", "CREATE INDEX idx_candidates_run ON candidate_pairs(run_id)"),
    ("idx_candidates_a_key", "CREATE INDEX idx_candidates_a_key ON candidate_pairs(a_key)"),
    ("idx_candidates_b_key", "CREATE INDEX idx_candidates_b_key ON candidate_pairs(b_key)"),
    ("idx_scores_run", "CREATE INDEX idx_scores_run ON match_scores(run_id)"),
    ("idx_scores_score", "CREATE INDEX idx_scores_score ON match_scores(score)"),
    ("idx_decisions_run", "CREATE INDEX idx_decisions_run ON match_decisions(run_id)"),
    ("idx_decisions_decision", "CREATE INDEX idx_decisions_decision ON match_decisions(decision)"),
]


def drop_bulk_write_indexes(pg_conn) -> None:
    """
    Call ONCE before a bulk backfill's persist phase starts (not per
    batch -- see rebuild_bulk_write_indexes' docstring for why),
    paired with rebuild_bulk_write_indexes after. These 3 tables are
    live and shared with concurrent runs/API routes (routes_matches.py
    etc query them directly) -- dropping their indexes mid-flight is
    only safe for a deliberate, exclusive backfill window, not routine
    per-run behavior. Every regular execute_values/COPY path still
    works with these indexes present or absent (indexes only affect
    read performance and write cost, never correctness), so this is
    purely an opt-in performance lever, never required for correctness.
    """
    with pg_conn.cursor() as cur:
        for name, _ in BULK_WRITE_INDEXES:
            cur.execute(f"DROP INDEX IF EXISTS {name}")
    pg_conn.commit()
    logger.info(f"Dropped {len(BULK_WRITE_INDEXES)} bulk-write indexes ahead of backfill persist phase")


def rebuild_bulk_write_indexes(pg_conn) -> None:
    """
    Rebuilds what drop_bulk_write_indexes dropped. Plain CREATE INDEX
    (not CONCURRENTLY): CONCURRENTLY can't run inside a multi-statement
    transaction, which pg_conn's explicit-commit usage throughout this
    module already assumes -- doing it here would need a second,
    separate autocommit connection, worth adding if this is ever run
    against a table serving real concurrent write traffic during
    rebuild, but not needed for a maintenance-window backfill where
    nothing else is writing here at the same time.
    """
    with pg_conn.cursor() as cur:
        for name, create_sql in BULK_WRITE_INDEXES:
            cur.execute(create_sql)
    pg_conn.commit()
    logger.info(f"Rebuilt {len(BULK_WRITE_INDEXES)} bulk-write indexes after backfill persist phase")


def persist_candidate_pairs_and_decisions(
    pg_conn,
    run_id: str,
    code_to_uuid: Dict[str, str],
    scores: dict,
    decisions: dict,
    ruleset_version: str,
) -> Tuple[int, int, int]:
    """
    scores/decisions: keyed by pair_id "a_key:b_key" -> MatchScore / MatchDecision
    (from pipeline.duckdb_orchestrator's get_scores()/get_decisions()).
    Only persists pairs where BOTH endpoints made it into code_to_uuid
    (i.e. were upserted -- callers should upsert first).

    pair_id is generated client-side (uuid4) rather than left to
    candidate_pairs' server-side default (uuid_generate_v4()). The
    prior version relied on the server default plus `RETURNING pair_id,
    a_key, b_key` to learn each inserted row's id -- correct, but at
    ~270K audited pairs that means shipping every row's a_key/b_key
    back over the wire a SECOND time just to read back an id we could
    have chosen ourselves, plus a conditional fallback SELECT for any
    row ON CONFLICT skipped. Measured as this function's dominant cost
    (98s of a 322s Doris run). Choosing pair_id up front removes both
    round trips entirely: candidate_pairs/match_scores/match_decisions
    can all be bulk-inserted independently.

    Independently profiled after that fix barely moved the number
    (98s -> 89.6s) -- the real cost was never the round trip, it's the
    per-row statement overhead `execute_values` still pays even at
    page_size=5000 (confirmed evenly split across all 3 tables:
    ~35s/25s/23s at ~271K rows each). Switched to COPY into a
    session-scoped TEMP staging table per target table, then one
    set-based `INSERT ... SELECT ... ON CONFLICT DO NOTHING` moves
    staged rows into the real table -- COPY's wire protocol skips
    per-row parse/plan entirely, and staging preserves the idempotency
    ON CONFLICT gives that plain COPY-into-target can't express. Array
    columns (TEXT[]) have no COPY-time adapter the way execute_values'
    psycopg2 list-adaptation gave for free, so `_pg_text_array_literal`
    hand-builds their `{"a","b"}` literal.

    Dedups on (a_uuid, b_uuid) client-side (`seen_uuid_pairs`) rather
    than leaning on candidate_pairs' ON CONFLICT (run_id, a_key,
    b_key) DO NOTHING to drop a repeat: two DIFFERENT code-pairs could
    in principle resolve to the same (a_uuid, b_uuid) if code_to_uuid
    ever isn't injective, and a client-chosen pair_id makes that
    dangerous instead of just redundant -- match_scores/match_decisions
    would still try to insert a row against a pair_id that candidate_pairs'
    conflict just silently dropped, an FK violation. Deduping before
    any row is built sidesteps this the same way ON CONFLICT DO
    NOTHING did, just without depending on the database to notice.
    """
    pair_rows, score_rows, decision_rows = [], [], []
    seen_uuid_pairs = set()

    for pair_id, score in scores.items():
        a_uuid = code_to_uuid.get(score.a_key)
        b_uuid = code_to_uuid.get(score.b_key)
        if not a_uuid or not b_uuid:
            continue
        a_uuid, b_uuid = sorted([a_uuid, b_uuid])
        if (a_uuid, b_uuid) in seen_uuid_pairs:
            continue
        seen_uuid_pairs.add((a_uuid, b_uuid))
        row_pair_id = str(uuid4())

        evidence_json = [
            {
                "field": e.field_name, "value_a": e.value_a, "value_b": e.value_b,
                "comparison_type": e.comparison_type, "similarity": e.similarity_score,
                "weight": e.match_weight, "explanation": e.explanation,
            }
            for e in score.evidence
        ]

        pair_rows.append((row_pair_id, run_id, a_uuid, b_uuid, score.hard_conflicts or score.signals_hit or ["blocked"]))
        score_rows.append((row_pair_id, run_id, float(score.score), evidence_json))

        decision = decisions.get(pair_id)
        if decision:
            decision_rows.append((
                row_pair_id, run_id, decision.value if hasattr(decision, "value") else str(decision),
                float(score.score), score.signals_hit, score.hard_conflicts, ruleset_version,
            ))

    if not pair_rows:
        return 0, 0, 0

    import time as _time
    with pg_conn.cursor() as cur:
        # SET LOCAL scopes to just this transaction (auto-reverts at
        # commit) -- a durability/latency tradeoff, not a correctness
        # one: the data written is identical either way, this only
        # controls whether COMMIT waits for the WAL fsync to complete
        # before returning. A hard crash in the following instant could
        # lose this transaction, but never corrupt or half-write it.
        # Tried after COPY-into-staging + dropped indexes still only
        # bought a modest win, to test whether WAL fsync -- not
        # indexes -- was the real remaining floor.
        cur.execute("SET LOCAL synchronous_commit = OFF")

        # COPY into an UNLOGGED-equivalent (session-scoped TEMP, which
        # is already unlogged and auto-cleaned at connection close) is
        # the standard fast-bulk-load pattern: COPY's wire protocol
        # skips per-row statement parsing/planning that even a
        # page_size=5000 execute_values still pays, and staging first
        # keeps ON CONFLICT / idempotency semantics COPY itself can't
        # express (COPY has no ON CONFLICT clause). DROP+CREATE (not
        # CREATE IF NOT EXISTS) because this function can run multiple
        # times per pg_conn -- low_memory_mode calls it once per score
        # batch on the SAME connection (see doris_orchestrator's
        # _score_and_decide_batched), and a stale staging table from
        # the previous batch must not leak rows into this one.
        cur.execute("DROP TABLE IF EXISTS _stage_candidate_pairs")
        cur.execute("""
            CREATE TEMP TABLE _stage_candidate_pairs
                (pair_id UUID, run_id UUID, a_key UUID, b_key UUID, blocking_reasons TEXT[])
        """)
        cur.execute("DROP TABLE IF EXISTS _stage_match_scores")
        cur.execute("""
            CREATE TEMP TABLE _stage_match_scores (pair_id UUID, run_id UUID, score DECIMAL(5,4), evidence_json JSONB)
        """)
        cur.execute("DROP TABLE IF EXISTS _stage_match_decisions")
        cur.execute("""
            CREATE TEMP TABLE _stage_match_decisions (
                pair_id UUID, run_id UUID, decision VARCHAR(20), threshold_used DECIMAL(5,4),
                signals_hit TEXT[], hard_conflict_flags TEXT[], ruleset_version VARCHAR(64)
            )
        """)

        _t0 = _time.perf_counter()
        pairs_buf = io.StringIO()
        pairs_csv = csv.writer(pairs_buf, lineterminator='\n')
        for row_pair_id, r_id, a_uuid, b_uuid, reasons in pair_rows:
            pairs_csv.writerow([row_pair_id, r_id, a_uuid, b_uuid, _pg_text_array_literal(reasons)])
        pairs_buf.seek(0)
        cur.copy_expert(
            "COPY _stage_candidate_pairs (pair_id, run_id, a_key, b_key, blocking_reasons) FROM STDIN WITH (FORMAT csv)",
            pairs_buf,
        )
        cur.execute("""
            INSERT INTO candidate_pairs (pair_id, run_id, a_key, b_key, blocking_reasons)
            SELECT pair_id, run_id, a_key, b_key, blocking_reasons FROM _stage_candidate_pairs
            ON CONFLICT (run_id, a_key, b_key) DO NOTHING
        """)
        _t1 = _time.perf_counter()

        scores_buf = io.StringIO()
        scores_csv = csv.writer(scores_buf, lineterminator='\n')
        for row_pair_id, r_id, score_val, evidence_json in score_rows:
            scores_csv.writerow([row_pair_id, r_id, score_val, json.dumps(evidence_json)])
        scores_buf.seek(0)
        cur.copy_expert(
            "COPY _stage_match_scores (pair_id, run_id, score, evidence_json) FROM STDIN WITH (FORMAT csv)",
            scores_buf,
        )
        cur.execute("""
            INSERT INTO match_scores (pair_id, run_id, score, evidence_json)
            SELECT pair_id, run_id, score, evidence_json FROM _stage_match_scores
            ON CONFLICT (pair_id) DO NOTHING
        """)
        _t2 = _time.perf_counter()

        if decision_rows:
            decisions_buf = io.StringIO()
            decisions_csv = csv.writer(decisions_buf, lineterminator='\n')
            for row_pair_id, r_id, decision_val, threshold, signals, conflicts, rv in decision_rows:
                decisions_csv.writerow([
                    row_pair_id, r_id, decision_val, threshold,
                    _pg_text_array_literal(signals), _pg_text_array_literal(conflicts), rv,
                ])
            decisions_buf.seek(0)
            cur.copy_expert(
                "COPY _stage_match_decisions (pair_id, run_id, decision, threshold_used, signals_hit, "
                "hard_conflict_flags, ruleset_version) FROM STDIN WITH (FORMAT csv)",
                decisions_buf,
            )
            cur.execute("""
                INSERT INTO match_decisions
                    (pair_id, run_id, decision, threshold_used, signals_hit, hard_conflict_flags, ruleset_version)
                SELECT pair_id, run_id, decision, threshold_used, signals_hit, hard_conflict_flags, ruleset_version
                FROM _stage_match_decisions
                ON CONFLICT (pair_id) DO NOTHING
            """)
        _t3 = _time.perf_counter()
        logger.info(
            f"persist_candidate_pairs_and_decisions breakdown (s): "
            f"candidate_pairs={_t1 - _t0:.3f} match_scores={_t2 - _t1:.3f} match_decisions={_t3 - _t2:.3f} "
            f"({len(pair_rows):,} pairs, {len(score_rows):,} scores, {len(decision_rows):,} decisions)"
        )

    pg_conn.commit()
    return len(pair_rows), len(score_rows), len(decision_rows)


def persist_clusters(
    pg_conn,
    code_to_uuid: Dict[str, str],
    clusters: Dict[str, List[str]],
    ruleset_version: str,
    cohesion_by_cluster: Dict[str, float] = None,
) -> int:
    cohesion_by_cluster = cohesion_by_cluster or {}
    rows = []
    for cluster_id, members in clusters.items():
        density = cohesion_by_cluster.get(cluster_id)
        cluster_uuid = _cluster_id_to_uuid(cluster_id)
        for member_code in members:
            member_uuid = code_to_uuid.get(member_code)
            if not member_uuid:
                continue
            rows.append((cluster_uuid, member_uuid, 1, ruleset_version, density))

    if not rows:
        return 0

    with pg_conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO clusters (cluster_id, customer_key, active_version, ruleset_version, cohesion_density)
            VALUES %s
            ON CONFLICT (cluster_id, customer_key, active_version) DO NOTHING
        """, [(r[0], r[1], r[2], r[3], r[4]) for r in rows], page_size=5000)
    pg_conn.commit()
    return len(rows)


def persist_entity_relationships(
    pg_conn,
    run_id: str,
    code_to_uuid: Dict[str, str],
    relationships: List[dict],
) -> int:
    """
    Persists cross-segment connections (Stage 5 -- see
    db/migrations/004_entity_relationships.sql and
    engine.segments.relationships): a person and a company sharing a
    phone/email/document/address, kept traceable but never merged into
    one identity the way a same-segment AUTO_LINK/REVIEW pair would be.

    `relationships`: [{"a_key","b_key","a_segment","b_segment","shared_evidence"}, ...]
    with a_key/b_key as raw customer_code strings (mirrors the shape
    pipeline orchestrators build in _stage_score_and_decide).
    """
    if not relationships:
        return 0

    rows = []
    for rel in relationships:
        a_uuid = code_to_uuid.get(rel["a_key"])
        b_uuid = code_to_uuid.get(rel["b_key"])
        if not a_uuid or not b_uuid:
            continue
        # entity_relationships' ordered_relationship_pair CHECK requires a_key < b_key.
        if a_uuid < b_uuid:
            a_uuid, b_uuid = a_uuid, b_uuid
            a_seg, b_seg = rel["a_segment"], rel["b_segment"]
        else:
            a_uuid, b_uuid = b_uuid, a_uuid
            a_seg, b_seg = rel["b_segment"], rel["a_segment"]
        rows.append((run_id, a_uuid, b_uuid, a_seg, b_seg, Json(rel["shared_evidence"])))

    if not rows:
        return 0

    with pg_conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO entity_relationships (run_id, a_key, b_key, a_segment, b_segment, shared_evidence)
            VALUES %s
            ON CONFLICT (run_id, a_key, b_key) DO NOTHING
        """, rows, page_size=5000)
    pg_conn.commit()
    return len(rows)


def update_run_fingerprints(pg_conn, run_id: str, fingerprints: dict, counters: dict) -> None:
    with pg_conn.cursor() as cur:
        cur.execute("""
            UPDATE runs SET
                ruleset_version = %s,
                input_fingerprint = %s,
                ruleset_fingerprint = %s,
                output_fingerprint = %s,
                records_in = %s,
                candidates_generated = %s,
                pairs_scored = %s,
                auto_links = %s,
                review_items = %s,
                rejected = %s
            WHERE run_id = %s
        """, (
            fingerprints.get("ruleset_version"),
            fingerprints.get("input_fingerprint"),
            fingerprints.get("ruleset_fingerprint"),
            fingerprints.get("output_fingerprint"),
            counters.get("records_in", 0),
            counters.get("candidates_generated", 0),
            counters.get("pairs_scored", 0),
            counters.get("auto_links", 0),
            counters.get("review_items", 0),
            counters.get("rejected", 0),
            run_id,
        ))
    pg_conn.commit()


def ensure_run_row(pg_conn, run_id: str, mode: str, description: str) -> None:
    """
    The DuckDB orchestrator's run_id is generated by services.run_service
    (a Python uuid4 string), independent of Postgres's `runs` table --
    insert a matching row so foreign keys from candidate_pairs etc. resolve.
    """
    with pg_conn.cursor() as cur:
        cur.execute("""
            INSERT INTO runs (run_id, mode, policy_version, status, description)
            VALUES (%s, %s, 1, 'RUNNING', %s)
            ON CONFLICT (run_id) DO NOTHING
        """, (run_id, mode, description))
    pg_conn.commit()
