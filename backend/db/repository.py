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

import logging
from typing import Dict, List, Tuple

import duckdb
import psycopg2
from psycopg2.extras import execute_values, Json

logger = logging.getLogger(__name__)


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
    """
    pair_rows, score_rows, decision_rows = [], [], []

    for pair_id, score in scores.items():
        a_uuid = code_to_uuid.get(score.a_key)
        b_uuid = code_to_uuid.get(score.b_key)
        if not a_uuid or not b_uuid:
            continue
        a_uuid, b_uuid = sorted([a_uuid, b_uuid])

        evidence_json = [
            {
                "field": e.field_name, "value_a": e.value_a, "value_b": e.value_b,
                "comparison_type": e.comparison_type, "similarity": e.similarity_score,
                "weight": e.match_weight, "explanation": e.explanation,
            }
            for e in score.evidence
        ]

        pair_rows.append((run_id, a_uuid, b_uuid, score.hard_conflicts or score.signals_hit or ["blocked"]))
        score_rows.append((run_id, a_uuid, b_uuid, float(score.score), Json(evidence_json)))

        decision = decisions.get(pair_id)
        if decision:
            decision_rows.append((
                run_id, a_uuid, b_uuid, decision.value if hasattr(decision, "value") else str(decision),
                float(score.score), score.signals_hit, score.hard_conflicts, ruleset_version,
            ))

    if not pair_rows:
        return 0, 0, 0

    with pg_conn.cursor() as cur:
        inserted = execute_values(cur, """
            INSERT INTO candidate_pairs (run_id, a_key, b_key, blocking_reasons)
            VALUES %s
            ON CONFLICT (run_id, a_key, b_key) DO NOTHING
            RETURNING pair_id, a_key, b_key
        """, pair_rows, fetch=True, page_size=5000)
        pair_id_map = {(str(r[1]), str(r[2])): r[0] for r in inserted}

        # Pairs already existing (ON CONFLICT DO NOTHING skipped them) still
        # need their pair_id for the score/decision inserts below.
        missing = [(a, b) for (_, a, b, _) in pair_rows if (a, b) not in pair_id_map]
        if missing:
            cur.execute("""
                SELECT pair_id, a_key, b_key FROM candidate_pairs
                WHERE run_id = %s AND (a_key, b_key) IN %s
            """, (run_id, tuple(missing)))
            for r in cur.fetchall():
                pair_id_map[(str(r[1]), str(r[2]))] = r[0]

        score_values = []
        for run_id_, a_uuid, b_uuid, score_val, ev in score_rows:
            pid = pair_id_map.get((a_uuid, b_uuid))
            if pid:
                score_values.append((pid, run_id_, score_val, ev))

        if score_values:
            execute_values(cur, """
                INSERT INTO match_scores (pair_id, run_id, score, evidence_json)
                VALUES %s
                ON CONFLICT (pair_id) DO NOTHING
            """, score_values, page_size=5000)

        decision_values = []
        for run_id_, a_uuid, b_uuid, decision_val, score_val, signals, conflicts, rv in decision_rows:
            pid = pair_id_map.get((a_uuid, b_uuid))
            if pid:
                decision_values.append((pid, run_id_, decision_val, score_val, signals, conflicts, rv))

        if decision_values:
            execute_values(cur, """
                INSERT INTO match_decisions
                    (pair_id, run_id, decision, threshold_used, signals_hit, hard_conflict_flags, ruleset_version)
                VALUES %s
                ON CONFLICT (pair_id) DO NOTHING
            """, decision_values, page_size=5000)

    pg_conn.commit()
    return len(pair_id_map), len(score_values), len(decision_values)


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
