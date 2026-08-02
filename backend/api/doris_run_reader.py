"""
CUIN v2 - Read-only Doris query helper for API routes (Stage 6)

Opens a connection to a completed run's own Doris database
(cuin_run_<run_id>, the same per-run database pipeline.doris_orchestrator
builds and pipeline.doris_orchestrator._sanitize_db_name names) and
answers /matches and /metrics routes directly from Doris's
pair_decisions/pair_contributions tables -- built unconditionally by
every Doris-backed run (compile_confidence_sql/compile_contributions_sql,
called regardless of low_memory_mode/distributed mode), so this data
exists for every run this backend has ever executed against Doris,
without needing RunService's in-memory `_orchestrators` registry to
still hold that run's orchestrator instance the way get_scores()/
get_decisions() require.

This replaces the Postgres round-trip persist_candidate_pairs_and_decisions
used to do for exactly this purpose -- see db/repository.py's and
pipeline/doris_orchestrator.py's Stage 6 comments (candidate_pairs/
match_scores/match_decisions are no longer written to Postgres at all).
pair_decisions covers EVERY pair (auto_link + review + reject), a
strict superset of what Postgres used to store (audited pairs only,
auto_link + review) -- callers that need the old audited-only view
filter by decision.
"""

import pymysql

from api.config import settings


def _sanitize_db_name(run_id: str) -> str:
    return "cuin_run_" + run_id.replace("-", "_")


def _connect(run_id: str):
    return pymysql.connect(
        host=settings.DORIS_HOST, port=settings.DORIS_MYSQL_PORT,
        user=settings.DORIS_USER, password=settings.DORIS_PASSWORD,
        database=_sanitize_db_name(run_id), autocommit=True,
    )


def _contributions_for_page(cur, pairs: list) -> tuple:
    """
    Returns (signals_by_pair, conflicts_by_pair) for exactly this page
    of (a_key, b_key) pairs. An OR-chain, not a tuple IN(...) -- Doris's
    SQL parser doesn't support row-value tuple comparisons (verified
    live, same limitation hit and worked around elsewhere this session
    in pipeline/doris_orchestrator.py's keyset pagination). Bounded by
    the caller's page_size (a handful to a few hundred pairs in
    practice, this is a paginated admin/debug view, not a hot path),
    not scale-safe for an unbounded pair count the way a staging-table
    JOIN would be -- acceptable here since page_size is always small.
    """
    if not pairs:
        return {}, {}
    where = " OR ".join(["(a_key = %s AND b_key = %s)"] * len(pairs))
    params = [v for pair in pairs for v in pair]

    cur.execute(f"SELECT a_key, b_key, label FROM pair_contributions WHERE matched = true AND ({where})", params)
    signals_by_pair = {}
    for a, b, label in cur.fetchall():
        signals_by_pair.setdefault((a, b), []).append(label)

    cur.execute(f"SELECT a_key, b_key, label FROM pair_contributions WHERE is_veto = true AND ({where})", params)
    conflicts_by_pair = {}
    for a, b, label in cur.fetchall():
        conflicts_by_pair.setdefault((a, b), []).append(label)

    return signals_by_pair, conflicts_by_pair


def run_has_doris_data(run_id: str) -> bool:
    """Whether this run_id has its own Doris database at all (a Doris-backed run, vs. duckdb/spark or nonexistent)."""
    try:
        conn = _connect(run_id)
        conn.close()
        return True
    except Exception:
        return False


def fetch_scores(run_id: str, page: int = 1, page_size: int = 50,
                  min_score=None, max_score=None, decision: str = None) -> dict:
    """Returns {"scores": [...], "total": n} -- pair_id/a_key/b_key/score/decision/signals_hit/hard_conflicts per row."""
    conn = _connect(run_id)
    try:
        with conn.cursor() as cur:
            where, params = [], []
            if min_score is not None:
                where.append("score >= %s"); params.append(min_score)
            if max_score is not None:
                where.append("score <= %s"); params.append(max_score)
            if decision is not None:
                where.append("decision = %s"); params.append(decision)
            where_sql = f"WHERE {' AND '.join(where)}" if where else ""

            cur.execute(f"SELECT COUNT(*) FROM pair_decisions {where_sql}", params)
            total = cur.fetchone()[0]

            offset = (page - 1) * page_size
            cur.execute(f"""
                SELECT a_key, b_key, score, decision FROM pair_decisions
                {where_sql}
                ORDER BY score DESC
                LIMIT %s OFFSET %s
            """, params + [page_size, offset])
            page_rows = cur.fetchall()

            pairs = [(a, b) for a, b, _, _ in page_rows]
            signals_by_pair, conflicts_by_pair = _contributions_for_page(cur, pairs)
    finally:
        conn.close()

    return {
        "scores": [
            {
                "pair_id": f"{a}:{b}", "a_key": a, "b_key": b, "score": float(score),
                "decision": dec,
                "signals_hit": signals_by_pair.get((a, b), []),
                "hard_conflicts": conflicts_by_pair.get((a, b), []),
            }
            for a, b, score, dec in page_rows
        ],
        "total": total,
    }


def fetch_decision_summary(run_id: str) -> dict:
    """Returns {"AUTO_LINK": n, "REVIEW": n, "REJECT": n} (only keys with a nonzero count)."""
    conn = _connect(run_id)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT decision, COUNT(*) FROM pair_decisions GROUP BY decision")
            return {dec: count for dec, count in cur.fetchall()}
    finally:
        conn.close()


def fetch_score_distribution(run_id: str, auto_threshold: float = 0.92, review_threshold: float = 0.55) -> dict:
    """Returns {"total_pairs": n, "average_score": f, "buckets": {...}, "above_auto_threshold": n, "in_review_zone": n, "below_threshold": n}."""
    conn = _connect(run_id)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    SUM(CASE WHEN score < 0.2 THEN 1 ELSE 0 END),
                    SUM(CASE WHEN score >= 0.2 AND score < 0.4 THEN 1 ELSE 0 END),
                    SUM(CASE WHEN score >= 0.4 AND score < 0.6 THEN 1 ELSE 0 END),
                    SUM(CASE WHEN score >= 0.6 AND score < 0.8 THEN 1 ELSE 0 END),
                    SUM(CASE WHEN score >= 0.8 THEN 1 ELSE 0 END),
                    COUNT(*), AVG(score),
                    SUM(CASE WHEN score >= %s THEN 1 ELSE 0 END),
                    SUM(CASE WHEN score >= %s AND score < %s THEN 1 ELSE 0 END),
                    SUM(CASE WHEN score < %s THEN 1 ELSE 0 END)
                FROM pair_decisions
            """, [auto_threshold, review_threshold, auto_threshold, review_threshold])
            (b1, b2, b3, b4, b5, total, avg_score,
             above_auto, in_review, below_review) = cur.fetchone()
    finally:
        conn.close()
    return {
        "total_pairs": total or 0,
        "average_score": float(avg_score) if avg_score is not None else 0.0,
        "above_auto_threshold": above_auto or 0,
        "in_review_zone": in_review or 0,
        "below_threshold": below_review or 0,
        "buckets": {
            "0.0-0.2": b1 or 0, "0.2-0.4": b2 or 0, "0.4-0.6": b3 or 0,
            "0.6-0.8": b4 or 0, "0.8-1.0": b5 or 0,
        },
    }
