"""
CUIN v2 - Decision Rule Compiler (Ruleset v2, Layer 4/5 as SQL)

Compiles engine.scoring.tiers.classify()/decide() -- currently a
Python loop executed once per candidate pair -- into a single SQL
statement over pair_identifier_evidence + pair_name_dob_evidence
(built by engine.scoring.evidence.build_pair_evidence). This is what
makes a threshold-slider move in the Settings UI return in
milliseconds instead of requiring a re-run: classify()/decide() are
pure functions of those two evidence tables, so re-deciding is one
aggregate query over data that already exists.

tiers.py remains the tested Python oracle -- see
tests/unit/test_decision_sql_parity.py, which asserts this compiled
SQL and tiers.classify()/decide() agree on every pair of the 5k
fixture. Do not change the CASE ordering below (veto check MUST come
first) without re-running that test: getting the precedence wrong
flips real AUTO_LINK/REVIEW/REJECT decisions.
"""

from engine.ports.dialect import SqlDialect
from engine.rules.scoring_rules import ScoringRules


def compile_decision_sql(scoring: ScoringRules, dialect: SqlDialect, table_name: str = "pair_decisions") -> str:
    """
    Returns the full `CREATE OR REPLACE TABLE {table_name} AS ...`
    statement. Requires `pair_identifier_evidence` and
    `pair_name_dob_evidence` (engine.scoring.evidence.build_pair_evidence)
    already built on the connection.

    Output columns: a_key, b_key, strong_count, medium_name,
    medium_dob, has_veto, decision, score.
    """
    strong_types_sql = ", ".join(dialect.quote_str(t) for t in scoring.strong_identifier_types)
    jaccard_threshold = scoring.medium_name_jaccard_threshold
    dob_precision = dialect.quote_str(scoring.medium_dob_requires_precision)

    select_sql = f"""
        WITH id_tiers AS (
            SELECT
                a_key, b_key,
                SUM(CASE WHEN id_type IN ({strong_types_sql})
                         AND {dialect.array_size('intersection')} > 0
                    THEN 1 ELSE 0 END) AS strong_count,
                MAX(CASE WHEN id_type = 'document' AND {dialect.array_size('intersection')} = 0
                    THEN 1 ELSE 0 END) AS veto_document
            FROM pair_identifier_evidence
            GROUP BY a_key, b_key
        ),
        name_dob_tiers AS (
            -- pair_name_dob_evidence has exactly one row per candidate_pairs
            -- row (see engine.scoring.evidence.build_pair_evidence) -- this
            -- CTE is the complete set of pairs; id_tiers may be missing a
            -- pair entirely if it shares no identifiers at all.
            SELECT
                a_key, b_key,
                CASE WHEN {dialect.array_size('token_union')} > 0
                     AND CAST({dialect.array_size('token_intersection')} AS DOUBLE)
                         / {dialect.array_size('token_union')} >= {jaccard_threshold}
                    THEN 1 ELSE 0 END AS medium_name,
                CASE WHEN dob_a IS NOT NULL AND dob_b IS NOT NULL
                     AND dob_precision_a = {dob_precision} AND dob_precision_b = {dob_precision}
                     AND dob_a = dob_b
                    THEN 1 ELSE 0 END AS medium_dob,
                CASE WHEN dob_a IS NOT NULL AND dob_b IS NOT NULL
                     AND dob_precision_a = {dob_precision} AND dob_precision_b = {dob_precision}
                     AND dob_a != dob_b
                    THEN 1 ELSE 0 END AS veto_dob
            FROM pair_name_dob_evidence
        ),
        combined AS (
            SELECT
                n.a_key, n.b_key,
                COALESCE(i.strong_count, 0) AS strong_count,
                COALESCE(i.veto_document, 0) AS veto_document,
                n.medium_name, n.medium_dob, n.veto_dob
            FROM name_dob_tiers n
            LEFT JOIN id_tiers i ON n.a_key = i.a_key AND n.b_key = i.b_key
        )
        SELECT
            a_key, b_key, strong_count, medium_name, medium_dob,
            (veto_document = 1 OR veto_dob = 1) AS has_veto,
            CASE
                -- Precedence MUST match engine.scoring.tiers.decide()
                -- exactly: veto first (absolute, no override), then
                -- strong-only auto-link, then strong+name auto-link,
                -- then strong-only review, then name+dob review, else
                -- reject. Re-run test_decision_sql_parity.py after any
                -- change here.
                WHEN veto_document = 1 OR veto_dob = 1 THEN 'REJECT'
                WHEN strong_count >= {scoring.auto_link_min_strong_only} THEN 'AUTO_LINK'
                WHEN strong_count >= {scoring.auto_link_min_strong_with_name} AND medium_name = 1 THEN 'AUTO_LINK'
                WHEN strong_count >= {scoring.review_min_strong} THEN 'REVIEW'
                WHEN medium_name = 1 AND medium_dob = 1 THEN 'REVIEW'
                ELSE 'REJECT'
            END AS decision,
            CASE
                WHEN veto_document = 1 OR veto_dob = 1 THEN 0.20
                WHEN strong_count >= {scoring.auto_link_min_strong_only} THEN 0.99
                WHEN strong_count >= {scoring.auto_link_min_strong_with_name} AND medium_name = 1 THEN 0.99
                WHEN strong_count >= {scoring.review_min_strong} THEN 0.65
                WHEN medium_name = 1 AND medium_dob = 1 THEN 0.65
                ELSE 0.20
            END AS score
        FROM combined
    """
    return dialect.create_or_replace_table(table_name, select_sql)


def decision_summary_sql(table_name: str = "pair_decisions") -> str:
    """Aggregate counts by decision -- used by the /runs/{id}/redecide endpoint's instant feedback."""
    return f"""
        SELECT decision, COUNT(*) AS n
        FROM {table_name}
        GROUP BY decision
    """
