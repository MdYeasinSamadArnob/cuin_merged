"""
CUIN v2 - Deterministic Blocking (Ruleset v2, Layer 3)

Self-joins the `identifiers` and `customer_scalars` tables (built by
engine.normalize.explode) on validated, non-suppressed keys to produce
candidate pairs. Exact-key only — no sampling, no `.limit()` truncation
(the old run_blocking() called `pairs_df.limit(max_total_pairs)` on an
unordered distributed DataFrame, which is itself a source of
non-determinism independent of Splink's EM training). Oversized blocks
are already neutralized by frequency suppression (Layer 2), so no cap
is needed here.

Reuses the CandidatePair provenance idea from engine.blocking (the
`blocking_reasons: List[str]` field) — populated here directly from
`list_sort(list(DISTINCT id_type||':'||value_norm))` so every pair
carries an auditable reason.
"""

import duckdb


def build_candidate_pairs(con: duckdb.DuckDBPyConnection) -> None:
    """
    Requires `identifiers` (engine.normalize.explode) and
    `customer_scalars` already built on `con`. Produces `candidate_pairs`
    with columns: a_key, b_key, blocking_reasons (VARCHAR[]).

    a_key < b_key always (ordered pair, matches db/schema.sql's
    `CONSTRAINT ordered_pair CHECK (a_key < b_key)`), and DISTINCT pairs
    only — a pair sharing 3 identifiers appears once, not 3 times.
    """
    con.execute("""
        CREATE OR REPLACE TABLE candidate_pairs AS
        WITH atomic_pairs AS (
            -- Exact-match blocking on validated, non-suppressed identifiers.
            -- 'address' is deliberately NOT a blocking key: engine.scoring.tiers
            -- never consults address evidence for AUTO_LINK or REVIEW (it is
            -- WEAK-tier, display-only corroboration), so an address-only pair
            -- always REJECTs regardless -- generating candidates from it is
            -- pure wasted compute. Any pair that ALSO shares mobile/email/
            -- document/name+dob is still generated via that block, and
            -- evidence.py independently attaches address evidence (if any)
            -- to whatever pairs already exist, so no audit information is lost.
            SELECT
                LEAST(a.customer_code, b.customer_code) AS a_key,
                GREATEST(a.customer_code, b.customer_code) AS b_key,
                a.id_type || ':' || a.value_norm AS reason
            FROM identifiers a
            JOIN identifiers b
                ON a.id_type = b.id_type
                AND a.value_norm = b.value_norm
                AND a.customer_code < b.customer_code
            WHERE a.is_valid AND b.is_valid
                AND NOT a.is_suppressed AND NOT b.is_suppressed
                AND a.id_type != 'address'

            UNION ALL

            -- name_key + FULL-precision dob_iso composite key: requires >=2
            -- rare name tokens (guards against "MD SAIFUL ISLAM" x1,382
            -- forming a mega-block on name alone) AND an exact populated
            -- DOB (not a year-only stub). Blocking on the FULL date, not
            -- just dob_year, matches exactly what tiers.classify()'s
            -- medium_dob requires (full-precision DOB equality) -- a name+
            -- dob_year-only block would generate large numbers of pairs
            -- that can never satisfy medium_dob and are therefore
            -- guaranteed to REJECT, which is wasted compute at scale.
            SELECT
                LEAST(a.customer_code, b.customer_code) AS a_key,
                GREATEST(a.customer_code, b.customer_code) AS b_key,
                'name_dob:' || a.name_key || '|' || a.dob_iso AS reason
            FROM customer_name_keys a
            JOIN customer_name_keys b
                ON a.name_key = b.name_key
                AND a.dob_iso = b.dob_iso
                AND a.customer_code < b.customer_code
            WHERE len(a.rare_tokens) >= 2
                AND len(b.rare_tokens) >= 2
        )
        SELECT
            a_key,
            b_key,
            list_sort(list(DISTINCT reason)) AS blocking_reasons
        FROM atomic_pairs
        GROUP BY a_key, b_key
        ORDER BY a_key, b_key
    """)


def build_name_dob_keys(
    con: duckdb.DuckDBPyConnection,
    name_token_max_records: int = 1500,
    name_dob_block_max_records: int = 20,
) -> None:
    """
    Builds `customer_name_keys`: a per-customer composite blocking key
    of (name_key = hash of full rare-token set, dob_year).

    Suppression is applied at TWO granularities, and the composite-block
    check is the one that actually matters:

    1. name_token_max_records: the atomic name_key's global frequency
       (across all DOBs combined) -- catches a name so common it
       shouldn't seed a block at all.
    2. name_dob_block_max_records: the size of the actual JOINED block
       -- i.e. how many customers share this EXACT (name_key, dob_iso)
       pair. This is the one that matters at scale: a name_key can sit
       comfortably under a global cap while still forming large blocks
       when joined against a coarse key. Capping the ACTUAL block size
       at the same order as other identifier types (20) closes that gap
       directly, rather than only bounding the wrong (atomic, pre-join)
       quantity.

    Only FULL-precision DOBs (real day/month, not a Jan-1 "year known
    only" stub) are included -- the blocking key is full dob_iso, not
    dob_year, so it is exactly as selective as tiers.classify()'s
    medium_dob check requires. Blocking on dob_year alone (a coarser
    key) generates large numbers of candidate pairs that can never
    satisfy medium_dob and are therefore guaranteed to REJECT: on the
    full 1.5M-row dataset, that coarser key alone produced ~3.6M
    candidate pairs where the vast majority were compute spent on
    pairs with no path to AUTO_LINK or REVIEW.
    """
    con.execute("""
        CREATE OR REPLACE TABLE customer_name_keys AS
        SELECT
            customer_code,
            name_tokens AS rare_tokens,
            md5(array_to_string(list_sort(name_tokens), '|')) AS name_key,
            dob_iso
        FROM customer_scalars
        WHERE name_tokens IS NOT NULL AND len(name_tokens) > 0
            AND dob_iso IS NOT NULL
            AND dob_precision = 'FULL'
    """)

    con.execute(f"""
        CREATE OR REPLACE TABLE name_key_frequency AS
        SELECT name_key, COUNT(DISTINCT customer_code) AS n_records
        FROM customer_name_keys
        GROUP BY name_key
    """)

    con.execute(f"""
        CREATE OR REPLACE TABLE name_dob_block_frequency AS
        SELECT name_key, dob_iso, COUNT(DISTINCT customer_code) AS n_records
        FROM customer_name_keys
        GROUP BY name_key, dob_iso
    """)

    con.execute(f"""
        CREATE OR REPLACE TABLE customer_name_keys AS
        SELECT k.*
        FROM customer_name_keys k
        JOIN name_key_frequency f ON k.name_key = f.name_key
        JOIN name_dob_block_frequency b ON k.name_key = b.name_key AND k.dob_iso = b.dob_iso
        WHERE f.n_records <= {name_token_max_records}
            AND b.n_records <= {name_dob_block_max_records}
    """)
