"""
CUIN v2 - Evidence Building, dialect-driven (Ruleset v2, Layer 4)

Dialect-portable twin of engine.scoring.evidence.build_pair_evidence --
same set-intersection logic, same materialization boundary (see
below), translated through engine.ports.dialect.SqlDialect.
engine.scoring.evidence itself is untouched; see
engine.normalize.explode_dialect's docstring for why this is a sibling
module.

This version always emits `values_`, `token_intersection`, and
`token_union` SORTED (dialect.collect_distinct_sorted /
array_intersect+array_sort). engine.scoring.evidence.py was updated to
match (list_sort() added to its `_agg_identifiers`/intersection/union
expressions) after this session found the unsorted form was NOT purely
cosmetic: array order leaks into engine.scoring.tiers.classify()'s
signal strings, which engine.determinism.fingerprint_edges hashes --
so unsorted-vs-sorted produced a genuinely different
output_fingerprint between engines despite identical decisions. Both
modules are now canonically sorted and
tests/unit/test_evidence_dialect_parity.py asserts exact (not
order-insensitive) equality.

Preserves the original's critical performance note: `_agg_identifiers`
is a real materialized TABLE, not an inline CTE, because it's joined
twice (once as `a`, once as `b`) -- without materialization the
optimizer has no cardinality estimate for the aggregated relation.
"""


def build_pair_evidence(con, dialect) -> None:
    agg_sql = f"""
        SELECT customer_code, id_type, doc_type, {dialect.collect_distinct_sorted('value_norm')} AS values_
        FROM identifiers
        WHERE is_valid AND NOT is_suppressed
        GROUP BY customer_code, id_type, doc_type
    """
    for stmt in dialect.create_or_replace_table("_agg_identifiers", agg_sql).split(";\n"):
        stmt = stmt.strip()
        if stmt:
            con.execute(stmt)

    id_evidence_sql = f"""
        SELECT
            p.a_key, p.b_key, a.id_type, a.doc_type,
            a.values_ AS values_a, b.values_ AS values_b,
            {dialect.array_sort(dialect.array_intersect('a.values_', 'b.values_'))} AS intersection
        FROM candidate_pairs p
        JOIN _agg_identifiers a ON a.customer_code = p.a_key
        JOIN _agg_identifiers b ON b.customer_code = p.b_key AND b.id_type = a.id_type
            AND (a.id_type != 'document' OR a.doc_type = b.doc_type)
    """
    for stmt in dialect.create_or_replace_table("pair_identifier_evidence", id_evidence_sql).split(";\n"):
        stmt = stmt.strip()
        if stmt:
            con.execute(stmt)

    name_dob_sql = f"""
        SELECT
            p.a_key, p.b_key,
            sa.name_norm AS name_a, sb.name_norm AS name_b,
            sa.name_tokens AS tokens_a, sb.name_tokens AS tokens_b,
            {dialect.array_sort(dialect.array_intersect('sa.name_tokens', 'sb.name_tokens'))} AS token_intersection,
            {dialect.array_union_distinct('sa.name_tokens', 'sb.name_tokens')} AS token_union,
            sa.dob_iso AS dob_a, sb.dob_iso AS dob_b,
            sa.dob_precision AS dob_precision_a, sb.dob_precision AS dob_precision_b
        FROM candidate_pairs p
        JOIN customer_scalars sa ON sa.customer_code = p.a_key
        JOIN customer_scalars sb ON sb.customer_code = p.b_key
    """
    for stmt in dialect.create_or_replace_table("pair_name_dob_evidence", name_dob_sql).split(";\n"):
        stmt = stmt.strip()
        if stmt:
            con.execute(stmt)
