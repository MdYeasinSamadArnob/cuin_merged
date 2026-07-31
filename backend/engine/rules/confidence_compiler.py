"""
CUIN v2 - Confidence Decision Compiler (Stage 2 of the banker-rule-engine migration)

SQL twin of engine.scoring.confidence.score_pair() -- the replacement
for engine.rules.decision_compiler.compile_decision_sql(). Compiles a
MatchRuleset into one `CREATE OR REPLACE TABLE pair_decisions AS ...`
statement over the SAME pair_identifier_evidence / pair_name_dob_evidence
tables the legacy compiler reads, through the same
engine.ports.dialect.SqlDialect seam so it runs unchanged on DuckDB
and Doris.

Driven from `candidate_pairs` (LEFT JOIN onto both evidence tables),
not from pair_name_dob_evidence as the legacy compiler was -- a
forward-compatible correctness improvement, not required by today's
evidence shape (pair_name_dob_evidence is still guaranteed exactly one
row per pair until Stage 3's unpivot), but avoids re-introducing the
same "driving table" assumption Stage 3 has to undo later.

Proven equivalent to engine.scoring.confidence.score_pair() by
tests/unit/test_confidence_sql_parity.py, which runs the SAME
MatchRuleset through both and diffs `pair_decisions` row-for-row.
"""

from engine.ports.dialect import SqlDialect
from engine.rules.match_rules import (
    MatchRuleset, ATTRIBUTE_RAW_COLUMN,
    AGGREGATION_PER_SUB_TYPE, VETO_BOTH_PRESENT_NO_OVERLAP, VETO_BOTH_QUALIFIED_AND_DIFFER,
)
from engine.rules.comparators import get_comparator

# Kept in sync with engine.scoring.confidence._ARRAY_COMPARATORS --
# see that module's comment for why the veto-kind split matters.
_ARRAY_COMPARATORS = frozenset({"set_intersect", "token_jaccard", "token_containment"})

_IDENTIFIER_ID_TYPE = {
    "DOCUMENT": "document", "MOBILE": "mobile", "EMAIL": "email", "FULL_ADDRESS": "address",
}


def _identifier_confidence_cte(rule, dialect: SqlDialect, alias: str, include_detail: bool = False) -> str:
    id_type = _IDENTIFIER_ID_TYPE[rule.attribute]
    sub_type_filter = f"AND doc_type = {dialect.quote_str(rule.sub_type)}" if rule.sub_type else ""
    matched = f"id_type = {dialect.quote_str(id_type)} {sub_type_filter} AND {dialect.array_size('intersection')} > 0"

    if rule.aggregation == AGGREGATION_PER_SUB_TYPE:
        confidence_expr = f"SUM(CASE WHEN {matched} THEN {rule.confidence_pct} ELSE 0 END)"
    else:
        confidence_expr = f"MAX(CASE WHEN {matched} THEN {rule.confidence_pct} ELSE 0 END)"

    veto_expr = "0"
    if rule.veto_kind == VETO_BOTH_PRESENT_NO_OVERLAP:
        both_present = f"{dialect.array_size('values_a')} > 0 AND {dialect.array_size('values_b')} > 0"
        no_overlap = f"{dialect.array_size('intersection')} = 0"
        veto_where = f"id_type = {dialect.quote_str(id_type)} {sub_type_filter} AND {both_present} AND {no_overlap}"
        veto_expr = f"MAX(CASE WHEN {veto_where} THEN 1 ELSE 0 END)"

    # Detail: one representative matched value (AGGREGATION_PER_SUB_TYPE
    # can match more than one sub_type, e.g. NID AND TIN -- MAX picks
    # one deterministically rather than needing an array-of-strings
    # aggregate whose NULL-skipping behavior isn't worth relying on
    # across both engines for what's only ever a UI display string).
    # For the workbench breakdown panel only, never read by
    # compile_confidence_sql's decision path.
    detail_col = ""
    if include_detail:
        one_value = dialect.array_element("intersection", 1)
        detail_col = f""",
                MAX(CASE WHEN {matched} THEN {one_value} ELSE NULL END) AS detail"""

    return f"""
        {alias} AS (
            SELECT a_key, b_key,
                {confidence_expr} AS confidence,
                {veto_expr} AS veto{detail_col}
            FROM pair_identifier_evidence
            GROUP BY a_key, b_key
        )
    """


def _name_confidence_cte(rule, dialect: SqlDialect, alias: str, include_detail: bool = False) -> str:
    threshold = rule.params.get("min", 1.0)
    union_size = dialect.array_size("token_union")
    inter_size = dialect.array_size("token_intersection")
    matched = f"{union_size} > 0 AND CAST({inter_size} AS DOUBLE) / {union_size} >= {threshold}"
    detail_col = f""",
                CASE WHEN {matched} THEN {dialect.array_to_string('token_intersection', ',')} END AS detail""" if include_detail else ""
    return f"""
        {alias} AS (
            SELECT a_key, b_key,
                CASE WHEN {matched} THEN {rule.confidence_pct} ELSE 0 END AS confidence,
                0 AS veto{detail_col}
            FROM pair_name_dob_evidence
        )
    """


def _dob_confidence_cte(rule, dialect: SqlDialect, alias: str, include_detail: bool = False) -> str:
    require_qualifier = rule.params.get("require_qualifier")
    both_sides = rule.params.get("both_sides", True)

    present = "dob_a IS NOT NULL AND dob_b IS NOT NULL" if both_sides else "TRUE"
    qualified = (
        f"dob_precision_a = {dialect.quote_str(require_qualifier)} AND dob_precision_b = {dialect.quote_str(require_qualifier)}"
        if require_qualifier else "TRUE"
    )
    matched = f"{present} AND {qualified} AND dob_a = dob_b"

    veto_expr = "0"
    if rule.veto_kind == VETO_BOTH_QUALIFIED_AND_DIFFER:
        veto_expr = f"CASE WHEN {present} AND {qualified} AND dob_a != dob_b THEN 1 ELSE 0 END"

    detail_col = f""",
                CASE WHEN {matched} THEN dob_a ELSE NULL END AS detail""" if include_detail else ""

    return f"""
        {alias} AS (
            SELECT a_key, b_key,
                CASE WHEN {matched} THEN {rule.confidence_pct} ELSE 0 END AS confidence,
                {veto_expr} AS veto{detail_col}
            FROM pair_name_dob_evidence
        )
    """


def _raw_column_confidence_cte(rule, dialect: SqlDialect, alias: str, include_detail: bool = False) -> str:
    """
    SQL twin of engine.scoring.confidence._score_raw_column_rule for a
    bank-added RAW_COLUMN match rule (any schema field). Self-joins
    `raw` directly by CUSTOMER_CODE for a scalar column (the OLTP
    export has one row per customer); for an array/multi-valued
    column, first collapses each customer's values into one
    normalized, deduped array via the same UNNEST-lateral pattern
    engine.rules.compiler._compile_raw_column uses for blocking, then
    joins that. `rule.params["column"]` is trusted pre-validated safe
    by MatchRule.validate()'s regex check -- this function does not
    re-check it, matching _compile_raw_column's identical trust
    boundary on the blocking side.
    """
    column = rule.params["column"]
    is_array = bool(rule.params.get("is_array"))
    comparator = get_comparator(rule.comparator)
    if comparator is None or comparator.sql is None:
        raise ValueError(f"{rule.rule_id}: comparator {rule.comparator!r} has no SQL implementation")

    # Array-valued custom fields are the uncommon case (most
    # bank-added fields -- mother's name, cousin's name -- are
    # scalar). Unlike _agg_identifiers (engine.scoring.evidence_dialect),
    # this aggregation stays an inline CTE rather than a materialized
    # table even though it's joined twice (as a and b) -- both engines
    # generally materialize a multiply-referenced CTE internally, and
    # adding a real setup-statement phase to compile_confidence_sql's
    # single-statement contract is a larger change not justified for
    # what should be a rare path.
    agg_cte = ""
    if is_array:
        agg_alias = f"_raw_agg_{alias}"
        elem_ref = dialect.unnest_column_ref("v")
        norm_expr = f"UPPER(TRIM(CAST({elem_ref} AS VARCHAR)))"
        agg_cte = f"""
            {agg_alias} AS (
                SELECT CUSTOMER_CODE AS customer_code, {dialect.collect_distinct_sorted(norm_expr)} AS vals
                FROM {dialect.unnest_lateral('raw', column, 'v')}
                WHERE {elem_ref} IS NOT NULL AND {norm_expr} != {dialect.quote_str('')}
                GROUP BY CUSTOMER_CODE
            ),
        """
        a_expr, b_expr = "a.vals", "b.vals"
        both_present = f"a.vals IS NOT NULL AND {dialect.array_size('a.vals')} > 0 AND b.vals IS NOT NULL AND {dialect.array_size('b.vals')} > 0"
        from_clause = f"FROM candidate_pairs cp LEFT JOIN {agg_alias} a ON cp.a_key = a.customer_code LEFT JOIN {agg_alias} b ON cp.b_key = b.customer_code"
    else:
        norm_a = f"UPPER(TRIM(CAST(a.{column} AS VARCHAR)))"
        norm_b = f"UPPER(TRIM(CAST(b.{column} AS VARCHAR)))"
        both_present = f"{norm_a} IS NOT NULL AND {norm_a} != {dialect.quote_str('')} AND {norm_b} IS NOT NULL AND {norm_b} != {dialect.quote_str('')}"
        if rule.comparator in _ARRAY_COMPARATORS:  # token_jaccard/token_containment on scalar text
            a_expr, b_expr = dialect.str_split(norm_a, " "), dialect.str_split(norm_b, " ")
        else:
            a_expr, b_expr = norm_a, norm_b
        from_clause = f"FROM candidate_pairs cp LEFT JOIN raw a ON cp.a_key = a.CUSTOMER_CODE LEFT JOIN raw b ON cp.b_key = b.CUSTOMER_CODE"

    matched_expr = comparator.sql(a_expr, b_expr, rule.params, dialect)
    if rule.comparator == "token_jaccard":
        threshold = rule.params.get("threshold", 0.5)
        matched_expr = f"({matched_expr} >= {threshold})"

    veto_expr = "0"
    if rule.veto_kind == VETO_BOTH_PRESENT_NO_OVERLAP and rule.comparator in _ARRAY_COMPARATORS:
        no_overlap = f"{dialect.array_size(dialect.array_intersect(a_expr, b_expr))} = 0"
        veto_expr = f"CASE WHEN {both_present} AND {no_overlap} THEN 1 ELSE 0 END"
    elif rule.veto_kind == VETO_BOTH_QUALIFIED_AND_DIFFER and rule.comparator not in _ARRAY_COMPARATORS:
        veto_expr = f"CASE WHEN {both_present} AND NOT {matched_expr} THEN 1 ELSE 0 END"

    detail_col = ""
    if include_detail:
        detail_expr = dialect.array_to_string(a_expr, ",") if (is_array or rule.comparator in _ARRAY_COMPARATORS) else a_expr
        detail_col = f",\n                CASE WHEN {both_present} AND {matched_expr} THEN {detail_expr} ELSE NULL END AS detail"

    return f"""
        {agg_cte}
        {alias} AS (
            SELECT cp.a_key, cp.b_key,
                CASE WHEN {both_present} AND {matched_expr} THEN {rule.confidence_pct} ELSE 0 END AS confidence,
                {veto_expr} AS veto{detail_col}
            {from_clause}
        )
    """


def compile_confidence_sql(ruleset: MatchRuleset, dialect: SqlDialect, table_name: str = "pair_decisions") -> str:
    ctes = []
    aliases = []

    for i, rule in enumerate(ruleset.match_rules):
        if not rule.enabled:
            continue
        alias = f"_rule_{i}"
        aliases.append(alias)
        if rule.attribute in _IDENTIFIER_ID_TYPE:
            ctes.append(_identifier_confidence_cte(rule, dialect, alias))
        elif rule.attribute == "NAME":
            ctes.append(_name_confidence_cte(rule, dialect, alias))
        elif rule.attribute == "BIRTH_DATE":
            ctes.append(_dob_confidence_cte(rule, dialect, alias))
        elif rule.attribute == ATTRIBUTE_RAW_COLUMN:
            ctes.append(_raw_column_confidence_cte(rule, dialect, alias))
        else:
            raise ValueError(f"{rule.rule_id}: unknown attribute {rule.attribute!r}")

    if not aliases:
        raise ValueError("MatchRuleset has no enabled match rules")

    confidence_sum = " + ".join(f"COALESCE({a}.confidence, 0)" for a in aliases)
    veto_or = " OR ".join(f"COALESCE({a}.veto, 0) = 1" for a in aliases)
    joins = "\n".join(
        f"LEFT JOIN {a} ON cp.a_key = {a}.a_key AND cp.b_key = {a}.b_key" for a in aliases
    )
    confidence_capped = f"LEAST({ruleset.confidence_cap}, {confidence_sum})"

    select_sql = f"""
        WITH {",".join(ctes)}
        SELECT
            cp.a_key, cp.b_key,
            {confidence_capped} AS confidence_pct,
            ({veto_or}) AS has_veto,
            CASE
                WHEN {veto_or} THEN 'REJECT'
                WHEN {confidence_capped} >= {ruleset.auto_link_min_confidence} THEN 'AUTO_LINK'
                WHEN {confidence_capped} >= {ruleset.review_min_confidence} THEN 'REVIEW'
                ELSE 'REJECT'
            END AS decision,
            {confidence_capped} / 100.0 AS score
        FROM candidate_pairs cp
        {joins}
    """
    return dialect.create_or_replace_table(table_name, select_sql)


def compile_contributions_sql(ruleset: MatchRuleset, dialect: SqlDialect, table_name: str = "pair_contributions") -> str:
    """
    Stage 1 of the entity resolution workbench plan -- the "why this
    score" audit table. Reuses the EXACT same per-rule CTE builders
    compile_confidence_sql() uses (with include_detail=True added),
    so this can never disagree with what a real run actually decided:
    same SQL, same joins, same veto/matched predicates -- just
    unpivoted to one row per (pair, rule) instead of summed into one
    row per pair. compile_confidence_sql() itself is untouched and
    this function is never called from that decision path, so it adds
    zero risk to output_fingerprint.

    One row per (a_key, b_key, rule_id) for every enabled rule,
    including non-matching ones (matched=false rows are what let the
    breakdown panel show "did not match" alongside "matched") --
    mirrors engine.scoring.confidence.score_pair()'s Contribution
    list, which already emits one entry per enabled rule for the same
    reason (see that module's docstring).

    `awarded_pct` vs `configured_pct`: for an AGGREGATION_PER_SUB_TYPE
    rule (DOCUMENT: NID + TIN both counted), awarded_pct can be a
    MULTIPLE of configured_pct (e.g. 100 awarded from a 50-point rule
    when both sub_types matched) -- that ratio IS the "fired twice"
    signal the UI renders, not a bug to normalize away.
    """
    ctes = []
    selects = []

    for i, rule in enumerate(ruleset.match_rules):
        if not rule.enabled:
            continue
        alias = f"_rule_{i}"
        if rule.attribute in _IDENTIFIER_ID_TYPE:
            ctes.append(_identifier_confidence_cte(rule, dialect, alias, include_detail=True))
        elif rule.attribute == "NAME":
            ctes.append(_name_confidence_cte(rule, dialect, alias, include_detail=True))
        elif rule.attribute == "BIRTH_DATE":
            ctes.append(_dob_confidence_cte(rule, dialect, alias, include_detail=True))
        elif rule.attribute == ATTRIBUTE_RAW_COLUMN:
            ctes.append(_raw_column_confidence_cte(rule, dialect, alias, include_detail=True))
        else:
            raise ValueError(f"{rule.rule_id}: unknown attribute {rule.attribute!r}")

        awarded = f"COALESCE({alias}.confidence, 0)"
        selects.append(f"""
            SELECT
                {dialect.quote_str(rule.rule_id)} AS rule_id,
                {i} AS ordinal,
                {dialect.quote_str(rule.label or rule.attribute)} AS label,
                {dialect.quote_str(rule.attribute)} AS attribute,
                {dialect.quote_str(rule.sub_type) if rule.sub_type else 'NULL'} AS sub_type,
                cp.a_key, cp.b_key,
                {awarded} AS awarded_pct,
                {rule.confidence_pct} AS configured_pct,
                ({awarded} > 0) AS matched,
                (COALESCE({alias}.veto, 0) = 1) AS is_veto,
                {alias}.detail AS detail
            FROM candidate_pairs cp
            LEFT JOIN {alias} ON cp.a_key = {alias}.a_key AND cp.b_key = {alias}.b_key
        """)

    if not selects:
        raise ValueError("MatchRuleset has no enabled match rules")

    select_sql = f"WITH {','.join(ctes)} " + " UNION ALL ".join(selects)
    return dialect.create_or_replace_table(table_name, select_sql)
