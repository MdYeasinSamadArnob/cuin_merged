"""
CUIN v2 - Blocking Rule Compiler

Compiles an ordered engine.rules.catalog.BlockingRule list into SQL
executed through an engine.ports.dialect.SqlDialect (currently only
DorisDialect), producing the `candidate_pairs(a_key, b_key,
blocking_reasons)` table the pipeline blocks on. Originally verified
to reproduce a hand-written DuckDB predecessor query's *output*
exactly (not necessarily the same SQL text) during the DuckDB-to-Doris
migration; that predecessor and its comparison test have since been
removed now that Doris is the only engine.

Each BlockingRuleType has one `_compile_<type>` function here,
returning `(setup_statements, atomic_select_sql)`:
  - `setup_statements`: SQL statements executed first, building any
    per-rule staging table (frequency caps, derived key columns).
  - `atomic_select_sql`: a SELECT of (a_key, b_key, reason) for this
    rule alone. All enabled rules' atomic selects are UNION ALL'd and
    grouped into the final candidate_pairs table by compile_and_build.

Rule input is never interpolated as free text -- `BlockingRule.
validate()` (called before compiling) restricts rule_id to a safe
slug and every field/param to the closed ALLOWED_FIELDS vocabulary in
engine.rules.catalog, so there is no SQL injection surface even though
the catalog is UI-edited.
"""

from dataclasses import dataclass
from typing import List, Optional, Tuple

from engine.ports.dialect import SqlDialect
from engine.rules.catalog import BlockingRule, BlockingRuleType


@dataclass(frozen=True)
class KeySource:
    """
    Where the FINAL (post-guard) blocking key for a rule lives, after
    its setup_statements have run -- the table the self-join reads
    both sides from, and the column(s) that form the key. `None` for
    EXACT_IDENTIFIER, which has no single staging table (it self-joins
    `identifiers` directly with a compound id_type+value_norm key);
    engine.rules.precheck handles that type via identifier_frequency
    instead of this.

    Exists so engine.rules.precheck can compute exact fan-out
    (SUM(n*(n-1)/2) per key) WITHOUT re-deriving each rule type's
    staging-table naming convention -- it reuses exactly the table/
    columns the self-join itself will read.
    """
    table: str
    key_columns: Tuple[str, ...]


def _rule_table(rule: BlockingRule, suffix: str) -> str:
    return f"_rule_{rule.rule_id}_{suffix}"


def _derived_field_expr(field: str, dialect: SqlDialect) -> str:
    """
    `name_key` is not a literal customer_scalars column -- it's
    md5(sorted rare name tokens), mirroring
    engine.blocking.deterministic_blocker.build_name_dob_keys exactly.
    Every other field name in ALLOWED_FIELDS is a literal column.
    """
    if field == "name_key":
        return dialect.md5(dialect.array_to_string(dialect.array_sort("name_tokens"), "|"))
    return field


def _field_not_null_predicate(field: str, dialect: SqlDialect) -> str:
    if field == "name_key":
        return f"name_tokens IS NOT NULL AND {dialect.array_size('name_tokens')} > 0"
    return f"{field} IS NOT NULL"


# ----------------------------------------------------------------------
# EXACT_IDENTIFIER -- mirrors deterministic_blocker's pass 1
# ----------------------------------------------------------------------
def _compile_exact_identifier(rule: BlockingRule, dialect: SqlDialect) -> Tuple[List[str], str, Optional[KeySource]]:
    id_types_sql = ", ".join(dialect.quote_str(t) for t in rule.fields)

    guard_join, guard_where = "", ""
    caps = [v for v in (rule.guards.max_block_size, rule.guards.max_key_frequency) if v]
    if caps:
        # identifiers_frequency already holds the global per-value
        # record count (built by engine.blocking.suppression); this
        # rule's guard can only ever TIGHTEN that global cap further,
        # so reuse it rather than recomputing -- take the stricter
        # (smaller) of the two guard concepts since EXACT_IDENTIFIER
        # has no atomic-vs-joined-block distinction (the value IS the
        # key, so "how many records share this value" already means
        # both things at once).
        cap = min(caps)
        guard_join = (
            "JOIN identifier_frequency fa ON fa.id_type = a.id_type AND fa.value_norm = a.value_norm\n"
            "JOIN identifier_frequency fb ON fb.id_type = b.id_type AND fb.value_norm = b.value_norm\n"
        )
        guard_where = f"AND fa.n_records <= {cap} AND fb.n_records <= {cap}\n"

    select_sql = f"""
        SELECT
            {dialect.least('a.customer_code', 'b.customer_code')} AS a_key,
            {dialect.greatest('a.customer_code', 'b.customer_code')} AS b_key,
            {dialect.concat('a.id_type', dialect.quote_str(':'), 'a.value_norm')} AS reason
        FROM identifiers a
        JOIN identifiers b
            ON a.id_type = b.id_type AND a.value_norm = b.value_norm AND a.customer_code < b.customer_code
        {guard_join}
        WHERE a.is_valid AND b.is_valid AND NOT a.is_suppressed AND NOT b.is_suppressed
            AND a.id_type IN ({id_types_sql})
        {guard_where}
    """
    return [], select_sql, None


# ----------------------------------------------------------------------
# COMPOSITE_KEY -- mirrors deterministic_blocker's pass 2 (name+dob)
# ----------------------------------------------------------------------
def _compile_composite_key(rule: BlockingRule, dialect: SqlDialect) -> Tuple[List[str], str, KeySource]:
    if len(rule.fields) != 2:
        raise ValueError(f"{rule.rule_id}: composite_key compiler requires exactly 2 fields")
    f0, f1 = rule.fields
    # A staging table is never rebuilt FROM ITSELF under a new name --
    # DuckDB's CREATE OR REPLACE evaluates the SELECT against the old
    # table before swapping, but Doris's create_or_replace_table compiles
    # to DROP TABLE; CREATE TABLE ... AS SELECT, so a self-referencing
    # rebuild would read from a table it just dropped. Each guard stage
    # below gets its own uniquely-named table instead (verified live
    # against Doris -- the self-reference form fails with "table does
    # not exist").
    raw_table = _rule_table(rule, "keys_raw")
    keys_table = _rule_table(rule, "keys") if (rule.guards.max_key_frequency or rule.guards.max_block_size or rule.guards.min_key_parts) else raw_table

    where_parts = [_field_not_null_predicate(f0, dialect), _field_not_null_predicate(f1, dialect)]
    require_precision = rule.params.get("require_dob_precision")
    if require_precision and f1 == "dob_iso":
        where_parts.append(f"dob_precision = {dialect.quote_str(require_precision)}")

    setup = [dialect.create_or_replace_table(raw_table, f"""
        SELECT customer_code,
            {_derived_field_expr(f0, dialect)} AS {f0},
            {_derived_field_expr(f1, dialect)} AS {f1},
            name_tokens AS _key_parts
        FROM customer_scalars
        WHERE {' AND '.join(where_parts)}
    """)]

    joins, wheres = [], []
    if rule.guards.max_key_frequency:
        freq_table = _rule_table(rule, "key_freq")
        setup.append(dialect.create_or_replace_table(freq_table, f"""
            SELECT {f0}, COUNT(DISTINCT customer_code) AS n_records
            FROM {raw_table} GROUP BY {f0}
        """))
        joins.append(f"JOIN {freq_table} kf ON k.{f0} = kf.{f0}")
        wheres.append(f"kf.n_records <= {rule.guards.max_key_frequency}")
    if rule.guards.max_block_size:
        block_table = _rule_table(rule, "block_freq")
        setup.append(dialect.create_or_replace_table(block_table, f"""
            SELECT {f0}, {f1}, COUNT(DISTINCT customer_code) AS n_records
            FROM {raw_table} GROUP BY {f0}, {f1}
        """))
        joins.append(f"JOIN {block_table} bf ON k.{f0} = bf.{f0} AND k.{f1} = bf.{f1}")
        wheres.append(f"bf.n_records <= {rule.guards.max_block_size}")
    if rule.guards.min_key_parts:
        wheres.append(f"{dialect.array_size('k._key_parts')} >= {rule.guards.min_key_parts}")

    if joins or wheres:
        setup.append(dialect.create_or_replace_table(keys_table, f"""
            SELECT k.customer_code, k.{f0}, k.{f1}, k._key_parts
            FROM {raw_table} k
            {' '.join(joins)}
            WHERE {' AND '.join(wheres) if wheres else 'TRUE'}
        """))

    select_sql = f"""
        SELECT
            {dialect.least('a.customer_code', 'b.customer_code')} AS a_key,
            {dialect.greatest('a.customer_code', 'b.customer_code')} AS b_key,
            {dialect.concat(dialect.quote_str(rule.rule_id + ':'), f'a.{f0}', dialect.quote_str('|'), f'a.{f1}')} AS reason
        FROM {keys_table} a
        JOIN {keys_table} b
            ON a.{f0} = b.{f0} AND a.{f1} = b.{f1} AND a.customer_code < b.customer_code
    """
    return setup, select_sql, KeySource(table=keys_table, key_columns=(f0, f1))


# ----------------------------------------------------------------------
# TOKEN_KEY -- explode an array field, self-join on one element
# ----------------------------------------------------------------------
def _compile_token_key(rule: BlockingRule, dialect: SqlDialect) -> Tuple[List[str], str, KeySource]:
    array_field = rule.fields[0]
    raw_table = _rule_table(rule, "tokens_raw")
    tok_ref = dialect.unnest_column_ref("tok")

    setup = [dialect.create_or_replace_table(raw_table, f"""
        SELECT customer_code, {tok_ref} AS token
        FROM {dialect.unnest_lateral('customer_scalars', array_field, 'tok')}
        WHERE {tok_ref} IS NOT NULL AND {tok_ref} != ''
    """)]

    # Both guards cap the SAME thing here (a token's global frequency),
    # unlike EXACT_IDENTIFIER/COMPOSITE_KEY where max_block_size caps
    # the joined pair count directly -- for a single-column self-join,
    # a token appearing in N records always produces exactly N*(N-1)/2
    # pairs, so capping frequency at sqrt(2*max_block_size) bounds the
    # worst case. Take the tighter of the two, same pattern as
    # _compile_prefix_key, so a rule's "Max block size" guard isn't
    # silently ignored just because it's a TOKEN_KEY rule.
    caps = []
    if rule.guards.max_key_frequency:
        caps.append(rule.guards.max_key_frequency)
    if rule.guards.max_block_size:
        caps.append(int((2 * rule.guards.max_block_size) ** 0.5))
    if caps:
        cap = min(caps)
        tokens_table = _rule_table(rule, "tokens")
        freq_table = _rule_table(rule, "freq")
        setup.append(dialect.create_or_replace_table(freq_table, f"""
            SELECT token, COUNT(DISTINCT customer_code) AS n_records
            FROM {raw_table} GROUP BY token
        """))
        setup.append(dialect.create_or_replace_table(tokens_table, f"""
            SELECT t.customer_code, t.token FROM {raw_table} t
            JOIN {freq_table} f ON t.token = f.token
            WHERE f.n_records <= {cap}
        """))
    else:
        tokens_table = raw_table

    select_sql = f"""
        SELECT
            {dialect.least('a.customer_code', 'b.customer_code')} AS a_key,
            {dialect.greatest('a.customer_code', 'b.customer_code')} AS b_key,
            {dialect.concat(dialect.quote_str(rule.rule_id + ':'), 'a.token')} AS reason
        FROM {tokens_table} a
        JOIN {tokens_table} b ON a.token = b.token AND a.customer_code < b.customer_code
    """
    return setup, select_sql, KeySource(table=tokens_table, key_columns=("token",))


# ----------------------------------------------------------------------
# PREFIX_KEY -- self-join on the first N characters of a scalar field
# ----------------------------------------------------------------------
def _compile_prefix_key(rule: BlockingRule, dialect: SqlDialect) -> Tuple[List[str], str, KeySource]:
    field = rule.fields[0]
    prefix_len = int(rule.params["prefix_len"])
    raw_table = _rule_table(rule, "prefix_raw")

    setup = [dialect.create_or_replace_table(raw_table, f"""
        SELECT customer_code, {dialect.substr(field, 1, prefix_len)} AS pfx
        FROM customer_scalars
        WHERE {field} IS NOT NULL AND {dialect.str_len(field)} >= {prefix_len}
    """)]

    caps = [v for v in (rule.guards.max_block_size, rule.guards.max_key_frequency) if v]
    if caps:
        cap = min(caps)
        pfx_table = _rule_table(rule, "prefix")
        freq_table = _rule_table(rule, "freq")
        setup.append(dialect.create_or_replace_table(freq_table, f"""
            SELECT pfx, COUNT(DISTINCT customer_code) AS n_records FROM {raw_table} GROUP BY pfx
        """))
        setup.append(dialect.create_or_replace_table(pfx_table, f"""
            SELECT p.customer_code, p.pfx FROM {raw_table} p
            JOIN {freq_table} f ON p.pfx = f.pfx
            WHERE f.n_records <= {cap}
        """))
    else:
        pfx_table = raw_table

    select_sql = f"""
        SELECT
            {dialect.least('a.customer_code', 'b.customer_code')} AS a_key,
            {dialect.greatest('a.customer_code', 'b.customer_code')} AS b_key,
            {dialect.concat(dialect.quote_str(rule.rule_id + ':'), 'a.pfx')} AS reason
        FROM {pfx_table} a
        JOIN {pfx_table} b ON a.pfx = b.pfx AND a.customer_code < b.customer_code
    """
    return setup, select_sql, KeySource(table=pfx_table, key_columns=("pfx",))


# ----------------------------------------------------------------------
# DATE_PART_KEY -- self-join on a date part (year/month/day)
# ----------------------------------------------------------------------
def _compile_date_part_key(rule: BlockingRule, dialect: SqlDialect) -> Tuple[List[str], str, KeySource]:
    date_field, part = rule.fields
    dp_table = _rule_table(rule, "datepart")

    # date_field (e.g. dob_iso) is stored as a VARCHAR ISO date string,
    # not a native DATE column -- year()/month()/day() require an
    # explicit CAST in DuckDB (Doris coerces implicitly, but the CAST
    # is harmless and keeps both dialects on the same explicit path).
    setup = [dialect.create_or_replace_table(dp_table, f"""
        SELECT customer_code, {dialect.date_part(part, f'CAST({date_field} AS DATE)')} AS dpart
        FROM customer_scalars WHERE {date_field} IS NOT NULL
    """)]

    select_sql = f"""
        SELECT
            {dialect.least('a.customer_code', 'b.customer_code')} AS a_key,
            {dialect.greatest('a.customer_code', 'b.customer_code')} AS b_key,
            {dialect.concat(dialect.quote_str(rule.rule_id + ':'), 'CAST(a.dpart AS VARCHAR)')} AS reason
        FROM {dp_table} a
        JOIN {dp_table} b ON a.dpart = b.dpart AND a.customer_code < b.customer_code
    """
    return setup, select_sql, KeySource(table=dp_table, key_columns=("dpart",))


# ----------------------------------------------------------------------
# RAW_COLUMN -- self-join directly on a raw source column, scalar or
# multi-valued. See BlockingRuleType.RAW_COLUMN's docstring: this is
# what makes "any discovered column" reachable for blocking without
# threading it through explode's closed 6-column vocabulary first.
# ----------------------------------------------------------------------
def _compile_raw_column(rule: BlockingRule, dialect: SqlDialect) -> Tuple[List[str], str, KeySource]:
    column = rule.fields[0]  # validated safe by BlockingRule.validate()'s _RAW_COLUMN_RE
    is_array = bool(rule.params.get("is_array"))
    raw_table = _rule_table(rule, "raw_key")

    if is_array:
        elem_ref = dialect.unnest_column_ref("v")
        norm_expr = f"UPPER(TRIM(CAST({elem_ref} AS VARCHAR)))"
        setup = [dialect.create_or_replace_table(raw_table, f"""
            SELECT CUSTOMER_CODE AS customer_code, {norm_expr} AS key_value
            FROM {dialect.unnest_lateral('raw', column, 'v')}
            WHERE {elem_ref} IS NOT NULL AND {norm_expr} != {dialect.quote_str('')}
        """)]
    else:
        norm_expr = f"UPPER(TRIM(CAST({column} AS VARCHAR)))"
        setup = [dialect.create_or_replace_table(raw_table, f"""
            SELECT CUSTOMER_CODE AS customer_code, {norm_expr} AS key_value
            FROM raw
            WHERE {column} IS NOT NULL AND {norm_expr} != {dialect.quote_str('')}
        """)]

    caps = [v for v in (rule.guards.max_block_size, rule.guards.max_key_frequency) if v]
    if caps:
        cap = min(caps)
        keys_table = _rule_table(rule, "raw_keys")
        freq_table = _rule_table(rule, "raw_freq")
        setup.append(dialect.create_or_replace_table(freq_table, f"""
            SELECT key_value, COUNT(DISTINCT customer_code) AS n_records
            FROM {raw_table} GROUP BY key_value
        """))
        setup.append(dialect.create_or_replace_table(keys_table, f"""
            SELECT k.customer_code, k.key_value FROM {raw_table} k
            JOIN {freq_table} f ON k.key_value = f.key_value
            WHERE f.n_records <= {cap}
        """))
    else:
        keys_table = raw_table

    select_sql = f"""
        SELECT
            {dialect.least('a.customer_code', 'b.customer_code')} AS a_key,
            {dialect.greatest('a.customer_code', 'b.customer_code')} AS b_key,
            {dialect.concat(dialect.quote_str(rule.rule_id + ':'), 'a.key_value')} AS reason
        FROM {keys_table} a
        JOIN {keys_table} b ON a.key_value = b.key_value AND a.customer_code < b.customer_code
    """
    return setup, select_sql, KeySource(table=keys_table, key_columns=("key_value",))


_COMPILERS = {
    BlockingRuleType.EXACT_IDENTIFIER: _compile_exact_identifier,
    BlockingRuleType.COMPOSITE_KEY: _compile_composite_key,
    BlockingRuleType.TOKEN_KEY: _compile_token_key,
    BlockingRuleType.PREFIX_KEY: _compile_prefix_key,
    BlockingRuleType.DATE_PART_KEY: _compile_date_part_key,
    BlockingRuleType.RAW_COLUMN: _compile_raw_column,
}


def _execute_multi(con, sql_maybe_multi_statement: str) -> None:
    """Executes a `;\\n`-joined batch (used by DorisDialect's DROP+CREATE pattern) one statement at a time."""
    for part in sql_maybe_multi_statement.split(";\n"):
        part = part.strip()
        if part:
            con.execute(part)


def compile_rule(rule: BlockingRule, dialect: SqlDialect) -> Tuple[List[str], str, Optional[KeySource]]:
    """
    Compiles a single enabled rule to (setup_statements, atomic_select_sql,
    key_source). `key_source` is None for EXACT_IDENTIFIER (see KeySource
    docstring) and a KeySource for every other rule type. Raises on an
    unknown rule type.
    """
    compiler_fn = _COMPILERS.get(rule.type)
    if compiler_fn is None:
        raise ValueError(f"{rule.rule_id}: no compiler registered for rule type {rule.type}")
    return compiler_fn(rule, dialect)


def scratch_table_names(rules: List[BlockingRule]) -> List[str]:
    """
    Every per-rule staging table name `compile_rule` MIGHT create,
    across all rule types -- not just the ones a given rule's guards
    actually trigger. Over-listing is fine: callers DROP TABLE IF
    EXISTS each one, which is a no-op for names that were never
    created. Kept in sync with each `_compile_*` function's table
    naming by construction (uses the same `_rule_table` helper).
    """
    names = []
    for rule in rules:
        if rule.type == BlockingRuleType.COMPOSITE_KEY:
            names += [_rule_table(rule, s) for s in ("keys_raw", "keys", "key_freq", "block_freq")]
        elif rule.type == BlockingRuleType.TOKEN_KEY:
            names += [_rule_table(rule, s) for s in ("tokens_raw", "tokens", "freq")]
        elif rule.type == BlockingRuleType.PREFIX_KEY:
            names += [_rule_table(rule, s) for s in ("prefix_raw", "prefix", "freq")]
        elif rule.type == BlockingRuleType.DATE_PART_KEY:
            names += [_rule_table(rule, "datepart")]
        elif rule.type == BlockingRuleType.RAW_COLUMN:
            names += [_rule_table(rule, s) for s in ("raw_key", "raw_keys", "raw_freq")]
        # EXACT_IDENTIFIER creates no staging table (see KeySource docstring).
    return names


def drop_scratch_tables(con, rules: List[BlockingRule]) -> None:
    """
    Drops every per-rule staging table a compile_and_build() call for
    this rule set may have created. Safe to call even if some/none of
    the tables exist (DROP TABLE IF EXISTS). Intended to run once at
    the end of a pipeline run, after candidate_pairs/pair_*_evidence
    have been derived from them -- see the "storage reclamation" phase
    of the lakehouse migration plan: these tables measured at ~36% of
    a run's total Doris disk footprint and are pure scratch, never
    read again once candidate_pairs exists.
    """
    for table in scratch_table_names(rules):
        con.execute(f"DROP TABLE IF EXISTS {table}")


def compile_and_build(con, rules: List[BlockingRule], dialect: SqlDialect) -> str:
    """
    Executes every enabled rule's setup SQL (in `order`), then builds
    the final `candidate_pairs` table as the UNION ALL of every rule's
    atomic pairs, grouped/deduped exactly like
    engine.blocking.deterministic_blocker.build_candidate_pairs did by
    hand. Returns the final SQL text (for logging/audit/precheck reuse).
    """
    enabled = sorted([r for r in rules if r.enabled], key=lambda r: r.order)
    if not enabled:
        raise ValueError("At least one enabled blocking rule is required")
    for r in enabled:
        r.validate()

    atomic_selects = []
    for rule in enabled:
        setup_statements, select_sql, _key_source = compile_rule(rule, dialect)
        for stmt in setup_statements:
            _execute_multi(con, stmt)
        atomic_selects.append(select_sql)

    union_sql = "\nUNION ALL\n".join(f"({s})" for s in atomic_selects)
    final_select = f"""
        WITH atomic_pairs AS (
            {union_sql}
        )
        SELECT a_key, b_key, {dialect.collect_distinct_sorted('reason')} AS blocking_reasons
        FROM atomic_pairs
        GROUP BY a_key, b_key
        ORDER BY a_key, b_key
    """
    final_sql = dialect.create_or_replace_table("candidate_pairs", final_select)
    _execute_multi(con, final_sql)
    return final_sql
