"""
CUIN v2 - Identifier Explosion, dialect-driven (Ruleset v2, Layer 1)

Dialect-portable twin of engine.normalize.explode -- same CTE
structure, same validation rules, same honorifics/boilerplate lists,
translated through engine.ports.dialect.SqlDialect so it runs
unchanged on DuckDB or Doris. engine.normalize.explode itself is left
untouched (it is what pipeline.duckdb_orchestrator uses, and
tests/unit/test_sql_python_parity.py pins its exact behavior) -- this
module exists purely to give pipeline.doris_orchestrator (and any
future engine) the same logic without hand-duplicating DuckDB-specific
SQL text.

tests/unit/test_explode_dialect_parity.py proves this module, run
with DuckDbDialect, produces byte-identical output to
engine.normalize.explode on the same fixture -- so drift between the
two is caught, not silent. A second live-Doris check in that same
file proves the DorisDialect path produces structurally equivalent
output against a real cluster.
"""

# Keep in sync with policies/ruleset_v2.yaml `identifiers.name.honorifics`,
# engine.normalize.identity's honorific set, and engine.normalize.explode's
# _HONORIFICS_SQL_LIST.
_HONORIFICS = ['MD', 'MOHD', 'MOHAMMAD', 'MUHAMMAD', 'MST', 'MRS', 'MR', 'MS', 'ALHAJ', 'HAJI', 'DR', 'ENGR', 'ADV', 'LATE', 'BEGUM']

_ADDRESS_BOILERPLATE = [
    'DO', 'SAME', 'NA', 'N/A', 'SAME AS PRESENT ADDRESS', 'SAME ADDRESS',
    'AS ABOVE', 'PRESENT ADDRESS', 'PERMANENT ADDRESS',
]

_BD_ADDRESS_ABBREV = [
    (r"\bROAD\b", "RD"), (r"\bSTREET\b", "ST"), (r"\bAVENUE\b", "AVE"),
    (r"\bBUILDING\b", "BLDG"), (r"\bFLOOR\b", "FL"), (r"\bAPARTMENT\b", "APT"),
    (r"\bBLOCK\b", "BLK"), (r"\bWARD\b", "WD"), (r"\bUPAZILA\b", "UPZ"),
    (r"\bDISTRICT\b", "DIST"), (r"\bNORTH\b", "N"), (r"\bSOUTH\b", "S"),
    (r"\bEAST\b", "E"), (r"\bWEST\b", "W"),
]


def _array_literal(values, dialect) -> str:
    return "[" + ", ".join(dialect.quote_str(v) for v in values) + "]"


def _address_normalize_expr(col_expr: str, dialect) -> str:
    expr = f"trim({dialect.strip_accents(f'upper({col_expr})')})"
    for pattern, repl in _BD_ADDRESS_ABBREV:
        expr = dialect.regexp_replace_all(expr, pattern, repl)
    expr = dialect.regexp_replace_all(expr, r"[^[:alnum:][:space:]]", " ")
    collapsed_ws = dialect.regexp_replace_all(expr, r"\s+", " ")
    expr = f"trim({collapsed_ws})"
    return expr


def build_identifiers_table(con, dialect, source_relation: str = "raw") -> None:
    """
    Mirrors engine.normalize.explode.build_identifiers_table exactly,
    through `dialect` instead of hardcoded DuckDB SQL. See that
    module's docstring for the full rationale (native SQL, not Python
    UDFs, for throughput at 1.5M+ rows) -- it applies identically here.
    """
    honorifics_lit = _array_literal(_HONORIFICS, dialect)
    boilerplate_lit = _array_literal(_ADDRESS_BOILERPLATE, dialect)
    addr_expr = _address_normalize_expr("value_raw", dialect)

    mobile_ref = dialect.unnest_column_ref("m")
    email_ref = dialect.unnest_column_ref("e")
    doc_ref = dialect.unnest_column_ref("d")
    addr_ref = dialect.unnest_column_ref("a")

    email_valid_expr = dialect.regexp_matches("norm", r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
    doc_dtype_expr = dialect.regexp_extract("upper(trim(value_raw))", r"^\s*([A-Z]+)\s*:\s*(.*)$", 1)
    doc_dvalue_raw = dialect.regexp_extract("upper(trim(value_raw))", r"^\s*([A-Z]+)\s*:\s*(.*)$", 2)
    doc_dvalue_expr = dialect.regexp_replace_all(doc_dvalue_raw, r"[\s\-\.]", "")

    select_sql = f"""
        WITH mobile_raw AS (
            SELECT DISTINCT CUSTOMER_CODE AS customer_code, {mobile_ref} AS value_raw
            FROM {dialect.unnest_lateral(source_relation, 'MOBILE', 'm')}
            WHERE {mobile_ref} IS NOT NULL
        ),
        mobile_parsed AS (
            SELECT
                customer_code, value_raw,
                {dialect.regexp_replace_all('value_raw', '[^0-9]', '')} AS digits_0
            FROM mobile_raw
        ),
        mobile_norm AS (
            SELECT
                customer_code, value_raw,
                CASE
                    WHEN digits_0 LIKE '880%' AND {dialect.str_len('digits_0')} IN (12, 13)
                        THEN {dialect.concat(dialect.quote_str('0'), dialect.substr_from('digits_0', 4))}
                    WHEN {dialect.str_len('digits_0')} = 10 AND {dialect.substr('digits_0', 1, 1)} = '1'
                         AND {dialect.substr('digits_0', 2, 1)} BETWEEN '3' AND '9'
                        THEN {dialect.concat(dialect.quote_str('0'), 'digits_0')}
                    ELSE digits_0
                END AS digits
            FROM mobile_parsed
        ),
        mobile_final AS (
            SELECT
                customer_code,
                'mobile' AS id_type,
                value_raw,
                CASE WHEN valid THEN digits ELSE NULL END AS value_norm,
                CAST(NULL AS VARCHAR) AS doc_type,
                valid AS is_valid
            FROM (
                SELECT *,
                    ({dialect.regexp_matches('digits', '^01[3-9][0-9]{8}$')}
                    AND {dialect.array_size(dialect.array_distinct(dialect.str_split(dialect.substr_from('digits', 4), '')))} >= 3)
                    AS valid
                FROM mobile_norm
            ) _mv
        ),

        email_raw AS (
            SELECT DISTINCT CUSTOMER_CODE AS customer_code, {email_ref} AS value_raw
            FROM {dialect.unnest_lateral(source_relation, 'EMAIL', 'e')}
            WHERE {email_ref} IS NOT NULL
        ),
        email_final AS (
            SELECT
                customer_code,
                'email' AS id_type,
                value_raw,
                CASE WHEN valid THEN norm ELSE NULL END AS value_norm,
                CAST(NULL AS VARCHAR) AS doc_type,
                valid AS is_valid
            FROM (
                SELECT *,
                    {email_valid_expr} AS valid
                FROM (
                    SELECT customer_code, value_raw, trim(lower(value_raw)) AS norm
                    FROM email_raw
                ) _en
            ) _ev
        ),

        document_raw AS (
            SELECT DISTINCT CUSTOMER_CODE AS customer_code, {doc_ref} AS value_raw
            FROM {dialect.unnest_lateral(source_relation, 'DOCUMENT', 'd')}
            WHERE {doc_ref} IS NOT NULL
        ),
        document_parsed AS (
            SELECT
                customer_code, value_raw,
                {doc_dtype_expr} AS dtype,
                {doc_dvalue_expr} AS dvalue
            FROM document_raw
        ),
        document_final AS (
            SELECT
                customer_code,
                'document' AS id_type,
                value_raw,
                CASE WHEN valid THEN dvalue ELSE NULL END AS value_norm,
                dtype AS doc_type,
                valid AS is_valid
            FROM (
                SELECT *,
                    CASE
                        WHEN dtype IS NULL OR dtype = '' OR dvalue = '' THEN FALSE
                        WHEN dtype = 'TIN' THEN {dialect.regexp_matches('dvalue', '^[0-9]{12}$')}
                        WHEN dtype = 'NID' THEN {dialect.regexp_matches('dvalue', '^[0-9]{10}$')}
                                              OR {dialect.regexp_matches('dvalue', '^[0-9]{13}$')}
                                              OR {dialect.regexp_matches('dvalue', '^[0-9]{17}$')}
                        WHEN dtype = 'PASSPORT' THEN {dialect.regexp_matches('dvalue', '^[A-Z]{1,2}[0-9]{6,7}$')}
                        WHEN dtype = 'BRC' THEN {dialect.regexp_matches('dvalue', '^[0-9]{17}$')}
                        ELSE
                            {dialect.str_len('dvalue')} >= 6
                            AND {dialect.regexp_matches('dvalue', '^[A-Z0-9]+$')}
                            AND {dialect.regexp_matches('dvalue', '[0-9]')}
                    END AS valid
                FROM document_parsed
            ) _dv
        ),

        address_raw AS (
            SELECT DISTINCT CUSTOMER_CODE AS customer_code, {addr_ref} AS value_raw
            FROM {dialect.unnest_lateral(source_relation, 'FULL_ADDRESS', 'a')}
            WHERE {addr_ref} IS NOT NULL
        ),
        address_final AS (
            SELECT
                customer_code,
                'address' AS id_type,
                value_raw,
                CASE WHEN valid THEN norm ELSE NULL END AS value_norm,
                CAST(NULL AS VARCHAR) AS doc_type,
                valid AS is_valid
            FROM (
                SELECT *,
                    (norm IS NOT NULL AND norm != ''
                     AND NOT {dialect.array_contains(boilerplate_lit, 'norm')}
                     AND {dialect.str_len('norm')} >= 12
                     AND {dialect.array_size(dialect.str_split('norm', ' '))} >= 3) AS valid
                FROM (
                    SELECT customer_code, value_raw, {addr_expr} AS norm
                    FROM address_raw
                ) _an
            ) _av
        )

        SELECT * FROM mobile_final
        UNION ALL SELECT * FROM email_final
        UNION ALL SELECT * FROM document_final
        UNION ALL SELECT * FROM address_final
    """
    for stmt in dialect.create_or_replace_table("identifiers", select_sql).split(";\n"):
        stmt = stmt.strip()
        if stmt:
            con.execute(stmt)

    _build_customer_scalars(con, dialect, source_relation, honorifics_lit)


def _build_customer_scalars(con, dialect, source_relation: str, honorifics_lit: str) -> None:
    name_stage1 = dialect.strip_accents("upper(NAME)")
    name_stage1 = dialect.regexp_replace_all(name_stage1, r"[^[:alnum:][:space:]-]", "")
    name_stage1 = f"trim({dialect.regexp_replace_all(name_stage1, '-', ' ')})"

    parsed_dt = dialect.try_parse_datetime("BIRTH_DATE", "%Y-%m-%dT%H:%M:%S")

    name_collapsed_ws = dialect.regexp_replace_all("name_stage1", r"\s+", " ")
    name_norm_expr = f"trim({name_collapsed_ws})"

    select_sql = f"""
        WITH name_norm_cte AS (
            SELECT
                CUSTOMER_CODE AS customer_code,
                {name_stage1} AS name_stage1,
                BIRTH_DATE
            FROM {source_relation}
        ),
        name_tokens_cte AS (
            SELECT
                customer_code,
                {name_norm_expr} AS name_norm,
                {dialect.str_split(name_norm_expr, ' ')} AS tokens0,
                BIRTH_DATE
            FROM name_norm_cte
        ),
        name_stripped_cte AS (
            SELECT
                customer_code, name_norm, BIRTH_DATE,
                CASE WHEN {dialect.array_size('tokens0')} > 0
                     AND {dialect.array_contains(honorifics_lit, dialect.array_element('tokens0', 1))}
                     THEN {dialect.array_slice_from('tokens0', 2)} ELSE tokens0 END AS tokens1
            FROM name_tokens_cte
        ),
        name_final_cte AS (
            SELECT
                customer_code, name_norm, BIRTH_DATE,
                CASE WHEN {dialect.array_size('tokens1')} > 0
                     AND {dialect.array_contains(honorifics_lit, dialect.array_element('tokens1', 1))}
                     THEN {dialect.array_slice_from('tokens1', 2)} ELSE tokens1 END AS name_tokens
            FROM name_stripped_cte
        )
        SELECT
            customer_code,
            CASE WHEN name_norm = '' THEN NULL ELSE name_norm END AS name_norm,
            {dialect.array_filter_nonempty('name_tokens')} AS name_tokens,
            {dialect.format_date(parsed_dt, '%Y-%m-%d')} AS dob_iso,
            CASE
                WHEN {parsed_dt} IS NULL THEN NULL
                WHEN {dialect.date_part('month', parsed_dt)} = 1
                     AND {dialect.date_part('day', parsed_dt)} = 1
                    THEN 'YEAR_ONLY'
                ELSE 'FULL'
            END AS dob_precision
        FROM name_final_cte
    """
    for stmt in dialect.create_or_replace_table("customer_scalars", select_sql).split(";\n"):
        stmt = stmt.strip()
        if stmt:
            con.execute(stmt)
