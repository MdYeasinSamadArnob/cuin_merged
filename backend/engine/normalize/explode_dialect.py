"""
CUIN v2 - Identifier Explosion, dialect-driven (Ruleset v2, Layer 1)

Explodes MOBILE/EMAIL/DOCUMENT/FULL_ADDRESS array columns into one
normalized identifier row per value, translated through
engine.ports.dialect.SqlDialect (currently only DorisDialect) so this
logic isn't hand-duplicated in engine-specific SQL text. Used by
pipeline.doris_orchestrator.

Originally written as a dialect-portable twin of a DuckDB-only
predecessor module during the DuckDB-to-Doris migration, proven
byte-identical to it on the same fixture; that predecessor and the
comparison test have since been removed now that Doris is the only
engine.
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

# Tokens dropped from address_tokens (customer_scalars, TOKEN_KEY
# blocking) -- the abbreviated forms _BD_ADDRESS_ABBREV produces, plus
# bare directionals. These appear in nearly every address (by the time
# tokenization runs, _address_normalize_expr has already collapsed
# "ROAD"/"STREET"/etc down to exactly these strings), so keeping them
# as blocking keys would create massive, non-selective blocks instead
# of useful ones -- the house number, street name, and area/city
# tokens are what actually discriminate one address from another.
_ADDRESS_TOKEN_STOPWORDS = ["RD", "ST", "AVE", "BLDG", "FL", "APT", "BLK", "WD", "UPZ", "DIST", "N", "S", "E", "W"]


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

    addr_stopwords_lit = _array_literal(_ADDRESS_TOKEN_STOPWORDS, dialect)
    addr_tok_ref = dialect.unnest_column_ref("atok")

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
        ),
        -- Fuzzy(-ish) address blocking support: word-tokenize the SAME
        -- validated, normalized address value already used for exact
        -- address blocking (identifiers WHERE id_type='address'), so
        -- both rules agree on what "the address" means. A customer can
        -- have multiple addresses (FULL_ADDRESS is an array column) --
        -- tokens from all of them are flattened into one deduped array
        -- per customer_code, same shape as name_tokens.
        address_word_arrays AS (
            SELECT customer_code, {dialect.str_split('value_norm', ' ')} AS words
            FROM identifiers
            WHERE id_type = 'address' AND value_norm IS NOT NULL
        ),
        address_tokens_raw AS (
            SELECT customer_code, {addr_tok_ref} AS token
            FROM {dialect.unnest_lateral('address_word_arrays', 'words', 'atok')}
        ),
        address_tokens_filtered AS (
            SELECT customer_code, token
            FROM address_tokens_raw
            WHERE token IS NOT NULL AND token != ''
              AND NOT {dialect.array_contains(addr_stopwords_lit, 'token')}
        ),
        address_tokens_agg AS (
            SELECT customer_code, {dialect.collect_distinct_sorted('token')} AS address_tokens
            FROM address_tokens_filtered
            GROUP BY customer_code
        )
        SELECT
            n.customer_code,
            CASE WHEN n.name_norm = '' THEN NULL ELSE n.name_norm END AS name_norm,
            {dialect.array_filter_nonempty('n.name_tokens')} AS name_tokens,
            a.address_tokens AS address_tokens,
            {dialect.format_date(parsed_dt, '%Y-%m-%d')} AS dob_iso,
            CASE
                WHEN {parsed_dt} IS NULL THEN NULL
                WHEN {dialect.date_part('month', parsed_dt)} = 1
                     AND {dialect.date_part('day', parsed_dt)} = 1
                    THEN 'YEAR_ONLY'
                ELSE 'FULL'
            END AS dob_precision
        FROM name_final_cte n
        LEFT JOIN address_tokens_agg a ON a.customer_code = n.customer_code
    """
    for stmt in dialect.create_or_replace_table("customer_scalars", select_sql).split(";\n"):
        stmt = stmt.strip()
        if stmt:
            con.execute(stmt)
