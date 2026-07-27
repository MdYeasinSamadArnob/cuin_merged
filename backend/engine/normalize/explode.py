"""
CUIN v2 - Identifier Explosion (Ruleset v2, Layer 1 SQL side)

Explodes the array<string> columns (MOBILE, EMAIL, DOCUMENT,
FULL_ADDRESS) into a long `identifiers` table, one row per
(customer_code, id_type, distinct value). This is the single step
that fixes root cause #2 from the audit: 62% of MOBILE rows and 74%
of DOCUMENT rows are multi-element arrays with internal duplicates
(e.g. ['0192...','0192...']), which made Splink's whole-array
ExactMatchLevel comparisons silently useless. Exploding + DISTINCT
collapses those to clean per-identifier sets.

IMPLEMENTATION NOTE: this is written as NATIVE DUCKDB SQL (regexp_*,
str_split, list_distinct, strip_accents), not Python UDFs registered
via con.create_function(). An earlier version used Python UDFs for
easier code reuse with engine.normalize.identity, but DuckDB calls
non-vectorized Python UDFs once per row -- at 1.5M+ rows with multiple
array elements each, that is millions of individual Python calls and
took minutes just for identifier explosion. The rules implemented in
SQL below are kept in lockstep with engine.normalize.identity, which
remains the tested "oracle": tests/unit/test_sql_python_parity.py
asserts the SQL and Python implementations agree on the same fixture
values, so drift between the two is caught rather than silent.
"""

import duckdb

# Keep in sync with policies/ruleset_v2.yaml `identifiers.name.honorifics`
# and engine.normalize.identity's honorific set.
_HONORIFICS_SQL_LIST = "['MD','MOHD','MOHAMMAD','MUHAMMAD','MST','MRS','MR','MS','ALHAJ','HAJI','DR','ENGR','ADV','LATE','BEGUM']"

_ADDRESS_BOILERPLATE_SQL_LIST = (
    "['DO','SAME','NA','N/A','SAME AS PRESENT ADDRESS','SAME ADDRESS',"
    "'AS ABOVE','PRESENT ADDRESS','PERMANENT ADDRESS']"
)

_BD_ADDRESS_ABBREV = [
    (r"\bROAD\b", "RD"), (r"\bSTREET\b", "ST"), (r"\bAVENUE\b", "AVE"),
    (r"\bBUILDING\b", "BLDG"), (r"\bFLOOR\b", "FL"), (r"\bAPARTMENT\b", "APT"),
    (r"\bBLOCK\b", "BLK"), (r"\bWARD\b", "WD"), (r"\bUPAZILA\b", "UPZ"),
    (r"\bDISTRICT\b", "DIST"), (r"\bNORTH\b", "N"), (r"\bSOUTH\b", "S"),
    (r"\bEAST\b", "E"), (r"\bWEST\b", "W"),
]


def _address_normalize_expr(col_expr: str) -> str:
    """Chained regexp_replace matching engine.normalize.identity.norm_address."""
    expr = f"trim(strip_accents(upper({col_expr})))"
    for pattern, repl in _BD_ADDRESS_ABBREV:
        expr = f"regexp_replace({expr}, '{pattern}', '{repl}', 'g')"
    expr = f"regexp_replace({expr}, '[^[:alnum:][:space:]]', ' ', 'g')"
    expr = f"trim(regexp_replace({expr}, '\\s+', ' ', 'g'))"
    return expr


def build_identifiers_table(con: duckdb.DuckDBPyConnection, source_relation: str = "raw") -> None:
    """
    Given a registered relation/view named `source_relation` with columns
    CUSTOMER_CODE, MOBILE (list), EMAIL (list), DOCUMENT (list),
    FULL_ADDRESS (list), NAME, BIRTH_DATE -- build the `identifiers` table.

    All normalization/validation is native SQL for throughput at 1.5M+
    rows -- see module docstring for why Python UDFs were abandoned.
    """
    addr_expr = _address_normalize_expr("value_raw")

    con.execute(f"""
        CREATE OR REPLACE TABLE identifiers AS
        WITH mobile_raw AS (
            SELECT DISTINCT CUSTOMER_CODE AS customer_code, m AS value_raw
            FROM {source_relation}, UNNEST(MOBILE) AS t(m)
            WHERE m IS NOT NULL
        ),
        mobile_parsed AS (
            SELECT
                customer_code, value_raw,
                regexp_replace(value_raw, '[^0-9]', '', 'g') AS digits_0
            FROM mobile_raw
        ),
        mobile_norm AS (
            SELECT
                customer_code, value_raw,
                CASE
                    WHEN digits_0 LIKE '880%' AND len(digits_0) IN (12, 13)
                        THEN '0' || substr(digits_0, 4)
                    WHEN len(digits_0) = 10 AND substr(digits_0, 1, 1) = '1'
                         AND substr(digits_0, 2, 1) BETWEEN '3' AND '9'
                        THEN '0' || digits_0
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
                    regexp_matches(digits, '^01[3-9][0-9]{{8}}$')
                    AND len(list_distinct(str_split(substr(digits, 4), ''))) >= 3
                    AS valid
                FROM mobile_norm
            )
        ),

        email_raw AS (
            SELECT DISTINCT CUSTOMER_CODE AS customer_code, e AS value_raw
            FROM {source_relation}, UNNEST(EMAIL) AS t(e)
            WHERE e IS NOT NULL
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
                    regexp_matches(norm, '^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$') AS valid
                FROM (
                    SELECT customer_code, value_raw, trim(lower(value_raw)) AS norm
                    FROM email_raw
                )
            )
        ),

        document_raw AS (
            SELECT DISTINCT CUSTOMER_CODE AS customer_code, d AS value_raw
            FROM {source_relation}, UNNEST(DOCUMENT) AS t(d)
            WHERE d IS NOT NULL
        ),
        document_parsed AS (
            SELECT
                customer_code, value_raw,
                regexp_extract(upper(trim(value_raw)), '^\\s*([A-Z]+)\\s*:\\s*(.*)$', 1) AS dtype,
                regexp_replace(
                    regexp_extract(upper(trim(value_raw)), '^\\s*([A-Z]+)\\s*:\\s*(.*)$', 2),
                    '[\\s\\-\\.]', '', 'g'
                ) AS dvalue
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
                        WHEN dtype = 'TIN' THEN regexp_matches(dvalue, '^[0-9]{{12}}$')
                        WHEN dtype = 'NID' THEN regexp_matches(dvalue, '^[0-9]{{10}}$')
                                              OR regexp_matches(dvalue, '^[0-9]{{13}}$')
                                              OR regexp_matches(dvalue, '^[0-9]{{17}}$')
                        WHEN dtype = 'PASSPORT' THEN regexp_matches(dvalue, '^[A-Z]{{1,2}}[0-9]{{6,7}}$')
                        WHEN dtype = 'BRC' THEN regexp_matches(dvalue, '^[0-9]{{17}}$')
                        ELSE
                            len(dvalue) >= 6
                            AND regexp_matches(dvalue, '^[A-Z0-9]+$')
                            AND regexp_matches(dvalue, '[0-9]')
                    END AS valid
                FROM document_parsed
            )
        ),

        address_raw AS (
            SELECT DISTINCT CUSTOMER_CODE AS customer_code, a AS value_raw
            FROM {source_relation}, UNNEST(FULL_ADDRESS) AS t(a)
            WHERE a IS NOT NULL
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
                     AND NOT list_contains({_ADDRESS_BOILERPLATE_SQL_LIST}, norm)
                     AND len(norm) >= 12
                     AND len(str_split(norm, ' ')) >= 3) AS valid
                FROM (
                    SELECT customer_code, value_raw, {addr_expr} AS norm
                    FROM address_raw
                )
            )
        )

        SELECT * FROM mobile_final
        UNION ALL SELECT * FROM email_final
        UNION ALL SELECT * FROM document_final
        UNION ALL SELECT * FROM address_final
    """)

    # Name and DOB are scalar (one per customer), normalized the same way
    # for use in blocking/evidence. Honorific-stripping peels up to 2
    # leading tokens (covers the realistic cases; a name with 3+ stacked
    # honorifics is vanishingly rare in this dataset).
    con.execute(f"""
        CREATE OR REPLACE TABLE customer_scalars AS
        WITH name_norm_cte AS (
            SELECT
                CUSTOMER_CODE AS customer_code,
                trim(regexp_replace(
                    regexp_replace(
                        strip_accents(upper(NAME)), '[^[:alnum:][:space:]-]', '', 'g'
                    ), '-', ' ', 'g'
                )) AS name_stage1,
                BIRTH_DATE
            FROM {source_relation}
        ),
        name_tokens_cte AS (
            SELECT
                customer_code,
                trim(regexp_replace(name_stage1, '\\s+', ' ', 'g')) AS name_norm,
                str_split(trim(regexp_replace(name_stage1, '\\s+', ' ', 'g')), ' ') AS tokens0,
                BIRTH_DATE
            FROM name_norm_cte
        ),
        name_stripped_cte AS (
            SELECT
                customer_code, name_norm, BIRTH_DATE,
                CASE WHEN len(tokens0) > 0 AND list_contains({_HONORIFICS_SQL_LIST}, tokens0[1])
                     THEN tokens0[2:] ELSE tokens0 END AS tokens1
            FROM name_tokens_cte
        ),
        name_final_cte AS (
            SELECT
                customer_code, name_norm, BIRTH_DATE,
                CASE WHEN len(tokens1) > 0 AND list_contains({_HONORIFICS_SQL_LIST}, tokens1[1])
                     THEN tokens1[2:] ELSE tokens1 END AS name_tokens
            FROM name_stripped_cte
        )
        SELECT
            customer_code,
            CASE WHEN name_norm = '' THEN NULL ELSE name_norm END AS name_norm,
            list_filter(name_tokens, x -> x != '') AS name_tokens,
            strftime(try_strptime(BIRTH_DATE, '%Y-%m-%dT%H:%M:%S'), '%Y-%m-%d') AS dob_iso,
            CASE
                WHEN try_strptime(BIRTH_DATE, '%Y-%m-%dT%H:%M:%S') IS NULL THEN NULL
                WHEN month(try_strptime(BIRTH_DATE, '%Y-%m-%dT%H:%M:%S')) = 1
                     AND day(try_strptime(BIRTH_DATE, '%Y-%m-%dT%H:%M:%S')) = 1
                    THEN 'YEAR_ONLY'
                ELSE 'FULL'
            END AS dob_precision
        FROM name_final_cte
    """)
