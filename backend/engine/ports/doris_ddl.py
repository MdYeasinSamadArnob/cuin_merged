"""
CUIN v2 - Doris Physical Design (migration plan Phase 8)

Defines the persistent, explicitly-distributed Doris tables for the
core ER fact tables -- identifiers, customer_scalars, candidate_pairs
-- as opposed to engine.rules.compiler's ephemeral, auto-bucketed
per-rule staging tables (small, rebuilt every reblock; Doris's default
CTAS auto-bucketing is adequate for those). Getting THIS distribution
right is the single largest lever in whether Doris ends up faster or
slower than DuckDB at scale:

  - `identifiers` is bucketed on (id_type, value_norm) -- the exact
    key deterministic_blocker's EXACT_IDENTIFIER self-join uses.
    Verified live (tests/integration/test_doris_colocate_plan.py):
    EXPLAIN of that self-join shows a LOCAL join with no shuffle, once
    both sides read the same bucketed table.

  - `candidate_pairs` is bucketed on `a_key` so evidence-building's
    join against `_agg_identifiers`/`customer_scalars` (keyed by
    customer_code) has one already-local side; the `b_key` side and
    the identifier-aggregate join go through Doris's runtime filter /
    broadcast instead of a second shuffle-heavy hash join.

  - `customer_scalars` is bucketed on `customer_code`, matching its
    role as the join target for every entity-keyed lookup (evidence
    building, search, golden-record assembly). The name/token/prefix/
    date-part blocking rules run against per-rule EPHEMERAL staging
    tables (engine.rules.compiler), not this table directly, so no
    single static distribution here could be optimal for every
    possible blocking rule -- that is a deliberate scope boundary, not
    an oversight. A future iteration could add per-rule DISTRIBUTED BY
    hints to those staging CTAS statements for very large deployments;
    tracked as a follow-up, not required to prove the colocation
    lever, which is demonstrated on `identifiers` here.

Session tuning (`SESSION_VARIABLES`) should be applied on every
connection used for blocking/scoring queries, not just at table
creation time.
"""

from typing import Dict

REPLICATION_NUM_DEFAULT = 1  # sandbox/single-BE default; production should set >= 3
COLOCATE_GROUP_IDENTITY = "cuin_er_identity_grp"

# Applied per-session (SET <key> = <value>), not per-table. Matches
# the plan's Phase 8 list: parallelism sized to BE cores, spill
# enabled as the Doris analogue of DUCKDB_MEMORY_LIMIT's backpressure
# at 10B-row scale. Names/defaults verified live against Doris 4.0.3
# (`SHOW VARIABLES LIKE '%...%'`) -- two corrections from the first
# draft, worth recording:
#   - There is no `enable_colocate_join` variable. Colocate planning
#     is ON BY DEFAULT (`disable_colocate_plan` defaults to `false`);
#     it is left out of this dict entirely rather than set to a
#     nonexistent variable.
#   - `runtime_filter_type`'s value is a comma list and the token is
#     `IN_OR_BLOOM_FILTER`, not `IN_OR_BLOOM` -- the default already
#     includes it (`IN_OR_BLOOM_FILTER,MIN_MAX`), listed here anyway
#     for explicitness/auditability.
SESSION_VARIABLES: Dict[str, str] = {
    "runtime_filter_type": "'IN_OR_BLOOM_FILTER,MIN_MAX'",
    "enable_spill": "true",
    "parallel_pipeline_task_num": "0",  # 0 = auto-size to BE cores
}


def session_variable_statements() -> list:
    return [f"SET {k} = {v}" for k, v in SESSION_VARIABLES.items()]


def create_identifiers_table_sql(
    buckets: int = 48,
    colocate_group: str = COLOCATE_GROUP_IDENTITY,
    replication_num: int = REPLICATION_NUM_DEFAULT,
) -> str:
    return f"""
CREATE TABLE IF NOT EXISTS identifiers (
    customer_code VARCHAR(64) NOT NULL,
    id_type       VARCHAR(20) NOT NULL,
    value_raw     VARCHAR(500),
    value_norm    VARCHAR(500),
    doc_type      VARCHAR(20),
    is_valid      BOOLEAN,
    is_suppressed BOOLEAN
)
DUPLICATE KEY(customer_code, id_type)
DISTRIBUTED BY HASH(id_type, value_norm) BUCKETS {buckets}
PROPERTIES (
    "colocate_with" = "{colocate_group}",
    "replication_num" = "{replication_num}"
)
""".strip()


def create_customer_scalars_table_sql(buckets: int = 48, replication_num: int = REPLICATION_NUM_DEFAULT) -> str:
    return f"""
CREATE TABLE IF NOT EXISTS customer_scalars (
    customer_code VARCHAR(64) NOT NULL,
    name_norm     VARCHAR(500),
    name_tokens   ARRAY<TEXT>,
    address_tokens ARRAY<TEXT>,
    dob_iso       VARCHAR(10),
    dob_precision VARCHAR(10)
)
DUPLICATE KEY(customer_code)
DISTRIBUTED BY HASH(customer_code) BUCKETS {buckets}
PROPERTIES ("replication_num" = "{replication_num}")
""".strip()


def create_candidate_pairs_table_sql(buckets: int = 48, replication_num: int = REPLICATION_NUM_DEFAULT) -> str:
    return f"""
CREATE TABLE IF NOT EXISTS candidate_pairs (
    a_key            VARCHAR(64) NOT NULL,
    b_key            VARCHAR(64) NOT NULL,
    blocking_reasons ARRAY<TEXT>
)
DUPLICATE KEY(a_key, b_key)
DISTRIBUTED BY HASH(a_key) BUCKETS {buckets}
PROPERTIES ("replication_num" = "{replication_num}")
""".strip()


def create_identifier_frequency_table_sql(buckets: int = 16, replication_num: int = REPLICATION_NUM_DEFAULT) -> str:
    return f"""
CREATE TABLE IF NOT EXISTS identifier_frequency (
    id_type       VARCHAR(20) NOT NULL,
    value_norm    VARCHAR(500) NOT NULL,
    n_records     INT,
    is_suppressed BOOLEAN
)
DUPLICATE KEY(id_type, value_norm)
DISTRIBUTED BY HASH(id_type, value_norm) BUCKETS {buckets}
PROPERTIES ("replication_num" = "{replication_num}")
""".strip()


def all_ddl_statements(buckets: int = 48, replication_num: int = REPLICATION_NUM_DEFAULT) -> list:
    return [
        create_identifiers_table_sql(buckets=buckets, replication_num=replication_num),
        create_customer_scalars_table_sql(buckets=buckets, replication_num=replication_num),
        create_candidate_pairs_table_sql(buckets=buckets, replication_num=replication_num),
        create_identifier_frequency_table_sql(replication_num=replication_num),
    ]
