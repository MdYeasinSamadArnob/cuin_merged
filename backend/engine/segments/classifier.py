"""
CUIN v2 - Record Segmentation (Stage 5 of the banker-rule-engine migration)

Classifies every customer as "COMPANY" or "INDIVIDUAL" so entity
resolution never merges a business and a person into one identity --
the two things a bank's AUTO_LINK path treated identically before this
stage, since `tiers.py`/the confidence model has no concept of record
type at all.

**Segmentation gates identity MERGING, not evidence.** Blocking still
runs unsegmented (see pipeline orchestrators' _stage_block) -- a
Company and an Individual sharing a phone number still become a
candidate pair, and that pair is still evidenced normally. What
changes is what happens to that evidence at the decide step: a
same-segment pair goes through the normal confidence model and can
become AUTO_LINK/REVIEW/REJECT; a cross-segment pair is recorded as an
`entity_relationships` row instead (see db/migrations/004) -- kept
traceable, never merged. See engine.scoring.confidence's segment-aware
wrapper for where that split happens.

Classification is column-mapping first (if the source ever has a real
customer-type column -- this dataset doesn't, so this path is defined
but unused today), falling back to NAME-pattern matching against a
configurable keyword list. Segmentation is OFF by default (a single
"ALL" segment, byte-identical to pre-Stage-5 behavior) -- see
SegmentationConfig.enabled and the compile-time elision this enables
in the callers below.
"""

import re
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

# Default keyword list -- verified against this deployment's real data
# (grep for LTD/LIMITED/COMPANY/CORP/BANK/ENTERPRISE/TRADERS/etc. found
# ~48,638 matching records). "M/S" (Bangladeshi convention for
# "Messrs", prefixing a proprietorship/business name) and "M/S." are
# included because they were the single most common company signal in
# a live sample -- more common than "LTD" itself in this dataset.
DEFAULT_COMPANY_KEYWORDS: Tuple[str, ...] = (
    "M/S", "MESSRS", "LTD", "LIMITED", "COMPANY", "CO.", "CORPORATION", "CORP",
    "ENTERPRISE", "ENTERPRISES", "TRADERS", "TRADING", "INDUSTRIES", "INDUSTRIAL",
    "MILLS", "GROUP", "ASSOCIATES", "AGENCY", "AGENCIES", "STORE", "STORES",
    "BANK", "INSURANCE", "FOUNDATION", "SOCIETY", "SCHOOL", "COLLEGE", "UNIVERSITY",
    "HOSPITAL", "CLINIC", "PHARMACY", "CONSTRUCTION", "TEXTILE", "PLC", "LLC", "INC",
)

SEGMENT_COMPANY = "COMPANY"
SEGMENT_INDIVIDUAL = "INDIVIDUAL"
SEGMENT_ALL = "ALL"  # the default, single-segment, segmentation-off value


@dataclass(frozen=True)
class SegmentationConfig:
    """
    `enabled=False` (the default) means every customer is segment
    "ALL" -- identical to pre-Stage-5 behavior, and the predicate is
    elided entirely at decide time rather than evaluated per pair (see
    engine.scoring.confidence.score_pair_segmented), so this is a
    structural no-op, not merely a behavioral one.
    """
    enabled: bool = False
    mode: str = "name_patterns"  # "column_map" | "name_patterns" -- column_map not yet wired to a real source column
    company_keywords: Tuple[str, ...] = DEFAULT_COMPANY_KEYWORDS
    column_map: Optional[dict] = None  # {source_column, value_to_segment} -- reserved for a real customer-type column

    def to_dict(self) -> dict:
        return {
            "enabled": self.enabled,
            "mode": self.mode,
            "company_keywords": list(self.company_keywords),
            "column_map": self.column_map,
        }

    @staticmethod
    def from_dict(d: Optional[dict]) -> "SegmentationConfig":
        d = d or {}
        return SegmentationConfig(
            enabled=d.get("enabled", False),
            mode=d.get("mode", "name_patterns"),
            company_keywords=tuple(d.get("company_keywords", DEFAULT_COMPANY_KEYWORDS)),
            column_map=d.get("column_map"),
        )


DEFAULT_SEGMENTATION_CONFIG = SegmentationConfig()


_BOUNDARY_PATTERN_CACHE: Dict[Tuple[str, ...], str] = {}
_BOUNDARY_REGEX_CACHE: Dict[Tuple[str, ...], "re.Pattern"] = {}


def _company_keyword_pattern_str(company_keywords: Tuple[str, ...]) -> str:
    """
    A single alternation regex requiring each keyword to be a whole
    "word" (bounded by start/end-of-string or a non-alphanumeric
    character on both sides), not merely a substring. Plain `kw in
    upper` false-positives badly on short keywords: "INC" matches
    inside "PRINCE", "TINCO", "VINCENT", "INCOME"; "STORE" matches
    inside "STOREKEEPER" -- all real individual-customer names in this
    dataset that a naive substring check mislabels COMPANY. Shared by
    both classify_segment_python and segment_sql_expr so the two stay
    provably identical (see test_sql_matches_python_oracle) rather than
    hand-maintaining two boundary implementations that could drift.
    """
    pattern = _BOUNDARY_PATTERN_CACHE.get(company_keywords)
    if pattern is None:
        alternation = "|".join(re.escape(kw) for kw in company_keywords)
        pattern = rf"(^|[^A-Za-z0-9])({alternation})([^A-Za-z0-9]|$)"
        _BOUNDARY_PATTERN_CACHE[company_keywords] = pattern
    return pattern


def _company_keyword_regex(company_keywords: Tuple[str, ...]) -> "re.Pattern":
    compiled = _BOUNDARY_REGEX_CACHE.get(company_keywords)
    if compiled is None:
        compiled = re.compile(_company_keyword_pattern_str(company_keywords))
        _BOUNDARY_REGEX_CACHE[company_keywords] = compiled
    return compiled


def classify_segment_python(name_norm: Optional[str], config: SegmentationConfig) -> str:
    """
    Python oracle -- mirrors the SQL expression built by
    segment_sql_expr() exactly. Used by tests and by any Python-side
    caller that already has a single record's normalized name in hand.
    """
    if not config.enabled:
        return SEGMENT_ALL
    if not name_norm:
        return SEGMENT_INDIVIDUAL
    upper = name_norm.upper()
    if _company_keyword_regex(config.company_keywords).search(upper):
        return SEGMENT_COMPANY
    return SEGMENT_INDIVIDUAL


def segment_sql_expr(name_column: str, config: SegmentationConfig, dialect) -> str:
    """
    SQL CASE expression computing the same classification as
    classify_segment_python(), portable across DuckDB/Doris via the
    dialect's regexp_matches() (RE2 on DuckDB, REGEXP on Doris -- both
    support the plain alternation/character-class/group syntax used
    here). Callers should short-circuit around this entirely when
    `not config.enabled` (see build_customer_segments below) rather
    than emit a CASE that always evaluates to 'ALL' -- both are
    correct, but skipping the computation is cheaper and makes the
    "segmentation is a no-op when disabled" claim structural.
    """
    if not config.enabled:
        return dialect.quote_str(SEGMENT_ALL)
    pattern = _company_keyword_pattern_str(config.company_keywords)
    match_expr = dialect.regexp_matches(f"UPPER({name_column})", pattern)
    return (
        f"CASE WHEN {name_column} IS NULL THEN {dialect.quote_str(SEGMENT_INDIVIDUAL)} "
        f"WHEN {match_expr} THEN {dialect.quote_str(SEGMENT_COMPANY)} "
        f"ELSE {dialect.quote_str(SEGMENT_INDIVIDUAL)} END"
    )


def build_customer_segments(con, dialect, config: SegmentationConfig = DEFAULT_SEGMENTATION_CONFIG) -> None:
    """
    Builds `customer_segments(customer_code, segment)` -- one row per
    customer_scalars row. Requires customer_scalars already built.
    Always creates the table (even with segmentation disabled, where
    every row is 'ALL') so downstream joins don't need to special-case
    its absence -- the elision that makes disabled segmentation free
    happens in the callers that USE this table (see
    engine.scoring.confidence.score_pair_segmented), not here.
    """
    expr = segment_sql_expr("name_norm", config, dialect)
    select_sql = f"SELECT customer_code, {expr} AS segment FROM customer_scalars"
    stmt = dialect.create_or_replace_table("customer_segments", select_sql)
    for part in stmt.split(";\n"):
        part = part.strip()
        if part:
            con.execute(part)


def segment_counts(con) -> dict:
    """{segment: count} -- used by the Settings UI's live Company/Individual split preview."""
    rows = con.execute("SELECT segment, COUNT(*) FROM customer_segments GROUP BY segment").fetchall()
    return {seg: n for seg, n in rows}
