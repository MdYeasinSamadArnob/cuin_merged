"""
CUIN v2 - Semantic Type Library

A closed vocabulary of "what kind of data lives in this column" for the
banker-facing Fields step of the Settings UI. Every semantic type
carries the information that step needs to render without exposing
engine internals: a plain-English label, whether values in a column of
this type are safe to use as an EXACT blocking key (a fuzzy/high-
cardinality-free-text key is a cartesian product waiting to happen --
see engine.rules.precheck), and which comparators make sense for it by
default when the field is used for matching (Phase 5's
engine.rules.comparators registry; the ids referenced here are a
forward-declared contract, not yet a runtime dependency).

This does NOT change how the pipeline actually normalizes/blocks/scores
data -- engine.normalize.explode_dialect and engine.blocking.
suppression_dialect are unchanged and still hardcoded to the six known
Oracle-export fields. This module only powers *discovery/profiling*
(engine.schema.discovery), which is read-only analysis over the raw
source Parquet. Making the actual pipeline mapping-driven from this
type library is a larger, separate change (see the migration plan's
Phase 4 note on explode_dialect/suppression_dialect) intentionally not
bundled here -- profiling is safe to ship on its own; rewriting the
proven-correct normalize pipeline to be schema-driven is not a change
to make in the same pass.
"""

from dataclasses import dataclass
from typing import List, Optional


@dataclass(frozen=True)
class SemanticType:
    id: str
    label: str
    description: str
    # Comparator ids a field of this type would default to if used for
    # MATCHING (not blocking) -- see engine.rules.comparators (Phase 5).
    default_comparators: List[str]
    # Safe to use as an EXACT-match blocking key without near-certain
    # explosion -- i.e. naturally high-cardinality per record (an ID,
    # a phone number) rather than a category shared by thousands of
    # records (branch code, city, document type).
    blocking_safe: bool
    is_multi_value: bool = False


SEMANTIC_TYPES = {
    "customer_id": SemanticType(
        id="customer_id", label="Customer / Record ID",
        description="A unique identifier for the record itself (primary key), not an attribute to match on.",
        default_comparators=["exact"], blocking_safe=True,
    ),
    "phone": SemanticType(
        id="phone", label="Phone number",
        description="A phone/mobile number. Often multi-valued per customer.",
        default_comparators=["exact", "set_intersect"], blocking_safe=True, is_multi_value=True,
    ),
    "email": SemanticType(
        id="email", label="Email address",
        description="An email address. Often multi-valued per customer.",
        default_comparators=["exact", "set_intersect"], blocking_safe=True, is_multi_value=True,
    ),
    "national_id": SemanticType(
        id="national_id", label="National ID / Document number",
        description="A government-issued identifier (NID, TIN, passport, etc.), usually typed and multi-valued.",
        default_comparators=["exact", "set_intersect"], blocking_safe=True, is_multi_value=True,
    ),
    "name": SemanticType(
        id="name", label="Person or entity name",
        description="A free-text name. High-cardinality but not exact-match safe -- the same person's name can be "
                     "spelled/ordered differently across records.",
        default_comparators=["token_jaccard", "token_containment", "trigram_jaccard", "phonetic"],
        blocking_safe=False,
    ),
    "date": SemanticType(
        id="date", label="Date",
        description="A calendar date (e.g. date of birth). Exact-match safe within a composite key, not alone "
                     "(shared by thousands of records).",
        default_comparators=["exact", "date_tolerance"], blocking_safe=False,
    ),
    "address": SemanticType(
        id="address", label="Address",
        description="A free-text postal address. Not blocking-safe on its own -- use as a matching signal only.",
        default_comparators=["token_containment", "token_jaccard"], blocking_safe=False, is_multi_value=True,
    ),
    "code": SemanticType(
        id="code", label="Category / Branch code",
        description="A low-cardinality classification code (branch, product, status). Shared by many records -- "
                     "NEVER safe as a standalone blocking key.",
        default_comparators=["exact"], blocking_safe=False,
    ),
    "numeric": SemanticType(
        id="numeric", label="Numeric value",
        description="A number (amount, count, age). Matches by exact value or a tolerance window.",
        default_comparators=["exact", "numeric_tolerance"], blocking_safe=False,
    ),
    "free_text": SemanticType(
        id="free_text", label="Free text",
        description="Unstructured text with no known matching semantics.",
        default_comparators=["token_jaccard"], blocking_safe=False,
    ),
    "unknown": SemanticType(
        id="unknown", label="Unclassified",
        description="Could not be confidently classified from the column name or sampled values.",
        default_comparators=[], blocking_safe=False,
    ),
}

# Column-name substrings -> semantic type id, checked in order (first
# match wins). Deliberately name-based, not purely statistical -- a
# bank's schema uses meaningful column names, and a banker reviewing
# this step can always override the guess, so a fast, explainable
# heuristic beats an opaque classifier here.
_NAME_HINTS = [
    (("customer_code", "customer_id", "cust_id", "record_id"), "customer_id"),
    (("mobile", "phone", "telephone", "contact_no"), "phone"),
    (("email",), "email"),
    (("document", "nid", "national_id", "passport", "tin"), "national_id"),
    (("branch", "status", "product", "category", "type_code"), "code"),
    (("name",), "name"),
    (("dob", "birth_date", "date_of_birth"), "date"),
    (("date",), "date"),
    (("address",), "address"),
]


def guess_semantic_type(column_name: str, parquet_type: str) -> str:
    """
    Name-based heuristic classifier. `parquet_type` currently only
    disambiguates numeric columns (a name-hint always wins when one
    matches, since a column literally named MOBILE is a phone number
    regardless of its storage type).
    """
    lower = column_name.lower()
    for hints, type_id in _NAME_HINTS:
        if any(h in lower for h in hints):
            return type_id
    if parquet_type.upper() in ("BIGINT", "INTEGER", "DOUBLE", "FLOAT", "DECIMAL", "HUGEINT", "SMALLINT"):
        return "numeric"
    return "free_text"


def get_semantic_type(type_id: str) -> Optional[SemanticType]:
    return SEMANTIC_TYPES.get(type_id)
