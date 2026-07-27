"""
CUIN v2 - Ruleset Configuration Loader

Loads policies/ruleset_v2.yaml into a frozen, typed config object.
Frozen so a run's ruleset cannot be mutated mid-execution — any
change requires loading a new instance, which is itself an auditable
event (a new ruleset_fingerprint).
"""

from dataclasses import dataclass, field
from typing import Dict, List, Tuple
import yaml


@dataclass(frozen=True)
class DocumentTypeRule:
    charset: str = "alnum"      # numeric | alnum
    length: Tuple[int, ...] = ()
    pattern: str = ""


@dataclass(frozen=True)
class RulesetConfig:
    mobile_pattern: str
    mobile_min_distinct_digits: int
    email_pattern: str
    document_pattern: str
    document_types: Dict[str, DocumentTypeRule]
    document_default_min_length: int
    name_honorifics: Tuple[str, ...]
    dob_year_only_month_day: Tuple[int, int]
    address_min_chars: int
    address_min_tokens: int

    suppression_mobile_max: int
    suppression_email_max: int
    suppression_document_max: int
    suppression_name_token_max: int
    suppression_address_max: int

    auto_link_min_strong_only: int
    auto_link_min_strong_with_name: int
    review_min_strong: int

    max_cluster_size: int
    min_density: float

    raw: dict = field(repr=False, compare=False, default_factory=dict)


def _parse_document_types(raw: dict) -> Dict[str, DocumentTypeRule]:
    out = {}
    for type_name, spec in (raw or {}).items():
        length = spec.get("length", ())
        if isinstance(length, int):
            length = (length,)
        else:
            length = tuple(length)
        out[type_name] = DocumentTypeRule(
            charset=spec.get("charset", "alnum"),
            length=length,
            pattern=spec.get("pattern", ""),
        )
    return out


def load_ruleset(path: str = "policies/ruleset_v2.yaml") -> RulesetConfig:
    with open(path, "r") as f:
        raw = yaml.safe_load(f)

    identifiers = raw["identifiers"]
    suppression = raw["suppression"]
    decisioning = raw["decisioning"]
    clustering = raw["clustering"]

    auto_link_strong_only = None
    auto_link_strong_with_name = None
    for rule in decisioning["auto_link"]:
        if rule.get("require_medium_name"):
            auto_link_strong_with_name = rule["min_strong"]
        else:
            auto_link_strong_only = rule["min_strong"]

    review_min_strong = None
    for rule in decisioning["review"]:
        if "min_strong" in rule:
            review_min_strong = rule["min_strong"]

    return RulesetConfig(
        mobile_pattern=identifiers["mobile"]["pattern"],
        mobile_min_distinct_digits=identifiers["mobile"]["min_distinct_digits"],
        email_pattern=identifiers["email"]["pattern"],
        document_pattern=identifiers["document"]["pattern"],
        document_types=_parse_document_types(identifiers["document"]["types"]),
        document_default_min_length=identifiers["document"]["default_min_length"],
        name_honorifics=tuple(identifiers["name"]["honorifics"]),
        dob_year_only_month_day=tuple(identifiers["dob"]["year_only_month_day"]),
        address_min_chars=identifiers["address"]["min_chars"],
        address_min_tokens=identifiers["address"]["min_tokens"],
        suppression_mobile_max=suppression["mobile_max_records"],
        suppression_email_max=suppression["email_max_records"],
        suppression_document_max=suppression["document_max_records"],
        suppression_name_token_max=suppression["name_token_max_records"],
        suppression_address_max=suppression["address_max_records"],
        auto_link_min_strong_only=auto_link_strong_only,
        auto_link_min_strong_with_name=auto_link_strong_with_name,
        review_min_strong=review_min_strong,
        max_cluster_size=clustering["max_cluster_size"],
        min_density=clustering["min_density"],
        raw=raw,
    )


_default_ruleset = None


def get_default_ruleset() -> RulesetConfig:
    global _default_ruleset
    if _default_ruleset is None:
        _default_ruleset = load_ruleset()
    return _default_ruleset
