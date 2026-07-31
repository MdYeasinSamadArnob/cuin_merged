"""
CUIN v2 - Blocking Rule Catalog (rules as data)

Replaces the two hardcoded self-join passes in
engine.blocking.deterministic_blocker with a typed, ordered,
UI-editable catalog. Every rule is DATA -- a dataclass with a fixed
set of typed fields/params -- never a raw SQL string from the client.
engine.rules.compiler turns a BlockingRule into SQL through
engine.ports.dialect.SqlDialect, so the compiled query is always
well-formed and injection-free regardless of what the UI sends.

DEFAULT_BLOCKING_RULES reproduces today's two hardcoded blocking
passes byte-for-byte (same predicates, same guards, same `reason`
strings emitted per matched pair) -- see
tests/unit/test_rules_zero_change.py, the acceptance test that proves
switching to the rule catalog does not change a single pipeline
output.
"""

import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, Optional, Tuple

_RULE_ID_RE = re.compile(r"^[a-z][a-z0-9_]{0,63}$")

# Column-name safety for RAW_COLUMN rules (see below): uppercase
# letters/digits/underscore only, starting with a letter. Not a
# closed vocabulary (any real source column is valid) -- this is an
# INJECTION guard, not a semantic allowlist, since raw column names
# are interpolated directly into the self-join SQL
# (engine.rules.compiler._compile_raw_column).
_RAW_COLUMN_RE = re.compile(r"^[A-Z][A-Z0-9_]{0,63}$")


class BlockingRuleType(str, Enum):
    # Self-join on an exact normalized identifier value shared by both
    # sides (mirrors deterministic_blocker.build_candidate_pairs pass 1).
    EXACT_IDENTIFIER = "exact_identifier"

    # Self-join on a composite key of >=2 scalar/derived fields, with an
    # optional minimum-cardinality guard on one of them (mirrors pass 2:
    # name_key + dob_iso, guarded by len(rare_tokens) >= 2).
    COMPOSITE_KEY = "composite_key"

    # Explodes an array field (e.g. name_tokens) and self-joins on a
    # single element -- broader recall than COMPOSITE_KEY, no dob
    # requirement. Not used by the default ruleset; available for a
    # bank that wants token-level blocking.
    TOKEN_KEY = "token_key"

    # Self-joins on the first N characters of a scalar field.
    PREFIX_KEY = "prefix_key"

    # Self-joins on a date part (year/month) extracted from a date
    # field -- coarser than an exact-date COMPOSITE_KEY.
    DATE_PART_KEY = "date_part_key"

    # Self-joins directly on a RAW source column (any column
    # engine.schema.discovery.profile_source() reports), scalar or
    # multi-valued -- bypasses the normalize pipeline entirely (no
    # validation, just upper/trim). This is what makes "any discovered
    # column" reachable for blocking without first threading it through
    # engine.normalize.explode's closed 6-column vocabulary. Value is
    # exact-match on the trimmed, uppercased raw string -- a banker who
    # needs real normalization (phone formats, document type parsing)
    # for a NEW column should ask for it to be added to the normalize
    # pipeline properly, not lean on this as a substitute.
    RAW_COLUMN = "raw_column"


# Fields each rule type is defined over, and what table it reads from.
# The compiler uses this to validate a rule before compiling it --
# unknown fields/tables never reach SQL generation.
SOURCE_TABLE_BY_TYPE: Dict[BlockingRuleType, str] = {
    BlockingRuleType.EXACT_IDENTIFIER: "identifiers",
    BlockingRuleType.COMPOSITE_KEY: "customer_scalars",
    BlockingRuleType.TOKEN_KEY: "customer_scalars",
    BlockingRuleType.PREFIX_KEY: "customer_scalars",
    BlockingRuleType.DATE_PART_KEY: "customer_scalars",
    BlockingRuleType.RAW_COLUMN: "raw",
}

# Every field any rule type is allowed to reference. The catalog is a
# closed vocabulary over the tables built by engine.normalize.explode
# and engine.blocking.suppression -- never an arbitrary column name.
ALLOWED_FIELDS: Dict[str, Tuple[str, ...]] = {
    "identifiers": ("id_type", "value_norm", "customer_code", "is_valid", "is_suppressed"),
    "customer_scalars": ("name_norm", "name_tokens", "name_key", "dob_iso", "dob_precision", "customer_code"),
}

VALID_ID_TYPES: Tuple[str, ...] = ("mobile", "email", "document", "address")
VALID_DATE_PARTS: Tuple[str, ...] = ("year", "month", "day")


@dataclass(frozen=True)
class RuleGuards:
    """
    Explosion guards, applied BEFORE the self-join runs -- a key whose
    frequency exceeds a guard never enters candidate generation, it is
    simply excluded (mirrors engine.blocking.suppression's approach:
    an absolute count, not a percentage, since a percentage of 1.5M+
    rows never fires in practice). `None` means "no cap for this guard".
    """
    max_key_frequency: Optional[int] = None   # atomic key's global frequency across all records
    max_block_size: Optional[int] = None       # size of the actual JOINED block (the guard that matters)
    min_key_parts: Optional[int] = None        # e.g. require >= N rare tokens before the key is usable

    def to_dict(self) -> dict:
        return {
            "max_key_frequency": self.max_key_frequency,
            "max_block_size": self.max_block_size,
            "min_key_parts": self.min_key_parts,
        }

    @staticmethod
    def from_dict(d: Optional[dict]) -> "RuleGuards":
        d = d or {}
        return RuleGuards(
            max_key_frequency=d.get("max_key_frequency"),
            max_block_size=d.get("max_block_size"),
            min_key_parts=d.get("min_key_parts"),
        )


@dataclass(frozen=True)
class BlockingRule:
    rule_id: str
    type: BlockingRuleType
    enabled: bool = True
    order: int = 0
    label: str = ""
    # Meaning depends on `type` -- see BlockingRuleType docstrings above:
    #   EXACT_IDENTIFIER -> identifiers.id_type values to include, e.g. ("mobile","email","document")
    #   COMPOSITE_KEY     -> customer_scalars columns forming the key, e.g. ("name_key","dob_iso")
    #   TOKEN_KEY         -> the array column to explode, e.g. ("name_tokens",)
    #   PREFIX_KEY        -> the scalar column to prefix, e.g. ("name_norm",)
    #   DATE_PART_KEY     -> (date_column, part), e.g. ("dob_iso", "year")
    fields: Tuple[str, ...] = ()
    params: Dict[str, Any] = field(default_factory=dict)
    guards: RuleGuards = field(default_factory=RuleGuards)
    description: str = ""

    @property
    def source_table(self) -> str:
        return SOURCE_TABLE_BY_TYPE[self.type]

    def validate(self) -> None:
        """
        Raises ValueError if this rule references anything outside the
        closed catalog vocabulary. `rule_id` is used verbatim to build
        SQL identifiers for this rule's staging tables (engine.rules.
        compiler), so it is restricted to a safe slug -- this is what
        makes the catalog injection-free even though it is user-edited
        via the Settings UI.
        """
        if not self.rule_id or not _RULE_ID_RE.match(self.rule_id):
            raise ValueError(
                f"rule_id '{self.rule_id}' must match {_RULE_ID_RE.pattern} "
                "(lowercase letters, digits, underscore, starting with a letter)"
            )

        if self.type == BlockingRuleType.RAW_COLUMN:
            if len(self.fields) != 1:
                raise ValueError(f"{self.rule_id}: raw_column requires exactly one column name")
            if not _RAW_COLUMN_RE.match(self.fields[0]):
                raise ValueError(
                    f"{self.rule_id}: '{self.fields[0]}' is not a safe column name "
                    f"({_RAW_COLUMN_RE.pattern})"
                )
            return

        allowed = ALLOWED_FIELDS[self.source_table]

        if self.type == BlockingRuleType.EXACT_IDENTIFIER:
            if not self.fields:
                raise ValueError(f"{self.rule_id}: exact_identifier requires at least one id_type in fields")
            for id_type in self.fields:
                if id_type not in VALID_ID_TYPES:
                    raise ValueError(f"{self.rule_id}: unknown id_type '{id_type}'")

        elif self.type == BlockingRuleType.COMPOSITE_KEY:
            if len(self.fields) < 2:
                raise ValueError(f"{self.rule_id}: composite_key requires >= 2 fields")
            for f in self.fields:
                if f not in allowed and f != "name_key":
                    raise ValueError(f"{self.rule_id}: unknown field '{f}' for customer_scalars")

        elif self.type == BlockingRuleType.TOKEN_KEY:
            if len(self.fields) != 1:
                raise ValueError(f"{self.rule_id}: token_key requires exactly one array field")
            if self.fields[0] not in allowed:
                raise ValueError(f"{self.rule_id}: unknown field '{self.fields[0]}'")

        elif self.type == BlockingRuleType.PREFIX_KEY:
            if len(self.fields) != 1:
                raise ValueError(f"{self.rule_id}: prefix_key requires exactly one field")
            if self.fields[0] not in allowed:
                raise ValueError(f"{self.rule_id}: unknown field '{self.fields[0]}'")
            if not isinstance(self.params.get("prefix_len"), int) or self.params["prefix_len"] < 1:
                raise ValueError(f"{self.rule_id}: prefix_key requires an integer params.prefix_len >= 1")

        elif self.type == BlockingRuleType.DATE_PART_KEY:
            if len(self.fields) != 2:
                raise ValueError(f"{self.rule_id}: date_part_key requires (date_field, part)")
            date_field, part = self.fields
            if date_field not in allowed:
                raise ValueError(f"{self.rule_id}: unknown field '{date_field}'")
            if part not in VALID_DATE_PARTS:
                raise ValueError(f"{self.rule_id}: unknown date part '{part}'")

    def to_dict(self) -> dict:
        return {
            "rule_id": self.rule_id,
            "type": self.type.value,
            "enabled": self.enabled,
            "order": self.order,
            "label": self.label,
            "fields": list(self.fields),
            "params": self.params,
            "guards": self.guards.to_dict(),
            "description": self.description,
        }

    @staticmethod
    def from_dict(d: dict) -> "BlockingRule":
        return BlockingRule(
            rule_id=d["rule_id"],
            type=BlockingRuleType(d["type"]),
            enabled=d.get("enabled", True),
            order=d.get("order", 0),
            label=d.get("label", d["rule_id"]),
            fields=tuple(d.get("fields", ())),
            params=d.get("params", {}) or {},
            guards=RuleGuards.from_dict(d.get("guards")),
            description=d.get("description", ""),
        )


# ----------------------------------------------------------------------
# Default catalog -- reproduces engine.blocking.deterministic_blocker's
# two hardcoded passes exactly. This is the seed used when no
# UI-authored rule catalog has been saved yet (see engine.rules.store).
# ----------------------------------------------------------------------

DEFAULT_BLOCKING_RULES: Tuple[BlockingRule, ...] = (
    BlockingRule(
        rule_id="exact_identifier_strong",
        type=BlockingRuleType.EXACT_IDENTIFIER,
        enabled=True,
        order=1,
        label="Exact identifier match (mobile / email / document)",
        fields=("mobile", "email", "document"),
        params={},
        guards=RuleGuards(),
        description=(
            "Self-joins validated, non-suppressed identifiers on exact "
            "normalized value within the same id_type. 'address' is "
            "deliberately excluded -- it is WEAK-tier evidence only, so "
            "an address-only pair can never AUTO_LINK or REVIEW; "
            "generating candidates from it is pure wasted compute."
        ),
    ),
    BlockingRule(
        # rule_id doubles as the audit `reason` prefix (see
        # engine.rules.compiler._compile_composite_key) -- kept as
        # "name_dob" (not "name_dob_composite") so the default catalog
        # reproduces deterministic_blocker.build_candidate_pairs's
        # historical reason strings byte-for-byte.
        rule_id="name_dob",
        type=BlockingRuleType.COMPOSITE_KEY,
        enabled=True,
        order=2,
        label="Name + full-precision DOB composite",
        fields=("name_key", "dob_iso"),
        params={"require_dob_precision": "FULL"},
        guards=RuleGuards(max_key_frequency=1500, max_block_size=20, min_key_parts=2),
        description=(
            "Self-joins on md5(sorted rare name tokens) + exact dob_iso. "
            "Requires >=2 rare tokens per side (guards against a single "
            "common name forming a mega-block) and FULL DOB precision "
            "(not a Jan-1 'year known only' stub) -- exactly as selective "
            "as tiers.classify()'s medium_dob check, so no candidate pair "
            "is generated that could never satisfy it."
        ),
    ),
)
