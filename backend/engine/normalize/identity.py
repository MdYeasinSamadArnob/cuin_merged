"""
CUIN v2 - Identity Normalization & Validation (Ruleset v2)

Every function returns (value, reject_reason). A rejection is always
explicit and auditable — never a silent None with no explanation.
This is the deterministic replacement for the array-blind, threshold-
free normalization in engine.normalize.standardize.

Reused as-is from engine.normalize.standardize:
  - the NFD accent-strip + punctuation removal shape of normalize_name
  - the multi-format date parsing list in normalize_dob
  - the abbreviation-substitution machinery in normalize_address
  - compute_record_hash's sha256("|".join(...)) idiom (see engine.determinism)

Deliberately NOT reused (each was measured to be wrong for this data):
  - normalize_phone: "keep last 10 digits" drops the identifying leading
    0 for BD numbers and accepts any >=7 digit string, so "0000000123"-
    style junk survives.
  - normalize_natid: strips all separators, fusing "TIN:574352752846"
    into "TIN574352752846" with no per-type validation.
  - normalize_record: maps CUSCOD/MOBLNO/NATLID field names that don't
    exist in this dataset's schema.
"""

import re
import unicodedata
from typing import Optional, Tuple, List

from engine.ruleset.config import RulesetConfig, get_default_ruleset

RejectReason = Optional[str]


# ----------------------------------------------------------------
# Mobile (Bangladesh)
# ----------------------------------------------------------------

def norm_mobile_bd(
    raw: Optional[str], ruleset: Optional[RulesetConfig] = None
) -> Tuple[Optional[str], RejectReason]:
    """
    Normalize and validate a Bangladeshi mobile number.

    Accepts +880/880/0 country-code variants, normalizes to the local
    11-digit 01[3-9]XXXXXXXX form, and structurally rejects round-number
    junk (01700000000, 0000000000, "123") by requiring the 8-digit
    subscriber part to contain enough distinct digits — no blacklist,
    so it generalizes to junk values not yet seen.
    """
    ruleset = ruleset or get_default_ruleset()

    if not raw or not isinstance(raw, str):
        return None, "empty"

    digits = re.sub(r"\D", "", raw)
    if not digits:
        return None, "no_digits"

    # Strip country code variants: 880XXXXXXXXXX -> 0XXXXXXXXXX
    if digits.startswith("880") and len(digits) == 13:
        digits = "0" + digits[3:]
    elif digits.startswith("880") and len(digits) == 12:
        digits = "0" + digits[3:]
    elif len(digits) == 10 and digits[0] == "1" and digits[1] in "3456789":
        # missing leading 0 (e.g. "1712345678" -> "01712345678")
        digits = "0" + digits

    if not re.match(ruleset.mobile_pattern, digits):
        return None, "invalid_format"

    subscriber_part = digits[3:]  # the 8 digits after "01X"
    if len(set(subscriber_part)) < ruleset.mobile_min_distinct_digits:
        return None, "low_entropy_junk"

    return digits, None


# ----------------------------------------------------------------
# Email
# ----------------------------------------------------------------

def norm_email(
    raw: Optional[str], ruleset: Optional[RulesetConfig] = None
) -> Tuple[Optional[str], RejectReason]:
    ruleset = ruleset or get_default_ruleset()

    if not raw or not isinstance(raw, str):
        return None, "empty"

    email = raw.strip().lower()
    if not re.match(ruleset.email_pattern, email):
        return None, "invalid_format"

    return email, None


# ----------------------------------------------------------------
# National document (format "TYPE:VALUE")
# ----------------------------------------------------------------

def parse_document(
    raw: Optional[str], ruleset: Optional[RulesetConfig] = None
) -> Tuple[Optional[str], Optional[str], RejectReason]:
    """
    Returns (doc_type, value_norm, reject_reason).

    Junk like "TIN:NO", "TIN:NO TIN", "NAI:123", "OTH:123",
    "EI:Enterprise Info" is rejected by per-type length/charset checks,
    not by a hardcoded blacklist — the same structural rule catches
    any future placeholder in the same shape.
    """
    ruleset = ruleset or get_default_ruleset()

    if not raw or not isinstance(raw, str):
        return None, None, "empty"

    m = re.match(ruleset.document_pattern, raw.strip().upper())
    if not m:
        return None, None, "unparseable_format"

    doc_type, value = m.group(1), m.group(2).strip()
    value = re.sub(r"[\s\-\.]", "", value)

    if not value:
        return doc_type, None, "empty_value"

    type_rule = ruleset.document_types.get(doc_type)

    if type_rule is not None:
        if type_rule.pattern:
            if not re.match(type_rule.pattern, value):
                return doc_type, None, "pattern_mismatch"
        else:
            if type_rule.charset == "numeric" and not value.isdigit():
                return doc_type, None, "non_numeric"
            if type_rule.length and len(value) not in type_rule.length:
                return doc_type, None, "wrong_length"
    else:
        # Unknown type: generic min-length + alnum guard. Real IDs
        # (TIN/NID/passport/BRC numbers) always contain digits; a value
        # with none is boilerplate text masquerading as an ID value
        # (e.g. "EI:Enterprise Info" -> "ENTERPRISEINFO").
        if len(value) < ruleset.document_default_min_length:
            return doc_type, None, "too_short"
        if not value.isalnum():
            return doc_type, None, "non_alnum"
        if not any(c.isdigit() for c in value):
            return doc_type, None, "no_digits_boilerplate"

    return doc_type, value, None


# ----------------------------------------------------------------
# Name
# ----------------------------------------------------------------

def _strip_accents_punct(name: str) -> str:
    name = unicodedata.normalize("NFD", name)
    name = "".join(c for c in name if unicodedata.category(c) != "Mn")
    name = re.sub(r"[^\w\s-]", "", name)
    name = name.replace("-", " ")
    return re.sub(r"\s+", " ", name).strip()


def norm_name(
    raw: Optional[str], ruleset: Optional[RulesetConfig] = None
) -> Tuple[Optional[str], List[str]]:
    """
    Returns (name_norm, rare_tokens) where rare_tokens has honorifics
    stripped. An all-honorific name ("MD.", "MST.") correctly yields
    an EMPTY token list — such records get no name-based blocking key,
    which is the intended behavior (see tests/unit/test_identity_normalization.py).
    """
    ruleset = ruleset or get_default_ruleset()

    if not raw or not isinstance(raw, str):
        return None, []

    name = _strip_accents_punct(raw.upper().strip())
    if not name:
        return None, []

    tokens = name.split(" ")
    honorifics = set(h.rstrip(".") for h in ruleset.name_honorifics)

    # Strip a LEADING run of honorific tokens only (not tokens elsewhere,
    # since "RAHMAN" etc. are legitimate surnames, not honorifics).
    i = 0
    while i < len(tokens) and tokens[i].rstrip(".") in honorifics:
        i += 1
    rare_tokens = [t for t in tokens[i:] if t]

    return name, rare_tokens


# ----------------------------------------------------------------
# Date of birth
# ----------------------------------------------------------------

from datetime import datetime

_DOB_FORMATS = [
    "%Y-%m-%dT%H:%M:%S",  # parquet's ISO datetime string form
    "%Y-%m-%d",
    "%d-%m-%Y",
    "%m-%d-%Y",
    "%Y/%m/%d",
    "%d/%m/%Y",
    "%m/%d/%Y",
    "%d.%m.%Y",
    "%Y%m%d",
    "%d %b %Y",
    "%d %B %Y",
]


def norm_dob(
    raw: Optional[str], ruleset: Optional[RulesetConfig] = None
) -> Tuple[Optional[str], Optional[str]]:
    """
    Returns (iso_date, precision) where precision is "FULL" or
    "YEAR_ONLY". YEAR_ONLY is assigned when month/day == (1, 1) — a
    very common "year known, day/month defaulted" placeholder pattern
    confirmed in this dataset (all top-10 most frequent BIRTH_DATE
    values are Jan-1 stubs).
    """
    ruleset = ruleset or get_default_ruleset()

    if not raw or not isinstance(raw, str):
        return None, None

    dob = raw.strip()
    for fmt in _DOB_FORMATS:
        try:
            parsed = datetime.strptime(dob, fmt)
        except ValueError:
            continue
        if not (1900 <= parsed.year <= 2030):
            continue
        precision = (
            "YEAR_ONLY"
            if (parsed.month, parsed.day) == tuple(ruleset.dob_year_only_month_day)
            else "FULL"
        )
        return parsed.strftime("%Y-%m-%d"), precision

    return None, None


# ----------------------------------------------------------------
# Address
# ----------------------------------------------------------------

_BD_ADDRESS_ABBREV = {
    r"\bROAD\b": "RD",
    r"\bSTREET\b": "ST",
    r"\bAVENUE\b": "AVE",
    r"\bBUILDING\b": "BLDG",
    r"\bFLOOR\b": "FL",
    r"\bAPARTMENT\b": "APT",
    r"\bBLOCK\b": "BLK",
    r"\bWARD\b": "WD",
    r"\bUPAZILA\b": "UPZ",
    r"\bDISTRICT\b": "DIST",
    r"\bNORTH\b": "N",
    r"\bSOUTH\b": "S",
    r"\bEAST\b": "E",
    r"\bWEST\b": "W",
}

_ADDRESS_BOILERPLATE = {
    "DO", "SAME", "NA", "N/A", "SAME AS PRESENT ADDRESS",
    "SAME ADDRESS", "AS ABOVE", "PRESENT ADDRESS", "PERMANENT ADDRESS",
}


def norm_address(
    raw: Optional[str], ruleset: Optional[RulesetConfig] = None
) -> Optional[str]:
    ruleset = ruleset or get_default_ruleset()

    if not raw or not isinstance(raw, str):
        return None

    address = raw.upper().strip()
    address = unicodedata.normalize("NFD", address)
    address = "".join(c for c in address if unicodedata.category(c) != "Mn")

    for pattern, repl in _BD_ADDRESS_ABBREV.items():
        address = re.sub(pattern, repl, address)

    address = re.sub(r"[^\w\s]", " ", address)
    address = re.sub(r"\s+", " ", address).strip()

    if not address or address in _ADDRESS_BOILERPLATE:
        return None
    if len(address) < ruleset.address_min_chars:
        return None
    if len(address.split(" ")) < ruleset.address_min_tokens:
        return None

    return address
