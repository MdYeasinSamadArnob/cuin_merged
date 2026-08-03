"""
CUIN v2 - Rule Catalog Persistence (policy_versions-backed)

Reads/writes the UI-editable blocking + scoring rule catalog through
the policy_versions table (maker-checker, one-active-version
constraint via `one_active_policy`, content hash) -- specified in full
in db/schema.sql but never written to before
db/migrations/003_rule_catalog.sql added `rule_catalog_json`.

Falls back to the YAML-equivalent hardcoded defaults when Postgres is
unreachable -- the same graceful-degradation pattern used throughout
this codebase (pipeline.doris_orchestrator._persist_to_postgres,
api/db_init.init_graph): a fresh checkout or a Postgres outage must
not stop the pipeline from running with sane defaults, only stop the
UI from persisting NEW edits.
"""

import hashlib
import json
import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from engine.rules.catalog import BlockingRule, DEFAULT_BLOCKING_RULES
from engine.rules.scoring_rules import ScoringRules, DEFAULT_SCORING_RULES
from engine.rules.match_rules import MatchRuleset, DEFAULT_MATCH_RULESET
from engine.segments.classifier import SegmentationConfig, DEFAULT_SEGMENTATION_CONFIG

logger = logging.getLogger(__name__)


@dataclass
class RuleCatalogVersion:
    policy_version: int
    blocking_rules: List[BlockingRule]
    match_ruleset: MatchRuleset
    is_active: bool
    created_by: str
    created_at: str
    approved_by: Optional[str]
    catalog_hash: str
    # Kept for backward-compat reads of pre-Stage-2 policy_versions rows
    # and any code path not yet migrated off it. No longer what a real
    # run's decision logic reads -- see engine.ruleset.effective and
    # engine.scoring.confidence.
    scoring_rules: ScoringRules = None
    # Stage 5: record segmentation (Company vs Individual). Disabled by
    # default -- see SegmentationConfig's docstring for why that's a
    # structural, not just behavioral, no-op.
    segmentation: SegmentationConfig = None
    # Per-segment confidence rules, keyed by segment id ("COMPANY" /
    # "INDIVIDUAL"). A segment with no entry here falls back to
    # `match_ruleset` -- see match_ruleset_for_segment(). Empty by
    # default: segmentation.enabled=False means every customer is
    # segment "ALL", which is never a key in this dict, so
    # match_ruleset alone is used regardless.
    match_rulesets_by_segment: Dict[str, MatchRuleset] = field(default_factory=dict)

    def __post_init__(self):
        if self.scoring_rules is None:
            self.scoring_rules = DEFAULT_SCORING_RULES
        if self.segmentation is None:
            self.segmentation = DEFAULT_SEGMENTATION_CONFIG

    def match_ruleset_for_segment(self, segment: str) -> MatchRuleset:
        return self.match_rulesets_by_segment.get(segment, self.match_ruleset)

    def to_dict(self) -> dict:
        return {
            "policy_version": self.policy_version,
            "blocking_rules": [r.to_dict() for r in self.blocking_rules],
            "match_ruleset": self.match_ruleset.to_dict(),
            "scoring_rules": self.scoring_rules.to_dict(),
            "segmentation": self.segmentation.to_dict(),
            "match_rulesets_by_segment": {k: v.to_dict() for k, v in self.match_rulesets_by_segment.items()},
            "is_active": self.is_active,
            "created_by": self.created_by,
            "created_at": self.created_at,
            "approved_by": self.approved_by,
            "catalog_hash": self.catalog_hash,
        }


def _catalog_hash(
    blocking_rules: List[BlockingRule], match_ruleset: MatchRuleset,
    segmentation: SegmentationConfig = None, match_rulesets_by_segment: Dict[str, MatchRuleset] = None,
) -> str:
    payload = json.dumps({
        "blocking_rules": [r.to_dict() for r in blocking_rules],
        "match_ruleset": match_ruleset.to_dict(),
        "segmentation": (segmentation or DEFAULT_SEGMENTATION_CONFIG).to_dict(),
        "match_rulesets_by_segment": {k: v.to_dict() for k, v in (match_rulesets_by_segment or {}).items()},
    }, sort_keys=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def default_catalog() -> Tuple[List[BlockingRule], MatchRuleset]:
    return list(DEFAULT_BLOCKING_RULES), DEFAULT_MATCH_RULESET


def _connect():
    from api.config import settings
    import psycopg2
    return psycopg2.connect(settings.DATABASE_URL)


def _match_ruleset_from_json(rule_json: dict) -> MatchRuleset:
    """
    rule_catalog_json rows saved before Stage 2 have no "match_ruleset"
    key -- migrate their (legacy) scoring_rules on read rather than
    crashing. See engine.rules.match_rules.migrate_scoring_rules.
    """
    if "match_ruleset" in rule_json:
        return MatchRuleset.from_dict(rule_json["match_ruleset"])
    from engine.rules.match_rules import migrate_scoring_rules
    legacy_scoring = ScoringRules.from_dict(rule_json.get("scoring_rules", {}))
    return migrate_scoring_rules(legacy_scoring)


def _segmentation_from_json(rule_json: dict) -> SegmentationConfig:
    """Pre-Stage-5 rows have no "segmentation" key -- default is disabled, a byte-identical no-op."""
    return SegmentationConfig.from_dict(rule_json.get("segmentation"))


def _match_rulesets_by_segment_from_json(rule_json: dict) -> Dict[str, MatchRuleset]:
    raw = rule_json.get("match_rulesets_by_segment") or {}
    return {k: MatchRuleset.from_dict(v) for k, v in raw.items()}


def get_active_catalog(pg_conn=None) -> RuleCatalogVersion:
    """
    Returns the currently active rule catalog. Falls back to the
    hardcoded defaults (policy_version=0, matching ruleset_v2.yaml
    byte-for-byte) if Postgres is unreachable or no version has ever
    been activated.
    """
    conn = pg_conn
    owns_conn = False
    try:
        if conn is None:
            conn = _connect()
            owns_conn = True

        with conn.cursor() as cur:
            cur.execute("""
                SELECT policy_version, rule_catalog_json, created_by, created_at, approved_by, policy_hash
                FROM policy_versions
                WHERE is_active = TRUE
                LIMIT 1
            """)
            row = cur.fetchone()

        if not row or not row[1]:
            blocking_rules, match_ruleset = default_catalog()
            return RuleCatalogVersion(
                policy_version=0, blocking_rules=blocking_rules, match_ruleset=match_ruleset,
                is_active=True, created_by="system", created_at="",
                approved_by=None, catalog_hash=_catalog_hash(blocking_rules, match_ruleset),
            )

        policy_version, rule_json, created_by, created_at, approved_by, policy_hash = row
        blocking_rules = [BlockingRule.from_dict(d) for d in rule_json.get("blocking_rules", [])]
        match_ruleset = _match_ruleset_from_json(rule_json)
        if not blocking_rules:
            blocking_rules, _ = default_catalog()
        return RuleCatalogVersion(
            policy_version=policy_version, blocking_rules=blocking_rules, match_ruleset=match_ruleset,
            is_active=True, created_by=created_by, created_at=str(created_at),
            approved_by=approved_by, catalog_hash=policy_hash,
            segmentation=_segmentation_from_json(rule_json),
            match_rulesets_by_segment=_match_rulesets_by_segment_from_json(rule_json),
        )
    except Exception as e:
        logger.warning(f"Could not load active rule catalog from Postgres, using defaults: {e}")
        blocking_rules, match_ruleset = default_catalog()
        return RuleCatalogVersion(
            policy_version=0, blocking_rules=blocking_rules, match_ruleset=match_ruleset,
            is_active=True, created_by="system", created_at="",
            approved_by=None, catalog_hash=_catalog_hash(blocking_rules, match_ruleset),
        )
    finally:
        if owns_conn and conn:
            conn.close()


def save_catalog(
    blocking_rules: List[BlockingRule],
    match_ruleset: MatchRuleset,
    created_by: str,
    activate: bool = True,
    pg_conn=None,
    segmentation: SegmentationConfig = None,
    match_rulesets_by_segment: Dict[str, MatchRuleset] = None,
) -> RuleCatalogVersion:
    """
    Inserts a new policy_versions row holding the given catalog. If
    activate=True, deactivates whatever version is currently active
    and activates this one in the same transaction -- the
    `one_active_policy` EXCLUDE constraint requires the deactivation
    to commit-or-fail together with the activation.
    """
    segmentation = segmentation or DEFAULT_SEGMENTATION_CONFIG
    match_rulesets_by_segment = match_rulesets_by_segment or {}

    for rule in blocking_rules:
        rule.validate()
    for match_rule in match_ruleset.match_rules:
        match_rule.validate()
    for seg_ruleset in match_rulesets_by_segment.values():
        for match_rule in seg_ruleset.match_rules:
            match_rule.validate()

    rule_json = {
        "blocking_rules": [r.to_dict() for r in blocking_rules],
        "match_ruleset": match_ruleset.to_dict(),
        "segmentation": segmentation.to_dict(),
        "match_rulesets_by_segment": {k: v.to_dict() for k, v in match_rulesets_by_segment.items()},
    }
    catalog_hash = _catalog_hash(blocking_rules, match_ruleset, segmentation, match_rulesets_by_segment)

    conn = pg_conn
    owns_conn = False
    if conn is None:
        conn = _connect()
        owns_conn = True

    try:
        with conn.cursor() as cur:
            if activate:
                cur.execute("UPDATE policy_versions SET is_active = FALSE WHERE is_active = TRUE")

            cur.execute("""
                INSERT INTO policy_versions
                    (policy_yaml, policy_hash, rule_catalog_json, created_by, is_active)
                VALUES (%s, %s, %s, %s, %s)
                RETURNING policy_version, created_at
            """, (
                "-- managed via rule_catalog_json, see engine.rules.store --",
                catalog_hash,
                json.dumps(rule_json),
                created_by,
                activate,
            ))
            policy_version, created_at = cur.fetchone()
        conn.commit()

        return RuleCatalogVersion(
            policy_version=policy_version, blocking_rules=blocking_rules, match_ruleset=match_ruleset,
            is_active=activate, created_by=created_by, created_at=str(created_at),
            approved_by=None, catalog_hash=catalog_hash,
            segmentation=segmentation, match_rulesets_by_segment=match_rulesets_by_segment,
        )
    finally:
        if owns_conn:
            conn.close()


def list_catalog_versions(pg_conn=None, limit: int = 50) -> List[dict]:
    conn = pg_conn
    owns_conn = False
    try:
        if conn is None:
            conn = _connect()
            owns_conn = True

        with conn.cursor() as cur:
            cur.execute("""
                SELECT policy_version, policy_hash, created_by, created_at, approved_by, approved_at, is_active
                FROM policy_versions
                WHERE rule_catalog_json IS NOT NULL
                ORDER BY policy_version DESC
                LIMIT %s
            """, (limit,))
            rows = cur.fetchall()

        return [
            {
                "policy_version": r[0], "policy_hash": r[1], "created_by": r[2],
                "created_at": str(r[3]), "approved_by": r[4],
                "approved_at": str(r[5]) if r[5] else None, "is_active": r[6],
            }
            for r in rows
        ]
    except Exception as e:
        logger.warning(f"Could not list rule catalog versions: {e}")
        return []
    finally:
        if owns_conn and conn:
            conn.close()


def get_version(policy_version: int, pg_conn=None) -> Optional[RuleCatalogVersion]:
    conn = pg_conn
    owns_conn = False
    try:
        if conn is None:
            conn = _connect()
            owns_conn = True

        with conn.cursor() as cur:
            cur.execute("""
                SELECT policy_version, rule_catalog_json, created_by, created_at, approved_by, policy_hash, is_active
                FROM policy_versions WHERE policy_version = %s
            """, (policy_version,))
            row = cur.fetchone()

        if not row or not row[1]:
            return None
        pv, rule_json, created_by, created_at, approved_by, policy_hash, is_active = row
        return RuleCatalogVersion(
            policy_version=pv,
            blocking_rules=[BlockingRule.from_dict(d) for d in rule_json.get("blocking_rules", [])],
            match_ruleset=_match_ruleset_from_json(rule_json),
            is_active=is_active, created_by=created_by, created_at=str(created_at),
            approved_by=approved_by, catalog_hash=policy_hash,
            segmentation=_segmentation_from_json(rule_json),
            match_rulesets_by_segment=_match_rulesets_by_segment_from_json(rule_json),
        )
    except Exception as e:
        logger.warning(f"Could not load rule catalog version {policy_version}: {e}")
        return None
    finally:
        if owns_conn and conn:
            conn.close()


def activate_version(policy_version: int, approved_by: str, pg_conn=None) -> None:
    conn = pg_conn
    owns_conn = False
    if conn is None:
        conn = _connect()
        owns_conn = True

    try:
        with conn.cursor() as cur:
            cur.execute("UPDATE policy_versions SET is_active = FALSE WHERE is_active = TRUE")
            cur.execute("""
                UPDATE policy_versions
                SET is_active = TRUE, approved_by = %s, approved_at = NOW()
                WHERE policy_version = %s
            """, (approved_by, policy_version))
        conn.commit()
    finally:
        if owns_conn:
            conn.close()
