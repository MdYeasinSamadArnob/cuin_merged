-- ============================================
-- CUIN v2 - Migration 003: UI-editable rule catalog
-- ============================================
-- policy_versions already has maker-checker (created_by/approved_by),
-- content-hashing (policy_hash), and a "one active version" constraint
-- (one_active_policy) -- it was fully specified but never written to
-- (see engine.ruleset.config, which only ever read ruleset_v2.yaml).
-- This adds a JSON column so the Settings UI's blocking-rule catalog
-- and scoring/threshold rules can be persisted and versioned there
-- instead of requiring a YAML edit + redeploy.
-- ============================================

ALTER TABLE policy_versions ADD COLUMN IF NOT EXISTS rule_catalog_json JSONB;

COMMENT ON COLUMN policy_versions.rule_catalog_json IS
    'UI-authored {"blocking_rules": [...], "scoring_rules": {...}} -- see engine.rules.catalog/scoring_rules. NULL for versions that predate the rule-catalog UI (those are YAML-only).';
