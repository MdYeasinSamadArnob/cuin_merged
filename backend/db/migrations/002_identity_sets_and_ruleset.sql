-- ============================================
-- CUIN v2 - Migration 002: Ruleset v2 support
-- ============================================
-- Adds ruleset/fingerprint tracking to runs, clusters, and
-- match_decisions so every persisted result is traceable to the exact
-- deterministic ruleset that produced it (engine.ruleset.version).
-- ============================================

ALTER TABLE runs ADD COLUMN IF NOT EXISTS ruleset_version VARCHAR(64);
ALTER TABLE runs ADD COLUMN IF NOT EXISTS input_fingerprint VARCHAR(64);
ALTER TABLE runs ADD COLUMN IF NOT EXISTS ruleset_fingerprint VARCHAR(64);
ALTER TABLE runs ADD COLUMN IF NOT EXISTS output_fingerprint VARCHAR(64);

ALTER TABLE match_decisions ADD COLUMN IF NOT EXISTS ruleset_version VARCHAR(64);

ALTER TABLE clusters ADD COLUMN IF NOT EXISTS ruleset_version VARCHAR(64);
ALTER TABLE clusters ADD COLUMN IF NOT EXISTS cohesion_density NUMERIC(5, 4);

-- ============================================
-- IDENTIFIER FREQUENCY TABLE
-- Persisted per run so an auditor can see exactly why a value was
-- treated as non-discriminative and suppressed from blocking/evidence
-- (engine.blocking.suppression).
-- ============================================
CREATE TABLE IF NOT EXISTS identifier_frequency (
    id SERIAL PRIMARY KEY,
    run_id UUID NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
    id_type VARCHAR(20) NOT NULL,
    value_norm TEXT NOT NULL,
    n_records INTEGER NOT NULL,
    is_suppressed BOOLEAN NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_identifier_frequency_run ON identifier_frequency(run_id);
CREATE INDEX IF NOT EXISTS idx_identifier_frequency_suppressed ON identifier_frequency(is_suppressed) WHERE is_suppressed;
