-- ============================================
-- CUIN v2 - Migration 008: Public Identity Recognition API
-- ============================================
-- Backs a new bank-facing, real-time "does this person already exist"
-- screening API (backend/api/routes_public_identity_api.py), mounted
-- as its own versioned sub-application with its own Swagger UI at
-- /api/v1/docs -- deliberately kept separate from the internal admin/
-- workbench surface so a bank integration partner only ever sees this
-- one contract, never the ~20 internal routers.
--
-- Two tables:
-- - api_keys: issued credentials for calling the public API. Only a
--   SHA-256 hash of the key is stored -- the raw key is shown to the
--   caller exactly once, at creation time (see POST /admin/api-keys),
--   matching how every real API-key-issuing platform works (Stripe,
--   GitHub, etc). Never store the raw key.
-- - api_screening_log: an audit trail of every screening call, for
--   both the bank's own compliance record and CUIN-side traceability.
--   Deliberately does NOT store the raw submitted PII a second time
--   (that already lives once in customer_scalars/identifiers) -- just
--   which fields were supplied and what was decided.
-- ============================================

CREATE TABLE IF NOT EXISTS api_keys (
    key_id             UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    key_hash           VARCHAR(64) NOT NULL UNIQUE,  -- sha256 hex digest of the raw key
    label              VARCHAR(128) NOT NULL,
    is_active          BOOLEAN NOT NULL DEFAULT TRUE,
    rate_limit_per_min INTEGER NOT NULL DEFAULT 60,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by         VARCHAR(128),
    last_used_at       TIMESTAMPTZ,
    revoked_at         TIMESTAMPTZ,
    revoked_by         VARCHAR(128)
);

CREATE INDEX IF NOT EXISTS idx_api_keys_active ON api_keys(is_active);

CREATE TABLE IF NOT EXISTS api_screening_log (
    query_id                  UUID PRIMARY KEY,
    api_key_id                UUID REFERENCES api_keys(key_id),
    run_id                    UUID,
    engine                    VARCHAR(16),
    requested_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    segment                   VARCHAR(16),
    fields_provided           TEXT[] NOT NULL DEFAULT '{}',
    candidate_pool_size       INTEGER NOT NULL DEFAULT 0,
    candidates_returned       INTEGER NOT NULL DEFAULT 0,
    already_known             BOOLEAN NOT NULL,
    recommendation            VARCHAR(32) NOT NULL,
    best_match_entity_id      UUID,
    best_match_confidence_pct NUMERIC(5,2),
    response_ms               INTEGER
);

CREATE INDEX IF NOT EXISTS idx_api_screening_log_requested_at ON api_screening_log (requested_at DESC);
CREATE INDEX IF NOT EXISTS idx_api_screening_log_api_key ON api_screening_log (api_key_id);

-- Seed a single default development key so the Swagger UI / a fresh
-- checkout works out of the box: raw key is
-- "cuin_demo_screening_key_dev_only" (surfaced in the /api page's
-- Getting Started panel and in the public API's OpenAPI description).
-- Revoke this before any real deployment via DELETE /admin/api-keys/{id}.
INSERT INTO api_keys (key_id, key_hash, label, is_active, rate_limit_per_min, created_by)
SELECT
    'd0000000-0000-0000-0000-000000000001',
    encode(sha256('cuin_demo_screening_key_dev_only'::bytea), 'hex'),
    'default-dev-key',
    TRUE,
    120,
    'SYSTEM_MIGRATION'
WHERE NOT EXISTS (SELECT 1 FROM api_keys WHERE key_id = 'd0000000-0000-0000-0000-000000000001');

COMMENT ON TABLE api_keys IS
    'Credentials for the public Identity Recognition API (/api/v1). Only key_hash (sha256) is stored; raw keys are shown once at creation.';
COMMENT ON TABLE api_screening_log IS
    'Audit trail of every /api/v1/identity/screen call -- traceability for banks and CUIN compliance, without duplicating raw submitted PII a second time.';
