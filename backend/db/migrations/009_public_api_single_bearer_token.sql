-- ============================================
-- CUIN v2 - Migration 009: Public API auth simplified to a single bearer token
-- ============================================
-- Migration 008 introduced a per-caller, DB-issued API key model
-- (api_keys table + POST/GET/DELETE /admin/api-keys). Superseded the
-- same day by a simpler design: ONE global bearer token, read from
-- settings.PUBLIC_API_BEARER_TOKEN (env-configurable via
-- PUBLIC_API_BEARER_TOKEN in .env, auto-generated at process startup
-- otherwise) -- see api/routes_public_identity_api.py's
-- require_bearer_token and GET /admin/api-token.
--
-- api_keys is left in place (no DROP -- purely a documentation
-- comment update, no data/behavior change) since nothing in this
-- session's data is load-bearing and dropping a table is a
-- destructive operation this migration doesn't need to make. A future
-- session reintroducing per-partner keys can resume using this table
-- as-is; api_screening_log.api_key_id is simply NULL for every row
-- logged under the single-bearer-token model.
-- ============================================

COMMENT ON TABLE api_keys IS
    'Per-caller API key model from migration 008 -- NOT currently used. Auth for /api/v1 is now a single global bearer token (settings.PUBLIC_API_BEARER_TOKEN, see api/routes_public_identity_api.py require_bearer_token). Left in place, unused, for a future per-partner-key reintroduction.';
