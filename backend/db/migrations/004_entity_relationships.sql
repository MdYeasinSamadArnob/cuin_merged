-- ============================================
-- CUIN v2 - Migration 004: Entity relationships (segmentation)
-- ============================================
-- Stage 5 of the banker-rule-engine migration adds record
-- segmentation (Company vs Individual) so entity resolution never
-- merges a company and a person into the same identity. But
-- segmentation must not make cross-segment signal disappear: when a
-- Company-segment record and an Individual-segment record share
-- strong evidence (same phone, email, document, or address) via the
-- SAME blocking pass used for identity resolution, that connection is
-- real and often exactly what a bank needs to trace (e.g. a person's
-- personal mobile number also registered against a business account
-- they own or operate). This table is where that connection is
-- recorded -- distinct from match_decisions, which is reserved for
-- same-segment identity merges (AUTO_LINK/REVIEW/REJECT).
-- ============================================

CREATE TABLE IF NOT EXISTS entity_relationships (
    relationship_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    run_id UUID NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
    a_key UUID NOT NULL REFERENCES customers_norm(customer_key),
    b_key UUID NOT NULL REFERENCES customers_norm(customer_key),

    a_segment VARCHAR(20) NOT NULL,
    b_segment VARCHAR(20) NOT NULL,

    -- What evidence connected them -- e.g.
    -- [{"field": "mobile", "value": "01712345678"}] -- never a
    -- confidence score or AUTO_LINK/REVIEW/REJECT decision: this is a
    -- relationship between two DIFFERENT kinds of entity, not a claim
    -- that they are the same entity.
    shared_evidence JSONB NOT NULL,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT ordered_relationship_pair CHECK (a_key < b_key),
    CONSTRAINT unique_relationship_per_run UNIQUE (run_id, a_key, b_key)
);

CREATE INDEX IF NOT EXISTS idx_relationships_run ON entity_relationships(run_id);
CREATE INDEX IF NOT EXISTS idx_relationships_a_key ON entity_relationships(a_key);
CREATE INDEX IF NOT EXISTS idx_relationships_b_key ON entity_relationships(b_key);

COMMENT ON TABLE entity_relationships IS
    'Cross-segment connections (e.g. person <-> company) found by the same blocking pass as identity resolution, kept traceable but never merged into one entity. See engine.segments and pipeline orchestrators'' _stage_score_and_decide.';
