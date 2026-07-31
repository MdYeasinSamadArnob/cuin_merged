-- ============================================
-- CUIN v2 - Migration 005: Entity registry (workbench Stage 2)
-- ============================================
-- The entity resolution workbench needs a Global Entity ID a bank
-- officer can assign, edit, and trust to survive a re-run -- but
-- `clusters.cluster_id` (engine.clustering.cluster_manager) is
-- sha256(RULESET_VERSION + sorted(members)): a pure content hash of
-- its own membership. Add or drop one member, or bump the ruleset
-- version, and the ID changes completely with zero lineage. Nothing
-- officer-assigned can be anchored to that.
--
-- This migration adds a SEPARATE identity layer, owned by the
-- platform, anchored to `customer_code` (the source system's stable
-- key -- NOT customers_norm.customer_key, which only exists for the
-- ~340K customers that happen to appear in an audited AUTO_LINK/
-- REVIEW pair; ~1.2M singletons need entity identity too, e.g. once
-- an officer manually assigns them a Global ID).
--
-- "Officer decision is law" (confirmed design decision): a merge or
-- split an officer confirms is written to resolution_overrides and is
-- re-applied by _stage_cluster on every future run, so it survives a
-- full re-run and re-clustering. See engine.clustering.build_clusters
-- (Stage 3 of the plan) for where these are read.
-- ============================================

CREATE TABLE IF NOT EXISTS entities (
    entity_id             UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

    -- The bank's own reference (e.g. a CIF number). NULL until an
    -- officer assigns one -- most entities never get one, and that's
    -- fine; a Global ID is opt-in, assigned when a bank actually wants
    -- to track that specific resolved identity downstream.
    global_ref            VARCHAR(64),
    global_ref_state      VARCHAR(16) NOT NULL DEFAULT 'UNASSIGNED'
                           CHECK (global_ref_state IN ('UNASSIGNED', 'DRAFT', 'CONFIRMED', 'CONFLICT', 'RETIRED')),

    status                VARCHAR(16) NOT NULL DEFAULT 'ACTIVE'
                           CHECK (status IN ('ACTIVE', 'MERGED', 'SPLIT', 'RETIRED')),
    -- Set when status='MERGED' -- this entity's Global ID (if any)
    -- moved to a survivor; never physically deleted, so a stale
    -- reference always resolves to where the identity actually went.
    merged_into_entity_id UUID REFERENCES entities(entity_id),

    display_name          VARCHAR(500),
    segment                VARCHAR(20),

    first_seen_run_id     UUID,
    last_seen_run_id      UUID,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by VARCHAR(255),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_by VARCHAR(255),

    notes TEXT
);

-- Case-insensitive uniqueness so "CIF-123" and "cif-123" can't both
-- exist -- a Global ID that syncs to core banking must be unambiguous.
CREATE UNIQUE INDEX IF NOT EXISTS ux_entities_global_ref
    ON entities (upper(global_ref)) WHERE global_ref IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_entities_status ON entities(status);


-- Membership anchored to customer_code, WITH history (valid_from/
-- valid_to) so carry-forward (Stage 2's resolve_entities) can answer
-- "who belonged to which entity as of the previous run", not just
-- "who belongs now". A record's CURRENT entity is the row where
-- valid_to IS NULL; the partial unique index below is the actual
-- database-enforced invariant behind "a Global ID is safe to sync
-- downstream" -- one code, one current entity, always.
CREATE TABLE IF NOT EXISTS entity_members (
    entity_id     UUID NOT NULL REFERENCES entities(entity_id),
    customer_code VARCHAR(64) NOT NULL,
    run_id        UUID,
    source        VARCHAR(16) NOT NULL CHECK (source IN ('PIPELINE', 'OFFICER')),
    valid_from    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    valid_to      TIMESTAMPTZ,
    PRIMARY KEY (entity_id, customer_code, valid_from)
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_entity_members_current
    ON entity_members(customer_code) WHERE valid_to IS NULL;
CREATE INDEX IF NOT EXISTS idx_entity_members_entity ON entity_members(entity_id) WHERE valid_to IS NULL;


-- Lineage log: every time an entity is created, carries forward from
-- a prior run, merges, splits, or has its Global ID touched. This is
-- what lets an officer see "why did this entity get a new ID" instead
-- of a silent identity change.
CREATE TABLE IF NOT EXISTS entity_lineage (
    id BIGSERIAL PRIMARY KEY,
    entity_id UUID NOT NULL REFERENCES entities(entity_id),
    run_id UUID,
    event VARCHAR(24) NOT NULL
          CHECK (event IN ('CREATED', 'CARRIED', 'MERGED_IN', 'MERGED_AWAY', 'SPLIT_OUT',
                            'RETIRED', 'GLOBAL_REF_ASSIGNED', 'GLOBAL_REF_EDITED',
                            'GLOBAL_REF_RETIRED', 'GLOBAL_REF_CONFLICT')),
    from_entity_ids UUID[],
    to_entity_ids   UUID[],
    jaccard NUMERIC(5,4),
    member_count INT,
    -- The CL_<sha256[:32]> content hash this entity's cluster carried
    -- at this run -- kept for cross-reference with the engine's own
    -- cluster_id, never used as the entity's real identity.
    cluster_id_at_run VARCHAR(64),
    actor VARCHAR(255),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_entity_lineage_entity ON entity_lineage(entity_id);
CREATE INDEX IF NOT EXISTS idx_entity_lineage_run ON entity_lineage(run_id);


-- "Officer decision is law": a confirmed pairwise verdict that
-- _stage_cluster (both orchestrators) re-applies on every future run,
-- BEFORE cohesion evaluation. Anchored to a pair of records, not to
-- an entity_id, because pairs compose correctly under union-find
-- across runs while an entity_id can be retired/merged out from
-- under a pin. See the plan's "Pipeline changes" section for exactly
-- how MUST_LINK and MUST_NOT_LINK are each applied -- MUST_NOT_LINK
-- in particular is NOT simply "don't union": a transitive chain can
-- still connect the pair through other auto-links, and that case is
-- surfaced to a reviewer rather than silently auto-cut.
CREATE TABLE IF NOT EXISTS resolution_overrides (
    override_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    verdict VARCHAR(16) NOT NULL CHECK (verdict IN ('MUST_LINK', 'MUST_NOT_LINK')),
    a_code VARCHAR(64) NOT NULL,
    b_code VARCHAR(64) NOT NULL,
    entity_id UUID REFERENCES entities(entity_id),

    reason_code VARCHAR(64) NOT NULL,
    reason TEXT NOT NULL,

    actor VARCHAR(255) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    decided_in_run_id UUID,
    ruleset_version VARCHAR(64),

    revoked_at TIMESTAMPTZ,
    revoked_by VARCHAR(255),
    revoke_reason TEXT,

    CONSTRAINT ordered_override CHECK (a_code < b_code)
);

-- Only one ACTIVE verdict per pair at a time -- a revoked override
-- doesn't block a new one on the same pair.
CREATE UNIQUE INDEX IF NOT EXISTS ux_override_active
    ON resolution_overrides(a_code, b_code) WHERE revoked_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_override_a ON resolution_overrides(a_code) WHERE revoked_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_override_b ON resolution_overrides(b_code) WHERE revoked_at IS NULL;


-- schema.sql defines a genesis audit_events row (audit_id
-- '00000000-...') but on this database it was never actually
-- inserted (audit_events has 0 rows in practice -- services.audit's
-- AuditChain has been an in-memory list, never Postgres-backed, until
-- Stage 3 of this plan). Insert it now if missing, matching
-- schema.sql's row byte-for-byte, so there's a real first link before
-- anything chains off it.
INSERT INTO audit_events (audit_id, event_type, payload_json, actor, prev_hash, this_hash, created_at)
SELECT
    '00000000-0000-0000-0000-000000000000',
    'SYSTEM_GENESIS',
    '{"message": "CUIN v2 audit chain initialized"}'::jsonb,
    'SYSTEM',
    '0000000000000000000000000000000000000000000000000000000000000000',
    encode(sha256('GENESIS'::bytea), 'hex'),
    NOW()
WHERE NOT EXISTS (SELECT 1 FROM audit_events WHERE audit_id = '00000000-0000-0000-0000-000000000000');

-- O(1) lock target for the audit hash chain (services.audit rewritten
-- in Stage 3 to be Postgres-backed instead of an in-memory list lost
-- on restart). Locking a multi-million-row audit_events table's tail
-- to serialize concurrent writers isn't viable; this single row is.
-- Seeded from the genesis event's this_hash (inserted immediately
-- above if it didn't already exist), so the chain head always points
-- at a real first link, never an arbitrary placeholder.
CREATE TABLE IF NOT EXISTS audit_chain_head (
    id SMALLINT PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    last_hash VARCHAR(64) NOT NULL,
    seq BIGINT NOT NULL DEFAULT 0
);
INSERT INTO audit_chain_head (id, last_hash, seq)
    SELECT 1, this_hash, 0 FROM audit_events WHERE audit_id = '00000000-0000-0000-0000-000000000000'
    ON CONFLICT (id) DO NOTHING;


COMMENT ON TABLE entities IS
    'Durable platform-owned entity identity + officer-assignable Global Ref, anchored to customer_code (not the content-derived engine cluster_id). See engine.clustering.cluster_manager for why cluster_id cannot serve this role.';
COMMENT ON TABLE entity_members IS
    'Which customer_codes belong to which entity, with history (valid_from/valid_to) for carry-forward lineage. ux_entity_members_current is the DB-enforced "one code, one current entity" invariant a synced Global ID depends on.';
COMMENT ON TABLE resolution_overrides IS
    'Officer-confirmed MUST_LINK/MUST_NOT_LINK verdicts, re-applied by every future pipeline run''s _stage_cluster -- the mechanism behind "officer decision is law".';
COMMENT ON TABLE entity_lineage IS
    'Append-only history of entity creation/carry-forward/merge/split/Global-Ref events, so an officer can see why an entity''s identity changed instead of it silently changing.';
