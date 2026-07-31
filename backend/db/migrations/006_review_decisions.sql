-- ============================================
-- CUIN v2 - Migration 006: Review decisions (workbench Stage 3)
-- ============================================
-- The legacy review_queue table (schema.sql) was designed but never
-- written to -- services/review_service.py stores decisions in flat
-- JSON files instead (data/runs/{run_id}_review_updates.jsonl), and
-- approve()/reject() only mutate an in-memory ClusterManager singleton
-- that's never saved -- so today NO officer decision survives a
-- restart, and audit_events (0 rows) can prove none of it happened.
--
-- review_decisions is the durable record of every officer action:
-- what was decided, why, by whom, and (via override_id/audit_id) the
-- exact resolution_overrides row and hash-chained audit_events row it
-- produced. One row per action, not per resulting entity mutation --
-- a merge changes many things (membership, possibly several implied
-- pair approvals) but is ONE decision.
-- ============================================

CREATE TABLE IF NOT EXISTS review_decisions (
    decision_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    run_id UUID,

    item_kind VARCHAR(24) NOT NULL CHECK (item_kind IN ('PAIR', 'ENTITY')),
    a_code VARCHAR(64),
    b_code VARCHAR(64),
    entity_id UUID REFERENCES entities(entity_id),

    action VARCHAR(24) NOT NULL
           CHECK (action IN ('APPROVE', 'REJECT', 'MERGE', 'SPLIT', 'ASSIGN_GLOBAL_REF', 'EDIT_GLOBAL_REF', 'RETIRE_GLOBAL_REF')),

    reason_code VARCHAR(64) NOT NULL,
    reason TEXT NOT NULL,

    decided_by VARCHAR(255) NOT NULL,
    decided_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- A merge implies approving every open REVIEW pair between the two
    -- merged entities -- those implied approvals get their own row
    -- with parent_decision_id pointing back to the merge, so an
    -- officer can see WHY a pair they never directly touched shows as
    -- approved.
    parent_decision_id UUID REFERENCES review_decisions(decision_id),

    override_id UUID REFERENCES resolution_overrides(override_id),
    audit_id UUID,

    CONSTRAINT ordered_pair_decision CHECK (a_code IS NULL OR b_code IS NULL OR a_code < b_code)
);

-- One CURRENT decision per pair -- re-deciding an already-decided pair
-- should be an explicit new action referencing history, not a second
-- silent row an officer has to notice.
CREATE INDEX IF NOT EXISTS idx_review_decisions_pair ON review_decisions(run_id, a_code, b_code) WHERE item_kind = 'PAIR';
CREATE INDEX IF NOT EXISTS idx_review_decisions_entity ON review_decisions(entity_id);
CREATE INDEX IF NOT EXISTS idx_review_decisions_run ON review_decisions(run_id);

COMMENT ON TABLE review_decisions IS
    'Durable record of every officer action (approve/reject/merge/split/Global-Ref) -- replaces the never-written legacy review_queue table and the in-memory-only ClusterManager.link() path that lost every decision on restart.';
