-- ============================================
-- CUIN v2 - Migration 007: Rollback for merges and Global Ref changes
-- ============================================
-- Before this migration, an officer merge (approve_pair/merge_entities)
-- or a Global Ref assign/edit/retire had no way to be undone -- only a
-- historical log (entity_lineage/audit_events) that PROVES what
-- happened, with no code path to REVERSE it. Worse, the exact member
-- list an absorbed entity had before a merge was never persisted
-- anywhere durable (only a transient Python variable inside
-- _merge_entities_tx, discarded after commit) -- so even a hand-rolled
-- reversal couldn't reconstruct it precisely.
--
-- This migration only widens the CHECK constraints that would
-- otherwise reject the new event/action labels services/
-- workbench_service.py's undo_merge/revert_global_ref/revoke_override
-- functions write. It does not change any existing row's data --
-- rollback of an OLDER merge (one made before this migration, whose
-- audit_events payload has no merge_snapshot) is intentionally
-- refused by undo_merge with a clear error, rather than guessed at.
--
-- audit_events.event_type has no CHECK constraint (plain VARCHAR(50)),
-- so "MERGE_REVERSED"/"GLOBAL_REF_REVERTED"/"OVERRIDE_REVOKED" audit
-- events need no schema change at all -- only entity_lineage.event and
-- review_decisions.action are constrained enums.
-- ============================================

ALTER TABLE entity_lineage DROP CONSTRAINT entity_lineage_event_check;
ALTER TABLE entity_lineage ADD CONSTRAINT entity_lineage_event_check
    CHECK (event IN ('CREATED', 'CARRIED', 'MERGED_IN', 'MERGED_AWAY', 'SPLIT_OUT',
                      'RETIRED', 'GLOBAL_REF_ASSIGNED', 'GLOBAL_REF_EDITED',
                      'GLOBAL_REF_RETIRED', 'GLOBAL_REF_CONFLICT',
                      'MERGE_REVERSED', 'UNMERGED_FROM', 'GLOBAL_REF_REVERTED'));

ALTER TABLE review_decisions DROP CONSTRAINT review_decisions_action_check;
ALTER TABLE review_decisions ADD CONSTRAINT review_decisions_action_check
    CHECK (action IN ('APPROVE', 'REJECT', 'MERGE', 'SPLIT', 'ASSIGN_GLOBAL_REF',
                       'EDIT_GLOBAL_REF', 'RETIRE_GLOBAL_REF',
                       'UNDO_MERGE', 'REVERT_GLOBAL_REF', 'REVOKE_OVERRIDE'));

COMMENT ON CONSTRAINT entity_lineage_event_check ON entity_lineage IS
    'Widened by migration 007 to add MERGE_REVERSED/UNMERGED_FROM/GLOBAL_REF_REVERTED -- see services.workbench_service.undo_merge/revert_global_ref.';
COMMENT ON CONSTRAINT review_decisions_action_check ON review_decisions IS
    'Widened by migration 007 to add UNDO_MERGE/REVERT_GLOBAL_REF/REVOKE_OVERRIDE.';
