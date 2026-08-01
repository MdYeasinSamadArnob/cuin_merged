"""
Stage 3 of the entity resolution workbench plan: proves
services.workbench_service's officer actions are durable and audited
-- the specific gap this plan closes (services/review_service.py's
approve()/reject() mutate an in-memory singleton never saved, and
audit_events had 0 rows in production).

Runs against a REAL Postgres transaction. IMPORTANT: unlike most of
this test suite, these are NOT rollback-isolated -- every
workbench_service function commits internally (an officer action must
be atomic and durable by design, see that module's docstring), so
there is nothing left to roll back by the time control returns to the
test. Isolation instead comes from a per-test random code prefix
(never collides with real data or other test runs) plus an explicit
teardown that deletes exactly what that test created, including
resetting audit_chain_head so the hash chain stays internally
consistent after test audit_events are removed.
"""

import os
import random
import string
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

import pytest
import psycopg2

from services import workbench_service as wb


def _rand_prefix() -> str:
    return "TWB" + "".join(random.choices(string.ascii_uppercase + string.digits, k=8)) + "_"


@pytest.fixture
def pg_conn():
    from api.config import settings
    try:
        conn = psycopg2.connect(settings.DATABASE_URL)
    except Exception as e:
        pytest.skip(f"Postgres not reachable: {e}")
    conn.autocommit = False
    yield conn
    try:
        conn.rollback()  # in case the test errored before a commit
    except Exception:
        pass
    conn.close()


@pytest.fixture
def prefix(pg_conn):
    """A unique customer-code prefix + matching actor tag for this test, with full teardown cleanup."""
    p = _rand_prefix()
    yield p

    from api.config import settings
    conn = psycopg2.connect(settings.DATABASE_URL)
    try:
        cur = conn.cursor()
        like = p + "%"
        # entity_ids may include some that only exist via resolution_overrides.entity_id
        # (e.g. merge_entities' representative override) even if entity_members was already
        # closed out -- union both sources so nothing is missed.
        cur.execute("SELECT DISTINCT entity_id::text FROM entity_members WHERE customer_code LIKE %s", (like,))
        entity_ids = {r[0] for r in cur.fetchall()}
        cur.execute("SELECT DISTINCT entity_id::text FROM resolution_overrides WHERE (a_code LIKE %s OR b_code LIKE %s) AND entity_id IS NOT NULL", (like, like))
        entity_ids |= {r[0] for r in cur.fetchall()}
        entity_ids = list(entity_ids)

        # FK order: things that reference entity_id first, then entity_members
        # (which entities.entity_id itself doesn't depend on), then entities last.
        cur.execute("DELETE FROM review_decisions WHERE a_code LIKE %s OR b_code LIKE %s", (like, like))
        if entity_ids:
            cur.execute("DELETE FROM review_decisions WHERE entity_id::text = ANY(%s)", (entity_ids,))
        cur.execute("DELETE FROM resolution_overrides WHERE a_code LIKE %s OR b_code LIKE %s", (like, like))
        if entity_ids:
            cur.execute("DELETE FROM entity_lineage WHERE entity_id::text = ANY(%s)", (entity_ids,))
            cur.execute("DELETE FROM entity_members WHERE entity_id::text = ANY(%s)", (entity_ids,))
            cur.execute("DELETE FROM entities WHERE entity_id::text = ANY(%s)", (entity_ids,))

        # Test audit_events are tagged via actor=f"{prefix}officer" -- remove them and, if any
        # were removed, reset the chain head to whatever the true last remaining event now is
        # (genesis if none), so verify_audit_chain stays correct for subsequent tests.
        cur.execute("SELECT COUNT(*) FROM audit_events WHERE actor LIKE %s", (p + "%",))
        n_test_events = cur.fetchone()[0]
        if n_test_events:
            cur.execute("DELETE FROM audit_events WHERE actor LIKE %s", (p + "%",))
            cur.execute("SELECT this_hash FROM audit_events ORDER BY created_at DESC, audit_id DESC LIMIT 1")
            row = cur.fetchone()
            if row:
                cur.execute("UPDATE audit_chain_head SET last_hash = %s, seq = GREATEST(0, seq - %s) WHERE id = 1", (row[0], n_test_events))
        conn.commit()
    finally:
        conn.close()


def test_approve_pair_creates_durable_override_and_survives_a_reconnect(pg_conn, prefix):
    """The literal bug this plan fixes: an approve() must be readable by a DIFFERENT connection afterward,
    proving it's real committed state, not an in-memory object that vanishes."""
    a, b = f"{prefix}B", f"{prefix}A"
    result = wb.approve_pair(pg_conn, None, a, b, "CONFIRMED_MATCH", "Same person, verified by officer", f"{prefix}officer_1")
    assert result["override_id"]
    assert result["entity_id"]

    from api.config import settings
    conn2 = psycopg2.connect(settings.DATABASE_URL)
    try:
        cur = conn2.cursor()
        cur.execute("SELECT verdict, a_code, b_code, reason FROM resolution_overrides WHERE override_id = %s", (result["override_id"],))
        row = cur.fetchone()
        assert row == ("MUST_LINK", f"{prefix}A", f"{prefix}B", "Same person, verified by officer")

        cur.execute("SELECT customer_code FROM entity_members WHERE entity_id = %s AND valid_to IS NULL", (result["entity_id"],))
        members = {r[0] for r in cur.fetchall()}
        assert members == {f"{prefix}A", f"{prefix}B"}

        cur.execute("SELECT event_type, actor FROM audit_events WHERE audit_id = %s", (result["audit_id"],))
        assert cur.fetchone() == ("REVIEW_APPROVED", f"{prefix}officer_1")
    finally:
        conn2.close()


def test_approve_twice_is_rejected(pg_conn, prefix):
    a, b = f"{prefix}A", f"{prefix}B"
    wb.approve_pair(pg_conn, None, a, b, "CONFIRMED_MATCH", "first decision", f"{prefix}officer_1")
    with pytest.raises(wb.WorkbenchError):
        wb.approve_pair(pg_conn, None, a, b, "CONFIRMED_MATCH", "second decision", f"{prefix}officer_2")


def test_reject_pair_writes_must_not_link_and_does_not_merge(pg_conn, prefix):
    a, b = f"{prefix}B", f"{prefix}A"
    result = wb.reject_pair(pg_conn, None, a, b, "DIFFERENT_PEOPLE", "Different DOB, coincidental name match", f"{prefix}officer_1")
    cur = pg_conn.cursor()
    cur.execute("SELECT verdict, a_code, b_code FROM resolution_overrides WHERE override_id = %s", (result["override_id"],))
    assert cur.fetchone() == ("MUST_NOT_LINK", f"{prefix}A", f"{prefix}B")
    cur.execute("SELECT COUNT(*) FROM entity_members WHERE customer_code IN (%s, %s)", (f"{prefix}A", f"{prefix}B"))
    assert cur.fetchone()[0] == 0


def test_merge_entities_survivor_gets_all_members_and_audit(pg_conn, prefix):
    res_a = wb.approve_pair(pg_conn, None, f"{prefix}A", f"{prefix}B", "CONFIRMED_MATCH", "seed entity A", f"{prefix}officer_1")
    res_c = wb.approve_pair(pg_conn, None, f"{prefix}C", f"{prefix}D", "CONFIRMED_MATCH", "seed entity B", f"{prefix}officer_1")
    entity_a, entity_c = res_a["entity_id"], res_c["entity_id"]
    assert entity_a != entity_c

    merge_result = wb.merge_entities(pg_conn, None, entity_a, entity_c, "SAME_PERSON_VERIFIED", "Confirmed same customer via ID doc", f"{prefix}officer_2")
    survivor = merge_result["kept_entity_id"]
    absorbed = merge_result["absorbed_entity_id"]
    assert {survivor, absorbed} == {entity_a, entity_c}

    cur = pg_conn.cursor()
    cur.execute("SELECT customer_code FROM entity_members WHERE entity_id = %s AND valid_to IS NULL", (survivor,))
    assert {r[0] for r in cur.fetchall()} == {f"{prefix}A", f"{prefix}B", f"{prefix}C", f"{prefix}D"}

    cur.execute("SELECT status, merged_into_entity_id::text FROM entities WHERE entity_id = %s", (absorbed,))
    assert cur.fetchone() == ("MERGED", survivor)

    cur.execute("SELECT event_type FROM audit_events WHERE audit_id = %s", (merge_result["audit_id"],))
    assert cur.fetchone() == ("CLUSTER_MERGED",)


def test_merge_conflicting_confirmed_global_refs_is_rejected(pg_conn, prefix):
    res_a = wb.approve_pair(pg_conn, None, f"{prefix}A", f"{prefix}B", "CONFIRMED_MATCH", "seed", f"{prefix}officer_1")
    res_c = wb.approve_pair(pg_conn, None, f"{prefix}C", f"{prefix}D", "CONFIRMED_MATCH", "seed", f"{prefix}officer_1")
    wb.assign_global_ref(pg_conn, res_a["entity_id"], f"CIF-{prefix}0001", "CONFIRMED", "assign", f"{prefix}officer_1")
    wb.assign_global_ref(pg_conn, res_c["entity_id"], f"CIF-{prefix}0002", "CONFIRMED", "assign", f"{prefix}officer_1")

    with pytest.raises(wb.WorkbenchError):
        wb.merge_entities(pg_conn, None, res_a["entity_id"], res_c["entity_id"], "SAME_PERSON", "would collide", f"{prefix}officer_2")


def test_split_record_removes_member_and_blocks_relink(pg_conn, prefix):
    res_ab = wb.approve_pair(pg_conn, None, f"{prefix}A", f"{prefix}B", "CONFIRMED_MATCH", "seed", f"{prefix}officer_1")
    entity_id = res_ab["entity_id"]
    wb.approve_pair(pg_conn, None, f"{prefix}B", f"{prefix}C", "CONFIRMED_MATCH", "grow to 3 members", f"{prefix}officer_1")

    cur = pg_conn.cursor()
    cur.execute("SELECT customer_code FROM entity_members WHERE entity_id = %s AND valid_to IS NULL", (entity_id,))
    before = {r[0] for r in cur.fetchall()}
    assert before == {f"{prefix}A", f"{prefix}B", f"{prefix}C"}

    split_result = wb.split_record(pg_conn, None, entity_id, f"{prefix}A", "INCORRECT_LINK", "Different person, name coincidence only", f"{prefix}officer_2")
    new_entity = split_result["new_entity_id"]

    cur.execute("SELECT customer_code FROM entity_members WHERE entity_id = %s AND valid_to IS NULL", (entity_id,))
    assert {r[0] for r in cur.fetchall()} == {f"{prefix}B", f"{prefix}C"}
    cur.execute("SELECT customer_code FROM entity_members WHERE entity_id = %s AND valid_to IS NULL", (new_entity,))
    assert {r[0] for r in cur.fetchall()} == {f"{prefix}A"}

    cur.execute(
        "SELECT a_code, b_code FROM resolution_overrides WHERE verdict='MUST_NOT_LINK' AND revoked_at IS NULL "
        "AND (a_code = %s OR b_code = %s)",
        (f"{prefix}A", f"{prefix}A"),
    )
    pairs = {tuple(sorted(r)) for r in cur.fetchall()}
    assert pairs == {(f"{prefix}A", f"{prefix}B"), (f"{prefix}A", f"{prefix}C")}


def test_assign_global_ref_uniqueness_enforced(pg_conn, prefix):
    res_a = wb.approve_pair(pg_conn, None, f"{prefix}A", f"{prefix}B", "CONFIRMED_MATCH", "seed", f"{prefix}officer_1")
    res_c = wb.approve_pair(pg_conn, None, f"{prefix}C", f"{prefix}D", "CONFIRMED_MATCH", "seed", f"{prefix}officer_1")

    ref = f"CIF-{prefix}UNIQUE"
    wb.assign_global_ref(pg_conn, res_a["entity_id"], ref, "CONFIRMED", "assign", f"{prefix}officer_1")
    with pytest.raises(wb.WorkbenchError):
        wb.assign_global_ref(pg_conn, res_c["entity_id"], ref.lower(), "CONFIRMED", "should collide case-insensitively", f"{prefix}officer_1")


def test_retire_global_ref(pg_conn, prefix):
    res_a = wb.approve_pair(pg_conn, None, f"{prefix}A", f"{prefix}B", "CONFIRMED_MATCH", "seed", f"{prefix}officer_1")
    wb.assign_global_ref(pg_conn, res_a["entity_id"], f"CIF-{prefix}RETIRE", "CONFIRMED", "assign", f"{prefix}officer_1")
    wb.retire_global_ref(pg_conn, res_a["entity_id"], "duplicate entity, superseded", f"{prefix}officer_1")

    cur = pg_conn.cursor()
    cur.execute("SELECT global_ref_state FROM entities WHERE entity_id = %s", (res_a["entity_id"],))
    assert cur.fetchone() == ("RETIRED",)


def test_audit_chain_verifies_after_a_sequence_of_actions(pg_conn, prefix):
    wb.approve_pair(pg_conn, None, f"{prefix}A", f"{prefix}B", "CONFIRMED_MATCH", "one", f"{prefix}officer_1")
    wb.reject_pair(pg_conn, None, f"{prefix}C", f"{prefix}D", "DIFFERENT_PEOPLE", "two", f"{prefix}officer_1")

    is_valid, error, checked = wb.verify_audit_chain(pg_conn)
    assert is_valid, error
    assert checked >= 3  # genesis + the two actions above


# ----------------------------------------------------------------------
# Rollback: undo_merge / revert_global_ref / revoke_override
# ----------------------------------------------------------------------

def test_undo_merge_restores_exact_prior_state(pg_conn, prefix):
    """The core promise: after undo_merge, membership/status/override state is
    byte-for-byte what it was immediately before the merge -- not an approximation."""
    result = wb.approve_pair(pg_conn, None, f"{prefix}A", f"{prefix}B", "CONFIRMED_MATCH", "seed", f"{prefix}officer_1")
    keep_id = result["entity_id"]
    cur = pg_conn.cursor()
    # entity_a (A's entity) is always keep_id in approve_pair, entity_b (B's fresh entity) is absorbed.
    cur.execute(
        "SELECT payload_json->'merge_snapshot'->>'absorb_id' FROM audit_events WHERE audit_id = %s", (result["audit_id"],),
    )
    absorb_id = cur.fetchone()[0]
    assert absorb_id != keep_id

    undo_result = wb.undo_merge(pg_conn, absorb_id, "was wrong, different people", f"{prefix}officer_2")
    assert undo_result["keep_id"] == keep_id
    assert undo_result["restored_member_count"] == 1

    cur.execute("SELECT status, merged_into_entity_id FROM entities WHERE entity_id = %s", (absorb_id,))
    status, merged_into = cur.fetchone()
    assert status == "ACTIVE"
    assert merged_into is None

    cur.execute("SELECT customer_code FROM entity_members WHERE entity_id = %s AND valid_to IS NULL", (absorb_id,))
    assert {r[0] for r in cur.fetchall()} == {f"{prefix}B"}
    cur.execute("SELECT customer_code FROM entity_members WHERE entity_id = %s AND valid_to IS NULL", (keep_id,))
    assert {r[0] for r in cur.fetchall()} == {f"{prefix}A"}

    cur.execute("SELECT revoked_at FROM resolution_overrides WHERE override_id = %s", (result["override_id"],))
    assert cur.fetchone()[0] is not None  # revoked, so a future pipeline run won't re-apply the merge


def test_undo_merge_refuses_when_not_currently_merged(pg_conn, prefix):
    result = wb.approve_pair(pg_conn, None, f"{prefix}A", f"{prefix}B", "CONFIRMED_MATCH", "seed", f"{prefix}officer_1")
    with pytest.raises(wb.WorkbenchError):
        wb.undo_merge(pg_conn, result["entity_id"], "not actually merged", f"{prefix}officer_2")


def test_undo_merge_refuses_when_member_moved_since(pg_conn, prefix):
    """Safety net: if a later action (another merge, a split) touched the moved
    member, undo must refuse rather than silently produce an inconsistent state."""
    result = wb.approve_pair(pg_conn, None, f"{prefix}A", f"{prefix}B", "CONFIRMED_MATCH", "seed", f"{prefix}officer_1")
    keep_id = result["entity_id"]
    cur = pg_conn.cursor()
    cur.execute("SELECT payload_json->'merge_snapshot'->>'absorb_id' FROM audit_events WHERE audit_id = %s", (result["audit_id"],))
    absorb_id = cur.fetchone()[0]

    wb.split_record(pg_conn, None, keep_id, f"{prefix}B", "INCORRECT_LINK", "moved away before undo attempted", f"{prefix}officer_2")

    with pytest.raises(wb.WorkbenchError):
        wb.undo_merge(pg_conn, absorb_id, "should be refused", f"{prefix}officer_3")


def test_undo_merge_restores_carried_global_ref(pg_conn, prefix):
    """When a merge carries the absorbed entity's CONFIRMED ref onto the
    survivor (survivor had none), undo must put the survivor back to
    unassigned -- not leave it holding a ref that came from an entity
    that no longer absorbs it."""
    res_a = wb.approve_pair(pg_conn, None, f"{prefix}A", f"{prefix}B", "CONFIRMED_MATCH", "seed keep", f"{prefix}officer_1")
    keep_id = res_a["entity_id"]
    res_c = wb.approve_pair(pg_conn, None, f"{prefix}C", f"{prefix}D", "CONFIRMED_MATCH", "seed absorb", f"{prefix}officer_1")
    absorb_id_before_merge = res_c["entity_id"]
    ref = f"CIF-{prefix}CARRIED"
    wb.assign_global_ref(pg_conn, absorb_id_before_merge, ref, "CONFIRMED", "give absorb entity a ref", f"{prefix}officer_1")

    merge_result = wb.merge_entities(pg_conn, None, keep_id, absorb_id_before_merge, "SAME_PERSON_VERIFIED", "merge", f"{prefix}officer_2")
    survivor = merge_result["kept_entity_id"]
    absorbed = merge_result["absorbed_entity_id"]
    # Both components have 2 members, so merge_entities' tie-break falls to
    # entity_id string comparison -- which of the two randomly-generated
    # UUIDs wins isn't predictable, so this test handles either outcome
    # rather than asserting one.

    cur = pg_conn.cursor()
    if absorbed == absorb_id_before_merge:
        # The ref-holder was the one absorbed -- the merge must have carried
        # its ref onto the survivor, so there's something for undo to revert.
        cur.execute("SELECT global_ref, global_ref_state FROM entities WHERE entity_id = %s", (survivor,))
        assert cur.fetchone() == (ref, "CONFIRMED")

        wb.undo_merge(pg_conn, absorbed, "undo the ref-carrying merge", f"{prefix}officer_3")
        cur.execute("SELECT global_ref, global_ref_state FROM entities WHERE entity_id = %s", (survivor,))
        assert cur.fetchone() == (None, "UNASSIGNED")
        cur.execute("SELECT global_ref, global_ref_state FROM entities WHERE entity_id = %s", (absorbed,))
        assert cur.fetchone() == (ref, "CONFIRMED")  # the absorbed entity's own row was never touched, still has it
    else:
        # The ref-holder was the survivor -- nothing was carried, so
        # undoing this merge should NOT touch its ref at all.
        wb.undo_merge(pg_conn, absorbed, "undo a merge that never touched the ref", f"{prefix}officer_3")
        cur.execute("SELECT global_ref, global_ref_state FROM entities WHERE entity_id = %s", (survivor,))
        assert cur.fetchone() == (ref, "CONFIRMED")


def test_revert_global_ref_restores_previous_value(pg_conn, prefix):
    result = wb.approve_pair(pg_conn, None, f"{prefix}A", f"{prefix}B", "CONFIRMED_MATCH", "seed", f"{prefix}officer_1")
    entity_id = result["entity_id"]
    ref1 = f"CIF-{prefix}V1"
    ref2 = f"CIF-{prefix}V2"
    wb.assign_global_ref(pg_conn, entity_id, ref1, "CONFIRMED", "first assign", f"{prefix}officer_1")
    wb.assign_global_ref(pg_conn, entity_id, ref2, "CONFIRMED", "edit to v2", f"{prefix}officer_1")

    revert_result = wb.revert_global_ref(pg_conn, entity_id, "v2 was a mistake", f"{prefix}officer_2")
    assert revert_result["restored_ref"] == ref1

    cur = pg_conn.cursor()
    cur.execute("SELECT global_ref, global_ref_state FROM entities WHERE entity_id = %s", (entity_id,))
    assert cur.fetchone() == (ref1, "CONFIRMED")


def test_revert_global_ref_with_no_history_raises(pg_conn, prefix):
    result = wb.approve_pair(pg_conn, None, f"{prefix}A", f"{prefix}B", "CONFIRMED_MATCH", "seed", f"{prefix}officer_1")
    with pytest.raises(wb.WorkbenchError):
        wb.revert_global_ref(pg_conn, result["entity_id"], "nothing to revert", f"{prefix}officer_2")


def test_revoke_override_marks_revoked_and_refuses_twice(pg_conn, prefix):
    result = wb.approve_pair(pg_conn, None, f"{prefix}A", f"{prefix}B", "CONFIRMED_MATCH", "seed", f"{prefix}officer_1")
    override_id = result["override_id"]

    revoke_result = wb.revoke_override(pg_conn, override_id, "was incorrect", f"{prefix}officer_2")
    assert revoke_result["override_id"] == override_id

    cur = pg_conn.cursor()
    cur.execute("SELECT revoked_at, revoked_by FROM resolution_overrides WHERE override_id = %s", (override_id,))
    revoked_at, revoked_by = cur.fetchone()
    assert revoked_at is not None
    assert revoked_by == f"{prefix}officer_2"

    with pytest.raises(wb.WorkbenchError):
        wb.revoke_override(pg_conn, override_id, "double revoke", f"{prefix}officer_3")


def test_audit_chain_verifies_after_rollback_actions(pg_conn, prefix):
    result = wb.approve_pair(pg_conn, None, f"{prefix}A", f"{prefix}B", "CONFIRMED_MATCH", "seed", f"{prefix}officer_1")
    cur = pg_conn.cursor()
    cur.execute("SELECT payload_json->'merge_snapshot'->>'absorb_id' FROM audit_events WHERE audit_id = %s", (result["audit_id"],))
    absorb_id = cur.fetchone()[0]
    wb.undo_merge(pg_conn, absorb_id, "undo for chain test", f"{prefix}officer_2")

    result2 = wb.approve_pair(pg_conn, None, f"{prefix}C", f"{prefix}D", "CONFIRMED_MATCH", "seed 2", f"{prefix}officer_1")
    wb.assign_global_ref(pg_conn, result2["entity_id"], f"CIF-{prefix}CHAIN", "CONFIRMED", "assign", f"{prefix}officer_1")
    wb.revert_global_ref(pg_conn, result2["entity_id"], "undo assign for chain test", f"{prefix}officer_2")
    wb.revoke_override(pg_conn, result2["override_id"], "revoke for chain test", f"{prefix}officer_2")

    is_valid, error, checked = wb.verify_audit_chain(pg_conn)
    assert is_valid, error
    assert checked >= 6


if __name__ == "__main__":
    print("Run via pytest (requires a live Postgres connection).")
