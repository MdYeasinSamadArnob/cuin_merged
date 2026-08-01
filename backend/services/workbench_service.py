"""
CUIN v2 - Workbench Service (Stage 3 of the entity resolution
workbench plan)

Officer actions -- approve, reject, merge, split, assign/edit/retire a
Global Ref -- each as ONE Postgres transaction, so an action is either
fully durable or doesn't happen at all. Fixes the specific gap this
plan was written to close: approve()/reject() in services/
review_service.py mutate an in-memory ClusterManager singleton that is
never saved, and audit_events has 0 rows despite the app running for
weeks -- meaning literally no officer decision made through the old
/review page could survive a restart, and none could be proven to a
bank auditor.

Every action:
  1. Takes `SELECT ... FOR UPDATE` on audit_chain_head FIRST, so two
     officers acting concurrently are serialized rather than both
     computing a hash off the same prev_hash (a silently broken chain
     an auditor would find). See _lock_audit_chain / _append_audit_event.
  2. Writes to resolution_overrides and/or entities/entity_members --
     the durable state engine.clustering.build_clusters re-applies on
     EVERY future pipeline run (see db/migrations/005_entity_registry.sql),
     which is the actual mechanism behind "officer decision is law".
  3. Writes one review_decisions row (what was decided, why, by whom).
  4. Writes one hash-chained audit_events row.
  5. Commits all four together, or rolls back all four together.
"""

import hashlib
import json
from datetime import datetime, timezone
from typing import List, Optional, Tuple
from uuid import uuid4

from psycopg2.extras import Json


class WorkbenchError(Exception):
    """Raised for invalid officer actions -- the API layer maps this to HTTP 400/404/409."""


def _order_pair(a_code: str, b_code: str) -> Tuple[str, str]:
    return (a_code, b_code) if a_code < b_code else (b_code, a_code)


def _lock_audit_chain(cur) -> None:
    """
    Must be the FIRST statement of any transaction that will call
    _append_audit_event -- takes the row lock that serializes
    concurrent officers so no two actions can compute a hash off the
    same prev_hash. Cheap: it's a single-row lock on a table with
    exactly one row (audit_chain_head), never a table-level lock.
    """
    cur.execute("SELECT last_hash FROM audit_chain_head WHERE id = 1 FOR UPDATE")


def _append_audit_event(cur, event_type: str, payload: dict, actor: str, run_id: Optional[str] = None) -> Tuple[str, str]:
    """Appends one row to audit_events chained off the current head, advances the head. Returns (audit_id, this_hash)."""
    cur.execute("SELECT last_hash FROM audit_chain_head WHERE id = 1")
    prev_hash = cur.fetchone()[0]
    created_at = datetime.now(timezone.utc)
    payload_json = json.dumps(payload, sort_keys=True, default=str)
    this_hash = hashlib.sha256(
        f"{prev_hash}{event_type}{payload_json}{created_at.isoformat()}".encode("utf-8")
    ).hexdigest()
    audit_id = str(uuid4())
    cur.execute(
        "INSERT INTO audit_events (audit_id, run_id, event_type, payload_json, actor, prev_hash, this_hash, created_at) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
        (audit_id, run_id, event_type, Json(payload), actor, prev_hash, this_hash, created_at),
    )
    cur.execute("UPDATE audit_chain_head SET last_hash = %s, seq = seq + 1 WHERE id = 1", (this_hash,))
    return audit_id, this_hash


def verify_audit_chain(pg_conn, limit: int = 100000) -> Tuple[bool, Optional[str], int]:
    """
    Recomputes every link from the genesis event forward and compares
    against the stored this_hash -- a windowed scan (bounded by
    `limit`), not the whole table forever, since a bank's chain only
    grows. Returns (is_valid, error_message_or_None, events_checked).

    The genesis row (audit_id all-zeros, event_type SYSTEM_GENESIS) is
    a special-cased bootstrap value -- schema.sql seeds its this_hash
    as `sha256('GENESIS')`, not this function's chain formula, since
    it has no real prev_hash to chain from. It's trusted as the root
    and skipped here, exactly like every other link's prev_hash is
    trusted to be the previous link's already-verified this_hash --
    verification starts for real at the first NON-genesis event.
    """
    cur = pg_conn.cursor()
    cur.execute(
        "SELECT audit_id, event_type, payload_json, prev_hash, this_hash, created_at "
        "FROM audit_events ORDER BY created_at, audit_id LIMIT %s",
        (limit,),
    )
    rows = cur.fetchall()
    checked = 0
    expected_prev_hash = None
    for audit_id, event_type, payload_json, prev_hash, this_hash, created_at in rows:
        if event_type == "SYSTEM_GENESIS":
            expected_prev_hash = this_hash
            checked += 1
            continue

        if expected_prev_hash is not None and prev_hash != expected_prev_hash:
            return False, f"event {audit_id} ({event_type}): prev_hash does not match the preceding event's this_hash -- chain broken or event missing", checked

        payload_str = json.dumps(payload_json, sort_keys=True, default=str)
        expected = hashlib.sha256(f"{prev_hash}{event_type}{payload_str}{created_at.isoformat()}".encode("utf-8")).hexdigest()
        if expected != this_hash:
            return False, f"event {audit_id} ({event_type}): hash mismatch -- tampering or corruption detected", checked
        checked += 1
        expected_prev_hash = this_hash
    return True, None, checked


def _get_current_entity(cur, customer_code: str) -> Optional[str]:
    cur.execute("SELECT entity_id::text FROM entity_members WHERE customer_code = %s AND valid_to IS NULL", (customer_code,))
    row = cur.fetchone()
    return row[0] if row else None


def _entity_members(cur, entity_id: str) -> List[str]:
    cur.execute("SELECT customer_code FROM entity_members WHERE entity_id = %s AND valid_to IS NULL", (entity_id,))
    return [r[0] for r in cur.fetchall()]


def _ensure_entity_for_code(cur, customer_code: str, run_id: Optional[str], actor: str) -> str:
    """A code with no current entity (e.g. it was a singleton, never clustered) gets a fresh one-member entity minted on demand."""
    existing = _get_current_entity(cur, customer_code)
    if existing:
        return existing
    entity_id = str(uuid4())
    cur.execute(
        "INSERT INTO entities (entity_id, created_by, first_seen_run_id, last_seen_run_id) VALUES (%s,%s,%s,%s)",
        (entity_id, actor, run_id, run_id),
    )
    cur.execute(
        "INSERT INTO entity_members (entity_id, customer_code, run_id, source) VALUES (%s,%s,%s,'OFFICER')",
        (entity_id, customer_code, run_id),
    )
    cur.execute(
        "INSERT INTO entity_lineage (entity_id, run_id, event, member_count, actor) VALUES (%s,%s,'CREATED',1,%s)",
        (entity_id, run_id, actor),
    )
    return entity_id


def _merge_entities_tx(cur, run_id: Optional[str], keep_id: str, absorb_id: str, reason_code: str, reason: str, actor: str) -> dict:
    """
    Moves every member of absorb_id into keep_id, marks absorb_id
    MERGED. Global Ref conflict handling: if BOTH entities carry a
    CONFIRMED Global Ref, this is a business decision no software
    should make silently -- raises WorkbenchError rather than
    guessing which ID survives (see the plan's risk register).

    Returns a reversal snapshot dict -- the caller embeds this (plus
    the resolution_overrides override_id it separately creates) into
    its audit_events payload under "merge_snapshot", which is the ONLY
    place this data survives after commit (previously just a local
    Python variable, discarded -- meaning a merge could never be
    precisely undone at all; see undo_merge's docstring).
    """
    cur.execute("SELECT global_ref, global_ref_state FROM entities WHERE entity_id = %s", (keep_id,))
    keep_ref, keep_state = cur.fetchone()
    cur.execute("SELECT global_ref, global_ref_state FROM entities WHERE entity_id = %s", (absorb_id,))
    absorb_ref, absorb_state = cur.fetchone()

    if keep_state == "CONFIRMED" and absorb_state == "CONFIRMED" and keep_ref != absorb_ref:
        raise WorkbenchError(
            f"Cannot merge: both entities have a CONFIRMED Global Ref ({keep_ref} and {absorb_ref}). "
            f"Retire one explicitly first, then merge."
        )

    members = _entity_members(cur, absorb_id)
    for code in members:
        cur.execute(
            "UPDATE entity_members SET valid_to = NOW() WHERE entity_id = %s AND customer_code = %s AND valid_to IS NULL",
            (absorb_id, code),
        )
        cur.execute(
            "INSERT INTO entity_members (entity_id, customer_code, run_id, source) VALUES (%s,%s,%s,'OFFICER')",
            (keep_id, code, run_id),
        )

    # If the absorbed entity had a Global Ref and the survivor doesn't, carry it forward.
    # Must ALSO clear it off the absorbed entity's own row -- and in THIS
    # order, clear-then-set, not set-then-clear. ux_entities_global_ref is
    # a non-deferred unique index (checked at the end of each statement,
    # not at commit), so setting keep_id's ref FIRST while absorb_id still
    # holds the identical string is a real (if transient) duplicate and
    # raises UniqueViolation immediately -- confirmed live, this failed
    # ~50% of the time (whichever side merge_entities' size/id tie-break
    # happened to pick as the ref-carrying case) until the order was
    # swapped. Found via test_undo_merge_restores_carried_global_ref -- a
    # genuine pre-existing bug (this UPDATE predates this session's
    # changes), not something the rollback work introduced: any merge
    # where the absorbed side had a CONFIRMED ref and the survivor didn't
    # would crash. undo_merge restores this row's ref/state from the
    # snapshot's absorb_global_ref_before/absorb_global_ref_state_before,
    # so clearing it here doesn't lose the data needed to reverse it.
    keep_ref_changed = absorb_state == "CONFIRMED" and keep_state != "CONFIRMED"
    if keep_ref_changed:
        cur.execute(
            "UPDATE entities SET global_ref = NULL, global_ref_state = 'UNASSIGNED', updated_at = NOW() WHERE entity_id = %s",
            (absorb_id,),
        )
        cur.execute(
            "UPDATE entities SET global_ref = %s, global_ref_state = 'CONFIRMED', updated_at = NOW() WHERE entity_id = %s",
            (absorb_ref, keep_id),
        )

    cur.execute(
        "UPDATE entities SET status = 'MERGED', merged_into_entity_id = %s, updated_at = NOW() WHERE entity_id = %s",
        (keep_id, absorb_id),
    )
    cur.execute(
        "INSERT INTO entity_lineage (entity_id, run_id, event, to_entity_ids, member_count, actor) VALUES (%s,%s,'MERGED_AWAY',%s::uuid[],%s,%s)",
        (absorb_id, run_id, [keep_id], len(members), actor),
    )
    cur.execute(
        "INSERT INTO entity_lineage (entity_id, run_id, event, from_entity_ids, member_count, actor) VALUES (%s,%s,'MERGED_IN',%s::uuid[],%s,%s)",
        (keep_id, run_id, [absorb_id], len(members), actor),
    )

    return {
        "keep_id": keep_id, "absorb_id": absorb_id, "moved_member_codes": members,
        "keep_global_ref_changed": keep_ref_changed,
        "keep_global_ref_before": keep_ref, "keep_global_ref_state_before": keep_state,
        "absorb_global_ref_before": absorb_ref, "absorb_global_ref_state_before": absorb_state,
    }


def approve_pair(pg_conn, run_id: Optional[str], a_code: str, b_code: str, reason_code: str, reason: str, actor: str) -> dict:
    """Officer confirms a pair IS a match -- a durable MUST_LINK override, re-applied on every future run."""
    a_code, b_code = _order_pair(a_code, b_code)
    cur = pg_conn.cursor()
    try:
        _lock_audit_chain(cur)

        cur.execute(
            "SELECT override_id FROM resolution_overrides WHERE a_code=%s AND b_code=%s AND revoked_at IS NULL",
            (a_code, b_code),
        )
        if cur.fetchone():
            raise WorkbenchError(f"{a_code}:{b_code} already has an active decision -- revoke it first to change the verdict")

        override_id = str(uuid4())
        cur.execute(
            "INSERT INTO resolution_overrides (override_id, verdict, a_code, b_code, reason_code, reason, actor, decided_in_run_id) "
            "VALUES (%s,'MUST_LINK',%s,%s,%s,%s,%s,%s)",
            (override_id, a_code, b_code, reason_code, reason, actor, run_id),
        )

        entity_a = _ensure_entity_for_code(cur, a_code, run_id, actor)
        entity_b = _ensure_entity_for_code(cur, b_code, run_id, actor)
        merge_snapshot = None
        if entity_a != entity_b:
            merge_snapshot = _merge_entities_tx(cur, run_id, entity_a, entity_b, reason_code, reason, actor)
            merge_snapshot["override_id"] = override_id
        merged_entity_id = entity_a

        audit_payload = {"a_code": a_code, "b_code": b_code, "reason_code": reason_code, "reason": reason, "entity_id": merged_entity_id}
        if merge_snapshot:
            audit_payload["merge_snapshot"] = merge_snapshot
        audit_id, this_hash = _append_audit_event(cur, "REVIEW_APPROVED", audit_payload, actor, run_id)

        decision_id = str(uuid4())
        cur.execute(
            "INSERT INTO review_decisions (decision_id, run_id, item_kind, a_code, b_code, entity_id, action, reason_code, reason, decided_by, override_id, audit_id) "
            "VALUES (%s,%s,'PAIR',%s,%s,%s,'APPROVE',%s,%s,%s,%s,%s)",
            (decision_id, run_id, a_code, b_code, merged_entity_id, reason_code, reason, actor, override_id, audit_id),
        )

        pg_conn.commit()
        return {"decision_id": decision_id, "override_id": override_id, "entity_id": merged_entity_id, "audit_id": audit_id, "this_hash": this_hash}
    except Exception:
        pg_conn.rollback()
        raise


def reject_pair(pg_conn, run_id: Optional[str], a_code: str, b_code: str, reason_code: str, reason: str, actor: str) -> dict:
    """Officer confirms a pair is NOT a match -- a durable MUST_NOT_LINK override."""
    a_code, b_code = _order_pair(a_code, b_code)
    cur = pg_conn.cursor()
    try:
        _lock_audit_chain(cur)

        cur.execute(
            "SELECT override_id FROM resolution_overrides WHERE a_code=%s AND b_code=%s AND revoked_at IS NULL",
            (a_code, b_code),
        )
        if cur.fetchone():
            raise WorkbenchError(f"{a_code}:{b_code} already has an active decision -- revoke it first to change the verdict")

        override_id = str(uuid4())
        cur.execute(
            "INSERT INTO resolution_overrides (override_id, verdict, a_code, b_code, reason_code, reason, actor, decided_in_run_id) "
            "VALUES (%s,'MUST_NOT_LINK',%s,%s,%s,%s,%s,%s)",
            (override_id, a_code, b_code, reason_code, reason, actor, run_id),
        )

        audit_id, this_hash = _append_audit_event(
            cur, "REVIEW_REJECTED", {"a_code": a_code, "b_code": b_code, "reason_code": reason_code, "reason": reason}, actor, run_id,
        )

        decision_id = str(uuid4())
        cur.execute(
            "INSERT INTO review_decisions (decision_id, run_id, item_kind, a_code, b_code, action, reason_code, reason, decided_by, override_id, audit_id) "
            "VALUES (%s,%s,'PAIR',%s,%s,'REJECT',%s,%s,%s,%s,%s)",
            (decision_id, run_id, a_code, b_code, reason_code, reason, actor, override_id, audit_id),
        )

        pg_conn.commit()
        return {"decision_id": decision_id, "override_id": override_id, "audit_id": audit_id, "this_hash": this_hash}
    except Exception:
        pg_conn.rollback()
        raise


def merge_entities(pg_conn, run_id: Optional[str], entity_id_a: str, entity_id_b: str, reason_code: str, reason: str, actor: str) -> dict:
    """Officer-initiated merge of two entities directly (not via a specific pair) -- e.g. from search results."""
    if entity_id_a == entity_id_b:
        raise WorkbenchError("Cannot merge an entity with itself")
    cur = pg_conn.cursor()
    try:
        _lock_audit_chain(cur)

        members_a = _entity_members(cur, entity_id_a)
        members_b = _entity_members(cur, entity_id_b)
        if not members_a:
            raise WorkbenchError(f"Entity {entity_id_a} not found or has no current members")
        if not members_b:
            raise WorkbenchError(f"Entity {entity_id_b} not found or has no current members")

        # Keep the larger entity as the survivor (deterministic, and minimizes membership churn);
        # tie-break on entity_id for determinism.
        keep_id, absorb_id = (entity_id_a, entity_id_b) if (len(members_a), entity_id_a) >= (len(members_b), entity_id_b) else (entity_id_b, entity_id_a)

        merge_snapshot = _merge_entities_tx(cur, run_id, keep_id, absorb_id, reason_code, reason, actor)

        # Representative MUST_LINK override so build_clusters() re-applies this merge on every future run.
        a_code, b_code = _order_pair(members_a[0], members_b[0])
        override_id = str(uuid4())
        cur.execute(
            "INSERT INTO resolution_overrides (override_id, verdict, a_code, b_code, entity_id, reason_code, reason, actor, decided_in_run_id) "
            "VALUES (%s,'MUST_LINK',%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
            (override_id, a_code, b_code, keep_id, reason_code, reason, actor, run_id),
        )
        merge_snapshot["override_id"] = override_id

        audit_id, this_hash = _append_audit_event(
            cur, "CLUSTER_MERGED",
            {"kept_entity_id": keep_id, "absorbed_entity_id": absorb_id, "reason_code": reason_code, "reason": reason, "merge_snapshot": merge_snapshot},
            actor, run_id,
        )

        decision_id = str(uuid4())
        cur.execute(
            "INSERT INTO review_decisions (decision_id, run_id, item_kind, entity_id, action, reason_code, reason, decided_by, override_id, audit_id) "
            "VALUES (%s,%s,'ENTITY',%s,'MERGE',%s,%s,%s,%s,%s)",
            (decision_id, run_id, keep_id, reason_code, reason, actor, override_id, audit_id),
        )

        pg_conn.commit()
        return {"decision_id": decision_id, "kept_entity_id": keep_id, "absorbed_entity_id": absorb_id, "audit_id": audit_id, "this_hash": this_hash}
    except Exception:
        pg_conn.rollback()
        raise


def split_record(pg_conn, run_id: Optional[str], entity_id: str, customer_code: str, reason_code: str, reason: str, actor: str) -> dict:
    """
    Officer determines `customer_code` was incorrectly linked into
    `entity_id` -- moves it into a fresh one-member entity, and writes
    a MUST_NOT_LINK override against every OTHER current member so
    build_clusters() never re-links it on a future run without a new,
    explicit officer decision.
    """
    cur = pg_conn.cursor()
    try:
        _lock_audit_chain(cur)

        current = _get_current_entity(cur, customer_code)
        if current != entity_id:
            raise WorkbenchError(f"{customer_code} is not a current member of entity {entity_id}")

        remaining = [c for c in _entity_members(cur, entity_id) if c != customer_code]
        if not remaining:
            raise WorkbenchError(f"Cannot split the only member out of entity {entity_id} -- use retire instead")

        new_entity_id = str(uuid4())
        cur.execute(
            "INSERT INTO entities (entity_id, created_by, first_seen_run_id, last_seen_run_id) VALUES (%s,%s,%s,%s)",
            (new_entity_id, actor, run_id, run_id),
        )
        cur.execute(
            "UPDATE entity_members SET valid_to = NOW() WHERE entity_id = %s AND customer_code = %s AND valid_to IS NULL",
            (entity_id, customer_code),
        )
        cur.execute(
            "INSERT INTO entity_members (entity_id, customer_code, run_id, source) VALUES (%s,%s,%s,'OFFICER')",
            (new_entity_id, customer_code, run_id),
        )

        override_ids = []
        for other_code in remaining:
            a_code, b_code = _order_pair(customer_code, other_code)
            cur.execute(
                "SELECT override_id FROM resolution_overrides WHERE a_code=%s AND b_code=%s AND revoked_at IS NULL",
                (a_code, b_code),
            )
            existing = cur.fetchone()
            if existing:
                cur.execute(
                    "UPDATE resolution_overrides SET revoked_at = NOW(), revoked_by = %s, revoke_reason = 'superseded by split' "
                    "WHERE override_id = %s",
                    (actor, existing[0]),
                )
            override_id = str(uuid4())
            cur.execute(
                "INSERT INTO resolution_overrides (override_id, verdict, a_code, b_code, entity_id, reason_code, reason, actor, decided_in_run_id) "
                "VALUES (%s,'MUST_NOT_LINK',%s,%s,%s,%s,%s,%s,%s)",
                (override_id, a_code, b_code, entity_id, reason_code, reason, actor, run_id),
            )
            override_ids.append(override_id)

        cur.execute(
            "INSERT INTO entity_lineage (entity_id, run_id, event, to_entity_ids, member_count, actor) VALUES (%s,%s,'SPLIT_OUT',%s::uuid[],1,%s)",
            (entity_id, run_id, [new_entity_id], actor),
        )
        cur.execute(
            "INSERT INTO entity_lineage (entity_id, run_id, event, from_entity_ids, member_count, actor) VALUES (%s,%s,'CREATED',%s::uuid[],1,%s)",
            (new_entity_id, run_id, [entity_id], actor),
        )

        audit_id, this_hash = _append_audit_event(
            cur, "CLUSTER_CREATED",
            {"action": "SPLIT", "customer_code": customer_code, "from_entity_id": entity_id, "new_entity_id": new_entity_id, "reason_code": reason_code, "reason": reason},
            actor, run_id,
        )

        decision_id = str(uuid4())
        cur.execute(
            "INSERT INTO review_decisions (decision_id, run_id, item_kind, entity_id, action, reason_code, reason, decided_by, audit_id) "
            "VALUES (%s,%s,'ENTITY',%s,'SPLIT',%s,%s,%s,%s)",
            (decision_id, run_id, new_entity_id, reason_code, reason, actor, audit_id),
        )

        pg_conn.commit()
        return {"decision_id": decision_id, "new_entity_id": new_entity_id, "override_ids": override_ids, "audit_id": audit_id, "this_hash": this_hash}
    except Exception:
        pg_conn.rollback()
        raise


def assign_global_ref(pg_conn, entity_id: str, global_ref: str, state: str, reason: str, actor: str) -> dict:
    """
    Assigns/edits an entity's bank-facing Global Ref. `state` is
    'DRAFT' or 'CONFIRMED' -- CONFIRMED is the state a bank would sync
    downstream; DRAFT lets an officer stage a value before committing.
    Uniqueness (case-insensitive) is enforced by the DB
    (ux_entities_global_ref); a collision raises WorkbenchError with
    the conflicting entity_id so the caller can 409 with useful detail.
    """
    if state not in ("DRAFT", "CONFIRMED"):
        raise WorkbenchError("state must be DRAFT or CONFIRMED")
    cur = pg_conn.cursor()
    try:
        _lock_audit_chain(cur)

        cur.execute(
            "SELECT entity_id::text FROM entities WHERE upper(global_ref) = upper(%s) AND entity_id != %s AND global_ref_state != 'RETIRED'",
            (global_ref, entity_id),
        )
        conflict = cur.fetchone()
        if conflict:
            raise WorkbenchError(f"Global Ref {global_ref!r} is already assigned to entity {conflict[0]}")

        cur.execute("SELECT global_ref, global_ref_state FROM entities WHERE entity_id = %s", (entity_id,))
        row = cur.fetchone()
        if row is None:
            raise WorkbenchError(f"Entity {entity_id} not found")
        previous_ref, previous_state = row
        event = "GLOBAL_REF_EDITED" if previous_ref else "GLOBAL_REF_ASSIGNED"

        cur.execute(
            "UPDATE entities SET global_ref = %s, global_ref_state = %s, updated_at = NOW(), updated_by = %s WHERE entity_id = %s",
            (global_ref, state, actor, entity_id),
        )
        cur.execute(
            "INSERT INTO entity_lineage (entity_id, event, actor) VALUES (%s,%s,%s)",
            (entity_id, event, actor),
        )

        # previous_ref/previous_state let revert_global_ref restore the
        # EXACT prior value (including back to no ref at all, if
        # previous_ref is None) without an officer having to retype it.
        audit_id, this_hash = _append_audit_event(
            cur, event,
            {"entity_id": entity_id, "global_ref": global_ref, "state": state,
             "previous_ref": previous_ref, "previous_state": previous_state, "reason": reason},
            actor,
        )

        decision_id = str(uuid4())
        cur.execute(
            "INSERT INTO review_decisions (decision_id, item_kind, entity_id, action, reason_code, reason, decided_by, audit_id) "
            "VALUES (%s,'ENTITY',%s,%s,'MANUAL',%s,%s,%s)",
            (decision_id, entity_id, "ASSIGN_GLOBAL_REF" if event == "GLOBAL_REF_ASSIGNED" else "EDIT_GLOBAL_REF", reason, actor, audit_id),
        )

        pg_conn.commit()
        return {"decision_id": decision_id, "entity_id": entity_id, "global_ref": global_ref, "state": state, "audit_id": audit_id, "this_hash": this_hash}
    except Exception:
        pg_conn.rollback()
        raise


def assign_global_ref_to_record(pg_conn, run_id: Optional[str], customer_code: str, global_ref: str, state: str, reason: str, actor: str) -> dict:
    """
    Lets an officer assign a Global ID directly to a single RECORD
    (customer_code) rather than an existing entity_id -- the path a
    singleton (never clustered, so it never earned an entity through
    resolve_entities' "only accepted, multi-member components" rule,
    see engine.clustering.entity_resolver's module docstring) needs to
    get a Global ID at all. Mints a fresh one-member entity on demand
    (_ensure_entity_for_code) in the SAME transaction as the global_ref
    assignment via assign_global_ref, so a rejected assignment (e.g. a
    global_ref collision) rolls back the entity mint too -- no orphan
    one-member entities left behind by a failed attempt. If the code
    already belongs to an entity (not actually a singleton), this is
    just an ordinary assign against that existing entity_id.
    """
    cur = pg_conn.cursor()
    entity_id = _ensure_entity_for_code(cur, customer_code, run_id, actor)
    return assign_global_ref(pg_conn, entity_id, global_ref, state, reason, actor)


def retire_global_ref(pg_conn, entity_id: str, reason: str, actor: str) -> dict:
    cur = pg_conn.cursor()
    try:
        _lock_audit_chain(cur)
        cur.execute("SELECT global_ref, global_ref_state FROM entities WHERE entity_id = %s", (entity_id,))
        row = cur.fetchone()
        if row is None:
            raise WorkbenchError(f"Entity {entity_id} not found")
        previous_ref, previous_state = row

        cur.execute(
            "UPDATE entities SET global_ref_state = 'RETIRED', updated_at = NOW(), updated_by = %s WHERE entity_id = %s",
            (actor, entity_id),
        )
        cur.execute("INSERT INTO entity_lineage (entity_id, event, actor) VALUES (%s,'GLOBAL_REF_RETIRED',%s)", (entity_id, actor))

        audit_id, this_hash = _append_audit_event(
            cur, "GLOBAL_REF_RETIRED",
            {"entity_id": entity_id, "previous_ref": previous_ref, "previous_state": previous_state, "reason": reason},
            actor,
        )
        decision_id = str(uuid4())
        cur.execute(
            "INSERT INTO review_decisions (decision_id, item_kind, entity_id, action, reason_code, reason, decided_by, audit_id) "
            "VALUES (%s,'ENTITY',%s,'RETIRE_GLOBAL_REF','MANUAL',%s,%s,%s)",
            (decision_id, entity_id, reason, actor, audit_id),
        )
        pg_conn.commit()
        return {"decision_id": decision_id, "entity_id": entity_id, "audit_id": audit_id, "this_hash": this_hash}
    except Exception:
        pg_conn.rollback()
        raise


# ----------------------------------------------------------------------
# Rollback -- undo a merge, revert a Global Ref change, or revoke an
# approve/reject override. Every action here is a NEW forward audit
# event whose effect cancels a prior one -- never a mutation/deletion
# of the original event or entities/entity_members row. The hash chain
# (_append_audit_event/verify_audit_chain) is append-only by design;
# "rollback" for a bank-grade audit trail has to mean "prove what was
# undone and when", not erase history.
# ----------------------------------------------------------------------

def undo_merge(pg_conn, entity_id: str, reason: str, actor: str) -> dict:
    """
    Reverses the most recent merge that absorbed `entity_id` into
    another entity -- resurrects it (status back to ACTIVE), moves its
    exact prior members back out of the survivor, restores any Global
    Ref the merge overwrote on the survivor, and revokes the
    resolution_overrides row the merge created so a future pipeline run
    doesn't silently re-apply it and undo the undo.

    Depends on `merge_snapshot` in the original merge's audit_events
    payload (see _merge_entities_tx/approve_pair/merge_entities) --
    before that was added, the exact member list an absorbed entity
    had was only a transient Python variable, discarded after commit,
    so no undo could ever be precise. Refuses (WorkbenchError) rather
    than guessing when:
      - entity_id isn't currently MERGED (nothing to undo).
      - No audit_events row for this specific merge carries a
        merge_snapshot -- true for every merge made before this
        capability existed; there's no other durable record of which
        codes moved.
      - Any of the merge's moved member codes are no longer under the
        surviving entity (a later merge or split touched them since)
        -- reversing here could not restore a consistent state.
    """
    cur = pg_conn.cursor()
    try:
        _lock_audit_chain(cur)

        cur.execute("SELECT status, merged_into_entity_id FROM entities WHERE entity_id = %s", (entity_id,))
        row = cur.fetchone()
        if row is None:
            raise WorkbenchError(f"Entity {entity_id} not found")
        status, merged_into = row
        if status != "MERGED" or not merged_into:
            raise WorkbenchError(f"Entity {entity_id} is not currently merged -- nothing to undo")

        cur.execute(
            "SELECT audit_id, payload_json FROM audit_events "
            "WHERE event_type IN ('REVIEW_APPROVED','CLUSTER_MERGED') "
            "AND payload_json->'merge_snapshot'->>'absorb_id' = %s "
            "ORDER BY created_at DESC LIMIT 1",
            (entity_id,),
        )
        found = cur.fetchone()
        if not found:
            raise WorkbenchError(
                f"No reversible merge record found for entity {entity_id} -- it may predate rollback support."
            )
        merge_audit_id, payload = found
        snapshot = payload["merge_snapshot"]
        keep_id = snapshot["keep_id"]
        moved_codes = snapshot["moved_member_codes"]
        override_id = snapshot.get("override_id")

        if keep_id != merged_into:
            raise WorkbenchError(
                f"Entity {entity_id} was merged into {merged_into}, but the most recent merge record points to "
                f"{keep_id} -- cannot safely undo (a later action likely changed where it went)."
            )

        cur.execute(
            "SELECT customer_code FROM entity_members WHERE entity_id = %s AND valid_to IS NULL AND customer_code = ANY(%s::text[])",
            (keep_id, moved_codes),
        )
        still_present = {r[0] for r in cur.fetchall()}
        missing = sorted(set(moved_codes) - still_present)
        if missing:
            raise WorkbenchError(
                f"Cannot safely undo -- {len(missing)} of {len(moved_codes)} member(s) moved by this merge are no "
                f"longer under entity {keep_id} (a later action touched them): {missing[:5]}"
            )

        for code in moved_codes:
            cur.execute(
                "UPDATE entity_members SET valid_to = NOW() WHERE entity_id = %s AND customer_code = %s AND valid_to IS NULL",
                (keep_id, code),
            )
            cur.execute(
                "INSERT INTO entity_members (entity_id, customer_code, source) VALUES (%s,%s,'OFFICER')",
                (entity_id, code),
            )

        cur.execute(
            "UPDATE entities SET status = 'ACTIVE', merged_into_entity_id = NULL, updated_at = NOW(), updated_by = %s WHERE entity_id = %s",
            (actor, entity_id),
        )

        if snapshot.get("keep_global_ref_changed"):
            # Order matters: clear keep_id's (carried-forward) ref BEFORE
            # restoring absorb_id's original one, or the two rows would
            # transiently hold the identical ref string and hit
            # ux_entities_global_ref -- same reason _merge_entities_tx
            # clears absorb_id's ref when it carries it forward.
            cur.execute(
                "UPDATE entities SET global_ref = %s, global_ref_state = %s, updated_at = NOW(), updated_by = %s WHERE entity_id = %s",
                (snapshot.get("keep_global_ref_before"), snapshot.get("keep_global_ref_state_before") or "UNASSIGNED", actor, keep_id),
            )
            cur.execute(
                "UPDATE entities SET global_ref = %s, global_ref_state = %s, updated_at = NOW(), updated_by = %s WHERE entity_id = %s",
                (snapshot.get("absorb_global_ref_before"), snapshot.get("absorb_global_ref_state_before") or "UNASSIGNED", actor, entity_id),
            )

        if override_id:
            cur.execute(
                "UPDATE resolution_overrides SET revoked_at = NOW(), revoked_by = %s, revoke_reason = %s "
                "WHERE override_id = %s AND revoked_at IS NULL",
                (actor, f"undone via UNDO_MERGE: {reason}", override_id),
            )

        cur.execute(
            "INSERT INTO entity_lineage (entity_id, event, to_entity_ids, member_count, actor) VALUES (%s,'MERGE_REVERSED',%s::uuid[],%s,%s)",
            (entity_id, [keep_id], len(moved_codes), actor),
        )
        cur.execute(
            "INSERT INTO entity_lineage (entity_id, event, from_entity_ids, member_count, actor) VALUES (%s,'UNMERGED_FROM',%s::uuid[],%s,%s)",
            (keep_id, [entity_id], len(moved_codes), actor),
        )

        audit_id, this_hash = _append_audit_event(
            cur, "MERGE_REVERSED",
            {"entity_id": entity_id, "keep_id": keep_id, "reversed_audit_id": merge_audit_id,
             "restored_member_codes": moved_codes, "reason": reason},
            actor,
        )

        decision_id = str(uuid4())
        cur.execute(
            "INSERT INTO review_decisions (decision_id, item_kind, entity_id, action, reason_code, reason, decided_by, audit_id) "
            "VALUES (%s,'ENTITY',%s,'UNDO_MERGE','MANUAL',%s,%s,%s)",
            (decision_id, entity_id, reason, actor, audit_id),
        )

        pg_conn.commit()
        return {
            "decision_id": decision_id, "entity_id": entity_id, "keep_id": keep_id,
            "restored_member_count": len(moved_codes), "audit_id": audit_id, "this_hash": this_hash,
        }
    except Exception:
        pg_conn.rollback()
        raise


def revert_global_ref(pg_conn, entity_id: str, reason: str, actor: str) -> dict:
    """
    Reverses this entity's most recent Global Ref action (assign, edit,
    or retire) back to whatever it was immediately before -- using
    previous_ref/previous_state captured in that action's own audit
    payload (see assign_global_ref/retire_global_ref). If there was no
    ref before (previous_ref is None), reverting clears it back to
    UNASSIGNED rather than leaving a stale value.
    """
    cur = pg_conn.cursor()
    try:
        _lock_audit_chain(cur)

        cur.execute(
            "SELECT audit_id, payload_json FROM audit_events "
            "WHERE event_type IN ('GLOBAL_REF_ASSIGNED','GLOBAL_REF_EDITED','GLOBAL_REF_RETIRED') "
            "AND payload_json->>'entity_id' = %s "
            "ORDER BY created_at DESC LIMIT 1",
            (entity_id,),
        )
        found = cur.fetchone()
        if not found:
            raise WorkbenchError(f"No Global Ref history found for entity {entity_id} to revert")
        source_audit_id, payload = found
        previous_ref = payload.get("previous_ref")
        previous_state = payload.get("previous_state") or "UNASSIGNED"

        if previous_ref and previous_state != "RETIRED":
            cur.execute(
                "SELECT entity_id::text FROM entities WHERE upper(global_ref) = upper(%s) AND entity_id != %s AND global_ref_state != 'RETIRED'",
                (previous_ref, entity_id),
            )
            conflict = cur.fetchone()
            if conflict:
                raise WorkbenchError(f"Cannot revert -- Global Ref {previous_ref!r} is now assigned to a different entity ({conflict[0]})")

        cur.execute(
            "UPDATE entities SET global_ref = %s, global_ref_state = %s, updated_at = NOW(), updated_by = %s WHERE entity_id = %s",
            (previous_ref, previous_state, actor, entity_id),
        )
        cur.execute("INSERT INTO entity_lineage (entity_id, event, actor) VALUES (%s,'GLOBAL_REF_REVERTED',%s)", (entity_id, actor))

        audit_id, this_hash = _append_audit_event(
            cur, "GLOBAL_REF_REVERTED",
            {"entity_id": entity_id, "reverted_audit_id": source_audit_id, "restored_ref": previous_ref,
             "restored_state": previous_state, "reason": reason},
            actor,
        )
        decision_id = str(uuid4())
        cur.execute(
            "INSERT INTO review_decisions (decision_id, item_kind, entity_id, action, reason_code, reason, decided_by, audit_id) "
            "VALUES (%s,'ENTITY',%s,'REVERT_GLOBAL_REF','MANUAL',%s,%s,%s)",
            (decision_id, entity_id, reason, actor, audit_id),
        )
        pg_conn.commit()
        return {
            "decision_id": decision_id, "entity_id": entity_id, "restored_ref": previous_ref,
            "restored_state": previous_state, "audit_id": audit_id, "this_hash": this_hash,
        }
    except Exception:
        pg_conn.rollback()
        raise


def revoke_override(pg_conn, override_id: str, reason: str, actor: str) -> dict:
    """
    Retracts a resolution_overrides row (an approve/reject verdict) so
    engine.clustering.build_clusters no longer re-applies it on future
    pipeline runs. Does NOT retroactively undo any merge that already
    happened as a result of the original approve -- if the pair is
    currently merged into one entity, use undo_merge separately for
    that; revoking here only stops FUTURE re-application.
    """
    cur = pg_conn.cursor()
    try:
        _lock_audit_chain(cur)

        cur.execute(
            "SELECT a_code, b_code, verdict, revoked_at FROM resolution_overrides WHERE override_id = %s",
            (override_id,),
        )
        row = cur.fetchone()
        if row is None:
            raise WorkbenchError(f"Override {override_id} not found")
        a_code, b_code, verdict, revoked_at = row
        if revoked_at is not None:
            raise WorkbenchError(f"Override {override_id} is already revoked")

        cur.execute(
            "UPDATE resolution_overrides SET revoked_at = NOW(), revoked_by = %s, revoke_reason = %s WHERE override_id = %s",
            (actor, reason, override_id),
        )

        audit_id, this_hash = _append_audit_event(
            cur, "OVERRIDE_REVOKED",
            {"override_id": override_id, "a_code": a_code, "b_code": b_code, "verdict": verdict, "reason": reason},
            actor,
        )
        decision_id = str(uuid4())
        cur.execute(
            "INSERT INTO review_decisions (decision_id, item_kind, a_code, b_code, action, reason_code, reason, decided_by, override_id, audit_id) "
            "VALUES (%s,'PAIR',%s,%s,'REVOKE_OVERRIDE','MANUAL',%s,%s,%s,%s)",
            (decision_id, a_code, b_code, reason, actor, override_id, audit_id),
        )
        pg_conn.commit()
        return {
            "decision_id": decision_id, "override_id": override_id, "verdict": verdict,
            "note": "This stops the decision from being re-applied on future pipeline runs. It does NOT undo any "
                    "merge or split that already happened as a result -- use Undo Merge on the entity separately if needed.",
            "audit_id": audit_id, "this_hash": this_hash,
        }
    except Exception:
        pg_conn.rollback()
        raise
