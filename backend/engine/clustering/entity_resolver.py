"""
CUIN v2 - Entity Carry-Forward Resolver (Stage 2 of the entity
resolution workbench plan)

Carries a durable platform entity_id forward across pipeline runs by
matching each newly-accepted cluster onto the entity whose CURRENT
membership has the highest Jaccard overlap -- see
db/migrations/005_entity_registry.sql and the plan's "Why identity
must anchor to records, not clusters". engine.clustering.cluster_
manager's cluster_id is a pure content hash of its own membership and
cannot serve this role: add or drop one member and it changes
completely with no lineage.

O(total members), NOT O(clusters x entities): 55,673 clusters against
a growing entity table would be billions of set comparisons if this
materialized full member sets to compare. Instead, Jaccard is derived
from three counts -- component size, existing-entity size, and overlap
count -- computed via ONE bulk customer_code -> entity_id fetch plus
in-memory tallying, never a per-candidate member-set fetch. This is a
single pass over ~1.5M memberships, seconds not hours.

Only ACCEPTED, multi-member components get entity identity. Rejected
components (cohesion.py's OVERSIZED_COMPONENT/LOW_COHESION verdicts)
and singletons never did and still don't here -- an entity is minted
lazily, when a cluster earns one or an officer assigns a Global ID
directly to a record.
"""

import logging
from dataclasses import dataclass
from typing import Dict, List, Optional, Set, Tuple
from uuid import uuid4

from psycopg2.extras import execute_values

logger = logging.getLogger(__name__)

# For a 2-member entity (the modal size), adding a member carries
# (J=0.67) but swapping one doesn't (J=0.33) -- 0.5 is a majority-of-
# union threshold, meaning at most one candidate entity can qualify
# except at an exact tie, which the deterministic tie-break below
# resolves. A policy, not a law -- worth exposing as a tunable in the
# rule catalog if a bank finds it too loose/strict in practice.
CARRY_MIN_JACCARD = 0.5


@dataclass
class EntityResolutionResult:
    root_to_entity: Dict[str, str]
    created_count: int
    carried_count: int
    split_count: int


def _fetch_current_membership(pg_conn) -> Tuple[Dict[str, str], Dict[str, int], Dict[str, dict]]:
    cur = pg_conn.cursor()
    cur.execute("SELECT customer_code, entity_id::text FROM entity_members WHERE valid_to IS NULL")
    code_to_entity: Dict[str, str] = dict(cur.fetchall())

    entity_size: Dict[str, int] = {}
    for eid in code_to_entity.values():
        entity_size[eid] = entity_size.get(eid, 0) + 1

    cur.execute("SELECT entity_id::text, global_ref, global_ref_state, created_at FROM entities WHERE status = 'ACTIVE'")
    entity_info = {
        eid: {"global_ref": ref, "global_ref_state": state, "created_at": created_at}
        for eid, ref, state, created_at in cur.fetchall()
    }
    return code_to_entity, entity_size, entity_info


def _pick_candidate(overlap_counts: Dict[str, int], component_size: int, entity_size: Dict[str, int], entity_info: Dict[str, dict]) -> Tuple[Optional[str], float]:
    """Deterministic: highest Jaccard, then highest raw overlap, then a CONFIRMED global_ref, then oldest, then lexicographic."""
    candidates = []
    for eid, overlap in overlap_counts.items():
        union_size = component_size + entity_size.get(eid, 0) - overlap
        jaccard = overlap / union_size if union_size > 0 else 0.0
        candidates.append((eid, overlap, jaccard))
    if not candidates:
        return None, 0.0

    def sort_key(c):
        eid, overlap, jaccard = c
        info = entity_info.get(eid, {})
        is_confirmed = info.get("global_ref_state") == "CONFIRMED"
        created_at = info.get("created_at")
        return (-jaccard, -overlap, 0 if is_confirmed else 1, created_at or "", eid)

    candidates.sort(key=sort_key)
    best_eid, _, best_jaccard = candidates[0]
    if best_jaccard >= CARRY_MIN_JACCARD:
        return best_eid, best_jaccard
    return None, 0.0


def resolve_entities(
    pg_conn,
    run_id: str,
    components: Dict[str, List[str]],
    accepted_roots: Set[str],
    actor: str = "pipeline",
    carry_forward: bool = True,
) -> EntityResolutionResult:
    """
    components: root -> member list (from UnionFind.get_clusters()).
    accepted_roots: which roots in `components` passed cohesion and
        have >= 2 members -- everything else is skipped, matching the
        module docstring's "only accepted, multi-member components".

    carry_forward: when False (a "run as new pipeline" / fresh-identity
        request from a bank officer -- see api/routes_datasource.py's
        DatasourceStartRequest.carry_forward), every accepted component
        mints a brand-new entity_id unconditionally, regardless of
        Jaccard overlap with prior entities -- achieved by simply never
        populating pass 1's overlap counts, so _pick_candidate always
        sees zero candidates and takes the CREATE branch. Membership
        deltas (closing old memberships, opening new ones) still use
        the REAL current membership snapshot either way -- codes moving
        to a fresh entity still need their prior membership closed, or
        ux_entity_members_current's one-current-entity-per-code
        invariant breaks. This is the escape hatch from the normal
        "update existing clusters" behavior every run has by default;
        most runs should never set this.

    Writes entities/entity_members/entity_lineage on `pg_conn` in
    BULK (psycopg2.extras.execute_values, page_size=5000 -- this
    codebase's established pattern, see db/repository.py) rather than
    one round trip per row: at first-run scale (55,673 new entities,
    ~1.5M memberships) a naive per-row cur.execute() loop measured as
    the dominant cost of the entire persist stage -- turning a ~4
    minute stage into 10+ minutes and climbing. Caller commits/rolls
    back -- this function issues no COMMIT itself, so it composes into
    a larger persistence transaction. Returns the root -> entity_id
    mapping plus counts for the caller's progress log.
    """
    code_to_entity, entity_size, entity_info = _fetch_current_membership(pg_conn)
    # Immutable pre-run snapshot -- code_to_entity itself gets mutated
    # during pass 2 (each root's "joined" loop updates it so later
    # roots in the SAME run see correct current-membership), so split
    # detection at the end needs a copy frozen before any of that.
    code_to_entity_before_this_run = dict(code_to_entity)
    cur = pg_conn.cursor()

    # Pass 1: compute each accepted root's overlap against the
    # PRE-RUN membership snapshot (never mutated during this pass), so
    # two components racing for the same entity (a genuine split, e.g.
    # a household cluster dividing into two people) are compared on
    # equal footing rather than whichever happens to be processed
    # first winning by default.
    root_overlap: Dict[str, Dict[str, int]] = {}
    root_best_jaccard: Dict[str, float] = {}
    for root in accepted_roots:
        members = components.get(root, [])
        if len(members) < 2:
            continue
        overlap_counts: Dict[str, int] = {}
        if carry_forward:
            for code in members:
                eid = code_to_entity.get(code)
                if eid:
                    overlap_counts[eid] = overlap_counts.get(eid, 0) + 1
        root_overlap[root] = overlap_counts
        _, best_j = _pick_candidate(overlap_counts, len(members), entity_size, entity_info)
        root_best_jaccard[root] = best_j

    # Pass 2: process strongest claims first (highest Jaccard, then
    # largest component, then root string for determinism) so the
    # component with the BEST claim to a prior entity wins it; a
    # weaker claim on an already-claimed entity falls through to its
    # next-best candidate or mints a new entity -- exactly the
    # "genuine split" case _detect_and_log_splits then reports.
    processing_order = sorted(
        root_overlap.keys(),
        key=lambda r: (-root_best_jaccard[r], -len(components[r]), r),
    )

    claimed_entities: Set[str] = set()
    root_to_entity: Dict[str, str] = {}
    created_count = carried_count = 0

    # Batch buffers -- filled during the pure in-memory pass below,
    # flushed to Postgres in bulk once at the end (see resolve_entities'
    # docstring for why this matters at 55K-entity scale).
    new_entity_rows: List[tuple] = []          # (entity_id, actor, run_id, run_id)
    carried_entity_ids: List[str] = []         # for the last_seen_run_id bulk update
    lineage_rows: List[tuple] = []             # (entity_id, run_id, event, from_ids_or_None, jaccard_or_None, member_count, actor)
    members_to_close: Set[Tuple[str, str]] = set()   # (entity_id, customer_code)
    members_to_open: List[tuple] = []          # (entity_id, customer_code, run_id, 'PIPELINE')

    for root in processing_order:
        members = components[root]
        overlap_counts = {eid: n for eid, n in root_overlap[root].items() if eid not in claimed_entities}
        component_size = len(members)
        chosen_entity, jaccard = _pick_candidate(overlap_counts, component_size, entity_size, entity_info)

        if chosen_entity:
            claimed_entities.add(chosen_entity)
            entity_id = chosen_entity
            carried_entity_ids.append(entity_id)
            lineage_rows.append((entity_id, run_id, "CARRIED", None, round(jaccard, 4), component_size, actor))
            carried_count += 1
        else:
            entity_id = str(uuid4())
            new_entity_rows.append((entity_id, actor, run_id, run_id))
            from_ids = list(overlap_counts.keys()) or None
            lineage_rows.append((entity_id, run_id, "CREATED", from_ids, None, component_size, actor))
            created_count += 1

        root_to_entity[root] = entity_id

        # Membership delta: only touch codes that actually moved.
        current_members_of_entity = {c for c, e in code_to_entity.items() if e == entity_id}
        new_member_set = set(members)
        joined = new_member_set - current_members_of_entity
        left = current_members_of_entity - new_member_set

        for code in left:
            members_to_close.add((entity_id, code))
        for code in joined:
            prior_entity = code_to_entity.get(code)
            if prior_entity and prior_entity != entity_id:
                # ux_entity_members_current allows only one CURRENT entity per code --
                # close the prior membership before opening this one.
                members_to_close.add((prior_entity, code))
            members_to_open.append((entity_id, code, run_id, "PIPELINE"))
            code_to_entity[code] = entity_id  # keep the in-memory map correct for subsequent roots in this loop

    if new_entity_rows:
        execute_values(
            cur, "INSERT INTO entities (entity_id, created_by, first_seen_run_id, last_seen_run_id) VALUES %s",
            new_entity_rows, template="(%s::uuid,%s,%s::uuid,%s::uuid)", page_size=5000,
        )
    if carried_entity_ids:
        execute_values(
            cur,
            "UPDATE entities SET last_seen_run_id = data.run_id::uuid, updated_at = NOW() "
            "FROM (VALUES %s) AS data(entity_id, run_id) "
            "WHERE entities.entity_id = data.entity_id::uuid",
            [(eid, run_id) for eid in carried_entity_ids],
            page_size=5000,
        )
    if lineage_rows:
        execute_values(
            cur,
            "INSERT INTO entity_lineage (entity_id, run_id, event, from_entity_ids, jaccard, member_count, actor) VALUES %s",
            lineage_rows, template="(%s::uuid,%s::uuid,%s,%s::uuid[],%s,%s,%s)", page_size=5000,
        )
    if members_to_close:
        execute_values(
            cur,
            "UPDATE entity_members SET valid_to = NOW() "
            "FROM (VALUES %s) AS data(entity_id, customer_code) "
            "WHERE entity_members.entity_id = data.entity_id::uuid "
            "AND entity_members.customer_code = data.customer_code "
            "AND entity_members.valid_to IS NULL",
            list(members_to_close), page_size=5000,
        )
    if members_to_open:
        execute_values(
            cur, "INSERT INTO entity_members (entity_id, customer_code, run_id, source) VALUES %s",
            members_to_open, template="(%s::uuid,%s,%s::uuid,%s)", page_size=5000,
        )

    split_count = _detect_and_log_splits(cur, run_id, code_to_entity_before_this_run, root_to_entity, components, accepted_roots, actor)

    return EntityResolutionResult(root_to_entity, created_count, carried_count, split_count)


def _detect_and_log_splits(cur, run_id, code_to_entity_before, root_to_entity, components, accepted_roots, actor) -> int:
    """
    A prior entity whose members are now spread across >= 2 DIFFERENT
    CURRENT entities this run (counting itself, if one successor
    carried its own entity_id forward -- that's still one of the
    "homes" its members ended up in) is a genuine split -- logged so
    an officer can see why that entity's Global ID (if it had one)
    stopped covering everyone it used to. Members that simply stopped
    clustering (now singletons) are not a split, just churn, and are
    not logged here. Deliberately does NOT filter out `prior ==
    new_entity` -- that case is exactly what makes "the entity kept
    one fragment and lost another" distinguishable from "the entity
    moved to one brand-new place", both of which are splits.
    """
    prior_entity_new_homes: Dict[str, set] = {}
    for root in accepted_roots:
        members = components.get(root, [])
        if len(members) < 2:
            continue
        new_entity = root_to_entity.get(root)
        if not new_entity:
            continue
        for code in members:
            prior = code_to_entity_before.get(code)
            if prior:
                prior_entity_new_homes.setdefault(prior, set()).add(new_entity)

    split_rows = [
        (prior_entity, run_id, list(new_homes), actor)
        for prior_entity, new_homes in prior_entity_new_homes.items()
        if len(new_homes) >= 2
    ]
    if split_rows:
        execute_values(
            cur,
            "INSERT INTO entity_lineage (entity_id, run_id, event, to_entity_ids, actor) VALUES %s",
            split_rows, template="(%s::uuid,%s::uuid,'SPLIT_OUT',%s::uuid[],%s)", page_size=5000,
        )
    return len(split_rows)
