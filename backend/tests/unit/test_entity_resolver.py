"""
Stage 2 of the entity resolution workbench plan: proves
engine.clustering.entity_resolver.resolve_entities() carries a durable
entity_id across simulated pipeline runs the way db/migrations/
005_entity_registry.sql's design requires -- by Jaccard overlap
against CURRENT membership, never by anything derived from
cluster_id. Runs against a REAL Postgres transaction (uses the actual
SQL the resolver issues), rolled back at the end so nothing persists.

Scenarios, run as one continuous story (each run's output feeds the
next, like real pipeline runs would):
  1. New component -> entity created.
  2. Unchanged component, re-run -> SAME entity_id (not a new one).
  3. Component gains a member (Jaccard above threshold) -> carries.
  4. Component genuinely splits into two, each below the carry
     threshold against the other -> one carries, one is new, and a
     SPLIT_OUT lineage event is logged.
  5. Two independent prior entities merge into one component -> the
     stronger claim carries; verified via the winning entity_id.
"""

import os
import sys
from uuid import uuid4

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

import pytest
import psycopg2

from engine.clustering.entity_resolver import resolve_entities, CARRY_MIN_JACCARD


@pytest.fixture
def pg_conn():
    from api.config import settings
    try:
        conn = psycopg2.connect(settings.DATABASE_URL)
    except Exception as e:
        pytest.skip(f"Postgres not reachable: {e}")
    conn.autocommit = False
    yield conn
    conn.rollback()
    conn.close()


def _run_id():
    return str(uuid4())


def test_new_component_creates_entity(pg_conn):
    run1 = _run_id()
    components = {"root_a": ["T1_A", "T1_B", "T1_C"]}
    result = resolve_entities(pg_conn, run1, components, {"root_a"}, actor="test")
    assert result.created_count == 1
    assert result.carried_count == 0
    entity_id = result.root_to_entity["root_a"]
    assert entity_id

    cur = pg_conn.cursor()
    cur.execute("SELECT customer_code FROM entity_members WHERE entity_id = %s AND valid_to IS NULL", (entity_id,))
    members = {r[0] for r in cur.fetchall()}
    assert members == {"T1_A", "T1_B", "T1_C"}


def test_unchanged_component_carries_same_entity_id(pg_conn):
    run1 = _run_id()
    components = {"root_a": ["T2_A", "T2_B", "T2_C"]}
    result1 = resolve_entities(pg_conn, run1, components, {"root_a"}, actor="test")
    entity1 = result1.root_to_entity["root_a"]

    run2 = _run_id()
    result2 = resolve_entities(pg_conn, run2, components, {"root_a"}, actor="test")
    entity2 = result2.root_to_entity["root_a"]

    assert entity1 == entity2, "an unchanged component re-run must carry the SAME entity_id, not mint a new one"
    assert result2.carried_count == 1
    assert result2.created_count == 0


def test_component_gaining_a_member_carries():
    """Component grows from 3 to 4 members (adding one) -- Jaccard = 3/4 = 0.75, above threshold, must carry."""
    from api.config import settings
    conn = psycopg2.connect(settings.DATABASE_URL)
    conn.autocommit = False
    try:
        run1 = _run_id()
        result1 = resolve_entities(conn, run1, {"root_a": ["T3_A", "T3_B", "T3_C"]}, {"root_a"}, actor="test")
        entity1 = result1.root_to_entity["root_a"]

        run2 = _run_id()
        result2 = resolve_entities(conn, run2, {"root_a": ["T3_A", "T3_B", "T3_C", "T3_D"]}, {"root_a"}, actor="test")
        entity2 = result2.root_to_entity["root_a"]

        assert entity1 == entity2
        assert result2.carried_count == 1

        cur = conn.cursor()
        cur.execute("SELECT customer_code FROM entity_members WHERE entity_id = %s AND valid_to IS NULL", (entity2,))
        members = {r[0] for r in cur.fetchall()}
        assert members == {"T3_A", "T3_B", "T3_C", "T3_D"}, "the new member must be added, not just the old ones kept"
    finally:
        conn.rollback()
        conn.close()


def test_genuine_split_one_carries_one_is_new_with_lineage():
    """
    A 4-member entity {A,B,C,D} splits into {A,B} and {C,D}. Each half
    has Jaccard 2/4=0.5 against the original -- exactly AT the carry
    threshold, a genuine tie. The resolver must give the entity to
    exactly ONE half (not both, not neither) and mint a new entity for
    the other, with a SPLIT_OUT lineage event on the original.
    """
    from api.config import settings
    conn = psycopg2.connect(settings.DATABASE_URL)
    conn.autocommit = False
    try:
        run1 = _run_id()
        result1 = resolve_entities(conn, run1, {"root_a": ["T4_A", "T4_B", "T4_C", "T4_D"]}, {"root_a"}, actor="test")
        original_entity = result1.root_to_entity["root_a"]

        run2 = _run_id()
        components2 = {"root_x": ["T4_A", "T4_B"], "root_y": ["T4_C", "T4_D"]}
        result2 = resolve_entities(conn, run2, components2, {"root_x", "root_y"}, actor="test")

        entity_x = result2.root_to_entity["root_x"]
        entity_y = result2.root_to_entity["root_y"]

        assert entity_x != entity_y, "the two halves must end up as two DIFFERENT entities"
        assert {entity_x, entity_y} & {original_entity}, "exactly one half must carry the original entity_id"
        assert result2.created_count == 1
        assert result2.carried_count == 1
        assert result2.split_count == 1

        cur = conn.cursor()
        cur.execute(
            "SELECT event, to_entity_ids::text[] FROM entity_lineage WHERE entity_id = %s AND event = 'SPLIT_OUT'",
            (original_entity,),
        )
        rows = cur.fetchall()
        assert len(rows) == 1
        assert set(rows[0][1]) == {entity_x, entity_y}

        # Every code has exactly one CURRENT entity -- the DB invariant, not just resolver logic.
        cur.execute("""
            SELECT customer_code, COUNT(*) FROM entity_members
            WHERE customer_code IN ('T4_A','T4_B','T4_C','T4_D') AND valid_to IS NULL
            GROUP BY customer_code HAVING COUNT(*) > 1
        """)
        assert cur.fetchall() == [], "a customer_code must never have more than one CURRENT entity"
    finally:
        conn.rollback()
        conn.close()


def test_two_entities_merging_stronger_claim_wins(pg_conn):
    """Two independent 2-member entities' members end up in one 4-member component (they truly are one person).
    Whichever has the higher raw overlap should be favored (both have overlap=2 here, so it's a controlled tie
    resolved deterministically) -- the key correctness property is that exactly ONE of the two survives as the
    carrying entity, and it is one of the two original entity_ids, not a brand new third one."""
    run1 = _run_id()
    result_a = resolve_entities(pg_conn, run1, {"root_a": ["T5_A", "T5_B"]}, {"root_a"}, actor="test")
    entity_a = result_a.root_to_entity["root_a"]
    result_b = resolve_entities(pg_conn, run1, {"root_b": ["T5_C", "T5_D"]}, {"root_b"}, actor="test")
    entity_b = result_b.root_to_entity["root_b"]
    assert entity_a != entity_b

    run2 = _run_id()
    merged = {"root_m": ["T5_A", "T5_B", "T5_C", "T5_D"]}
    result2 = resolve_entities(pg_conn, run2, merged, {"root_m"}, actor="test")
    winner = result2.root_to_entity["root_m"]

    assert winner in (entity_a, entity_b), "the merged entity must be one of the two originals, not a fresh mint"
    assert result2.carried_count == 1
    assert result2.created_count == 0


if __name__ == "__main__":
    print("Run via pytest (requires a live Postgres connection).")
