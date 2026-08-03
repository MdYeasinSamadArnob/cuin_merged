"""
CUIN v2 - Parallel pair scoring

The per-pair scoring/decision step (engine.scoring.confidence.score_pair,
called once per candidate pair) was a single-threaded Python `for` loop
in doris_orchestrator.py -- measured at ~757 pairs/second on one core
(549,879 pairs / 726s, the same Doris run cited throughout the infra
report), the actual throughput bottleneck for the whole pipeline,
independent of how fast the underlying database is. Doris's own
ingestion/query engine was never the slow part; this unparallelized
Python loop was.

Each pair's score is fully independent of every other pair's (no
shared mutable state, no ordering dependency), so this is genuinely
"embarrassingly parallel" -- not a hopeful assumption, a property of
the workload. This module splits the candidate pairs into chunks and
scores them across a process pool, not a thread pool: score_pair() is
pure-Python, CPU-bound work (dict/list operations, dataclass
construction) that the GIL would otherwise fully serialize even with
threads -- only separate processes actually run this concurrently.

Workers receive evidence pre-sliced to just their own chunk's pairs
(not the whole run's evidence dict) to keep inter-process pickling
cost bounded, and return plain, picklable result tuples -- no side
effects (no review-queue writes, no shared mutable state) happen
inside a worker; the caller merges every chunk's results after it
completes. This makes the function a behavior-preserving drop-in
replacement for the old serial loop: same inputs, same outputs, just
computed concurrently.
"""

import os
from concurrent.futures import ProcessPoolExecutor
from typing import Dict, List, Optional, Tuple

from engine.scoring.confidence import score_pair
from engine.scoring.evidence import evidence_to_field_evidence
from engine.segments.relationships import detect_relationship_evidence
from engine.structures import MatchDecision, MatchScore

_EMPTY_NAME_DOB = {"tokens_a": [], "tokens_b": [], "token_intersection": [], "token_union": []}


def _score_chunk(
    chunk_pairs: List[Tuple[str, str]],
    id_evidence: Dict[Tuple[str, str], list],
    name_dob_evidence: Dict[Tuple[str, str], dict],
    segments: Dict[str, str],
    raw_columns: List[str],
    raw_by_code: Dict[str, dict],
    catalog,
    build_full_evidence: bool,
) -> list:
    """
    Runs in a worker process -- must be a module-level function (not a
    closure/method) so ProcessPoolExecutor can pickle a reference to it.
    Pure function: reads only its arguments, returns plain tuples, no
    I/O and no mutation of anything outside its own locals. Mirrors the
    exact per-pair logic the old serial loop ran, rule for rule.
    """
    results = []
    for a_key, b_key in chunk_pairs:
        key = (a_key, b_key)
        evidence = {
            "identifiers": id_evidence.get(key, []),
            "name_dob": name_dob_evidence.get(key, _EMPTY_NAME_DOB),
        }
        if raw_columns:
            fields_a, fields_b = raw_by_code.get(a_key, {}), raw_by_code.get(b_key, {})
            evidence["raw_fields"] = {col: (fields_a.get(col), fields_b.get(col)) for col in raw_columns}

        seg_a = segments.get(a_key, "ALL")
        seg_b = segments.get(b_key, "ALL")
        if seg_a != seg_b:
            # Cross-segment: not an identity decision -- a traceable
            # relationship instead. Mirrors the orchestrators' original
            # inline handling exactly.
            rel_evidence = detect_relationship_evidence(evidence)
            results.append((
                "relationship", a_key, b_key, seg_a, seg_b,
                [e.to_dict() for e in rel_evidence] if rel_evidence else None,
            ))
            continue

        segment_ruleset = catalog.match_ruleset_for_segment(seg_a)
        pair_score = score_pair(evidence, segment_ruleset)
        decision = MatchDecision(pair_score.decision)
        score_value = pair_score.confidence_pct / 100.0
        # Building the human-readable per-field explanation strings is
        # real, measurable per-pair cost that nothing in the default
        # (Doris-native workbench) read path actually consumes -- see
        # the caller's build_full_evidence docstring for exactly when
        # this is/isn't skipped, and why that's safe.
        field_evidence = evidence_to_field_evidence(evidence) if build_full_evidence else []

        results.append((
            "scored", a_key, b_key, decision.value, score_value,
            field_evidence, pair_score.vetoes, pair_score.signals_hit,
        ))
    return results


def _chunk(items: list, n_chunks: int) -> List[list]:
    if not items:
        return []
    n_chunks = max(1, min(n_chunks, len(items)))
    size = -(-len(items) // n_chunks)  # ceil division
    return [items[i:i + size] for i in range(0, len(items), size)]


def score_pairs_parallel(
    pairs: List[Tuple[str, str]],
    id_evidence: Dict[Tuple[str, str], list],
    name_dob_evidence: Dict[Tuple[str, str], dict],
    segments: Dict[str, str],
    raw_columns: List[str],
    raw_by_code: Dict[str, dict],
    catalog,
    build_full_evidence: bool = True,
    max_workers: Optional[int] = None,
):
    """
    Drop-in replacement for the orchestrators' old serial
    `for a_key, b_key in pairs: ...` scoring loop. Returns
    (auto_links, review_items, rejected, scores, decisions, relationships)
    -- the exact same shapes the serial loop produced.

    build_full_evidence controls whether the expensive per-field
    explanation strings (engine.scoring.evidence.evidence_to_field_evidence)
    get built for every pair. Callers should pass build_full_evidence=False
    unconditionally -- verified its only consumer, api/routes_matches.py's
    POST /{pair_id}/explain (reading MatchScore.evidence's comparison_type/
    similarity_score fields), is itself unreachable from production: the
    frontend's "Ask Referee" button calls api.explainMatch(), which is
    never actually defined in frontend/src/lib/api.ts (only called via
    an `as any` cast, which is how it silently avoided a build-time type
    error). The live review workbench's referee feature
    (api/routes_review.py) reads its own stored explanation from
    Postgres's referee_explanations table instead, generated once at
    score time if invoked for a gray-zone pair -- never from
    MatchScore.evidence. The production workbench
    (/workbench/pairs/{a}/{b}/breakdown) similarly reads evidence
    straight from this run's own pair_identifier_evidence/
    pair_name_dob_evidence/pair_contributions tables, never from
    MatchScore.evidence. Building this data was therefore pure waste at
    any scale -- confirmed as the dominant remaining cost in
    persist_candidate_pairs_and_decisions (match_scores.evidence_json,
    a JSONB column) once the scoring-loop and entity-resolver bottlenecks
    were fixed. Kept as a parameter rather than deleted outright in case
    /explain is ever wired up for real in the future.
    """
    max_workers = max_workers or os.cpu_count() or 4
    chunks = _chunk(pairs, max_workers)

    auto_links, review_items, rejected = [], [], []
    scores: Dict[str, MatchScore] = {}
    decisions: Dict[str, MatchDecision] = {}
    relationships: List[dict] = []

    if not chunks:
        return auto_links, review_items, rejected, scores, decisions, relationships

    with ProcessPoolExecutor(max_workers=len(chunks)) as pool:
        futures = []
        for chunk_pairs in chunks:
            # Slice evidence down to just this chunk's pairs/codes so
            # each worker is only pickled its own share of the run's
            # evidence, not the whole thing.
            chunk_keys = set(chunk_pairs)
            chunk_codes = {c for pair in chunk_pairs for c in pair}
            chunk_id_evidence = {k: v for k, v in id_evidence.items() if k in chunk_keys}
            chunk_name_dob = {k: v for k, v in name_dob_evidence.items() if k in chunk_keys}
            chunk_segments = {c: segments[c] for c in chunk_codes if c in segments}
            chunk_raw_by_code = (
                {c: raw_by_code[c] for c in chunk_codes if c in raw_by_code} if raw_columns else {}
            )
            futures.append(pool.submit(
                _score_chunk, chunk_pairs, chunk_id_evidence, chunk_name_dob,
                chunk_segments, raw_columns, chunk_raw_by_code, catalog, build_full_evidence,
            ))

        for future in futures:
            for row in future.result():
                if row[0] == "relationship":
                    _, a_key, b_key, seg_a, seg_b, shared_evidence = row
                    if shared_evidence:
                        relationships.append({
                            "a_key": a_key, "b_key": b_key,
                            "a_segment": seg_a, "b_segment": seg_b,
                            "shared_evidence": shared_evidence,
                        })
                    continue

                _, a_key, b_key, decision_value, score_value, field_evidence, hard_conflicts, signals_hit = row
                pair_id = f"{a_key}:{b_key}"
                decision = MatchDecision(decision_value)
                match_score = MatchScore(
                    pair_id=pair_id, a_key=a_key, b_key=b_key, score=score_value,
                    evidence=field_evidence, hard_conflicts=hard_conflicts, signals_hit=signals_hit,
                )
                scores[pair_id] = match_score
                decisions[pair_id] = decision

                if decision == MatchDecision.AUTO_LINK:
                    auto_links.append((a_key, b_key))
                elif decision == MatchDecision.REVIEW:
                    review_items.append((a_key, b_key))
                else:
                    rejected.append((a_key, b_key))

    return auto_links, review_items, rejected, scores, decisions, relationships
