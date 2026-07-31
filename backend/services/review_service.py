"""
CUIN v2 - Review Service

Manages the human review queue for uncertain matches.
Implements maker-checker workflow with audit logging.

Persisted PER RUN under data/runs/{run_id}_review_queue.json (one file
per run, not one global file) plus a small pair->run index
(data/review_pair_index.json) so a lookup by pair_id can find the
right run's file without scanning every run. This replaces the
original single global data/review_queue.json design, which measured
at 1.87 GB / 1,711,686 items across 9 runs and was:

1. Fully re-`json.dump`ed on EVERY approve/reject click AND after every
   pipeline run's bulk queue_for_review loop -- writing the whole
   history to disk to persist one item's status change.
2. Built via `dataclasses.asdict(item)`, which deep-copies every
   nested field (evidence lists) for every item, every save -- far
   more expensive than the flat, hand-written `ReviewItem.to_dict()`
   already used for API responses.
3. Loaded entirely into memory at import time (module-level singleton
   construction), so a fresh server boot paid the full multi-GB
   deserialization cost before handling a single request, even if
   nobody opened the review queue.

Now: a run's items live only in their own file, and no run's items are
loaded into memory until something actually asks for them
(queue_for_review, get_by_pair, or a browse-all query that touches
that run). The "browse all runs" queue view (the officer inbox, which
has no run_id filter) still needs to enumerate across runs, so it
lazily loads the N most recently started runs
(_MAX_RUNS_IN_BROWSE_VIEW) rather than the unbounded historical set --
a retention window, not a hard delete; nothing is removed from disk.

The base snapshot (`_run_queue_path`) is still one JSON file per run,
written once via `_save_queue(run_id)` after a pipeline's bulk
queue_for_review loop -- fine, since even a 190K-item run's evidence
payload is tens of MB and this happens once per run, not once per
click. What changed for the interactive path: approve()/reject() no
longer rewrite that snapshot at all. A human decision instead appends
one line to a per-run JSONL delta log (`_run_updates_path`) -- O(1)
disk I/O regardless of how large the run's queue is, instead of
O(queue size) on every click. `_ensure_run_loaded` replays the delta
log on top of the base snapshot to reconstruct current status.
"""

from datetime import datetime
from typing import Dict, List, Optional, Tuple
from uuid import uuid4
from dataclasses import dataclass
from enum import Enum
import json
import logging
import os

from services.audit import log_audit_event, AuditEventType
from engine.clustering import get_cluster_manager
from agents.referee_agent import get_referee

logger = logging.getLogger(__name__)

_RUNS_DIR = "data/runs"
_PAIR_INDEX_PATH = "data/review_pair_index.json"
_QUEUE_SUFFIX = "_review_queue.json"
_UPDATES_SUFFIX = "_review_updates.jsonl"

# How many of the most-recently-started runs the "browse all runs"
# queue view (no run_id filter -- the officer's default inbox) will
# lazily load. Older runs' review items are still on disk and still
# reachable via an explicit ?run_id= filter or a direct pair_id lookup
# (both go through the pair index, not this cap) -- this only bounds
# the unfiltered inbox view so it can't degrade into reading dozens of
# runs' worth of files on every page load.
_MAX_RUNS_IN_BROWSE_VIEW = 20


def _run_queue_path(run_id: str) -> str:
    return os.path.join(_RUNS_DIR, f"{run_id}{_QUEUE_SUFFIX}")


def _run_updates_path(run_id: str) -> str:
    return os.path.join(_RUNS_DIR, f"{run_id}{_UPDATES_SUFFIX}")


class ReviewStatus(str, Enum):
    PENDING = "PENDING"
    APPROVED = "APPROVED"
    REJECTED = "REJECTED"


@dataclass
class ReviewItem:
    """A review queue item."""
    review_id: str
    pair_id: str
    run_id: str
    a_key: str
    b_key: str
    score: float
    evidence: List[dict]
    signals: List[str]
    status: ReviewStatus
    reviewer: Optional[str] = None
    review_reason: Optional[str] = None
    reviewed_at: Optional[datetime] = None
    created_at: datetime = None
    has_ai_explanation: bool = False

    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.utcnow()

    def to_dict(self) -> dict:
        return {
            'review_id': self.review_id,
            'pair_id': self.pair_id,
            'run_id': self.run_id,
            'a_key': self.a_key,
            'b_key': self.b_key,
            'score': self.score,
            'evidence': self.evidence,
            'signals': self.signals,
            'status': self.status.value,
            'reviewer': self.reviewer,
            'review_reason': self.review_reason,
            'reviewed_at': self.reviewed_at.isoformat() if self.reviewed_at else None,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'has_ai_explanation': self.has_ai_explanation,
        }


class ReviewService:
    """
    Service for managing the human review queue.

    Features:
    - Queue items for review
    - Approve/reject with mandatory reason
    - Request AI explanation for gray-zone cases
    - Audit all decisions
    """

    def __init__(self):
        self._items: Dict[str, ReviewItem] = {}
        self._by_pair: Dict[str, str] = {}  # pair_id -> review_id
        self._loaded_runs: set = set()
        self._dirty_runs: set = set()  # runs with in-memory changes not yet index-flushed
        self._pair_index: Dict[str, str] = {}  # pair_id -> run_id (cheap, loaded eagerly)
        self._load_pair_index()

    # ------------------------------------------------------------------
    # Persistence -- per-run queue files + a small cross-run pair index
    # ------------------------------------------------------------------
    def _load_pair_index(self) -> None:
        try:
            if os.path.exists(_PAIR_INDEX_PATH):
                with open(_PAIR_INDEX_PATH, "r") as f:
                    self._pair_index = json.load(f)
                logger.info(f"Loaded review pair index ({len(self._pair_index)} entries)")
        except Exception as e:
            logger.error(f"Failed to load review pair index: {e}")

    def _save_pair_index(self) -> None:
        try:
            os.makedirs(os.path.dirname(_PAIR_INDEX_PATH) or ".", exist_ok=True)
            with open(_PAIR_INDEX_PATH, "w") as f:
                json.dump(self._pair_index, f)
        except Exception as e:
            logger.error(f"Failed to save review pair index: {e}")

    def _ensure_run_loaded(self, run_id: str) -> None:
        """Idempotent lazy load: a run's items are read from disk at most once per process lifetime."""
        if run_id in self._loaded_runs:
            return
        self._loaded_runs.add(run_id)
        path = _run_queue_path(run_id)
        if os.path.exists(path):
            try:
                with open(path, "r") as f:
                    data = json.load(f)
                for rid, item_data in data.items():
                    if item_data.get("created_at"):
                        item_data["created_at"] = datetime.fromisoformat(item_data["created_at"])
                    if item_data.get("reviewed_at"):
                        item_data["reviewed_at"] = datetime.fromisoformat(item_data["reviewed_at"])
                    item_data["status"] = ReviewStatus(item_data["status"])
                    self._items[rid] = ReviewItem(**item_data)
                    self._by_pair[item_data["pair_id"]] = rid
                    self._pair_index[item_data["pair_id"]] = run_id
            except Exception as e:
                logger.error(f"Failed to load review queue for run {run_id}: {e}")

        # Replay approve/reject deltas recorded since the base snapshot
        # was last written -- see _append_update.
        updates_path = _run_updates_path(run_id)
        if os.path.exists(updates_path):
            try:
                with open(updates_path, "r") as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        delta = json.loads(line)
                        item = self._items.get(delta["review_id"])
                        if item:
                            item.status = ReviewStatus(delta["status"])
                            item.reviewer = delta["reviewer"]
                            item.review_reason = delta["review_reason"]
                            item.reviewed_at = datetime.fromisoformat(delta["reviewed_at"])
            except Exception as e:
                logger.error(f"Failed to replay review updates for run {run_id}: {e}")

    def _save_queue(self, run_id: str) -> None:
        """
        Persist ONE run's items to its own base-snapshot file, and
        clear its delta log (the snapshot now already reflects any
        replayed deltas). Not called from queue_for_review directly --
        pipeline orchestrators queue review items in a tight loop (up
        to ~190K calls on the full dataset); callers doing bulk
        queueing must call this ONCE after the loop finishes, scoped to
        their own run_id. Interactive approve()/reject() do NOT call
        this -- see _append_update -- specifically because rewriting a
        190K-item run's full snapshot on every single click was exactly
        the cost this redesign targets.
        """
        try:
            os.makedirs(_RUNS_DIR, exist_ok=True)
            run_items = {
                rid: item.to_dict()
                for rid, item in self._items.items()
                if item.run_id == run_id
            }
            with open(_run_queue_path(run_id), "w") as f:
                json.dump(run_items, f)
            updates_path = _run_updates_path(run_id)
            if os.path.exists(updates_path):
                os.remove(updates_path)
            self._save_pair_index()
        except Exception as e:
            logger.error(f"Failed to save review queue for run {run_id}: {e}")

    def _append_update(self, item: "ReviewItem") -> None:
        """
        O(1) persistence for a single approve()/reject() decision --
        append one JSON line to the run's delta log rather than
        rewriting the full per-run snapshot. _ensure_run_loaded replays
        this log on top of the base snapshot on next load.
        """
        try:
            os.makedirs(_RUNS_DIR, exist_ok=True)
            delta = {
                "review_id": item.review_id,
                "status": item.status.value,
                "reviewer": item.reviewer,
                "review_reason": item.review_reason,
                "reviewed_at": item.reviewed_at.isoformat(),
            }
            with open(_run_updates_path(item.run_id), "a") as f:
                f.write(json.dumps(delta) + "\n")
        except Exception as e:
            logger.error(f"Failed to append review update for run {item.run_id}: {e}")

    def queue_for_review(
        self,
        pair_id: str,
        run_id: str,
        a_key: str,
        b_key: str,
        score: float,
        evidence: List[dict],
        signals: List[str] = None
    ) -> ReviewItem:
        """Add a pair to the review queue."""
        self._ensure_run_loaded(run_id)
        review_id = str(uuid4())

        # Check if AI explanation should be generated
        referee = get_referee()
        has_explanation = False

        if referee.should_invoke(score, []):
            # Generate explanation for gray-zone cases
            # This would be called with full record data in real impl
            has_explanation = True

        item = ReviewItem(
            review_id=review_id,
            pair_id=pair_id,
            run_id=run_id,
            a_key=a_key,
            b_key=b_key,
            score=score,
            evidence=evidence,
            signals=signals or [],
            status=ReviewStatus.PENDING,
            has_ai_explanation=has_explanation,
        )

        self._items[review_id] = item
        self._by_pair[pair_id] = review_id
        self._pair_index[pair_id] = run_id

        # Log audit event
        log_audit_event(
            AuditEventType.DECISION_REVIEW,
            {
                'pair_id': pair_id,
                'review_id': review_id,
                'score': score,
                'signals': signals,
            },
            run_id=run_id
        )

        return item

    def _resolve_run_for_pair(self, pair_id: str) -> Optional[str]:
        if pair_id in self._by_pair:
            return self._items[self._by_pair[pair_id]].run_id
        return self._pair_index.get(pair_id)

    def get_item(self, review_id: str) -> Optional[ReviewItem]:
        """Get a review item by ID. Only resolves items whose run has already been loaded."""
        return self._items.get(review_id)

    def get_by_pair(self, pair_id: str) -> Optional[ReviewItem]:
        """Get a review item by pair ID, lazily loading its run if needed."""
        run_id = self._resolve_run_for_pair(pair_id)
        if run_id:
            self._ensure_run_loaded(run_id)
        review_id = self._by_pair.get(pair_id)
        if review_id:
            return self._items.get(review_id)
        return None

    def _browse_run_ids(self, explicit_run_id: Optional[str]) -> List[str]:
        """
        Which runs' files a query should load: just the one given, or
        (unfiltered inbox) the _MAX_RUNS_IN_BROWSE_VIEW most recently
        started runs -- the retention window that keeps the default
        officer inbox from re-reading the entire run history on every
        page load as the number of runs grows.
        """
        if explicit_run_id:
            return [explicit_run_id]
        from services.run_service import get_run_service
        recent_runs, _ = get_run_service().list_runs(page=1, page_size=_MAX_RUNS_IN_BROWSE_VIEW)
        return [r.run_id for r in recent_runs]

    def get_queue(
        self,
        status: Optional[ReviewStatus] = None,
        run_id: Optional[str] = None,
        page: int = 1,
        page_size: int = 20,
        sort_by: str = "score",
        sort_desc: bool = True
    ) -> Tuple[List[ReviewItem], int]:
        """Get review queue with filtering and pagination."""
        for rid in self._browse_run_ids(run_id):
            self._ensure_run_loaded(rid)

        if run_id:
            items = [i for i in self._items.values() if i.run_id == run_id]
        else:
            loaded_recent = set(self._browse_run_ids(None))
            items = [i for i in self._items.values() if i.run_id in loaded_recent]

        # Filter by status
        if status:
            items = [i for i in items if i.status == status]

        # Sort
        if sort_by == "score":
            items.sort(key=lambda x: x.score, reverse=sort_desc)
        elif sort_by == "created_at":
            items.sort(key=lambda x: x.created_at, reverse=sort_desc)

        # Paginate
        total = len(items)
        start = (page - 1) * page_size
        end = start + page_size

        return items[start:end], total

    def approve(
        self,
        review_id: str,
        reviewer: str,
        reason: str
    ) -> ReviewItem:
        """
        Approve a review item (confirm the match).

        Args:
            review_id: Review item ID
            reviewer: User who approved
            reason: Mandatory reason for approval
        """
        item = self._items.get(review_id)
        if not item:
            raise ValueError(f"Review item {review_id} not found")

        if item.status != ReviewStatus.PENDING:
            raise ValueError(f"Review item {review_id} is not pending")

        if not reason or len(reason.strip()) < 5:
            raise ValueError("Reason is required (minimum 5 characters)")

        # Update item
        item.status = ReviewStatus.APPROVED
        item.reviewer = reviewer
        item.review_reason = reason
        item.reviewed_at = datetime.utcnow()

        # Add to cluster
        cluster_manager = get_cluster_manager()
        cluster_manager.link(item.a_key, item.b_key)

        # Log audit event
        log_audit_event(
            AuditEventType.REVIEW_APPROVED,
            {
                'review_id': review_id,
                'pair_id': item.pair_id,
                'reviewer': reviewer,
                'reason': reason,
            },
            actor=reviewer,
            run_id=item.run_id
        )

        self._append_update(item)
        return item

    def reject(
        self,
        review_id: str,
        reviewer: str,
        reason: str
    ) -> ReviewItem:
        """
        Reject a review item (confirm not a match).

        Args:
            review_id: Review item ID
            reviewer: User who rejected
            reason: Mandatory reason for rejection
        """
        item = self._items.get(review_id)
        if not item:
            raise ValueError(f"Review item {review_id} not found")

        if item.status != ReviewStatus.PENDING:
            raise ValueError(f"Review item {review_id} is not pending")

        if not reason or len(reason.strip()) < 5:
            raise ValueError("Reason is required (minimum 5 characters)")

        # Update item
        item.status = ReviewStatus.REJECTED
        item.reviewer = reviewer
        item.review_reason = reason
        item.reviewed_at = datetime.utcnow()

        # Log audit event
        log_audit_event(
            AuditEventType.REVIEW_REJECTED,
            {
                'review_id': review_id,
                'pair_id': item.pair_id,
                'reviewer': reviewer,
                'reason': reason,
            },
            actor=reviewer,
            run_id=item.run_id
        )

        self._append_update(item)
        return item

    def get_stats(self) -> dict:
        """Get review queue statistics, over the same retention window as the unfiltered browse view."""
        for rid in self._browse_run_ids(None):
            self._ensure_run_loaded(rid)
        loaded_recent = set(self._browse_run_ids(None))
        items = [i for i in self._items.values() if i.run_id in loaded_recent]

        pending = sum(1 for i in items if i.status == ReviewStatus.PENDING)
        approved = sum(1 for i in items if i.status == ReviewStatus.APPROVED)
        rejected = sum(1 for i in items if i.status == ReviewStatus.REJECTED)

        # Calculate average time to review
        reviewed = [i for i in items if i.reviewed_at]
        avg_time = 0
        if reviewed:
            times = [(i.reviewed_at - i.created_at).total_seconds() for i in reviewed]
            avg_time = sum(times) / len(times)

        return {
            'pending': pending,
            'approved': approved,
            'rejected': rejected,
            'total': len(items),
            'avg_review_time_seconds': avg_time,
            'with_ai_explanation': sum(1 for i in items if i.has_ai_explanation),
        }


# Singleton instance
_review_service = ReviewService()


def get_review_service() -> ReviewService:
    """Get the global review service instance."""
    return _review_service
