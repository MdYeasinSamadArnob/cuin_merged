"""
CUIN v2 - Distributed scoring worker (Stage 4 proof-of-concept)

Standalone entry point for one scoring worker in a fixed N-way
partition of a run's candidate_pairs (see DorisPipelineOrchestrator.
_score_partition's docstring for the partitioning scheme and why it
needs no coordination between workers). Invoked as:

    python -m pipeline.distributed_scoring_worker <run_id> <worker_idx> <num_workers>

Each worker constructs its own DorisPipelineOrchestrator pointed at
the SAME run_id (reusing _connect()'s cuin_run_<run_id> database
convention -- every worker and the coordinator resolve to the
identical Doris database with zero extra wiring), scores its
partition, persists its own audited pairs to Postgres, and pushes its
result to Redis for the coordinator to collect. Proves the
distribution MECHANISM is correct (no shared in-memory state between
workers -- everything coordinates through Doris/Postgres/Redis, the
same three systems real separate machines would use) even though on
this single dev host it cannot demonstrate genuine multi-machine
wall-clock speedup (see the scaling plan's Stage 1 note on the same
limitation for Doris BE scale-out).
"""

import json
import sys

REDIS_URL = "redis://localhost:6381/0"


def main():
    if len(sys.argv) != 4:
        print("usage: python -m pipeline.distributed_scoring_worker <run_id> <worker_idx> <num_workers>", file=sys.stderr)
        sys.exit(2)

    run_id, worker_idx, num_workers = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])

    from pipeline.doris_orchestrator import DorisPipelineOrchestrator
    orchestrator = DorisPipelineOrchestrator(run_id=run_id)
    orchestrator._mode = "FULL"

    result = orchestrator._score_partition(worker_idx, num_workers)

    import redis
    r = redis.from_url(REDIS_URL)
    r.rpush(f"cuin:dist:{run_id}:results", json.dumps(result))
    print(f"worker {worker_idx}/{num_workers}: {len(result['auto_links'])} auto_links, "
          f"{result['review_count']} review, {result['rejected_count']} rejected")


if __name__ == "__main__":
    main()
