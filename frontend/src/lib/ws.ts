import { useState, useEffect, useRef, useCallback } from 'react';

const API_URL = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000';

// ─── Types ───────────────────────────────────────────────────────────────────

type WebSocketEventPayload = {
    type?: string;
    data?: Record<string, unknown> & { error?: string };
    payload?: Record<string, unknown>;
    run_id?: string;
    [key: string]: unknown;
};

// ─── Polling-based replacement for useWebSocket ───────────────────────────────
//
// Emits synthetic events with the same shape as the old WebSocket events so
// every consumer (dashboard, pipeline, runs/[id]) continues to work unchanged.
//
// Polling cadence:
//   • Idle (no active run)  → poll /runs every 5 s to detect a new RUNNING run
//   • Active run (RUNNING)  → poll /runs/{id} every 2 s for stage/status changes
//   • Terminal (COMPLETED / FAILED) → emit final event, revert to idle polling

export function useWebSocket() {
    const [isConnected, setIsConnected] = useState(false);          // true = API reachable
    const [lastEvent, setLastEvent] = useState<WebSocketEventPayload | null>(null);

    // Mutable refs so interval callbacks always see latest values without re-registering
    const activeRunIdRef = useRef<string | null>(null);
    const lastStageRef   = useRef<string | null>(null);
    const lastStatusRef  = useRef<string | null>(null);
    const timerRef       = useRef<ReturnType<typeof setTimeout> | null>(null);
    const disposedRef    = useRef(false);

    const emit = useCallback((event: WebSocketEventPayload) => {
        setLastEvent(event);
    }, []);

    // ── Core poll against /runs/{id} when a run is RUNNING ──────────────────
    const pollActiveRun = useCallback(async (runId: string) => {
        if (disposedRef.current) return;
        try {
            const res = await fetch(`${API_URL}/runs/${runId}`);
            if (!res.ok) throw new Error('bad response');
            const run = await res.json();
            setIsConnected(true);

            const stage  = run.current_stage as string | null;
            const status = run.status as string;

            // Stage changed → emit STAGE_PROGRESS
            if (stage && stage !== lastStageRef.current) {
                lastStageRef.current = stage;
                emit({
                    type: 'STAGE_PROGRESS',
                    run_id: runId,
                    data: {
                        stage,
                        status: 'running',
                        message: `Processing stage: ${stage}`,
                        records_in:  run.counters?.records_in  ?? 0,
                        records_out: run.counters?.records_normalized ?? 0,
                        duration_ms: 0,
                    },
                });
            }

            // Status changed to terminal
            if (status !== lastStatusRef.current) {
                lastStatusRef.current = status;

                if (status === 'COMPLETED') {
                    emit({ type: 'RUN_COMPLETE', run_id: runId, data: { counters: run.counters } });
                    // Revert to idle after a completed run
                    activeRunIdRef.current = null;
                    lastStageRef.current   = null;
                    lastStatusRef.current  = null;
                    scheduleIdlePoll();
                    return;
                }

                if (status === 'FAILED') {
                    emit({ type: 'RUN_FAILED', run_id: runId, data: { error: 'Run failed' } });
                    activeRunIdRef.current = null;
                    lastStageRef.current   = null;
                    lastStatusRef.current  = null;
                    scheduleIdlePoll();
                    return;
                }
            }

            // Still running → poll again in 2 s
            timerRef.current = setTimeout(() => pollActiveRun(runId), 2000);
        } catch {
            setIsConnected(false);
            // Retry in 3 s on error
            timerRef.current = setTimeout(() => pollActiveRun(runId), 3000);
        }
    }, [emit]);

    // ── Idle poll: check for any new RUNNING run ─────────────────────────────
    const scheduleIdlePoll = useCallback(() => {
        if (disposedRef.current) return;
        timerRef.current = setTimeout(idlePoll, 5000);
    }, []);                                    // idlePoll defined below

    const idlePoll = useCallback(async () => {
        if (disposedRef.current) return;
        try {
            const res = await fetch(`${API_URL}/runs?page=1&page_size=5`);
            if (!res.ok) throw new Error('bad response');
            const data = await res.json();
            setIsConnected(true);

            const runs: any[] = data.runs ?? data ?? [];
            const running = runs.find((r: any) => r.status === 'RUNNING');

            if (running && running.run_id !== activeRunIdRef.current) {
                // Switch to active run polling
                activeRunIdRef.current = running.run_id;
                lastStageRef.current   = null;
                lastStatusRef.current  = 'RUNNING';
                pollActiveRun(running.run_id);
            } else {
                scheduleIdlePoll();
            }
        } catch {
            setIsConnected(false);
            scheduleIdlePoll();
        }
    }, [pollActiveRun, scheduleIdlePoll]);

    // ── Mount / Unmount ──────────────────────────────────────────────────────
    useEffect(() => {
        disposedRef.current = false;

        // Kick off with an immediate idle poll to check for existing running run
        void idlePoll();

        return () => {
            disposedRef.current = true;
            if (timerRef.current) clearTimeout(timerRef.current);
        };
    }, [idlePoll]);

    return { isConnected, lastEvent };
}
