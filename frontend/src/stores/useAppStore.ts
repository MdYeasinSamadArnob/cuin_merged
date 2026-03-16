/**
 * CUIN v2 - Zustand Store
 * 
 * Global application state management.
 */

import { create } from "zustand";
import { persist } from "zustand/middleware";
import { Run, DashboardMetrics } from "@/lib/api";

interface AppState {
    // Current run being viewed
    currentRunId: string | null;
    setCurrentRunId: (runId: string | null) => void;

    // Runs list
    runs: Run[];
    setRuns: (runs: Run[]) => void;
    addRun: (run: Run) => void;
    updateRun: (runId: string, updates: Partial<Run>) => void;

    // Dashboard KPIs
    kpis: DashboardMetrics | null;
    setKpis: (kpis: DashboardMetrics) => void;

    // UI state
    sidebarCollapsed: boolean;
    toggleSidebar: () => void;
    theme: 'dark' | 'light';
    toggleTheme: () => void;

    // Pipeline progress (for real-time updates)
    pipelineProgress: Record<string, PipelineStage>;
    updatePipelineStage: (runId: string, stage: PipelineStage) => void;
    clearPipelineProgress: (runId: string) => void;
}

interface PipelineStage {
    name: string;
    status: "pending" | "running" | "complete" | "error";
    recordsIn: number;
    recordsOut: number;
    reductionPct: number;
    durationMs: number;
    message?: string;
}

export const useAppStore = create<AppState>()(
    persist(
        (set) => ({
            // Current run
            currentRunId: null,
            setCurrentRunId: (runId) => set({ currentRunId: runId }),

            // Runs
            runs: [],
            setRuns: (runs) => set({ runs }),
            addRun: (run) => set((state) => ({ runs: [run, ...state.runs] })),
            updateRun: (runId, updates) =>
                set((state) => ({
                    runs: state.runs.map((r) =>
                        r.run_id === runId ? { ...r, ...updates } : r
                    ),
                })),

            // KPIs
            kpis: null,
            setKpis: (kpis) => set({ kpis }),

            // UI
            sidebarCollapsed: false,
            toggleSidebar: () =>
                set((state) => ({ sidebarCollapsed: !state.sidebarCollapsed })),
            theme: 'dark',
            toggleTheme: () =>
                set((state) => ({ theme: state.theme === 'dark' ? 'light' : 'dark' })),

            // Pipeline progress
            pipelineProgress: {},
            updatePipelineStage: (runId, stage) =>
                set((state) => ({
                    pipelineProgress: {
                        ...state.pipelineProgress,
                        [`${runId}:${stage.name}`]: stage,
                    },
                })),
            clearPipelineProgress: (runId) =>
                set((state) => {
                    const newProgress = { ...state.pipelineProgress };
                    Object.keys(newProgress).forEach((key) => {
                        if (key.startsWith(`${runId}:`)) {
                            delete newProgress[key];
                        }
                    });
                    return { pipelineProgress: newProgress };
                }),
        }),
        {
            name: 'cuin-storage',
            partialize: (state) => ({
                theme: state.theme,
                sidebarCollapsed: state.sidebarCollapsed
            }),
        }
    )
);
