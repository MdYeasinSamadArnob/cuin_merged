'use client';

import { useState, useEffect, useCallback, useRef } from 'react';
import { api } from '../../lib/api';
import { useWebSocket } from '../../lib/ws';
import Link from "next/link";
import { FileSpreadsheet, ArrowRight } from "lucide-react";
// Hook for counting animation
const useCounter = (end: number, duration: number = 1000) => {
    const [count, setCount] = useState(0);
    const countRef = useRef(0);
    const startTimeRef = useRef<number | null>(null);

    useEffect(() => {
        countRef.current = end;
        startTimeRef.current = null;

        const animate = (timestamp: number) => {
            if (!startTimeRef.current) startTimeRef.current = timestamp;
            const progress = timestamp - startTimeRef.current;
            const percentage = Math.min(progress / duration, 1);

            // Ease out quart
            const ease = 1 - Math.pow(1 - percentage, 4);

            setCount(Math.floor(countRef.current * ease));

            if (percentage < 1) {
                requestAnimationFrame(animate);
            }
        };

        requestAnimationFrame(animate);
    }, [end, duration]);

    return count;
};

interface StageInfo {
    id: string;
    name: string;
    icon: string;
    status: 'pending' | 'running' | 'complete' | 'error';
    recordsIn: number;
    recordsOut: number;
    reductionPct: number;
    durationMs: number;
    message: string;
}

interface RunInfo {
    run_id: string;
    mode: string;
    status: string;
    description: string;
    counters: {
        records_in: number;
        records_normalized: number;
        blocks_created: number;
        candidates_generated: number;
        pairs_scored: number;
        auto_links: number;
        review_items: number;
        rejected: number;
    };
    started_at: string;
    ended_at: string | null;
    duration_seconds: number | null;
    current_stage?: string | null;
}

const DEFAULT_STAGES: StageInfo[] = [
    { id: 'ingest', name: 'Ingest', icon: '📥', status: 'pending', recordsIn: 0, recordsOut: 0, reductionPct: 0, durationMs: 0, message: '' },
    { id: 'normalize', name: 'Normalize', icon: '🔧', status: 'pending', recordsIn: 0, recordsOut: 0, reductionPct: 0, durationMs: 0, message: '' },
    { id: 'block', name: 'Block', icon: '📦', status: 'pending', recordsIn: 0, recordsOut: 0, reductionPct: 0, durationMs: 0, message: '' },
    { id: 'candidates', name: 'Candidates', icon: '🔗', status: 'pending', recordsIn: 0, recordsOut: 0, reductionPct: 0, durationMs: 0, message: '' },
    { id: 'score', name: 'Score', icon: '📊', status: 'pending', recordsIn: 0, recordsOut: 0, reductionPct: 0, durationMs: 0, message: '' },
    { id: 'decide', name: 'Decide', icon: '✅', status: 'pending', recordsIn: 0, recordsOut: 0, reductionPct: 0, durationMs: 0, message: '' },
    { id: 'cluster', name: 'Cluster', icon: '🕸️', status: 'pending', recordsIn: 0, recordsOut: 0, reductionPct: 0, durationMs: 0, message: '' },
];

export default function PipelinePage() {
    const [stages, setStages] = useState<StageInfo[]>(DEFAULT_STAGES);
    const [activeRun, setActiveRun] = useState<RunInfo | null>(null);
    const [isLoading, setIsLoading] = useState(false);
    const [error, setError] = useState<string | null>(null);
    const { lastEvent } = useWebSocket();

    // Stats for counting animation
    const recordsIn = useCounter(activeRun?.counters?.records_in || 0);
    const candidates = useCounter(activeRun?.counters?.candidates_generated || 0);
    const autoLinks = useCounter(activeRun?.counters?.auto_links || 0);

    // Handle WebSocket messages
    useEffect(() => {
        if (!lastEvent) return;

        try {
            const event = lastEvent;
            if (event.type === 'STAGE_PROGRESS' || event.type === 'STAGE_COMPLETE') {
                const payload = event.data as any;
                setStages(prevStages =>
                    prevStages.map(stage =>
                        stage.id === payload.stage
                            ? {
                                ...stage,
                                status: payload.status as StageInfo['status'],
                                recordsIn: payload.records_in || stage.recordsIn,
                                recordsOut: payload.records_out || stage.recordsOut,
                                reductionPct: payload.reduction_pct || stage.reductionPct,
                                durationMs: payload.duration_ms || stage.durationMs,
                                message: payload.message || stage.message,
                            }
                            : stage
                    )
                );
            }

            if (event.type === 'RUN_COMPLETE' || event.type === 'RUN_FAILED') {
                // Refresh run info
                if (activeRun) {
                    fetchRunInfo(activeRun.run_id);
                }
            }
        } catch {
            console.error('Failed to parse WebSocket message');
        }
    }, [lastEvent, activeRun]);

    const fetchRunInfo = async (runId: string) => {
        try {
            const run = await api.getRun(runId);
            setActiveRun(run);

            // Update stages based on counters
            if (run.status === 'COMPLETED') {
                setStages(prevStages =>
                    prevStages.map(stage => ({
                        ...stage,
                        status: 'complete' as const,
                        message: getStageMessage(stage.id, run.counters),
                    }))
                );
            }
        } catch {
            console.error('Failed to fetch run info');
        }
    };

    const getStageMessage = (stageId: string, counters: RunInfo['counters']) => {
        switch (stageId) {
            case 'ingest':
                return `Loaded ${counters.records_in} records`;
            case 'normalize':
                return `Normalized ${counters.records_normalized} records`;
            case 'block':
                return `Created ${counters.blocks_created} blocking keys`;
            case 'candidates':
                return `Generated ${counters.candidates_generated} candidate pairs`;
            case 'score':
                return `Scored ${counters.pairs_scored} pairs`;
            case 'decide':
                return `Auto: ${counters.auto_links}, Review: ${counters.review_items}, Reject: ${counters.rejected}`;
            case 'cluster':
                return 'Clusters updated';
            default:
                return '';
        }
    };

    const startNewRun = async () => {
        setIsLoading(true);
        setError(null);
        setStages(DEFAULT_STAGES);

        try {
            const run = await api.startRun('FULL', 'Manual run from UI');
            setActiveRun(run);

            // Poll for completion
            const pollInterval = setInterval(async () => {
                const updatedRun = await api.getRun(run.run_id);
                setActiveRun(updatedRun);

                if (updatedRun.status === 'COMPLETED' || updatedRun.status === 'FAILED') {
                    clearInterval(pollInterval);
                    setIsLoading(false);
                    fetchRunInfo(run.run_id);
                }
            }, 500);
        } catch (err) {
            setError('Failed to start pipeline run');
            setIsLoading(false);
        }
    };

    const getStageStatusColor = (status: StageInfo['status']) => {
        switch (status) {
            case 'pending':
                return 'bg-gray-800 border-gray-700 opacity-60';
            case 'running':
                return 'bg-gradient-to-br from-blue-900/80 to-indigo-900/80 border-blue-400 shadow-[0_0_15px_rgba(59,130,246,0.5)] animate-pulse scale-105';
            case 'complete':
                return 'bg-gradient-to-br from-emerald-900/80 to-green-900/80 border-emerald-500 shadow-[0_0_10px_rgba(16,185,129,0.3)]';
            case 'error':
                return 'bg-red-900/80 border-red-500 shadow-[0_0_10px_rgba(239,68,68,0.3)]';
        }
    };

    const getStageStatusIcon = (status: StageInfo['status']) => {
        switch (status) {
            case 'pending':
                return '⏳';
            case 'running':
                return '⚡';
            case 'complete':
                return '✅';
            case 'error':
                return '❌';
        }
    };

    // Calculate progress percentage
    const completedStages = stages.filter(s => s.status === 'complete').length;
    const progressPct = (completedStages / stages.length) * 100;

    return (
        <div className="p-8 space-y-8 min-h-screen bg-gray-50 dark:bg-gray-950/50 transition-colors duration-300">
            {/* Header */}
            <div className="flex items-center justify-between">
                <div>
                    <h1 className="text-4xl font-bold text-transparent bg-clip-text bg-gradient-to-r from-blue-600 to-indigo-600 dark:from-blue-400 dark:to-indigo-400">
                        Pipeline Visualizer
                    </h1>
                    <p className="text-gray-600 dark:text-gray-400 mt-2 text-lg">
                        Real-time entity resolution & matching engine
                    </p>
                </div>
                <button
                    onClick={startNewRun}
                    disabled={isLoading}
                    className="group relative overflow-hidden bg-gradient-to-r from-blue-600 to-indigo-600 hover:from-blue-500 hover:to-indigo-500 disabled:opacity-50 text-white px-8 py-4 rounded-xl font-bold transition-all duration-200 shadow-lg hover:shadow-blue-500/25 disabled:shadow-none"
                >
                    <div className="absolute inset-0 bg-white/20 group-hover:translate-x-full transition-transform duration-500 skew-x-12 -translate-x-full" />
                    <div className="flex items-center gap-3 relative z-10">
                        {isLoading ? (
                            <>
                                <span className="animate-spin text-xl">⚙️</span>
                                <span>Processing...</span>
                            </>
                        ) : (
                            <>
                                <span className="text-xl">🚀</span>
                                <span>Launch New Run</span>
                            </>
                        )}
                    </div>
                </button>
            </div>

            {/* Global Progress Bar */}
            <div className="relative h-2 bg-gray-200 dark:bg-gray-800 rounded-full overflow-hidden">
                <div
                    className="absolute top-0 left-0 h-full bg-gradient-to-r from-blue-500 via-indigo-500 to-purple-500 transition-all duration-500 ease-out"
                    style={{ width: `${progressPct}%` }}
                />
            </div>

            {error && (
                <div className="bg-red-900/20 border border-red-500/50 rounded-xl p-4 text-red-400 flex items-center gap-3 animate-in fade-in slide-in-from-top-4">
                    <span className="text-2xl">⚠️</span>
                    {error}
                </div>
            )}

            {/* Run Info Card */}
            {activeRun && (
                <div className="bg-white/50 dark:bg-gray-900/50 border border-gray-200 dark:border-gray-800 rounded-2xl p-6 shadow-xl dark:shadow-none backdrop-blur-sm">
                    <div className="flex items-center justify-between">
                        <div>
                            <div className="flex items-center gap-3 mb-2">
                                <h2 className="text-lg font-semibold text-gray-900 dark:text-white">
                                    Run ID: <span className="font-mono text-blue-600 dark:text-blue-400">{activeRun.run_id.slice(0, 8)}</span>
                                </h2>
                                <span className={`px-3 py-1 rounded-full text-xs font-bold tracking-wider ${activeRun.status === 'COMPLETED' ? 'bg-emerald-100 dark:bg-emerald-500/10 text-emerald-600 dark:text-emerald-400 border border-emerald-200 dark:border-emerald-500/20' :
                                    activeRun.status === 'RUNNING' ? 'bg-blue-100 dark:bg-blue-500/10 text-blue-600 dark:text-blue-400 border border-blue-200 dark:border-blue-500/20 animate-pulse' :
                                        'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400'
                                    }`}>
                                    {activeRun.status}
                                </span>
                            </div>
                            <p className="text-gray-500 dark:text-gray-400 text-sm">
                                {activeRun.description || 'No description provided'}
                            </p>
                        </div>
                        {activeRun.duration_seconds && (
                            <div className="text-right">
                                <p className="text-xs text-gray-500 mb-1">Duration</p>
                                <span className="text-2xl font-mono text-gray-900 dark:text-white">
                                    {(activeRun.duration_seconds).toFixed(2)}<span className="text-sm text-gray-500">s</span>
                                </span>
                            </div>
                        )}
                    </div>
                </div>
            )}

            {/* Pipeline Stages */}
            <div className="space-y-4">
                <h2 className="text-xl font-semibold text-white flex items-center gap-2">
                    <span className="text-blue-500">⚡</span> Live Pipeline
                </h2>
                <div className="flex flex-wrap gap-4 items-center justify-center p-8 bg-gray-900/30 rounded-3xl border border-gray-800 border-dashed">
                    {stages.map((stage, index) => (
                        <div key={stage.id} className="flex items-center gap-4 group">
                            <div
                                className={`relative p-6 rounded-2xl border-2 transition-all duration-300 min-w-[200px] hover:scale-105 cursor-default ${getStageStatusColor(stage.status)}`}
                            >
                                <div className="absolute -top-3 -right-3 w-8 h-8 rounded-full bg-gray-900 border-2 border-current flex items-center justify-center text-sm z-10 shadow-lg">
                                    {getStageStatusIcon(stage.status)}
                                </div>

                                <div className="flex flex-col items-center text-center gap-3">
                                    <span className="text-4xl filter drop-shadow-lg">{stage.icon}</span>
                                    <div>
                                        <div className="font-bold text-white text-lg tracking-wide">{stage.name}</div>
                                        {stage.message && (
                                            <p className="text-xs text-blue-200 mt-2 font-medium bg-black/20 px-2 py-1 rounded-full">{stage.message}</p>
                                        )}
                                        {stage.durationMs > 0 && (
                                            <p className="text-xs text-gray-400 mt-1 font-mono">{stage.durationMs}ms</p>
                                        )}
                                    </div>
                                </div>
                            </div>
                            {index < stages.length - 1 && (
                                <div className="text-gray-700 text-2xl group-hover:text-blue-500 transition-colors">➜</div>
                            )}
                        </div>
                    ))}
                </div>
            </div>

            {/* Results Summary */}
            {activeRun && (activeRun.status === 'COMPLETED' || activeRun.status === 'RUNNING') && (
                <div className="bg-gray-900/50 border border-gray-800 rounded-2xl p-8 shadow-xl">
                    <h2 className="text-xl font-semibold text-white mb-6 flex items-center gap-2">
                        <span className="text-purple-500">📊</span> Metrics & Funnel
                    </h2>

                    <div className="grid grid-cols-2 md:grid-cols-4 gap-6 mb-12">
                        {[
                            { label: 'Processed Records', value: recordsIn, color: 'text-white' },
                            { label: 'Candidate Pairs', value: candidates, color: 'text-cyan-400' },
                            { label: 'Auto Linked', value: autoLinks, color: 'text-emerald-400' },
                            { label: 'Review Queue', value: activeRun.counters.review_items, color: 'text-yellow-400' }
                        ].map((metric, i) => (
                            <div key={i} className="bg-black/30 rounded-xl p-5 border border-white/5 backdrop-blur-sm">
                                <p className="text-gray-400 text-sm font-medium mb-1">{metric.label}</p>
                                <p className={`text-4xl font-bold font-mono ${metric.color}`}>
                                    {metric.value.toLocaleString()}
                                </p>
                            </div>
                        ))}
                    </div>

                    {/* Funnel visualization */}
                    <div className="relative pt-8 pb-4 border-t border-gray-800">
                        <div className="flex items-center justify-between text-center relative z-10">
                            {[
                                { val: recordsIn, label: 'Input' },
                                { val: candidates, label: 'Candidates' },
                                { val: activeRun.counters.pairs_scored, label: 'Scored' },
                                { val: autoLinks, label: 'Match' }
                            ].map((step, i, arr) => (
                                <div key={i} className="flex-1 flex flex-col items-center">
                                    <div className={`w-16 h-16 rounded-full flex items-center justify-center mb-3 bg-gray-800 border-4 border-gray-700 shadow-lg ${step.val > 0 ? 'border-blue-500/50 shadow-blue-500/20' : ''
                                        }`}>
                                        <span className="text-xl">
                                            {i === 0 ? '📥' : i === 1 ? '🔗' : i === 2 ? '⚖️' : '🤝'}
                                        </span>
                                    </div>
                                    <div className="text-2xl font-bold text-white mb-1">
                                        {step.val.toLocaleString()}
                                    </div>
                                    <div className="text-xs font-bold text-gray-500 uppercase tracking-widest">{step.label}</div>
                                </div>
                            ))}
                        </div>

                        {/* Connection Line */}
                        <div className="absolute top-[3.5rem] left-0 right-0 h-1 bg-gray-800 -z-0">
                            <div
                                className="h-full bg-blue-500/30 transition-all duration-1000"
                                style={{ width: `${(Math.min(activeRun.counters.pairs_scored / (recordsIn || 1), 1)) * 100}%` }}
                            />
                        </div>
                    </div>
                </div>
            )}
        </div>
    );
}
