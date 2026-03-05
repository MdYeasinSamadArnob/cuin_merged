'use client';

import { useState, useEffect, useRef } from 'react';
import {
    Database, Play, CheckCircle, AlertCircle, Loader2, ArrowRight,
    Search, Activity, Network, Cpu, Shield, Zap, FileText, Terminal, ArrowLeft
} from 'lucide-react';
import { api } from '@/lib/api';
import { useWebSocket } from '@/lib/ws';
import Link from 'next/link';

// --- Agents Config (Synced with Run Details Page) ---
const AGENTS = {
    ingest: { name: 'Data Ingestion', role: 'System', icon: Database, color: 'text-blue-600 dark:text-blue-400', message: 'Loading customer records...' },
    normalize: { name: 'Data Standardization', role: 'Processing', icon: FileText, color: 'text-cyan-600 dark:text-cyan-400', message: 'Standardizing data formats...' },
    block: { name: 'Record Blocking', role: 'Indexing', icon: Cpu, color: 'text-indigo-600 dark:text-indigo-400', message: 'Grouping similar records...' },
    candidates: { name: 'Match Candidate Generation', role: 'Analysis', icon: Search, color: 'text-yellow-600 dark:text-yellow-400', message: 'Identifying potential matches...' },
    score: { name: 'Similarity Scoring', role: 'Evaluation', icon: Activity, color: 'text-pink-600 dark:text-pink-400', message: 'Calculating match confidence...' },
    decide: { name: 'Decision Engine', role: 'Classification', icon: Shield, color: 'text-emerald-600 dark:text-emerald-400', message: 'Applying matching rules...' },
    cluster: { name: 'Entity Resolution', role: 'Consolidation', icon: Network, color: 'text-purple-600 dark:text-purple-400', message: 'Creating unified customer records...' },
    complete: { name: 'System', role: 'Complete', icon: CheckCircle, color: 'text-green-600 dark:text-green-500', message: 'Processing complete.' }
};

const pipelineStages = ['ingest', 'normalize', 'block', 'candidates', 'score', 'decide', 'cluster', 'complete'];

interface LogEntry {
    time: string;
    agent: string;
    msg: string;
    type: 'info' | 'success' | 'warn';
    stage: string;
}

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

export default function DataSourcePage() {
    const [ingesting, setIngesting] = useState(false);
    const [result, setResult] = useState<any>(null);
    const [error, setError] = useState<string | null>(null);
    const [activeRun, setActiveRun] = useState<any>(null);
    const [visualStage, setVisualStage] = useState<string>('ingest');
    const [agentLogs, setAgentLogs] = useState<LogEntry[]>([]);

    const { lastEvent } = useWebSocket();
    const logsEndRef = useRef<HTMLDivElement>(null);

    // Stats for counting animation
    const recordsInCount = useCounter(activeRun?.counters?.records_in || 0);
    const candidatesCount = useCounter(activeRun?.counters?.candidates_generated || 0);
    const autoLinksCount = useCounter(activeRun?.counters?.auto_links || 0);
    const reviewItemsCount = useCounter(activeRun?.counters?.review_items || 0);

    const activeStageIndex = pipelineStages.indexOf(visualStage);

    // Handle WebSocket messages
    useEffect(() => {
        if (!lastEvent) return;

        try {
            const event = lastEvent;
            const runId = result?.run_id || activeRun?.run_id;
            if (event.run_id && event.run_id !== runId) return;

            if (event.type === 'STAGE_PROGRESS' || event.type === 'STAGE_COMPLETE') {
                const payload = event.data as any;
                setVisualStage(payload.stage);

                const agent = AGENTS[payload.stage as keyof typeof AGENTS] || AGENTS.complete;
                if (payload.status === 'running') {
                    addLog(agent.name, payload.message || agent.message, 'info', payload.stage);
                } else if (payload.status === 'complete') {
                    addLog(agent.name, `${agent.role} task finished.`, 'success', payload.stage);
                }
            }

            if (event.type === 'RUN_COMPLETE' || event.type === 'RUN_FAILED') {
                if (runId) {
                    fetchRunInfo(runId);
                    if (event.type === 'RUN_COMPLETE') {
                        setVisualStage('complete');
                        addLog("System", "Processing completed successfully.", 'success', 'complete');
                    } else {
                        addLog("System", `Run failed: ${event.data?.error || 'Unknown error'}`, 'warn', visualStage);
                    }
                }
            }
        } catch (err) {
            console.error('Failed to parse WebSocket message', err);
        }
    }, [lastEvent, activeRun, result, visualStage]);

    // Scroll logs
    useEffect(() => {
        if (logsEndRef.current) {
            logsEndRef.current.scrollIntoView({ behavior: 'auto', block: 'nearest' });
        }
    }, [agentLogs]);

    const addLog = (agent: string, msg: string, type: 'info' | 'success' | 'warn' = 'info', stage: string = 'ingest') => {
        setAgentLogs(prev => [...prev.slice(-40), {
            time: new Date().toLocaleTimeString([], { hour12: false, hour: '2-digit', minute: '2-digit', second: '2-digit' }),
            agent,
            msg,
            type,
            stage
        }]);
    };

    const fetchRunInfo = async (runId: string) => {
        try {
            const run = await api.getRun(runId);
            setActiveRun(run);

            if (run.status === 'COMPLETED') {
                setVisualStage('complete');
            } else if (run.current_stage) {
                setVisualStage(run.current_stage);
            }
        } catch (err) {
            console.error('Failed to fetch run info', err);
        }
    };

    const handleStartIngestion = async () => {
        setIngesting(true);
        setError(null);
        setResult(null);
        setActiveRun(null);
        setVisualStage('ingest');
        setAgentLogs([]);
        addLog("System", "Initializing pipeline...", "info", 'ingest');

        try {
            const apiUrl = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000';
            const response = await fetch(`${apiUrl}/datasource/ingest`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({ run_mode: 'AUTO' }),
            });

            if (!response.ok) {
                const data = await response.json();
                throw new Error(data.detail || 'Ingestion failed');
            }

            const data = await response.json();
            setResult(data);

            // Initial fetch
            fetchRunInfo(data.run_id);

            // Background polling fallback
            const pollInterval = setInterval(async () => {
                try {
                    const updatedRun = await api.getRun(data.run_id);
                    setActiveRun(updatedRun);

                    if (updatedRun.status === 'COMPLETED' || updatedRun.status === 'FAILED') {
                        clearInterval(pollInterval);
                    }
                } catch (e) {
                    console.error("Polling error", e);
                }
            }, 2000);

        } catch (err: any) {
            setError(err.message);
            addLog("System", `Error starting ingestion: ${err.message}`, "warn", 'ingest');
        } finally {
            setIngesting(false);
        }
    };

    const currentAgent = AGENTS[visualStage as keyof typeof AGENTS] || AGENTS.ingest;
    const AgentIcon = currentAgent.icon;

    return (
        <div className="min-h-screen bg-gray-50 dark:bg-gray-950 py-12 px-4 sm:px-6 lg:px-8 transition-colors duration-300">
            <div className="max-w-6xl mx-auto">
                <div className="flex items-center justify-between mb-8">
                    <div className="flex items-center gap-4">
                        <div className="p-3 bg-blue-100 dark:bg-blue-900/30 rounded-xl">
                            <Database className="w-8 h-8 text-blue-600 dark:text-blue-400" />
                        </div>
                        <div>
                            <h1 className="text-3xl font-bold text-gray-900 dark:text-white">Enterprise Data Source</h1>
                            <p className="text-gray-500 dark:text-gray-400">Connect and ingest data from Oracle Core Banking</p>
                        </div>
                    </div>
                    {activeRun && (
                        <div className="flex items-center gap-2">
                            <div className={`w-3 h-3 rounded-full ${activeRun.status === 'RUNNING' ? 'bg-green-500 animate-ping' : 'bg-gray-500'}`} />
                            <span className="font-mono text-sm uppercase text-gray-600 dark:text-gray-400">{activeRun.status}</span>
                        </div>
                    )}
                </div>

                {!result ? (
                    <div className="grid grid-cols-1 lg:grid-cols-4 gap-8 mb-8">
                        <div className="lg:col-span-1 space-y-6">
                            <div className="bg-white dark:bg-gray-900 p-6 rounded-2xl shadow-sm border border-gray-200 dark:border-gray-800">
                                <h3 className="text-xs font-bold text-gray-400 uppercase tracking-widest mb-4">Source Details</h3>
                                <div className="space-y-4">
                                    <div>
                                        <p className="text-xs text-gray-500 mb-1">Type</p>
                                        <p className="font-semibold text-gray-900 dark:text-white">Oracle Parquet</p>
                                    </div>
                                    <div>
                                        <p className="text-xs text-gray-500 mb-1">Resource</p>
                                        <p className="font-mono text-sm text-gray-900 dark:text-white">oracle_data.parquet</p>
                                    </div>
                                    <div>
                                        <p className="text-xs text-gray-500 mb-1">Status</p>
                                        <div className="flex items-center gap-2">
                                            <span className="w-2 h-2 rounded-full bg-green-500"></span>
                                            <p className="font-semibold text-gray-900 dark:text-white uppercase text-xs">Connected</p>
                                        </div>
                                    </div>
                                </div>
                            </div>

                            <button
                                onClick={handleStartIngestion}
                                disabled={ingesting}
                                className={`w-full flex items-center justify-center gap-3 py-4 rounded-xl text-lg font-bold transition-all transform hover:scale-[1.02] active:scale-95 shadow-lg
                                    ${ingesting
                                        ? 'bg-gray-100 dark:bg-gray-800 text-gray-400 cursor-not-allowed shadow-none'
                                        : 'bg-blue-600 hover:bg-blue-700 text-white shadow-blue-500/25'}`}
                            >
                                {ingesting ? (
                                    <>
                                        <Loader2 className="w-6 h-6 animate-spin" />
                                        Processing...
                                    </>
                                ) : (
                                    <>
                                        <Play className="w-5 h-5 fill-current" />
                                        Start Ingestion
                                    </>
                                )}
                            </button>

                            {error && (
                                <div className="p-4 bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800/50 rounded-xl flex items-start gap-3">
                                    <AlertCircle className="w-5 h-5 text-red-600 dark:text-red-400 mt-0.5" />
                                    <div className="text-xs text-red-700 dark:text-red-300 font-medium leading-relaxed">
                                        {error}
                                    </div>
                                </div>
                            )}
                        </div>

                        <div className="lg:col-span-3">
                            <div className="bg-white dark:bg-gray-900 rounded-3xl p-12 shadow-xl border border-gray-100 dark:border-gray-800 flex flex-col items-center text-center justify-center min-h-[400px]">
                                <div className="w-24 h-24 bg-blue-50 dark:bg-blue-900/20 rounded-full flex items-center justify-center mb-8">
                                    <Database className="w-12 h-12 text-blue-600 dark:text-blue-400" />
                                </div>
                                <h2 className="text-2xl font-bold text-gray-900 dark:text-white mb-4">Ready for Ingestion</h2>
                                <p className="text-gray-500 dark:text-gray-400 max-w-md mx-auto mb-8">
                                    Trigger the high-speed identity resolution pipeline.
                                    This will process approximately 2,000 records from the Oracle staging area
                                    using multi-pass blocking and Splink probabilistic matching.
                                </p>
                                <div className="grid grid-cols-2 md:grid-cols-4 gap-4 w-full max-w-2xl px-4">
                                    {['Cleaning', 'Blocking', 'Scoring', 'Graphing'].map((step, i) => (
                                        <div key={step} className="p-4 bg-gray-50 dark:bg-gray-800 rounded-2xl border border-gray-100 dark:border-gray-700/50">
                                            <p className="text-xs font-bold text-gray-400 mb-1 uppercase tracking-tighter">Phase {i + 1}</p>
                                            <p className="font-semibold text-gray-900 dark:text-white">{step}</p>
                                        </div>
                                    ))}
                                </div>
                            </div>
                        </div>
                    </div>
                ) : (
                    <div className="space-y-8 animate-fadeIn">
                        {/* Pipeline Progress Navigation */}
                        <div className="overflow-x-auto pb-6">
                            <div className="flex items-center justify-between min-w-[800px] relative px-10">
                                {/* Connecting Line */}
                                <div className="absolute top-1/2 left-0 w-full h-1 bg-gray-200 dark:bg-gray-800 -z-0 -translate-y-1/2" />
                                <div
                                    className="absolute top-1/2 left-0 h-1 bg-gradient-to-r from-blue-600 to-emerald-500 -z-0 -translate-y-1/2 transition-all duration-1000"
                                    style={{ width: `${(activeStageIndex / (pipelineStages.length - 1)) * 100}%` }}
                                />

                                {pipelineStages.map((stageKey, idx) => {
                                    const agent = AGENTS[stageKey as keyof typeof AGENTS];
                                    const isPast = idx < activeStageIndex;
                                    const isActive = idx === activeStageIndex;

                                    return (
                                        <div
                                            key={stageKey}
                                            className="relative z-10 flex flex-col items-center gap-2 group"
                                        >
                                            <div className={`w-12 h-12 rounded-full flex items-center justify-center border-2 transition-all duration-500
                                                ${isActive ? `bg-white dark:bg-gray-900 ${agent.color.replace('text-', 'border-')} shadow-[0_0_20px_rgba(59,130,246,0.5)] scale-110` :
                                                    isPast ? 'bg-gray-200 dark:bg-gray-800 border-gray-400 dark:border-gray-600 text-gray-500 dark:text-gray-400' :
                                                        'bg-white dark:bg-gray-950 border-gray-200 dark:border-gray-800 text-gray-400 dark:text-gray-700'}
                                            `}>
                                                <agent.icon size={20} className={isActive ? agent.color : isPast ? 'text-gray-500 dark:text-gray-400' : 'text-gray-400 dark:text-gray-700'} />
                                            </div>
                                            <div className={`text-[10px] font-bold uppercase tracking-wider transition-colors duration-300 ${isActive ? 'text-gray-900 dark:text-white' : 'text-gray-500 dark:text-gray-600'}`}>
                                                {agent.name.split(' ')[0]}
                                            </div>
                                        </div>
                                    );
                                })}
                            </div>
                        </div>

                        <div className="grid grid-cols-1 lg:grid-cols-3 gap-8">
                            {/* Left: Component Spotlight & Logs */}
                            <div className="lg:col-span-2 space-y-6">
                                {/* Spotlight Card */}
                                <div className="relative overflow-hidden bg-white dark:bg-gray-900/50 border border-blue-200 dark:border-blue-500/30 rounded-2xl p-8 shadow-xl">
                                    <div className="absolute top-0 right-0 p-4 opacity-10">
                                        <AgentIcon size={120} className={currentAgent.color} />
                                    </div>

                                    <div className="relative z-10">
                                        <h2 className="text-xs font-bold text-gray-500 dark:text-gray-400 uppercase tracking-widest mb-2">Live Processing Stage</h2>
                                        <div className="flex items-center gap-4 mb-6">
                                            <div className={`p-4 rounded-xl bg-gray-50 dark:bg-gray-800 border-2 border-dashed ${currentAgent.color.replace('text-', 'border-')}`}>
                                                <AgentIcon size={40} className={currentAgent.color} />
                                            </div>
                                            <div>
                                                <h3 className="text-2xl font-bold text-gray-900 dark:text-white">{currentAgent.name}</h3>
                                                <p className={`text-sm ${currentAgent.color} font-medium opacity-90`}>{currentAgent.role}</p>
                                            </div>
                                        </div>

                                        <div className="bg-white/80 dark:bg-black/30 backdrop-blur rounded-lg p-4 border-l-4 border-blue-500">
                                            <p className="text-lg font-mono text-blue-800 dark:text-blue-200 animate-pulse">
                                                {`> ${currentAgent.message}`}
                                            </p>
                                        </div>
                                    </div>

                                    {/* Progress line */}
                                    <div className="absolute bottom-0 left-0 w-full h-1 bg-gray-100 dark:bg-gray-800">
                                        <div className="h-full bg-blue-500 animate-progress-indeterminate" />
                                    </div>
                                </div>

                                {/* System Logs */}
                                <div className="bg-white dark:bg-black border border-gray-200 dark:border-gray-800 rounded-xl p-4 font-mono text-sm h-[300px] flex flex-col shadow-inner relative overflow-hidden">
                                    <div className="flex items-center gap-2 text-gray-500 border-b border-gray-200 dark:border-gray-800 pb-2 mb-2">
                                        <Terminal size={14} />
                                        <span>System.Agent.Log</span>
                                    </div>
                                    <div className="flex-1 overflow-y-auto space-y-2 pr-2 custom-scrollbar">
                                        {agentLogs.length === 0 ? (
                                            <div className="text-gray-500 italic p-4 text-center">Waiting for pipeline events...</div>
                                        ) : (
                                            agentLogs.map((log, i) => (
                                                <div key={i} className={`flex gap-3 animate-in fade-in slide-in-from-left-2 ${log.type === 'warn' ? 'text-red-700 dark:text-red-400' :
                                                    log.type === 'success' ? 'text-green-700 dark:text-green-400' : 'text-blue-800 dark:text-blue-200'
                                                    }`}>
                                                    <span className="text-gray-400 dark:text-gray-600 shrink-0 text-[10px] mt-1">[{log.time}]</span>
                                                    <span className="font-bold shrink-0 w-24 border-r border-gray-200 dark:border-gray-800 mr-2 opacity-70 truncate">{log.agent}</span>
                                                    <span className="break-words">{log.msg}</span>
                                                </div>
                                            ))
                                        )}
                                        <div ref={logsEndRef} />
                                    </div>
                                </div>
                            </div>

                            {/* Right: Real-time Metrics */}
                            <div className="space-y-6">
                                <div className="bg-white dark:bg-gray-900 border border-gray-200 dark:border-gray-800 rounded-xl p-6 shadow-lg">
                                    <div className="flex justify-between items-center mb-6">
                                        <h3 className="text-gray-500 dark:text-gray-400 text-xs uppercase font-bold tracking-widest">Real-time Metrics</h3>
                                        <span className="text-[10px] text-green-600 font-bold animate-pulse">● LIVE SYNC</span>
                                    </div>

                                    <div className="space-y-4">
                                        <MetricCard label="Records Ingested" value={recordsInCount} icon={Database} highlight={activeStageIndex >= 0} />
                                        <MetricCard label="Candidates Found" value={candidatesCount} icon={Search} highlight={activeStageIndex >= 3} />
                                        <MetricCard label="Auto-Linked" value={autoLinksCount} icon={Zap} highlight={activeStageIndex >= 6} />
                                        <MetricCard label="Manual Review" value={reviewItemsCount} icon={FileText} highlight={activeStageIndex >= 5} />
                                    </div>
                                </div>

                                {activeRun?.status === 'COMPLETED' && (
                                    <div className="bg-emerald-50 dark:bg-emerald-900/10 border border-emerald-100 dark:border-emerald-900/30 p-6 rounded-2xl text-center animate-fadeIn">
                                        <CheckCircle className="w-12 h-12 text-emerald-600 dark:text-emerald-400 mx-auto mb-4" />
                                        <h4 className="text-lg font-bold text-gray-900 dark:text-white mb-1">Ingestion Successful</h4>
                                        <p className="text-sm text-gray-600 dark:text-gray-400 mb-6">Pipeline processing finished without errors.</p>
                                        <Link
                                            href={`/runs/${result.run_id}`}
                                            className="flex items-center justify-center gap-2 w-full py-3 bg-gray-900 dark:bg-white text-white dark:text-gray-900 rounded-xl font-bold hover:scale-[1.02] transition-transform"
                                        >
                                            View Mission Report
                                            <ArrowRight size={18} />
                                        </Link>
                                    </div>
                                )}
                            </div>
                        </div>
                    </div>
                )}
            </div>

            <style jsx global>{`
                @keyframes progress-indeterminate {
                    0% { transform: translateX(-100%) scaleX(0.2); }
                    50% { transform: translateX(0%) scaleX(0.5); }
                    100% { transform: translateX(100%) scaleX(0.2); }
                }
                .animate-progress-indeterminate {
                    animation: progress-indeterminate 2s infinite linear;
                }
                @keyframes fadeIn {
                    from { opacity: 0; transform: translateY(10px); }
                    to { opacity: 1; transform: translateY(0); }
                }
                .animate-fadeIn {
                    animation: fadeIn 0.5s ease-out forwards;
                }
            `}</style>
        </div>
    );
}

const MetricCard = ({ label, value, icon: Icon, highlight }: any) => (
    <div className={`flex items-center gap-4 p-4 rounded-xl transform transition-all duration-500 border-l-4
        ${highlight ? 'border-blue-500 bg-blue-50/30 dark:bg-blue-900/10 scale-[1.02]' : 'border-gray-100 dark:border-gray-800 opacity-60 bg-white dark:bg-gray-900'}
    `}>
        <div className={`p-2 rounded-lg ${highlight ? 'bg-blue-100 dark:bg-blue-900/30 text-blue-600 dark:text-blue-400' : 'bg-gray-100 dark:bg-gray-900 text-gray-400'}`}>
            <Icon size={18} />
        </div>
        <div>
            <div className={`text-xl font-bold font-mono ${highlight ? 'text-gray-900 dark:text-white' : 'text-gray-400'}`}>
                {value.toLocaleString()}
            </div>
            <div className="text-[10px] text-gray-500 uppercase font-bold tracking-wider">{label}</div>
        </div>
    </div>
);
