"use client";

import { useEffect, useState, useRef } from "react";
import { useParams, useRouter } from "next/navigation";
import { api } from "@/lib/api";
import { useWebSocket } from "@/lib/ws";
import Link from "next/link";
import {
    ArrowLeft, Terminal, Activity, Search, Network,
    Cpu, Database, Shield, Zap, FileText, CheckCircle, Share2, ArrowRight, Users
} from "lucide-react";
import { ClusterGraph } from '@/components/explorer/ClusterGraph';

// --- Agents Config ---
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

interface LogEntry {
    time: string;
    agent: string;
    msg: string;
    type: 'info' | 'success' | 'warn';
    stage: string;
}

export default function RunDetailsPage() {
    const params = useParams();
    const router = useRouter();
    const runId = params.id as string;
    const { lastEvent } = useWebSocket();

    const [run, setRun] = useState<any>(null);
    const [activeStage, setActiveStage] = useState<string>('ingest');
    const [visualStage, setVisualStage] = useState<string>('ingest'); // For cinematic playback
    const [isReplaying, setIsReplaying] = useState(false);
    const [agentLogs, setAgentLogs] = useState<LogEntry[]>([]);
    const [selectedStep, setSelectedStep] = useState<string | null>(null); // For filtering logs
    const [liveMatches, setLiveMatches] = useState<any[]>([]);
    const [graphData, setGraphData] = useState<any>(null);
    const [liveMessage, setLiveMessage] = useState<string>('');  // latest WS message for current stage
    const [clustersCreated, setClustersCreated] = useState(0);  // unique identity clusters after cluster stage
    const [scoreEta, setScoreEta] = useState<string | null>(null);  // ETA for score stage
    const [tick, setTick] = useState(0);  // 1s heartbeat for live elapsed display
    const lastLoggedStageRef = useRef<Record<string, string>>({});  // stage → last logged message (dedup)
    const stageStartRef = useRef<Record<string, number>>({});  // unix ms when each stage entered running
    const stageDurRef = useRef<Record<string, number>>({});    // actual duration_ms for completed stages
    const logsEndRef = useRef<HTMLDivElement>(null);

    // Ordered stages for the pipeline view
    const pipelineStages = ['ingest', 'normalize', 'block', 'candidates', 'score', 'decide', 'cluster', 'complete'];
    const activeStageIndex = pipelineStages.indexOf(visualStage);
    // Initial Fetch & Cinematic Start
    useEffect(() => {
        if (runId) fetchRunDetails();
    }, [runId]);

    // Polling fallback — keeps UI in sync even when WS events are missed
    // (e.g. user navigated mid-run, tab backgrounded, WS reconnect gap)
    const lastPolledStageRef = useRef<string>('');
    useEffect(() => {
        if (!runId) return;
        const interval = setInterval(async () => {
            if (isReplaying) return;
            try {
                const data = await api.getRun(runId);
                if (!data) return;

                // Sync run object (counters + status)
                setRun((prev: any) => {
                    if (!prev) return data;
                    return {
                        ...prev,
                        status:           data.status,
                        counters:         data.counters,
                        current_stage:    data.current_stage ?? prev.current_stage,
                        ended_at:         data.ended_at,
                        duration_seconds: data.duration_seconds,
                    };
                });

                // Sync visual stage and log a transition entry when stage changes
                if (data.current_stage && data.current_stage !== lastPolledStageRef.current) {
                    const prevStage = lastPolledStageRef.current;
                    lastPolledStageRef.current = data.current_stage;
                    setActiveStage(data.current_stage);
                    setVisualStage(data.current_stage);

                    // Log completion of previous stage
                    if (prevStage) {
                        const prevAgent = AGENTS[prevStage as keyof typeof AGENTS];
                        if (prevAgent && lastLoggedStageRef.current[prevStage] !== 'complete') {
                            lastLoggedStageRef.current[prevStage] = 'complete';
                            addLog(prevAgent.name, `${prevAgent.role} stage completed`, 'success', prevStage);
                        }
                    }
                    // Log start of new stage
                    const nextAgent = AGENTS[data.current_stage as keyof typeof AGENTS];
                    if (nextAgent && lastLoggedStageRef.current[data.current_stage] !== 'running'
                                  && lastLoggedStageRef.current[data.current_stage] !== 'complete') {
                        lastLoggedStageRef.current[data.current_stage] = 'running';
                        addLog(nextAgent.name, `${nextAgent.role} stage started`, 'info', data.current_stage);
                    }
                }

                // Stop polling once terminal state reached
                if (data.status === 'COMPLETED' || data.status === 'FAILED' || data.status === 'CANCELLED') {
                    clearInterval(interval);
                    if (data.status === 'COMPLETED') {
                        setActiveStage('complete');
                        setVisualStage('complete');
                        if (lastLoggedStageRef.current['complete'] !== 'done') {
                            lastLoggedStageRef.current['complete'] = 'done';
                            addLog('System', 'Pipeline completed successfully.', 'success', 'complete');
                        }
                    }
                }
            } catch { /* silent — WS is primary */ }
        }, 3000);
        return () => clearInterval(interval);
    }, [runId, isReplaying]);

    // Playback Effect
    useEffect(() => {
        if (isReplaying && run) {
            let currentIndex = 0;
            const interval = setInterval(() => {
                if (currentIndex >= pipelineStages.length) {
                    setIsReplaying(false);
                    clearInterval(interval);
                    return;
                }
                const stage = pipelineStages[currentIndex];
                setVisualStage(stage);
                setSelectedStep(stage); // Auto-select the active step during replay

                // Add fake log for the replay experience
                const agent = AGENTS[stage as keyof typeof AGENTS];
                if (agent) {
                    addLog(agent.name, agent.message, 'info', stage);
                }

                currentIndex++;
            }, 1500); // Slower: 1.5s per stage
            return () => clearInterval(interval);
        }
    }, [isReplaying, run]);

    // WebSocket Handling (Live updates)
    useEffect(() => {
        if (!lastEvent || !runId || isReplaying) return; // Ignore live updates during replay

        const event = lastEvent;
        const payload = event.data as any;
        const eventRunId = event.run_id || payload?.run_id;

        if (eventRunId && eventRunId !== runId) return;

        if (event.type === 'STAGE_PROGRESS') {
            const payload = event.data as any;
            setActiveStage(payload.stage);
            setVisualStage(payload.stage);
            setSelectedStep(payload.stage);
            if (payload.message) setLiveMessage(payload.message);

            const agent = AGENTS[payload.stage as keyof typeof AGENTS] || AGENTS.complete;
            const fmt = (n: number) => n > 0 ? n.toLocaleString() : null;

            if (payload.status === 'running') {
                const d = payload.data || {};

                // Mark stage start time the first time it enters running state
                if (!stageStartRef.current[payload.stage]) {
                    stageStartRef.current[payload.stage] = Date.now();
                }
                // Compute ETA for the score stage from progress_pct + elapsed_sec
                if (payload.stage === 'score' && d.progress_pct > 0 && d.elapsed_sec > 0) {
                    const remainSec = Math.round(d.elapsed_sec * (100 - d.progress_pct) / d.progress_pct);
                    setScoreEta(remainSec > 60 ? `~${Math.ceil(remainSec / 60)}m` : `~${remainSec}s`);
                }

                if (payload.stage === 'score') {
                    // Always log every score heartbeat — it's the longest stage
                    // and management needs to see it's making progress
                    if (d.em_iteration > 0) {
                        const emLine = `EM iteration ${d.em_iteration}/${d.em_max} — ${d.sub_step || payload.message} [${d.progress_pct ?? 0}%]`;
                        addLog(agent.name, emLine, 'info', payload.stage);
                    } else {
                        // Sub-stage change or initial
                        const prev = lastLoggedStageRef.current[payload.stage + ':sub'];
                        const label = d.sub_step || payload.message;
                        if (label && label !== prev) {
                            lastLoggedStageRef.current[payload.stage + ':sub'] = label;
                            addLog(agent.name, label, 'info', payload.stage);
                        }
                    }
                } else {
                    // For all other stages, log first time only
                    const prev = lastLoggedStageRef.current[payload.stage];
                    if (prev !== 'running' && prev !== 'complete') {
                        lastLoggedStageRef.current[payload.stage] = 'running';
                        addLog(agent.name, payload.message || agent.message, 'info', payload.stage);
                    }
                }
            } else if (payload.status === 'complete') {
                lastLoggedStageRef.current[payload.stage] = 'complete';
                // Track actual stage duration for timeline display
                if (payload.duration_ms > 0) stageDurRef.current[payload.stage] = payload.duration_ms;
                // Capture cluster stats for "Unique Identities" metric card
                if (payload.stage === 'cluster') {
                    const cs = payload.data?.cluster_stats;
                    if (cs?.clusters_created > 0) setClustersCreated(cs.clusters_created);
                }
                // Build a rich completion line with record counts and timing
                const parts: string[] = [payload.message || `${agent.role} complete`];
                if (fmt(payload.records_in))  parts.push(`in: ${fmt(payload.records_in)}`);
                if (fmt(payload.records_out)) parts.push(`out: ${fmt(payload.records_out)}`);
                if (payload.reduction_pct > 0) parts.push(`reduced ${payload.reduction_pct.toFixed(1)}%`);
                if (payload.duration_ms > 0)   parts.push(`${(payload.duration_ms / 1000).toFixed(2)}s`);
                addLog(agent.name, parts.join('  |  '), 'success', payload.stage);
            }

            // Update run counters for real-time metrics
            setRun((prev: any) => {
                if (!prev) return prev;
                const newCounters = { ...prev.counters };

                if (payload.stage === 'ingest') newCounters.records_in = payload.records_in;
                if (payload.stage === 'candidates') newCounters.candidates_generated = payload.records_out; // Or appropriate field
                if (payload.stage === 'score') newCounters.auto_links = payload.records_out;

                return { ...prev, counters: newCounters, current_stage: payload.stage };
            });

            // Handle live data payloads
            if (payload.data?.sample_matches) {
                setLiveMatches(payload.data.sample_matches);

                // Construct live graph data from matches
                const nodes: any[] = [];
                const edges: any[] = [];
                const nodeIds = new Set();

                payload.data.sample_matches.forEach((m: any) => {
                    if (!nodeIds.has(m.id1)) {
                        nodes.push({ id: m.id1, label: `ID: ${m.id1}`, type: 'record', properties: {} });
                        nodeIds.add(m.id1);
                    }
                    if (!nodeIds.has(m.id2)) {
                        nodes.push({ id: m.id2, label: `ID: ${m.id2}`, type: 'record', properties: {} });
                        nodeIds.add(m.id2);
                    }
                    edges.push({ source: m.id1, target: m.id2, type: 'MATCHES' });
                });
                setGraphData({ nodes, edges });
            }
        }
        else if (event.type === 'RUN_COMPLETE') {
            setActiveStage('complete');
            setVisualStage('complete');
            addLog("System", "Processing completed successfully.", 'success', 'complete');
            fetchRunDetails();
        }
        else if (event.type === 'RUN_FAILED') {
            const payload = event.data as any;
            addLog("System", `Run failed: ${payload?.error || 'Unknown error'}`, 'warn', activeStage);
        }
    }, [lastEvent, runId, isReplaying]);

    // 1-second tick so elapsed-time displays in stage nodes update in real time
    useEffect(() => {
        const t = setInterval(() => setTick(n => n + 1), 1000);
        return () => clearInterval(t);
    }, []);

    // Cleanup logs scrolling - use auto scroll instead of smooth to prevent jumping
    useEffect(() => {
        if (logsEndRef.current) {
            logsEndRef.current.scrollIntoView({ behavior: 'auto', block: 'nearest' });
        }
    }, [agentLogs]);

    const fetchRunDetails = async () => {
        try {
            const data = await api.getRun(runId);
            setRun(data);
            // Restore cluster count from persisted counters (survives page reload)
            if (data.counters?.clusters_created > 0) {
                setClustersCreated(data.counters.clusters_created);
            }

            if (data.status === 'COMPLETED' && !isReplaying) {
                setIsReplaying(true);
            } else if (data.current_stage) {
                setActiveStage(data.current_stage);
                setVisualStage(data.current_stage);
            }

            // On first load: generate catch-up log entries for every stage
            // that has already started/completed so the log isn't empty mid-run.
            if (agentLogs.length === 0 && data.status !== 'PENDING') {
                const currentIdx = pipelineStages.indexOf(data.current_stage || 'ingest');
                pipelineStages.forEach((stageKey, idx) => {
                    const ag = AGENTS[stageKey as keyof typeof AGENTS];
                    if (!ag) return;
                    if (idx < currentIdx) {
                        // Already completed stages
                        lastLoggedStageRef.current[stageKey] = 'complete';
                        addLog(ag.name, `${ag.role} stage completed`, 'success', stageKey);
                    } else if (idx === currentIdx && data.status === 'RUNNING') {
                        // Current running stage
                        lastLoggedStageRef.current[stageKey] = 'running';
                        addLog(ag.name, `${ag.role} stage in progress...`, 'info', stageKey);
                    }
                });
                if (data.status === 'COMPLETED') {
                    addLog('System', 'Pipeline completed successfully.', 'success', 'complete');
                } else if (data.status === 'FAILED') {
                    addLog('System', `Pipeline failed: ${data.error_message || 'Unknown error'}`, 'warn', data.current_stage || 'ingest');
                }
            }
        } catch (err) {
            console.error('Failed to load run', err);
        }
    };

    const addLog = (agent: string, msg: string, type: 'info' | 'success' | 'warn' = 'info', stage: string = 'ingest') => {
        setAgentLogs(prev => [...prev.slice(-100), {
            time: new Date().toLocaleTimeString([], { hour12: false, hour: '2-digit', minute: '2-digit', second: '2-digit' }),
            agent,
            msg,
            type,
            stage
        }]);
    };

    // Stage timing helper — returns formatted duration (completed) or elapsed/ETA (active)
    const getStageTime = (stageKey: string, isActive: boolean): string | null => {
        void tick; // bind to 1s heartbeat so elapsed display updates each second
        const dur = stageDurRef.current[stageKey];
        if (dur != null) {
            if (dur < 60000) return `${(dur / 1000).toFixed(1)}s`;
            return `${Math.floor(dur / 60000)}m${Math.round((dur % 60000) / 1000)}s`;
        }
        if (isActive && stageStartRef.current[stageKey]) {
            if (stageKey === 'score' && scoreEta) return `ETA ${scoreEta}`;
            const elapsed = Math.round((Date.now() - stageStartRef.current[stageKey]) / 1000);
            if (elapsed < 60) return `${elapsed}s`;
            return `${Math.floor(elapsed / 60)}m${elapsed % 60}s`;
        }
        return null;
    };

    // calculate simulated metrics based on progress if replaying; else usage real metrics
    const getDisplayMetrics = () => {
        const clust = Math.max(run?.counters?.clusters_created || 0, clustersCreated);
        if (!run) return { records_in: 0, candidates_generated: 0, auto_links: 0, review_items: 0, clusters_created: 0 };
        if (!isReplaying) return { ...run.counters, clusters_created: clust };

        // Gradual Reveal Math
        const progress = activeStageIndex / (pipelineStages.length - 1);
        return {
            records_in: activeStageIndex >= 0 ? Math.floor(run.counters.records_in * Math.min(progress * 2, 1)) : 0,
            candidates_generated: activeStageIndex >= 3 ? Math.floor(run.counters.candidates_generated * Math.min((progress - 0.3) * 2, 1)) : 0,
            auto_links: activeStageIndex >= 6 ? run.counters.auto_links : 0,
            review_items: activeStageIndex >= 5 ? Math.floor(run.counters.review_items * progress) : 0,
            clusters_created: activeStageIndex >= 7 ? clust : 0,
        };
    };

    const metrics = getDisplayMetrics();
    const currentAgent = AGENTS[visualStage as keyof typeof AGENTS] || AGENTS.ingest;
    const AgentIcon = currentAgent.icon;

    if (!run) return <div className="flex justify-center items-center h-screen text-blue-500 animate-pulse">Loading pipeline status...</div>;

    // Filter logs based on selection
    const displayedLogs = selectedStep
        ? agentLogs.filter(l => l.stage === selectedStep || l.agent === 'System')
        : agentLogs;

    return (
        <div className="min-h-screen bg-gray-50 dark:bg-gray-950 text-gray-900 dark:text-white p-6 font-sans transition-colors duration-300">
            {/* Top Bar with Breadcrumbs */}
            <div className="flex justify-between items-center mb-8 border-b border-gray-200 dark:border-gray-800 pb-4">
                <div className="flex items-center gap-4">
                    <Link href="/pipeline" className="group flex items-center gap-2 text-gray-600 dark:text-gray-400 hover:text-gray-900 dark:hover:text-white transition">
                        <div className="p-2 rounded-full bg-gray-200 dark:bg-gray-900 group-hover:bg-gray-300 dark:group-hover:bg-gray-800">
                            <ArrowLeft size={16} />
                        </div>
                        <span className="text-sm font-medium">Back to Pipeline</span>
                    </Link>
                    <div className="h-8 w-[1px] bg-gray-200 dark:bg-gray-800 mx-2" />
                    <div>
                        <h1 className="text-2xl font-bold bg-clip-text text-transparent bg-gradient-to-r from-blue-600 to-emerald-600 dark:from-blue-400 dark:to-emerald-400">
                            Identity Resolution Pipeline
                        </h1>
                        <div className="flex items-center gap-2 text-xs text-gray-500 font-mono mt-1">
                            <span>RUN-ID: {runId.split('-')[0].toUpperCase()}</span>
                            <span>•</span>
                            <span className="text-gray-500 dark:text-gray-400">{run.description || "Manual Upload"}</span>
                        </div>
                    </div>
                </div>
                <div className="flex items-center gap-2">
                    <div className={`w-3 h-3 rounded-full ${run.status === 'RUNNING' || isReplaying ? 'bg-green-500 animate-ping' : 'bg-gray-500'}`} />
                    <span className="font-mono text-sm">{isReplaying ? 'REPLAYING' : run.status}</span>
                </div>
            </div>

            {/* Processing Pipeline Status */}
            <div className="mb-8 overflow-x-auto pb-4">
                <div className="flex items-center justify-between min-w-[700px] max-w-full relative px-4 md:px-10">
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
                        const isSelected = selectedStep === stageKey;

                        return (
                            <button
                                key={stageKey}
                                onClick={() => setSelectedStep(stageKey)}
                                className={`relative z-10 flex flex-col items-center gap-2 group transition-all duration-300 focus:outline-none
                                    ${isSelected ? 'scale-110' : 'hover:scale-105'}
                                `}
                            >
                                <div className={`w-12 h-12 rounded-full flex items-center justify-center border-2 transition-all duration-500 cursor-pointer
                                    ${isActive || isSelected ? `bg-gray-100 dark:bg-gray-900 ${agent.color.replace('text-', 'border-')} shadow-[0_0_25px_rgba(59,130,246,0.6)]` :
                                        isPast ? 'bg-gray-200 dark:bg-gray-800 border-gray-400 dark:border-gray-600 text-gray-500 dark:text-gray-400' :
                                            'bg-white dark:bg-gray-950 border-gray-200 dark:border-gray-800 text-gray-400 dark:text-gray-700'}
                                `}>
                                    <agent.icon size={20} className={isActive || isSelected ? agent.color : isPast ? 'text-gray-500 dark:text-gray-400' : 'text-gray-400 dark:text-gray-700'} />
                                </div>
                                <div className={`text-xs font-bold uppercase tracking-wider transition-colors duration-300 ${isActive || isSelected ? 'text-gray-900 dark:text-white' : 'text-gray-500 dark:text-gray-600'
                                    }`}>
                                    {agent.name.split(' ')[0]}
                                </div>
                                {/* Stage timing: actual duration for completed, elapsed/ETA for active */}
                                {(() => {
                                    const t = getStageTime(stageKey, isActive);
                                    return t ? (
                                        <div className={`text-[9px] font-mono tabular-nums -mt-0.5 transition-colors duration-300
                                            ${isPast ? 'text-emerald-600 dark:text-emerald-400' : 'text-blue-500 dark:text-blue-400 animate-pulse'}`}>
                                            {t}
                                        </div>
                                    ) : null;
                                })()}
                                {isSelected && (
                                    <div className="absolute -bottom-8 bg-blue-600 text-white text-[10px] px-2 py-1 rounded shadow-lg animate-bounce">
                                        Viewing Logs
                                    </div>
                                )}
                            </button>
                        );
                    })}
                </div>
            </div>

            <div className="grid grid-cols-1 lg:grid-cols-3 gap-8">

                {/* Left: Live Agent Spotlight */}
                <div className="lg:col-span-2 space-y-6">
                    {/* Active Agent Card */}
                    <div className="relative overflow-hidden bg-white dark:bg-gray-900/50 border border-blue-200 dark:border-blue-500/30 rounded-2xl p-8 shadow-xl dark:shadow-2xl">
                        <div className="absolute top-0 right-0 p-4 opacity-10 dark:opacity-20">
                            <AgentIcon size={120} className={currentAgent.color} />
                        </div>

                        <div className="relative z-10">
                            <h2 className="text-sm font-bold text-gray-500 dark:text-gray-400 uppercase tracking-widest mb-2">
                                {isReplaying ? 'Replay: Current Stage' : 'Current Processing Stage'}
                            </h2>
                            <div className="flex items-center gap-4 mb-6">
                                <div className={`p-4 rounded-xl bg-gray-100 dark:bg-gray-800 border-2 border-dashed ${currentAgent.color.replace('text-', 'border-')}`}>
                                    <AgentIcon size={40} className={currentAgent.color} />
                                </div>
                                <div>
                                    <h3 className="text-3xl font-bold text-gray-900 dark:text-white transition-all duration-300">{currentAgent.name}</h3>
                                    <p className={`text-lg ${currentAgent.color} opacity-90 transition-all duration-300`}>{currentAgent.role}</p>
                                </div>
                            </div>

                            <div className="bg-white/90 dark:bg-black/30 backdrop-blur rounded-lg p-4 border-l-4 border-blue-500">
                                <p className="text-xl font-mono text-blue-800 dark:text-blue-200 animate-pulse">
                                    {`> ${liveMessage || currentAgent.message}`}
                                </p>
                            </div>
                        </div>

                        {/* Progress Line */}
                        <div className="absolute bottom-0 left-0 w-full h-1 bg-gray-200 dark:bg-gray-800">
                            <div className="h-full bg-blue-500 animate-progress-indeterminate" />
                        </div>
                    </div>

                    {/* Processing Log */}
                    <div className="bg-white dark:bg-black border border-gray-200 dark:border-gray-800 rounded-xl p-4 font-mono text-sm h-[450px] max-h-[450px] flex flex-col shadow-inner relative overflow-hidden">
                        <div className="flex items-center justify-between text-gray-500 border-b border-gray-200 dark:border-gray-800 pb-2 mb-2">
                            <div className="flex items-center gap-2">
                                <Terminal size={14} />
                                <span>System.Agent.Log</span>
                            </div>
                            {selectedStep && (
                                <button onClick={() => setSelectedStep(null)} className="text-xs text-blue-600 dark:text-blue-400 hover:text-blue-500 dark:hover:text-blue-300">
                                    Clear Filter [Viewing: {selectedStep}]
                                </button>
                            )}
                        </div>
                        <div className="flex-1 overflow-y-auto space-y-1.5 pr-2 custom-scrollbar">
                            {displayedLogs.length === 0 ? (
                                <div className="text-gray-500 dark:text-gray-600 italic p-4 text-center">No logs recorded for this stage yet...</div>
                            ) : (
                                displayedLogs.map((log, i) => (
                                    <div key={i} className={`flex gap-2 items-start group animate-in fade-in slide-in-from-left-2 duration-200 rounded px-1 py-0.5
                                        ${log.type === 'warn'    ? 'bg-red-50 dark:bg-red-900/10' :
                                          log.type === 'success' ? 'bg-green-50 dark:bg-emerald-900/10' :
                                                                   'hover:bg-gray-50 dark:hover:bg-gray-900/30'}`}>
                                        {/* Timestamp */}
                                        <span className="text-gray-400 dark:text-gray-600 shrink-0 text-[10px] mt-0.5 font-mono tabular-nums">[{log.time}]</span>
                                        {/* Badge */}
                                        <span className={`shrink-0 text-[9px] font-bold uppercase tracking-wider px-1.5 py-0.5 rounded mt-0.5
                                            ${log.type === 'warn'    ? 'bg-red-100 dark:bg-red-900/30 text-red-600 dark:text-red-400' :
                                              log.type === 'success' ? 'bg-emerald-100 dark:bg-emerald-900/30 text-emerald-700 dark:text-emerald-400' :
                                                                       'bg-blue-100 dark:bg-blue-900/30 text-blue-700 dark:text-blue-300'}`}>
                                            {log.type === 'success' ? '✓ OK' : log.type === 'warn' ? '⚠ ERR' : '● INFO'}
                                        </span>
                                        {/* Agent */}
                                        <span className={`shrink-0 text-[10px] font-bold w-28 truncate mt-0.5
                                            ${log.stage === selectedStep ? 'text-yellow-600 dark:text-yellow-300' : 'text-gray-500 dark:text-gray-500'}`}>
                                            {log.agent}
                                        </span>
                                        {/* Message */}
                                        <span className={`break-all text-xs leading-tight mt-0.5
                                            ${log.type === 'warn'    ? 'text-red-700 dark:text-red-300' :
                                              log.type === 'success' ? 'text-emerald-700 dark:text-emerald-300' :
                                                                       'text-gray-800 dark:text-blue-100'}`}>
                                            {log.msg}
                                        </span>
                                    </div>
                                ))
                            )}
                            <div ref={logsEndRef} />
                        </div>
                    </div>
                </div>

                {/* Right: Metrics & Actions */}
                <div className="space-y-6">
                    <div className="bg-white dark:bg-gray-900 border border-gray-200 dark:border-gray-800 rounded-xl p-6 relative overflow-hidden">
                        {/* Gradual Reveal Mask during Replay could go here, but controlling values is cleaner */}
                        <div className="flex justify-between items-center mb-4">
                            <h3 className="text-gray-500 dark:text-gray-400 text-xs uppercase font-bold">Real-time Metrics</h3>
                            {isReplaying && <span className="text-[10px] text-green-600 dark:text-green-400 animate-pulse">● LIVE SYNC</span>}
                        </div>

                        <div className="grid grid-cols-1 gap-4">
                            <MetricCard label="Records Ingested" value={metrics.records_in} icon={Database} highlight={activeStageIndex >= 0} />
                            <MetricCard label="Candidates Found" value={metrics.candidates_generated} icon={Search} highlight={activeStageIndex >= 3} />
                            <MetricCard label="Auto-Linked" value={metrics.auto_links} icon={Zap} highlight={activeStageIndex >= 6} />
                            <MetricCard label="Manual Review" value={metrics.review_items} icon={FileText} highlight={activeStageIndex >= 5} />
                            <MetricCard label="Unique Identities" value={metrics.clusters_created} icon={Users} highlight={activeStageIndex >= 7} />
                        </div>
                    </div>

                    <div className="grid grid-cols-2 gap-3">
                        <Link href={`/explorer?runId=${runId}`} className="p-4 bg-blue-100 dark:bg-blue-600/20 border border-blue-200 dark:border-blue-500/50 hover:bg-blue-200 dark:hover:bg-blue-600/30 rounded-xl text-center group transition-all">
                            <Search className="mx-auto mb-2 text-blue-600 dark:text-blue-400 group-hover:scale-110 transition-transform" />
                            <span className="text-sm font-bold text-blue-800 dark:text-blue-200">Inspect Results</span>
                        </Link>
                        <Link href={`/graph?runId=${runId}`} className="block w-full text-center p-3 bg-fuchsia-100 dark:bg-fuchsia-600/20 border border-fuchsia-200 dark:border-fuchsia-500/50 hover:bg-fuchsia-200 dark:hover:bg-fuchsia-600/30 rounded-lg group transition-all">
                            <Network className="mx-auto mb-1 text-fuchsia-600 dark:text-fuchsia-400 group-hover:scale-110 transition-transform" size={18} />
                            <span className="text-xs font-bold text-fuchsia-800 dark:text-fuchsia-200">Graph View</span>
                        </Link>

                        <button
                            onClick={() => { setVisualStage('ingest'); setIsReplaying(true); setSelectedStep('ingest'); setAgentLogs([]); }}
                            disabled={isReplaying}
                            className="col-span-2 block w-full text-center p-3 bg-gray-100 dark:bg-gray-800 border border-gray-200 dark:border-gray-700 hover:bg-gray-200 dark:hover:bg-gray-700 rounded-lg group transition-all disabled:opacity-50"
                        >
                            <ArrowLeft className="inline mr-2 text-gray-500 dark:text-gray-400 group-hover:-translate-x-1 transition-transform" size={16} />
                            <span className="text-sm font-bold text-gray-700 dark:text-gray-300">{isReplaying ? 'Replaying Analysis...' : 'Replay Full Sequence'}</span>
                        </button>
                    </div>
                </div>

                {/* Live Insight Section (New) */}
                {(liveMatches.length > 0 || visualStage === 'cluster' || visualStage === 'score') && (
                    <div className="lg:col-span-3 grid grid-cols-1 lg:grid-cols-3 gap-8 animate-in fade-in slide-in-from-bottom-8 duration-700">
                        {/* Live Match Feed */}
                        <div className="lg:col-span-1 bg-white dark:bg-gray-900 border border-gray-200 dark:border-gray-800 rounded-2xl overflow-hidden flex flex-col h-[400px] shadow-lg">
                            <div className="p-4 border-b border-gray-200 dark:border-gray-800 bg-gray-50 dark:bg-gray-900/50 flex items-center justify-between">
                                <h3 className="text-gray-900 dark:text-white font-bold flex items-center gap-2 text-sm">
                                    <Zap className="text-yellow-500 w-4 h-4 animate-pulse" />
                                    Live Match Signals
                                </h3>
                                <span className="text-[10px] text-gray-500 uppercase tracking-widest font-bold">Realtime</span>
                            </div>
                            <div className="flex-1 overflow-y-auto p-4 space-y-3 custom-scrollbar">
                                {liveMatches.length > 0 ? (
                                    liveMatches.map((match, i) => (
                                        <div key={i} className="p-3 bg-gray-50 dark:bg-gray-800/30 border border-gray-100 dark:border-gray-700/50 rounded-xl hover:bg-white dark:hover:bg-gray-800/50 transition-colors group shadow-sm">
                                            <div className="flex items-center justify-between mb-2">
                                                <div className="flex items-center gap-2">
                                                    <span className="w-2 h-2 rounded-full bg-blue-500 animate-pulse"></span>
                                                    <span className="text-[10px] text-blue-600 dark:text-blue-400 font-mono font-bold">MATCH</span>
                                                </div>
                                                <span className="text-[10px] font-mono text-gray-500">Prob: {(match.probability * 100).toFixed(1)}%</span>
                                            </div>
                                            <div className="flex items-center gap-2 text-gray-900 dark:text-white">
                                                <div className="flex flex-col">
                                                    <span className="text-xs font-mono">{match.id1}</span>
                                                </div>
                                                <ArrowRight className="w-3 h-3 text-gray-400 group-hover:text-blue-500 transition-colors" />
                                                <div className="flex flex-col text-right">
                                                    <span className="text-xs font-mono">{match.id2}</span>
                                                </div>
                                            </div>
                                        </div>
                                    ))
                                ) : (
                                    <div className="h-full flex items-center justify-center text-gray-400 text-xs italic">
                                        Detecting potential duplicates...
                                    </div>
                                )}
                            </div>
                        </div>

                        {/* Live Identity Graph */}
                        <div className="lg:col-span-2 bg-white dark:bg-gray-900 border border-gray-200 dark:border-gray-800 rounded-2xl overflow-hidden flex flex-col h-[400px] shadow-lg">
                            <div className="p-4 border-b border-gray-200 dark:border-gray-800 bg-gray-50 dark:bg-gray-900/50 flex items-center justify-between">
                                <h3 className="text-gray-900 dark:text-white font-bold flex items-center gap-2 text-sm">
                                    <Share2 className="text-purple-500 w-4 h-4" />
                                    Identity Network Visualization
                                </h3>
                                <div className="flex items-center gap-2 text-[10px] text-gray-500 uppercase font-bold">
                                    <Activity className="text-blue-500 w-3 h-3 animate-ping" />
                                    Live
                                </div>
                            </div>
                            <div className="flex-1 relative">
                                <ClusterGraph
                                    data={graphData}
                                    loading={!graphData && (run?.status === 'RUNNING' || isReplaying)}
                                    loadingMessage="Constructing live identity graph..."
                                />
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
            `}</style>
        </div>
    );
}

const MetricCard = ({ label, value, icon: Icon, highlight }: any) => (
    <div className={`flex items-center gap-4 bg-white/50 dark:bg-gray-800/50 p-4 rounded-lg transform transition-all duration-500
        ${highlight ? 'border-l-4 border-blue-500 scale-102 bg-white dark:bg-gray-800' : 'border-l-2 border-gray-200 dark:border-gray-700 opacity-80'}
    `}>
        <div className={`p-2 rounded-md ${highlight ? 'bg-blue-100 dark:bg-blue-900/30 text-blue-600 dark:text-blue-400' : 'bg-gray-100 dark:bg-gray-900 text-gray-400 dark:text-gray-500'}`}>
            <Icon size={18} />
        </div>
        <div>
            <div className={`text-2xl font-bold font-mono transition-colors duration-500 ${highlight ? 'text-gray-900 dark:text-white' : 'text-gray-400 dark:text-gray-500'}`}>
                {Number(value).toLocaleString()}
            </div>
            <div className="text-xs text-gray-500 uppercase">{label}</div>
        </div>
    </div>
);
