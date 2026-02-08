"use client";

import { useEffect, useState, useRef } from "react";
import { useParams, useRouter } from "next/navigation";
import { api } from "@/lib/api";
import { useWebSocket } from "@/lib/ws";
import Link from "next/link";
import {
    ArrowLeft, Terminal, Activity, Search, Network,
    Cpu, Database, Shield, Zap, FileText, CheckCircle
} from "lucide-react";

// --- Agents Config ---
const AGENTS = {
    ingest: { name: 'Data Ingestor', role: 'System', icon: Database, color: 'text-blue-400', message: 'Reading raw data stream...' },
    normalize: { name: 'Standardizer Droid', role: 'Cleaner', icon: FileText, color: 'text-cyan-400', message: 'Formatting phone numbers & addresses...' },
    block: { name: 'Blocker Bee', role: 'Indexer', icon: Cpu, color: 'text-indigo-400', message: 'Creating candidate clusters...' },
    candidates: { name: 'Pair Finder', role: 'Scout', icon: Search, color: 'text-yellow-400', message: 'Identifying potential duplicates...' },
    score: { name: 'Splink Oracle', role: 'Analyst', icon: Activity, color: 'text-pink-400', message: 'Calculating similarity vectors...' },
    decide: { name: 'Judge AI', role: 'Decision', icon: Shield, color: 'text-emerald-400', message: 'Applying business rules...' },
    cluster: { name: 'Graph Weaver', role: 'Architect', icon: Network, color: 'text-purple-400', message: 'Resolving identity clusters...' },
    complete: { name: 'System', role: 'Admin', icon: CheckCircle, color: 'text-green-500', message: 'Pipeline complete.' }
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
    const logsEndRef = useRef<HTMLDivElement>(null);

    // Ordered stages for the pipeline view
    const pipelineStages = ['ingest', 'normalize', 'block', 'candidates', 'score', 'decide', 'cluster', 'complete'];
    const activeStageIndex = pipelineStages.indexOf(visualStage);
    // Initial Fetch & Cinematic Start
    useEffect(() => {
        if (runId) fetchRunDetails();
    }, [runId]);

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

        const data = lastEvent;
        if (data.run_id && data.run_id !== runId) return;

        if (data.type === 'STAGE_PROGRESS') {
            const payload = data.data as any;
            setActiveStage(payload.stage);
            setVisualStage(payload.stage); // Sync visual
            setSelectedStep(payload.stage); // Auto-focus active

            const agent = AGENTS[payload.stage as keyof typeof AGENTS] || AGENTS.complete;
            if (payload.status === 'running') {
                addLog(agent.name, payload.message || agent.message, 'info', payload.stage);
            } else if (payload.status === 'complete') {
                addLog(agent.name, `${agent.role} task finished.`, 'success', payload.stage);
            }
        }
        else if (data.type === 'RUN_COMPLETE') {
            setActiveStage('complete');
            setVisualStage('complete');
            addLog("System", "Run analysis completed successfully.", 'success', 'complete');
            fetchRunDetails();
        }
        else if (data.type === 'RUN_FAILED') {
            addLog("System", `Run failed: ${data.data?.error || 'Unknown error'}`, 'warn', activeStage);
        }
    }, [lastEvent, runId, isReplaying]);

    // Cleanup logs scrolling
    useEffect(() => {
        logsEndRef.current?.scrollIntoView({ behavior: 'smooth' });
    }, [agentLogs]);

    const fetchRunDetails = async () => {
        try {
            const data = await api.getRun(runId);
            setRun(data);

            // If run is already done when we load, trigger cinematic replay once
            if (data.status === 'COMPLETED' && !isReplaying) {
                // Check if we haven't played it yet (could use ref or local state)
                // For now, always replay on fresh load if completed, it's cool.
                setIsReplaying(true);
            } else if (data.current_stage) {
                setActiveStage(data.current_stage);
                setVisualStage(data.current_stage);
            }

            // Populate initial logs if empty
            if (agentLogs.length === 0 && data.status !== 'PENDING') {
                addLog("System", "Initializing agent swarm...", "info", 'ingest');
            }
        } catch (err) {
            console.error("Failed to load run", err);
        }
    };

    const addLog = (agent: string, msg: string, type: 'info' | 'success' | 'warn' = 'info', stage: string = 'ingest') => {
        setAgentLogs(prev => [...prev.slice(-40), {
            time: new Date().toLocaleTimeString([], { hour12: false, hour: '2-digit', minute: '2-digit', second: '2-digit' }),
            agent,
            msg,
            type,
            stage
        }]);
    };

    // calculate simulated metrics based on progress if replaying; else usage real metrics
    const getDisplayMetrics = () => {
        if (!run) return { records_in: 0, candidates_generated: 0, auto_links: 0, review_items: 0 };
        if (!isReplaying) return run.counters;

        // Gradual Reveal Math
        const progress = activeStageIndex / (pipelineStages.length - 1);
        return {
            records_in: activeStageIndex >= 0 ? Math.floor(run.counters.records_in * Math.min(progress * 2, 1)) : 0,
            candidates_generated: activeStageIndex >= 3 ? Math.floor(run.counters.candidates_generated * Math.min((progress - 0.3) * 2, 1)) : 0,
            auto_links: activeStageIndex >= 6 ? run.counters.auto_links : 0,
            review_items: activeStageIndex >= 5 ? Math.floor(run.counters.review_items * progress) : 0,
        };
    };

    const metrics = getDisplayMetrics();
    const currentAgent = AGENTS[visualStage as keyof typeof AGENTS] || AGENTS.ingest;
    const AgentIcon = currentAgent.icon;

    if (!run) return <div className="flex justify-center items-center h-screen text-blue-500 animate-pulse">Initializing Agent Swarm...</div>;

    // Filter logs based on selection
    const displayedLogs = selectedStep
        ? agentLogs.filter(l => l.stage === selectedStep || l.agent === 'System')
        : agentLogs;

    return (
        <div className="min-h-screen bg-gray-950 text-white p-6 font-sans">
            {/* Top Bar with Breadcrumbs */}
            <div className="flex justify-between items-center mb-8 border-b border-gray-800 pb-4">
                <div className="flex items-center gap-4">
                    <Link href="/pipeline" className="group flex items-center gap-2 text-gray-400 hover:text-white transition">
                        <div className="p-2 rounded-full bg-gray-900 group-hover:bg-gray-800">
                            <ArrowLeft size={16} />
                        </div>
                        <span className="text-sm font-medium">Back to Pipeline</span>
                    </Link>
                    <div className="h-8 w-[1px] bg-gray-800 mx-2" />
                    <div>
                        <h1 className="text-2xl font-bold bg-clip-text text-transparent bg-gradient-to-r from-blue-400 to-emerald-400">
                            Agentic Analysis Protocol
                        </h1>
                        <div className="flex items-center gap-2 text-xs text-gray-500 font-mono mt-1">
                            <span>RUN-ID: {runId.split('-')[0].toUpperCase()}</span>
                            <span>•</span>
                            <span className="text-gray-400">{run.description || "Manual Upload"}</span>
                        </div>
                    </div>
                </div>
                <div className="flex items-center gap-2">
                    <div className={`w-3 h-3 rounded-full ${run.status === 'RUNNING' || isReplaying ? 'bg-green-500 animate-ping' : 'bg-gray-500'}`} />
                    <span className="font-mono text-sm">{isReplaying ? 'REPLAYING' : run.status}</span>
                </div>
            </div>

            {/* Mission Progress Pipeline (Interactive) */}
            <div className="mb-8 overflow-x-auto pb-4">
                <div className="flex items-center justify-between min-w-[800px] relative px-10">
                    {/* Connecting Line */}
                    <div className="absolute top-1/2 left-0 w-full h-1 bg-gray-800 -z-0 -translate-y-1/2" />
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
                                    ${isActive || isSelected ? `bg-gray-900 ${agent.color.replace('text-', 'border-')} shadow-[0_0_25px_rgba(59,130,246,0.6)]` :
                                        isPast ? 'bg-gray-800 border-gray-600 text-gray-400' :
                                            'bg-gray-950 border-gray-800 text-gray-700'}
                                `}>
                                    <agent.icon size={20} className={isActive || isSelected ? agent.color : isPast ? 'text-gray-400' : 'text-gray-700'} />
                                </div>
                                <div className={`text-xs font-bold uppercase tracking-wider transition-colors duration-300 ${isActive || isSelected ? 'text-white' : 'text-gray-600'
                                    }`}>
                                    {agent.name.split(' ')[0]}
                                </div>
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
                    <div className="relative overflow-hidden bg-gray-900/50 border border-blue-500/30 rounded-2xl p-8 shadow-2xl">
                        <div className="absolute top-0 right-0 p-4 opacity-20">
                            <AgentIcon size={120} className={currentAgent.color} />
                        </div>

                        <div className="relative z-10">
                            <h2 className="text-sm font-bold text-gray-400 uppercase tracking-widest mb-2">
                                {isReplaying ? 'Live Replay: Active Agent' : 'Current Active Agent'}
                            </h2>
                            <div className="flex items-center gap-4 mb-6">
                                <div className={`p-4 rounded-xl bg-gray-800 border-2 border-dashed ${currentAgent.color.replace('text-', 'border-')}`}>
                                    <AgentIcon size={40} className={currentAgent.color} />
                                </div>
                                <div>
                                    <h3 className="text-3xl font-bold text-white transition-all duration-300">{currentAgent.name}</h3>
                                    <p className={`text-lg ${currentAgent.color} opacity-90 transition-all duration-300`}>{currentAgent.role}</p>
                                </div>
                            </div>

                            <div className="bg-black/30 backdrop-blur rounded-lg p-4 border-l-4 border-blue-500">
                                <p className="text-xl font-mono text-blue-200 animate-pulse">
                                    {`> ${currentAgent.message}`}
                                </p>
                            </div>
                        </div>

                        {/* Progress Line */}
                        <div className="absolute bottom-0 left-0 w-full h-1 bg-gray-800">
                            <div className="h-full bg-blue-500 animate-progress-indeterminate" />
                        </div>
                    </div>

                    {/* Interactive Log Console */}
                    <div className="bg-black border border-gray-800 rounded-xl p-4 font-mono text-sm h-[350px] flex flex-col shadow-inner relative">
                        <div className="flex items-center justify-between text-gray-500 border-b border-gray-800 pb-2 mb-2">
                            <div className="flex items-center gap-2">
                                <Terminal size={14} />
                                <span>System.Agent.Log</span>
                            </div>
                            {selectedStep && (
                                <button onClick={() => setSelectedStep(null)} className="text-xs text-blue-400 hover:text-blue-300">
                                    Clear Filter [Viewing: {selectedStep}]
                                </button>
                            )}
                        </div>
                        <div className="flex-1 overflow-y-auto space-y-2 pr-2 custom-scrollbar">
                            {displayedLogs.length === 0 ? (
                                <div className="text-gray-600 italic p-4 text-center">No logs recorded for this stage yet...</div>
                            ) : (
                                displayedLogs.map((log, i) => (
                                    <div key={i} className={`flex gap-3 group animate-in fade-in slide-in-from-left-2 duration-300 ${log.type === 'warn' ? 'text-red-400' :
                                            log.type === 'success' ? 'text-green-400' : 'text-blue-200'
                                        }`}>
                                        <span className="text-gray-600 shrink-0 text-[10px] mt-1">[{log.time}]</span>
                                        <span className={`font-bold shrink-0 w-24 border-r border-gray-800 mr-2 opacity-70 ${log.stage === selectedStep ? 'text-yellow-300' : ''
                                            }`}>
                                            {log.agent}
                                        </span>
                                        <span className="break-words w-full">{log.msg}</span>
                                    </div>
                                ))
                            )}
                            <div ref={logsEndRef} />
                        </div>
                    </div>
                </div>

                {/* Right: Metrics & Actions */}
                <div className="space-y-6">
                    <div className="bg-gray-900 border border-gray-800 rounded-xl p-6 relative overflow-hidden">
                        {/* Gradual Reveal Mask during Replay could go here, but controlling values is cleaner */}
                        <div className="flex justify-between items-center mb-4">
                            <h3 className="text-gray-400 text-xs uppercase font-bold">Real-time Metrics</h3>
                            {isReplaying && <span className="text-[10px] text-green-400 animate-pulse">● LIVE SYNC</span>}
                        </div>

                        <div className="grid grid-cols-1 gap-4">
                            <MetricCard label="Records Ingested" value={metrics.records_in} icon={Database} highlight={activeStageIndex >= 0} />
                            <MetricCard label="Candidates Found" value={metrics.candidates_generated} icon={Search} highlight={activeStageIndex >= 3} />
                            <MetricCard label="Auto-Linked" value={metrics.auto_links} icon={Zap} highlight={activeStageIndex >= 6} />
                            <MetricCard label="Manual Review" value={metrics.review_items} icon={FileText} highlight={activeStageIndex >= 5} />
                        </div>
                    </div>

                    <div className="grid grid-cols-2 gap-3">
                        <Link href={`/explorer?runId=${runId}`} className="p-4 bg-blue-600/20 border border-blue-500/50 hover:bg-blue-600/30 rounded-xl text-center group transition-all">
                            <Search className="mx-auto mb-2 text-blue-400 group-hover:scale-110 transition-transform" />
                            <span className="text-sm font-bold text-blue-200">Inspect Results</span>
                        </Link>
                        <Link href={`/graph?runId=${runId}`} className="block w-full text-center p-3 bg-fuchsia-600/20 border border-fuchsia-500/50 hover:bg-fuchsia-600/30 rounded-lg group transition-all">
                            <Network className="mx-auto mb-1 text-fuchsia-400 group-hover:scale-110 transition-transform" size={18} />
                            <span className="text-xs font-bold text-fuchsia-200">Graph View</span>
                        </Link>

                        <button
                            onClick={() => { setVisualStage('ingest'); setIsReplaying(true); setSelectedStep('ingest'); setAgentLogs([]); }}
                            disabled={isReplaying}
                            className="col-span-2 block w-full text-center p-3 bg-gray-800 border border-gray-700 hover:bg-gray-700 rounded-lg group transition-all disabled:opacity-50"
                        >
                            <ArrowLeft className="inline mr-2 text-gray-400 group-hover:-translate-x-1 transition-transform" size={16} />
                            <span className="text-sm font-bold text-gray-300">{isReplaying ? 'Replaying Analysis...' : 'Replay Full Sequence'}</span>
                        </button>
                    </div>
                </div>
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
    <div className={`flex items-center gap-4 bg-gray-800/50 p-4 rounded-lg transform transition-all duration-500
        ${highlight ? 'border-l-4 border-blue-500 scale-102 bg-gray-800' : 'border-l-2 border-gray-700 opacity-80'}
    `}>
        <div className={`p-2 rounded-md ${highlight ? 'bg-blue-900/30 text-blue-400' : 'bg-gray-900 text-gray-500'}`}>
            <Icon size={18} />
        </div>
        <div>
            <div className={`text-2xl font-bold font-mono transition-colors duration-500 ${highlight ? 'text-white' : 'text-gray-500'}`}>
                {Number(value).toLocaleString()}
            </div>
            <div className="text-xs text-gray-500 uppercase">{label}</div>
        </div>
    </div>
);
