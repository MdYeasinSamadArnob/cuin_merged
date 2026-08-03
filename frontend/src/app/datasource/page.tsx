"use client";

import { useState } from "react";
import { useRouter } from "next/navigation";
import { motion } from "framer-motion";
import { Database, Zap, Cpu, Network, CheckCircle2, Boxes, RefreshCw, Sparkles, AlertTriangle } from "lucide-react";

const API_BASE_URL = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000";

// The actual mechanism behind "update existing" is engine.clustering.
// entity_resolver's Jaccard carry-forward, which already runs on EVERY
// pipeline run by default -- re-running against corrected source data
// naturally updates existing entity clusters/Global IDs instead of
// creating a disconnected parallel identity world. "Run as new
// pipeline" is the explicit, rare opt-out: every resolved cluster
// mints a brand-new identity regardless of overlap with what's already
// there. See backend/api/routes_datasource.py's DatasourceStartRequest
// and engine/clustering/entity_resolver.py's resolve_entities docstring.
const RUN_MODES: { id: "update" | "new"; label: string; description: string; icon: any; warn?: boolean }[] = [
    {
        id: "update",
        label: "Update Existing",
        description: "Re-run against the current source data. Matches carry forward onto existing entity clusters and Global IDs -- corrections update what's already there.",
        icon: RefreshCw,
    },
    {
        id: "new",
        label: "Run as New Pipeline",
        description: "Every resolved cluster gets a brand-new identity, even where it overlaps with existing entities. Existing Global IDs and entity groupings are NOT reused.",
        icon: Sparkles,
        warn: true,
    },
];

export default function DatasourcePage() {
    const [isStarting, setIsStarting] = useState(false);
    const [runMode, setRunMode] = useState<"update" | "new">("update");
    const router = useRouter();

    const handleStartDemo = async () => {
        setIsStarting(true);
        try {
            const res = await fetch(`${API_BASE_URL}/datasource/demo`, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ mode: "FULL", carry_forward: runMode === "update" })
            });
            const data = await res.json();

            if (res.ok && data.run_id) {
                // Navigate to the runs page to watch the pipeline
                router.push(`/runs/${data.run_id}`);
            } else {
                console.error("Failed to start pipeline:", data);
                alert("Failed to start demo pipeline.");
                setIsStarting(false);
            }
        } catch (error) {
            console.error(error);
            alert("Error connecting to backend.");
            setIsStarting(false);
        }
    };

    return (
        <div className="max-w-4xl mx-auto py-8">
            <div className="mb-8">
                <h1 className="text-3xl font-bold bg-clip-text text-transparent bg-gradient-to-r from-blue-600 to-emerald-600 dark:from-blue-400 dark:to-emerald-400 mb-2">
                    Datasource Integration
                </h1>
                <p className="text-gray-600 dark:text-gray-400">
                    Run entity resolution over the Oracle Parquet datasource via Apache Doris --
                    distributed, colocated-join execution of the deterministic Ruleset v2
                    blocking/scoring logic.
                </p>
            </div>

            <div className="grid grid-cols-1 md:grid-cols-2 gap-6 mb-10">
                {/* Information Card */}
                <motion.div
                    initial={{ opacity: 0, y: 20 }}
                    animate={{ opacity: 1, y: 0 }}
                    transition={{ duration: 0.5 }}
                    className="bg-white dark:bg-gray-900 border border-gray-200 dark:border-gray-800 rounded-2xl p-6 shadow-xl dark:shadow-none"
                >
                    <div className="flex items-center gap-4 mb-4">
                        <div className="p-3 bg-blue-100 dark:bg-blue-900/30 text-blue-600 dark:text-blue-400 rounded-full">
                            <Database size={24} />
                        </div>
                        <h2 className="text-xl font-semibold text-gray-900 dark:text-white">Active Source</h2>
                    </div>

                    <div className="space-y-4 font-mono text-sm">
                        <div className="flex justify-between items-center border-b border-gray-100 dark:border-gray-800 pb-2">
                            <span className="text-gray-500">FORMAT</span>
                            <span className="font-bold text-gray-900 dark:text-white">Parquet</span>
                        </div>
                        <div className="flex justify-between items-center border-b border-gray-100 dark:border-gray-800 pb-2">
                            <span className="text-gray-500">PATH</span>
                            <span className="font-bold text-gray-900 dark:text-white">backend/data_source/oracle_data.parquet</span>
                        </div>
                        <div className="flex justify-between items-center pb-2">
                            <span className="text-gray-500">ENGINE</span>
                            <span className="font-bold text-blue-600 dark:text-blue-400 flex items-center gap-1.5">
                                <Boxes size={14} /> Apache Doris
                            </span>
                        </div>
                    </div>
                </motion.div>

                {/* Pipeline Overview Card */}
                <motion.div
                    initial={{ opacity: 0, y: 20 }}
                    animate={{ opacity: 1, y: 0 }}
                    transition={{ duration: 0.5, delay: 0.1 }}
                    className="bg-gradient-to-br from-slate-900 to-blue-950 dark:from-gray-900 dark:to-blue-950 rounded-2xl p-6 text-white shadow-xl relative overflow-hidden"
                >
                    <div className="absolute top-0 right-0 p-4 opacity-10">
                        <Zap size={100} />
                    </div>
                    <h2 className="text-xl font-semibold mb-6 flex items-center gap-2 relative z-10">
                        <Cpu size={20} className="text-blue-400" /> Executive Overview
                    </h2>

                    <ul className="space-y-4 relative z-10">
                        <li className="flex items-start gap-3">
                            <CheckCircle2 size={18} className="text-emerald-400 mt-0.5 shrink-0" />
                            <span className="text-blue-100 text-sm">Stream-loads the Parquet dataset into a dedicated Doris database for this run.</span>
                        </li>
                        <li className="flex items-start gap-3">
                            <CheckCircle2 size={18} className="text-emerald-400 mt-0.5 shrink-0" />
                            <span className="text-blue-100 text-sm">Deterministic rule-based blocking and tiered decisioning, compiled to Doris SQL -- colocated joins, no shuffle.</span>
                        </li>
                        <li className="flex items-start gap-3">
                            <CheckCircle2 size={18} className="text-emerald-400 mt-0.5 shrink-0" />
                            <span className="text-blue-100 text-sm">Same output every run -- bit-reproducible, audit-friendly.</span>
                        </li>
                    </ul>
                </motion.div>
            </div>

            {/* Run Mode Selector */}
            <div className="mb-4">
                <h3 className="text-sm font-semibold text-gray-700 dark:text-gray-300 mb-3">Run Mode</h3>
                <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
                    {RUN_MODES.map((m) => {
                        const Icon = m.icon;
                        const selected = runMode === m.id;
                        return (
                            <button
                                key={m.id}
                                onClick={() => setRunMode(m.id)}
                                className={`text-left p-4 rounded-xl border-2 transition-all ${
                                    selected
                                        ? m.warn
                                            ? "border-amber-500 bg-amber-50 dark:bg-amber-950/20 shadow-lg"
                                            : "border-blue-500 bg-blue-50 dark:bg-blue-950/30 shadow-lg"
                                        : "border-gray-200 dark:border-gray-800 bg-white dark:bg-gray-900 hover:border-gray-300 dark:hover:border-gray-700"
                                }`}
                            >
                                <div className="flex items-center gap-2 mb-2">
                                    <Icon size={18} className={selected ? (m.warn ? "text-amber-600 dark:text-amber-400" : "text-blue-600 dark:text-blue-400") : "text-gray-400"} />
                                    <span className={`font-semibold ${selected ? (m.warn ? "text-amber-700 dark:text-amber-300" : "text-blue-700 dark:text-blue-300") : "text-gray-900 dark:text-white"}`}>
                                        {m.label}
                                    </span>
                                    {m.id === "update" && <span className="badge badge-info !text-[10px] !py-0">default</span>}
                                    {m.warn && selected && <AlertTriangle size={14} className="text-amber-500 ml-auto" />}
                                </div>
                                <p className="text-xs text-gray-500 dark:text-gray-400">{m.description}</p>
                            </button>
                        );
                    })}
                </div>
            </div>

            {/* Action Bar */}
            <motion.div
                initial={{ opacity: 0, scale: 0.95 }}
                animate={{ opacity: 1, scale: 1 }}
                transition={{ duration: 0.5, delay: 0.2 }}
                className="flex items-center justify-between bg-white dark:bg-gray-900 p-6 rounded-2xl border border-gray-200 dark:border-gray-800 shadow-lg"
            >
                <div className="flex items-center gap-4">
                    <div className="p-4 bg-emerald-100 dark:bg-emerald-900/30 text-emerald-600 dark:text-emerald-400 rounded-full animate-pulse">
                        <Network size={28} />
                    </div>
                    <div>
                        <h3 className="text-lg font-bold text-gray-900 dark:text-white">Ready to Ingest</h3>
                        <p className="text-sm text-gray-500">
                            Trigger the Apache Doris pipeline -- {RUN_MODES.find((m) => m.id === runMode)?.label}
                        </p>
                    </div>
                </div>

                <button
                    onClick={handleStartDemo}
                    disabled={isStarting}
                    className={`
                        flex items-center justify-center gap-2 px-8 py-4 rounded-xl font-bold text-white transition-all shadow-xl
                        ${isStarting
                            ? 'bg-gray-400 cursor-not-allowed'
                            : runMode === 'new'
                                ? 'bg-gradient-to-r from-amber-600 to-orange-500 hover:from-amber-500 hover:to-orange-400 hover:scale-105 active:scale-95'
                                : 'bg-gradient-to-r from-blue-600 to-emerald-500 hover:from-blue-500 hover:to-emerald-400 hover:scale-105 active:scale-95'
                        }
                    `}
                >
                    {isStarting ? (
                        <>
                            <div className="animate-spin rounded-full h-5 w-5 border-b-2 border-white"></div>
                            Starting Apache Doris...
                        </>
                    ) : (
                        <>
                            <Zap size={20} />
                            {runMode === "update" ? "Start Demo Ingestion" : "Start as New Pipeline"}
                        </>
                    )}
                </button>
            </motion.div>
        </div>
    );
}
