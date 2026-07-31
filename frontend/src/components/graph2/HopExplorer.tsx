"use client";

import { AlertTriangle } from "lucide-react";
import { MiniForceGraph, MiniNode, MiniEdge } from "./MiniForceGraph";

interface HopData {
    seed_codes: string[];
    hops: number;
    max_nodes: number;
    nodes: { id: string; label: string; segment: string; is_seed: boolean }[];
    edges: { source: string; target: string; kind: string }[];
    truncated: boolean;
}

export function HopExplorer({
    data, loading, seedLabel, depth, onDepthChange, onClose,
}: {
    data: HopData | null;
    loading: boolean;
    seedLabel: string;
    depth: number;
    onDepthChange: (d: number) => void;
    onClose: () => void;
}) {
    const miniNodes: MiniNode[] = (data?.nodes || []).map((n) => ({ id: n.id, label: n.label, segment: n.segment, isCenter: n.is_seed }));
    const miniEdges: MiniEdge[] = (data?.edges || []).map((e) => ({ source: e.source, target: e.target, kind: e.kind }));

    return (
        <div className="space-y-3">
            <div className="flex items-center justify-between">
                <div>
                    <h3 className="text-sm font-semibold text-gray-900 dark:text-white">Relationship explorer</h3>
                    <p className="text-xs text-gray-500 dark:text-gray-400">From {seedLabel}</p>
                </div>
                <button onClick={onClose} className="text-xs text-gray-400 hover:text-gray-700 dark:hover:text-gray-200">Close</button>
            </div>

            <div className="flex items-center gap-2 text-xs">
                <span className="text-gray-500 dark:text-gray-400">Hops</span>
                {[1, 2, 3].map((d) => (
                    <button
                        key={d}
                        onClick={() => onDepthChange(d)}
                        className={`px-2.5 py-1 rounded-lg transition-colors ${depth === d ? "bg-blue-600 text-white" : "bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-200 dark:hover:bg-gray-600"}`}
                    >
                        {d}
                    </button>
                ))}
            </div>

            {loading && <p className="text-xs text-gray-400">Exploring...</p>}

            {data && (
                <>
                    <div className="flex items-center gap-3 text-[11px] text-gray-500 dark:text-gray-400">
                        <span>{data.nodes.length} records</span>
                        <span>{data.edges.length} links</span>
                        <span className="flex items-center gap-1"><span className="inline-block w-2 h-2 rounded-full bg-emerald-500" /> same-segment match</span>
                        <span className="flex items-center gap-1"><span className="inline-block w-2 h-2 rounded-full bg-purple-500" /> cross-segment relationship</span>
                    </div>
                    {data.truncated && (
                        <div className="flex items-center gap-1.5 text-[11px] text-amber-600 dark:text-amber-400">
                            <AlertTriangle size={12} /> Showing {data.nodes.length} of possibly more -- capped at {data.max_nodes} nodes.
                        </div>
                    )}
                    <MiniForceGraph nodes={miniNodes} edges={miniEdges} height={320} />
                </>
            )}
        </div>
    );
}
