"use client";

import { Building2, User as UserIcon, IdCard, Waypoints, Link2 } from "lucide-react";
import { MiniForceGraph, MiniNode, MiniEdge } from "./MiniForceGraph";
import { ClusterBridgesSection } from "./BridgesPanel";

interface ClusterDetail {
    entity_id: string;
    global_ref: string | null;
    global_ref_state: string | null;
    status: string;
    nodes: { id: string; label: string; segment: string }[];
    edges: { source: string; target: string; confidence_pct: number; has_veto: boolean; decision: string }[];
}

export function ClusterDrillDown({
    data, loading, onExploreFrom, bridges, bridgesLoading, onJumpToEntity,
}: {
    data: ClusterDetail | null;
    loading: boolean;
    onExploreFrom: (customerCode: string, label: string) => void;
    bridges?: any[] | null;
    bridgesLoading?: boolean;
    onJumpToEntity?: (entityId: string) => void;
}) {
    if (loading) return <p className="text-xs text-gray-400">Loading cluster...</p>;
    if (!data) return <p className="text-xs text-gray-400">Click a bubble to see that cluster's real members and match evidence.</p>;

    const miniNodes: MiniNode[] = data.nodes.map((n) => ({ id: n.id, label: n.label, segment: n.segment }));
    const miniEdges: MiniEdge[] = data.edges.map((e) => ({
        source: e.source, target: e.target, confidence_pct: e.confidence_pct, has_veto: e.has_veto, decision: e.decision,
    }));

    return (
        <div className="space-y-3">
            <div className="flex items-center justify-between">
                <span className="font-mono text-xs text-gray-500">{data.entity_id}</span>
                <span className="badge badge-info !text-[10px]">{data.status}</span>
            </div>
            {data.global_ref && (
                <div className="flex items-center gap-1.5 text-xs text-emerald-600 dark:text-emerald-400">
                    <IdCard size={12} /> {data.global_ref} <span className="text-gray-400">({data.global_ref_state})</span>
                </div>
            )}

            <MiniForceGraph nodes={miniNodes} edges={miniEdges} height={280} />

            <div>
                <h4 className="text-[11px] font-semibold text-gray-500 dark:text-gray-400 uppercase tracking-wider mb-1.5">
                    Members ({data.nodes.length})
                </h4>
                <div className="space-y-1 max-h-52 overflow-y-auto">
                    {data.nodes.map((n) => {
                        const Icon = n.segment === "COMPANY" ? Building2 : UserIcon;
                        return (
                            <div key={n.id} className="flex items-center justify-between p-1.5 rounded bg-gray-50 dark:bg-gray-900/40 text-xs">
                                <div className="flex items-center gap-1.5 min-w-0">
                                    <Icon size={11} className="text-gray-400 shrink-0" />
                                    <span className="font-mono text-gray-500 shrink-0">{n.id}</span>
                                    <span className="text-gray-700 dark:text-gray-300 truncate">{n.label}</span>
                                </div>
                                <button
                                    onClick={() => onExploreFrom(n.id, n.label)}
                                    className="flex items-center gap-1 text-blue-600 dark:text-blue-400 hover:underline shrink-0 ml-2"
                                    title="Explore relationships from this record"
                                >
                                    <Waypoints size={12} />
                                </button>
                            </div>
                        );
                    })}
                </div>
            </div>

            {onJumpToEntity && (
                <div>
                    <h4 className="text-[11px] font-semibold text-gray-500 dark:text-gray-400 uppercase tracking-wider mb-1.5 flex items-center gap-1">
                        <Link2 size={11} /> External connections
                    </h4>
                    <ClusterBridgesSection bridges={bridges ?? null} loading={!!bridgesLoading} onJump={onJumpToEntity} />
                </div>
            )}
        </div>
    );
}
