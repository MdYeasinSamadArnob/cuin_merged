"use client";

import { ArrowRight, Building2, User as UserIcon, Waypoints, IdCard } from "lucide-react";

interface BridgeItem {
    entity_id_a: string;
    entity_id_b: string;
    entity_a_name: string;
    entity_b_name: string;
    entity_a_size: number | null;
    entity_b_size: number | null;
    entity_a_global_ref: string | null;
    entity_b_global_ref: string | null;
    kind: "SAME_SEGMENT" | "CROSS_SEGMENT";
    confidence_pct: number | null;
    decision: string | null;
    has_veto: boolean;
    evidence: { field: string; value: string }[] | null;
}

function KindBadge({ kind }: { kind: string }) {
    return kind === "CROSS_SEGMENT" ? (
        <span className="badge badge-warning !text-[10px] shrink-0">Cross-segment</span>
    ) : (
        <span className="badge badge-info !text-[10px] shrink-0">Same-segment</span>
    );
}

function EntitySide({ entityId, name, size, globalRef, onJump }: {
    entityId: string; name: string; size: number | null; globalRef: string | null;
    onJump: (entityId: string) => void;
}) {
    return (
        <button
            onClick={() => onJump(entityId)}
            className="flex-1 min-w-0 text-left p-2 rounded-lg bg-gray-50 dark:bg-gray-900/40 hover:bg-gray-100 dark:hover:bg-gray-900/70 transition-colors"
            title="View this cluster"
        >
            <div className="text-xs font-semibold text-gray-900 dark:text-white truncate">{name}</div>
            <div className="flex items-center gap-1.5 text-[10px] text-gray-500 dark:text-gray-400 mt-0.5">
                <span>{size ?? "?"} members</span>
                {globalRef && (
                    <span className="flex items-center gap-0.5 text-emerald-600 dark:text-emerald-400">
                        <IdCard size={9} /> {globalRef}
                    </span>
                )}
            </div>
        </button>
    );
}

export function BridgesPanel({
    items, loading, total, page, pageSize, onPageChange, onJump,
    minConfidence, onMinConfidenceChange,
}: {
    items: BridgeItem[];
    loading: boolean;
    total: number;
    page: number;
    pageSize: number;
    onPageChange: (page: number) => void;
    onJump: (entityId: string) => void;
    minConfidence: number | undefined;
    onMinConfidenceChange: (v: number | undefined) => void;
}) {
    const totalPages = Math.max(1, Math.ceil(total / pageSize));

    return (
        <div className="space-y-3">
            <div className="flex flex-wrap items-center gap-3 bg-white dark:bg-gray-800/50 border border-gray-200 dark:border-gray-700 rounded-xl p-3">
                <span className="text-xs text-gray-500 dark:text-gray-400">
                    Two clusters connected by real matching evidence -- a shared phone/address strong enough to blocking-match but not merged, or a cross-segment (person/company) relationship.
                </span>
                <div className="flex items-center gap-1.5 text-xs text-gray-500 dark:text-gray-400 ml-auto">
                    <span>Min confidence</span>
                    <input
                        type="number" min={0} max={100} placeholder="any" className="w-16 text-xs py-1 px-1.5 text-center"
                        value={minConfidence ?? ''} onChange={(e) => onMinConfidenceChange(e.target.value === '' ? undefined : Number(e.target.value))}
                    />
                </div>
            </div>

            <div className="glass-card p-3">
                <div className="flex items-center justify-between mb-2 px-1">
                    <span className="text-xs text-gray-500 dark:text-gray-400">
                        {total.toLocaleString()} cross-cluster bridges
                    </span>
                </div>
                {loading ? (
                    <p className="text-xs text-gray-400 p-3">Loading bridges...</p>
                ) : items.length === 0 ? (
                    <p className="text-xs text-gray-400 p-3">No cross-cluster bridges found for this run.</p>
                ) : (
                    <div className="space-y-1.5 max-h-[560px] overflow-y-auto">
                        {items.map((b, idx) => (
                            <div key={`${b.entity_id_a}-${b.entity_id_b}-${idx}`} className="p-2 rounded-lg border border-gray-100 dark:border-gray-800">
                                <div className="flex items-center gap-2">
                                    <EntitySide entityId={b.entity_id_a} name={b.entity_a_name} size={b.entity_a_size} globalRef={b.entity_a_global_ref} onJump={onJump} />
                                    <ArrowRight size={14} className="text-gray-400 shrink-0" />
                                    <EntitySide entityId={b.entity_id_b} name={b.entity_b_name} size={b.entity_b_size} globalRef={b.entity_b_global_ref} onJump={onJump} />
                                </div>
                                <div className="flex items-center gap-2 mt-1.5 px-1">
                                    <KindBadge kind={b.kind} />
                                    {b.confidence_pct !== null && (
                                        <span className="text-[11px] text-gray-500 dark:text-gray-400">{b.confidence_pct.toFixed(0)}% confidence</span>
                                    )}
                                    {b.decision && b.kind === 'SAME_SEGMENT' && (
                                        <span className="text-[11px] text-gray-400">{b.decision}</span>
                                    )}
                                    {b.evidence && b.evidence.length > 0 && (
                                        <span className="text-[11px] text-gray-400 truncate">
                                            via {b.evidence.map((e) => e.field).join(", ")}
                                        </span>
                                    )}
                                </div>
                            </div>
                        ))}
                    </div>
                )}
                <div className="flex justify-between items-center mt-3 pt-3 border-t border-gray-200 dark:border-gray-700 text-xs">
                    <button onClick={() => onPageChange(Math.max(1, page - 1))} disabled={page <= 1} className="btn btn-ghost !py-1 !px-2 disabled:opacity-30">Previous</button>
                    <span className="text-gray-400">Page {page} of {totalPages.toLocaleString()}</span>
                    <button onClick={() => onPageChange(Math.min(totalPages, page + 1))} disabled={page >= totalPages} className="btn btn-ghost !py-1 !px-2 disabled:opacity-30">Next</button>
                </div>
            </div>
        </div>
    );
}

export function ClusterBridgesSection({
    bridges, loading, onJump,
}: {
    bridges: {
        target_entity_id: string;
        target_representative_name: string | null;
        global_ref: string | null;
        member_count: number | null;
        connecting_pairs: {
            code_in_cluster_name: string; code_in_target_name: string;
            confidence_pct: number | null; kind: string; decision: string | null;
        }[];
    }[] | null;
    loading: boolean;
    onJump: (entityId: string) => void;
}) {
    if (loading) return <p className="text-[11px] text-gray-400">Checking for external connections...</p>;
    if (!bridges || bridges.length === 0) {
        return <p className="text-[11px] text-gray-400">No connections to other clusters found.</p>;
    }
    return (
        <div className="space-y-1.5">
            {bridges.map((b) => (
                <button
                    key={b.target_entity_id}
                    onClick={() => onJump(b.target_entity_id)}
                    className="w-full text-left p-1.5 rounded bg-gray-50 dark:bg-gray-900/40 hover:bg-gray-100 dark:hover:bg-gray-900/70 transition-colors text-xs"
                >
                    <div className="flex items-center justify-between gap-2">
                        <span className="text-gray-700 dark:text-gray-300 truncate">{b.target_representative_name || b.target_entity_id.slice(0, 8)}</span>
                        <KindBadge kind={b.connecting_pairs[0]?.kind || "SAME_SEGMENT"} />
                    </div>
                    <div className="text-[10px] text-gray-400 mt-0.5">
                        {b.member_count ?? "?"} members
                        {b.connecting_pairs[0]?.confidence_pct != null && ` · ${b.connecting_pairs[0].confidence_pct.toFixed(0)}% match`}
                        {b.connecting_pairs.length > 1 && ` · ${b.connecting_pairs.length} connections`}
                    </div>
                </button>
            ))}
        </div>
    );
}
