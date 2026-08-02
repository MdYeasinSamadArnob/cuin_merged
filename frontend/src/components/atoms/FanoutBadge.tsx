"use client";

import { AlertTriangle, CheckCircle2, XCircle, Loader2 } from "lucide-react";
import type { FanoutEstimate } from "@/types/settings";

function fmt(n: number): string {
    if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
    if (n >= 1_000) return `${(n / 1_000).toFixed(1)}K`;
    return String(n);
}

export function FanoutBadge({ estimate, loading }: { estimate: FanoutEstimate | null; loading: boolean }) {
    if (loading) {
        return (
            <span className="badge badge-neutral gap-1">
                <Loader2 size={12} className="animate-spin" /> checking fan-out...
            </span>
        );
    }
    if (!estimate) {
        return <span className="badge badge-neutral">not checked</span>;
    }

    const styles: Record<string, { cls: string; icon: React.ReactNode; text: string }> = {
        SAFE: { cls: "badge-success", icon: <CheckCircle2 size={12} />, text: "SAFE" },
        WARN: { cls: "badge-warning", icon: <AlertTriangle size={12} />, text: "WARN" },
        BLOCK: { cls: "badge-danger", icon: <XCircle size={12} />, text: "BLOCK" },
    };
    const s = styles[estimate.severity] || styles.SAFE;

    return (
        <span className={`badge ${s.cls} gap-1`} title={`${estimate.n_pairs.toLocaleString()} candidate pairs from ${estimate.n_keys.toLocaleString()} blocking keys (largest block: ${estimate.largest_block.toLocaleString()} records)`}>
            {s.icon} {s.text} · {fmt(estimate.n_pairs)} pairs
        </span>
    );
}

export function FanoutDetail({ estimate }: { estimate: FanoutEstimate }) {
    if (estimate.severity === "SAFE" && estimate.heaviest_keys.length === 0) return null;
    return (
        <div className="mt-3 p-3 rounded-lg bg-gray-50 dark:bg-gray-900/50 border border-gray-200 dark:border-gray-700 text-xs">
            <div className="flex justify-between text-gray-600 dark:text-gray-300 mb-2">
                <span>{estimate.n_keys.toLocaleString()} keys produce pairs · largest block {estimate.largest_block.toLocaleString()} records</span>
                {estimate.suggested_max_block_size != null && (
                    <span className="text-blue-600 dark:text-blue-400">
                        suggested max_block_size: {estimate.suggested_max_block_size}
                    </span>
                )}
            </div>
            {estimate.heaviest_keys.length > 0 && (
                <div className="max-h-40 overflow-y-auto space-y-1">
                    {estimate.heaviest_keys.slice(0, 10).map((h, i) => (
                        <div key={i} className="flex justify-between font-mono text-gray-500 dark:text-gray-400">
                            <span className="truncate max-w-[60%]" title={h.key}>{h.key}</span>
                            <span>{h.n_records.toLocaleString()} recs -&gt; {h.n_pairs.toLocaleString()} pairs</span>
                        </div>
                    ))}
                </div>
            )}
        </div>
    );
}
