"use client";

import { Fragment, useState } from "react";
import { AlertTriangle, CheckCircle2, XCircle, ChevronDown, ChevronRight, Table2 } from "lucide-react";

interface BlockingVerdict {
    would_explode: boolean;
    severity: "SAFE" | "WARN" | "BLOCK";
    n_distinct_values: number;
    n_pairs_if_blocked: number;
    largest_group: number;
    message: string;
}

interface FieldProfile {
    name: string;
    parquet_type: string;
    is_array: boolean;
    fill_rate: number;
    distinct_count: number;
    avg_length: number | null;
    top_values: { value: string; count: number }[];
    semantic_type: string;
    semantic_label: string;
    blocking_safe: boolean;
    default_comparators: string[];
    blocking_verdict: BlockingVerdict;
    good_for: string[];
}

function fmtNum(n: number): string {
    if (n >= 1_000_000_000) return `${(n / 1_000_000_000).toFixed(1)}B`;
    if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
    if (n >= 1_000) return `${(n / 1_000).toFixed(1)}K`;
    return String(n);
}

function VerdictBadge({ v }: { v: BlockingVerdict }) {
    const styles: Record<string, { cls: string; icon: React.ReactNode }> = {
        SAFE: { cls: "badge-success", icon: <CheckCircle2 size={12} /> },
        WARN: { cls: "badge-warning", icon: <AlertTriangle size={12} /> },
        BLOCK: { cls: "badge-danger", icon: <XCircle size={12} /> },
    };
    const s = styles[v.severity] || styles.SAFE;
    return (
        <span className={`badge ${s.cls} gap-1`} title={v.message}>
            {s.icon} {v.severity === "BLOCK" ? "Would explode" : v.severity === "WARN" ? "Risky alone" : "Safe alone"} · {fmtNum(v.n_pairs_if_blocked)} pairs
        </span>
    );
}

export function FieldsPanel({ fields, loading, error }: { fields: FieldProfile[] | undefined; loading: boolean; error: string | null }) {
    const [expanded, setExpanded] = useState<string | null>(null);

    return (
        <div>
            <p className="text-sm text-gray-500 dark:text-gray-400 mb-4">
                Every column actually present in the source Parquet dataset, profiled directly -- not a hardcoded
                list. The pair count next to each field is the EXACT number of candidate pairs blocking on that raw
                value alone would produce (same math the live precheck below uses), so you can see at a glance which
                fields are safe to block on and which would explode before you ever touch a rule.
            </p>

            {loading && <p className="text-sm text-gray-400">Profiling source columns...</p>}
            {error && <p className="text-sm text-red-500">{error}</p>}

            {fields && (
                <div className="overflow-x-auto">
                    <table className="w-full text-xs">
                        <thead>
                            <tr className="text-left text-gray-400 border-b border-gray-200 dark:border-gray-700">
                                <th className="py-2 pr-3 font-medium">Field</th>
                                <th className="py-2 pr-3 font-medium">Type</th>
                                <th className="py-2 pr-3 font-medium">Fill</th>
                                <th className="py-2 pr-3 font-medium">Distinct</th>
                                <th className="py-2 pr-3 font-medium">Blocking verdict</th>
                                <th className="py-2 pr-3 font-medium">Good for</th>
                                <th className="py-2 font-medium"></th>
                            </tr>
                        </thead>
                        <tbody>
                            {fields.map((f) => {
                                const isOpen = expanded === f.name;
                                return (
                                    <Fragment key={f.name}>
                                        <tr
                                            key={f.name}
                                            className="border-b border-gray-100 dark:border-gray-800 hover:bg-gray-50 dark:hover:bg-gray-900/40 cursor-pointer"
                                            onClick={() => setExpanded(isOpen ? null : f.name)}
                                        >
                                            <td className="py-2 pr-3 font-mono font-semibold text-gray-900 dark:text-white">
                                                {f.name}
                                                {f.is_array && <span className="ml-1 text-gray-400 font-normal">[]</span>}
                                            </td>
                                            <td className="py-2 pr-3">
                                                <span className="badge badge-info !text-[10px]">{f.semantic_label}</span>
                                            </td>
                                            <td className="py-2 pr-3 text-gray-600 dark:text-gray-300">{Math.round(f.fill_rate * 100)}%</td>
                                            <td className="py-2 pr-3 text-gray-600 dark:text-gray-300">{fmtNum(f.distinct_count)}</td>
                                            <td className="py-2 pr-3"><VerdictBadge v={f.blocking_verdict} /></td>
                                            <td className="py-2 pr-3 text-gray-500">
                                                {f.good_for.filter((g) => g !== "would_explode").join(", ") || "—"}
                                            </td>
                                            <td className="py-2 text-gray-400">
                                                {isOpen ? <ChevronDown size={14} /> : <ChevronRight size={14} />}
                                            </td>
                                        </tr>
                                        {isOpen && (
                                            <tr key={`${f.name}-detail`} className="bg-gray-50 dark:bg-gray-900/40">
                                                <td colSpan={7} className="p-3">
                                                    <div className="grid grid-cols-2 gap-4 text-xs">
                                                        <div>
                                                            <div className="text-gray-500 mb-1">Explosion check</div>
                                                            <p className="text-gray-700 dark:text-gray-300">{f.blocking_verdict.message}</p>
                                                            <p className="text-gray-400 mt-1">
                                                                Largest group sharing one value: {f.blocking_verdict.largest_group.toLocaleString()} records
                                                            </p>
                                                        </div>
                                                        <div>
                                                            <div className="text-gray-500 mb-1">Sample values</div>
                                                            <div className="flex flex-wrap gap-1">
                                                                {f.top_values.map((v, i) => (
                                                                    <span key={i} className="font-mono px-1.5 py-0.5 rounded bg-gray-200 dark:bg-gray-800 text-gray-600 dark:text-gray-300">
                                                                        {v.value} ({v.count})
                                                                    </span>
                                                                ))}
                                                            </div>
                                                        </div>
                                                    </div>
                                                </td>
                                            </tr>
                                        )}
                                    </Fragment>
                                );
                            })}
                        </tbody>
                    </table>
                </div>
            )}
        </div>
    );
}
