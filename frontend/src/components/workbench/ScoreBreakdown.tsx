"use client";

import { ShieldAlert, CheckCircle2, Circle } from "lucide-react";

interface Contribution {
    rule_id: string;
    ordinal: number;
    label: string;
    attribute: string;
    sub_type: string | null;
    matched: boolean;
    awarded_pct: number;
    configured_pct: number;
    is_veto: boolean;
    detail: string | null;
}

interface Breakdown {
    a_key: string;
    b_key: string;
    confidence_pct: number;
    has_veto: boolean;
    decision: string;
    contributions: Contribution[];
}

const DECISION_COLOR: Record<string, string> = {
    AUTO_LINK: "text-emerald-600 dark:text-emerald-400",
    REVIEW: "text-amber-600 dark:text-amber-400",
    REJECT: "text-gray-500 dark:text-gray-400",
};

export function ScoreBreakdown({ breakdown, loading }: { breakdown: Breakdown | null; loading: boolean }) {
    if (loading) return <p className="text-xs text-gray-400">Loading breakdown...</p>;
    if (!breakdown) return <p className="text-xs text-gray-400">Select a pair to see its score breakdown.</p>;

    const rawSum = breakdown.contributions.reduce((s, c) => s + c.awarded_pct, 0);
    const wasCapped = rawSum > breakdown.confidence_pct + 0.01;

    return (
        <div className="space-y-3">
            <div className="flex items-center justify-between">
                <div className="text-2xl font-bold text-gray-900 dark:text-white">{breakdown.confidence_pct.toFixed(0)}%</div>
                <span className={`badge ${DECISION_COLOR[breakdown.decision]} font-semibold`}>{breakdown.decision.replace("_", " ")}</span>
            </div>
            {wasCapped && (
                <p className="text-[11px] text-amber-600 dark:text-amber-400">
                    Raw sum {rawSum.toFixed(0)}% capped at {breakdown.confidence_pct.toFixed(0)}%.
                </p>
            )}
            {breakdown.has_veto && (
                <div className="flex items-center gap-2 p-2 rounded-lg bg-red-50 dark:bg-red-900/20 text-red-700 dark:text-red-300 text-xs">
                    <ShieldAlert size={14} /> A rule vetoed this pair -- forced REJECT regardless of score.
                </div>
            )}
            <div className="space-y-2">
                {breakdown.contributions.map((c) => (
                    <div key={c.rule_id} className={`p-2 rounded-lg text-xs ${c.is_veto ? "bg-red-50 dark:bg-red-900/20" : c.matched ? "bg-emerald-50 dark:bg-emerald-900/10" : "bg-gray-50 dark:bg-gray-900/40"}`}>
                        <div className="flex items-center justify-between">
                            <div className="flex items-center gap-1.5">
                                {c.is_veto ? <ShieldAlert size={12} className="text-red-500" /> : c.matched ? <CheckCircle2 size={12} className="text-emerald-500" /> : <Circle size={12} className="text-gray-300" />}
                                <span className={c.matched ? "text-gray-900 dark:text-white font-medium" : "text-gray-400"}>{c.label}</span>
                            </div>
                            <span className={c.matched ? "font-mono text-emerald-600 dark:text-emerald-400" : "font-mono text-gray-400"}>
                                {c.matched ? `+${c.awarded_pct.toFixed(0)}%` : "0%"}
                                {c.matched && c.awarded_pct !== c.configured_pct && (
                                    <span className="text-gray-400"> ({c.awarded_pct / c.configured_pct}x{c.configured_pct.toFixed(0)}%)</span>
                                )}
                            </span>
                        </div>
                        {c.detail && <p className="text-gray-500 dark:text-gray-400 mt-1 font-mono">{c.detail}</p>}
                        {c.is_veto && <p className="text-red-600 dark:text-red-400 mt-1">This rule's mismatch forces REJECT.</p>}
                    </div>
                ))}
            </div>
        </div>
    );
}
