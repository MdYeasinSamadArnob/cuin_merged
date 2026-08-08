"use client";

import { ShieldAlert, CheckCircle2, Circle, ArrowRight } from "lucide-react";

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

interface FieldEvidence {
    field_name: string;
    value_a: string | null;
    value_b: string | null;
    comparison_type: string;
    similarity_score: number;
    match_weight: number;
    explanation: string;
}

interface Breakdown {
    a_key: string;
    b_key: string;
    confidence_pct: number;
    has_veto: boolean;
    decision: string;
    contributions: Contribution[];
    field_evidence?: FieldEvidence[];
}

const DECISION_COLOR: Record<string, string> = {
    AUTO_LINK: "text-emerald-600 dark:text-emerald-400",
    REVIEW: "text-amber-600 dark:text-amber-400",
    REJECT: "text-gray-500 dark:text-gray-400",
};

// Same tier names the Workbench tabs already use (Strong Match/Potential
// Match/System Rejected) -- this badge showed the raw pipeline code
// ("AUTO LINK") instead of matching that language everywhere else.
const DECISION_LABEL: Record<string, string> = {
    AUTO_LINK: "Strong Match",
    REVIEW: "Potential Match",
    REJECT: "System Rejected",
};

// Maps a pair_contributions RULE's `attribute` (what the audited score was
// computed against) to the matching pair_identifier_evidence/
// pair_name_dob_evidence FIELD name (what the raw comparison was computed
// against) -- two different tables, same underlying pipeline run, joined
// here purely for display. RAW_COLUMN (bank-specific custom-field rules)
// has no fixed field_evidence counterpart and is intentionally omitted.
const ATTRIBUTE_TO_FIELD: Record<string, string> = {
    DOCUMENT: "document", MOBILE: "mobile", EMAIL: "email",
    NAME: "name", BIRTH_DATE: "dob", FULL_ADDRESS: "address",
};

export function ScoreBreakdown({ breakdown, loading }: { breakdown: Breakdown | null; loading: boolean }) {
    if (loading) return <p className="text-xs text-gray-400">Loading breakdown...</p>;
    if (!breakdown) return <p className="text-xs text-gray-400">Select a pair to see its score breakdown.</p>;

    const rawSum = breakdown.contributions.reduce((s, c) => s + c.awarded_pct, 0);
    const wasCapped = rawSum > breakdown.confidence_pct + 0.01;
    const evidenceByField: Record<string, FieldEvidence> = {};
    (breakdown.field_evidence || []).forEach((fe) => { evidenceByField[fe.field_name] = fe; });

    return (
        <div className="space-y-3">
            <div className="flex items-center justify-between">
                <div className="text-2xl font-bold text-gray-900 dark:text-white">{breakdown.confidence_pct.toFixed(0)}%</div>
                <span className={`badge ${DECISION_COLOR[breakdown.decision]} font-semibold`}>{DECISION_LABEL[breakdown.decision] || breakdown.decision.replace("_", " ")}</span>
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
                {breakdown.contributions.map((c) => {
                    const fe = evidenceByField[ATTRIBUTE_TO_FIELD[c.attribute]];
                    const isFuzzyName = c.attribute === "NAME" && fe;
                    return (
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

                            {/* Raw field comparison -- informational only, computed by the
                                SAME pipeline run from pair_identifier_evidence/
                                pair_name_dob_evidence, never a substitute for the audited
                                +N% / 0% score above. A rule can require an exact/strict match
                                (0% awarded) while the raw values are still highly similar --
                                e.g. "SYED KAMRUL HASSAN" vs "SYED KAMRUL HASSAN AND" is a 75%
                                token-Jaccard overlap even though the exact-match name rule
                                didn't fire -- both numbers are real and worth seeing together. */}
                            {fe && (fe.value_a || fe.value_b) && (
                                <div className="mt-1.5 pt-1.5 border-t border-gray-200/70 dark:border-gray-700/60 space-y-1">
                                    <div className="flex items-center gap-1.5 font-mono text-gray-600 dark:text-gray-300 min-w-0">
                                        <span className="truncate">{fe.value_a || "—"}</span>
                                        <ArrowRight size={9} className="text-gray-400 shrink-0" />
                                        <span className="truncate">{fe.value_b || "—"}</span>
                                    </div>
                                    <div className="flex items-center justify-between text-[10px] text-gray-400">
                                        <span>
                                            {isFuzzyName ? "Fuzzy name similarity" : "Raw similarity"}:{" "}
                                            <span className={`font-mono ${isFuzzyName ? "text-blue-600 dark:text-blue-400 font-semibold" : "text-gray-600 dark:text-gray-300"}`}>
                                                {(fe.similarity_score * 100).toFixed(0)}%
                                            </span>
                                        </span>
                                        <span className="font-mono" title="Comparison algorithm used to compute the raw similarity above">{fe.comparison_type}</span>
                                    </div>
                                </div>
                            )}
                        </div>
                    );
                })}
            </div>
        </div>
    );
}
