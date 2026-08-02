"use client";

import { useMemo } from "react";
import { Loader2, TrendingUp, TrendingDown, Minus, Zap, X, ShieldAlert } from "lucide-react";
import type { MatchRuleset, MatchRule, DecisionCounts } from "@/types/settings";
import { ATTRIBUTE_RAW_COLUMN, vetoKindForComparator } from "@/types/settings";

interface Props {
    ruleset: MatchRuleset;
    onChange: (ruleset: MatchRuleset) => void;
    counts: DecisionCounts | null;
    baseline: DecisionCounts | null;
    delta: DecisionCounts | null;
    loading: boolean;
    elapsedMs: number | null;
    onDeleteRule?: (ruleId: string) => void;
}

// Tailwind needs full class names present verbatim in source to
// generate them -- a template-literal `text-${accent}-600` would be
// silently dropped from the production build, so this is a lookup
// table, not string interpolation.
const ACCENT_CLASSES: Record<string, { text: string; range: string }> = {
    blue: { text: "text-blue-600 dark:text-blue-400", range: "accent-blue-500" },
    red: { text: "text-red-600 dark:text-red-400", range: "accent-red-500" },
    emerald: { text: "text-emerald-600 dark:text-emerald-400", range: "accent-emerald-500" },
    amber: { text: "text-amber-600 dark:text-amber-400", range: "accent-amber-500" },
};

function PercentSlider({
    label, sublabel, value, onChange, accent = "blue",
}: { label: string; sublabel?: string; value: number; onChange: (v: number) => void; accent?: string }) {
    const cls = ACCENT_CLASSES[accent] || ACCENT_CLASSES.blue;
    return (
        <div>
            <div className="flex justify-between text-sm mb-1">
                <span className="text-gray-700 dark:text-gray-300">{label}</span>
                <span className={`font-mono ${cls.text}`}>{value.toFixed(0)}%</span>
            </div>
            {sublabel && <p className="text-[11px] text-gray-400 mb-1.5">{sublabel}</p>}
            <input
                type="range" min={0} max={100} step={5} value={value}
                onChange={(e) => onChange(Number(e.target.value))}
                className={`w-full ${cls.range}`}
            />
        </div>
    );
}

function DeltaBadge({ label, count, delta }: { label: string; count: number; delta: number }) {
    const color = label === "AUTO_LINK" ? "text-emerald-600 dark:text-emerald-400" : label === "REVIEW" ? "text-amber-600 dark:text-amber-400" : "text-gray-500 dark:text-gray-400";
    return (
        <div className="flex-1 text-center p-3 rounded-lg bg-gray-50 dark:bg-gray-900/50">
            <div className={`text-2xl font-bold ${color}`}>{count.toLocaleString()}</div>
            <div className="text-xs text-gray-500 dark:text-gray-400 mt-1">{label.replace("_", " ")}</div>
            {delta !== 0 && (
                <div className={`text-xs mt-1 flex items-center justify-center gap-1 ${delta > 0 ? "text-emerald-500" : "text-red-500"}`}>
                    {delta > 0 ? <TrendingUp size={12} /> : <TrendingDown size={12} />}
                    {delta > 0 ? "+" : ""}{delta.toLocaleString()}
                </div>
            )}
            {delta === 0 && <div className="text-xs mt-1 flex items-center justify-center gap-1 text-gray-400"><Minus size={12} /> no change</div>}
        </div>
    );
}

/** A worked example: the two highest-weighted enabled, non-veto rules combined -- shows how confidence actually adds up. */
function WorkedExample({ ruleset }: { ruleset: MatchRuleset }) {
    const contenders = [...ruleset.match_rules]
        .filter((r) => r.enabled && r.confidence_pct > 0)
        .sort((a, b) => b.confidence_pct - a.confidence_pct);

    if (contenders.length === 0) return null;

    const [first, second] = contenders;
    const combined = second ? Math.min(ruleset.confidence_cap, first.confidence_pct + second.confidence_pct) : first.confidence_pct;
    const decision = combined >= ruleset.auto_link_min_confidence ? "AUTO-LINK" : combined >= ruleset.review_min_confidence ? "REVIEW" : "REJECT";
    const decisionColor = decision === "AUTO-LINK" ? "text-emerald-600 dark:text-emerald-400" : decision === "REVIEW" ? "text-amber-600 dark:text-amber-400" : "text-gray-500";

    return (
        <div className="p-3 rounded-lg bg-blue-50 dark:bg-blue-900/20 text-xs flex items-center gap-2">
            <Zap size={14} className="text-blue-500 shrink-0" />
            <span className="text-gray-700 dark:text-gray-300">
                Example: {first.label || first.attribute} ({first.confidence_pct.toFixed(0)}%)
                {second && ` + ${second.label || second.attribute} (${second.confidence_pct.toFixed(0)}%)`}
                {" = "}
                <span className="font-semibold">{combined.toFixed(0)}%</span>
                {" -> "}
                <span className={`font-semibold ${decisionColor}`}>{decision}</span>
            </span>
        </div>
    );
}

export function ConfidencePanel({ ruleset, onChange, counts, baseline, delta, loading, elapsedMs, onDeleteRule }: Props) {
    const setRule = (ruleId: string, patch: Partial<MatchRule>) => {
        onChange({
            ...ruleset,
            match_rules: ruleset.match_rules.map((r) => (r.rule_id === ruleId ? { ...r, ...patch } : r)),
        });
    };
    const setThreshold = (patch: Partial<MatchRuleset>) => onChange({ ...ruleset, ...patch });
    const toggleVeto = (rule: MatchRule, on: boolean) => {
        setRule(rule.rule_id, { veto_kind: on ? vetoKindForComparator(rule.comparator) : null });
    };

    return (
        <div className="space-y-6">
            <div>
                <h4 className="text-xs font-semibold text-gray-500 dark:text-gray-400 uppercase tracking-wider mb-3">
                    How much does each match count?
                </h4>
                <div className="space-y-4">
                    {ruleset.match_rules.map((rule) => {
                        const isCustom = rule.attribute === ATTRIBUTE_RAW_COLUMN;
                        return (
                            <div key={rule.rule_id} className="flex items-start gap-3">
                                <input
                                    type="checkbox" checked={rule.enabled}
                                    onChange={(e) => setRule(rule.rule_id, { enabled: e.target.checked })}
                                    className="mt-2"
                                />
                                <div className="flex-1">
                                    <PercentSlider
                                        label={rule.label || rule.attribute}
                                        sublabel={
                                            isCustom
                                                ? `Custom field: ${rule.params.column}`
                                                : undefined
                                        }
                                        value={rule.confidence_pct}
                                        onChange={(v) => setRule(rule.rule_id, { confidence_pct: v })}
                                        accent={rule.veto_kind ? "red" : "blue"}
                                    />
                                    <label className="flex items-center gap-1.5 mt-1.5 text-[11px] text-gray-500 dark:text-gray-400 cursor-pointer select-none">
                                        <input
                                            type="checkbox" checked={!!rule.veto_kind}
                                            onChange={(e) => toggleVeto(rule, e.target.checked)}
                                            className="w-3 h-3"
                                        />
                                        <ShieldAlert size={12} className={rule.veto_kind ? "text-red-500" : "text-gray-400"} />
                                        If both sides have this and it clearly doesn't match, always reject -- no matter what else matches
                                    </label>
                                </div>
                                {isCustom && onDeleteRule && (
                                    <button
                                        onClick={() => onDeleteRule(rule.rule_id)}
                                        className="mt-1.5 text-gray-400 hover:text-red-500"
                                        title="Remove this field from matching"
                                    >
                                        <X size={14} />
                                    </button>
                                )}
                            </div>
                        );
                    })}
                </div>
                <div className="mt-3">
                    <WorkedExample ruleset={ruleset} />
                </div>
            </div>

            <div className="border-t border-gray-200 dark:border-gray-700 pt-4 space-y-4">
                <h4 className="text-xs font-semibold text-gray-500 dark:text-gray-400 uppercase tracking-wider">
                    What confidence means what?
                </h4>
                <PercentSlider
                    label="Auto-link automatically at"
                    sublabel="Pairs at or above this confidence are merged with no human review."
                    value={ruleset.auto_link_min_confidence}
                    onChange={(v) => setThreshold({ auto_link_min_confidence: v })}
                    accent="emerald"
                />
                <PercentSlider
                    label="Send to human review at"
                    sublabel="Pairs at or above this (but below auto-link) go to a reviewer. Below this, pairs are rejected."
                    value={ruleset.review_min_confidence}
                    onChange={(v) => setThreshold({ review_min_confidence: v })}
                    accent="amber"
                />
            </div>

            <div className="border-t border-gray-200 dark:border-gray-700 pt-4">
                <div className="flex items-center justify-between mb-3">
                    <h4 className="text-xs font-semibold text-gray-500 dark:text-gray-400 uppercase tracking-wider">Live decision impact</h4>
                    {loading ? (
                        <span className="text-xs text-gray-400 flex items-center gap-1"><Loader2 size={12} className="animate-spin" /> recomputing...</span>
                    ) : elapsedMs != null ? (
                        <span className="text-xs text-gray-400">{elapsedMs}ms</span>
                    ) : null}
                </div>
                {counts ? (
                    <div className="flex gap-3">
                        <DeltaBadge label="AUTO_LINK" count={counts.AUTO_LINK || 0} delta={delta?.AUTO_LINK || 0} />
                        <DeltaBadge label="REVIEW" count={counts.REVIEW || 0} delta={delta?.REVIEW || 0} />
                        <DeltaBadge label="REJECT" count={counts.REJECT || 0} delta={delta?.REJECT || 0} />
                    </div>
                ) : (
                    <p className="text-xs text-gray-400">Select a run above to see live decision counts as you move these sliders.</p>
                )}
                {baseline && (
                    <p className="text-[11px] text-gray-400 mt-2">
                        Baseline (as the run originally decided): {baseline.AUTO_LINK || 0} auto-link, {baseline.REVIEW || 0} review, {baseline.REJECT || 0} reject.
                    </p>
                )}
            </div>
        </div>
    );
}
