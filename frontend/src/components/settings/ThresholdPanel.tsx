"use client";

import { Loader2, TrendingUp, TrendingDown, Minus } from "lucide-react";
import type { ScoringRules, DecisionCounts } from "./types";

interface Props {
    scoring: ScoringRules;
    onChange: (scoring: ScoringRules) => void;
    counts: DecisionCounts | null;
    baseline: DecisionCounts | null;
    delta: DecisionCounts | null;
    loading: boolean;
    elapsedMs: number | null;
}

function IntSlider({ label, value, min, max, onChange }: { label: string; value: number; min: number; max: number; onChange: (v: number) => void }) {
    return (
        <div>
            <div className="flex justify-between text-sm mb-1">
                <span className="text-gray-600 dark:text-gray-300">{label}</span>
                <span className="font-mono text-blue-600 dark:text-blue-400">{value}</span>
            </div>
            <input type="range" min={min} max={max} step={1} value={value} onChange={(e) => onChange(Number(e.target.value))} className="w-full accent-blue-500" />
        </div>
    );
}

function DeltaBadge({ label, count, delta }: { label: string; count: number; delta: number }) {
    const color = label === "AUTO_LINK" ? "text-emerald-600 dark:text-emerald-400" : label === "REVIEW" ? "text-amber-600 dark:text-amber-400" : "text-gray-500 dark:text-gray-400";
    return (
        <div className="flex-1 text-center p-3 rounded-lg bg-gray-50 dark:bg-gray-900/50">
            <div className={`text-2xl font-bold ${color}`}>{count.toLocaleString()}</div>
            <div className="text-xs text-gray-500 dark:text-gray-400 mt-1">{label}</div>
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

export function ThresholdPanel({ scoring, onChange, counts, baseline, delta, loading, elapsedMs }: Props) {
    const set = (patch: Partial<ScoringRules>) => onChange({ ...scoring, ...patch });

    return (
        <div className="space-y-6">
            <div className="space-y-4">
                <IntSlider label="AUTO_LINK: min strong identifiers alone" value={scoring.auto_link_min_strong_only} min={1} max={4} onChange={(v) => set({ auto_link_min_strong_only: v })} />
                <IntSlider label="AUTO_LINK: min strong + name corroboration" value={scoring.auto_link_min_strong_with_name} min={0} max={3} onChange={(v) => set({ auto_link_min_strong_with_name: v })} />
                <IntSlider label="REVIEW: min strong identifiers" value={scoring.review_min_strong} min={0} max={3} onChange={(v) => set({ review_min_strong: v })} />
                <IntSlider label="Cluster cohesion: max cluster size" value={scoring.max_cluster_size} min={2} max={50} onChange={(v) => set({ max_cluster_size: v })} />
                <div>
                    <div className="flex justify-between text-sm mb-1">
                        <span className="text-gray-600 dark:text-gray-300">Cluster cohesion: min density</span>
                        <span className="font-mono text-blue-600 dark:text-blue-400">{scoring.min_density.toFixed(2)}</span>
                    </div>
                    <input type="range" min={0.05} max={1} step={0.05} value={scoring.min_density} onChange={(e) => set({ min_density: Number(e.target.value) })} className="w-full accent-blue-500" />
                </div>
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
