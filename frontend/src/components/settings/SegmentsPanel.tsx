"use client";

import { useState } from "react";
import { Building2, User, Plus, X, Link2 } from "lucide-react";
import type { SegmentationConfig, SegmentCounts } from "./types";

function fmtNum(n: number): string {
    if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
    if (n >= 1_000) return `${(n / 1_000).toFixed(1)}K`;
    return String(n);
}

export function SegmentsPanel({
    config,
    onChange,
    stats,
    statsLoading,
    relationshipCount,
}: {
    config: SegmentationConfig;
    onChange: (next: SegmentationConfig) => void;
    stats: SegmentCounts | null;
    statsLoading: boolean;
    relationshipCount: number | null;
}) {
    const [newKeyword, setNewKeyword] = useState("");

    const addKeyword = () => {
        const kw = newKeyword.trim().toUpperCase();
        if (!kw || config.company_keywords.includes(kw)) return;
        onChange({ ...config, company_keywords: [...config.company_keywords, kw] });
        setNewKeyword("");
    };
    const removeKeyword = (kw: string) => {
        onChange({ ...config, company_keywords: config.company_keywords.filter((k) => k !== kw) });
    };

    const companyCount = stats?.segment_counts?.COMPANY ?? 0;
    const individualCount = stats?.segment_counts?.INDIVIDUAL ?? 0;
    const total = stats?.total ?? 0;
    const companyPct = total > 0 ? Math.round((companyCount / total) * 100) : 0;

    return (
        <div>
            <p className="text-sm text-gray-500 dark:text-gray-400 mb-4">
                Keep companies and people apart so two accounts never get merged into one identity by mistake.
                When on, a business and a person are matched against their own rules and are never auto-linked
                into each other -- but if they DO share something (a phone number, a document, an address), that
                connection is kept and shown below, not thrown away.
            </p>

            <label className="flex items-center gap-3 mb-4 cursor-pointer select-none">
                <input
                    type="checkbox"
                    checked={config.enabled}
                    onChange={(e) => onChange({ ...config, enabled: e.target.checked })}
                    className="w-4 h-4"
                />
                <span className="text-sm font-medium text-gray-900 dark:text-white">
                    Separate companies from individuals
                </span>
            </label>

            {config.enabled && (
                <div className="space-y-4">
                    <div>
                        <p className="text-xs font-medium text-gray-500 dark:text-gray-400 mb-2">
                            A record is a Company if its name contains any of these words:
                        </p>
                        <div className="flex flex-wrap gap-1.5 mb-2">
                            {config.company_keywords.map((kw) => (
                                <span
                                    key={kw}
                                    className="inline-flex items-center gap-1 px-2 py-1 rounded-full text-[11px] font-mono bg-indigo-100 dark:bg-indigo-500/20 text-indigo-700 dark:text-indigo-300"
                                >
                                    {kw}
                                    <button onClick={() => removeKeyword(kw)} className="hover:text-red-500">
                                        <X size={11} />
                                    </button>
                                </span>
                            ))}
                        </div>
                        <div className="flex items-center gap-2">
                            <input
                                value={newKeyword}
                                onChange={(e) => setNewKeyword(e.target.value)}
                                onKeyDown={(e) => e.key === "Enter" && (e.preventDefault(), addKeyword())}
                                placeholder="e.g. TRUST, FUND..."
                                className="text-xs py-1.5 flex-1"
                            />
                            <button onClick={addKeyword} className="btn btn-ghost gap-1 !py-1.5 !px-3 text-xs">
                                <Plus size={14} /> Add word
                            </button>
                        </div>
                        <p className="text-[11px] text-gray-400 mt-1">
                            Everything else is treated as an Individual. Not case-sensitive.
                        </p>
                    </div>

                    <div className="grid grid-cols-2 gap-3">
                        <div className="p-3 rounded-lg bg-blue-50 dark:bg-blue-900/20">
                            <div className="flex items-center gap-2 text-blue-700 dark:text-blue-300">
                                <Building2 size={16} />
                                <span className="text-xs font-medium">Companies</span>
                            </div>
                            <p className="text-xl font-bold text-blue-900 dark:text-blue-100 mt-1">
                                {statsLoading ? "…" : fmtNum(companyCount)}
                            </p>
                            <p className="text-[11px] text-blue-600/70 dark:text-blue-300/70">{companyPct}% of records</p>
                        </div>
                        <div className="p-3 rounded-lg bg-emerald-50 dark:bg-emerald-900/20">
                            <div className="flex items-center gap-2 text-emerald-700 dark:text-emerald-300">
                                <User size={16} />
                                <span className="text-xs font-medium">Individuals</span>
                            </div>
                            <p className="text-xl font-bold text-emerald-900 dark:text-emerald-100 mt-1">
                                {statsLoading ? "…" : fmtNum(individualCount)}
                            </p>
                            <p className="text-[11px] text-emerald-600/70 dark:text-emerald-300/70">{100 - companyPct}% of records</p>
                        </div>
                    </div>

                    {relationshipCount !== null && (
                        <div className="flex items-center gap-2 p-3 rounded-lg bg-amber-50 dark:bg-amber-900/20 text-amber-700 dark:text-amber-300 text-sm">
                            <Link2 size={16} />
                            <span>
                                <strong>{fmtNum(relationshipCount)}</strong> company &lt;-&gt; person connections found
                                and kept traceable (never merged) -- see Connections below.
                            </span>
                        </div>
                    )}
                </div>
            )}
        </div>
    );
}
