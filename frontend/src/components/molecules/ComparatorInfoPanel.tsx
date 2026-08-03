"use client";

import { CheckCircle2, Clock, XCircle } from "lucide-react";

interface Comparator {
    id: string;
    label: string;
    description: string;
    tier: "A" | "B";
    implemented: boolean;
    applicable_semantic_types: string[];
    unavailable_reason: string | null;
}

interface ExcludedComparator {
    id: string;
    label: string;
    reason: string;
}

export function ComparatorInfoPanel({
    comparators,
    excluded,
}: {
    comparators: Comparator[] | undefined;
    excluded: ExcludedComparator[] | undefined;
}) {
    if (!comparators) return null;

    const available = comparators.filter((c) => c.implemented);
    const comingSoon = comparators.filter((c) => !c.implemented);

    return (
        <div className="space-y-4 text-xs">
            <p className="text-gray-500 dark:text-gray-400">
                Only comparators proven to behave identically in Python and compiled Doris SQL are offered -- a rule
                that matches differently depending which path evaluates it would be worse than not offering it.
                Excluded options are shown below with the reason, not hidden.
            </p>

            <div>
                <div className="text-gray-500 mb-2 font-medium">Available now</div>
                <div className="space-y-2">
                    {available.map((c) => (
                        <div key={c.id} className="flex items-start gap-2 p-2 rounded-lg bg-gray-50 dark:bg-gray-900/40">
                            <CheckCircle2 size={14} className="text-emerald-500 mt-0.5 shrink-0" />
                            <div>
                                <div className="font-semibold text-gray-900 dark:text-white">{c.label}</div>
                                <div className="text-gray-500 dark:text-gray-400">{c.description}</div>
                            </div>
                        </div>
                    ))}
                </div>
            </div>

            {comingSoon.length > 0 && (
                <div>
                    <div className="text-gray-500 mb-2 font-medium">Planned (not yet wired in)</div>
                    <div className="space-y-2">
                        {comingSoon.map((c) => (
                            <div key={c.id} className="flex items-start gap-2 p-2 rounded-lg bg-gray-50 dark:bg-gray-900/40 opacity-60">
                                <Clock size={14} className="text-amber-500 mt-0.5 shrink-0" />
                                <div>
                                    <div className="font-semibold text-gray-900 dark:text-white">{c.label}</div>
                                    <div className="text-gray-500 dark:text-gray-400">{c.description}</div>
                                    {c.unavailable_reason && (
                                        <div className="text-gray-400 italic mt-0.5">{c.unavailable_reason}</div>
                                    )}
                                </div>
                            </div>
                        ))}
                    </div>
                </div>
            )}

            {excluded && excluded.length > 0 && (
                <div>
                    <div className="text-gray-500 mb-2 font-medium">Not offered</div>
                    <div className="space-y-2">
                        {excluded.map((c) => (
                            <div key={c.id} className="flex items-start gap-2 p-2 rounded-lg bg-red-50 dark:bg-red-900/10 opacity-75">
                                <XCircle size={14} className="text-red-400 mt-0.5 shrink-0" />
                                <div>
                                    <div className="font-semibold text-gray-700 dark:text-gray-300">{c.label}</div>
                                    <div className="text-gray-500 dark:text-gray-400">{c.reason}</div>
                                </div>
                            </div>
                        ))}
                    </div>
                </div>
            )}
        </div>
    );
}
