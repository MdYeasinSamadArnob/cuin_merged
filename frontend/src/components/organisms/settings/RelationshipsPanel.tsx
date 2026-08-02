"use client";

import { useState } from "react";
import { Search, Building2, User, ArrowLeftRight } from "lucide-react";
import type { EntityRelationship } from "@/types/settings";

function PartyBadge({ party }: { party: EntityRelationship["a"] }) {
    const isCompany = party.segment === "COMPANY";
    return (
        <span className="inline-flex items-center gap-1.5">
            {isCompany ? (
                <Building2 size={13} className="text-blue-500 shrink-0" />
            ) : (
                <User size={13} className="text-emerald-500 shrink-0" />
            )}
            <span className="flex flex-col leading-tight">
                <span className="font-medium text-gray-900 dark:text-white text-xs">{party.name || "—"}</span>
                <span className="font-mono text-[10px] text-gray-400">{party.customer_code}</span>
            </span>
        </span>
    );
}

export function RelationshipsPanel({
    runId,
    relationships,
    total,
    loading,
    searchValue,
    onSearchChange,
}: {
    runId: string;
    relationships: EntityRelationship[];
    total: number;
    loading: boolean;
    searchValue: string;
    onSearchChange: (v: string) => void;
}) {
    return (
        <div>
            <p className="text-sm text-gray-500 dark:text-gray-400 mb-4">
                Every place a company and a person turned out to share a phone, document, email, or address --
                found by the exact same blocking rules as identity matching, but kept as a traceable link instead
                of being merged into one identity. Search a customer code to see everything connected to them.
            </p>

            <div className="relative mb-4">
                <Search size={14} className="absolute left-3 top-1/2 -translate-y-1/2 text-gray-400" />
                <input
                    value={searchValue}
                    onChange={(e) => onSearchChange(e.target.value)}
                    placeholder="Search by customer code..."
                    className="w-full text-sm"
                    style={{ paddingLeft: "2.25rem" }}
                />
            </div>

            {!runId && !searchValue && (
                <p className="text-xs text-gray-400">Select a run above, or search a customer code, to see connections.</p>
            )}

            {loading && <p className="text-xs text-gray-400">Loading connections...</p>}

            {!loading && (runId || searchValue) && relationships.length === 0 && (
                <p className="text-xs text-gray-400">No cross-segment connections found.</p>
            )}

            {relationships.length > 0 && (
                <div className="space-y-2 max-h-96 overflow-y-auto">
                    {relationships.map((rel) => (
                        <div
                            key={rel.relationship_id}
                            className="flex items-center gap-3 p-2.5 rounded-lg bg-gray-50 dark:bg-gray-900/40 text-xs"
                        >
                            <PartyBadge party={rel.a} />
                            <ArrowLeftRight size={12} className="text-gray-400 shrink-0" />
                            <PartyBadge party={rel.b} />
                            <div className="ml-auto flex flex-wrap gap-1 justify-end">
                                {rel.shared_evidence.map((ev, i) => (
                                    <span
                                        key={i}
                                        className="px-1.5 py-0.5 rounded bg-amber-100 dark:bg-amber-500/20 text-amber-700 dark:text-amber-300 font-mono text-[10px]"
                                        title={`Shared ${ev.field}`}
                                    >
                                        {ev.field}: {ev.value}
                                    </span>
                                ))}
                            </div>
                        </div>
                    ))}
                    {total > relationships.length && (
                        <p className="text-[11px] text-gray-400 pt-1">
                            +{total - relationships.length} more -- narrow the search to see more.
                        </p>
                    )}
                </div>
            )}
        </div>
    );
}
