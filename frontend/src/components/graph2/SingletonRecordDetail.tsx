"use client";

import { Building2, User as UserIcon, Waypoints, IdCard } from "lucide-react";

interface SingletonRecord {
    customer_code: string;
    resolved: boolean;
    name_norm?: string | null;
    dob_iso?: string | null;
    dob_precision?: string | null;
    identifiers?: Record<string, string[]>;
    segment?: string | null;
    entity_id?: string | null;
    global_ref?: string | null;
    global_ref_state?: string | null;
}

const FIELD_ROWS: { label: string; get: (r: SingletonRecord) => string | null }[] = [
    { label: "Date of birth", get: (r) => r.dob_iso || null },
    { label: "Mobile", get: (r) => (r.identifiers?.mobile || []).join(", ") || null },
    { label: "Email", get: (r) => (r.identifiers?.email || []).join(", ") || null },
    { label: "Document / NID", get: (r) => (r.identifiers?.document || []).join(", ") || null },
    { label: "Address", get: (r) => (r.identifiers?.address || []).join(" | ") || null },
];

export function SingletonRecordDetail({
    data, loading, onExploreFrom,
}: {
    data: SingletonRecord | null;
    loading: boolean;
    onExploreFrom: (customerCode: string, label: string) => void;
}) {
    if (loading) return <p className="text-xs text-gray-400">Loading record...</p>;
    if (!data) return <p className="text-xs text-gray-400">Click a bubble to see that record's full details.</p>;
    if (!data.resolved) return <p className="text-xs text-amber-500">This record could not be found for the selected run.</p>;

    const Icon = data.segment === "COMPANY" ? Building2 : UserIcon;

    return (
        <div className="space-y-3">
            <div className="flex items-center justify-between">
                <span className="font-mono text-xs text-gray-500">{data.customer_code}</span>
                <span className="badge badge-warning !text-[10px]">Singleton -- no candidate match</span>
            </div>
            <div className="rounded-lg p-3 bg-gray-50 dark:bg-gray-900/40">
                <div className="flex items-center justify-between">
                    <span className="font-semibold text-gray-900 dark:text-white">{data.name_norm || `Unknown (${data.customer_code})`}</span>
                    {data.segment && (
                        <span className="flex items-center gap-1 text-[10px] text-gray-500 dark:text-gray-400">
                            <Icon size={11} /> {data.segment}
                        </span>
                    )}
                </div>
                {data.global_ref && (
                    <div className="flex items-center gap-1.5 mt-1.5 text-xs text-emerald-600 dark:text-emerald-400">
                        <IdCard size={12} /> {data.global_ref} <span className="text-gray-400">({data.global_ref_state})</span>
                    </div>
                )}
            </div>

            <div className="space-y-1">
                {FIELD_ROWS.map((row) => {
                    const val = row.get(data);
                    if (!val) return null;
                    return (
                        <div key={row.label} className="grid grid-cols-[100px_1fr] items-center gap-2 p-2 rounded-lg text-xs bg-gray-50 dark:bg-gray-900/30">
                            <span className="text-gray-500 dark:text-gray-400">{row.label}</span>
                            <span className="text-gray-900 dark:text-white truncate" title={val}>{val}</span>
                        </div>
                    );
                })}
            </div>

            <button
                onClick={() => onExploreFrom(data.customer_code, data.name_norm || data.customer_code)}
                className="flex items-center gap-1.5 text-xs text-blue-600 dark:text-blue-400 hover:underline"
            >
                <Waypoints size={12} /> Explore relationships from this record
            </button>
        </div>
    );
}
