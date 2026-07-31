"use client";

import { CheckCircle2, XCircle, ArrowRight, Building2, User } from "lucide-react";

interface WbRecord {
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

interface Contribution {
    attribute: string;
    matched: boolean;
    is_veto: boolean;
}

const FIELD_ROWS: { label: string; attribute: string; get: (r: WbRecord) => string | null }[] = [
    { label: "Name", attribute: "NAME", get: (r) => r.name_norm || null },
    { label: "Date of birth", attribute: "BIRTH_DATE", get: (r) => r.dob_iso || null },
    { label: "Mobile", attribute: "MOBILE", get: (r) => (r.identifiers?.mobile || []).join(", ") || null },
    { label: "Email", attribute: "EMAIL", get: (r) => (r.identifiers?.email || []).join(", ") || null },
    { label: "Document / NID", attribute: "DOCUMENT", get: (r) => (r.identifiers?.document || []).join(", ") || null },
    { label: "Address", attribute: "FULL_ADDRESS", get: (r) => (r.identifiers?.address || []).join(" | ") || null },
];

function fieldStatus(attribute: string, contributions: Contribution[]): "matched" | "veto" | "neutral" {
    const rows = contributions.filter((c) => c.attribute === attribute);
    if (rows.some((c) => c.is_veto && c.matched)) return "veto";
    if (rows.some((c) => c.matched)) return "matched";
    return "neutral";
}

function RecordHeader({ record, side, onViewEntity }: { record: WbRecord; side: "a" | "b"; onViewEntity?: (entityId: string) => void }) {
    const accent = side === "a" ? "text-blue-600 dark:text-blue-400" : "text-purple-600 dark:text-purple-400";
    const bg = side === "a" ? "bg-blue-50 dark:bg-blue-900/20" : "bg-purple-50 dark:bg-purple-900/20";
    const Icon = record.segment === "COMPANY" ? Building2 : User;
    return (
        <div className={`rounded-lg p-3 ${bg}`}>
            <div className="flex items-center justify-between">
                <span className={`text-xs font-semibold uppercase tracking-wide ${accent}`}>Record {side.toUpperCase()}</span>
                {record.segment && (
                    <span className="flex items-center gap-1 text-[10px] text-gray-500 dark:text-gray-400">
                        <Icon size={11} /> {record.segment}
                    </span>
                )}
            </div>
            <div className="font-semibold text-gray-900 dark:text-white mt-1">{record.name_norm || `Unknown (${record.customer_code})`}</div>
            <div className="text-[11px] font-mono text-gray-400">{record.customer_code}</div>
            {record.entity_id && (
                <button
                    onClick={() => onViewEntity?.(record.entity_id!)}
                    className="mt-1.5 flex items-center gap-1 text-[11px] text-emerald-600 dark:text-emerald-400 hover:underline"
                >
                    {record.global_ref ? `Part of entity · ${record.global_ref}` : "Part of an existing entity"} <ArrowRight size={10} />
                </button>
            )}
        </div>
    );
}

export function RecordCompare({
    recordA,
    recordB,
    contributions,
    onViewEntity,
}: {
    recordA: WbRecord | null;
    recordB: WbRecord | null;
    contributions: Contribution[];
    onViewEntity?: (entityId: string) => void;
}) {
    if (!recordA || !recordB) {
        return <p className="text-xs text-gray-400">Loading records...</p>;
    }
    if (!recordA.resolved || !recordB.resolved) {
        return <p className="text-xs text-amber-500">One or both records could not be found for this run.</p>;
    }

    return (
        <div className="space-y-3">
            <div className="grid grid-cols-2 gap-3">
                <RecordHeader record={recordA} side="a" onViewEntity={onViewEntity} />
                <RecordHeader record={recordB} side="b" onViewEntity={onViewEntity} />
            </div>
            <div className="space-y-1">
                {FIELD_ROWS.map((row) => {
                    const valA = row.get(recordA);
                    const valB = row.get(recordB);
                    if (!valA && !valB) return null;
                    const status = fieldStatus(row.attribute, contributions);
                    const rowBg =
                        status === "veto"
                            ? "bg-red-50 dark:bg-red-900/20"
                            : status === "matched"
                            ? "bg-emerald-50 dark:bg-emerald-900/10"
                            : "bg-gray-50 dark:bg-gray-900/30";
                    return (
                        <div key={row.attribute} className={`grid grid-cols-[80px_1fr_auto_1fr] items-center gap-2 p-2 rounded-lg text-xs ${rowBg}`}>
                            <span className="text-gray-500 dark:text-gray-400">{row.label}</span>
                            <span className="text-gray-900 dark:text-white truncate" title={valA || undefined}>{valA || "—"}</span>
                            {status === "veto" ? (
                                <XCircle size={13} className="text-red-500" />
                            ) : status === "matched" ? (
                                <CheckCircle2 size={13} className="text-emerald-500" />
                            ) : (
                                <span className="w-[13px]" />
                            )}
                            <span className="text-gray-900 dark:text-white truncate" title={valB || undefined}>{valB || "—"}</span>
                        </div>
                    );
                })}
            </div>
        </div>
    );
}
