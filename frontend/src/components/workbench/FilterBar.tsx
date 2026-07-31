"use client";

import { Search as SearchIcon } from "lucide-react";

export type RecordType = "ALL" | "COMPANY" | "INDIVIDUAL";

const RECORD_TYPE_OPTIONS: { value: RecordType; label: string }[] = [
    { value: "ALL", label: "All" },
    { value: "COMPANY", label: "Companies" },
    { value: "INDIVIDUAL", label: "Individuals" },
];

function Chip({ active, onClick, children }: { active: boolean; onClick: () => void; children: React.ReactNode }) {
    return (
        <button
            onClick={onClick}
            className={`px-3 py-1.5 rounded-lg text-sm transition-colors ${
                active
                    ? "bg-blue-600 text-white shadow-sm"
                    : "bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-200 dark:hover:bg-gray-600"
            }`}
        >
            {children}
        </button>
    );
}

export interface PairFilters {
    q: string;
    recordType: RecordType;
    minConf?: number;
    maxConf?: number;
    hasVeto?: boolean;
}

export interface EntityFilters {
    q: string;
    recordType: RecordType;
    hasGlobalRef?: boolean;
}

export function FilterBar(
    props:
        | { mode: "pairs"; value: PairFilters; onChange: (v: PairFilters) => void }
        | { mode: "entities"; value: EntityFilters; onChange: (v: EntityFilters) => void }
) {
    const { mode, value, onChange } = props;

    return (
        <div className="flex flex-wrap items-center gap-3 bg-white dark:bg-gray-800/50 border border-gray-200 dark:border-gray-700 rounded-xl p-3">
            <div className="relative flex-1 min-w-[200px]">
                <SearchIcon size={14} className="absolute left-2.5 top-1/2 -translate-y-1/2 text-gray-400" />
                <input
                    value={value.q}
                    onChange={(e) => onChange({ ...value, q: e.target.value } as any)}
                    placeholder={mode === "pairs" ? "Search name, ID, or code..." : "Search name, ID, code, or Global ID..."}
                    className="w-full text-sm py-1.5"
                    style={{ paddingLeft: "1.75rem" }}
                />
            </div>

            <div className="flex gap-1.5">
                {RECORD_TYPE_OPTIONS.map((opt) => (
                    <Chip key={opt.value} active={value.recordType === opt.value} onClick={() => onChange({ ...value, recordType: opt.value } as any)}>
                        {opt.label}
                    </Chip>
                ))}
            </div>

            {mode === "pairs" && (
                <>
                    <label className="flex items-center gap-1.5 text-xs text-gray-500 dark:text-gray-400 cursor-pointer select-none">
                        <input
                            type="checkbox"
                            className="w-3.5 h-3.5"
                            checked={(value as PairFilters).hasVeto === true}
                            onChange={(e) => onChange({ ...value, hasVeto: e.target.checked ? true : undefined } as any)}
                        />
                        Vetoed only
                    </label>
                    <div className="flex items-center gap-1.5 text-xs text-gray-500 dark:text-gray-400">
                        <span>Score</span>
                        <input
                            type="number" min={0} max={100} placeholder="0"
                            className="w-14 text-xs py-1 px-1.5 text-center"
                            value={(value as PairFilters).minConf ?? ''}
                            onChange={(e) => {
                                const n = e.target.value === '' ? undefined : Math.max(0, Math.min(100, Number(e.target.value)));
                                onChange({ ...value, minConf: n } as any);
                            }}
                            title="Only show pairs at or above this score (e.g. 80 for 'above 80%')"
                        />
                        <span>to</span>
                        <input
                            type="number" min={0} max={100} placeholder="100"
                            className="w-14 text-xs py-1 px-1.5 text-center"
                            value={(value as PairFilters).maxConf ?? ''}
                            onChange={(e) => {
                                const n = e.target.value === '' ? undefined : Math.max(0, Math.min(100, Number(e.target.value)));
                                onChange({ ...value, maxConf: n } as any);
                            }}
                            title="Only show pairs at or below this score (e.g. 70 for 'below 70%')"
                        />
                        <span>%</span>
                    </div>
                </>
            )}

            {mode === "entities" && (
                <label className="flex items-center gap-1.5 text-xs text-gray-500 dark:text-gray-400 cursor-pointer select-none">
                    <input
                        type="checkbox"
                        className="w-3.5 h-3.5"
                        checked={(value as EntityFilters).hasGlobalRef === true}
                        onChange={(e) => onChange({ ...value, hasGlobalRef: e.target.checked ? true : undefined } as any)}
                    />
                    Has Global ID
                </label>
            )}
        </div>
    );
}
