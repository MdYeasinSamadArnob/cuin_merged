"use client";

import { CheckCircle2, RotateCcw } from "lucide-react";

interface VersionRow {
    policy_version: number;
    policy_hash: string;
    created_by: string;
    created_at: string;
    approved_by: string | null;
    approved_at: string | null;
    is_active: boolean;
}

export function VersionHistory({ versions, onActivate }: { versions: VersionRow[]; onActivate: (v: number) => void }) {
    if (versions.length === 0) {
        return <p className="text-xs text-gray-400">No saved versions yet -- Save Changes below creates the first one.</p>;
    }
    return (
        <div className="space-y-2 max-h-64 overflow-y-auto">
            {versions.map((v) => (
                <div key={v.policy_version} className="flex items-center justify-between p-2.5 rounded-lg bg-gray-50 dark:bg-gray-900/50 text-xs">
                    <div className="min-w-0">
                        <div className="flex items-center gap-2">
                            <span className="font-mono font-medium text-gray-900 dark:text-white">v{v.policy_version}</span>
                            {v.is_active && <span className="badge badge-success gap-1"><CheckCircle2 size={10} /> active</span>}
                        </div>
                        <div className="text-gray-400 truncate">
                            {v.created_by} · {new Date(v.created_at).toLocaleString()}
                            {v.approved_by && ` · approved by ${v.approved_by}`}
                        </div>
                    </div>
                    {!v.is_active && (
                        <button onClick={() => onActivate(v.policy_version)} className="btn btn-ghost !py-1 !px-2 gap-1 shrink-0">
                            <RotateCcw size={12} /> Activate
                        </button>
                    )}
                </div>
            ))}
        </div>
    );
}
