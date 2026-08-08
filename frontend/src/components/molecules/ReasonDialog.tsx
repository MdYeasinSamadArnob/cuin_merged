"use client";

import { useState } from "react";
import { X } from "lucide-react";
import { useAuthStore } from "@/stores/useAuthStore";

export interface ReasonDialogResult {
    reasonCode: string;
    reason: string;
}

const REASON_CODES: Record<string, string[]> = {
    approve: ["CONFIRMED_MATCH", "VERIFIED_BY_DOCUMENT", "VERIFIED_BY_PHONE", "OTHER"],
    reject: ["DIFFERENT_PEOPLE", "COINCIDENTAL_MATCH", "DATA_ERROR", "OTHER"],
    merge: ["SAME_PERSON_VERIFIED", "CONFIRMED_BY_OFFICER", "OTHER"],
    split: ["INCORRECT_LINK", "DIFFERENT_PERSON", "DATA_ERROR", "OTHER"],
    "undo-merge": ["INCORRECT_MERGE", "OFFICER_ERROR", "OTHER"],
    "revert-global-ref": ["INCORRECT_ASSIGNMENT", "OFFICER_ERROR", "OTHER"],
    "revoke-override": ["INCORRECT_DECISION", "OFFICER_ERROR", "OTHER"],
};

export function ReasonDialog({
    title, action, onConfirm, onCancel,
}: {
    title: string;
    action: "approve" | "reject" | "merge" | "split" | "undo-merge" | "revert-global-ref" | "revoke-override";
    onConfirm: (result: ReasonDialogResult) => void;
    onCancel: () => void;
}) {
    const [reasonCode, setReasonCode] = useState(REASON_CODES[action][0]);
    const [reason, setReason] = useState("");
    const currentUser = useAuthStore((s) => s.user);

    const canConfirm = reason.trim().length >= 5;

    const confirm = () => {
        if (!canConfirm) return;
        onConfirm({ reasonCode, reason: reason.trim() });
    };

    return (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4" onClick={onCancel}>
            {/* Deliberately solid, not .glass-card's translucent style -- this
                confirms a permanent, audit-logged decision, so the busy list/
                detail content behind it bleeding through at 20-30% opacity is
                the wrong look here even though it's right for a page card. */}
            <div
                className="w-full max-w-md p-6 bg-white dark:bg-gray-900 border border-gray-200 dark:border-gray-800 rounded-2xl shadow-xl"
                onClick={(e) => e.stopPropagation()}
            >
                <div className="flex items-center justify-between mb-4">
                    <h3 className="font-semibold text-gray-900 dark:text-white">{title}</h3>
                    <button onClick={onCancel} className="text-gray-400 hover:text-gray-700 dark:hover:text-gray-200">
                        <X size={18} />
                    </button>
                </div>
                <div className="space-y-3">
                    <div>
                        <label className="block text-xs text-gray-500 dark:text-gray-400 mb-1">Reason code</label>
                        <select value={reasonCode} onChange={(e) => setReasonCode(e.target.value)} className="w-full text-sm">
                            {REASON_CODES[action].map((c) => (
                                <option key={c} value={c}>{c.replace(/_/g, " ")}</option>
                            ))}
                        </select>
                    </div>
                    <div>
                        <label className="block text-xs text-gray-500 dark:text-gray-400 mb-1">Reason (min 5 characters, required)</label>
                        <textarea
                            value={reason} onChange={(e) => setReason(e.target.value)}
                            className="w-full text-sm h-20 resize-none"
                            placeholder="Why is this the correct decision? This is permanently recorded in the audit trail."
                        />
                    </div>
                    <p className="text-xs text-gray-400">
                        Acting as <span className="font-medium text-gray-600 dark:text-gray-300">{currentUser?.display_name || currentUser?.email}</span> -- recorded in the audit trail.
                    </p>
                </div>
                <div className="flex justify-end gap-2 mt-5">
                    <button onClick={onCancel} className="btn btn-ghost !py-1.5 !px-3 text-sm">Cancel</button>
                    <button onClick={confirm} disabled={!canConfirm} className="btn btn-primary !py-1.5 !px-3 text-sm disabled:opacity-40">Confirm</button>
                </div>
            </div>
        </div>
    );
}
