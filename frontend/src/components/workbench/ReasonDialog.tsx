"use client";

import { useState } from "react";
import { X } from "lucide-react";

export interface ReasonDialogResult {
    reasonCode: string;
    reason: string;
    actor: string;
}

const REASON_CODES: Record<string, string[]> = {
    approve: ["CONFIRMED_MATCH", "VERIFIED_BY_DOCUMENT", "VERIFIED_BY_PHONE", "OTHER"],
    reject: ["DIFFERENT_PEOPLE", "COINCIDENTAL_MATCH", "DATA_ERROR", "OTHER"],
    merge: ["SAME_PERSON_VERIFIED", "CONFIRMED_BY_OFFICER", "OTHER"],
    split: ["INCORRECT_LINK", "DIFFERENT_PERSON", "DATA_ERROR", "OTHER"],
};

export function ReasonDialog({
    title, action, onConfirm, onCancel,
}: {
    title: string;
    action: "approve" | "reject" | "merge" | "split";
    onConfirm: (result: ReasonDialogResult) => void;
    onCancel: () => void;
}) {
    const [reasonCode, setReasonCode] = useState(REASON_CODES[action][0]);
    const [reason, setReason] = useState("");
    const [actor, setActor] = useState(() => (typeof window !== "undefined" ? localStorage.getItem("wb_actor") || "" : ""));

    const canConfirm = reason.trim().length >= 5 && actor.trim().length >= 2;

    const confirm = () => {
        if (!canConfirm) return;
        if (typeof window !== "undefined") localStorage.setItem("wb_actor", actor.trim());
        onConfirm({ reasonCode, reason: reason.trim(), actor: actor.trim() });
    };

    return (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4" onClick={onCancel}>
            <div className="glass-card w-full max-w-md p-6" onClick={(e) => e.stopPropagation()}>
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
                    <div>
                        <label className="block text-xs text-gray-500 dark:text-gray-400 mb-1">Your name / ID (required)</label>
                        <input value={actor} onChange={(e) => setActor(e.target.value)} className="w-full text-sm" placeholder="e.g. officer.rahman" />
                    </div>
                </div>
                <div className="flex justify-end gap-2 mt-5">
                    <button onClick={onCancel} className="btn btn-ghost !py-1.5 !px-3 text-sm">Cancel</button>
                    <button onClick={confirm} disabled={!canConfirm} className="btn btn-primary !py-1.5 !px-3 text-sm disabled:opacity-40">Confirm</button>
                </div>
            </div>
        </div>
    );
}
