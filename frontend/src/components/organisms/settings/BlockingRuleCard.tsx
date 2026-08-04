"use client";

import { useState } from "react";
import { ChevronUp, ChevronDown, Trash2, ChevronRight, GripVertical } from "lucide-react";
import { clsx } from "clsx";
import type { BlockingRule, BlockingRuleType, FanoutEstimate } from "@/types/settings";
import { RULE_TYPE_LABELS } from "@/types/settings";
import { FanoutBadge, FanoutDetail } from "@/components/atoms/FanoutBadge";

const ID_TYPE_LABELS: Record<string, string> = {
    mobile: "Phone number", email: "Email address", document: "National ID / document", address: "Address",
};
const ID_TYPES = Object.keys(ID_TYPE_LABELS);
const SCALAR_FIELDS = ["name_norm", "dob_iso", "name_key"];
const ARRAY_FIELDS = ["name_tokens", "address_tokens"];
const DATE_PARTS = ["year", "month", "day"];

interface Props {
    rule: BlockingRule;
    estimate: FanoutEstimate | null;
    estimating: boolean;
    isFirst: boolean;
    isLast: boolean;
    onChange: (rule: BlockingRule) => void;
    onDelete: () => void;
    onMove: (direction: -1 | 1) => void;
}

export function BlockingRuleCard({ rule, estimate, estimating, isFirst, isLast, onChange, onDelete, onMove }: Props) {
    const [expanded, setExpanded] = useState(false);

    const set = (patch: Partial<BlockingRule>) => onChange({ ...rule, ...patch });
    const setGuard = (key: keyof BlockingRule["guards"], value: string) => {
        const n = value === "" ? null : Number(value);
        onChange({ ...rule, guards: { ...rule.guards, [key]: n } });
    };
    const setParam = (key: string, value: any) => onChange({ ...rule, params: { ...rule.params, [key]: value } });

    const blocked = estimate?.severity === "BLOCK";

    return (
        <div className={clsx(
            "rounded-lg border p-4 transition-colors",
            blocked ? "border-red-300 dark:border-red-700 bg-red-50/50 dark:bg-red-900/10" : "border-gray-200 dark:border-gray-700"
        )}>
            <div className="flex items-start gap-3">
                <div className="flex flex-col gap-0.5 pt-1 text-gray-400">
                    <button disabled={isFirst} onClick={() => onMove(-1)} className="disabled:opacity-20 hover:text-gray-700 dark:hover:text-gray-200">
                        <ChevronUp size={16} />
                    </button>
                    <GripVertical size={14} className="mx-auto opacity-40" />
                    <button disabled={isLast} onClick={() => onMove(1)} className="disabled:opacity-20 hover:text-gray-700 dark:hover:text-gray-200">
                        <ChevronDown size={16} />
                    </button>
                </div>

                <div className="flex-1 min-w-0">
                    <div className="flex items-center gap-2 flex-wrap">
                        <input
                            type="checkbox"
                            checked={rule.enabled}
                            onChange={(e) => set({ enabled: e.target.checked })}
                            className="w-4 h-4"
                        />
                        <input
                            value={rule.label}
                            onChange={(e) => set({ label: e.target.value })}
                            className="!bg-transparent !border-0 !p-0 font-medium text-gray-900 dark:text-white text-sm flex-1 min-w-[120px]"
                        />
                        <span className="badge badge-info shrink-0">{RULE_TYPE_LABELS[rule.type]}</span>
                        <span className="text-xs text-gray-400 font-mono shrink-0">order {rule.order}</span>
                        <FanoutBadge estimate={estimate} loading={estimating} />
                        <button onClick={onDelete} className="text-gray-400 hover:text-red-500 shrink-0" title="Delete rule">
                            <Trash2 size={15} />
                        </button>
                        <button onClick={() => setExpanded(!expanded)} className="text-gray-400 hover:text-gray-700 dark:hover:text-gray-200 shrink-0">
                            <ChevronRight size={16} className={clsx("transition-transform", expanded && "rotate-90")} />
                        </button>
                    </div>

                    {expanded && (
                        <div className="mt-3 pl-1 space-y-3 text-sm">
                            <FieldEditor rule={rule} onFieldsChange={(fields) => set({ fields })} onParamChange={setParam} />

                            <div>
                                <p className="text-[11px] text-gray-500 dark:text-gray-400 mb-2">
                                    Safety limits -- leave blank ("none") for no limit. A save is always blocked
                                    automatically if a rule would still generate too many pairs, so these are
                                    optional fine-tuning, not required for safety.
                                </p>
                                <div className="grid grid-cols-3 gap-3">
                                    <GuardInput
                                        label="Max key frequency"
                                        help="Skip a value shared by more than this many records -- e.g. ignore a phone number reused by 500 customers -- before any pairing happens."
                                        value={rule.guards.max_key_frequency} onChange={(v) => setGuard("max_key_frequency", v)}
                                    />
                                    <GuardInput
                                        label="Max block size"
                                        help="Skip a resulting match-group bigger than this many records. This is usually the one that matters most -- it's the direct cap on how many pairs one rule can produce."
                                        value={rule.guards.max_block_size} onChange={(v) => setGuard("max_block_size", v)}
                                    />
                                    <GuardInput
                                        label="Min key parts"
                                        help="Only for rules built from multiple pieces (like name + birth date) -- require at least this many pieces to match before grouping, so a weak partial match is ignored."
                                        value={rule.guards.min_key_parts} onChange={(v) => setGuard("min_key_parts", v)}
                                    />
                                </div>
                            </div>

                            <textarea
                                value={rule.description}
                                onChange={(e) => set({ description: e.target.value })}
                                placeholder="Why this rule exists (shown in the audit trail)..."
                                className="w-full text-xs h-14 resize-none"
                            />

                            {estimate && <FanoutDetail estimate={estimate} />}

                            {blocked && (
                                <div className="text-xs text-red-600 dark:text-red-400 font-medium">
                                    This rule would generate {estimate!.n_pairs.toLocaleString()} candidate pairs -- saving is disabled until it's guarded down
                                    {estimate!.suggested_max_block_size != null && ` (try max_block_size = ${estimate!.suggested_max_block_size})`}.
                                </div>
                            )}
                        </div>
                    )}
                </div>
            </div>
        </div>
    );
}

function GuardInput({ label, help, value, onChange }: { label: string; help: string; value: number | null; onChange: (v: string) => void }) {
    return (
        <div>
            <label className="block text-[11px] font-medium text-gray-600 dark:text-gray-300 mb-1">{label}</label>
            <input
                type="number"
                min={1}
                value={value ?? ""}
                placeholder="none"
                onChange={(e) => onChange(e.target.value)}
                className="w-full text-xs py-1.5"
            />
            <p className="text-[10px] text-gray-400 mt-1 leading-snug">{help}</p>
        </div>
    );
}

function FieldEditor({
    rule, onFieldsChange, onParamChange,
}: { rule: BlockingRule; onFieldsChange: (f: string[]) => void; onParamChange: (k: string, v: any) => void }) {
    switch (rule.type) {
        case "exact_identifier":
            return (
                <div>
                    <label className="block text-[11px] text-gray-500 dark:text-gray-400 mb-1">Records group together when they share the same...</label>
                    <div className="flex gap-3 flex-wrap">
                        {ID_TYPES.map((t) => (
                            <label key={t} className="flex items-center gap-1.5 text-xs text-gray-700 dark:text-gray-300">
                                <input
                                    type="checkbox"
                                    checked={rule.fields.includes(t)}
                                    onChange={(e) => {
                                        const next = e.target.checked ? [...rule.fields, t] : rule.fields.filter((f) => f !== t);
                                        onFieldsChange(next);
                                    }}
                                />
                                {ID_TYPE_LABELS[t]}
                            </label>
                        ))}
                    </div>
                </div>
            );

        case "raw_column":
            return (
                <div>
                    <label className="block text-[11px] text-gray-500 dark:text-gray-400 mb-1">
                        Records group together when they share the same value in this field
                    </label>
                    <div className="text-sm font-mono px-3 py-1.5 rounded-lg bg-gray-50 dark:bg-gray-900/50 text-gray-700 dark:text-gray-300 inline-block">
                        {rule.fields[0] || "(no field selected)"}
                    </div>
                    <p className="text-[11px] text-gray-400 mt-1">
                        Matched on the exact value (not fuzzy) -- to change the field, delete this rule and add a new one from the field list above.
                    </p>
                </div>
            );

        case "composite_key":
            return (
                <div className="grid grid-cols-2 gap-3">
                    <div>
                        <label className="block text-[11px] text-gray-500 dark:text-gray-400 mb-1">Field 1</label>
                        <select value={rule.fields[0] || ""} onChange={(e) => onFieldsChange([e.target.value, rule.fields[1] || "dob_iso"])} className="w-full text-xs py-1.5">
                            {[...SCALAR_FIELDS].map((f) => <option key={f} value={f}>{f}</option>)}
                        </select>
                    </div>
                    <div>
                        <label className="block text-[11px] text-gray-500 dark:text-gray-400 mb-1">Field 2</label>
                        <select value={rule.fields[1] || ""} onChange={(e) => onFieldsChange([rule.fields[0] || "name_key", e.target.value])} className="w-full text-xs py-1.5">
                            {[...SCALAR_FIELDS].map((f) => <option key={f} value={f}>{f}</option>)}
                        </select>
                    </div>
                    <div className="col-span-2">
                        <label className="flex items-center gap-1.5 text-xs text-gray-700 dark:text-gray-300">
                            <input
                                type="checkbox"
                                checked={rule.params.require_dob_precision === "FULL"}
                                onChange={(e) => onParamChange("require_dob_precision", e.target.checked ? "FULL" : undefined)}
                            />
                            Require FULL-precision DOB (not a year-only stub)
                        </label>
                    </div>
                </div>
            );

        case "token_key":
            return (
                <div>
                    <label className="block text-[11px] text-gray-500 dark:text-gray-400 mb-1">Array field to explode</label>
                    <select value={rule.fields[0] || ""} onChange={(e) => onFieldsChange([e.target.value])} className="w-full text-xs py-1.5">
                        {ARRAY_FIELDS.map((f) => <option key={f} value={f}>{f}</option>)}
                    </select>
                </div>
            );

        case "prefix_key":
            return (
                <div className="grid grid-cols-2 gap-3">
                    <div>
                        <label className="block text-[11px] text-gray-500 dark:text-gray-400 mb-1">Field</label>
                        <select value={rule.fields[0] || ""} onChange={(e) => onFieldsChange([e.target.value])} className="w-full text-xs py-1.5">
                            {SCALAR_FIELDS.filter((f) => f !== "name_key").map((f) => <option key={f} value={f}>{f}</option>)}
                        </select>
                    </div>
                    <div>
                        <label className="block text-[11px] text-gray-500 dark:text-gray-400 mb-1">Prefix length</label>
                        <input
                            type="number" min={1} max={32}
                            value={rule.params.prefix_len ?? 4}
                            onChange={(e) => onParamChange("prefix_len", Number(e.target.value))}
                            className="w-full text-xs py-1.5"
                        />
                    </div>
                </div>
            );

        case "date_part_key":
            return (
                <div className="grid grid-cols-2 gap-3">
                    <div>
                        <label className="block text-[11px] text-gray-500 dark:text-gray-400 mb-1">Date field</label>
                        <select value={rule.fields[0] || "dob_iso"} onChange={(e) => onFieldsChange([e.target.value, rule.fields[1] || "year"])} className="w-full text-xs py-1.5">
                            <option value="dob_iso">dob_iso</option>
                        </select>
                    </div>
                    <div>
                        <label className="block text-[11px] text-gray-500 dark:text-gray-400 mb-1">Part</label>
                        <select value={rule.fields[1] || "year"} onChange={(e) => onFieldsChange([rule.fields[0] || "dob_iso", e.target.value])} className="w-full text-xs py-1.5">
                            {DATE_PARTS.map((p) => <option key={p} value={p}>{p}</option>)}
                        </select>
                    </div>
                </div>
            );
    }
}
