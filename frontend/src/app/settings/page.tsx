"use client";

// Settings, unified. Used to be split across two pages: a plain
// /settings (Blocking Strategy + Matching Weights sliders) and a much
// larger /settings/rules (the real rule-catalog editor: fields,
// blocking rules, match confidence, segmentation, connections,
// comparators, version history). Merged into one page per an explicit
// request after tracing that the OLD /settings sliders were dead --
// they wrote to the now-removed routes_config.py in-memory
// _config_store, which only the legacy ad-hoc Excel/CSV upload path
// ever read. The real Datasource pipeline (pipeline/doris_orchestrator.py)
// reads exclusively from engine.rules.store.get_active_catalog() --
// i.e. everything below under "Blocking Rules" / "Matching & Confidence".
//
// Tabbed instead of one long stack of 8 cards, so getting to "Blocking
// Rules" doesn't require scrolling past Fields/Search/Segments/
// Comparators/History first. Save & Activate stays in the persistent
// header (outside the tabs) since it's one save across the whole
// catalog regardless of which tab is active.

import { useEffect, useMemo, useState } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { motion, AnimatePresence } from "framer-motion";
import {
    Plus, Save, RefreshCw, Sliders, ListTree, History, Search as SearchIcon, AlertCircle,
    Table2, GitCompareArrows, Users, Link2, RotateCcw,
} from "lucide-react";
import { api } from "@/lib/api";
import { BlockingRuleCard } from "@/components/organisms/settings/BlockingRuleCard";
import { ConfidencePanel } from "@/components/organisms/settings/ConfidencePanel";
import { VersionHistory } from "@/components/molecules/VersionHistory";
import { FieldsPanel } from "@/components/organisms/settings/FieldsPanel";
import { ComparatorInfoPanel } from "@/components/molecules/ComparatorInfoPanel";
import { SegmentsPanel } from "@/components/organisms/settings/SegmentsPanel";
import { RelationshipsPanel } from "@/components/organisms/settings/RelationshipsPanel";
import type { BlockingRule, MatchRuleset, FanoutEstimate, DecisionCounts, SegmentationConfig } from "@/types/settings";
import { defaultRuleForField, defaultMatchRuleForField } from "@/types/settings";

function useDebounced<T>(value: T, delayMs: number): T {
    const [debounced, setDebounced] = useState(value);
    useEffect(() => {
        const t = setTimeout(() => setDebounced(value), delayMs);
        return () => clearTimeout(t);
    }, [value, delayMs]);
    return debounced;
}

const TABS = [
    { id: "fields", label: "Fields", icon: Table2 },
    { id: "blocking", label: "Blocking Rules", icon: ListTree },
    { id: "matching", label: "Matching & Confidence", icon: Sliders },
    { id: "segments", label: "Segments & Connections", icon: Users },
    { id: "reference", label: "Reference & History", icon: History },
] as const;
type TabId = typeof TABS[number]["id"];

export default function SettingsPage() {
    const queryClient = useQueryClient();
    const [activeTab, setActiveTab] = useState<TabId>("fields");

    const { data: runsData } = useQuery({
        queryKey: ["runs-for-rules"],
        queryFn: () => api.listRuns(1, 50),
    });
    const completedRuns = useMemo(
        () => (runsData?.runs || []).filter((r: any) => r.status === "COMPLETED"),
        [runsData]
    );
    const [runId, setRunId] = useState<string>("");
    useEffect(() => {
        if (!runId && completedRuns.length > 0) setRunId(completedRuns[0].run_id);
    }, [completedRuns, runId]);

    const { data: catalog, isLoading: catalogLoading } = useQuery({
        queryKey: ["active-rules"],
        queryFn: () => api.getActiveRules(),
    });

    const [blockingRules, setBlockingRules] = useState<BlockingRule[]>([]);
    const [matchRuleset, setMatchRuleset] = useState<MatchRuleset | null>(null);
    const [segmentation, setSegmentation] = useState<SegmentationConfig | null>(null);
    const [dirty, setDirty] = useState(false);

    useEffect(() => {
        if (catalog && !dirty) {
            setBlockingRules(catalog.blocking_rules);
            setMatchRuleset(catalog.match_ruleset);
            setSegmentation(catalog.segmentation);
        }
    }, [catalog, dirty]);

    // ---- Segments: live split + connection count for the selected run ----
    const { data: segmentStats, isFetching: segmentStatsLoading } = useQuery({
        queryKey: ["segment-stats", runId],
        queryFn: () => api.getSegmentStats(runId),
        enabled: !!runId && !!segmentation?.enabled,
        retry: false,
    });

    const { data: relationshipsData, isFetching: relationshipsLoading } = useQuery({
        queryKey: ["relationships", runId],
        queryFn: () => api.listRelationships({ runId, limit: 20 }),
        enabled: !!runId && !!segmentation?.enabled,
        retry: false,
    });

    // ---- Connections: separate customer-code search, independent of the run picker ----
    const [connectionSearch, setConnectionSearch] = useState("");
    const debouncedConnectionSearch = useDebounced(connectionSearch, 400);
    const { data: connectionSearchData, isFetching: connectionSearchLoading } = useQuery({
        queryKey: ["relationships-search", debouncedConnectionSearch],
        queryFn: () => api.listRelationships({ customerCode: debouncedConnectionSearch, limit: 20 }),
        enabled: debouncedConnectionSearch.length >= 2,
        retry: false,
    });

    const shownRelationships = debouncedConnectionSearch.length >= 2 ? connectionSearchData : relationshipsData;

    const { data: versionsData, refetch: refetchVersions } = useQuery({
        queryKey: ["rule-versions"],
        queryFn: () => api.listRuleVersions(50),
    });

    const { data: schemaData, isLoading: schemaLoading, error: schemaError } = useQuery({
        queryKey: ["datasource-schema"],
        queryFn: () => api.getDatasourceSchema(),
        staleTime: 5 * 60 * 1000,
    });

    const { data: comparatorsData } = useQuery({
        queryKey: ["comparators"],
        queryFn: () => api.getComparators(),
        staleTime: 5 * 60 * 1000,
    });

    // ---- Live fan-out precheck (debounced on blockingRules + runId) ----
    const debouncedBlockingRules = useDebounced(blockingRules, 450);
    const [estimates, setEstimates] = useState<FanoutEstimate[]>([]);
    const [precheckLoading, setPrecheckLoading] = useState(false);
    const [precheckError, setPrecheckError] = useState<string | null>(null);

    useEffect(() => {
        if (!runId || debouncedBlockingRules.length === 0) return;
        let cancelled = false;
        setPrecheckLoading(true);
        setPrecheckError(null);
        api.precheckRules(runId, debouncedBlockingRules)
            .then((res: any) => {
                if (!cancelled) setEstimates(res.estimates || []);
            })
            .catch((e: any) => {
                if (!cancelled) setPrecheckError(String(e.message || e));
            })
            .finally(() => {
                if (!cancelled) setPrecheckLoading(false);
            });
        return () => { cancelled = true; };
    }, [debouncedBlockingRules, runId]);

    const anyBlocked = estimates.some((e) => e.severity === "BLOCK");

    // ---- Instant redecide (debounced on matchRuleset + runId) ----
    const debouncedMatchRuleset = useDebounced(matchRuleset, 350);
    const [decisionCounts, setDecisionCounts] = useState<DecisionCounts | null>(null);
    const [baselineCounts, setBaselineCounts] = useState<DecisionCounts | null>(null);
    const [decisionDelta, setDecisionDelta] = useState<DecisionCounts | null>(null);
    const [redecideLoading, setRedecideLoading] = useState(false);
    const [redecideElapsed, setRedecideElapsed] = useState<number | null>(null);

    useEffect(() => {
        if (!runId || !debouncedMatchRuleset) return;
        let cancelled = false;
        setRedecideLoading(true);
        api.redecide(runId, debouncedMatchRuleset)
            .then((res: any) => {
                if (cancelled) return;
                setDecisionCounts(res.decision_counts);
                setBaselineCounts(res.baseline_decision_counts);
                setDecisionDelta(res.delta);
                setRedecideElapsed(res.elapsed_ms);
            })
            .catch(() => { /* run may have no persisted evidence -- non-fatal for the editor */ })
            .finally(() => { if (!cancelled) setRedecideLoading(false); });
        return () => { cancelled = true; };
    }, [debouncedMatchRuleset, runId]);

    // ---- Save ----
    const [saving, setSaving] = useState(false);
    const [saveMessage, setSaveMessage] = useState<string | null>(null);

    const handleSave = async () => {
        if (!matchRuleset || anyBlocked) return;
        try {
            setSaving(true);
            await api.saveRules(blockingRules, matchRuleset, "ui", true, segmentation ?? undefined);
            setDirty(false);
            setSaveMessage("Saved and activated as a new version.");
            queryClient.invalidateQueries({ queryKey: ["active-rules"] });
            refetchVersions();
        } catch (e: any) {
            setSaveMessage(`Save failed: ${e.message || e}`);
        } finally {
            setSaving(false);
            setTimeout(() => setSaveMessage(null), 4000);
        }
    };

    const handleActivateVersion = async (version: number) => {
        await api.activateRuleVersion(version, "ui");
        setDirty(false);
        queryClient.invalidateQueries({ queryKey: ["active-rules"] });
        refetchVersions();
    };

    // ---- Rule list mutators ----
    const updateRule = (index: number, next: BlockingRule) => {
        setDirty(true);
        setBlockingRules((prev) => prev.map((r, i) => (i === index ? next : r)));
    };
    const deleteRule = (index: number) => {
        setDirty(true);
        setBlockingRules((prev) => prev.filter((_, i) => i !== index));
    };
    const moveRule = (index: number, dir: -1 | 1) => {
        setDirty(true);
        setBlockingRules((prev) => {
            const arr = [...prev];
            const j = index + dir;
            if (j < 0 || j >= arr.length) return prev;
            [arr[index], arr[j]] = [arr[j], arr[index]];
            return arr.map((r, i) => ({ ...r, order: i + 1 }));
        });
    };
    // Fields already covered by an existing blocking rule, so the field
    // picker only ever offers what isn't already in use.
    const usedFieldNames = useMemo(() => {
        const idTypeToColumn: Record<string, string> = { mobile: "MOBILE", email: "EMAIL", document: "DOCUMENT" };
        const names = new Set<string>();
        for (const r of blockingRules) {
            if (r.type === "exact_identifier") {
                for (const idType of r.fields) if (idTypeToColumn[idType]) names.add(idTypeToColumn[idType]);
            } else if (r.type === "raw_column") {
                names.add(r.fields[0]);
            }
        }
        return names;
    }, [blockingRules]);

    const [addingField, setAddingField] = useState<string>("");
    const availableFields = (schemaData?.fields || []).filter((f: any) => f.name !== "CUSTOMER_CODE" && !usedFieldNames.has(f.name));

    // Fields already covered by an existing MATCH rule -- built-ins map
    // to their source column directly; custom (RAW_COLUMN) rules carry
    // it in params.column. A separate set from blocking's, since a
    // field can be used for blocking, matching, both, or neither.
    const usedMatchFieldNames = useMemo(() => {
        const attrToColumn: Record<string, string> = {
            MOBILE: "MOBILE", EMAIL: "EMAIL", FULL_ADDRESS: "FULL_ADDRESS",
            NAME: "NAME", BIRTH_DATE: "BIRTH_DATE", DOCUMENT: "DOCUMENT",
        };
        const names = new Set<string>();
        for (const r of matchRuleset?.match_rules || []) {
            if (r.attribute === "RAW_COLUMN") names.add(r.params.column);
            else if (attrToColumn[r.attribute]) names.add(attrToColumn[r.attribute]);
        }
        return names;
    }, [matchRuleset]);
    const [addingMatchField, setAddingMatchField] = useState<string>("");
    const availableMatchFields = (schemaData?.fields || []).filter(
        (f: any) => f.name !== "CUSTOMER_CODE" && !usedMatchFieldNames.has(f.name) && (f.default_comparators || []).length > 0
    );
    const addMatchRule = () => {
        const field = availableMatchFields.find((f: any) => f.name === addingMatchField) || availableMatchFields[0];
        if (!field || !matchRuleset) return;
        setDirty(true);
        setMatchRuleset({ ...matchRuleset, match_rules: [...matchRuleset.match_rules, defaultMatchRuleForField(field)] });
        setAddingMatchField("");
    };
    const deleteMatchRule = (ruleId: string) => {
        if (!matchRuleset) return;
        setDirty(true);
        setMatchRuleset({ ...matchRuleset, match_rules: matchRuleset.match_rules.filter((r) => r.rule_id !== ruleId) });
    };

    // ---- Reset to defaults: loads the seeded config into the editor
    // (still requires an explicit Save & Activate) -- recovery path if
    // a banker's edits go sideways, per an explicit design requirement.
    const [resetting, setResetting] = useState(false);
    const handleResetToDefaults = async () => {
        if (!confirm("Load the seeded default rules into the editor? This replaces everything below -- you still need to click Save & Activate to apply it.")) return;
        setResetting(true);
        try {
            const defaults = await api.getDefaultRules();
            setBlockingRules(defaults.blocking_rules);
            setMatchRuleset(defaults.match_ruleset);
            setSegmentation(defaults.segmentation);
            setDirty(true);
        } finally {
            setResetting(false);
        }
    };

    const addRule = () => {
        const field = availableFields.find((f: any) => f.name === addingField) || availableFields[0];
        if (!field) return;
        setDirty(true);
        setBlockingRules((prev) => [...prev, defaultRuleForField(field, prev.length + 1)]);
        setAddingField("");
    };

    // ---- Fuzzy(-ish) address blocking: a TOKEN_KEY rule against
    // customer_scalars.address_tokens (word-tokenized address, common
    // structural words like RD/ST/APT/directionals excluded server-side
    // -- see engine/normalize/explode_dialect.py). TOKEN_KEY rules have
    // no generic "pick a field" creation path above (that dropdown only
    // ever builds RAW_COLUMN rules from the discovered source schema;
    // address_tokens is a derived pipeline column, not a source
    // column), so this is a dedicated one-off "add" action rather than
    // a dropdown entry.
    const hasAddressTokenRule = blockingRules.some((r) => r.type === "token_key" && r.fields[0] === "address_tokens");
    const addAddressTokenRule = () => {
        setDirty(true);
        setBlockingRules((prev) => [...prev, {
            rule_id: `rule_${Date.now().toString(36)}`,
            type: "token_key",
            enabled: true,
            order: prev.length + 1,
            label: "Fuzzy address match (shared word)",
            fields: ["address_tokens"],
            params: {},
            // Deliberately tight. Verified live against a real 1.5M-row
            // dataset: address components have a MUCH fatter frequency
            // distribution than name tokens (a country has a few
            // thousand distinct area/city names, not tens of thousands
            // of distinct surnames) -- "DHAKA" alone appeared on
            // ~144,000 records. A guard that looks generous for
            // name-token blocking (e.g. 2000) is catastrophic here: it
            // took Doris to an 85GB OOM and killed the query. At
            // max_key_frequency=20, estimated join output is ~5.6M rows
            // (empirically measured); a banker who wants more recall
            // can raise this, but should watch pipeline duration/memory
            // closely when doing so.
            guards: { max_key_frequency: 20, max_block_size: null, min_key_parts: null },
            description: (
                "Self-joins on any shared word from a customer's address(es) -- " +
                "house number, street name, area/city. Common structural words " +
                "(RD/ST/APT/BLK and bare directionals) are excluded before this " +
                "rule ever sees them, so a match here means a real distinguishing " +
                "word was shared. Broader recall than exact address match: catches " +
                "a typo/abbreviation difference in ONE word as long as another word " +
                "still matches exactly -- it is not similarity-scored, just token overlap."
            ),
        }]);
    };

    // ---- Search ----
    const [searchQuery, setSearchQuery] = useState("");
    const debouncedSearch = useDebounced(searchQuery, 400);
    const { data: searchResults, isFetching: searching } = useQuery({
        queryKey: ["settings-search", debouncedSearch, runId],
        queryFn: () => api.search(debouncedSearch, { runId, pageSize: 8 }),
        enabled: debouncedSearch.length >= 2,
    });

    if (catalogLoading) {
        return <div className="p-8 text-gray-900 dark:text-white">Loading rule catalog...</div>;
    }

    return (
        <div className="space-y-6">
            {/* Persistent header -- title, run picker, reset/save. Stays
                visible regardless of which tab is open since Save &
                Activate applies to the whole catalog at once. */}
            <div className="flex flex-wrap justify-between items-center gap-4">
                <div>
                    <h1 className="text-2xl font-bold text-gray-900 dark:text-white">Settings</h1>
                    <p className="text-gray-600 dark:text-gray-400 mt-1 text-sm">
                        Active version: <span className="font-mono">v{catalog?.policy_version}</span>
                        {dirty && <span className="text-amber-500 ml-2">(unsaved changes)</span>}
                    </p>
                </div>
                <div className="flex items-center gap-3">
                    <select value={runId} onChange={(e) => setRunId(e.target.value)} className="text-sm py-2">
                        <option value="">Select a run for live preview...</option>
                        {completedRuns.map((r: any) => (
                            <option key={r.run_id} value={r.run_id}>
                                {r.run_id.slice(0, 8)} · {new Date(r.started_at).toLocaleString()} · {r.counters?.records_in?.toLocaleString?.() || 0} records
                            </option>
                        ))}
                    </select>
                    <button
                        onClick={handleResetToDefaults} disabled={resetting}
                        className="btn btn-ghost gap-2 disabled:opacity-40" title="Load the seeded safe defaults into the editor"
                    >
                        {resetting ? <RefreshCw className="animate-spin" size={16} /> : <RotateCcw size={16} />}
                        Reset to Defaults
                    </button>
                    <button onClick={handleSave} disabled={saving || anyBlocked || !dirty} className="btn btn-primary gap-2 disabled:opacity-40">
                        {saving ? <RefreshCw className="animate-spin" size={16} /> : <Save size={16} />}
                        Save &amp; Activate
                    </button>
                </div>
            </div>

            {saveMessage && (
                <div className="text-sm p-3 rounded-lg bg-blue-50 dark:bg-blue-900/20 text-blue-700 dark:text-blue-300">{saveMessage}</div>
            )}
            {!runId && (
                <div className="flex items-center gap-2 text-sm p-3 rounded-lg bg-amber-50 dark:bg-amber-900/20 text-amber-700 dark:text-amber-300">
                    <AlertCircle size={16} /> Select a completed run above to see live fan-out checks and decision-count previews.
                </div>
            )}

            {/* Tab bar */}
            <div className="flex flex-wrap gap-1 bg-gray-100 dark:bg-gray-800 rounded-lg p-1 w-fit">
                {TABS.map((t) => (
                    <button
                        key={t.id}
                        onClick={() => setActiveTab(t.id)}
                        className={`flex items-center gap-1.5 px-3 py-2 rounded-md text-sm transition-colors ${
                            activeTab === t.id
                                ? "bg-white dark:bg-gray-700 text-blue-600 dark:text-white shadow-sm"
                                : "text-gray-500 dark:text-gray-400 hover:text-gray-800 dark:hover:text-gray-200"
                        }`}
                    >
                        <t.icon size={14} /> {t.label}
                        {t.id === "blocking" && precheckError && <span className="w-1.5 h-1.5 rounded-full bg-red-500" />}
                    </button>
                ))}
            </div>

            {/* Tab content */}
            <AnimatePresence mode="wait">
                <motion.div
                    key={activeTab}
                    initial={{ opacity: 0, y: 6 }}
                    animate={{ opacity: 1, y: 0 }}
                    exit={{ opacity: 0 }}
                    transition={{ duration: 0.15 }}
                >
                    {activeTab === "fields" && (
                        <div className="glass-card p-6">
                            <div className="flex items-center gap-3 mb-4">
                                <div className="p-2 rounded-lg bg-indigo-100 dark:bg-indigo-500/20">
                                    <Table2 size={20} className="text-indigo-600 dark:text-indigo-400" />
                                </div>
                                <div>
                                    <h2 className="font-semibold text-gray-900 dark:text-white">Fields</h2>
                                    <p className="text-xs text-gray-500 dark:text-gray-400">What's actually in the source data -- reference this before adding blocking or matching rules.</p>
                                </div>
                            </div>
                            <FieldsPanel
                                fields={schemaData?.fields}
                                loading={schemaLoading}
                                error={schemaError ? String((schemaError as any).message || schemaError) : null}
                            />
                        </div>
                    )}

                    {activeTab === "blocking" && (
                        <div className="glass-card p-6">
                            <div className="flex items-center gap-3 mb-4">
                                <div className="p-2 rounded-lg bg-blue-100 dark:bg-blue-500/20">
                                    <ListTree size={20} className="text-blue-600 dark:text-blue-400" />
                                </div>
                                <h2 className="font-semibold text-gray-900 dark:text-white">Blocking Rules</h2>
                                {precheckError && <span className="text-xs text-red-500">{precheckError}</span>}
                            </div>

                            <div className="space-y-3">
                                {blockingRules.map((rule, i) => (
                                    <BlockingRuleCard
                                        key={rule.rule_id}
                                        rule={rule}
                                        estimate={estimates.find((e) => e.rule_id === rule.rule_id) || null}
                                        estimating={precheckLoading}
                                        isFirst={i === 0}
                                        isLast={i === blockingRules.length - 1}
                                        onChange={(next) => updateRule(i, next)}
                                        onDelete={() => deleteRule(i)}
                                        onMove={(dir) => moveRule(i, dir)}
                                    />
                                ))}
                            </div>

                            <div className="flex items-center gap-2 mt-4 pt-4 border-t border-gray-200 dark:border-gray-700">
                                <select
                                    value={addingField}
                                    onChange={(e) => setAddingField(e.target.value)}
                                    className="text-xs py-1.5 flex-1"
                                    disabled={availableFields.length === 0}
                                >
                                    <option value="">
                                        {availableFields.length === 0 ? "All fields already used" : "Pick a field to block on..."}
                                    </option>
                                    {availableFields.map((f: any) => (
                                        <option key={f.name} value={f.name}>
                                            {f.name} -- {f.semantic_label}
                                            {f.blocking_verdict?.severity === "BLOCK" ? " (would explode -- needs a guard)" : ""}
                                        </option>
                                    ))}
                                </select>
                                <button onClick={addRule} disabled={!addingField && availableFields.length > 0} className="btn btn-ghost gap-1 !py-1.5 !px-3 text-xs disabled:opacity-40">
                                    <Plus size={14} /> Add rule
                                </button>
                            </div>
                            <p className="text-[11px] text-gray-400 mt-2">
                                Every field is listed in the Fields tab with its explosion-risk check -- pick any of them here to block on it.
                            </p>

                            <div className="flex items-center justify-between gap-3 mt-3 pt-3 border-t border-gray-100 dark:border-gray-800">
                                <p className="text-[11px] text-gray-400">
                                    Exact address match above only groups byte-identical addresses. Word-token blocking
                                    catches typos/abbreviation differences in one word as long as another word still matches.
                                </p>
                                <button
                                    onClick={addAddressTokenRule}
                                    disabled={hasAddressTokenRule}
                                    className="btn btn-ghost gap-1 !py-1.5 !px-3 text-xs disabled:opacity-40 shrink-0"
                                    title={hasAddressTokenRule ? "Already added" : "Add a fuzzy(-ish) address blocking rule"}
                                >
                                    <Plus size={14} /> Add fuzzy address blocking
                                </button>
                            </div>
                        </div>
                    )}

                    {activeTab === "matching" && (
                        <div className="glass-card p-6">
                            <div className="flex items-center gap-3 mb-4">
                                <div className="p-2 rounded-lg bg-emerald-100 dark:bg-emerald-500/20">
                                    <Sliders size={20} className="text-emerald-600 dark:text-emerald-400" />
                                </div>
                                <h2 className="font-semibold text-gray-900 dark:text-white">Matching &amp; Confidence</h2>
                            </div>
                            {matchRuleset && (
                                <ConfidencePanel
                                    ruleset={matchRuleset}
                                    onChange={(s) => { setDirty(true); setMatchRuleset(s); }}
                                    counts={decisionCounts}
                                    baseline={baselineCounts}
                                    delta={decisionDelta}
                                    loading={redecideLoading}
                                    elapsedMs={redecideElapsed}
                                    onDeleteRule={deleteMatchRule}
                                />
                            )}
                            <div className="flex items-center gap-2 mt-4 pt-4 border-t border-gray-200 dark:border-gray-700">
                                <select
                                    value={addingMatchField}
                                    onChange={(e) => setAddingMatchField(e.target.value)}
                                    className="text-xs py-1.5 flex-1"
                                    disabled={availableMatchFields.length === 0}
                                >
                                    <option value="">
                                        {availableMatchFields.length === 0 ? "All matchable fields already used" : "Give another field a weight..."}
                                    </option>
                                    {availableMatchFields.map((f: any) => (
                                        <option key={f.name} value={f.name}>
                                            {f.name} -- {f.semantic_label}
                                        </option>
                                    ))}
                                </select>
                                <button onClick={addMatchRule} disabled={!addingMatchField && availableMatchFields.length > 0} className="btn btn-ghost gap-1 !py-1.5 !px-3 text-xs disabled:opacity-40">
                                    <Plus size={14} /> Add field
                                </button>
                            </div>
                            <p className="text-[11px] text-gray-400 mt-2">
                                Any field can count toward a match, not just the ones above -- e.g. mother's name, cousin's
                                name, or any field added to the source later. New fields start at 0% until you set a weight.
                            </p>
                        </div>
                    )}

                    {activeTab === "segments" && (
                        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                            <div className="glass-card p-6">
                                <div className="flex items-center gap-3 mb-4">
                                    <div className="p-2 rounded-lg bg-blue-100 dark:bg-blue-500/20">
                                        <Users size={20} className="text-blue-600 dark:text-blue-400" />
                                    </div>
                                    <h2 className="font-semibold text-gray-900 dark:text-white">Companies vs. Individuals</h2>
                                </div>
                                {segmentation && (
                                    <SegmentsPanel
                                        config={segmentation}
                                        onChange={(s) => { setDirty(true); setSegmentation(s); }}
                                        stats={segmentStats ?? null}
                                        statsLoading={segmentStatsLoading}
                                        relationshipCount={relationshipsData?.total ?? null}
                                    />
                                )}
                            </div>

                            <div className="glass-card p-6">
                                <div className="flex items-center gap-3 mb-4">
                                    <div className="p-2 rounded-lg bg-amber-100 dark:bg-amber-500/20">
                                        <Link2 size={20} className="text-amber-600 dark:text-amber-400" />
                                    </div>
                                    <h2 className="font-semibold text-gray-900 dark:text-white">Connections</h2>
                                </div>
                                <RelationshipsPanel
                                    runId={runId}
                                    relationships={shownRelationships?.relationships || []}
                                    total={shownRelationships?.total || 0}
                                    loading={debouncedConnectionSearch.length >= 2 ? connectionSearchLoading : relationshipsLoading}
                                    searchValue={connectionSearch}
                                    onSearchChange={setConnectionSearch}
                                />
                            </div>
                        </div>
                    )}

                    {activeTab === "reference" && (
                        <div className="space-y-6">
                            <div className="glass-card p-6">
                                <div className="flex items-center gap-3 mb-4">
                                    <div className="p-2 rounded-lg bg-purple-100 dark:bg-purple-500/20">
                                        <SearchIcon size={20} className="text-purple-600 dark:text-purple-400" />
                                    </div>
                                    <h2 className="font-semibold text-gray-900 dark:text-white">Search Records</h2>
                                </div>
                                <input
                                    value={searchQuery}
                                    onChange={(e) => setSearchQuery(e.target.value)}
                                    placeholder="Search by mobile, email, document, name token, or customer code..."
                                    className="w-full text-sm"
                                />
                                {searching && <p className="text-xs text-gray-400 mt-2">Searching...</p>}
                                {searchResults && searchResults.results?.length > 0 && (
                                    <div className="mt-3 space-y-1 max-h-56 overflow-y-auto">
                                        {searchResults.results.map((r: any) => (
                                            <div key={r.customer_code} className="flex justify-between text-xs p-2 rounded bg-gray-50 dark:bg-gray-900/50">
                                                <span className="font-mono">{r.customer_code}</span>
                                                <span className="text-gray-600 dark:text-gray-300">{r.name_norm || "—"}</span>
                                                <span className="text-gray-400">{Object.keys(r.identifiers || {}).join(", ")}</span>
                                                {r.cluster_id && <span className="badge badge-info">{r.cluster_id.slice(0, 10)}</span>}
                                            </div>
                                        ))}
                                        {searchResults.total > searchResults.results.length && (
                                            <p className="text-[11px] text-gray-400">+{searchResults.total - searchResults.results.length} more results</p>
                                        )}
                                    </div>
                                )}
                                {searchResults && debouncedSearch.length >= 2 && searchResults.results?.length === 0 && (
                                    <p className="text-xs text-gray-400 mt-2">No matches.</p>
                                )}
                            </div>

                            <div className="glass-card p-6">
                                <div className="flex items-center gap-3 mb-4">
                                    <div className="p-2 rounded-lg bg-orange-100 dark:bg-orange-500/20">
                                        <GitCompareArrows size={20} className="text-orange-600 dark:text-orange-400" />
                                    </div>
                                    <h2 className="font-semibold text-gray-900 dark:text-white">Matching Comparators</h2>
                                </div>
                                <ComparatorInfoPanel
                                    comparators={comparatorsData?.comparators}
                                    excluded={comparatorsData?.excluded}
                                />
                            </div>

                            <div className="glass-card p-6">
                                <div className="flex items-center gap-3 mb-4">
                                    <div className="p-2 rounded-lg bg-gray-200 dark:bg-gray-700">
                                        <History size={20} className="text-gray-600 dark:text-gray-300" />
                                    </div>
                                    <h2 className="font-semibold text-gray-900 dark:text-white">Version History</h2>
                                </div>
                                <VersionHistory versions={versionsData?.versions || []} onActivate={handleActivateVersion} />
                            </div>
                        </div>
                    )}
                </motion.div>
            </AnimatePresence>

        </div>
    );
}
