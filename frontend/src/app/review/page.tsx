'use client';

// Entity Resolution Workbench (Stage 5 of the workbench plan, refined per
// officer feedback: side-by-side record comparison, search/filters across
// every population, and bulk actions). See docs in api/routes_workbench.py
// and services/workbench_service.py. /explorer and /graph are untouched
// legacy pages; this is the new, unified place a bank officer works.

import { useEffect, useMemo, useState } from 'react';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { motion } from 'framer-motion';
import {
    Users, GitMerge, GitBranch, XCircle, UserX, AlertTriangle, ShieldCheck, ShieldAlert,
    Search as SearchIcon, CheckCircle2, IdCard, History, Building2, User as UserIcon, ArrowLeft, ChevronRight, UserPlus,
} from 'lucide-react';
import { api } from '@/lib/api';
import { ScoreBreakdown } from '@/components/workbench/ScoreBreakdown';
import { ReasonDialog, ReasonDialogResult } from '@/components/workbench/ReasonDialog';
import { RecordCompare } from '@/components/workbench/RecordCompare';
import { FilterBar, PairFilters, EntityFilters, SingletonFilters } from '@/components/workbench/FilterBar';

type Population = 'REVIEW' | 'AUTO_LINK' | 'REJECT' | 'ENTITIES' | 'APPROVED' | 'SINGLETONS';

const BULK_SELECTION_CAP = 100;

function useDebounced<T>(value: T, delayMs: number): T {
    const [debounced, setDebounced] = useState(value);
    useEffect(() => {
        const t = setTimeout(() => setDebounced(value), delayMs);
        return () => clearTimeout(t);
    }, [value, delayMs]);
    return debounced;
}

function toggleInSet(prev: Set<string>, id: string): Set<string> {
    const next = new Set(prev);
    if (next.has(id)) {
        next.delete(id);
    } else if (next.size < BULK_SELECTION_CAP) {
        next.add(id);
    }
    return next;
}

function PopCard({ label, count, active, icon: Icon, accent, onClick }: {
    label: string; count: number | undefined; active: boolean; icon: any; accent: string; onClick: () => void;
}) {
    return (
        <button
            onClick={onClick}
            className={`glass-card p-4 text-left transition-all ${active ? 'ring-2 ring-blue-500' : 'hover:bg-gray-50 dark:hover:bg-gray-900/40'}`}
        >
            <div className="flex items-center gap-2 mb-1">
                <Icon size={16} className={accent} />
                <span className="text-xs text-gray-500 dark:text-gray-400">{label}</span>
            </div>
            <div className="text-xl font-bold text-gray-900 dark:text-white">
                {count === undefined ? '…' : count.toLocaleString()}
            </div>
        </button>
    );
}

function DetailField({ label, value }: { label: string; value?: string | null }) {
    if (!value) return null;
    return (
        <div className="flex gap-2">
            <span className="text-gray-400 w-24 shrink-0">{label}</span>
            <span className="text-gray-700 dark:text-gray-300 break-words">{value}</span>
        </div>
    );
}

// A member row starts collapsed (just code + name, as before); clicking it
// fetches and expands the full record profile inline -- dob, segment, and
// every identifier -- via the same GET /workbench/records/{code} RecordCompare
// already uses, so "view full details for this person" doesn't need a new
// endpoint or a second modal type.
function MemberRow({ member, runId, expanded, onToggle, canSplit, onSplit }: {
    member: { customer_code: string; name_norm?: string | null };
    runId: string;
    expanded: boolean;
    onToggle: () => void;
    canSplit: boolean;
    onSplit: () => void;
}) {
    const { data: detail, isFetching } = useQuery({
        queryKey: ['wb-member-detail', runId, member.customer_code],
        queryFn: () => api.wbGetRecord(member.customer_code, runId),
        enabled: expanded,
    });

    return (
        <div className="rounded-lg bg-gray-50 dark:bg-gray-900/40 text-xs overflow-hidden">
            <div className="flex items-center justify-between p-2 cursor-pointer" onClick={onToggle}>
                <div className="flex items-center gap-1.5 min-w-0">
                    <ChevronRight size={12} className={`text-gray-400 shrink-0 transition-transform ${expanded ? 'rotate-90' : ''}`} />
                    <span className="font-mono text-gray-700 dark:text-gray-300 shrink-0">{member.customer_code}</span>
                    {member.name_norm && <span className="text-gray-500 truncate">{member.name_norm}</span>}
                </div>
                {canSplit && (
                    <button
                        onClick={(e) => { e.stopPropagation(); onSplit(); }}
                        className="text-gray-400 hover:text-red-500 shrink-0" title="Split this record out"
                    >
                        <UserX size={13} />
                    </button>
                )}
            </div>
            {expanded && (
                <div className="px-2 pb-2 pt-1.5 border-t border-gray-200 dark:border-gray-700 space-y-1">
                    {isFetching && <p className="text-gray-400">Loading details...</p>}
                    {detail && detail.resolved === false && <p className="text-amber-500">Record not found for this run.</p>}
                    {detail && detail.resolved !== false && (
                        <>
                            <DetailField label="Name" value={detail.name_norm} />
                            <DetailField label="Date of birth" value={detail.dob_iso} />
                            <DetailField label="Segment" value={detail.segment} />
                            <DetailField label="Mobile" value={(detail.identifiers?.mobile || []).join(', ')} />
                            <DetailField label="Email" value={(detail.identifiers?.email || []).join(', ')} />
                            <DetailField label="Document / NID" value={(detail.identifiers?.document || []).join(', ')} />
                            <DetailField label="Address" value={(detail.identifiers?.address || []).join(' | ')} />
                        </>
                    )}
                </div>
            )}
        </div>
    );
}

const MATCH_DECISION_COLOR: Record<string, string> = {
    AUTO_LINK: 'text-emerald-600 dark:text-emerald-400',
    REVIEW: 'text-amber-600 dark:text-amber-400',
    REJECT: 'text-gray-500 dark:text-gray-400',
};

// One direct, scored edge between two of an entity's members -- the actual
// evidence a union-find over these edges collapsed into this one cluster.
// Expands to the same rule-by-rule ScoreBreakdown a pair gets in the
// REVIEW/AUTO_LINK/REJECT lists, so "why is this one entity" is answered
// with real scores, not just a member list.
function EntityMatchRow({ item, runId, expanded, onToggle }: {
    item: { a_key: string; b_key: string; a_name?: string | null; b_name?: string | null; confidence_pct: number; has_veto: boolean; decision: string };
    runId: string;
    expanded: boolean;
    onToggle: () => void;
}) {
    const { data: breakdown, isFetching } = useQuery({
        queryKey: ['wb-entity-match-breakdown', runId, item.a_key, item.b_key],
        queryFn: () => api.wbPairBreakdown(item.a_key, item.b_key, runId),
        enabled: expanded,
    });

    return (
        <div className="rounded-lg bg-gray-50 dark:bg-gray-900/40 text-xs overflow-hidden">
            <div className="flex items-center justify-between p-2 cursor-pointer gap-2" onClick={onToggle}>
                <div className="flex items-center gap-1.5 min-w-0">
                    <ChevronRight size={12} className={`text-gray-400 shrink-0 transition-transform ${expanded ? 'rotate-90' : ''}`} />
                    <span className="text-gray-900 dark:text-white truncate">
                        {item.a_name || item.a_key} ↔ {item.b_name || item.b_key}
                    </span>
                </div>
                <div className="flex items-center gap-2 shrink-0">
                    {item.has_veto && <ShieldAlert size={12} className="text-red-500" />}
                    <span className={`font-semibold ${MATCH_DECISION_COLOR[item.decision] || 'text-gray-500'}`}>{item.decision.replace('_', ' ')}</span>
                    <span className={`font-mono ${item.has_veto ? 'text-red-500' : 'text-gray-600 dark:text-gray-300'}`}>{item.confidence_pct.toFixed(0)}%</span>
                </div>
            </div>
            {expanded && (
                <div className="px-2 pb-2 pt-1 border-t border-gray-200 dark:border-gray-700">
                    <ScoreBreakdown breakdown={breakdown || null} loading={isFetching} />
                </div>
            )}
        </div>
    );
}

// A singleton -- zero candidate_pairs edges, so it never earned an entity
// through the normal pipeline path (see engine.clustering.entity_resolver's
// module docstring: "only accepted, multi-member components... an entity
// is minted lazily, when a cluster earns one or an officer assigns a
// Global ID directly to a record"). This is that direct-assignment path:
// no entity to select, just the raw record plus a form that mints a
// fresh one-member entity the moment a Global ID is confirmed.
function SingletonPanel({
    customerCode, record, loading, globalRefInput, onGlobalRefInputChange, onAssign, assigning,
}: {
    customerCode: string;
    record: any | null;
    loading: boolean;
    globalRefInput: string;
    onGlobalRefInputChange: (v: string) => void;
    onAssign: () => void;
    assigning: boolean;
}) {
    if (loading) return <p className="text-xs text-gray-400">Loading record...</p>;
    if (!record || record.resolved === false) return <p className="text-xs text-amber-500">Record not found for this run.</p>;

    const TypeIcon = record.segment === 'COMPANY' ? Building2 : UserIcon;

    return (
        <div className="space-y-5">
            <div className="flex items-center justify-between gap-2">
                <div className="min-w-0">
                    <h2 className="font-semibold text-gray-900 dark:text-white truncate">{record.name_norm || customerCode}</h2>
                    <span className="font-mono text-xs text-gray-400">{customerCode}</span>
                </div>
                <span className="flex items-center gap-1 text-xs text-gray-500 dark:text-gray-400 shrink-0">
                    <TypeIcon size={13} /> {record.segment || '—'}
                </span>
            </div>

            <div className="flex items-start gap-2 p-3 rounded-lg bg-amber-50 dark:bg-amber-900/20 text-xs text-amber-700 dark:text-amber-300">
                <AlertTriangle size={14} className="mt-0.5 shrink-0" />
                <div>
                    Singleton -- no candidate match was found for this record in this run, so it has no entity yet. Assigning a Global ID below creates a one-member entity for it.
                </div>
            </div>

            <div className="space-y-1 text-xs">
                <DetailField label="Date of birth" value={record.dob_iso} />
                <DetailField label="Mobile" value={(record.identifiers?.mobile || []).join(', ')} />
                <DetailField label="Email" value={(record.identifiers?.email || []).join(', ')} />
                <DetailField label="Document / NID" value={(record.identifiers?.document || []).join(', ')} />
                <DetailField label="Address" value={(record.identifiers?.address || []).join(' | ')} />
            </div>

            <div>
                <h3 className="text-xs font-semibold text-gray-500 dark:text-gray-400 uppercase tracking-wider mb-2">Global ID</h3>
                <div className="flex gap-2">
                    <input
                        value={globalRefInput} onChange={(e) => onGlobalRefInputChange(e.target.value)}
                        placeholder="e.g. CIF-0012345" className="flex-1 text-xs py-1.5"
                    />
                    <button onClick={onAssign} disabled={!globalRefInput.trim() || assigning} className="btn btn-primary !py-1.5 !px-3 text-xs disabled:opacity-40">
                        {assigning ? 'Assigning...' : 'Assign'}
                    </button>
                </div>
            </div>
        </div>
    );
}

const EMPTY_PAIR_FILTERS: PairFilters = { q: '', recordType: 'ALL', minConf: undefined, maxConf: undefined, hasVeto: undefined };
const EMPTY_ENTITY_FILTERS: EntityFilters = { q: '', recordType: 'ALL', hasGlobalRef: undefined };
const EMPTY_SINGLETON_FILTERS: SingletonFilters = { q: '', recordType: 'ALL' };

export default function WorkbenchPage() {
    const queryClient = useQueryClient();

    const { data: runsData } = useQuery({ queryKey: ['wb-runs'], queryFn: () => api.wbListRuns(1, 50) });
    const completedRuns = useMemo(() => (runsData?.runs || []).filter((r: any) => r.status === 'COMPLETED'), [runsData]);
    const [runId, setRunId] = useState<string>('');
    useEffect(() => {
        if (!runId && completedRuns.length > 0) setRunId(completedRuns[0].run_id);
    }, [completedRuns, runId]);

    const { data: populations } = useQuery({
        queryKey: ['wb-populations', runId],
        queryFn: () => api.wbPopulations(runId),
        enabled: !!runId,
    });

    const { data: auditStatus } = useQuery({
        queryKey: ['wb-audit-verify'],
        queryFn: () => api.wbAuditVerify(),
        refetchInterval: 60000,
    });

    const [population, setPopulation] = useState<Population>('REVIEW');
    const [page, setPage] = useState(1);
    const PAGE_SIZE = 20;

    const [pairFilters, setPairFilters] = useState<PairFilters>(EMPTY_PAIR_FILTERS);
    const [entityFilters, setEntityFilters] = useState<EntityFilters>(EMPTY_ENTITY_FILTERS);
    const [singletonFilters, setSingletonFilters] = useState<SingletonFilters>(EMPTY_SINGLETON_FILTERS);
    const debouncedPairQ = useDebounced(pairFilters.q, 400);
    const debouncedEntityQ = useDebounced(entityFilters.q, 400);
    const debouncedSingletonQ = useDebounced(singletonFilters.q, 400);

    useEffect(() => setPage(1), [
        population, runId, debouncedPairQ, pairFilters.recordType, pairFilters.minConf, pairFilters.maxConf, pairFilters.hasVeto,
        debouncedEntityQ, entityFilters.recordType, entityFilters.hasGlobalRef, debouncedSingletonQ, singletonFilters.recordType,
    ]);

    const [selectedPair, setSelectedPair] = useState<{ a_key: string; b_key: string } | null>(null);
    const [selectedEntity, setSelectedEntity] = useState<string | null>(null);
    const [expandedMember, setExpandedMember] = useState<string | null>(null);
    useEffect(() => setExpandedMember(null), [selectedEntity]);

    // A singleton picked from global search -- no entity_id exists yet, so
    // it can't route through selectedEntity/jumpToEntity like everything
    // else. Cleared whenever a pair/entity gets explicitly selected
    // elsewhere so the detail panel never shows a stale singleton over a
    // fresh selection.
    const [selectedSingleton, setSelectedSingleton] = useState<string | null>(null);
    const [assigningSingletonRef, setAssigningSingletonRef] = useState(false);

    const { data: singletonRecord, isFetching: singletonRecordLoading } = useQuery({
        queryKey: ['wb-singleton-record', selectedSingleton, runId],
        queryFn: () => api.wbGetRecord(selectedSingleton!, runId),
        enabled: !!selectedSingleton,
    });

    // Navigation history -- jumping to an entity (from a pair's "part of an
    // existing entity" link, or a global search hit) changes population and
    // selection out from under whatever the officer was looking at, with no
    // way back. Each jump pushes a snapshot of where it came from; "Back"
    // pops it and restores population + selection so the officer can hop
    // entity -> entity -> pair and always retrace their steps.
    interface NavSnapshot { population: Population; selectedPair: { a_key: string; b_key: string } | null; selectedEntity: string | null; label: string }
    const [navStack, setNavStack] = useState<NavSnapshot[]>([]);

    // Bulk selection -- scoped to the current population/run, survives
    // pagination within it (an officer filtering then paging shouldn't lose
    // their picks), cleared on population/run switch.
    const [selectedPairKeys, setSelectedPairKeys] = useState<Set<string>>(new Set());
    const [selectedEntityIds, setSelectedEntityIds] = useState<Set<string>>(new Set());
    useEffect(() => { setSelectedPairKeys(new Set()); setSelectedEntityIds(new Set()); }, [population, runId]);
    useEffect(() => { setNavStack([]); }, [runId]);

    const { data: pairsData, isFetching: pairsLoading } = useQuery({
        queryKey: ['wb-pairs', runId, population, page, debouncedPairQ, pairFilters.recordType, pairFilters.minConf, pairFilters.maxConf, pairFilters.hasVeto],
        queryFn: () => api.wbListPairs({
            runId, decision: population, page, pageSize: PAGE_SIZE,
            q: debouncedPairQ || undefined, recordType: pairFilters.recordType,
            minConf: pairFilters.minConf, maxConf: pairFilters.maxConf, hasVeto: pairFilters.hasVeto,
        }),
        enabled: !!runId && population !== 'ENTITIES' && population !== 'APPROVED' && population !== 'SINGLETONS',
    });

    const { data: entitiesData, isFetching: entitiesLoading } = useQuery({
        queryKey: ['wb-entities', runId, page, debouncedEntityQ, entityFilters.recordType, entityFilters.hasGlobalRef],
        queryFn: () => api.wbListEntities({
            page, pageSize: PAGE_SIZE, runId,
            q: debouncedEntityQ || undefined, recordType: entityFilters.recordType, hasGlobalRef: entityFilters.hasGlobalRef,
        }),
        enabled: population === 'ENTITIES',
    });

    // Singletons -- records with zero candidate_pairs edges, so they never
    // earned an entity through the normal pipeline path (see
    // engine.clustering.entity_resolver's module docstring). Reuses the
    // same /graph/v2/singletons endpoint the graph page already browses --
    // it's the same underlying data (records with no entity yet), no
    // reason to duplicate it as a workbench-specific endpoint.
    const { data: singletonsListData, isFetching: singletonsListLoading } = useQuery({
        queryKey: ['wb-singletons-list', runId, page, debouncedSingletonQ, singletonFilters.recordType],
        queryFn: () => api.graphSingletons({
            runId, page, pageSize: PAGE_SIZE, recordType: singletonFilters.recordType, q: debouncedSingletonQ || undefined,
        }),
        enabled: population === 'SINGLETONS',
    });

    // "Approved" tab -- the durable ledger of every officer decision
    // (resolution_overrides), independent of any single run's own pair
    // populations. Toggle between MUST_LINK (approved) and MUST_NOT_LINK
    // (rejected) for full traceability, not just approvals.
    const [overrideVerdict, setOverrideVerdict] = useState<'MUST_LINK' | 'MUST_NOT_LINK'>('MUST_LINK');
    useEffect(() => setPage(1), [overrideVerdict]);
    const { data: overridesData, isFetching: overridesLoading } = useQuery({
        queryKey: ['wb-overrides', runId, page, overrideVerdict],
        queryFn: () => api.wbListOverrides({ page, pageSize: PAGE_SIZE, verdict: overrideVerdict, runId }),
        enabled: population === 'APPROVED',
    });
    const { data: approvedCountData } = useQuery({
        queryKey: ['wb-overrides-count', runId],
        queryFn: () => api.wbListOverrides({ page: 1, pageSize: 1, verdict: 'MUST_LINK', runId }),
        enabled: !!runId,
    });

    const { data: breakdown, isFetching: breakdownLoading } = useQuery({
        queryKey: ['wb-breakdown', runId, selectedPair?.a_key, selectedPair?.b_key],
        queryFn: () => api.wbPairBreakdown(selectedPair!.a_key, selectedPair!.b_key, runId),
        enabled: !!selectedPair && !!runId,
    });

    const { data: recordA } = useQuery({
        queryKey: ['wb-record', runId, selectedPair?.a_key],
        queryFn: () => api.wbGetRecord(selectedPair!.a_key, runId),
        enabled: !!selectedPair && !!runId,
    });
    const { data: recordB } = useQuery({
        queryKey: ['wb-record', runId, selectedPair?.b_key],
        queryFn: () => api.wbGetRecord(selectedPair!.b_key, runId),
        enabled: !!selectedPair && !!runId,
    });

    const { data: entityDetail, isFetching: entityDetailLoading } = useQuery({
        queryKey: ['wb-entity-detail', selectedEntity, runId],
        queryFn: () => api.wbGetEntity(selectedEntity!, runId),
        enabled: !!selectedEntity,
    });

    const { data: entityMatches, isFetching: entityMatchesLoading } = useQuery({
        queryKey: ['wb-entity-matches', selectedEntity, runId],
        queryFn: () => api.wbEntityMatches(selectedEntity!, runId),
        enabled: !!selectedEntity,
    });
    const [expandedMatchKey, setExpandedMatchKey] = useState<string | null>(null);
    useEffect(() => setExpandedMatchKey(null), [selectedEntity]);

    const jumpToEntity = (entityId: string) => {
        setNavStack((prev) => [...prev, {
            population, selectedPair, selectedEntity,
            label: population === 'ENTITIES' && selectedEntity ? `entity ${selectedEntity.slice(0, 8)}` : population.replace('_', ' '),
        }]);
        setPopulation('ENTITIES');
        setSelectedEntity(entityId);
        setSelectedPair(null);
        setSelectedSingleton(null);
    };

    const goBack = () => {
        setSelectedSingleton(null);
        setNavStack((prev) => {
            if (prev.length === 0) return prev;
            const last = prev[prev.length - 1];
            setPopulation(last.population);
            setSelectedPair(last.selectedPair);
            setSelectedEntity(last.selectedEntity);
            return prev.slice(0, -1);
        });
    };

    // ---- Global search ----
    const [globalQuery, setGlobalQuery] = useState('');
    const debouncedGlobalQuery = useDebounced(globalQuery, 400);
    const { data: searchResults, isFetching: searching } = useQuery({
        queryKey: ['wb-search', debouncedGlobalQuery, runId],
        queryFn: () => api.wbSearch(debouncedGlobalQuery, runId, 1, 10),
        enabled: debouncedGlobalQuery.length >= 2 && !!runId,
    });

    // ---- Single-item actions ----
    const [dialog, setDialog] = useState<null | { action: 'approve' | 'reject' | 'merge' | 'split'; title: string; bulk?: boolean }>(null);
    const [actionError, setActionError] = useState<string | null>(null);
    const [mergeTarget, setMergeTarget] = useState<string>('');
    const [splitCode, setSplitCode] = useState<string>('');
    const [globalRefInput, setGlobalRefInput] = useState('');
    useEffect(() => setGlobalRefInput(''), [selectedSingleton]);

    // ---- Bulk actions ----
    const [bulkProgress, setBulkProgress] = useState<{ done: number; total: number; failures: string[] } | null>(null);

    const invalidateAll = () => {
        queryClient.invalidateQueries({ queryKey: ['wb-populations'] });
        queryClient.invalidateQueries({ queryKey: ['wb-pairs'] });
        queryClient.invalidateQueries({ queryKey: ['wb-breakdown'] });
        queryClient.invalidateQueries({ queryKey: ['wb-entities'] });
        queryClient.invalidateQueries({ queryKey: ['wb-entity-detail'] });
        queryClient.invalidateQueries({ queryKey: ['wb-entity-matches'] });
        queryClient.invalidateQueries({ queryKey: ['wb-audit-verify'] });
    };

    const runBulkPairAction = async (action: 'approve' | 'reject', result: ReasonDialogResult) => {
        const keys = Array.from(selectedPairKeys);
        const failures: string[] = [];
        setBulkProgress({ done: 0, total: keys.length, failures: [] });
        for (let i = 0; i < keys.length; i++) {
            const [a, b] = keys[i].split(':::');
            try {
                if (action === 'approve') await api.wbApprove(runId, a, b, result.reasonCode, result.reason, result.actor);
                else await api.wbReject(runId, a, b, result.reasonCode, result.reason, result.actor);
            } catch (e: any) {
                failures.push(`${a} : ${b} -- ${e.message || e}`);
            }
            setBulkProgress({ done: i + 1, total: keys.length, failures: [...failures] });
        }
        setSelectedPairKeys(new Set());
        invalidateAll();
    };

    // Bulk merge folds selected entities pairwise into a running survivor.
    // merge_entities() doesn't always keep the first argument -- it picks
    // whichever entity has more members -- so the next call's anchor must
    // be the `kept_entity_id` the previous call actually returned. Halts
    // (rather than skipping) on the first conflict so the officer sees
    // exactly which pair needs a manual decision.
    const runBulkMerge = async (result: ReasonDialogResult) => {
        const ids = Array.from(selectedEntityIds);
        if (ids.length < 2) return;
        const failures: string[] = [];
        setBulkProgress({ done: 0, total: ids.length - 1, failures: [] });
        let survivor = ids[0];
        for (let i = 1; i < ids.length; i++) {
            try {
                const res = await api.wbMerge(runId, survivor, ids[i], result.reasonCode, result.reason, result.actor);
                survivor = res.kept_entity_id;
            } catch (e: any) {
                failures.push(`${ids[i]} -- ${e.message || e}`);
                setBulkProgress({ done: i, total: ids.length - 1, failures: [...failures] });
                break;
            }
            setBulkProgress({ done: i, total: ids.length - 1, failures: [...failures] });
        }
        setSelectedEntityIds(new Set());
        invalidateAll();
    };

    const runDialogAction = async (result: ReasonDialogResult) => {
        setActionError(null);
        try {
            if (dialog?.bulk && (dialog.action === 'approve' || dialog.action === 'reject')) {
                await runBulkPairAction(dialog.action, result);
            } else if (dialog?.bulk && dialog.action === 'merge') {
                await runBulkMerge(result);
            } else if (dialog?.action === 'approve' && selectedPair) {
                await api.wbApprove(runId, selectedPair.a_key, selectedPair.b_key, result.reasonCode, result.reason, result.actor);
                // Deliberately NOT clearing selectedPair here -- staying on the
                // pair after approving is what lets the officer actually SEE
                // their decision took effect (the "Approved by ..." banner and
                // list badge), instead of the panel just going blank.
                invalidateAll();
            } else if (dialog?.action === 'reject' && selectedPair) {
                await api.wbReject(runId, selectedPair.a_key, selectedPair.b_key, result.reasonCode, result.reason, result.actor);
                invalidateAll();
            } else if (dialog?.action === 'merge' && selectedEntity && mergeTarget) {
                await api.wbMerge(runId, selectedEntity, mergeTarget, result.reasonCode, result.reason, result.actor);
                setMergeTarget('');
                invalidateAll();
            } else if (dialog?.action === 'split' && selectedEntity && splitCode) {
                await api.wbSplit(runId, selectedEntity, splitCode, result.reasonCode, result.reason, result.actor);
                setSplitCode('');
                invalidateAll();
            }
        } catch (e: any) {
            setActionError(String(e.message || e));
        } finally {
            setDialog(null);
        }
    };

    const assignGlobalRef = async () => {
        if (!selectedEntity || !globalRefInput.trim()) return;
        const actor = localStorage.getItem('wb_actor') || 'officer';
        try {
            setActionError(null);
            await api.wbAssignGlobalRef(selectedEntity, globalRefInput.trim(), 'CONFIRMED', 'Assigned via workbench', actor);
            setGlobalRefInput('');
            invalidateAll();
        } catch (e: any) {
            setActionError(String(e.message || e));
        }
    };

    // Assigning to a singleton mints a fresh one-member entity server-side
    // (see POST /workbench/records/{code}/global-ref) -- once that
    // succeeds, jump straight to viewing it as a real entity so the officer
    // sees the result of their action (lineage, member list, the Global ID
    // now attached) rather than a banner asking them to trust it worked.
    const assignGlobalRefToSingleton = async () => {
        if (!selectedSingleton || !globalRefInput.trim()) return;
        const actor = localStorage.getItem('wb_actor') || 'officer';
        setAssigningSingletonRef(true);
        try {
            setActionError(null);
            const res = await api.wbAssignGlobalRefToRecord(selectedSingleton, runId, globalRefInput.trim(), 'CONFIRMED', 'Assigned via workbench', actor);
            setGlobalRefInput('');
            invalidateAll();
            jumpToEntity(res.entity_id);
        } catch (e: any) {
            setActionError(String(e.message || e));
        } finally {
            setAssigningSingletonRef(false);
        }
    };

    const totalPairs = pairsData?.total ?? 0;
    const totalEntities = entitiesData?.total ?? 0;
    const totalOverrides = overridesData?.total ?? 0;
    const totalSingletons = singletonsListData?.total ?? 0;
    const totalPages = Math.max(1, Math.ceil(
        (population === 'ENTITIES' ? totalEntities
            : population === 'APPROVED' ? totalOverrides
            : population === 'SINGLETONS' ? totalSingletons
            : totalPairs) / PAGE_SIZE
    ));

    return (
        <div className="space-y-6">
            <div className="flex flex-wrap justify-between items-center gap-4">
                <div>
                    <h1 className="text-2xl font-bold text-gray-900 dark:text-white">Entity Resolution Workbench</h1>
                    <p className="text-gray-600 dark:text-gray-400 mt-1 text-sm">
                        Every population, one place -- rule-by-rule scoring, durable Global IDs, full audit trail.
                    </p>
                </div>
                <div className="flex items-center gap-3">
                    {auditStatus && (
                        <span className={`badge gap-1 ${auditStatus.valid ? 'badge-success' : 'badge-danger'}`} title={auditStatus.error || 'Audit chain integrity'}>
                            {auditStatus.valid ? <ShieldCheck size={12} /> : <ShieldAlert size={12} />}
                            {auditStatus.valid ? 'Audit chain verified' : 'Audit chain FAILED'}
                        </span>
                    )}
                    <select value={runId} onChange={(e) => setRunId(e.target.value)} className="text-sm py-2">
                        <option value="">Select a run...</option>
                        {completedRuns.map((r: any) => (
                            <option key={r.run_id} value={r.run_id}>
                                {r.engine} · {r.run_id.slice(0, 8)} · {new Date(r.started_at).toLocaleString()}
                            </option>
                        ))}
                    </select>
                </div>
            </div>

            {/* Global search */}
            <motion.div initial={{ opacity: 0, y: 10 }} animate={{ opacity: 1, y: 0 }} className="glass-card p-4">
                <div className="relative">
                    <SearchIcon size={16} className="absolute left-3 top-1/2 -translate-y-1/2 text-gray-400" />
                    <input
                        value={globalQuery} onChange={(e) => setGlobalQuery(e.target.value)}
                        placeholder="Search by name, identifier, customer code, or Global ID..."
                        className="w-full text-sm" style={{ paddingLeft: '2.25rem' }}
                    />
                </div>
                {searching && <p className="text-xs text-gray-400 mt-2">Searching...</p>}
                {searchResults && searchResults.results?.length > 0 && (
                    <div className="mt-3 space-y-1 max-h-56 overflow-y-auto">
                        {searchResults.results.map((r: any) => (
                            <button
                                key={r.customer_code}
                                onClick={async () => {
                                    const rec = await api.wbGetRecord(r.customer_code, runId);
                                    if (rec.entity_id) {
                                        jumpToEntity(rec.entity_id);
                                    } else {
                                        // A singleton -- no entity_id yet. Show the raw
                                        // record with a Global ID form instead of a dead
                                        // click (this used to silently do nothing).
                                        setSelectedPair(null);
                                        setSelectedEntity(null);
                                        setSelectedSingleton(r.customer_code);
                                    }
                                }}
                                className="w-full flex justify-between text-xs p-2 rounded bg-gray-50 dark:bg-gray-900/50 hover:bg-gray-100 dark:hover:bg-gray-800 text-left"
                            >
                                <span className="font-mono">{r.customer_code}</span>
                                <span className="text-gray-600 dark:text-gray-300">{r.name_norm || '—'}</span>
                            </button>
                        ))}
                    </div>
                )}
            </motion.div>

            {navStack.length > 0 && (
                <button
                    onClick={goBack}
                    className="flex items-center gap-1.5 text-sm text-blue-600 dark:text-blue-400 hover:underline w-fit"
                >
                    <ArrowLeft size={14} /> Back to {navStack[navStack.length - 1].label}
                </button>
            )}

            {/* Population cards */}
            <div className="grid grid-cols-2 md:grid-cols-6 gap-3">
                <PopCard label="Needs Review" count={populations?.needs_review} active={population === 'REVIEW'} icon={AlertTriangle} accent="text-amber-500" onClick={() => { setNavStack([]); setSelectedSingleton(null); setPopulation('REVIEW'); }} />
                <PopCard label="Auto-Linked" count={populations?.auto_linked} active={population === 'AUTO_LINK'} icon={GitMerge} accent="text-emerald-500" onClick={() => { setNavStack([]); setSelectedSingleton(null); setPopulation('AUTO_LINK'); }} />
                <PopCard label="Rejected" count={populations?.rejected} active={population === 'REJECT'} icon={XCircle} accent="text-gray-400" onClick={() => { setNavStack([]); setSelectedSingleton(null); setPopulation('REJECT'); }} />
                <PopCard label="Entities" count={populations?.entities} active={population === 'ENTITIES'} icon={Users} accent="text-blue-500" onClick={() => { setNavStack([]); setSelectedSingleton(null); setPopulation('ENTITIES'); }} />
                <PopCard label="Approved" count={approvedCountData?.total} active={population === 'APPROVED'} icon={CheckCircle2} accent="text-emerald-500" onClick={() => { setNavStack([]); setSelectedSingleton(null); setPopulation('APPROVED'); }} />
                <PopCard label="Singletons" count={populations?.singletons} active={population === 'SINGLETONS'} icon={UserPlus} accent="text-purple-500" onClick={() => { setNavStack([]); setSelectedSingleton(null); setPopulation('SINGLETONS'); }} />
            </div>
            <div className="grid grid-cols-2 gap-3 text-xs text-gray-500 dark:text-gray-400">
                <div>With Global ID: <span className="font-semibold text-gray-700 dark:text-gray-300">{populations?.entities_with_global_ref?.toLocaleString() ?? '…'}</span></div>
                <div className={populations?.global_ref_conflicts ? 'text-red-500 font-semibold' : ''}>Global ID conflicts: {populations?.global_ref_conflicts ?? '…'}</div>
            </div>

            {/* Filters */}
            {population === 'ENTITIES' ? (
                <FilterBar mode="entities" value={entityFilters} onChange={setEntityFilters} />
            ) : population === 'SINGLETONS' ? (
                <FilterBar mode="singletons" value={singletonFilters} onChange={setSingletonFilters} />
            ) : population === 'APPROVED' ? (
                <div className="flex gap-1.5">
                    <button
                        onClick={() => setOverrideVerdict('MUST_LINK')}
                        className={`px-3 py-1.5 rounded-lg text-sm transition-colors ${overrideVerdict === 'MUST_LINK' ? 'bg-blue-600 text-white shadow-sm' : 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-200 dark:hover:bg-gray-600'}`}
                    >
                        Approved
                    </button>
                    <button
                        onClick={() => setOverrideVerdict('MUST_NOT_LINK')}
                        className={`px-3 py-1.5 rounded-lg text-sm transition-colors ${overrideVerdict === 'MUST_NOT_LINK' ? 'bg-blue-600 text-white shadow-sm' : 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-200 dark:hover:bg-gray-600'}`}
                    >
                        Rejected
                    </button>
                </div>
            ) : (
                <FilterBar mode="pairs" value={pairFilters} onChange={setPairFilters} />
            )}

            {actionError && (
                <div className="text-sm p-3 rounded-lg bg-red-50 dark:bg-red-900/20 text-red-700 dark:text-red-300">{actionError}</div>
            )}
            {!runId && (
                <div className="flex items-center gap-2 text-sm p-3 rounded-lg bg-amber-50 dark:bg-amber-900/20 text-amber-700 dark:text-amber-300">
                    <AlertTriangle size={16} /> Select a completed run above.
                </div>
            )}

            {/* Bulk toolbar */}
            {population !== 'ENTITIES' && selectedPairKeys.size > 0 && (
                <div className="flex items-center justify-between gap-3 p-3 rounded-lg bg-blue-50 dark:bg-blue-900/20 text-sm">
                    <span className="text-blue-700 dark:text-blue-300">
                        {selectedPairKeys.size} selected{selectedPairKeys.size >= BULK_SELECTION_CAP ? ` (cap ${BULK_SELECTION_CAP})` : ''}
                    </span>
                    <div className="flex gap-2">
                        <button onClick={() => setSelectedPairKeys(new Set())} className="btn btn-ghost !py-1 !px-2 text-xs">Clear</button>
                        <button onClick={() => setDialog({ action: 'reject', title: `Reject ${selectedPairKeys.size} matches`, bulk: true })} className="btn btn-ghost !py-1 !px-2 text-xs text-red-600">Bulk reject</button>
                        <button onClick={() => setDialog({ action: 'approve', title: `Approve ${selectedPairKeys.size} matches`, bulk: true })} className="btn btn-primary !py-1 !px-2 text-xs">Bulk approve</button>
                    </div>
                </div>
            )}
            {population === 'ENTITIES' && selectedEntityIds.size > 0 && (
                <div className="flex items-center justify-between gap-3 p-3 rounded-lg bg-blue-50 dark:bg-blue-900/20 text-sm">
                    <span className="text-blue-700 dark:text-blue-300">
                        {selectedEntityIds.size} selected{selectedEntityIds.size >= BULK_SELECTION_CAP ? ` (cap ${BULK_SELECTION_CAP})` : ''}
                    </span>
                    <div className="flex gap-2">
                        <button onClick={() => setSelectedEntityIds(new Set())} className="btn btn-ghost !py-1 !px-2 text-xs">Clear</button>
                        <button
                            onClick={() => setDialog({ action: 'merge', title: `Merge ${selectedEntityIds.size} entities`, bulk: true })}
                            disabled={selectedEntityIds.size < 2}
                            className="btn btn-primary !py-1 !px-2 text-xs disabled:opacity-40"
                        >
                            Merge selected
                        </button>
                    </div>
                </div>
            )}
            {bulkProgress && (
                <div className="p-3 rounded-lg bg-gray-50 dark:bg-gray-900/40 text-xs space-y-1">
                    <div className="flex items-center justify-between">
                        <span className="text-gray-600 dark:text-gray-300">
                            {bulkProgress.done < bulkProgress.total ? 'Working...' : 'Done'} -- {bulkProgress.done}/{bulkProgress.total}
                        </span>
                        {bulkProgress.done >= bulkProgress.total && (
                            <button onClick={() => setBulkProgress(null)} className="text-gray-400 hover:text-gray-700 dark:hover:text-gray-200">Dismiss</button>
                        )}
                    </div>
                    {bulkProgress.failures.length > 0 && (
                        <div className="text-red-500">
                            {bulkProgress.failures.length} failed:
                            <ul className="list-disc list-inside">
                                {bulkProgress.failures.map((f, i) => <li key={i}>{f}</li>)}
                            </ul>
                        </div>
                    )}
                </div>
            )}

            <div className="grid grid-cols-1 lg:grid-cols-5 gap-6">
                {/* List */}
                <motion.div initial={{ opacity: 0, y: 10 }} animate={{ opacity: 1, y: 0 }} className="glass-card p-6 lg:col-span-2">
                    <div className="flex items-center justify-between mb-4">
                        <h2 className="font-semibold text-gray-900 dark:text-white">
                            {population === 'ENTITIES' ? 'Entities' : population === 'SINGLETONS' ? 'Singletons' : population.replace('_', ' ')}
                        </h2>
                        {population !== 'ENTITIES' && population !== 'APPROVED' && population !== 'SINGLETONS' && (pairsData?.items || []).length > 0 && (
                            <label className="flex items-center gap-1.5 text-[11px] text-gray-400 cursor-pointer select-none">
                                <input
                                    type="checkbox"
                                    className="w-3.5 h-3.5"
                                    checked={(pairsData!.items as any[]).every((p) => selectedPairKeys.has(`${p.a_key}:::${p.b_key}`))}
                                    onChange={(e) => {
                                        const keys = (pairsData!.items as any[]).map((p) => `${p.a_key}:::${p.b_key}`);
                                        setSelectedPairKeys((prev) => {
                                            const next = new Set(prev);
                                            if (e.target.checked) keys.forEach((k) => next.size < BULK_SELECTION_CAP && next.add(k));
                                            else keys.forEach((k) => next.delete(k));
                                            return next;
                                        });
                                    }}
                                />
                                Select page
                            </label>
                        )}
                        {population === 'ENTITIES' && (entitiesData?.items || []).length > 0 && (
                            <label className="flex items-center gap-1.5 text-[11px] text-gray-400 cursor-pointer select-none">
                                <input
                                    type="checkbox"
                                    className="w-3.5 h-3.5"
                                    checked={(entitiesData!.items as any[]).every((e) => selectedEntityIds.has(e.entity_id))}
                                    onChange={(e) => {
                                        const ids = (entitiesData!.items as any[]).map((it) => it.entity_id);
                                        setSelectedEntityIds((prev) => {
                                            const next = new Set(prev);
                                            if (e.target.checked) ids.forEach((id) => next.size < BULK_SELECTION_CAP && next.add(id));
                                            else ids.forEach((id) => next.delete(id));
                                            return next;
                                        });
                                    }}
                                />
                                Select page
                            </label>
                        )}
                    </div>

                    {population === 'APPROVED' ? (
                        <div className="space-y-1.5 max-h-[520px] overflow-y-auto">
                            {overridesLoading && <p className="text-xs text-gray-400">Loading...</p>}
                            {!overridesLoading && (overridesData?.items || []).length === 0 && (
                                <p className="text-xs text-gray-400">No {overrideVerdict === 'MUST_LINK' ? 'approved' : 'rejected'} decisions yet.</p>
                            )}
                            {(overridesData?.items || []).map((it: any) => {
                                const isSelected = selectedPair?.a_key === it.a_code && selectedPair?.b_key === it.b_code;
                                return (
                                    <div
                                        key={it.override_id}
                                        role="button"
                                        tabIndex={0}
                                        onClick={() => { setSelectedSingleton(null); setSelectedPair({ a_key: it.a_code, b_key: it.b_code }); }}
                                        className={`w-full text-left p-2.5 rounded-lg border text-xs transition-colors cursor-pointer ${
                                            isSelected ? 'border-blue-400 bg-blue-50 dark:bg-blue-900/20' : 'border-gray-200 dark:border-gray-700 hover:bg-gray-50 dark:hover:bg-gray-900/40'
                                        }`}
                                    >
                                        <div className="flex justify-between items-center">
                                            <span className="font-medium text-gray-900 dark:text-white truncate">{it.a_name || it.a_code} ↔ {it.b_name || it.b_code}</span>
                                            {it.verdict === 'MUST_LINK' ? (
                                                <span className="flex items-center gap-0.5 text-emerald-600 dark:text-emerald-400 shrink-0"><CheckCircle2 size={11} /> Approved</span>
                                            ) : (
                                                <span className="flex items-center gap-0.5 text-red-500 shrink-0"><XCircle size={11} /> Rejected</span>
                                            )}
                                        </div>
                                        <div className="text-gray-400 font-mono mt-0.5">{it.a_code} · {it.b_code}</div>
                                        <div className="text-gray-500 dark:text-gray-400 mt-0.5">
                                            by {it.actor} · {new Date(it.created_at).toLocaleDateString()}
                                        </div>
                                    </div>
                                );
                            })}
                        </div>
                    ) : population === 'SINGLETONS' ? (
                        <div className="space-y-1.5 max-h-[520px] overflow-y-auto">
                            {singletonsListLoading && <p className="text-xs text-gray-400">Loading...</p>}
                            {!singletonsListLoading && (singletonsListData?.items || []).length === 0 && (
                                <p className="text-xs text-gray-400">No singletons match these filters.</p>
                            )}
                            {(singletonsListData?.items || []).map((s: any) => {
                                const TypeIcon = s.record_type === 'COMPANY' ? Building2 : UserIcon;
                                const isSelected = selectedSingleton === s.customer_code;
                                return (
                                    <div
                                        key={s.customer_code}
                                        role="button"
                                        tabIndex={0}
                                        onClick={() => { setSelectedPair(null); setSelectedEntity(null); setSelectedSingleton(s.customer_code); }}
                                        className={`w-full text-left p-2.5 rounded-lg border text-xs transition-colors cursor-pointer flex items-center justify-between gap-2 ${
                                            isSelected ? 'border-blue-400 bg-blue-50 dark:bg-blue-900/20' : 'border-gray-200 dark:border-gray-700 hover:bg-gray-50 dark:hover:bg-gray-900/40'
                                        }`}
                                    >
                                        <div className="min-w-0">
                                            <div className="font-medium text-gray-900 dark:text-white truncate">{s.name_norm || s.customer_code}</div>
                                            <div className="text-gray-400 font-mono mt-0.5">{s.customer_code}</div>
                                        </div>
                                        <TypeIcon size={12} className="text-gray-400 shrink-0" />
                                    </div>
                                );
                            })}
                        </div>
                    ) : population !== 'ENTITIES' ? (
                        <div className="space-y-1.5 max-h-[520px] overflow-y-auto">
                            {pairsLoading && <p className="text-xs text-gray-400">Loading...</p>}
                            {!pairsLoading && (pairsData?.items || []).length === 0 && <p className="text-xs text-gray-400">No pairs match these filters.</p>}
                            {(pairsData?.items || []).map((p: any) => {
                                const key = `${p.a_key}:::${p.b_key}`;
                                const isSelected = selectedPair?.a_key === p.a_key && selectedPair?.b_key === p.b_key;
                                return (
                                    <div
                                        key={key}
                                        role="button"
                                        tabIndex={0}
                                        onClick={() => { setSelectedSingleton(null); setSelectedPair({ a_key: p.a_key, b_key: p.b_key }); }}
                                        className={`w-full text-left p-2.5 rounded-lg border text-xs transition-colors cursor-pointer flex items-start gap-2 ${
                                            isSelected ? 'border-blue-400 bg-blue-50 dark:bg-blue-900/20' : 'border-gray-200 dark:border-gray-700 hover:bg-gray-50 dark:hover:bg-gray-900/40'
                                        }`}
                                    >
                                        <input
                                            type="checkbox"
                                            className="mt-0.5 w-3.5 h-3.5 shrink-0"
                                            checked={selectedPairKeys.has(key)}
                                            onClick={(e) => e.stopPropagation()}
                                            onChange={() => setSelectedPairKeys((prev) => toggleInSet(prev, key))}
                                        />
                                        <div className="flex-1 min-w-0">
                                            <div className="flex justify-between items-center">
                                                <span className="font-medium text-gray-900 dark:text-white truncate">{p.a_name || p.a_key} ↔ {p.b_name || p.b_key}</span>
                                                <span className={`font-mono ${p.has_veto ? 'text-red-500' : 'text-gray-600 dark:text-gray-300'}`}>{p.confidence_pct.toFixed(0)}%</span>
                                            </div>
                                            <div className="flex items-center justify-between mt-0.5">
                                                <span className="text-gray-400 font-mono">{p.a_key} · {p.b_key}</span>
                                                {p.officer_verdict === 'MUST_LINK' && (
                                                    <span className="flex items-center gap-0.5 text-emerald-600 dark:text-emerald-400"><CheckCircle2 size={11} /> Approved</span>
                                                )}
                                                {p.officer_verdict === 'MUST_NOT_LINK' && (
                                                    <span className="flex items-center gap-0.5 text-red-500"><XCircle size={11} /> Rejected</span>
                                                )}
                                            </div>
                                        </div>
                                    </div>
                                );
                            })}
                        </div>
                    ) : (
                        <div className="space-y-1.5 max-h-[520px] overflow-y-auto">
                            {entitiesLoading && <p className="text-xs text-gray-400">Loading...</p>}
                            {!entitiesLoading && (entitiesData?.items || []).length === 0 && <p className="text-xs text-gray-400">No entities match these filters.</p>}
                            {(entitiesData?.items || []).map((e: any) => {
                                const TypeIcon = e.record_type_preview === 'COMPANY' ? Building2 : UserIcon;
                                return (
                                    <div
                                        key={e.entity_id}
                                        role="button"
                                        tabIndex={0}
                                        onClick={() => { setSelectedSingleton(null); setSelectedEntity(e.entity_id); }}
                                        className={`w-full text-left p-2.5 rounded-lg border text-xs transition-colors cursor-pointer flex items-start gap-2 ${
                                            selectedEntity === e.entity_id ? 'border-blue-400 bg-blue-50 dark:bg-blue-900/20' : 'border-gray-200 dark:border-gray-700 hover:bg-gray-50 dark:hover:bg-gray-900/40'
                                        }`}
                                    >
                                        <input
                                            type="checkbox"
                                            className="mt-0.5 w-3.5 h-3.5 shrink-0"
                                            checked={selectedEntityIds.has(e.entity_id)}
                                            onClick={(ev) => ev.stopPropagation()}
                                            onChange={() => setSelectedEntityIds((prev) => toggleInSet(prev, e.entity_id))}
                                        />
                                        <div className="flex-1 min-w-0">
                                            <div className="flex justify-between items-center gap-2">
                                                <span className="font-medium text-gray-900 dark:text-white truncate">
                                                    {(e.member_names_preview || []).join(', ') || `(${e.entity_id.slice(0, 8)})`}
                                                    {e.member_count > (e.member_names_preview || []).length && (
                                                        <span className="text-gray-400"> +{e.member_count - e.member_names_preview.length} more</span>
                                                    )}
                                                </span>
                                                {e.record_type_preview && <TypeIcon size={12} className="text-gray-400 shrink-0" />}
                                            </div>
                                            <div className="flex justify-between items-center mt-0.5">
                                                <span className="font-mono text-gray-400">{e.entity_id.slice(0, 8)}</span>
                                                <span className="text-gray-500 dark:text-gray-400">{e.member_count} member{e.member_count === 1 ? '' : 's'}</span>
                                            </div>
                                            {e.global_ref && (
                                                <div className="flex items-center gap-1 mt-1 text-emerald-600 dark:text-emerald-400">
                                                    <IdCard size={11} /> {e.global_ref} <span className="text-gray-400">({e.global_ref_state})</span>
                                                </div>
                                            )}
                                        </div>
                                    </div>
                                );
                            })}
                        </div>
                    )}

                    <div className="flex justify-between items-center mt-4 pt-3 border-t border-gray-200 dark:border-gray-700 text-xs">
                        <button onClick={() => setPage((p) => Math.max(1, p - 1))} disabled={page <= 1} className="btn btn-ghost !py-1 !px-2 disabled:opacity-30">Previous</button>
                        <span className="text-gray-400">Page {page} of {totalPages.toLocaleString()}</span>
                        <button onClick={() => setPage((p) => Math.min(totalPages, p + 1))} disabled={page >= totalPages} className="btn btn-ghost !py-1 !px-2 disabled:opacity-30">Next</button>
                    </div>
                </motion.div>

                {/* Detail */}
                <motion.div initial={{ opacity: 0, y: 10 }} animate={{ opacity: 1, y: 0 }} transition={{ delay: 0.05 }} className="glass-card p-6 lg:col-span-3">
                    {selectedSingleton ? (
                        <SingletonPanel
                            customerCode={selectedSingleton}
                            record={singletonRecord || null}
                            loading={singletonRecordLoading}
                            globalRefInput={globalRefInput}
                            onGlobalRefInputChange={setGlobalRefInput}
                            onAssign={assignGlobalRefToSingleton}
                            assigning={assigningSingletonRef}
                        />
                    ) : population !== 'ENTITIES' ? (
                        selectedPair ? (
                            <>
                                <div className="flex items-center justify-between mb-4">
                                    <h2 className="font-semibold text-gray-900 dark:text-white">Side-by-side comparison</h2>
                                    {!breakdown?.officer_verdict && (
                                        <div className="flex gap-2">
                                            <button onClick={() => setDialog({ action: 'reject', title: 'Reject this match' })} className="btn btn-ghost !py-1.5 !px-3 text-xs gap-1 text-red-600">
                                                <XCircle size={14} /> Reject
                                            </button>
                                            <button onClick={() => setDialog({ action: 'approve', title: 'Approve this match' })} className="btn btn-primary !py-1.5 !px-3 text-xs gap-1">
                                                <CheckCircle2 size={14} /> Approve
                                            </button>
                                        </div>
                                    )}
                                </div>
                                {breakdown?.officer_verdict && (
                                    <div className={`flex items-start gap-2 p-3 rounded-lg text-xs mb-4 ${
                                        breakdown.officer_verdict === 'MUST_LINK' ? 'bg-emerald-50 dark:bg-emerald-900/20 text-emerald-700 dark:text-emerald-300' : 'bg-red-50 dark:bg-red-900/20 text-red-700 dark:text-red-300'
                                    }`}>
                                        {breakdown.officer_verdict === 'MUST_LINK' ? <CheckCircle2 size={14} className="mt-0.5 shrink-0" /> : <XCircle size={14} className="mt-0.5 shrink-0" />}
                                        <div className="flex-1">
                                            <div className="font-semibold">
                                                {breakdown.officer_verdict === 'MUST_LINK' ? 'Approved' : 'Rejected'} by {breakdown.officer_actor} on {new Date(breakdown.officer_decided_at).toLocaleString()}
                                            </div>
                                            <div className="opacity-90">"{breakdown.officer_reason}"</div>
                                            {breakdown.officer_verdict === 'MUST_LINK' && (
                                                <div className="opacity-90 mt-1">
                                                    Both records have already been merged into one entity{breakdown.officer_entity_id ? ' -- ready for a Global ID.' : '.'}
                                                </div>
                                            )}
                                            <div className="opacity-75 mt-1">
                                                This run's own stored score/decision above is never rewritten (that would falsify history) -- but the entity registry and clustering are already updated, and this verdict is re-applied on every future pipeline run too.
                                            </div>
                                            {breakdown.officer_entity_id && (
                                                <button
                                                    onClick={() => jumpToEntity(breakdown.officer_entity_id)}
                                                    className="flex items-center gap-1 mt-2 font-semibold hover:underline"
                                                >
                                                    View merged entity <ArrowLeft size={11} className="rotate-180" />
                                                </button>
                                            )}
                                        </div>
                                    </div>
                                )}
                                <RecordCompare recordA={recordA || null} recordB={recordB || null} contributions={breakdown?.contributions || []} onViewEntity={jumpToEntity} />
                                <h3 className="font-semibold text-gray-900 dark:text-white mt-6 mb-3 text-sm">Why this score</h3>
                                <ScoreBreakdown breakdown={breakdown || null} loading={breakdownLoading} />
                            </>
                        ) : (
                            <p className="text-xs text-gray-400">Select a pair to compare both records and see the score breakdown.</p>
                        )
                    ) : entityDetail ? (
                        <div className="space-y-5">
                            <div className="flex items-center justify-between">
                                <div>
                                    <h2 className="font-semibold text-gray-900 dark:text-white font-mono text-sm">{entityDetail.entity_id}</h2>
                                    <span className="badge badge-info !text-[10px] mt-1">{entityDetail.status}</span>
                                </div>
                                {entityDetail.status === 'MERGED' && (
                                    <span className="text-xs text-gray-400">Merged into {entityDetail.merged_into_entity_id?.slice(0, 8)}</span>
                                )}
                            </div>

                            <div>
                                <h3 className="text-xs font-semibold text-gray-500 dark:text-gray-400 uppercase tracking-wider mb-2">Global ID</h3>
                                {/* A RETIRED ref keeps its string for history (retire_global_ref never
                                    clears global_ref, only flips global_ref_state) -- but that's not an
                                    ACTIVE assignment, so it must not be treated like one here. Checking
                                    only global_ref's truthiness left retired entities permanently stuck
                                    showing a confirmed-looking badge with no way to assign a fresh ID. */}
                                {entityDetail.global_ref && entityDetail.global_ref_state !== 'RETIRED' ? (
                                    <div className="flex items-center justify-between p-2.5 rounded-lg bg-emerald-50 dark:bg-emerald-900/20">
                                        <span className="font-mono text-emerald-700 dark:text-emerald-300">{entityDetail.global_ref}</span>
                                        <span className="badge badge-success !text-[10px]">{entityDetail.global_ref_state}</span>
                                    </div>
                                ) : (
                                    <div className="space-y-2">
                                        {entityDetail.global_ref && entityDetail.global_ref_state === 'RETIRED' && (
                                            <div className="flex items-center justify-between p-2.5 rounded-lg bg-gray-50 dark:bg-gray-900/40 text-xs">
                                                <span className="font-mono text-gray-500 dark:text-gray-400">{entityDetail.global_ref}</span>
                                                <span className="badge !text-[10px]">RETIRED</span>
                                            </div>
                                        )}
                                        <div className="flex gap-2">
                                            <input value={globalRefInput} onChange={(e) => setGlobalRefInput(e.target.value)} placeholder="e.g. CIF-0012345" className="flex-1 text-xs py-1.5" />
                                            <button onClick={assignGlobalRef} disabled={!globalRefInput.trim()} className="btn btn-primary !py-1.5 !px-3 text-xs disabled:opacity-40">Assign</button>
                                        </div>
                                    </div>
                                )}
                            </div>

                            <div>
                                <div className="flex items-center justify-between mb-2">
                                    <h3 className="text-xs font-semibold text-gray-500 dark:text-gray-400 uppercase tracking-wider">Match evidence -- why this is one cluster</h3>
                                    {entityMatches && (
                                        <span className="text-[10px] text-gray-400">
                                            {entityMatches.directly_evidenced_pairs} of {entityMatches.possible_pairs} possible pairs directly compared
                                        </span>
                                    )}
                                </div>
                                {entityMatchesLoading && <p className="text-xs text-gray-400">Loading match evidence...</p>}
                                {entityMatches && entityMatches.items.length === 0 && (
                                    <p className="text-xs text-gray-400">No directly-scored pairs found for this run -- this entity was likely carried forward from an earlier run.</p>
                                )}
                                {entityMatches && entityMatches.items.length > 0 && (
                                    <>
                                        {entityMatches.possible_pairs > entityMatches.directly_evidenced_pairs && (
                                            <p className="text-[11px] text-amber-600 dark:text-amber-400 mb-2">
                                                {entityMatches.possible_pairs - entityMatches.directly_evidenced_pairs} member pair(s) were never directly compared -- connected only transitively through another member.
                                            </p>
                                        )}
                                        <div className="space-y-1 max-h-64 overflow-y-auto">
                                            {entityMatches.items.map((it: any) => {
                                                const key = `${it.a_key}:::${it.b_key}`;
                                                return (
                                                    <EntityMatchRow
                                                        key={key}
                                                        item={it}
                                                        runId={runId}
                                                        expanded={expandedMatchKey === key}
                                                        onToggle={() => setExpandedMatchKey((prev) => (prev === key ? null : key))}
                                                    />
                                                );
                                            })}
                                        </div>
                                    </>
                                )}
                            </div>

                            <div>
                                <div className="flex items-center justify-between mb-2">
                                    <h3 className="text-xs font-semibold text-gray-500 dark:text-gray-400 uppercase tracking-wider">Members ({entityDetail.members.length})</h3>
                                    <span className="text-[10px] text-gray-400">Click a member to see full details</span>
                                </div>
                                <div className="space-y-1">
                                    {entityDetail.members.map((m: any) => (
                                        <MemberRow
                                            key={m.customer_code}
                                            member={m}
                                            runId={runId}
                                            expanded={expandedMember === m.customer_code}
                                            onToggle={() => setExpandedMember((prev) => (prev === m.customer_code ? null : m.customer_code))}
                                            canSplit={entityDetail.members.length > 1}
                                            onSplit={() => { setSplitCode(m.customer_code); setDialog({ action: 'split', title: `Split ${m.customer_code} out` }); }}
                                        />
                                    ))}
                                </div>
                            </div>

                            <div>
                                <h3 className="text-xs font-semibold text-gray-500 dark:text-gray-400 uppercase tracking-wider mb-2">Merge with another entity</h3>
                                <div className="flex gap-2">
                                    <input value={mergeTarget} onChange={(e) => setMergeTarget(e.target.value)} placeholder="Target entity ID" className="flex-1 text-xs py-1.5 font-mono" />
                                    <button
                                        onClick={() => setDialog({ action: 'merge', title: 'Merge entities' })}
                                        disabled={!mergeTarget.trim()}
                                        className="btn btn-ghost !py-1.5 !px-3 text-xs gap-1 disabled:opacity-40"
                                    >
                                        <GitBranch size={13} /> Merge
                                    </button>
                                </div>
                            </div>

                            <div>
                                <h3 className="text-xs font-semibold text-gray-500 dark:text-gray-400 uppercase tracking-wider mb-2 flex items-center gap-1">
                                    <History size={12} /> Lineage
                                </h3>
                                <div className="space-y-1 max-h-48 overflow-y-auto">
                                    {entityDetail.lineage.map((l: any, i: number) => (
                                        <div key={i} className="text-[11px] p-2 rounded bg-gray-50 dark:bg-gray-900/40">
                                            <span className="font-semibold text-gray-700 dark:text-gray-300">{l.event}</span>
                                            {l.jaccard != null && <span className="text-gray-400"> · J={l.jaccard.toFixed(2)}</span>}
                                            <span className="text-gray-400"> · {l.actor} · {new Date(l.created_at).toLocaleString()}</span>
                                        </div>
                                    ))}
                                </div>
                            </div>
                        </div>
                    ) : (
                        <p className="text-xs text-gray-400">{entityDetailLoading ? 'Loading...' : 'Select an entity to see its detail.'}</p>
                    )}
                </motion.div>
            </div>

            {dialog && (
                <ReasonDialog
                    title={dialog.title}
                    action={dialog.action}
                    onCancel={() => setDialog(null)}
                    onConfirm={runDialogAction}
                />
            )}
        </div>
    );
}
