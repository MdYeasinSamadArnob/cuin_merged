'use client';

// Identity Graph 360 v2. Replaces the old page's "force-simulate every
// node in the dataset at once" approach (up to 20,000 raw dots, no
// pagination -- see /graph/v2's backend module docstring for the full
// story) with a paginated cluster-bubble overview (one bubble per
// cluster, sized/colored by member count) plus bounded drill-down and
// N-hop relationship views. Backed entirely by /graph/v2/* -- the
// legacy /graph/* endpoints, ClusterGraph.tsx, and TuningPanel.tsx
// (still used by /explorer, /pipeline, and /runs/[id]) are untouched.

import { useEffect, useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { motion } from 'framer-motion';
import {
    Search as SearchIcon, Building2, User as UserIcon, Users, Network, CircleDot,
    IdCard, RefreshCw, LayoutGrid, Waypoints, Sliders, Type, Link2,
} from 'lucide-react';
import { api } from '@/lib/api';
import { ClusterBubbleMap, TierLegend, ClusterItem } from '@/components/organisms/graph-explorer/ClusterBubbleMap';
import { ClusterDrillDown } from '@/components/organisms/graph-explorer/ClusterDrillDown';
import { SingletonRecordDetail } from '@/components/organisms/graph-explorer/SingletonRecordDetail';
import { HopExplorer } from '@/components/organisms/graph-explorer/HopExplorer';
import { ClassicClusterGraph } from '@/components/organisms/graph-explorer/ClassicClusterGraph';
import { BridgesPanel } from '@/components/organisms/graph-explorer/BridgesPanel';
// TuningPanel is reused read-only, exactly as /explorer already does --
// never modified. Its "Run Preview" button intentionally keeps calling
// the OLD api.previewClustering()/ClusterManager-backed endpoint (a
// what-if simulation feature, architecturally separate from "browse this
// run's real persisted clusters fast" -- swapping its data source isn't
// part of this work).
import { TuningPanel } from '@/components/organisms/frozen/TuningPanel';

function useDebounced<T>(value: T, delayMs: number): T {
    const [debounced, setDebounced] = useState(value);
    useEffect(() => {
        const t = setTimeout(() => setDebounced(value), delayMs);
        return () => clearTimeout(t);
    }, [value, delayMs]);
    return debounced;
}

function StatCard({ label, value, icon: Icon, accent }: { label: string; value: string | number | undefined; icon: any; accent: string }) {
    return (
        <div className="glass-card p-3">
            <div className="flex items-center gap-1.5 mb-1">
                <Icon size={13} className={accent} />
                <span className="text-[11px] text-gray-500 dark:text-gray-400">{label}</span>
            </div>
            <div className="text-lg font-bold text-gray-900 dark:text-white">
                {value === undefined ? '…' : typeof value === 'number' ? value.toLocaleString() : value}
            </div>
        </div>
    );
}

type RecordType = 'ALL' | 'COMPANY' | 'INDIVIDUAL';
// 60, not e.g. 120 -- at typical bubble radii, ~120 circles' combined
// area exceeds a reasonably-sized card's area, so some overlap becomes
// mathematically unavoidable no matter how good the layout is (confirmed
// live). 60 keeps the map legible; deep pagination stays fast regardless
// (backend Stage A benchmarked <150ms even at OFFSET 50000).
const PAGE_SIZE = 60;

export default function GraphPage() {
    const { data: runsData } = useQuery({ queryKey: ['g2-runs'], queryFn: () => api.wbListRuns(1, 50) });
    const completedRuns = (runsData?.runs || []).filter((r: any) => r.status === 'COMPLETED');
    const [runId, setRunId] = useState('');
    useEffect(() => {
        if (!runId && completedRuns.length > 0) setRunId(completedRuns[0].run_id);
    }, [completedRuns, runId]);

    // Overview filters
    const [population, setPopulation] = useState<'clusters' | 'singletons'>('clusters');
    const [page, setPage] = useState(1);
    const [sort, setSort] = useState<'size_desc' | 'size_asc'>('size_desc');
    const [minSize, setMinSize] = useState<number | undefined>(undefined);
    const [maxSize, setMaxSize] = useState<number | undefined>(undefined);
    const [recordType, setRecordType] = useState<RecordType>('ALL');
    const [hasGlobalRef, setHasGlobalRef] = useState<boolean | undefined>(undefined);
    const [q, setQ] = useState('');
    const debouncedQ = useDebounced(q, 400);
    useEffect(() => setPage(1), [runId, population, sort, minSize, maxSize, recordType, hasGlobalRef, debouncedQ]);

    const { data: stats } = useQuery({
        queryKey: ['g2-stats', runId],
        queryFn: () => api.graphStats(runId),
        enabled: !!runId,
    });

    const { data: overview, isFetching: overviewLoading } = useQuery({
        queryKey: ['g2-overview', runId, page, sort, minSize, maxSize, recordType, hasGlobalRef, debouncedQ],
        queryFn: () => api.graphOverview({
            runId, page, pageSize: PAGE_SIZE, sort, minSize, maxSize, recordType,
            hasGlobalRef, q: debouncedQ || undefined,
        }),
        enabled: !!runId && population === 'clusters',
    });

    // Singletons -- records with zero candidate_pairs edges, never
    // browsable before. Reshaped into the SAME ClusterItem shape
    // ClusterBubbleMap already renders (member_count: 1, size_tier:
    // 'small') so no component changes were needed to show them.
    const { data: singletonsData, isFetching: singletonsLoading } = useQuery({
        queryKey: ['g2-singletons', runId, page, recordType, debouncedQ],
        queryFn: () => api.graphSingletons({ runId, page, pageSize: PAGE_SIZE, recordType, q: debouncedQ || undefined }),
        enabled: !!runId && population === 'singletons',
    });

    // Selection: a drilled-into cluster, a singleton record, or a hop explore
    const [selectedEntityId, setSelectedEntityId] = useState<string | null>(null);
    const [selectedSingletonCode, setSelectedSingletonCode] = useState<string | null>(null);
    const [hopSeed, setHopSeed] = useState<{ customerCode: string; label: string } | null>(null);
    const [hopDepth, setHopDepth] = useState(2);

    const { data: clusterDetail, isFetching: clusterLoading } = useQuery({
        queryKey: ['g2-cluster', selectedEntityId, runId],
        queryFn: () => api.graphCluster(selectedEntityId!, runId),
        enabled: !!selectedEntityId && !hopSeed,
    });

    // External connections for whichever cluster is currently drilled into --
    // fed into ClusterDrillDown's "External connections" section.
    const { data: clusterBridgesData, isFetching: clusterBridgesLoading } = useQuery({
        queryKey: ['g2-cluster-bridges', selectedEntityId, runId],
        queryFn: () => api.graphClusterBridges(selectedEntityId!, runId),
        enabled: !!selectedEntityId && !hopSeed,
    });

    const { data: singletonRecord, isFetching: singletonRecordLoading } = useQuery({
        queryKey: ['g2-singleton-record', selectedSingletonCode, runId],
        queryFn: () => api.wbGetRecord(selectedSingletonCode!, runId),
        enabled: !!selectedSingletonCode && !hopSeed,
    });

    const { data: hopData, isFetching: hopLoading } = useQuery({
        queryKey: ['g2-hops', hopSeed?.customerCode, runId, hopDepth],
        queryFn: () => api.graphHops({ customerCode: hopSeed!.customerCode, runId, hops: hopDepth }),
        enabled: !!hopSeed,
    });

    const selectItem = (id: string) => {
        setHopSeed(null);
        if (population === 'singletons') {
            setSelectedSingletonCode(id);
            setSelectedEntityId(null);
        } else {
            setSelectedEntityId(id);
            setSelectedSingletonCode(null);
        }
    };
    const exploreFrom = (customerCode: string, label: string) => {
        setHopSeed({ customerCode, label });
    };

    const items: ClusterItem[] = population === 'singletons'
        ? (singletonsData?.items || []).map((s: any) => ({
              entity_id: s.customer_code, member_count: 1, record_type: s.record_type,
              size_tier: 'small' as const, representative_names: [s.name_norm || s.customer_code], global_ref: null,
          }))
        : (overview?.items || []);
    const currentTotal = population === 'singletons' ? (singletonsData?.total || 0) : (overview?.total || 0);
    const totalPages = Math.max(1, Math.ceil(currentTotal / PAGE_SIZE));

    // ---- Classic mode: the original force-directed graph, drag and all ----
    // Fine at the scale a human actually drags around interactively; the
    // problem was only ever using it as the DEFAULT, unbounded, un-paginated
    // view for the whole dataset. Kept as an explicit opt-in, now paginated
    // and filterable via /graph/v2/canvas instead of the legacy 2000-cluster
    // single fetch.
    const [viewMode, setViewMode] = useState<'overview' | 'classic' | 'bridges'>('overview');
    const [classicData, setClassicData] = useState<any>(null);
    const [classicLoading, setClassicLoading] = useState(false);
    const [showTuning, setShowTuning] = useState(false);
    const [showLabels, setShowLabels] = useState(true);

    const [classicPage, setClassicPage] = useState(1);
    const CLASSIC_PAGE_SIZE = 20;
    const [classicSort, setClassicSort] = useState<'size_desc' | 'size_asc'>('size_desc');
    const [classicRecordType, setClassicRecordType] = useState<RecordType>('ALL');
    const [classicMinSize, setClassicMinSize] = useState<number | undefined>(undefined);
    const [classicMaxSize, setClassicMaxSize] = useState<number | undefined>(undefined);
    const [classicQ, setClassicQ] = useState('');
    const debouncedClassicQ = useDebounced(classicQ, 400);
    const [classicTotal, setClassicTotal] = useState(0);
    useEffect(() => setClassicPage(1), [runId, classicSort, classicRecordType, classicMinSize, classicMaxSize, debouncedClassicQ]);

    const fetchClassicGraph = async () => {
        if (!runId) return;
        setClassicLoading(true);
        try {
            const data = await api.graphCanvas({
                runId, page: classicPage, pageSize: CLASSIC_PAGE_SIZE, sort: classicSort,
                minSize: classicMinSize, maxSize: classicMaxSize, recordType: classicRecordType,
                q: debouncedClassicQ || undefined,
            });
            setClassicData(data);
            setClassicTotal(data.total || 0);
        } catch (err) {
            console.error('Failed to fetch graph data:', err);
        } finally {
            setClassicLoading(false);
        }
    };

    useEffect(() => {
        if (viewMode === 'classic') fetchClassicGraph();
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [viewMode, runId, classicPage, classicSort, classicRecordType, classicMinSize, classicMaxSize, debouncedClassicQ]);

    const classicTotalPages = Math.max(1, Math.ceil(classicTotal / CLASSIC_PAGE_SIZE));

    // ---- Bridges: global cross-cluster connections analytics ----
    const [bridgesPage, setBridgesPage] = useState(1);
    const BRIDGES_PAGE_SIZE = 30;
    const [bridgesMinConfidence, setBridgesMinConfidence] = useState<number | undefined>(undefined);
    useEffect(() => setBridgesPage(1), [runId, bridgesMinConfidence]);

    const { data: bridgesData, isFetching: bridgesLoading } = useQuery({
        queryKey: ['g2-bridges', runId, bridgesPage, bridgesMinConfidence],
        queryFn: () => api.graphBridges({ runId, page: bridgesPage, pageSize: BRIDGES_PAGE_SIZE, minConfidence: bridgesMinConfidence }),
        enabled: !!runId && viewMode === 'bridges',
    });

    // Jump from a bridge (or a cluster's "External connections" section) to
    // that cluster in Overview -- reuses the exact same selection flow
    // Overview's own bubble clicks already go through.
    const jumpToEntity = (entityId: string) => {
        setViewMode('overview');
        setPopulation('clusters');
        setHopSeed(null);
        setSelectedSingletonCode(null);
        setSelectedEntityId(entityId);
    };

    const handlePreview = async (config: any) => {
        setClassicLoading(true);
        try {
            const data = await api.previewClustering(runId || undefined, config);
            setClassicData(data);
        } catch (err) {
            console.error('Preview failed:', err);
        } finally {
            setClassicLoading(false);
        }
    };

    const handleSaveConfig = async (config: any) => {
        try {
            await api.updateConfig({
                match_name_weight: config.name_weight,
                match_phone_weight: config.phone_weight,
                match_email_weight: config.email_weight,
                match_dob_weight: config.dob_weight,
                match_natid_weight: config.natid_weight,
                match_address_weight: config.address_weight,
                auto_link_threshold: config.auto_link_threshold,
                review_threshold: config.review_threshold,
            });
        } catch (err) {
            console.error('Failed to save config:', err);
        }
    };

    return (
        <div className="space-y-6">
            <div className="flex flex-wrap justify-between items-center gap-4">
                <div>
                    <h1 className="text-2xl font-bold text-gray-900 dark:text-white">Identity Graph 360</h1>
                    <p className="text-gray-600 dark:text-gray-400 mt-1 text-sm">
                        Every cluster, one bubble -- sized and colored by size, drill in for match evidence, or explore relationships hop by hop.
                    </p>
                </div>
                <div className="flex items-center gap-3">
                    <div className="flex gap-1 bg-gray-100 dark:bg-gray-800 rounded-lg p-1">
                        <button
                            onClick={() => setViewMode('overview')}
                            className={`flex items-center gap-1.5 px-3 py-1.5 rounded-md text-sm transition-colors ${viewMode === 'overview' ? 'bg-white dark:bg-gray-700 text-blue-600 dark:text-white shadow-sm' : 'text-gray-500 dark:text-gray-400 hover:text-gray-800 dark:hover:text-gray-200'}`}
                            title="Fast, paginated cluster overview"
                        >
                            <LayoutGrid size={14} /> Overview
                        </button>
                        <button
                            onClick={() => setViewMode('classic')}
                            className={`flex items-center gap-1.5 px-3 py-1.5 rounded-md text-sm transition-colors ${viewMode === 'classic' ? 'bg-white dark:bg-gray-700 text-blue-600 dark:text-white shadow-sm' : 'text-gray-500 dark:text-gray-400 hover:text-gray-800 dark:hover:text-gray-200'}`}
                            title="The original force-directed graph -- drag nodes, tune clustering live"
                        >
                            <Waypoints size={14} /> Classic
                        </button>
                        <button
                            onClick={() => setViewMode('bridges')}
                            className={`flex items-center gap-1.5 px-3 py-1.5 rounded-md text-sm transition-colors ${viewMode === 'bridges' ? 'bg-white dark:bg-gray-700 text-blue-600 dark:text-white shadow-sm' : 'text-gray-500 dark:text-gray-400 hover:text-gray-800 dark:hover:text-gray-200'}`}
                            title="Clusters connected by real matching evidence, but not merged"
                        >
                            <Link2 size={14} /> Bridges
                        </button>
                    </div>
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

            {viewMode === 'classic' && (
                <>
                    {/* Classic mode filters */}
                    <div className="flex flex-wrap items-center gap-3 bg-white dark:bg-gray-800/50 border border-gray-200 dark:border-gray-700 rounded-xl p-3">
                        <div className="relative flex-1 min-w-[200px]">
                            <SearchIcon size={14} className="absolute left-2.5 top-1/2 -translate-y-1/2 text-gray-400" />
                            <input
                                value={classicQ} onChange={(e) => setClassicQ(e.target.value)}
                                placeholder="Search cluster name, code, or Global ID..."
                                className="w-full text-sm py-1.5" style={{ paddingLeft: '1.75rem' }}
                            />
                        </div>
                        <div className="flex gap-1.5">
                            {(['ALL', 'COMPANY', 'INDIVIDUAL'] as RecordType[]).map((rt) => (
                                <button
                                    key={rt} onClick={() => setClassicRecordType(rt)}
                                    className={`px-3 py-1.5 rounded-lg text-sm transition-colors ${classicRecordType === rt ? 'bg-blue-600 text-white shadow-sm' : 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-200 dark:hover:bg-gray-600'}`}
                                >
                                    {rt === 'ALL' ? 'All' : rt === 'COMPANY' ? 'Companies' : 'Individuals'}
                                </button>
                            ))}
                        </div>
                        <div className="flex gap-1.5">
                            <button
                                onClick={() => setClassicSort('size_desc')}
                                className={`px-3 py-1.5 rounded-lg text-sm transition-colors ${classicSort === 'size_desc' ? 'bg-blue-600 text-white shadow-sm' : 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-200 dark:hover:bg-gray-600'}`}
                            >
                                Highest clusters
                            </button>
                            <button
                                onClick={() => setClassicSort('size_asc')}
                                className={`px-3 py-1.5 rounded-lg text-sm transition-colors ${classicSort === 'size_asc' ? 'bg-blue-600 text-white shadow-sm' : 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-200 dark:hover:bg-gray-600'}`}
                            >
                                Smallest first
                            </button>
                        </div>
                        <div className="flex items-center gap-1.5 text-xs text-gray-500 dark:text-gray-400">
                            <span>Size</span>
                            <input
                                type="number" min={2} placeholder="min" className="w-14 text-xs py-1 px-1.5 text-center"
                                value={classicMinSize ?? ''} onChange={(e) => setClassicMinSize(e.target.value === '' ? undefined : Number(e.target.value))}
                            />
                            <span>to</span>
                            <input
                                type="number" min={2} placeholder="max" className="w-14 text-xs py-1 px-1.5 text-center"
                                value={classicMaxSize ?? ''} onChange={(e) => setClassicMaxSize(e.target.value === '' ? undefined : Number(e.target.value))}
                            />
                        </div>
                    </div>

                    <motion.div initial={{ opacity: 0, y: 10 }} animate={{ opacity: 1, y: 0 }} className="glass-card relative overflow-hidden" style={{ height: 680 }}>
                        <div className="absolute top-3 right-3 z-10 flex items-center gap-2">
                            <button
                                onClick={() => setShowTuning(!showTuning)}
                                className={`p-2 rounded-lg border transition-colors ${showTuning ? 'bg-blue-600 border-blue-500 text-white' : 'bg-white dark:bg-gray-800 border-gray-200 dark:border-gray-700 text-gray-500 dark:text-gray-400 hover:text-gray-900 dark:hover:text-white'}`}
                                title="Toggle Tuning Panel"
                            >
                                <Sliders size={16} />
                            </button>
                            <button
                                onClick={() => setShowLabels(!showLabels)}
                                className={`p-2 rounded-lg border transition-colors ${showLabels ? 'bg-blue-100 dark:bg-blue-900/50 border-blue-200 dark:border-blue-800 text-blue-600 dark:text-blue-400' : 'bg-white dark:bg-gray-800 border-gray-200 dark:border-gray-700 text-gray-500 dark:text-gray-400 hover:text-gray-900 dark:hover:text-white'}`}
                                title={showLabels ? 'Hide Labels' : 'Show Labels'}
                            >
                                <Type size={16} />
                            </button>
                            <button
                                onClick={() => fetchClassicGraph()}
                                className="p-2 rounded-lg bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 text-gray-500 dark:text-gray-400 hover:text-gray-900 dark:hover:text-white hover:bg-gray-50 dark:hover:bg-gray-700 transition-colors"
                                title="Refresh Data"
                            >
                                <RefreshCw size={16} />
                            </button>
                        </div>

                        <div className="w-full h-full relative">
                            <ClassicClusterGraph data={classicData} loading={classicLoading} showLabels={showLabels} />
                        </div>

                        {showTuning && (
                            <div className="w-80 border-l border-gray-200 dark:border-gray-800 bg-white/95 dark:bg-gray-900/95 backdrop-blur absolute right-0 top-0 bottom-0 z-10 shadow-2xl overflow-y-auto">
                                <div className="p-4">
                                    <h2 className="text-sm font-semibold text-gray-500 dark:text-gray-400 uppercase tracking-wider mb-4 flex items-center gap-2">
                                        <Sliders size={14} /> Clustering Logic
                                    </h2>
                                    <TuningPanel
                                        onPreview={handlePreview} onSave={handleSaveConfig}
                                        loading={classicLoading} isOpen={showTuning} setIsOpen={setShowTuning}
                                    />
                                </div>
                            </div>
                        )}
                    </motion.div>

                    <div className="flex justify-between items-center text-xs">
                        <button onClick={() => setClassicPage((p) => Math.max(1, p - 1))} disabled={classicPage <= 1} className="btn btn-ghost !py-1 !px-2 disabled:opacity-30">Previous</button>
                        <span className="text-gray-400">Page {classicPage} of {classicTotalPages.toLocaleString()} -- {classicTotal.toLocaleString()} clusters</span>
                        <button onClick={() => setClassicPage((p) => Math.min(classicTotalPages, p + 1))} disabled={classicPage >= classicTotalPages} className="btn btn-ghost !py-1 !px-2 disabled:opacity-30">Next</button>
                    </div>
                </>
            )}

            {viewMode === 'bridges' && (
                <BridgesPanel
                    items={bridgesData?.items || []}
                    loading={bridgesLoading}
                    total={bridgesData?.total || 0}
                    page={bridgesPage}
                    pageSize={BRIDGES_PAGE_SIZE}
                    onPageChange={setBridgesPage}
                    onJump={jumpToEntity}
                    minConfidence={bridgesMinConfidence}
                    onMinConfidenceChange={setBridgesMinConfidence}
                />
            )}

            {viewMode === 'overview' && (
            <>
            {/* Insights bar */}
            <div className="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-7 gap-2.5">
                <StatCard label="Clusters" value={stats?.total_clusters} icon={CircleDot} accent="text-blue-500" />
                <StatCard label="Records" value={stats?.total_records} icon={Users} accent="text-cyan-500" />
                <StatCard label="Singletons" value={stats?.singletons} icon={UserIcon} accent="text-gray-400" />
                <StatCard label="Largest cluster" value={stats?.largest_cluster_size} icon={Network} accent="text-orange-500" />
                <StatCard label="Avg size" value={stats?.avg_cluster_size} icon={CircleDot} accent="text-emerald-500" />
                <StatCard label="Companies" value={stats?.company_records} icon={Building2} accent="text-amber-500" />
                <StatCard label="With Global ID" value={stats?.entities_with_global_ref} icon={IdCard} accent="text-emerald-500" />
            </div>

            {/* Filters */}
            <div className="flex flex-wrap items-center gap-3 bg-white dark:bg-gray-800/50 border border-gray-200 dark:border-gray-700 rounded-xl p-3">
                <div className="flex gap-1.5">
                    <button
                        onClick={() => { setPopulation('clusters'); setSelectedSingletonCode(null); }}
                        className={`px-3 py-1.5 rounded-lg text-sm transition-colors ${population === 'clusters' ? 'bg-blue-600 text-white shadow-sm' : 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-200 dark:hover:bg-gray-600'}`}
                    >
                        Clusters
                    </button>
                    <button
                        onClick={() => { setPopulation('singletons'); setSelectedEntityId(null); }}
                        className={`px-3 py-1.5 rounded-lg text-sm transition-colors ${population === 'singletons' ? 'bg-blue-600 text-white shadow-sm' : 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-200 dark:hover:bg-gray-600'}`}
                        title="Records with no candidate match at all"
                    >
                        Singletons
                    </button>
                </div>
                <div className="relative flex-1 min-w-[200px]">
                    <SearchIcon size={14} className="absolute left-2.5 top-1/2 -translate-y-1/2 text-gray-400" />
                    <input
                        value={q} onChange={(e) => setQ(e.target.value)}
                        placeholder={population === 'singletons' ? 'Search name or code...' : 'Search cluster name, code, or Global ID...'}
                        className="w-full text-sm py-1.5" style={{ paddingLeft: '1.75rem' }}
                    />
                </div>
                <div className="flex gap-1.5">
                    {(['ALL', 'COMPANY', 'INDIVIDUAL'] as RecordType[]).map((rt) => (
                        <button
                            key={rt} onClick={() => setRecordType(rt)}
                            className={`px-3 py-1.5 rounded-lg text-sm transition-colors ${recordType === rt ? 'bg-blue-600 text-white shadow-sm' : 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-200 dark:hover:bg-gray-600'}`}
                        >
                            {rt === 'ALL' ? 'All' : rt === 'COMPANY' ? 'Companies' : 'Individuals'}
                        </button>
                    ))}
                </div>
                {population === 'clusters' && (
                    <>
                        <div className="flex gap-1.5">
                            <button
                                onClick={() => setSort('size_desc')}
                                className={`px-3 py-1.5 rounded-lg text-sm transition-colors ${sort === 'size_desc' ? 'bg-blue-600 text-white shadow-sm' : 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-200 dark:hover:bg-gray-600'}`}
                            >
                                Highest clusters
                            </button>
                            <button
                                onClick={() => setSort('size_asc')}
                                className={`px-3 py-1.5 rounded-lg text-sm transition-colors ${sort === 'size_asc' ? 'bg-blue-600 text-white shadow-sm' : 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-200 dark:hover:bg-gray-600'}`}
                            >
                                Smallest first
                            </button>
                        </div>
                        <div className="flex items-center gap-1.5 text-xs text-gray-500 dark:text-gray-400">
                            <span>Size</span>
                            <input
                                type="number" min={2} placeholder="min" className="w-14 text-xs py-1 px-1.5 text-center"
                                value={minSize ?? ''} onChange={(e) => setMinSize(e.target.value === '' ? undefined : Number(e.target.value))}
                            />
                            <span>to</span>
                            <input
                                type="number" min={2} placeholder="max" className="w-14 text-xs py-1 px-1.5 text-center"
                                value={maxSize ?? ''} onChange={(e) => setMaxSize(e.target.value === '' ? undefined : Number(e.target.value))}
                            />
                        </div>
                        <label className="flex items-center gap-1.5 text-xs text-gray-500 dark:text-gray-400 cursor-pointer select-none">
                            <input
                                type="checkbox" className="w-3.5 h-3.5"
                                checked={hasGlobalRef === true}
                                onChange={(e) => setHasGlobalRef(e.target.checked ? true : undefined)}
                            />
                            Has Global ID
                        </label>
                    </>
                )}
                <TierLegend />
            </div>

            <div className="grid grid-cols-1 lg:grid-cols-5 gap-6">
                <motion.div initial={{ opacity: 0, y: 10 }} animate={{ opacity: 1, y: 0 }} className="glass-card p-4 lg:col-span-3 flex flex-col" style={{ minHeight: 680 }}>
                    <div className="flex items-center justify-between mb-2">
                        <span className="text-xs text-gray-500 dark:text-gray-400">
                            {currentTotal.toLocaleString()} {population === 'singletons' ? 'singleton records' : 'clusters'}
                        </span>
                        {(population === 'singletons' ? singletonsLoading : overviewLoading) && (
                            <RefreshCw size={12} className="animate-spin text-gray-400" />
                        )}
                    </div>
                    <div className="flex-1 min-h-0">
                        <ClusterBubbleMap
                            items={items}
                            maxClusterSize={overview?.max_cluster_size || 12}
                            onSelect={selectItem}
                            selectedId={population === 'singletons' ? selectedSingletonCode : selectedEntityId}
                        />
                    </div>
                    <div className="flex justify-between items-center mt-3 pt-3 border-t border-gray-200 dark:border-gray-700 text-xs">
                        <button onClick={() => setPage((p) => Math.max(1, p - 1))} disabled={page <= 1} className="btn btn-ghost !py-1 !px-2 disabled:opacity-30">Previous</button>
                        <span className="text-gray-400">Page {page} of {totalPages.toLocaleString()}</span>
                        <button onClick={() => setPage((p) => Math.min(totalPages, p + 1))} disabled={page >= totalPages} className="btn btn-ghost !py-1 !px-2 disabled:opacity-30">Next</button>
                    </div>
                </motion.div>

                <motion.div initial={{ opacity: 0, y: 10 }} animate={{ opacity: 1, y: 0 }} transition={{ delay: 0.05 }} className="glass-card p-4 lg:col-span-2">
                    {hopSeed ? (
                        <HopExplorer
                            data={hopData || null} loading={hopLoading} seedLabel={hopSeed.label}
                            depth={hopDepth} onDepthChange={setHopDepth} onClose={() => setHopSeed(null)}
                        />
                    ) : population === 'singletons' ? (
                        <SingletonRecordDetail data={singletonRecord || null} loading={singletonRecordLoading} onExploreFrom={exploreFrom} />
                    ) : (
                        <ClusterDrillDown
                            data={clusterDetail || null} loading={clusterLoading} onExploreFrom={exploreFrom}
                            bridges={clusterBridgesData?.bridges} bridgesLoading={clusterBridgesLoading} onJumpToEntity={jumpToEntity}
                        />
                    )}
                </motion.div>
            </div>
            </>
            )}
        </div>
    );
}
