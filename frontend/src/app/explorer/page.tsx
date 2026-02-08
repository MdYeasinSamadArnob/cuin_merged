'use client';

import { useState, useEffect } from 'react';
import { useSearchParams } from 'next/navigation';
import { api } from '@/lib/api';
import { Search, CheckCircle, XCircle, LayoutGrid, List } from 'lucide-react';
import { TuningPanel } from '@/components/explorer/TuningPanel';
import { ClusterGraph } from '@/components/explorer/ClusterGraph';

interface MatchScore {
    pair_id: string;
    a_key: string;
    b_key: string;
    score: number;
    signals_hit: string[];
    hard_conflicts: string[];
}

interface RunInfo {
    run_id: string;
    status: string;
    counters: {
        auto_links: number;
        review_items: number;
        rejected: number;
        pairs_scored: number;
    };
}

const formatFieldValue = (value: any) => {
    if (value === null || value === undefined) return '—';
    if (typeof value === 'object') return JSON.stringify(value);
    return String(value);
};

const getDisplayName = (record: any) => {
    if (!record) return 'Unknown';
    const name = record.name_norm;
    // Check if name is valid (not null, not "—", not starting with "UNKNOWN")
    if (name && name !== '—' && !String(name).toUpperCase().startsWith('UNKNOWN')) {
        return name;
    }
    
    // Fallback chain
    if (record.email_norm && record.email_norm !== '—') return record.email_norm;
    if (record.phone_norm && record.phone_norm !== '—') return record.phone_norm;
    if (record.source_customer_id && record.source_customer_id !== '—') return record.source_customer_id;
    
    return 'Unknown Entity';
};

const RecordCard = ({ title, data, color }: { title: string, data: any, color: string }) => (
    <div className={`p-4 rounded-xl border ${color === 'blue' ? 'bg-blue-50 dark:bg-blue-900/10 border-blue-200 dark:border-blue-800' : 'bg-purple-50 dark:bg-purple-900/10 border-purple-200 dark:border-purple-800'}`}>
        <div className="flex items-center gap-2 mb-3">
            <div className={`w-2 h-2 rounded-full ${color === 'blue' ? 'bg-blue-500 dark:bg-blue-400' : 'bg-purple-500 dark:bg-purple-400'}`} />
            <h3 className="font-medium text-gray-900 dark:text-white">{title}</h3>
            {/* Rich Metadata Badges */}
            {data.metadata?.status && (
                <span className={`text-[10px] px-1.5 py-0.5 rounded border uppercase ${data.metadata.status === 'ACT' ? 'bg-green-100 dark:bg-green-900/30 border-green-200 dark:border-green-800 text-green-600 dark:text-green-400' :
                    data.metadata.status === 'SUSP' ? 'bg-red-100 dark:bg-red-900/30 border-red-200 dark:border-red-800 text-red-600 dark:text-red-400' :
                        'bg-gray-100 dark:bg-gray-800 border-gray-200 dark:border-gray-700 text-gray-500 dark:text-gray-400'
                    }`}>
                    {data.metadata.status}
                </span>
            )}
            {data.metadata?.cust_type && (
                <span className="text-[10px] px-1.5 py-0.5 rounded border border-gray-200 dark:border-gray-700 bg-gray-100 dark:bg-gray-800 text-gray-600 dark:text-gray-300">
                    {data.metadata.cust_type}
                </span>
            )}
        </div>

        <div className="space-y-2 text-sm">
            <div className="flex justify-between">
                <span className="text-gray-500 dark:text-gray-400">ID</span>
                <span className="text-gray-700 dark:text-gray-300 font-mono">{formatFieldValue(data.source_customer_id || '—')}</span>
            </div>
            <div className="flex justify-between">
                <span className="text-gray-500 dark:text-gray-400">Name</span>
                <span className="text-gray-900 dark:text-white font-medium">{getDisplayName(data)}</span>
            </div>
            <div className="flex justify-between">
                <span className="text-gray-500 dark:text-gray-400">Phone</span>
                <span className="text-gray-700 dark:text-gray-300">{formatFieldValue(data.phone_norm)}</span>
            </div>
            <div className="flex justify-between">
                <span className="text-gray-500 dark:text-gray-400">Email</span>
                <span className="text-gray-700 dark:text-gray-300 truncate max-w-[150px]" title={data.email_norm}>
                    {formatFieldValue(data.email_norm)}
                </span>
            </div>
            <div className="flex justify-between">
                <span className="text-gray-500 dark:text-gray-400">DOB</span>
                <span className="text-gray-700 dark:text-gray-300">{formatFieldValue(data.dob_norm)}</span>
            </div>
            <div className="col-span-2 pt-2 border-t border-gray-200 dark:border-gray-800/50 mt-2">
                <div className="text-xs text-gray-500 dark:text-gray-400 mb-1 uppercase tracking-wider">Address</div>
                <div className="text-gray-600 dark:text-gray-400 text-xs leading-relaxed">{formatFieldValue(data.address_norm)}</div>
            </div>

            {/* Extra Metadata Grid */}
            {data.metadata && Object.keys(data.metadata).length > 0 && (
                <div className="grid grid-cols-2 gap-2 pt-2 border-t border-gray-200 dark:border-gray-800/50 mt-2">
                    {data.metadata.gender && (
                        <div className="flex flex-col">
                            <span className="text-[10px] text-gray-500 dark:text-gray-400">Gender</span>
                            <span className="text-xs text-gray-700 dark:text-gray-300">{data.metadata.gender}</span>
                        </div>
                    )}
                    {data.metadata.branch && (
                        <div className="flex flex-col">
                            <span className="text-[10px] text-gray-500 dark:text-gray-400">Branch</span>
                            <span className="text-xs text-gray-700 dark:text-gray-300">{data.metadata.branch}</span>
                        </div>
                    )}
                    {data.metadata.sponsor && (
                        <div className="flex flex-col col-span-2">
                            <span className="text-[10px] text-gray-500 dark:text-gray-400">Sponsor</span>
                            <span className="text-xs text-gray-700 dark:text-gray-300">{data.metadata.sponsor}</span>
                        </div>
                    )}
                </div>
            )}
        </div>
    </div>
);

export default function ExplorerPage() {
    const [runs, setRuns] = useState<RunInfo[]>([]);
    const [selectedRunId, setSelectedRunId] = useState<string | null>(null);
    const [scores, setScores] = useState<MatchScore[]>([]);
    const [selectedMatch, setSelectedMatch] = useState<any | null>(null);
    const [isLoading, setIsLoading] = useState(true);
    const [isLoadingDetails, setIsLoadingDetails] = useState(false);
    const [isExplaining, setIsExplaining] = useState(false);
    const [explanation, setExplanation] = useState<any | null>(null);

    const [filter, setFilter] = useState<'all' | 'auto_link' | 'review' | 'reject' | 'unique' | 'entities'>('all');
    const [minScore, setMinScore] = useState(0);

    // Pagination state
    const [currentPage, setCurrentPage] = useState(1);
    const [pageSize] = useState(50);
    const [totalScores, setTotalScores] = useState(0);
    
    // Graph / Tuning View State
    const [viewMode, setViewMode] = useState<'list' | 'graph'>('list');
    const [previewData, setPreviewData] = useState<any>(null);
    const [previewLoading, setPreviewLoading] = useState(false);
    const [loadingMessage, setLoadingMessage] = useState("Running Clustering Algorithm...");
    const [isTuningOpen, setIsTuningOpen] = useState(false);

    useEffect(() => {
        if (viewMode === 'graph' && !previewData && selectedRunId) {
            setPreviewLoading(true);
            setLoadingMessage("Loading Graph Data...");
            (api as any).getGraphData(selectedRunId)
                .then((data: any) => setPreviewData(data))
                .catch((err: any) => console.error('Failed to load graph:', err))
                .finally(() => setPreviewLoading(false));
        }
    }, [viewMode, selectedRunId]);

    const handlePreview = async (config: any) => {
        setPreviewLoading(true);
        setLoadingMessage("Running Clustering Algorithm...");
        try {
            const data = await (api as any).previewClustering(selectedRunId, config);
            setPreviewData(data);
        } catch (err) {
            console.error('Preview failed:', err);
            // Optionally set error state to show in UI
        } finally {
            setPreviewLoading(false);
        }
    };

    const handleSaveConfig = async (config: any) => {
        try {
            // Map to UpdateConfigRequest format
            const updatePayload = {
                match_name_weight: config.name_weight,
                match_phone_weight: config.phone_weight,
                match_email_weight: config.email_weight,
                match_dob_weight: config.dob_weight,
                match_natid_weight: config.natid_weight,
                match_address_weight: config.address_weight,
                auto_link_threshold: config.auto_link_threshold,
                review_threshold: config.review_threshold
            };
            await api.updateConfig(updatePayload);
            console.log('Config saved');
        } catch (err) {
            console.error('Failed to save config:', err);
        }
    };

    const searchParams = useSearchParams();
    const urlRunId = searchParams.get('runId');

    useEffect(() => {
        fetchRuns();
    }, []);

    useEffect(() => {
        if (urlRunId && runs.length > 0) {
            setSelectedRunId(urlRunId);
        } else if (runs.length > 0 && !selectedRunId) {
            setSelectedRunId(runs[0].run_id);
        }
    }, [urlRunId, runs]);

    useEffect(() => {
        if (selectedRunId) {
            // Reset to page 1 when filters change
            setCurrentPage(1);
            if (filter === 'unique') {
                fetchUniques(selectedRunId, 1);
            } else if (filter === 'entities') {
                fetchClusters(selectedRunId, 1);
            } else {
                fetchScores(selectedRunId, 1);
            }
        }
    }, [selectedRunId, filter, minScore]);

    useEffect(() => {
        if (selectedRunId) {
            if (filter === 'unique') {
                fetchUniques(selectedRunId, currentPage);
            } else if (filter === 'entities') {
                fetchClusters(selectedRunId, currentPage);
            } else {
                fetchScores(selectedRunId, currentPage);
            }
        }
    }, [currentPage]);

    // ... (fetchRun existing ... )

    const fetchRuns = async () => {
        try {
            const data = await api.listRuns(1, 10);
            setRuns(data.runs);
            if (data.runs.length > 0) {
                setSelectedRunId(data.runs[0].run_id);
            }
        } catch (err) {
            console.error('Failed to fetch runs:', err);
        } finally {
            setIsLoading(false);
        }
    };

    const fetchClusters = async (runId: string, page: number) => {
        try {
            const data = await (api as any).getClusters(runId, page, pageSize);
            setScores(data.clusters?.map((c: any) => ({
                pair_id: c.cluster_id,
                a_key: c.representative_record.customer_key || c.representative_record.source_customer_id,
                b_key: '',
                score: c.size > 1 ? 1.0 : 0.0,
                signals_hit: [],
                hard_conflicts: [],
                _is_cluster: true,
                _cluster: c
            })) || []);
            setTotalScores(data.total || 0);
            setSelectedMatch(null);
        } catch (err) {
            console.error('Failed to fetch clusters:', err);
            setScores([]);
        }
    };

    const fetchUniques = async (runId: string, page: number) => {
        try {
            const data = await (api as any).getUniques(runId, page, pageSize);
            setScores(data.records?.map((r: any) => ({
                // Mock score object for viewing
                pair_id: r.customer_key || r.source_customer_id,
                a_key: r.customer_key || r.source_customer_id,
                b_key: '',
                score: 0,
                signals_hit: [],
                hard_conflicts: [],
                _is_unique: true,
                _record: r
            })) || []);
            setTotalScores(data.total || 0);
            setSelectedMatch(null); // Clear detail view
        } catch (err) {
            console.error('Failed to fetch uniques:', err);
            setScores([]);
        }
    };

    const fetchScores = async (runId: string, page: number) => {
        try {
            const data = await api.getMatchScores(runId, page, pageSize, minScore > 0 ? minScore : undefined);
            setScores(data.scores || []);
            setTotalScores(data.total || 0);
        } catch (err) {
            console.error('Failed to fetch scores:', err);
            setScores([]);
        }
    };



    const handleAskReferee = async () => {
        if (!selectedMatch) return;
        setIsExplaining(true);
        try {
            // Support both regular pairs and clusters (using cluster_id as pair_id)
            const id = selectedMatch.pair_id || selectedMatch._cluster?.cluster_id;
            const result = await (api as any).explainMatch(id);
            setExplanation(result);
        } catch (err) {
            console.error("Referee failed:", err);
        } finally {
            setIsExplaining(false);
        }
    };

    // Clear explanation when changing matches or filters
    useEffect(() => {
        setExplanation(null);
    }, [selectedRunId, filter]);

    // Helper to find related matches (transitivity)
    const getRelatedMatches = (currentPairId: string, recordA: any, recordB: any) => {
        if (!recordA || !recordB) return [];

        const keyA = recordA.customer_key || recordA.source_customer_id;
        const keyB = recordB.customer_key || recordB.source_customer_id;

        return scores.filter(s =>
            s.pair_id !== currentPairId &&
            (s.a_key === keyA || s.a_key === keyB || s.b_key === keyA || s.b_key === keyB)
        );
    };

    const fetchMatchDetails = async (pairId: string) => {
        setIsLoadingDetails(true);
        try {
            const details = await api.getMatchDetails(pairId);
            setSelectedMatch(details);
        } catch (err) {
            console.error('Failed to fetch match details:', err);
        } finally {
            setIsLoadingDetails(false);
        }
    };

    const getScoreColor = (score: number) => {
        if (score >= 0.85) return 'text-emerald-600 dark:text-emerald-400';
        if (score >= 0.6) return 'text-yellow-600 dark:text-yellow-400';
        if (score >= 0.4) return 'text-orange-600 dark:text-orange-400';
        return 'text-red-600 dark:text-red-400';
    };

    const getScoreBg = (score: number) => {
        if (score >= 0.85) return 'bg-emerald-50 dark:bg-emerald-900/30 border-emerald-200 dark:border-emerald-700';
        if (score >= 0.6) return 'bg-yellow-50 dark:bg-yellow-900/30 border-yellow-200 dark:border-yellow-700';
        if (score >= 0.4) return 'bg-orange-50 dark:bg-orange-900/30 border-orange-200 dark:border-orange-700';
        return 'bg-red-50 dark:bg-red-900/30 border-red-200 dark:border-red-700';
    };

    // formatFieldValue moved to top scope

    if (isLoading) {
        return (
            <div className="p-8 flex items-center justify-center min-h-screen">
                <div className="text-gray-400">Loading explorer...</div>
            </div>
        );
    }

    return (
        <div className="p-8 space-y-6">
            {/* Header */}
            <div className="flex items-center justify-between">
                <div>
                    <h1 className="text-3xl font-bold text-gray-900 dark:text-white">Match Explorer</h1>
                    <p className="text-gray-500 dark:text-gray-400 mt-1">
                        Explore and compare match pairs side by side
                    </p>
                </div>
            </div>

            {/* Filters */}
            <div className="flex flex-wrap items-center gap-4 bg-white dark:bg-gray-800/50 border border-gray-200 dark:border-gray-700 rounded-xl p-4 shadow-sm">
                <div>
                    <label className="block text-sm text-gray-500 dark:text-gray-400 mb-1">Select Run</label>
                    <select
                        value={selectedRunId || ''}
                        onChange={(e) => setSelectedRunId(e.target.value)}
                        className="px-4 py-2 bg-gray-50 dark:bg-gray-900 border border-gray-200 dark:border-gray-700 rounded-lg text-gray-900 dark:text-white focus:border-blue-500 focus:outline-none"
                    >
                        {runs.map((run) => (
                            <option key={run.run_id} value={run.run_id}>
                                {run.run_id.slice(0, 8)}... ({run.counters.pairs_scored} pairs)
                            </option>
                        ))}
                    </select>
                </div>

                <div>
                    <label className="block text-sm text-gray-500 dark:text-gray-400 mb-1">Min Score</label>
                    <input
                        type="range"
                        value={minScore}
                        onChange={(e) => setMinScore(Number(e.target.value))}
                        min={0}
                        max={100}
                        step={5}
                        className="w-32 accent-blue-600"
                    />
                    <span className="ml-2 text-gray-500 dark:text-gray-400">{minScore}%</span>
                </div>

                <div>
                    <label className="block text-sm text-gray-500 dark:text-gray-400 mb-1">View</label>
                    <div className="flex gap-2 bg-gray-100 dark:bg-gray-900 rounded-lg p-1 border border-gray-200 dark:border-gray-700">
                        <button
                            onClick={() => setViewMode('list')}
                            className={`p-1.5 rounded ${viewMode === 'list' ? 'bg-white dark:bg-gray-700 text-blue-600 dark:text-white shadow-sm' : 'text-gray-500 dark:text-gray-400 hover:text-gray-700 dark:hover:text-gray-300'}`}
                            title="List View"
                        >
                            <List size={18} />
                        </button>
                        <button
                            onClick={() => setViewMode('graph')}
                            className={`p-1.5 rounded ${viewMode === 'graph' ? 'bg-white dark:bg-gray-700 text-blue-600 dark:text-white shadow-sm' : 'text-gray-500 dark:text-gray-400 hover:text-gray-700 dark:hover:text-gray-300'}`}
                            title="Graph View"
                        >
                            <LayoutGrid size={18} />
                        </button>
                    </div>
                </div>

                <div>
                    <label className="block text-sm text-gray-500 dark:text-gray-400 mb-1">Filter</label>
                    <div className="flex gap-2">
                        {['all', 'auto_link', 'review', 'reject', 'unique', 'entities'].map((f) => (
                            <button
                                key={f}
                                onClick={() => setFilter(f as any)}
                                className={`px-3 py-1.5 rounded-lg text-sm transition-colors ${filter === f
                                    ? 'bg-blue-600 text-white shadow-sm'
                                    : 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-200 dark:hover:bg-gray-600'
                                    }`}
                            >
                                {f === 'all' ? 'All' : f.replace('_', ' ').toUpperCase()}
                            </button>
                        ))}
                    </div>
                </div>
            </div>

            {/* Main Content */}
            {viewMode === 'graph' ? (
                <div className="relative h-[700px] bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 overflow-hidden group shadow-sm">
                    <div className={`absolute top-4 left-4 z-50 transition-opacity duration-300 ${!isTuningOpen ? 'opacity-0 group-hover:opacity-100' : 'opacity-100'}`}>
                        <TuningPanel 
                            onPreview={handlePreview} 
                            onSave={handleSaveConfig}
                            loading={previewLoading}
                            isOpen={isTuningOpen}
                            setIsOpen={setIsTuningOpen}
                        />
                    </div>
                    <div className="w-full h-full">
                        <ClusterGraph 
                            data={previewData} 
                            loading={previewLoading} 
                            loadingMessage={loadingMessage}
                        />
                    </div>
                </div>
            ) : (
                <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
                    {/* Scores List */}
                <div className="bg-white dark:bg-gray-800/50 border border-gray-200 dark:border-gray-700 rounded-xl p-4 flex flex-col h-[700px] shadow-sm">
                    <h2 className="text-lg font-semibold text-gray-900 dark:text-white mb-4 flex justify-between items-center">
                        <span>Match Pairs ({totalScores})</span>
                        <span className="text-xs text-gray-500 font-normal">Page {currentPage}</span>
                    </h2>

                    {scores.length === 0 ? (
                        <div className="text-center py-8 text-gray-400">
                            No matches found
                        </div>
                    ) : (
                        <div className="space-y-2 overflow-y-auto flex-1 pr-2">
                            {scores.map((score: any) => (
                                score._is_cluster ? (
                                    <div
                                        key={score.pair_id}
                                        onClick={() => setSelectedMatch({ record_a: score._cluster.representative_record, record_b: null, _is_cluster: true, _cluster: score._cluster })}
                                        className={`p-3 rounded-lg border cursor-pointer transition-all ${selectedMatch?._cluster?.cluster_id === score.pair_id
                                            ? 'border-blue-500 bg-blue-50 dark:bg-blue-900/20'
                                            : 'border-gray-200 dark:border-gray-700 bg-gray-50 dark:bg-gray-900/50 hover:border-gray-300 dark:hover:border-gray-600'
                                            }`}
                                    >
                                        <div className="flex items-center justify-between">
                                            <span className={`px-2 py-0.5 rounded border text-sm font-medium ${score._cluster.size > 1 ? 'bg-purple-100 dark:bg-purple-900/30 border-purple-200 dark:border-purple-700 text-purple-700 dark:text-purple-300' : 'bg-gray-100 dark:bg-gray-800 border-gray-200 dark:border-gray-600 text-gray-600 dark:text-gray-300'}`}>
                                                {score._cluster.size > 1 ? `Entity (${score._cluster.size})` : 'Singleton'}
                                            </span>
                                        </div>
                                        <div className="mt-2 text-sm text-gray-900 dark:text-white font-medium">
                                            {getDisplayName(score._cluster.representative_record)}
                                        </div>
                                        <div className="text-xs font-mono text-gray-500 truncate">
                                            ID: {formatFieldValue(score._cluster.representative_record.source_customer_id)}
                                        </div>
                                    </div>
                                ) : score._is_unique ? (
                                    <div
                                        key={score.pair_id}
                                        onClick={() => setSelectedMatch({ record_a: score._record, record_b: null, _is_unique: true })}
                                        className={`p-3 rounded-lg border cursor-pointer transition-all ${selectedMatch?.record_a?.customer_key === score.pair_id
                                            ? 'border-blue-500 bg-blue-50 dark:bg-blue-900/20'
                                            : 'border-gray-200 dark:border-gray-700 bg-gray-50 dark:bg-gray-900/50 hover:border-gray-300 dark:hover:border-gray-600'
                                            }`}
                                    >
                                        <div className="flex items-center justify-between">
                                            <span className="px-2 py-0.5 rounded border text-sm font-medium bg-gray-100 dark:bg-gray-800 border-gray-200 dark:border-gray-600 text-gray-600 dark:text-gray-300">
                                                Singleton
                                            </span>
                                        </div>
                                        <div className="mt-2 text-sm text-gray-900 dark:text-white font-medium">
                                            {getDisplayName(score._record)}
                                        </div>
                                        <div className="text-xs font-mono text-gray-500 truncate">
                                            ID: {formatFieldValue(score._record.source_customer_id)}
                                        </div>
                                    </div>
                                ) : (
                                    <div
                                        key={score.pair_id}
                                        onClick={() => fetchMatchDetails(score.pair_id)}
                                        className={`p-3 rounded-lg border cursor-pointer transition-all ${selectedMatch?.pair_id === score.pair_id
                                            ? 'border-blue-500 bg-blue-50 dark:bg-blue-900/20'
                                            : 'border-gray-200 dark:border-gray-700 bg-gray-50 dark:bg-gray-900/50 hover:border-gray-300 dark:hover:border-gray-600'
                                            }`}
                                    >
                                        <div className="flex items-center justify-between">
                                            <div className="flex items-center gap-2">
                                                <span className={`px-2 py-0.5 rounded border text-sm font-medium ${getScoreBg(score.score)} ${getScoreColor(score.score)}`}>
                                                    {(score.score * 100).toFixed(0)}%
                                                </span>
                                            </div>
                                            {score.signals_hit.length > 0 && (
                                                <span className="text-xs text-gray-500">
                                                    {score.signals_hit.length} signals
                                                </span>
                                            )}
                                        </div>
                                        <div className="mt-2 text-xs font-mono text-gray-500">
                                            {score.a_key.slice(0, 8)}... ↔ {score.b_key.slice(0, 8)}...
                                        </div>
                                        {score.hard_conflicts.length > 0 && (
                                            <div className="mt-1 text-xs text-red-500 dark:text-red-400">
                                                ⚠️ {score.hard_conflicts.length} conflict(s)
                                            </div>
                                        )}
                                    </div>
                                )
                            ))}
                        </div>
                    )}

                    {/* Pagination Controls */}
                    <div className="mt-4 pt-4 border-t border-gray-200 dark:border-gray-700 flex items-center justify-between">
                        <button
                            onClick={() => setCurrentPage(prev => Math.max(1, prev - 1))}
                            disabled={currentPage === 1}
                            className="px-3 py-1.5 text-sm rounded bg-gray-100 dark:bg-gray-800 border border-gray-200 dark:border-gray-600 text-gray-700 dark:text-gray-300 disabled:opacity-50 hover:bg-gray-200 dark:hover:bg-gray-700 transition-colors"
                        >
                            Previous
                        </button>
                        <span className="text-xs text-gray-500">
                            {((currentPage - 1) * pageSize) + 1} - {Math.min(currentPage * pageSize, totalScores)} of {totalScores}
                        </span>
                        <button
                            onClick={() => setCurrentPage(prev => (prev * pageSize < totalScores) ? prev + 1 : prev)}
                            disabled={currentPage * pageSize >= totalScores}
                            className="px-3 py-1.5 text-sm rounded bg-gray-100 dark:bg-gray-800 border border-gray-200 dark:border-gray-600 text-gray-700 dark:text-gray-300 disabled:opacity-50 hover:bg-gray-200 dark:hover:bg-gray-700 transition-colors"
                        >
                            Next
                        </button>
                    </div>
                </div>

                {/* Right Column: Match Details */}
                <div className="lg:col-span-2 space-y-6">
                    {!selectedMatch ? (
                        <div className="bg-white dark:bg-gray-800/50 border border-gray-200 dark:border-gray-700 rounded-xl p-12 text-center text-gray-500 dark:text-gray-400 shadow-sm">
                            Select a record to view details
                        </div>
                    ) : isLoadingDetails ? (
                        <div className="bg-white dark:bg-gray-800/50 border border-gray-200 dark:border-gray-700 rounded-xl p-12 text-center text-gray-500 dark:text-gray-400 shadow-sm">
                            Loading details...
                        </div>
                    ) : selectedMatch._is_unique ? (
                        <div className="space-y-6">
                            <div className="bg-white dark:bg-gray-800/50 border border-gray-200 dark:border-gray-700 rounded-xl p-6 shadow-sm">
                                <h2 className="text-xl font-semibold text-gray-900 dark:text-white mb-4">Singleton Entity</h2>
                                <p className="text-gray-500 dark:text-gray-400 mb-6">This record has no high-confidence matches in the current run.</p>
                                <div className="max-w-xl">
                                    <RecordCard title="Single Record" data={selectedMatch.record_a} color="blue" />
                                </div>
                            </div>
                        </div>
                    ) : selectedMatch._is_cluster ? (
                        <div className="space-y-6">
                            <div className="bg-white dark:bg-gray-800/50 border border-gray-200 dark:border-gray-700 rounded-xl p-6 shadow-sm">
                                <h2 className="text-xl font-semibold text-gray-900 dark:text-white mb-2">Resolved Entity</h2>
                                <div className="flex gap-2 mb-6">
                                    <span className="px-2 py-1 bg-purple-100 dark:bg-purple-900/40 border border-purple-200 dark:border-purple-700 text-purple-700 dark:text-purple-300 rounded text-xs">
                                        Cluster ID: {selectedMatch._cluster.cluster_id.slice(0, 8)}...
                                    </span>
                                    <span className="px-2 py-1 bg-gray-100 dark:bg-gray-800 border border-gray-200 dark:border-gray-700 text-gray-700 dark:text-gray-300 rounded text-xs">
                                        Size: {selectedMatch._cluster.size}
                                    </span>
                                </div>

                                <div className="mb-8">
                                    <h3 className="text-sm font-medium text-gray-500 dark:text-gray-400 mb-3 uppercase tracking-wider">Representative / Golden Record</h3>
                                    <div className="max-w-xl">
                                        <RecordCard title="Representative Record" data={selectedMatch.record_a} color="blue" />
                                    </div>
                                </div>

                                {selectedMatch._cluster.members && selectedMatch._cluster.members.length > 0 && (
                                    <div>
                                        <h3 className="text-sm font-medium text-gray-500 dark:text-gray-400 mb-3 uppercase tracking-wider">
                                            Member Records ({selectedMatch._cluster.members.length})
                                        </h3>
                                        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                                            {selectedMatch._cluster.members.map((member: any, idx: number) => (
                                                <div key={idx} className="scale-90 origin-top-left opacity-90 hover:scale-100 hover:opacity-100 transition-all">
                                                    <RecordCard
                                                        title={`Member ${idx + 1}`}
                                                        data={member}
                                                        color="purple"
                                                    />
                                                </div>
                                            ))}
                                        </div>
                                    </div>
                                )}
                            </div>
                        </div>
                    ) : (
                        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                            <RecordCard title="Record A" data={selectedMatch.record_a} color="blue" />
                            <RecordCard title="Record B" data={selectedMatch.record_b} color="purple" />
                        </div>
                    )}
                </div>
            </div>
            )}

            {/* Evidence Detail (Hide for unique/cluster) */}
            {selectedMatch && !selectedMatch._is_unique && !selectedMatch._is_cluster && (
                <div className="bg-white dark:bg-gray-800/50 border border-gray-200 dark:border-gray-700 rounded-xl p-6 shadow-sm">
                    <div className="flex items-center justify-between mb-4">
                        <h2 className="text-xl font-semibold text-gray-900 dark:text-white">Match Evidence</h2>
                        <button
                            onClick={handleAskReferee}
                            disabled={isExplaining}
                            className="flex items-center gap-2 px-3 py-1.5 bg-indigo-600 hover:bg-indigo-500 disabled:opacity-50 disabled:cursor-not-allowed text-white text-sm font-medium rounded-lg transition-colors shadow-lg shadow-indigo-900/20"
                        >
                            {isExplaining ? (
                                <>
                                    <div className="w-4 h-4 border-2 border-white/30 border-t-white rounded-full animate-spin" />
                                    Analyzing...
                                </>
                            ) : (
                                <>
                                    ✨ Ask Referee
                                </>
                            )}
                        </button>
                    </div>

                    {explanation && (
                        <div className="mb-6 animate-in fade-in slide-in-from-top-4 duration-500">
                            <div className={`p-4 rounded-xl border ${explanation.judgement === 'MATCH' ? 'bg-green-50 dark:bg-green-900/20 border-green-200 dark:border-green-800' :
                                explanation.judgement === 'NO_MATCH' ? 'bg-red-50 dark:bg-red-900/20 border-red-200 dark:border-red-800' :
                                    'bg-yellow-50 dark:bg-yellow-900/20 border-yellow-200 dark:border-yellow-800'
                                }`}>
                                <div className="flex items-center gap-3 mb-2">
                                    <span className={`px-2 py-0.5 rounded text-xs font-bold uppercase tracking-wider ${explanation.judgement === 'MATCH' ? 'bg-green-100 dark:bg-green-900/40 text-green-700 dark:text-green-300 border border-green-200 dark:border-green-700' :
                                        explanation.judgement === 'NO_MATCH' ? 'bg-red-100 dark:bg-red-900/40 text-red-700 dark:text-red-300 border border-red-200 dark:border-red-700' :
                                            'bg-yellow-100 dark:bg-yellow-900/40 text-yellow-700 dark:text-yellow-300 border border-yellow-200 dark:border-yellow-700'
                                        }`}>
                                        {explanation.judgement.replace('_', ' ')}
                                    </span>
                                    <span className="text-xs text-gray-500 font-mono">
                                        Model: {explanation.meta?.model}
                                    </span>
                                </div>
                                <p className="text-gray-700 dark:text-gray-300 text-sm leading-relaxed">
                                    {explanation.explanation}
                                </p>
                            </div>
                        </div>
                    )}

                    <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                        {/* Score Gauge */}
                        <div className="flex items-center justify-center">
                            <div className={`w-32 h-32 rounded-full border-8 flex items-center justify-center ${getScoreBg(selectedMatch.score)}`}>
                                <div className="text-center">
                                    <span className={`text-3xl font-bold ${getScoreColor(selectedMatch.score)}`}>
                                        {(selectedMatch.score * 100).toFixed(0)}%
                                    </span>
                                    <p className="text-sm text-gray-500 dark:text-gray-400">Score</p>
                                </div>
                            </div>
                        </div>

                        {/* Signals & Conflicts */}
                        <div className="space-y-4">
                            <div>
                                <h3 className="text-sm font-medium text-gray-500 dark:text-gray-400 mb-2">Decision</h3>
                                <span className={`px-3 py-1.5 rounded-lg text-sm ${selectedMatch.decision === 'AUTO_LINK' ? 'bg-emerald-100 dark:bg-emerald-900/50 text-emerald-700 dark:text-emerald-400' :
                                    selectedMatch.decision === 'REVIEW' ? 'bg-yellow-100 dark:bg-yellow-900/50 text-yellow-700 dark:text-yellow-400' :
                                        selectedMatch.decision === 'REJECT' ? 'bg-red-100 dark:bg-red-900/50 text-red-700 dark:text-red-400' :
                                            'bg-gray-100 dark:bg-gray-700 text-gray-500 dark:text-gray-400'
                                    }`}>
                                    {selectedMatch.decision || 'Pending'}
                                </span>
                            </div>

                            <div>
                                <h3 className="text-sm font-medium text-gray-500 dark:text-gray-400 mb-2">Signals Hit</h3>
                                <div className="flex flex-wrap gap-2">
                                    {selectedMatch.signals_hit?.map((signal: string) => (
                                        <span key={signal} className="px-2 py-1 bg-purple-100 dark:bg-purple-900/30 border border-purple-200 dark:border-purple-700 text-purple-700 dark:text-purple-400 rounded text-xs">
                                            {signal}
                                        </span>
                                    ))}
                                    {(!selectedMatch.signals_hit || selectedMatch.signals_hit.length === 0) && (
                                        <span className="text-gray-500 text-sm">No signals</span>
                                    )}
                                </div>
                            </div>

                            {selectedMatch.hard_conflicts?.length > 0 && (
                                <div>
                                    <h3 className="text-sm font-medium text-red-500 dark:text-red-400 mb-2">⚠️ Hard Conflicts</h3>
                                    <div className="flex flex-wrap gap-2">
                                        {selectedMatch.hard_conflicts.map((conflict: string) => (
                                            <span key={conflict} className="px-2 py-1 bg-red-100 dark:bg-red-900/30 border border-red-200 dark:border-red-700 text-red-700 dark:text-red-400 rounded text-xs">
                                                {conflict}
                                            </span>
                                        ))}
                                    </div>
                                </div>
                            )}

                            {/* Related Matches (Transitivity) */}
                            {selectedMatch && !selectedMatch._is_unique && !selectedMatch._is_cluster && (
                                <div className="pt-4 border-t border-gray-200 dark:border-gray-700">
                                    <h3 className="text-sm font-medium text-gray-500 dark:text-gray-400 mb-2">🔗 Network Context</h3>
                                    {getRelatedMatches(selectedMatch.pair_id, selectedMatch.record_a, selectedMatch.record_b).length > 0 ? (
                                        <div className="space-y-2">
                                            {getRelatedMatches(selectedMatch.pair_id, selectedMatch.record_a, selectedMatch.record_b).slice(0, 3).map((rel: any) => (
                                                <div key={rel.pair_id}
                                                    className="p-2 bg-gray-50 dark:bg-gray-900/40 rounded border border-gray-200 dark:border-gray-700 text-xs flex justify-between items-center cursor-pointer hover:border-gray-300 dark:hover:border-gray-500 transition-colors"
                                                    onClick={() => fetchMatchDetails(rel.pair_id)}
                                                >
                                                    <span className="text-gray-600 dark:text-gray-300 font-mono">{rel.pair_id.slice(0, 8)}...</span>
                                                    <span className={`font-bold ${getScoreColor(rel.score)}`}>{(rel.score * 100).toFixed(0)}%</span>
                                                </div>
                                            ))}
                                            <p className="text-xs text-gray-500 mt-1">
                                                + {getRelatedMatches(selectedMatch.pair_id, selectedMatch.record_a, selectedMatch.record_b).length - 3 > 0 ?
                                                    `${getRelatedMatches(selectedMatch.pair_id, selectedMatch.record_a, selectedMatch.record_b).length - 3} more links` :
                                                    'Transitive links found'}
                                            </p>
                                        </div>
                                    ) : (
                                        <p className="text-xs text-gray-500">No direct transitive matches found in this view.</p>
                                    )}
                                </div>
                            )}
                        </div>
                    </div>

                    {/* Evidence Table */}
                    {selectedMatch.evidence && selectedMatch.evidence.length > 0 && (
                        <div className="mt-6 pt-6 border-t border-gray-200 dark:border-gray-700">
                            <h3 className="text-lg font-medium text-gray-900 dark:text-white mb-3">Field Comparison</h3>
                            <div className="overflow-x-auto">
                                <table className="w-full">
                                    <thead>
                                        <tr className="text-left text-gray-500 dark:text-gray-400 text-sm border-b border-gray-200 dark:border-gray-700">
                                            <th className="pb-2 pl-4">Field</th>
                                            <th className="pb-2">Value A</th>
                                            <th className="pb-2">Value B</th>
                                            <th className="pb-2">Similarity</th>
                                            <th className="pb-2">Type</th>
                                        </tr>
                                    </thead>
                                    <tbody className="text-sm">
                                        {selectedMatch.evidence.map((ev: any, idx: number) => (
                                            <tr key={idx} className="border-b border-gray-100 dark:border-gray-800 hover:bg-gray-50 dark:hover:bg-gray-800/30 transition-colors">
                                                <td className="py-3 pl-4 font-medium text-gray-900 dark:text-white capitalize">{ev.field.replace('_norm', '').replace('_', ' ')}</td>
                                                <td className="py-3 text-blue-600 dark:text-blue-300 font-mono text-xs">
                                                    {formatFieldValue(ev.value_a)}
                                                </td>
                                                <td className="py-3 text-purple-600 dark:text-purple-300 font-mono text-xs">
                                                    {formatFieldValue(ev.value_b)}
                                                </td>
                                                <td className="py-3">
                                                    <div className="flex items-center gap-2">
                                                        <div className="w-16 h-1.5 bg-gray-200 dark:bg-gray-700 rounded-full overflow-hidden">
                                                            <div
                                                                className={`h-full rounded-full ${ev.similarity >= 0.99 ? 'bg-emerald-500' :
                                                                    ev.similarity >= 0.8 ? 'bg-emerald-400' :
                                                                        ev.similarity >= 0.6 ? 'bg-yellow-400' : 'bg-red-400'
                                                                    }`}
                                                                style={{ width: `${(ev.similarity || 0) * 100}%` }}
                                                            />
                                                        </div>
                                                        <span className={`text-xs font-medium ${ev.similarity >= 0.8 ? 'text-emerald-600 dark:text-emerald-400' :
                                                            ev.similarity >= 0.5 ? 'text-yellow-600 dark:text-yellow-400' : 'text-red-600 dark:text-red-400'
                                                            }`}>
                                                            {((ev.similarity || 0) * 100).toFixed(0)}%
                                                        </span>
                                                    </div>
                                                </td>
                                                <td className="py-3">
                                                    <span className={`inline-flex items-center px-2 py-1 rounded text-xs border ${ev.comparison_type === 'exact_match' ? 'bg-emerald-100 dark:bg-emerald-900/30 border-emerald-200 dark:border-emerald-800 text-emerald-700 dark:text-emerald-400' :
                                                        ev.comparison_type === 'fuzzy_match' ? 'bg-blue-100 dark:bg-blue-900/30 border-blue-200 dark:border-blue-800 text-blue-700 dark:text-blue-400' :
                                                            ev.comparison_type === 'low_similarity' ? 'bg-yellow-100 dark:bg-yellow-900/30 border-yellow-200 dark:border-yellow-800 text-yellow-700 dark:text-yellow-400' :
                                                                ev.comparison_type === 'mismatch' ? 'bg-red-100 dark:bg-red-900/30 border-red-200 dark:border-red-800 text-red-700 dark:text-red-400' :
                                                                    'bg-gray-100 dark:bg-gray-800 border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-400'
                                                        }`}>
                                                        {ev.comparison_type === 'exact_match' && <CheckCircle size={12} className="mr-1" />}
                                                        {ev.comparison_type === 'fuzzy_match' && <Search size={12} className="mr-1" />}
                                                        {ev.comparison_type === 'mismatch' && <XCircle size={12} className="mr-1" />}
                                                        {ev.comparison_type?.replace('_', ' ') || '—'}
                                                    </span>
                                                </td>
                                            </tr>
                                        ))}
                                    </tbody>
                                </table>
                            </div>
                        </div>
                    )}

                    {/* Related Matches (Cluster Context) */}
                    <div className="mt-8 pt-6 border-t border-gray-200 dark:border-gray-700">
                        <h3 className="text-lg font-medium text-gray-900 dark:text-white mb-3">Related Matches (Cluster Context)</h3>

                        {getRelatedMatches(selectedMatch.pair_id, selectedMatch.record_a, selectedMatch.record_b).length === 0 ? (
                            <div className="text-gray-500 dark:text-gray-400 text-sm italic">No other related records found in this run.</div>
                        ) : (
                            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
                                {getRelatedMatches(selectedMatch.pair_id, selectedMatch.record_a, selectedMatch.record_b).map(related => (
                                    <div
                                        key={related.pair_id}
                                        onClick={() => fetchMatchDetails(related.pair_id)}
                                        className="p-3 bg-gray-50 dark:bg-gray-900 border border-gray-200 dark:border-gray-700 rounded-lg cursor-pointer hover:border-gray-300 dark:hover:border-gray-500 transition-colors"
                                    >
                                        <div className="flex justify-between items-center mb-2">
                                            <span className={`px-2 py-0.5 rounded text-xs font-medium ${getScoreBg(related.score)} ${getScoreColor(related.score)}`}>
                                                {(related.score * 100).toFixed(0)}%
                                            </span>
                                            <span className="text-xs text-blue-600 dark:text-blue-400 hover:underline">
                                                View Pair
                                            </span>
                                        </div>
                                        <div className="text-xs font-mono text-gray-500 dark:text-gray-400 truncate mb-1" title={related.a_key}>
                                            A: {related.a_key.slice(0, 15)}...
                                        </div>
                                        <div className="text-xs font-mono text-gray-500 dark:text-gray-400 truncate" title={related.b_key}>
                                            B: {related.b_key.slice(0, 15)}...
                                        </div>
                                    </div>
                                ))}
                            </div>
                        )}
                    </div>
                </div>
            )}
        </div >
    );
}
