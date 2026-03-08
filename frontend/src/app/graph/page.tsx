'use client';

import { useState, useEffect } from 'react';
import { api } from '@/lib/api';
import { ClusterGraph } from '@/components/explorer/ClusterGraph';
import { TuningPanel } from '@/components/explorer/TuningPanel';
import { RefreshCw, Sliders, Play, Type } from 'lucide-react';

export default function GraphPage() {
    const [graphData, setGraphData] = useState<any>(null);
    const [isLoading, setIsLoading] = useState(true);
    const [runs, setRuns] = useState<any[]>([]);
    const [selectedRunId, setSelectedRunId] = useState<string | null>(null);
    const [showTuning, setShowTuning] = useState(false);
    const [showLabels, setShowLabels] = useState(true);

    // Initial Fetch
    useEffect(() => {
        fetchRuns();
    }, []);

    // Fetch Graph when Run changes
    useEffect(() => {
        if (selectedRunId) {
            fetchGraph(selectedRunId);
        }
    }, [selectedRunId]);

    const fetchRuns = async () => {
        try {
            const data = await api.listRuns(1, 10);
            setRuns(data.runs);
            if (data.runs.length > 0) {
                // Select most recent run
                setSelectedRunId(data.runs[0].run_id);
            } else {
                // No runs, fetch default graph (might return empty or all records)
                fetchGraph();
            }
        } catch (err) {
            console.error('Failed to fetch runs:', err);
            // Fallback
            fetchGraph();
        }
    };

    const fetchGraph = async (runId?: string) => {
        setIsLoading(true);
        try {
            // Fetch graph data with singletons included
            const data = await api.getGraphData(runId, 2000);
            setGraphData(data);
        } catch (err) {
            console.error('Failed to fetch graph data:', err);
        } finally {
            setIsLoading(false);
        }
    };

    const handlePreview = async (config: any) => {
        setIsLoading(true);
        try {
            const data = await api.previewClustering(selectedRunId ?? undefined, config);
            setGraphData(data);
        } catch (err) {
            console.error('Preview failed:', err);
        } finally {
            setIsLoading(false);
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

    return (
        <div className="flex flex-col h-[calc(100vh-6rem)] bg-gray-50 dark:bg-gray-900 text-gray-900 dark:text-white transition-colors duration-300">
            {/* Header */}
            <div className="flex items-center justify-between px-6 py-4 border-b border-gray-200 dark:border-gray-800 bg-white/50 dark:bg-gray-900/50 backdrop-blur">
                <div>
                    <h1 className="text-xl font-bold bg-gradient-to-r from-blue-600 to-purple-600 dark:from-blue-400 dark:to-purple-400 bg-clip-text text-transparent">
                        Identity Graph 360
                    </h1>
                    <p className="text-xs text-gray-500 mt-1">Interactive Customer Single View</p>
                </div>

                <div className="flex items-center gap-4">
                    {/* Run Selector */}
                    {runs.length > 0 && (
                        <select
                            value={selectedRunId || ''}
                            onChange={(e) => setSelectedRunId(e.target.value)}
                            className="bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 text-gray-900 dark:text-white text-sm rounded-lg px-3 py-1.5 focus:outline-none focus:ring-2 focus:ring-blue-500"
                        >
                            {runs.map(r => (
                                <option key={r.run_id} value={r.run_id}>
                                    Run: {r.run_id.substring(0, 8)}... ({r.status})
                                </option>
                            ))}
                        </select>
                    )}

                    {/* Stats Badges */}
                    {graphData?.stats && (
                        <div className="flex gap-2">
                            <div className="px-3 py-1 rounded bg-purple-50 dark:bg-purple-900/30 border border-purple-200 dark:border-purple-800">
                                <span className="text-xs text-purple-600 dark:text-purple-400 block">Clusters</span>
                                <span className="text-lg font-bold text-gray-900 dark:text-white">{graphData.stats.total_clusters}</span>
                            </div>
                            <div className="px-3 py-1 rounded bg-blue-50 dark:bg-blue-900/30 border border-blue-200 dark:border-blue-800">
                                <span className="text-xs text-blue-600 dark:text-blue-400 block">Identities</span>
                                <span className="text-lg font-bold text-gray-900 dark:text-white">{graphData.stats.total_members}</span>
                            </div>
                        </div>
                    )}

                    <button
                        onClick={() => setShowTuning(!showTuning)}
                        className={`p-2 rounded-lg border transition-colors ${showTuning ? 'bg-blue-600 border-blue-500 text-white' : 'bg-white dark:bg-gray-800 border-gray-200 dark:border-gray-700 text-gray-500 dark:text-gray-400 hover:text-gray-900 dark:hover:text-white'}`}
                        title="Toggle Tuning Panel"
                    >
                        <Sliders size={18} />
                    </button>

                    <button
                        onClick={() => setShowLabels(!showLabels)}
                        className={`p-2 rounded-lg border transition-colors ${showLabels ? 'bg-blue-100 dark:bg-blue-900/50 border-blue-200 dark:border-blue-800 text-blue-600 dark:text-blue-400' : 'bg-white dark:bg-gray-800 border-gray-200 dark:border-gray-700 text-gray-500 dark:text-gray-400 hover:text-gray-900 dark:hover:text-white'}`}
                        title={showLabels ? "Hide Labels" : "Show Labels"}
                    >
                        <Type size={18} />
                    </button>

                    <button
                        onClick={() => selectedRunId && fetchGraph(selectedRunId)}
                        className="p-2 rounded-lg bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 text-gray-500 dark:text-gray-400 hover:text-gray-900 dark:hover:text-white hover:bg-gray-50 dark:hover:bg-gray-700 transition-colors"
                        title="Refresh Data"
                    >
                        <RefreshCw size={18} />
                    </button>
                </div>
            </div>

            {/* Main Content */}
            <div className="flex-1 flex overflow-hidden relative">
                {/* Graph Canvas */}
                <div className="flex-1 h-full relative">
                    <ClusterGraph
                        data={graphData}
                        loading={isLoading}
                        showLabels={showLabels}
                    />
                </div>

                {/* Tuning Panel (Overlay or Sidebar) */}
                {showTuning && (
                    <div className="w-80 border-l border-gray-200 dark:border-gray-800 bg-white/95 dark:bg-gray-900/95 backdrop-blur absolute right-0 top-0 bottom-0 z-10 shadow-2xl overflow-y-auto transition-transform">
                        <div className="p-4">
                            <h2 className="text-sm font-semibold text-gray-500 dark:text-gray-400 uppercase tracking-wider mb-4 flex items-center gap-2">
                                <Sliders size={14} />
                                Clustering Logic
                            </h2>
                            <TuningPanel
                                onPreview={handlePreview}
                                onSave={handleSaveConfig}
                                loading={isLoading}
                                isOpen={showTuning}
                                setIsOpen={setShowTuning}
                            />
                        </div>
                    </div>
                )}
            </div>
        </div>
    );
}
