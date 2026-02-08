'use client';

import { useState, useEffect } from 'react';
import { api } from '@/lib/api';
import { ClusterGraph } from '@/components/explorer/ClusterGraph';
import { TuningPanel } from '@/components/explorer/TuningPanel';
import { RefreshCw, Sliders, Play } from 'lucide-react';

export default function GraphPage() {
    const [graphData, setGraphData] = useState<any>(null);
    const [isLoading, setIsLoading] = useState(true);
    const [runs, setRuns] = useState<any[]>([]);
    const [selectedRunId, setSelectedRunId] = useState<string | null>(null);
    const [showTuning, setShowTuning] = useState(false);

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
            const data = await (api as any).getGraphData(runId, 2000);
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
            const data = await (api as any).previewClustering(selectedRunId, config);
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
        <div className="flex flex-col h-[calc(100vh-6rem)] bg-gray-900 text-white">
            {/* Header */}
            <div className="flex items-center justify-between px-6 py-4 border-b border-gray-800 bg-gray-900/50 backdrop-blur">
                <div>
                    <h1 className="text-xl font-bold bg-gradient-to-r from-blue-400 to-purple-400 bg-clip-text text-transparent">
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
                            className="bg-gray-800 border border-gray-700 text-sm rounded-lg px-3 py-1.5 focus:outline-none focus:ring-2 focus:ring-blue-500"
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
                            <div className="px-3 py-1 rounded bg-purple-900/30 border border-purple-800">
                                <span className="text-xs text-purple-400 block">Clusters</span>
                                <span className="text-lg font-bold">{graphData.stats.total_clusters}</span>
                            </div>
                            <div className="px-3 py-1 rounded bg-blue-900/30 border border-blue-800">
                                <span className="text-xs text-blue-400 block">Identities</span>
                                <span className="text-lg font-bold">{graphData.stats.total_members}</span>
                            </div>
                        </div>
                    )}

                    <button 
                        onClick={() => setShowTuning(!showTuning)}
                        className={`p-2 rounded-lg border transition-colors ${showTuning ? 'bg-blue-600 border-blue-500 text-white' : 'bg-gray-800 border-gray-700 text-gray-400 hover:text-white'}`}
                        title="Toggle Tuning Panel"
                    >
                        <Sliders size={18} />
                    </button>
                    
                    <button 
                        onClick={() => selectedRunId && fetchGraph(selectedRunId)}
                        className="p-2 rounded-lg bg-gray-800 border border-gray-700 text-gray-400 hover:text-white hover:bg-gray-700 transition-colors"
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
                    />
                </div>

                {/* Tuning Panel (Overlay or Sidebar) */}
                {showTuning && (
                    <div className="w-80 border-l border-gray-800 bg-gray-900/95 backdrop-blur absolute right-0 top-0 bottom-0 z-10 shadow-2xl overflow-y-auto transition-transform">
                        <div className="p-4">
                            <h2 className="text-sm font-semibold text-gray-400 uppercase tracking-wider mb-4 flex items-center gap-2">
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
