'use client';

import { useState, useEffect } from 'react';
import { api } from '@/lib/api';
import { useWebSocket } from '@/lib/ws';

interface DashboardMetrics {
    total_records: number;
    total_clusters: number;
    duplicates_detected: number;
    duplicate_rate_pct: number;
    review_backlog: number;
    auto_link_rate_pct: number;
    avg_run_duration_seconds: number;
    last_run_at: string | null;
}

interface RecentRun {
    run_id: string;
    mode: string;
    status: string;
    counters: {
        records_in: number;
        auto_links: number;
        review_items: number;
    };
    started_at: string;
    duration_seconds: number | null;
}

export default function DashboardPage() {
    const [metrics, setMetrics] = useState<DashboardMetrics | null>(null);
    const [recentRuns, setRecentRuns] = useState<RecentRun[]>([]);
    const [isLoading, setIsLoading] = useState(true);
    const { isConnected } = useWebSocket();

    useEffect(() => {
        fetchDashboardData();
        const interval = setInterval(fetchDashboardData, 5000);
        return () => clearInterval(interval);
    }, []);

    const fetchDashboardData = async () => {
        try {
            const [metricsData, runsData] = await Promise.all([
                api.getDashboardMetrics(),
                api.listRuns(1, 5),
            ]);
            setMetrics(metricsData);
            setRecentRuns(runsData.runs);
        } catch (err) {
            console.error('Failed to fetch dashboard data:', err);
        } finally {
            setIsLoading(false);
        }
    };

    const startQuickRun = async () => {
        try {
            await api.startRun('FULL', 'Quick run from dashboard');
            fetchDashboardData();
        } catch (err) {
            console.error('Failed to start run:', err);
        }
    };

    const formatDuration = (seconds: number | null) => {
        if (!seconds) return '—';
        if (seconds < 1) return `${(seconds * 1000).toFixed(0)}ms`;
        return `${seconds.toFixed(1)}s`;
    };

    const formatTimeAgo = (isoString: string | null) => {
        if (!isoString) return 'Never';
        const date = new Date(isoString);
        const now = new Date();
        const diffMs = now.getTime() - date.getTime();
        const diffMins = Math.floor(diffMs / 60000);
        if (diffMins < 1) return 'Just now';
        if (diffMins < 60) return `${diffMins}m ago`;
        return `${Math.floor(diffMins / 60)}h ago`;
    };

    if (isLoading) {
        return (
            <div className="p-8 flex items-center justify-center min-h-screen">
                <div className="text-gray-500 dark:text-gray-400">Loading dashboard...</div>
            </div>
        );
    }

    return (
        <div className="p-8 space-y-8">
            {/* Header */}
            <div className="flex items-center justify-between">
                <div>
                    <h1 className="text-3xl font-bold text-gray-900 dark:text-white">Dashboard</h1>
                    <p className="text-gray-500 dark:text-gray-400 mt-1">
                        CUIN v2 Identity Intelligence Platform
                    </p>
                </div>
                <div className="flex items-center gap-4">
                    <div className={`flex items-center gap-2 px-3 py-1.5 rounded-full ${isConnected
                        ? 'bg-emerald-100 dark:bg-emerald-900/30 border border-emerald-200 dark:border-emerald-700'
                        : 'bg-red-100 dark:bg-red-900/30 border border-red-200 dark:border-red-700'
                        }`}>
                        <div className={`w-2 h-2 rounded-full ${isConnected ? 'bg-emerald-500 dark:bg-emerald-400 animate-pulse' : 'bg-red-500 dark:bg-red-400'}`} />
                        <span className={`text-sm ${isConnected ? 'text-emerald-700 dark:text-emerald-400' : 'text-red-700 dark:text-red-400'}`}>
                            {isConnected ? 'Connected' : 'Disconnected'}
                        </span>
                    </div>
                    <button
                        onClick={startQuickRun}
                        className="bg-gradient-to-r from-blue-600 to-indigo-600 hover:from-blue-500 hover:to-indigo-500 text-white px-4 py-2 rounded-lg font-semibold transition-all duration-200 shadow-sm"
                    >
                        ▶️ Start Run
                    </button>
                </div>
            </div>

            {/* KPI Cards */}
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
                {/* Total Records */}
                <div className="bg-white dark:bg-gray-800/50 border border-gray-200 dark:border-gray-700 rounded-xl p-6 hover:border-gray-300 dark:hover:border-gray-600 transition-colors shadow-sm dark:shadow-none">
                    <div className="flex items-center justify-between">
                        <div>
                            <p className="text-gray-500 dark:text-gray-400 text-sm">Total Records</p>
                            <p className="text-3xl font-bold text-gray-900 dark:text-white mt-2">
                                {metrics?.total_records.toLocaleString() || '0'}
                            </p>
                        </div>
                        <div className="text-4xl opacity-50 grayscale dark:grayscale-0">📊</div>
                    </div>
                </div>

                {/* Total Clusters */}
                <div className="bg-white dark:bg-gray-800/50 border border-gray-200 dark:border-gray-700 rounded-xl p-6 hover:border-gray-300 dark:hover:border-gray-600 transition-colors shadow-sm dark:shadow-none">
                    <div className="flex items-center justify-between">
                        <div>
                            <p className="text-gray-500 dark:text-gray-400 text-sm">Identity Clusters</p>
                            <p className="text-3xl font-bold text-cyan-600 dark:text-cyan-400 mt-2">
                                {metrics?.total_clusters.toLocaleString() || '0'}
                            </p>
                        </div>
                        <div className="text-4xl opacity-50 grayscale dark:grayscale-0">🔗</div>
                    </div>
                </div>

                {/* Duplicates Found */}
                <div className="bg-white dark:bg-gray-800/50 border border-gray-200 dark:border-gray-700 rounded-xl p-6 hover:border-gray-300 dark:hover:border-gray-600 transition-colors shadow-sm dark:shadow-none">
                    <div className="flex items-center justify-between">
                        <div>
                            <p className="text-gray-500 dark:text-gray-400 text-sm">Duplicates Found</p>
                            <p className="text-3xl font-bold text-purple-600 dark:text-purple-400 mt-2">
                                {metrics?.duplicates_detected.toLocaleString() || '0'}
                            </p>
                            <p className="text-sm text-gray-500 mt-1">
                                {(metrics?.duplicate_rate_pct || 0).toFixed(1)}% rate
                            </p>
                        </div>
                        <div className="text-4xl opacity-50 grayscale dark:grayscale-0">👥</div>
                    </div>
                </div>

                {/* Review Backlog */}
                <div className={`border rounded-xl p-6 transition-colors shadow-sm dark:shadow-none ${(metrics?.review_backlog || 0) > 0
                    ? 'bg-yellow-50 dark:bg-yellow-900/20 border-yellow-200 dark:border-yellow-700 hover:border-yellow-300 dark:hover:border-yellow-600'
                    : 'bg-white dark:bg-gray-800/50 border-gray-200 dark:border-gray-700 hover:border-gray-300 dark:hover:border-gray-600'
                    }`}>
                    <div className="flex items-center justify-between">
                        <div>
                            <p className="text-gray-500 dark:text-gray-400 text-sm">Review Backlog</p>
                            <p className={`text-3xl font-bold mt-2 ${(metrics?.review_backlog || 0) > 0 ? 'text-yellow-600 dark:text-yellow-400' : 'text-gray-900 dark:text-white'
                                }`}>
                                {metrics?.review_backlog.toLocaleString() || '0'}
                            </p>
                            <p className="text-sm text-gray-500 mt-1">
                                needs review
                            </p>
                        </div>
                        <div className="text-4xl opacity-50 grayscale dark:grayscale-0">📝</div>
                    </div>
                </div>
            </div>

            {/* Quick Stats */}
            <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
                <div className="bg-white dark:bg-gray-800/50 border border-gray-200 dark:border-gray-700 rounded-xl p-6 shadow-sm dark:shadow-none">
                    <h3 className="text-lg font-semibold text-gray-900 dark:text-white mb-4">Automation Rate</h3>
                    <div className="relative h-32 flex items-center justify-center">
                        <div className="text-center">
                            <p className="text-4xl font-bold text-emerald-600 dark:text-emerald-400">
                                {(metrics?.auto_link_rate_pct || 0).toFixed(0)}%
                            </p>
                            <p className="text-gray-500 dark:text-gray-400 text-sm mt-1">auto-linked</p>
                        </div>
                    </div>
                    <div className="h-2 bg-gray-100 dark:bg-gray-700 rounded-full overflow-hidden mt-4">
                        <div
                            className="h-full bg-gradient-to-r from-emerald-500 to-cyan-500 rounded-full transition-all duration-500"
                            style={{ width: `${metrics?.auto_link_rate_pct || 0}%` }}
                        />
                    </div>
                </div>

                <div className="bg-white dark:bg-gray-800/50 border border-gray-200 dark:border-gray-700 rounded-xl p-6 shadow-sm dark:shadow-none">
                    <h3 className="text-lg font-semibold text-gray-900 dark:text-white mb-4">Last Run</h3>
                    <div className="h-32 flex items-center justify-center">
                        <div className="text-center">
                            <p className="text-2xl font-bold text-gray-900 dark:text-white">
                                {formatTimeAgo(metrics?.last_run_at || null)}
                            </p>
                            <p className="text-gray-500 dark:text-gray-400 text-sm mt-2">
                                Avg duration: {formatDuration(metrics?.avg_run_duration_seconds || null)}
                            </p>
                        </div>
                    </div>
                </div>

                <div className="bg-white dark:bg-gray-800/50 border border-gray-200 dark:border-gray-700 rounded-xl p-6 shadow-sm dark:shadow-none">
                    <h3 className="text-lg font-semibold text-gray-900 dark:text-white mb-4">System Health</h3>
                    <div className="h-32 flex items-center justify-center">
                        <div className="grid grid-cols-2 gap-4 w-full">
                            <div className="text-center p-3 bg-emerald-50 dark:bg-emerald-900/30 rounded-lg border border-emerald-200 dark:border-emerald-800">
                                <p className="text-emerald-600 dark:text-emerald-400 font-semibold">API</p>
                                <p className="text-emerald-500 dark:text-emerald-300 text-sm">✓ Online</p>
                            </div>
                            <div className={`text-center p-3 rounded-lg border ${isConnected
                                ? 'bg-emerald-50 dark:bg-emerald-900/30 border-emerald-200 dark:border-emerald-800'
                                : 'bg-red-50 dark:bg-red-900/30 border-red-200 dark:border-red-800'
                                }`}>
                                <p className={isConnected ? 'text-emerald-600 dark:text-emerald-400' : 'text-red-600 dark:text-red-400'}>WS</p>
                                <p className={`text-sm ${isConnected ? 'text-emerald-500 dark:text-emerald-300' : 'text-red-500 dark:text-red-300'}`}>
                                    {isConnected ? '✓ Connected' : '✗ Disconnected'}
                                </p>
                            </div>
                        </div>
                    </div>
                </div>
            </div>

            {/* Recent Runs */}
            <div className="bg-white dark:bg-gray-800/50 border border-gray-200 dark:border-gray-700 rounded-xl p-6 shadow-sm dark:shadow-none">
                <div className="flex items-center justify-between mb-4">
                    <h2 className="text-xl font-semibold text-gray-900 dark:text-white">Recent Runs</h2>
                    <a href="/pipeline" className="text-blue-600 dark:text-blue-400 hover:text-blue-500 dark:hover:text-blue-300 text-sm">
                        View All →
                    </a>
                </div>

                {recentRuns.length === 0 ? (
                    <div className="text-center py-8 text-gray-500 dark:text-gray-400">
                        No runs yet. Start your first pipeline run!
                    </div>
                ) : (
                    <div className="overflow-x-auto">
                        <table className="w-full">
                            <thead>
                                <tr className="text-left text-gray-500 dark:text-gray-400 text-sm border-b border-gray-200 dark:border-gray-700">
                                    <th className="pb-3">Run ID</th>
                                    <th className="pb-3">Mode</th>
                                    <th className="pb-3">Status</th>
                                    <th className="pb-3">Records</th>
                                    <th className="pb-3">Auto-Link</th>
                                    <th className="pb-3">Review</th>
                                    <th className="pb-3">Duration</th>
                                </tr>
                            </thead>
                            <tbody className="text-gray-700 dark:text-gray-300">
                                {recentRuns.map((run) => (
                                    <tr key={run.run_id} className="border-b border-gray-100 dark:border-gray-800 hover:bg-gray-50 dark:hover:bg-gray-700/30">
                                        <td className="py-3 font-mono text-sm">
                                            {run.run_id.slice(0, 8)}...
                                        </td>
                                        <td className="py-3">
                                            <span className="px-2 py-1 rounded text-xs bg-gray-100 dark:bg-gray-700 border border-gray-200 dark:border-gray-600">
                                                {run.mode}
                                            </span>
                                        </td>
                                        <td className="py-3">
                                            <span className={`px-2 py-1 rounded text-xs ${run.status === 'COMPLETED' ? 'bg-emerald-100 dark:bg-emerald-900/50 text-emerald-700 dark:text-emerald-400' :
                                                run.status === 'RUNNING' ? 'bg-blue-100 dark:bg-blue-900/50 text-blue-700 dark:text-blue-400' :
                                                    run.status === 'FAILED' ? 'bg-red-100 dark:bg-red-900/50 text-red-700 dark:text-red-400' :
                                                        'bg-gray-100 dark:bg-gray-700 text-gray-500 dark:text-gray-400'
                                                }`}>
                                                {run.status}
                                            </span>
                                        </td>
                                        <td className="py-3">{run.counters.records_in}</td>
                                        <td className="py-3 text-emerald-600 dark:text-emerald-400">{run.counters.auto_links}</td>
                                        <td className="py-3 text-yellow-600 dark:text-yellow-400">{run.counters.review_items}</td>
                                        <td className="py-3 text-gray-500 dark:text-gray-400">
                                            {formatDuration(run.duration_seconds)}
                                        </td>
                                    </tr>
                                ))}
                            </tbody>
                        </table>
                    </div>
                )}
            </div>
        </div>
    );
}
