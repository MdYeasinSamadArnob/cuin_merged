'use client';

import { useState, useEffect } from 'react';
import { api } from '@/lib/api';

interface ReviewItem {
    review_id: string;
    pair_id: string;
    run_id: string;
    a_key: string;
    b_key: string;
    score: number;
    evidence: Array<{
        field: string;
        type?: string;
        value_a?: string;
        value_b?: string;
        similarity?: number;
    }>;
    signals: string[];
    status: string;
    reviewer: string | null;
    review_reason: string | null;
    reviewed_at: string | null;
    created_at: string;
    has_ai_explanation: boolean;
}

interface ReviewStats {
    pending: number;
    approved: number;
    rejected: number;
    total: number;
    avg_review_time_seconds: number;
    with_ai_explanation: number;
}

export default function ReviewPage() {
    const [queue, setQueue] = useState<ReviewItem[]>([]);
    const [stats, setStats] = useState<ReviewStats | null>(null);
    const [selectedItem, setSelectedItem] = useState<ReviewItem | null>(null);
    const [isLoading, setIsLoading] = useState(true);
    const [actionLoading, setActionLoading] = useState(false);
    const [reviewReason, setReviewReason] = useState('');
    const [reviewerName, setReviewerName] = useState('Reviewer');
    const [error, setError] = useState<string | null>(null);
    const [explanation, setExplanation] = useState<string | null>(null);
    const [isExplanationLoading, setIsExplanationLoading] = useState(false);

    useEffect(() => {
        fetchReviewData();
    }, []);

    useEffect(() => {
        if (selectedItem?.has_ai_explanation) {
            setIsExplanationLoading(true);
            api.getExplanation(selectedItem.pair_id)
                .then(data => {
                    if (data.available) {
                        setExplanation(data.explanation_text);
                    } else {
                        setExplanation(null);
                    }
                })
                .catch(err => {
                    console.error('Failed to fetch explanation:', err);
                    setExplanation(null);
                })
                .finally(() => setIsExplanationLoading(false));
        } else {
            setExplanation(null);
        }
    }, [selectedItem]);

    const fetchReviewData = async () => {
        try {
            const [queueData, statsData] = await Promise.all([
                api.getReviewQueue(),
                api.getReviewStats(),
            ]);
            setQueue(queueData.items);
            setStats(statsData);
        } catch (err) {
            console.error('Failed to fetch review data:', err);
            setError('Failed to load review queue');
        } finally {
            setIsLoading(false);
        }
    };

    const handleApprove = async () => {
        if (!selectedItem || !reviewReason.trim()) {
            setError('Please provide a reason for approval');
            return;
        }

        setActionLoading(true);
        setError(null);

        try {
            await api.approveReview(selectedItem.pair_id, reviewerName, reviewReason);
            setSelectedItem(null);
            setReviewReason('');
            fetchReviewData();
        } catch (err) {
            setError('Failed to approve review');
        } finally {
            setActionLoading(false);
        }
    };

    const handleReject = async () => {
        if (!selectedItem || !reviewReason.trim()) {
            setError('Please provide a reason for rejection');
            return;
        }

        setActionLoading(true);
        setError(null);

        try {
            await api.rejectReview(selectedItem.pair_id, reviewerName, reviewReason);
            setSelectedItem(null);
            setReviewReason('');
            fetchReviewData();
        } catch (err) {
            setError('Failed to reject review');
        } finally {
            setActionLoading(false);
        }
    };

    const getScoreColor = (score: number) => {
        if (score >= 0.7) return 'text-emerald-600 dark:text-emerald-400';
        if (score >= 0.5) return 'text-yellow-600 dark:text-yellow-400';
        return 'text-orange-600 dark:text-orange-400';
    };

    const getScoreBg = (score: number) => {
        if (score >= 0.7) return 'bg-emerald-100 dark:bg-emerald-900/30 border-emerald-200 dark:border-emerald-700';
        if (score >= 0.5) return 'bg-yellow-100 dark:bg-yellow-900/30 border-yellow-200 dark:border-yellow-700';
        return 'bg-orange-100 dark:bg-orange-900/30 border-orange-200 dark:border-orange-700';
    };

    if (isLoading) {
        return (
            <div className="p-8 flex items-center justify-center min-h-screen bg-gray-50 dark:bg-gray-950 transition-colors">
                <div className="text-gray-500 dark:text-gray-400">Loading review queue...</div>
            </div>
        );
    }

    return (
        <div className="p-8 space-y-6 min-h-screen bg-gray-50 dark:bg-gray-950 transition-colors">
            {/* Header */}
            <div className="flex items-center justify-between">
                <div>
                    <h1 className="text-3xl font-bold text-gray-900 dark:text-white">Review Queue</h1>
                    <p className="text-gray-600 dark:text-gray-400 mt-1">
                        Human-in-the-loop review for uncertain matches
                    </p>
                </div>
            </div>

            {error && (
                <div className="bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-700 rounded-lg p-4 text-red-600 dark:text-red-400">
                    {error}
                </div>
            )}

            {/* Stats Cards */}
            <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
                <div className="bg-yellow-50 dark:bg-yellow-900/20 border border-yellow-200 dark:border-yellow-700 rounded-xl p-4">
                    <p className="text-yellow-700 dark:text-yellow-400 text-sm">Pending</p>
                    <p className="text-2xl font-bold text-yellow-700 dark:text-yellow-400">{stats?.pending || 0}</p>
                </div>
                <div className="bg-emerald-50 dark:bg-emerald-900/20 border border-emerald-200 dark:border-emerald-700 rounded-xl p-4">
                    <p className="text-emerald-700 dark:text-emerald-400 text-sm">Approved</p>
                    <p className="text-2xl font-bold text-emerald-700 dark:text-emerald-400">{stats?.approved || 0}</p>
                </div>
                <div className="bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-700 rounded-xl p-4">
                    <p className="text-red-700 dark:text-red-400 text-sm">Rejected</p>
                    <p className="text-2xl font-bold text-red-700 dark:text-red-400">{stats?.rejected || 0}</p>
                </div>
                <div className="bg-white dark:bg-gray-800/50 border border-gray-200 dark:border-gray-700 rounded-xl p-4">
                    <p className="text-gray-500 dark:text-gray-400 text-sm">Total</p>
                    <p className="text-2xl font-bold text-gray-900 dark:text-white">{stats?.total || 0}</p>
                </div>
            </div>

            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                {/* Queue List */}
                <div className="bg-white dark:bg-gray-800/50 border border-gray-200 dark:border-gray-700 rounded-xl p-6 shadow-sm dark:shadow-none">
                    <h2 className="text-xl font-semibold text-gray-900 dark:text-white mb-4">Pending Reviews</h2>

                    {queue.filter(item => item.status === 'PENDING').length === 0 ? (
                        <div className="text-center py-12 text-gray-500 dark:text-gray-400">
                            <p className="text-4xl mb-4">✅</p>
                            <p>No items pending review!</p>
                        </div>
                    ) : (
                        <div className="space-y-3 max-h-[500px] overflow-y-auto">
                            {queue
                                .filter(item => item.status === 'PENDING')
                                .map((item) => (
                                    <div
                                        key={item.review_id}
                                        onClick={() => {
                                            setSelectedItem(item);
                                            setReviewReason('');
                                            setError(null);
                                        }}
                                        className={`p-4 rounded-lg border cursor-pointer transition-all ${selectedItem?.review_id === item.review_id
                                            ? 'border-blue-500 bg-blue-50 dark:bg-blue-900/20'
                                            : 'border-gray-200 dark:border-gray-700 bg-gray-50 dark:bg-gray-900/50 hover:border-gray-300 dark:hover:border-gray-600'
                                            }`}
                                    >
                                        <div className="flex items-center justify-between">
                                            <div className="flex items-center gap-3">
                                                <span className={`px-2 py-1 rounded border text-sm font-medium ${getScoreBg(item.score)} ${getScoreColor(item.score)}`}>
                                                    {(item.score * 100).toFixed(0)}%
                                                </span>
                                                <div>
                                                    <p className="text-gray-900 dark:text-white font-mono text-sm">
                                                        {item.a_key.slice(0, 8)}... ↔ {item.b_key.slice(0, 8)}...
                                                    </p>
                                                    <p className="text-gray-500 text-xs mt-1">
                                                        {item.signals.length} signals hit
                                                    </p>
                                                </div>
                                            </div>
                                            {item.has_ai_explanation && (
                                                <span className="text-purple-500 dark:text-purple-400 text-sm" title="AI Explanation Available">
                                                    🤖
                                                </span>
                                            )}
                                        </div>
                                    </div>
                                ))}
                        </div>
                    )}
                </div>

                {/* Detail Panel */}
                <div className="bg-white dark:bg-gray-800/50 border border-gray-200 dark:border-gray-700 rounded-xl p-6 shadow-sm dark:shadow-none">
                    <h2 className="text-xl font-semibold text-gray-900 dark:text-white mb-4">Review Details</h2>

                    {!selectedItem ? (
                        <div className="text-center py-12 text-gray-500 dark:text-gray-400">
                            <p className="text-4xl mb-4">👆</p>
                            <p>Select an item from the queue to review</p>
                        </div>
                    ) : (
                        <div className="space-y-6">
                            {/* Score */}
                            <div className="text-center">
                                <div className={`inline-flex items-center justify-center w-24 h-24 rounded-full border-4 ${getScoreBg(selectedItem.score)}`}>
                                    <span className={`text-3xl font-bold ${getScoreColor(selectedItem.score)}`}>
                                        {(selectedItem.score * 100).toFixed(0)}%
                                    </span>
                                </div>
                                <p className="text-gray-500 dark:text-gray-400 mt-2">Match Score</p>
                            </div>

                            {/* AI Explanation */}
                            {(selectedItem.has_ai_explanation || explanation) && (
                                <div className="bg-purple-50 dark:bg-purple-900/10 border border-purple-200 dark:border-purple-700/50 rounded-lg p-4">
                                    <h3 className="text-lg font-medium text-purple-700 dark:text-purple-300 mb-2 flex items-center gap-2">
                                        <span>🤖</span> AI Analysis
                                    </h3>
                                    {isExplanationLoading ? (
                                        <div className="text-gray-500 dark:text-gray-400 text-sm animate-pulse">
                                            Generating explanation...
                                        </div>
                                    ) : explanation ? (
                                        <div className="whitespace-pre-wrap font-sans text-sm text-gray-700 dark:text-gray-300 bg-white dark:bg-black/20 p-3 rounded border border-purple-200 dark:border-purple-900/30">
                                            {explanation}
                                        </div>
                                    ) : (
                                        <div className="text-gray-500 text-sm">
                                            Explanation details not available
                                        </div>
                                    )}
                                </div>
                            )}

                            {/* Evidence */}
                            <div>
                                <h3 className="text-lg font-medium text-gray-900 dark:text-white mb-3">Evidence</h3>
                                <div className="space-y-2">
                                    {selectedItem.evidence.length > 0 ? (
                                        selectedItem.evidence.map((ev, idx) => (
                                            <div key={idx} className="flex items-center justify-between p-3 bg-gray-50 dark:bg-gray-900/50 rounded-lg border border-gray-100 dark:border-gray-800">
                                                <div>
                                                    <span className="text-gray-700 dark:text-gray-300 font-medium">{ev.field}</span>
                                                    <span className={`ml-2 text-xs px-2 py-0.5 rounded ${ev.type === 'exact_match' ? 'bg-emerald-100 dark:bg-emerald-900/50 text-emerald-700 dark:text-emerald-400' :
                                                        ev.type === 'fuzzy_match' ? 'bg-yellow-100 dark:bg-yellow-900/50 text-yellow-700 dark:text-yellow-400' :
                                                            'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400'
                                                        }`}>
                                                        {ev.type || 'unknown'}
                                                    </span>
                                                </div>
                                                {ev.similarity !== undefined && (
                                                    <span className="text-gray-600 dark:text-gray-400">
                                                        {(ev.similarity * 100).toFixed(0)}%
                                                    </span>
                                                )}
                                            </div>
                                        ))
                                    ) : (
                                        <p className="text-gray-500 text-sm">No evidence details available</p>
                                    )}
                                </div>
                            </div>

                            {/* Signals */}
                            <div>
                                <h3 className="text-lg font-medium text-gray-900 dark:text-white mb-3">Signals Hit</h3>
                                <div className="flex flex-wrap gap-2">
                                    {selectedItem.signals.map((signal, idx) => (
                                        <span key={idx} className="px-3 py-1 bg-purple-100 dark:bg-purple-900/30 border border-purple-200 dark:border-purple-700 text-purple-700 dark:text-purple-400 rounded-full text-sm">
                                            {signal}
                                        </span>
                                    ))}
                                    {selectedItem.signals.length === 0 && (
                                        <span className="text-gray-500 text-sm">No signals</span>
                                    )}
                                </div>
                            </div>

                            {/* Reviewer Input */}
                            <div className="border-t border-gray-200 dark:border-gray-700 pt-4 space-y-4">
                                <div>
                                    <label className="block text-sm text-gray-600 dark:text-gray-400 mb-2">Reviewer Name</label>
                                    <input
                                        type="text"
                                        value={reviewerName}
                                        onChange={(e) => setReviewerName(e.target.value)}
                                        className="w-full px-4 py-2 bg-white dark:bg-gray-900 border border-gray-300 dark:border-gray-700 rounded-lg text-gray-900 dark:text-white focus:border-blue-500 focus:outline-none"
                                        placeholder="Your name"
                                    />
                                </div>
                                <div>
                                    <label className="block text-sm text-gray-600 dark:text-gray-400 mb-2">Reason (Required)</label>
                                    <textarea
                                        value={reviewReason}
                                        onChange={(e) => setReviewReason(e.target.value)}
                                        className="w-full px-4 py-2 bg-white dark:bg-gray-900 border border-gray-300 dark:border-gray-700 rounded-lg text-gray-900 dark:text-white focus:border-blue-500 focus:outline-none resize-none"
                                        rows={3}
                                        placeholder="Explain your decision..."
                                    />
                                </div>
                            </div>

                            {/* Action Buttons */}
                            <div className="flex gap-4">
                                <button
                                    onClick={handleApprove}
                                    disabled={actionLoading || !reviewReason.trim()}
                                    className="flex-1 bg-emerald-600 hover:bg-emerald-500 disabled:opacity-50 disabled:cursor-not-allowed text-white py-3 rounded-lg font-semibold transition-colors"
                                >
                                    ✓ Approve Match
                                </button>
                                <button
                                    onClick={handleReject}
                                    disabled={actionLoading || !reviewReason.trim()}
                                    className="flex-1 bg-red-600 hover:bg-red-500 disabled:opacity-50 disabled:cursor-not-allowed text-white py-3 rounded-lg font-semibold transition-colors"
                                >
                                    ✗ Reject Match
                                </button>
                            </div>
                        </div>
                    )}
                </div>
            </div>

            {/* Completed Reviews */}
            <div className="bg-white dark:bg-gray-800/50 border border-gray-200 dark:border-gray-700 rounded-xl p-6 shadow-sm dark:shadow-none">
                <h2 className="text-xl font-semibold text-gray-900 dark:text-white mb-4">Recently Reviewed</h2>
                <div className="overflow-x-auto">
                    <table className="w-full">
                        <thead>
                            <tr className="text-left text-gray-500 dark:text-gray-400 text-sm border-b border-gray-200 dark:border-gray-700">
                                <th className="pb-3 pl-2">Pair</th>
                                <th className="pb-3">Score</th>
                                <th className="pb-3">Decision</th>
                                <th className="pb-3">Reviewer</th>
                                <th className="pb-3">Reason</th>
                            </tr>
                        </thead>
                        <tbody className="text-gray-700 dark:text-gray-300">
                            {queue
                                .filter(item => item.status !== 'PENDING')
                                .slice(0, 10)
                                .map((item) => (
                                    <tr key={item.review_id} className="border-b border-gray-100 dark:border-gray-800 hover:bg-gray-50 dark:hover:bg-gray-800/30 transition-colors">
                                        <td className="py-3 pl-2 font-mono text-sm">
                                            {item.a_key.slice(0, 8)}... ↔ {item.b_key.slice(0, 8)}...
                                        </td>
                                        <td className="py-3">
                                            <span className={getScoreColor(item.score)}>
                                                {(item.score * 100).toFixed(0)}%
                                            </span>
                                        </td>
                                        <td className="py-3">
                                            <span className={`px-2 py-1 rounded text-xs ${item.status === 'APPROVED'
                                                ? 'bg-emerald-100 dark:bg-emerald-900/50 text-emerald-700 dark:text-emerald-400'
                                                : 'bg-red-100 dark:bg-red-900/50 text-red-700 dark:text-red-400'
                                                }`}>
                                                {item.status}
                                            </span>
                                        </td>
                                        <td className="py-3">{item.reviewer || '—'}</td>
                                        <td className="py-3 text-gray-500 dark:text-gray-400 text-sm max-w-[200px] truncate">
                                            {item.review_reason || '—'}
                                        </td>
                                    </tr>
                                ))}
                            {queue.filter(item => item.status !== 'PENDING').length === 0 && (
                                <tr>
                                    <td colSpan={5} className="py-8 text-center text-gray-500">
                                        No reviews completed yet
                                    </td>
                                </tr>
                            )}
                        </tbody>
                    </table>
                </div>
            </div>
        </div>
    );
}
