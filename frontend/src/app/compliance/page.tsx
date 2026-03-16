'use client';

import { useState, useEffect } from 'react';
import { api } from '@/lib/api';

interface AuditEvent {
    audit_id: string;
    event_type: string;
    payload: any;
    actor: string;
    run_id: string | null;
    prev_hash: string;
    this_hash: string;
    created_at: string;
}

interface ChainVerification {
    valid: boolean;
    error: string | null;
    verified_at: string;
    chain_length: number;
}

interface ComplianceReport {
    chain_valid: boolean;
    validation_error: string | null;
    total_events: number;
    event_counts: Record<string, number>;
    first_event: string | null;
    last_event: string | null;
    last_hash: string;
    verified_at: string;
}

export default function CompliancePage() {
    const [events, setEvents] = useState<AuditEvent[]>([]);
    const [verification, setVerification] = useState<ChainVerification | null>(null);
    const [report, setReport] = useState<ComplianceReport | null>(null);
    const [isLoading, setIsLoading] = useState(true);
    const [isVerifying, setIsVerifying] = useState(false);
    const [error, setError] = useState<string | null>(null);

    useEffect(() => {
        fetchComplianceData();
    }, []);

    const fetchComplianceData = async () => {
        try {
            const [eventsData, reportData] = await Promise.all([
                api.getAuditEvents(undefined, undefined, 1, 50),
                api.getComplianceReport(),
            ]);
            setEvents(eventsData.events);
            setReport(reportData);
        } catch (err) {
            console.error('Failed to fetch compliance data:', err);
            setError('Failed to load compliance data');
        } finally {
            setIsLoading(false);
        }
    };

    const handleVerifyChain = async () => {
        setIsVerifying(true);
        setError(null);

        try {
            const result = await api.verifyAuditChain();
            setVerification(result);
        } catch (err) {
            setError('Failed to verify audit chain');
        } finally {
            setIsVerifying(false);
        }
    };

    const getEventTypeColor = (type: string) => {
        if (type.includes('COMPLETE') || type.includes('APPROVED')) return 'bg-emerald-100 dark:bg-emerald-900/50 text-emerald-600 dark:text-emerald-400';
        if (type.includes('FAILED') || type.includes('REJECTED')) return 'bg-red-100 dark:bg-red-900/50 text-red-600 dark:text-red-400';
        if (type.includes('STARTED') || type.includes('CREATED')) return 'bg-blue-100 dark:bg-blue-900/50 text-blue-600 dark:text-blue-400';
        if (type.includes('REVIEW')) return 'bg-yellow-100 dark:bg-yellow-900/50 text-yellow-600 dark:text-yellow-400';
        return 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400';
    };

    const formatTime = (isoString: string) => {
        const date = new Date(isoString);
        return date.toLocaleString();
    };

    if (isLoading) {
        return (
            <div className="p-8 flex items-center justify-center min-h-screen">
                <div className="text-gray-400">Loading compliance data...</div>
            </div>
        );
    }

    return (
        <div className="p-8 space-y-6">
            {/* Header */}
            <div className="flex items-center justify-between">
                <div>
                    <h1 className="text-3xl font-bold text-gray-900 dark:text-white">Compliance & Audit</h1>
                    <p className="text-gray-600 dark:text-gray-400 mt-1">
                        Tamper-evident audit chain with SHA-256 hash linking
                    </p>
                </div>
                <button
                    onClick={handleVerifyChain}
                    disabled={isVerifying}
                    className="bg-gradient-to-r from-purple-600 to-indigo-600 hover:from-purple-500 hover:to-indigo-500 disabled:opacity-50 text-white px-4 py-2 rounded-lg font-semibold transition-all duration-200"
                >
                    {isVerifying ? 'Verifying...' : '🔐 Verify Chain'}
                </button>
            </div>

            {error && (
                <div className="bg-red-100 dark:bg-red-900/20 border border-red-200 dark:border-red-700 rounded-lg p-4 text-red-600 dark:text-red-400">
                    {error}
                </div>
            )}

            {/* Verification Result */}
            {verification && (
                <div className={`border rounded-xl p-6 ${verification.valid
                        ? 'bg-emerald-50 dark:bg-emerald-900/20 border-emerald-200 dark:border-emerald-700'
                        : 'bg-red-50 dark:bg-red-900/20 border-red-200 dark:border-red-700'
                    }`}>
                    <div className="flex items-center gap-4">
                        <div className={`text-5xl ${verification.valid ? 'text-emerald-600 dark:text-emerald-400' : 'text-red-600 dark:text-red-400'}`}>
                            {verification.valid ? '✓' : '✗'}
                        </div>
                        <div>
                            <h2 className={`text-xl font-bold ${verification.valid ? 'text-emerald-600 dark:text-emerald-400' : 'text-red-600 dark:text-red-400'}`}>
                                {verification.valid ? 'Chain Verified Successfully' : 'Chain Verification Failed'}
                            </h2>
                            <p className="text-gray-600 dark:text-gray-400 mt-1">
                                {verification.chain_length} events verified at {formatTime(verification.verified_at)}
                            </p>
                            {verification.error && (
                                <p className="text-red-600 dark:text-red-400 mt-2">{verification.error}</p>
                            )}
                        </div>
                    </div>
                </div>
            )}

            {/* Stats Cards */}
            <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
                <div className="bg-white dark:bg-gray-800/50 border border-gray-200 dark:border-gray-700 rounded-xl p-4">
                    <p className="text-gray-600 dark:text-gray-400 text-sm">Total Events</p>
                    <p className="text-2xl font-bold text-gray-900 dark:text-white">{report?.total_events || 0}</p>
                </div>
                <div className="bg-white dark:bg-gray-800/50 border border-gray-200 dark:border-gray-700 rounded-xl p-4">
                    <p className="text-gray-600 dark:text-gray-400 text-sm">Chain Status</p>
                    <p className={`text-2xl font-bold ${report?.chain_valid ? 'text-emerald-600 dark:text-emerald-400' : 'text-red-600 dark:text-red-400'}`}>
                        {report?.chain_valid ? '✓ Valid' : '✗ Invalid'}
                    </p>
                </div>
                <div className="bg-white dark:bg-gray-800/50 border border-gray-200 dark:border-gray-700 rounded-xl p-4">
                    <p className="text-gray-600 dark:text-gray-400 text-sm">First Event</p>
                    <p className="text-lg font-medium text-gray-900 dark:text-white">
                        {report?.first_event ? formatTime(report.first_event).split(',')[0] : '—'}
                    </p>
                </div>
                <div className="bg-white dark:bg-gray-800/50 border border-gray-200 dark:border-gray-700 rounded-xl p-4">
                    <p className="text-gray-600 dark:text-gray-400 text-sm">Last Event</p>
                    <p className="text-lg font-medium text-gray-900 dark:text-white">
                        {report?.last_event ? formatTime(report.last_event).split(',')[0] : '—'}
                    </p>
                </div>
            </div>

            {/* Event Breakdown */}
            {report && Object.keys(report.event_counts).length > 0 && (
                <div className="bg-white dark:bg-gray-800/50 border border-gray-200 dark:border-gray-700 rounded-xl p-6">
                    <h2 className="text-xl font-semibold text-gray-900 dark:text-white mb-4">Event Breakdown</h2>
                    <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                        {Object.entries(report.event_counts).map(([type, count]) => (
                            <div key={type} className="bg-gray-50 dark:bg-gray-900/50 rounded-lg p-4">
                                <span className={`px-2 py-1 rounded text-xs ${getEventTypeColor(type)}`}>
                                    {type}
                                </span>
                                <p className="text-2xl font-bold text-gray-900 dark:text-white mt-2">{count}</p>
                            </div>
                        ))}
                    </div>
                </div>
            )}

            {/* Last Hash */}
            {report?.last_hash && (
                <div className="bg-white dark:bg-gray-800/50 border border-gray-200 dark:border-gray-700 rounded-xl p-6">
                    <h2 className="text-lg font-semibold text-gray-900 dark:text-white mb-2">Latest Chain Hash</h2>
                    <code className="block w-full bg-gray-100 dark:bg-gray-900 p-4 rounded-lg font-mono text-sm text-cyan-600 dark:text-cyan-400 overflow-x-auto">
                        {report.last_hash}
                    </code>
                    <p className="text-gray-500 text-sm mt-2">
                        SHA-256 hash of the most recent audit event
                    </p>

                </div>
            )}

            {/* Audit Events Table */}
            <div className="bg-gray-800/50 border border-gray-700 rounded-xl p-6">
                <h2 className="text-xl font-semibold text-white mb-4">Audit Trail</h2>

                {events.length === 0 ? (
                    <div className="text-center py-12 text-gray-400">
                        <p className="text-4xl mb-4">📋</p>
                        <p>No audit events recorded yet</p>
                    </div>
                ) : (
                    <div className="overflow-x-auto">
                        <table className="w-full">
                            <thead>
                                <tr className="text-left text-gray-400 text-sm border-b border-gray-700">
                                    <th className="pb-3">Time</th>
                                    <th className="pb-3">Type</th>
                                    <th className="pb-3">Actor</th>
                                    <th className="pb-3">Run</th>
                                    <th className="pb-3">Hash</th>
                                </tr>
                            </thead>
                            <tbody className="text-gray-300">
                                {events.map((event) => (
                                    <tr key={event.audit_id} className="border-b border-gray-800 hover:bg-gray-700/30">
                                        <td className="py-3 text-sm">
                                            {formatTime(event.created_at)}
                                        </td>
                                        <td className="py-3">
                                            <span className={`px-2 py-1 rounded text-xs ${getEventTypeColor(event.event_type)}`}>
                                                {event.event_type}
                                            </span>
                                        </td>
                                        <td className="py-3">{event.actor}</td>
                                        <td className="py-3 font-mono text-sm">
                                            {event.run_id ? event.run_id.slice(0, 8) + '...' : '—'}
                                        </td>
                                        <td className="py-3 font-mono text-xs text-gray-500">
                                            {event.this_hash}
                                        </td>
                                    </tr>
                                ))}
                            </tbody>
                        </table>
                    </div>
                )}
            </div>

            {/* Export Button */}
            <div className="flex justify-end">
                <a
                    href={`${process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000'}/audit/export`}
                    target="_blank"
                    rel="noopener noreferrer"
                    className="bg-gray-700 hover:bg-gray-600 text-white px-4 py-2 rounded-lg font-medium transition-colors"
                >
                    📥 Export Audit Log
                </a>
            </div>
        </div>
    );
}
