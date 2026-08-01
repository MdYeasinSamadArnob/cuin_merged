'use client';

import { useState, useEffect, useCallback } from 'react';
import { Copy, Check, KeyRound, ExternalLink, RefreshCw } from 'lucide-react';
import { api, API_BASE_URL } from '@/lib/api';

interface ApiTokenInfo {
    bearer_token: string;
    source: 'env' | 'auto-generated';
}

const PUBLIC_API_BASE = `${API_BASE_URL}/api/v1`;

function CopyButton({ text }: { text: string }) {
    const [copied, setCopied] = useState(false);
    return (
        <button
            onClick={() => { navigator.clipboard.writeText(text); setCopied(true); setTimeout(() => setCopied(false), 1500); }}
            className="p-1.5 rounded-md hover:bg-gray-200 dark:hover:bg-gray-700 text-gray-500 dark:text-gray-400 transition-colors shrink-0"
            title="Copy"
        >
            {copied ? <Check size={14} className="text-emerald-500" /> : <Copy size={14} />}
        </button>
    );
}

export default function ApiDocsPage() {
    const [tokenInfo, setTokenInfo] = useState<ApiTokenInfo | null>(null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState<string | null>(null);

    const loadToken = useCallback(async () => {
        setLoading(true);
        try {
            const data = await api.getApiToken();
            setTokenInfo(data);
        } catch (e: any) {
            setError(String(e.message || e));
        } finally {
            setLoading(false);
        }
    }, []);

    useEffect(() => { loadToken(); }, [loadToken]);

    const token = tokenInfo?.bearer_token ?? '<loading...>';
    const curlExample = `curl -X POST ${PUBLIC_API_BASE}/identity/screen \\
  -H "Content-Type: application/json" \\
  -H "Authorization: Bearer ${token}" \\
  -d '{
    "full_name": "MD RAHIM UDDIN",
    "mobile": "01712345678",
    "date_of_birth": "1985-03-14"
  }'`;

    return (
        <div className="p-8 space-y-6">
            {/* Header */}
            <div>
                <h1 className="text-3xl font-bold text-gray-900 dark:text-white">Identity Recognition API</h1>
                <p className="text-gray-600 dark:text-gray-400 mt-1">
                    Real-time "does this person already exist" screening for new-account entity resolution -- reuses the exact same rules, thresholds, and Global ID registry the officer workbench uses.
                </p>
            </div>

            {/* Getting Started */}
            <div className="glass-card p-6 space-y-5">
                <h2 className="font-semibold text-gray-900 dark:text-white">Getting Started</h2>

                <div>
                    <div className="text-xs text-gray-500 dark:text-gray-400 mb-1">Base URL</div>
                    <div className="flex items-center gap-2 bg-gray-50 dark:bg-gray-900/60 border border-gray-200 dark:border-gray-700 rounded-lg px-3 py-2 font-mono text-sm text-gray-800 dark:text-gray-200">
                        <span className="flex-1 truncate">{PUBLIC_API_BASE}</span>
                        <CopyButton text={PUBLIC_API_BASE} />
                    </div>
                </div>

                {/* Bearer token */}
                <div>
                    <div className="flex items-center justify-between mb-1">
                        <div className="text-xs text-gray-500 dark:text-gray-400 flex items-center gap-1.5">
                            <KeyRound size={13} /> Bearer Token
                        </div>
                        <button onClick={loadToken} className="text-xs text-gray-400 hover:text-gray-600 dark:hover:text-gray-300 flex items-center gap-1">
                            <RefreshCw size={11} /> Refresh
                        </button>
                    </div>

                    {error && <p className="text-xs text-red-500 mb-2">{error}</p>}

                    <div className="flex items-center gap-2 bg-gray-50 dark:bg-gray-900/60 border border-gray-200 dark:border-gray-700 rounded-lg px-3 py-2 font-mono text-sm text-gray-800 dark:text-gray-200">
                        <span className="flex-1 truncate">{loading ? 'Loading...' : token}</span>
                        {!loading && <CopyButton text={token} />}
                    </div>

                    <p className="text-xs text-gray-400 dark:text-gray-500 mt-1.5">
                        {tokenInfo?.source === 'env' ? (
                            <>Pinned via <code className="font-mono">PUBLIC_API_BEARER_TOKEN</code> in the backend's <code className="font-mono">.env</code> -- stable across restarts.</>
                        ) : (
                            <>Auto-generated at process startup (no <code className="font-mono">PUBLIC_API_BEARER_TOKEN</code> set) -- this value changes every backend restart. Set it in <code className="font-mono">.env</code> to pin it for real bank integrations.</>
                        )}
                        {' '}One global token for the whole API -- there is no per-partner key management.
                    </p>
                </div>

                <div>
                    <div className="text-xs text-gray-500 dark:text-gray-400 mb-1">Example request</div>
                    <div className="relative bg-gray-900 dark:bg-black/60 border border-gray-800 rounded-lg p-3 font-mono text-xs text-gray-200 overflow-x-auto">
                        <pre className="whitespace-pre">{curlExample}</pre>
                        <div className="absolute top-2 right-2">
                            <CopyButton text={curlExample} />
                        </div>
                    </div>
                </div>
            </div>

            {/* Swagger UI */}
            <div className="glass-card p-0 overflow-hidden">
                <div className="flex items-center justify-between px-6 py-3 border-b border-gray-200 dark:border-gray-700">
                    <h2 className="font-semibold text-gray-900 dark:text-white text-sm">Interactive API Reference</h2>
                    <a
                        href={`${PUBLIC_API_BASE}/docs`}
                        target="_blank"
                        rel="noreferrer"
                        className="flex items-center gap-1 text-xs text-blue-600 dark:text-blue-400 hover:underline"
                    >
                        Open full-screen <ExternalLink size={12} />
                    </a>
                </div>
                <iframe
                    src={`${PUBLIC_API_BASE}/docs`}
                    className="w-full bg-white"
                    style={{ height: '900px', border: 'none' }}
                    title="CUIN Identity Recognition API -- Swagger UI"
                />
            </div>
        </div>
    );
}
