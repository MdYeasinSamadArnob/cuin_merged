const API_URL = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000';

// Type exports for use by stores
export interface Run {
    run_id: string;
    status: string;
    description?: string;
    counters: {
        auto_links: number;
        review_items: number;
        rejected: number;
        pairs_scored: number;
    };
    created_at?: string;
    updated_at?: string;
}

export interface DashboardMetrics {
    total_records: number;
    total_entities: number;
    dedup_rate: number;
    pending_reviews: number;
    runs_today: number;
}

export interface ResetDataResponse {
    success: boolean;
    message: string;
    deleted_files: number;
    deleted_runs: number;
}

async function fetchJson(endpoint: string, options: RequestInit = {}) {
    const res = await fetch(`${API_URL}${endpoint}`, {
        ...options,
        headers: {
            'Content-Type': 'application/json',
            ...options.headers,
        },
    });
    if (!res.ok) {
        throw new Error(`API Error: ${res.statusText}`);
    }
    return res.json();
}

export const api = {
    // Metrics
    getDashboardMetrics: () => fetchJson('/metrics/dashboard'),

    // Runs
    listRuns: (page = 1, limit = 50) => fetchJson(`/runs?page=${page}&page_size=${limit}`),
    getRun: (runId: string) => fetchJson(`/runs/${runId}`),
    startRun: (mode: string, description: string) => fetchJson('/runs', {
        method: 'POST',
        body: JSON.stringify({ mode, description }),
    }),

    // Config
    getConfig: () => fetchJson('/config'),
    updateConfig: (data: any) => fetchJson('/config', {
        method: 'POST',
        body: JSON.stringify(data),
    }),

    // Review
    getReviewQueue: () => fetchJson('/review/queue'),
    getReviewStats: () => fetchJson('/review/stats'),
    approveReview: (pairId: string, reviewer: string, reason: string) => fetchJson(`/review/${pairId}/approve`, {
        method: 'POST',
        body: JSON.stringify({ reviewer, reason }),
    }),
    rejectReview: (pairId: string, reviewer: string, reason: string) => fetchJson(`/review/${pairId}/reject`, {
        method: 'POST',
        body: JSON.stringify({ reviewer, reason }),
    }),
    getExplanation: (pairId: string) => fetchJson(`/review/${pairId}/explanation`),
    explainMatch: (pairId: string) => fetchJson(`/review/${pairId}/explanation`), // Alias for ExplorerPage

    // Matches / Explorer
    getMatchScores: (runId: string, page: number, limit: number, minScore?: number) => {
        let url = `/matches/run/${runId}/scores?page=${page}&page_size=${limit}`;
        if (minScore !== undefined) {
            url += `&min_score=${minScore}`;
        }
        return fetchJson(url);
    },
    getMatchDetails: (pairId: string) => fetchJson(`/matches/${pairId}`), // Use matches endpoint for scored pairs

    // Uniques - list unique/singleton records
    getUniques: (runId: string, page: number, limit: number) => {
        return fetchJson(`/matches/run/${runId}/uniques?page=${page}&page_size=${limit}`);
    },

    // Audit
    getAuditEvents: (entityId?: string, runId?: string, page = 1, limit = 50) => {
        const params = new URLSearchParams({ page: String(page), page_size: String(limit) });
        if (entityId) params.append('entity_id', entityId);
        if (runId) params.append('run_id', runId);
        return fetchJson(`/audit/events?${params.toString()}`);
    },
    getComplianceReport: () => fetchJson('/audit/report'),
    verifyAuditChain: () => fetchJson('/audit/verify', { method: 'POST' }),

    // Graph / Clusters (Explorer)
    getClusters: (runId: string, page: number, limit: number) => {
        // Use the matches clusters endpoint which has proper pagination
        return fetchJson(`/matches/run/${runId}/clusters?page=${page}&page_size=${limit}`);
    },

    getClusterEntities: (page: number, limit: number, minSize: number, runId: string) => {
        return fetchJson(`/matches/run/${runId}/clusters?page=${page}&page_size=${limit}&min_size=${minSize}`);
    },

    getGraphData: (runId?: string, limit: number = 2000) => {
        let url = `/graph/clusters?limit=${limit}`;
        if (runId) url += `&run_id=${runId}`;
        // Ensure singletons are included for full visibility
        url += `&include_singletons=true`;
        return fetchJson(url);
    },

    // Preview
    previewClustering: (runId: string | undefined, scoring: any) => fetchJson('/graph/preview', {
        method: 'POST',
        body: JSON.stringify({ run_id: runId, scoring }),
    }),

    resetAllData: (): Promise<ResetDataResponse> => fetchJson('/admin/reset', {
        method: 'POST',
    }),
};
