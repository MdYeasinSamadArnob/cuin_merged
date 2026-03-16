/**
 * API Client for CUIN v2 Backend
 * 
 * Centralized HTTP client for all backend API calls.
 */

const API_BASE_URL = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000';

class ApiClient {
    private baseUrl: string;

    constructor(baseUrl: string) {
        this.baseUrl = baseUrl;
    }

    private async request<T>(endpoint: string, options: RequestInit = {}): Promise<T> {
        const url = `${this.baseUrl}${endpoint}`;
        const response = await fetch(url, {
            ...options,
            headers: {
                'Content-Type': 'application/json',
                ...options.headers,
            },
        });

        if (!response.ok) {
            const error = await response.text();
            throw new Error(`API Error: ${response.status} - ${error}`);
        }

        return response.json();
    }

    // Dashboard
    async getDashboardMetrics() {
        return this.request('/metrics/dashboard');
    }

    // Runs
    async listRuns(page: number = 1, pageSize: number = 10) {
        return this.request(`/runs?page=${page}&page_size=${pageSize}`);
    }

    async getRun(runId: string) {
        return this.request(`/runs/${runId}`);
    }

    async startDatasourcePipeline(mode: string) {
        return this.request('/datasource/pipeline/start', {
            method: 'POST',
            body: JSON.stringify({ mode }),
        });
    }

    // Config
    async getConfig() {
        return this.request('/config');
    }

    async updateConfig(config: any) {
        return this.request('/config', {
            method: 'PUT',
            body: JSON.stringify(config),
        });
    }

    // Review
    async getReviewQueue() {
        return this.request('/review/queue');
    }

    async getReviewStats() {
        return this.request('/review/stats');
    }

    async approveReview(pairId: string, reviewer: string, reason: string) {
        return this.request(`/review/${pairId}/approve`, {
            method: 'POST',
            body: JSON.stringify({ reviewer, reason }),
        });
    }

    async rejectReview(pairId: string, reviewer: string, reason: string) {
        return this.request(`/review/${pairId}/reject`, {
            method: 'POST',
            body: JSON.stringify({ reviewer, reason }),
        });
    }

    async getExplanation(pairId: string) {
        return this.request(`/matches/${pairId}/explanation`);
    }

    // Graph
    async getGraphData(runId: string, limit: number = 1000) {
        return this.request(`/graph/data?run_id=${runId}&limit=${limit}`);
    }

    async previewClustering(runId: string | undefined, config: any) {
        const query = runId ? `?run_id=${runId}` : '';
        return this.request(`/graph/preview${query}`, {
            method: 'POST',
            body: JSON.stringify(config),
        });
    }

    async getClusters(runId: string, page: number = 1, pageSize: number = 10) {
        return this.request(`/graph/clusters?run_id=${runId}&page=${page}&page_size=${pageSize}`);
    }

    async getUniques(runId: string, page: number = 1, pageSize: number = 10) {
        return this.request(`/graph/uniques?run_id=${runId}&page=${page}&page_size=${pageSize}`);
    }

    async getClusterEntities(page: number, pageSize: number, minSize: number, runId?: string) {
        const query = new URLSearchParams({
            page: page.toString(),
            page_size: pageSize.toString(),
            min_size: minSize.toString(),
            ...(runId && { run_id: runId }),
        });
        return this.request(`/graph/cluster-entities?${query.toString()}`);
    }

    // Matches
    async getMatchScores(runId: string, page: number = 1, pageSize: number = 10, minScore?: number) {
        const params = new URLSearchParams({
            run_id: runId,
            page: page.toString(),
            page_size: pageSize.toString(),
        });
        if (minScore !== undefined) {
            params.append('min_score', minScore.toString());
        }
        return this.request(`/matches/scores?${params.toString()}`);
    }

    async getMatchDetails(pairId: string) {
        return this.request(`/matches/${pairId}`);
    }

    // Audit
    async getAuditEvents(eventType?: string, entityId?: string, page: number = 1, pageSize: number = 50) {
        const params = new URLSearchParams({
            page: page.toString(),
            page_size: pageSize.toString(),
        });
        if (eventType) params.append('event_type', eventType);
        if (entityId) params.append('entity_id', entityId);
        return this.request(`/audit/events?${params.toString()}`);
    }

    async getComplianceReport() {
        return this.request('/audit/compliance-report');
    }

    async verifyAuditChain() {
        return this.request('/audit/verify-chain');
    }

    // Admin
    async resetAllData() {
        return this.request('/admin/reset', {
            method: 'POST',
        });
    }
}

export const api = new ApiClient(API_BASE_URL);
