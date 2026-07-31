/**
 * API Client for CUIN v2 Backend
 * 
 * Centralized HTTP client for all backend API calls.
 */

/**
 * `http://localhost:8000` only resolves to the backend when the
 * BROWSER itself is running on the same machine as the backend. Any
 * other viewer -- someone on the LAN hitting this app by IP/hostname,
 * a real deployment behind a domain -- has "localhost" resolve to
 * THEIR OWN machine instead, so every API call fails silently (caught,
 * console.error'd, UI stuck on a loading state forever, with no
 * visible error). Deriving the API host from wherever the page itself
 * was loaded from fixes this for every access pattern without any
 * per-environment config. NEXT_PUBLIC_API_URL (baked in at build
 * time) is kept as an explicit override for setups where the API
 * genuinely lives elsewhere (e.g. reverse-proxied to a different
 * host/port in production).
 */
function resolveApiBaseUrl(): string {
    if (process.env.NEXT_PUBLIC_API_URL) return process.env.NEXT_PUBLIC_API_URL;
    if (typeof window !== 'undefined') return `${window.location.protocol}//${window.location.hostname}:8000`;
    return 'http://localhost:8000';
}

const API_BASE_URL = resolveApiBaseUrl();

class ApiClient {
    private baseUrl: string;

    constructor(baseUrl: string) {
        this.baseUrl = baseUrl;
    }

    // Defaulting T to `any` (not leaving it to infer as `unknown`) matches
    // how every call site in this file already uses it -- none pass an
    // explicit type argument, and callers destructure/assign the result
    // directly into typed state. Without this default, `next build`'s
    // full TypeScript pass (which `next dev` does not run) fails on every
    // single page that does so.
    private async request<T = any>(endpoint: string, options: RequestInit = {}): Promise<T> {
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

    async startDatasourcePipeline(mode: string, engine: string = 'duckdb') {
        // The backend route is /datasource/demo -- /datasource/pipeline/start
        // does not exist and was 404ing every "Start New Run" click on
        // /pipeline (the /datasource page worked because it called the
        // correct path directly via a raw fetch instead of this client).
        return this.request('/datasource/demo', {
            method: 'POST',
            body: JSON.stringify({ mode, engine }),
        });
    }

    // Config (legacy -- superseded by the rules API below, kept for
    // /settings' existing blocking/weight sliders)
    async getConfig() {
        return this.request('/config');
    }

    async updateConfig(config: any) {
        return this.request('/config', {
            method: 'POST',
            body: JSON.stringify(config),
        });
    }

    // Rules (typed blocking-rule catalog + scoring thresholds, versioned)
    async getActiveRules() {
        return this.request('/rules');
    }

    async getDefaultRules() {
        return this.request('/rules/defaults');
    }

    async saveRules(
        blockingRules: any[], matchRuleset: any, createdBy: string = 'ui', activate: boolean = true,
        segmentation?: any, matchRulesetsBySegment?: Record<string, any>,
    ) {
        return this.request('/rules', {
            method: 'POST',
            body: JSON.stringify({
                blocking_rules: blockingRules, match_ruleset: matchRuleset, created_by: createdBy, activate,
                segmentation, match_rulesets_by_segment: matchRulesetsBySegment,
            }),
        });
    }

    // Stage 5 -- Company/Individual split + traceable cross-segment connections.
    async getSegmentStats(runId: string) {
        return this.request(`/rules/runs/${runId}/segments`);
    }

    async listRelationships(params: { runId?: string; customerCode?: string; limit?: number; offset?: number }) {
        const q = new URLSearchParams();
        if (params.runId) q.set('run_id', params.runId);
        if (params.customerCode) q.set('customer_code', params.customerCode);
        q.set('limit', String(params.limit ?? 50));
        q.set('offset', String(params.offset ?? 0));
        return this.request(`/rules/relationships?${q.toString()}`);
    }

    async listRuleVersions(limit: number = 50) {
        return this.request(`/rules/versions?limit=${limit}`);
    }

    async activateRuleVersion(policyVersion: number, approvedBy: string = 'ui') {
        return this.request(`/rules/versions/${policyVersion}/activate`, {
            method: 'POST',
            body: JSON.stringify({ approved_by: approvedBy }),
        });
    }

    async precheckRules(runId: string, blockingRules: any[], warnPairs?: number, blockPairs?: number) {
        return this.request('/rules/precheck', {
            method: 'POST',
            body: JSON.stringify({ run_id: runId, blocking_rules: blockingRules, warn_pairs: warnPairs, block_pairs: blockPairs }),
        });
    }

    async redecide(runId: string, matchRuleset: any) {
        return this.request(`/rules/runs/${runId}/redecide`, {
            method: 'POST',
            body: JSON.stringify({ match_ruleset: matchRuleset }),
        });
    }

    async reblock(runId: string, blockingRules: any[], matchRuleset?: any) {
        return this.request(`/rules/runs/${runId}/reblock`, {
            method: 'POST',
            body: JSON.stringify({ blocking_rules: blockingRules, match_ruleset: matchRuleset }),
        });
    }

    async getComparators() {
        return this.request('/rules/comparators');
    }

    async getDatasourceSchema() {
        return this.request('/datasource/schema');
    }

    // Search
    async search(q: string, opts?: { fields?: string; runId?: string; page?: number; pageSize?: number }) {
        const params = new URLSearchParams({ q });
        if (opts?.fields) params.append('fields', opts.fields);
        if (opts?.runId) params.append('run_id', opts.runId);
        if (opts?.page) params.append('page', String(opts.page));
        if (opts?.pageSize) params.append('page_size', String(opts.pageSize));
        return this.request(`/search?${params.toString()}`);
    }

    // Review
    async getReviewQueue(page: number = 1, pageSize: number = 20, status?: string) {
        const params = new URLSearchParams({
            page: page.toString(),
            page_size: pageSize.toString(),
        });
        if (status) {
            params.append('status', status);
        }
        return this.request(`/review/queue?${params.toString()}`);
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
        return this.request(`/review/${pairId}/explanation`);
    }

    // Graph
    async getGraphData(runId?: string, limit: number = 1000) {
        const params = new URLSearchParams({ limit: limit.toString() });
        if (runId) {
            params.append('run_id', runId);
        }
        return this.request(`/graph/data?${params.toString()}`);
    }

    async previewClustering(runId: string | undefined, scoring: any) {
        // PreviewRequest (backend/api/routes_graph.py) is {run_id, scoring}
        // as the WHOLE body -- run_id is not a query param, and `scoring`
        // is a required field, not the top-level payload. Sending the
        // config object directly as the body (the old behavior) 422'd
        // every "Run Preview" click.
        return this.request('/graph/preview', {
            method: 'POST',
            body: JSON.stringify({ run_id: runId, scoring }),
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
    async getMatchScores(runId: string, page: number = 1, pageSize: number = 10, minScore?: number, decision?: string) {
        const params = new URLSearchParams({
            run_id: runId,
            page: page.toString(),
            page_size: pageSize.toString(),
        });
        if (minScore !== undefined) {
            params.append('min_score', minScore.toString());
        }
        if (decision) {
            params.append('decision', decision);
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
        // Backend route is /audit/compliance/report, not /compliance-report.
        return this.request('/audit/compliance/report');
    }

    async verifyAuditChain() {
        // Backend route is /audit/verify, not /verify-chain.
        return this.request('/audit/verify');
    }

    // Admin
    async resetAllData() {
        return this.request('/admin/reset', {
            method: 'POST',
        });
    }

    // ------------------------------------------------------------------
    // Entity resolution workbench (Stage 4/5 of the workbench plan) --
    // purely additive, does not touch /graph or /explorer's methods above.
    // ------------------------------------------------------------------

    async wbListRuns(page: number = 1, pageSize: number = 20) {
        return this.request(`/workbench/runs?page=${page}&page_size=${pageSize}`);
    }

    async wbPopulations(runId?: string) {
        const q = runId ? `?run_id=${runId}` : '';
        return this.request(`/workbench/populations${q}`);
    }

    async wbListPairs(params: { runId?: string; decision?: string; minConf?: number; maxConf?: number; hasVeto?: boolean; q?: string; recordType?: string; page?: number; pageSize?: number }) {
        const q = new URLSearchParams();
        if (params.runId) q.set('run_id', params.runId);
        if (params.decision) q.set('decision', params.decision);
        if (params.minConf !== undefined) q.set('min_conf', String(params.minConf));
        if (params.maxConf !== undefined) q.set('max_conf', String(params.maxConf));
        if (params.hasVeto !== undefined) q.set('has_veto', String(params.hasVeto));
        if (params.q) q.set('q', params.q);
        if (params.recordType && params.recordType !== 'ALL') q.set('record_type', params.recordType);
        q.set('page', String(params.page ?? 1));
        q.set('page_size', String(params.pageSize ?? 20));
        return this.request(`/workbench/pairs?${q.toString()}`);
    }

    async wbPairBreakdown(aKey: string, bKey: string, runId?: string) {
        const q = runId ? `?run_id=${runId}` : '';
        return this.request(`/workbench/pairs/${encodeURIComponent(aKey)}/${encodeURIComponent(bKey)}/breakdown${q}`);
    }

    async wbGetRecord(customerCode: string, runId?: string) {
        const q = runId ? `?run_id=${runId}` : '';
        return this.request(`/workbench/records/${encodeURIComponent(customerCode)}${q}`);
    }

    async wbSearch(q: string, runId?: string, page: number = 1, pageSize: number = 20) {
        const params = new URLSearchParams({ q, page: String(page), page_size: String(pageSize) });
        if (runId) params.set('run_id', runId);
        return this.request(`/workbench/search?${params.toString()}`);
    }

    async wbListEntities(params: { page?: number; pageSize?: number; hasGlobalRef?: boolean; q?: string; recordType?: string; runId?: string }) {
        const q = new URLSearchParams();
        q.set('page', String(params.page ?? 1));
        q.set('page_size', String(params.pageSize ?? 20));
        if (params.hasGlobalRef !== undefined) q.set('has_global_ref', String(params.hasGlobalRef));
        if (params.q) q.set('q', params.q);
        if (params.recordType && params.recordType !== 'ALL') q.set('record_type', params.recordType);
        if (params.runId) q.set('run_id', params.runId);
        return this.request(`/workbench/entities?${q.toString()}`);
    }

    async wbGetEntity(entityId: string, runId?: string) {
        const q = runId ? `?run_id=${runId}` : '';
        return this.request(`/workbench/entities/${entityId}${q}`);
    }

    async wbEntityMatches(entityId: string, runId?: string) {
        const q = runId ? `?run_id=${runId}` : '';
        return this.request(`/workbench/entities/${entityId}/matches${q}`);
    }

    async wbApprove(runId: string | undefined, aCode: string, bCode: string, reasonCode: string, reason: string, actor: string) {
        return this.request('/workbench/actions/approve', {
            method: 'POST',
            body: JSON.stringify({ run_id: runId, a_code: aCode, b_code: bCode, reason_code: reasonCode, reason, actor }),
        });
    }

    async wbReject(runId: string | undefined, aCode: string, bCode: string, reasonCode: string, reason: string, actor: string) {
        return this.request('/workbench/actions/reject', {
            method: 'POST',
            body: JSON.stringify({ run_id: runId, a_code: aCode, b_code: bCode, reason_code: reasonCode, reason, actor }),
        });
    }

    async wbMerge(runId: string | undefined, entityIdA: string, entityIdB: string, reasonCode: string, reason: string, actor: string) {
        return this.request('/workbench/actions/merge', {
            method: 'POST',
            body: JSON.stringify({ run_id: runId, entity_id_a: entityIdA, entity_id_b: entityIdB, reason_code: reasonCode, reason, actor }),
        });
    }

    async wbSplit(runId: string | undefined, entityId: string, customerCode: string, reasonCode: string, reason: string, actor: string) {
        return this.request('/workbench/actions/split', {
            method: 'POST',
            body: JSON.stringify({ run_id: runId, entity_id: entityId, customer_code: customerCode, reason_code: reasonCode, reason, actor }),
        });
    }

    async wbAssignGlobalRef(entityId: string, globalRef: string, state: string, reason: string, actor: string) {
        return this.request(`/workbench/entities/${entityId}/global-ref`, {
            method: 'POST',
            body: JSON.stringify({ global_ref: globalRef, state, reason, actor }),
        });
    }

    async wbRetireGlobalRef(entityId: string, reason: string, actor: string) {
        return this.request(`/workbench/entities/${entityId}/global-ref`, {
            method: 'DELETE',
            body: JSON.stringify({ reason, actor }),
        });
    }

    async wbAuditVerify() {
        return this.request('/workbench/audit/verify');
    }

    async wbListAudit(entityId?: string, page: number = 1, pageSize: number = 50) {
        const q = new URLSearchParams({ page: String(page), page_size: String(pageSize) });
        if (entityId) q.set('entity_id', entityId);
        return this.request(`/workbench/audit?${q.toString()}`);
    }

    async wbListOverrides(params: { page?: number; pageSize?: number; verdict?: string; runId?: string }) {
        const q = new URLSearchParams();
        q.set('page', String(params.page ?? 1));
        q.set('page_size', String(params.pageSize ?? 50));
        if (params.verdict) q.set('verdict', params.verdict);
        if (params.runId) q.set('run_id', params.runId);
        return this.request(`/workbench/overrides?${q.toString()}`);
    }
}

export const api = new ApiClient(API_BASE_URL);
