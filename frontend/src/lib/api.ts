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

export const API_BASE_URL = resolveApiBaseUrl();

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

    async deleteRun(runId: string) {
        return this.request(`/runs/${runId}`, { method: 'DELETE' });
    }

    async startDatasourcePipeline(mode: string) {
        // The backend route is /datasource/demo -- /datasource/pipeline/start
        // does not exist and was 404ing every "Start New Run" click on
        // /pipeline (the /datasource page worked because it called the
        // correct path directly via a raw fetch instead of this client).
        return this.request('/datasource/demo', {
            method: 'POST',
            body: JSON.stringify({ mode }),
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

    // Same as wbAssignGlobalRef, but for a record with no entity yet (a
    // singleton -- never clustered, so it never earned one automatically).
    // Mints a one-member entity on demand server-side.
    async wbAssignGlobalRefToRecord(customerCode: string, runId: string | undefined, globalRef: string, state: string, reason: string, actor: string) {
        return this.request(`/workbench/records/${encodeURIComponent(customerCode)}/global-ref`, {
            method: 'POST',
            body: JSON.stringify({ run_id: runId, global_ref: globalRef, state, reason, actor }),
        });
    }

    // Rollback -- undo a merge, revert a Global ID change, or revoke an
    // approve/reject override. See backend/services/workbench_service.py's
    // "Rollback" section: each of these is a NEW forward audit event that
    // reverses a prior one, never a mutation of the original.
    async wbUndoMerge(entityId: string, reason: string, actor: string) {
        return this.request(`/workbench/entities/${entityId}/undo-merge`, {
            method: 'POST',
            body: JSON.stringify({ reason, actor }),
        });
    }

    async wbRevertGlobalRef(entityId: string, reason: string, actor: string) {
        return this.request(`/workbench/entities/${entityId}/revert-global-ref`, {
            method: 'POST',
            body: JSON.stringify({ reason, actor }),
        });
    }

    async wbRevokeOverride(overrideId: string, reason: string, actor: string) {
        return this.request(`/workbench/overrides/${overrideId}/revoke`, {
            method: 'POST',
            body: JSON.stringify({ reason, actor }),
        });
    }

    async wbAuditVerify() {
        return this.request('/workbench/audit/verify');
    }

    async wbListOverrides(params: { page?: number; pageSize?: number; verdict?: string; runId?: string }) {
        const q = new URLSearchParams();
        q.set('page', String(params.page ?? 1));
        q.set('page_size', String(params.pageSize ?? 50));
        if (params.verdict) q.set('verdict', params.verdict);
        if (params.runId) q.set('run_id', params.runId);
        return this.request(`/workbench/overrides?${q.toString()}`);
    }

    // ------------------------------------------------------------------
    // Identity Graph 360 v2 -- purely additive, does not touch /graph's
    // legacy methods above (getGraphData, getClusterEntities, etc.).
    // ------------------------------------------------------------------

    async graphOverview(params: {
        runId?: string; page?: number; pageSize?: number; sort?: 'size_desc' | 'size_asc';
        minSize?: number; maxSize?: number; recordType?: string; hasGlobalRef?: boolean; q?: string;
    }) {
        const p = new URLSearchParams();
        if (params.runId) p.set('run_id', params.runId);
        p.set('page', String(params.page ?? 1));
        p.set('page_size', String(params.pageSize ?? 100));
        p.set('sort', params.sort ?? 'size_desc');
        if (params.minSize !== undefined) p.set('min_size', String(params.minSize));
        if (params.maxSize !== undefined) p.set('max_size', String(params.maxSize));
        if (params.recordType && params.recordType !== 'ALL') p.set('record_type', params.recordType);
        if (params.hasGlobalRef !== undefined) p.set('has_global_ref', String(params.hasGlobalRef));
        if (params.q) p.set('q', params.q);
        return this.request(`/graph/v2/overview?${p.toString()}`);
    }

    async graphStats(runId?: string) {
        const q = runId ? `?run_id=${runId}` : '';
        return this.request(`/graph/v2/stats${q}`);
    }

    async graphCluster(entityId: string, runId?: string) {
        const q = runId ? `?run_id=${runId}` : '';
        return this.request(`/graph/v2/cluster/${entityId}${q}`);
    }

    async graphHops(params: { customerCode?: string; entityId?: string; runId?: string; hops?: number; maxNodes?: number }) {
        const p = new URLSearchParams();
        if (params.customerCode) p.set('customer_code', params.customerCode);
        if (params.entityId) p.set('entity_id', params.entityId);
        if (params.runId) p.set('run_id', params.runId);
        p.set('hops', String(params.hops ?? 2));
        p.set('max_nodes', String(params.maxNodes ?? 300));
        return this.request(`/graph/v2/hops?${p.toString()}`);
    }

    async graphCanvas(params: {
        runId?: string; page?: number; pageSize?: number; sort?: 'size_desc' | 'size_asc';
        minSize?: number; maxSize?: number; recordType?: string; hasGlobalRef?: boolean; q?: string;
    }) {
        const p = new URLSearchParams();
        if (params.runId) p.set('run_id', params.runId);
        p.set('page', String(params.page ?? 1));
        p.set('page_size', String(params.pageSize ?? 20));
        p.set('sort', params.sort ?? 'size_desc');
        if (params.minSize !== undefined) p.set('min_size', String(params.minSize));
        if (params.maxSize !== undefined) p.set('max_size', String(params.maxSize));
        if (params.recordType && params.recordType !== 'ALL') p.set('record_type', params.recordType);
        if (params.hasGlobalRef !== undefined) p.set('has_global_ref', String(params.hasGlobalRef));
        if (params.q) p.set('q', params.q);
        return this.request(`/graph/v2/canvas?${p.toString()}`);
    }

    async graphSingletons(params: { runId?: string; page?: number; pageSize?: number; recordType?: string; q?: string }) {
        const p = new URLSearchParams();
        if (params.runId) p.set('run_id', params.runId);
        p.set('page', String(params.page ?? 1));
        p.set('page_size', String(params.pageSize ?? 60));
        if (params.recordType && params.recordType !== 'ALL') p.set('record_type', params.recordType);
        if (params.q) p.set('q', params.q);
        return this.request(`/graph/v2/singletons?${p.toString()}`);
    }

    async graphClusterBridges(entityId: string, runId?: string) {
        const q = runId ? `?run_id=${runId}` : '';
        return this.request(`/graph/v2/cluster/${entityId}/bridges${q}`);
    }

    async graphBridges(params: { runId?: string; page?: number; pageSize?: number; minConfidence?: number }) {
        const p = new URLSearchParams();
        if (params.runId) p.set('run_id', params.runId);
        p.set('page', String(params.page ?? 1));
        p.set('page_size', String(params.pageSize ?? 30));
        if (params.minConfidence !== undefined) p.set('min_confidence', String(params.minConfidence));
        return this.request(`/graph/v2/bridges?${p.toString()}`);
    }

    // -- Public Identity Recognition API (/api/v1) auth --
    // Hits the INTERNAL admin router (/admin/api-token), not the public
    // sub-app itself. Auth for /api/v1 is a single global bearer token
    // (settings.PUBLIC_API_BEARER_TOKEN) -- this just reads back its
    // current effective value so the Getting Started panel can show it.
    async getApiToken() {
        return this.request('/admin/api-token');
    }

    // -- Data Viewer: browse the raw, un-normalized source Parquet
    // file directly (before running any pipeline), backed by a
    // materialized Doris table with inverted-index search. See
    // backend/engine/ports/doris_raw_preview.py.
    async getDataViewerStatus() {
        return this.request('/data-viewer/status');
    }

    async refreshDataViewer() {
        return this.request('/data-viewer/refresh', { method: 'POST' });
    }

    async getDataViewerRows(params: {
        page: number;
        pageSize: number;
        q?: string;
        filters?: Array<{ column: string; op: string; value?: string }>;
        sortCol?: string;
        sortDir?: 'asc' | 'desc';
    }) {
        const p = new URLSearchParams();
        p.set('page', String(params.page));
        p.set('page_size', String(params.pageSize));
        if (params.q) p.set('q', params.q);
        if (params.filters && params.filters.length > 0) p.set('filters', JSON.stringify(params.filters));
        if (params.sortCol) p.set('sort_col', params.sortCol);
        if (params.sortDir) p.set('sort_dir', params.sortDir);
        return this.request(`/data-viewer/rows?${p.toString()}`);
    }
}

export const api = new ApiClient(API_BASE_URL);
