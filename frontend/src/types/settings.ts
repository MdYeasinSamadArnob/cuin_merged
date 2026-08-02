// Mirrors backend/engine/rules/catalog.py + scoring_rules.py + precheck.py field-for-field.

export type BlockingRuleType =
    | "exact_identifier"
    | "composite_key"
    | "token_key"
    | "prefix_key"
    | "date_part_key"
    | "raw_column";

export interface RuleGuards {
    max_key_frequency: number | null;
    max_block_size: number | null;
    min_key_parts: number | null;
}

export interface BlockingRule {
    rule_id: string;
    type: BlockingRuleType;
    enabled: boolean;
    order: number;
    label: string;
    fields: string[];
    params: Record<string, any>;
    guards: RuleGuards;
    description: string;
}

export interface ScoringRules {
    strong_identifier_types: string[];
    medium_name_jaccard_threshold: number;
    medium_dob_requires_precision: string;
    veto_document_type_mismatch: boolean;
    veto_dob_full_mismatch: boolean;
    auto_link_min_strong_only: number;
    auto_link_min_strong_with_name: number;
    review_min_strong: number;
    review_requires_medium_name_and_dob: boolean;
    name_alone_gates_nothing: boolean;
    max_cluster_size: number;
    min_density: number;
}

// Confidence-based scoring (Stage 2) -- what a real run's decision
// logic actually reads. Mirrors backend/engine/rules/match_rules.py.
export interface MatchRule {
    rule_id: string;
    attribute: string; // "DOCUMENT" | "MOBILE" | "EMAIL" | "FULL_ADDRESS" | "NAME" | "BIRTH_DATE" | "RAW_COLUMN"
    sub_type: string | null;
    comparator: string;
    params: Record<string, any>; // RAW_COLUMN: {column, is_array, ...comparator params}
    confidence_pct: number; // 0-100
    aggregation: "once" | "per_sub_type";
    veto_kind: "both_present_no_overlap" | "both_qualified_and_differ" | null;
    enabled: boolean;
    label: string;
}

export interface MatchRuleset {
    match_rules: MatchRule[];
    auto_link_min_confidence: number;
    review_min_confidence: number;
    confidence_cap: number;
    max_cluster_size: number;
    min_density: number;
}

// Stage 5.1 -- "give any field a weight, dynamically". A bank-added
// match rule on any schema column, not one of the 6 built-ins.
export const ATTRIBUTE_RAW_COLUMN = "RAW_COLUMN";

// Array/set-valued comparators -- only these support the "share at
// least one value" veto; scalar comparators use the "clearly differ"
// veto instead. Mirrors backend/engine/scoring/confidence.py's
// _ARRAY_COMPARATORS exactly -- keep in sync.
const ARRAY_COMPARATORS = new Set(["set_intersect", "token_jaccard", "token_containment"]);

/** Which veto_kind a "hard-reject on a clear mismatch" toggle should set, given the rule's comparator. */
export function vetoKindForComparator(comparatorId: string): "both_present_no_overlap" | "both_qualified_and_differ" {
    return ARRAY_COMPARATORS.has(comparatorId) ? "both_present_no_overlap" : "both_qualified_and_differ";
}

/** A fresh, inert (0% weight) match rule for a schema field a bank wants to start scoring on. */
export function defaultMatchRuleForField(field: { name: string; semantic_label: string; is_array: boolean; default_comparators: string[] }): MatchRule {
    const comparator = field.default_comparators[0] || "exact";
    return {
        rule_id: `match_${Date.now().toString(36)}`,
        attribute: ATTRIBUTE_RAW_COLUMN,
        sub_type: null,
        comparator,
        params: { column: field.name, is_array: field.is_array },
        confidence_pct: 0,
        aggregation: "once",
        veto_kind: null,
        enabled: true,
        label: field.semantic_label,
    };
}

// Stage 5 -- Company/Individual split. Segmentation only gates identity
// MERGING: a cross-segment pair still shows up as a traceable
// "relationship" (below), never silently dropped. Mirrors
// backend/engine/segments/classifier.py's SegmentationConfig.
export interface SegmentationConfig {
    enabled: boolean;
    mode: "name_patterns" | "column_map";
    company_keywords: string[];
    column_map: Record<string, any> | null;
}

export interface RuleCatalog {
    policy_version: number;
    blocking_rules: BlockingRule[];
    match_ruleset: MatchRuleset;
    segmentation: SegmentationConfig;
    match_rulesets_by_segment: Record<string, MatchRuleset>;
    is_active: boolean;
    created_by: string;
    created_at: string;
    approved_by: string | null;
    catalog_hash: string;
}

export interface SegmentCounts {
    run_id: string;
    engine: string;
    segment_counts: Record<string, number>;
    total: number;
}

// A traceable, non-merging connection between two records in different
// segments (e.g. a person and a company they share a phone/document
// with) -- see backend/db/migrations/004_entity_relationships.sql.
export interface EntityRelationshipParty {
    customer_code: string;
    name: string | null;
    segment: string;
}

export interface EntityRelationship {
    relationship_id: string;
    run_id: string;
    a: EntityRelationshipParty;
    b: EntityRelationshipParty;
    shared_evidence: { field: string; value: string }[];
    created_at: string;
}

export interface HeavyKey {
    key: string;
    n_records: number;
    n_pairs: number;
}

export type FanoutSeverity = "SAFE" | "WARN" | "BLOCK";

export interface FanoutEstimate {
    rule_id: string;
    n_keys: number;
    n_pairs: number;
    largest_block: number;
    heaviest_keys: HeavyKey[];
    severity: FanoutSeverity;
    suggested_max_block_size: number | null;
}

export interface DecisionCounts {
    AUTO_LINK?: number;
    REVIEW?: number;
    REJECT?: number;
}

export const RULE_TYPE_LABELS: Record<BlockingRuleType, string> = {
    exact_identifier: "Exact identifier match",
    composite_key: "Name + date of birth",
    token_key: "Shares a name word",
    prefix_key: "Starts the same",
    date_part_key: "Same date part",
    raw_column: "Field match",
};

export function defaultRuleForType(type: BlockingRuleType, order: number): BlockingRule {
    const base = {
        rule_id: `rule_${Date.now().toString(36)}`,
        type,
        enabled: true,
        order,
        label: RULE_TYPE_LABELS[type],
        params: {} as Record<string, any>,
        guards: { max_key_frequency: null, max_block_size: null, min_key_parts: null } as RuleGuards,
        description: "",
    };
    switch (type) {
        case "exact_identifier":
            return { ...base, fields: ["mobile"] };
        case "composite_key":
            return { ...base, fields: ["name_key", "dob_iso"], params: { require_dob_precision: "FULL" }, guards: { ...base.guards, min_key_parts: 2 } };
        case "token_key":
            return { ...base, fields: ["name_tokens"] };
        case "prefix_key":
            return { ...base, fields: ["name_norm"], params: { prefix_len: 4 } };
        case "date_part_key":
            return { ...base, fields: ["dob_iso", "year"] };
        case "raw_column":
            return { ...base, fields: ["BRANCH_CODE"], params: { is_array: false } };
    }
}

/** Which BlockingRuleType a discovered source field should use, given its profile from GET /datasource/schema. */
export function ruleTypeForField(field: { name: string; is_array: boolean; semantic_type: string }): BlockingRuleType {
    // Pre-normalized identifier types already have a validated,
    // deduplicated path via `identifiers` -- prefer it over a raw
    // exact-match self-join, which skips validation entirely.
    const idTypeByColumn: Record<string, string> = { MOBILE: "mobile", EMAIL: "email", DOCUMENT: "document" };
    if (idTypeByColumn[field.name]) return "exact_identifier";
    return "raw_column";
}

export function defaultRuleForField(field: { name: string; is_array: boolean; semantic_type: string; semantic_label: string }, order: number): BlockingRule {
    const type = ruleTypeForField(field);
    if (type === "exact_identifier") {
        const idTypeByColumn: Record<string, string> = { MOBILE: "mobile", EMAIL: "email", DOCUMENT: "document" };
        return {
            rule_id: `rule_${Date.now().toString(36)}`,
            type, enabled: true, order,
            label: `Same ${field.semantic_label.toLowerCase()}`,
            fields: [idTypeByColumn[field.name]],
            params: {},
            guards: { max_key_frequency: null, max_block_size: null, min_key_parts: null },
            description: "",
        };
    }
    return {
        rule_id: `rule_${Date.now().toString(36)}`,
        type: "raw_column", enabled: true, order,
        label: `Same ${field.semantic_label.toLowerCase()} (${field.name})`,
        fields: [field.name],
        params: { is_array: field.is_array },
        guards: { max_key_frequency: null, max_block_size: null, min_key_parts: null },
        description: "",
    };
}
