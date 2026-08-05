"use client";

// Raw source Data Viewer -- lets a bank officer browse the raw,
// un-normalized source Parquet file directly, before running any
// pipeline. Backed by a materialized Doris table with real
// inverted-index full-text search (not just LIKE) plus a per-column
// advanced filter builder. See backend/engine/ports/doris_raw_preview.py
// for the Doris-side mechanics and api/routes_data_viewer.py for the
// API surface this page talks to.

import { useEffect, useState } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { motion, AnimatePresence } from "framer-motion";
import {
    Search as SearchIcon, RefreshCw, Database, Filter, X, Plus, ChevronDown,
    Eye, AlertCircle, Clock,
} from "lucide-react";
import { api } from "@/lib/api";

function useDebounced<T>(value: T, delayMs: number): T {
    const [debounced, setDebounced] = useState(value);
    useEffect(() => {
        const t = setTimeout(() => setDebounced(value), delayMs);
        return () => clearTimeout(t);
    }, [value, delayMs]);
    return debounced;
}

interface ColumnInfo {
    name: string;
    doris_type: string;
    is_array: boolean;
    searchable: boolean;
    filterable: boolean;
}

interface FilterRow {
    id: string;
    column: string;
    op: string;
    value: string;
}

const OPERATORS_SCALAR = [
    { value: "contains", label: "contains" },
    { value: "eq", label: "equals" },
    { value: "neq", label: "not equals" },
    { value: "starts_with", label: "starts with" },
    { value: "gt", label: "greater than" },
    { value: "gte", label: "greater or equal" },
    { value: "lt", label: "less than" },
    { value: "lte", label: "less or equal" },
    { value: "is_null", label: "is empty" },
    { value: "is_not_null", label: "is not empty" },
];
const OPERATORS_ARRAY = [
    { value: "array_contains", label: "contains exactly" },
];

export default function DataViewerPage() {
    const queryClient = useQueryClient();

    const { data: status, isLoading: statusLoading } = useQuery({
        queryKey: ["data-viewer-status"],
        queryFn: () => api.getDataViewerStatus(),
    });

    const refreshMutation = useMutation({
        mutationFn: () => api.refreshDataViewer(),
        onSuccess: () => {
            queryClient.invalidateQueries({ queryKey: ["data-viewer-status"] });
            queryClient.invalidateQueries({ queryKey: ["data-viewer-rows"] });
        },
    });

    const [page, setPage] = useState(1);
    const [pageSize, setPageSize] = useState(50);
    const [quickSearch, setQuickSearch] = useState("");
    const debouncedSearch = useDebounced(quickSearch, 400);
    const [filters, setFilters] = useState<FilterRow[]>([]);
    const [appliedFilters, setAppliedFilters] = useState<FilterRow[]>([]);
    const [showFilters, setShowFilters] = useState(false);
    const [sortCol, setSortCol] = useState<string | null>(null);
    const [sortDir, setSortDir] = useState<"asc" | "desc">("asc");
    const [selectedRow, setSelectedRow] = useState<Record<string, unknown> | null>(null);

    const columns: ColumnInfo[] = status?.columns || [];

    const { data: rowsData, isLoading: rowsLoading, isFetching: rowsFetching, error: rowsError } = useQuery({
        queryKey: ["data-viewer-rows", page, pageSize, debouncedSearch, appliedFilters, sortCol, sortDir],
        queryFn: () => api.getDataViewerRows({
            page, pageSize,
            q: debouncedSearch || undefined,
            filters: appliedFilters.filter((f) => f.column && f.op).map((f) => ({ column: f.column, op: f.op, value: f.value })),
            sortCol: sortCol || undefined,
            sortDir,
        }),
        enabled: !!status?.materialized,
        placeholderData: (prev) => prev,
    });

    const totalPages = rowsData ? Math.max(1, Math.ceil(rowsData.total / pageSize)) : 1;

    const addFilterRow = () => {
        const firstCol = columns[0];
        setFilters((prev) => [...prev, {
            id: `f_${Date.now()}`,
            column: firstCol?.name || "",
            op: firstCol?.is_array ? "array_contains" : "contains",
            value: "",
        }]);
        setShowFilters(true);
    };
    const updateFilterRow = (id: string, patch: Partial<FilterRow>) => {
        setFilters((prev) => prev.map((f) => (f.id === id ? { ...f, ...patch } : f)));
    };
    const removeFilterRow = (id: string) => {
        setFilters((prev) => prev.filter((f) => f.id !== id));
    };
    const applyFilters = () => { setAppliedFilters(filters); setPage(1); };
    const clearFilters = () => { setFilters([]); setAppliedFilters([]); setPage(1); };
    const toggleSort = (col: ColumnInfo) => {
        // Doris rejects ORDER BY on an ARRAY<TEXT> column outright --
        // never send that request in the first place.
        if (col.is_array) return;
        if (sortCol === col.name) setSortDir((d) => (d === "asc" ? "desc" : "asc"));
        else { setSortCol(col.name); setSortDir("asc"); }
        setPage(1);
    };

    const formatCell = (col: ColumnInfo, value: unknown) => {
        if (value === null || value === undefined || value === "") {
            return <span className="text-gray-400 dark:text-gray-500">—</span>;
        }
        if (col.is_array) {
            const arr = value as unknown[];
            if (arr.length === 0) return <span className="text-gray-400 dark:text-gray-500">—</span>;
            return (
                <div className="flex flex-wrap gap-1 max-w-[220px]">
                    {arr.slice(0, 2).map((v, i) => (
                        <span key={i} className="px-1.5 py-0.5 rounded bg-gray-100 dark:bg-gray-800 text-[11px] truncate max-w-[160px]">
                            {String(v)}
                        </span>
                    ))}
                    {arr.length > 2 && <span className="text-[11px] text-gray-400 self-center">+{arr.length - 2} more</span>}
                </div>
            );
        }
        const str = String(value);
        return <span className="truncate block max-w-[220px]" title={str}>{str}</span>;
    };

    return (
        <div className="space-y-6">
            <div>
                <h1 className="text-2xl font-bold text-gray-900 dark:text-white">Data Viewer</h1>
                <p className="text-sm text-gray-500 dark:text-gray-400 mt-1">
                    Browse the raw source data exactly as the pipeline will read it -- every column,
                    before you commit to running anything.
                </p>
            </div>

            <motion.div initial={{ opacity: 0, y: 10 }} animate={{ opacity: 1, y: 0 }} className="glass-card p-4 flex items-center justify-between flex-wrap gap-3">
                <div className="flex items-center gap-2 text-sm">
                    <Database size={16} className="text-blue-500 shrink-0" />
                    <span className="text-gray-600 dark:text-gray-300">
                        {statusLoading ? (
                            "Checking..."
                        ) : status?.materialized ? (
                            <>
                                <strong className="text-gray-900 dark:text-white">{status.row_count.toLocaleString()}</strong> records,{" "}
                                <strong className="text-gray-900 dark:text-white">{columns.length}</strong> columns loaded from{" "}
                                <code className="text-xs bg-gray-100 dark:bg-gray-800 px-1 py-0.5 rounded">data_source/oracle_data.parquet</code>
                            </>
                        ) : (
                            "Raw source data hasn't been loaded into the viewer yet"
                        )}
                    </span>
                </div>
                <button
                    onClick={() => refreshMutation.mutate()}
                    disabled={refreshMutation.isPending}
                    className="btn btn-primary gap-2 !py-1.5 !px-3 text-xs disabled:opacity-40 shrink-0"
                    title="Re-reads the current source file -- do this after replacing it with a new upload"
                >
                    <RefreshCw size={14} className={refreshMutation.isPending ? "animate-spin" : ""} />
                    {status?.materialized ? "Refresh from source" : "Load source data"}
                </button>
            </motion.div>

            {refreshMutation.isSuccess && refreshMutation.data && (
                <div className="text-xs text-emerald-600 dark:text-emerald-400 -mt-3 flex items-center gap-1.5">
                    <Clock size={12} />
                    Loaded {refreshMutation.data.row_count.toLocaleString()} rows just now, indexed{" "}
                    {refreshMutation.data.indexed_columns?.length || 0} of {columns.length} columns for search.
                    {refreshMutation.data.failed_columns?.length > 0 && (
                        <span className="text-gray-400"> ({refreshMutation.data.failed_columns.join(", ")} not indexable, still filterable.)</span>
                    )}
                </div>
            )}
            {refreshMutation.isError && (
                <div className="text-xs text-red-500 -mt-3 flex items-center gap-1">
                    <AlertCircle size={12} /> Failed to load source data: {(refreshMutation.error as Error).message}
                </div>
            )}

            {!statusLoading && !status?.materialized && (
                <motion.div initial={{ opacity: 0 }} animate={{ opacity: 1 }} className="glass-card p-10 text-center">
                    <Database size={32} className="mx-auto text-gray-400 dark:text-gray-500 mb-3" />
                    <p className="text-gray-500 dark:text-gray-400 text-sm mb-4">
                        Load the raw source file to start browsing -- this reads it in place, no copy kept
                        elsewhere, and only takes a couple of seconds even at millions of rows.
                    </p>
                    <button onClick={() => refreshMutation.mutate()} disabled={refreshMutation.isPending} className="btn btn-primary gap-2 mx-auto">
                        <RefreshCw size={16} className={refreshMutation.isPending ? "animate-spin" : ""} />
                        Load source data
                    </button>
                </motion.div>
            )}

            {status?.materialized && (
                <>
                    <motion.div initial={{ opacity: 0, y: 10 }} animate={{ opacity: 1, y: 0 }} transition={{ delay: 0.03 }} className="glass-card p-4 space-y-3">
                        <div className="flex items-center gap-2">
                            <div className="relative flex-1">
                                <SearchIcon size={16} className="absolute left-3 top-1/2 -translate-y-1/2 text-gray-400" />
                                <input
                                    value={quickSearch}
                                    onChange={(e) => { setQuickSearch(e.target.value); setPage(1); }}
                                    placeholder="Search across every column -- name, phone, email, address, ID..."
                                    className="w-full text-sm"
                                    style={{ paddingLeft: "2.25rem" }}
                                />
                            </div>
                            <button
                                onClick={() => setShowFilters((s) => !s)}
                                className={`btn btn-ghost gap-1.5 !py-2 !px-3 text-xs shrink-0 ${appliedFilters.length > 0 ? "ring-1 ring-blue-400" : ""}`}
                            >
                                <Filter size={14} /> Advanced{appliedFilters.length > 0 ? ` (${appliedFilters.length})` : ""}
                                <ChevronDown size={14} className={`transition-transform ${showFilters ? "rotate-180" : ""}`} />
                            </button>
                        </div>
                        <p className="text-[11px] text-gray-400">
                            Quick search is full-text (Doris inverted index, case-insensitive, word-based) across every
                            searchable (
                            <span className="text-blue-400">•</span>
                            -marked) column below -- not just a substring match.
                        </p>

                        <AnimatePresence>
                            {showFilters && (
                                <motion.div
                                    initial={{ height: 0, opacity: 0 }} animate={{ height: "auto", opacity: 1 }} exit={{ height: 0, opacity: 0 }}
                                    transition={{ duration: 0.2 }} className="overflow-hidden"
                                >
                                    <div className="pt-3 border-t border-gray-200 dark:border-gray-700 space-y-2">
                                        {filters.length === 0 && (
                                            <p className="text-xs text-gray-400">No filters yet -- add one to narrow down by a specific column.</p>
                                        )}
                                        {filters.map((f) => {
                                            const col = columns.find((c) => c.name === f.column);
                                            const ops = col?.is_array ? OPERATORS_ARRAY : OPERATORS_SCALAR;
                                            const needsValue = f.op !== "is_null" && f.op !== "is_not_null";
                                            return (
                                                <div key={f.id} className="flex items-center gap-2">
                                                    <select
                                                        value={f.column}
                                                        onChange={(e) => {
                                                            const newCol = columns.find((c) => c.name === e.target.value);
                                                            updateFilterRow(f.id, {
                                                                column: e.target.value,
                                                                op: newCol?.is_array ? "array_contains" : "contains",
                                                            });
                                                        }}
                                                        className="text-xs py-1.5 w-40 shrink-0"
                                                    >
                                                        {columns.map((c) => <option key={c.name} value={c.name}>{c.name}</option>)}
                                                    </select>
                                                    <select
                                                        value={f.op}
                                                        onChange={(e) => updateFilterRow(f.id, { op: e.target.value })}
                                                        className="text-xs py-1.5 w-40 shrink-0"
                                                    >
                                                        {ops.map((o) => <option key={o.value} value={o.value}>{o.label}</option>)}
                                                    </select>
                                                    {needsValue && (
                                                        <input
                                                            value={f.value}
                                                            onChange={(e) => updateFilterRow(f.id, { value: e.target.value })}
                                                            placeholder="value..."
                                                            className="text-xs py-1.5 flex-1"
                                                        />
                                                    )}
                                                    <button onClick={() => removeFilterRow(f.id)} className="text-gray-400 hover:text-red-500 p-1 shrink-0">
                                                        <X size={14} />
                                                    </button>
                                                </div>
                                            );
                                        })}
                                        <div className="flex items-center gap-2 pt-1">
                                            <button onClick={addFilterRow} className="btn btn-ghost gap-1 !py-1 !px-2 text-xs">
                                                <Plus size={12} /> Add filter
                                            </button>
                                            <button onClick={applyFilters} disabled={filters.length === 0} className="btn btn-primary !py-1 !px-3 text-xs disabled:opacity-40">
                                                Apply
                                            </button>
                                            {(filters.length > 0 || appliedFilters.length > 0) && (
                                                <button onClick={clearFilters} className="btn btn-ghost !py-1 !px-2 text-xs text-gray-400">Clear all</button>
                                            )}
                                        </div>
                                    </div>
                                </motion.div>
                            )}
                        </AnimatePresence>
                    </motion.div>

                    <motion.div initial={{ opacity: 0, y: 10 }} animate={{ opacity: 1, y: 0 }} transition={{ delay: 0.06 }} className="glass-card p-4">
                        {rowsError ? (
                            <div className="text-center py-10 text-red-500 text-sm flex items-center justify-center gap-2">
                                <AlertCircle size={16} /> {(rowsError as Error).message}
                            </div>
                        ) : rowsLoading ? (
                            <div className="text-center py-10 text-gray-400 text-sm">Loading records...</div>
                        ) : rowsData?.rows.length === 0 ? (
                            <div className="text-center py-10 text-gray-400 text-sm">No records match your search/filters.</div>
                        ) : (
                            <div className={`overflow-x-auto transition-opacity ${rowsFetching ? "opacity-60" : ""}`}>
                                <table className="w-full text-sm">
                                    <thead>
                                        <tr className="text-left text-gray-500 dark:text-gray-400 text-xs border-b border-gray-200 dark:border-gray-700">
                                            {columns.map((c) => (
                                                <th
                                                    key={c.name}
                                                    onClick={() => toggleSort(c)}
                                                    className={`pb-2 pr-4 select-none whitespace-nowrap ${
                                                        c.is_array
                                                            ? "cursor-default"
                                                            : "cursor-pointer hover:text-gray-700 dark:hover:text-gray-200"
                                                    }`}
                                                    title={c.is_array ? `${c.doris_type} -- not sortable` : c.doris_type}
                                                >
                                                    {c.name}
                                                    {c.searchable && <span className="text-blue-400 ml-1" title="Full-text searchable">•</span>}
                                                    {sortCol === c.name && (sortDir === "asc" ? " ↑" : " ↓")}
                                                </th>
                                            ))}
                                            <th className="pb-2 pl-2 w-8 sticky right-0 bg-white dark:bg-gray-900"></th>
                                        </tr>
                                    </thead>
                                    <tbody className="text-gray-700 dark:text-gray-300">
                                        {rowsData?.rows.map((row: Record<string, unknown>, i: number) => (
                                            <tr key={i} className="border-b border-gray-100 dark:border-gray-800 hover:bg-gray-50 dark:hover:bg-gray-800/30 group">
                                                {columns.map((c) => (
                                                    <td key={c.name} className="py-2 pr-4">
                                                        {formatCell(c, row[c.name])}
                                                    </td>
                                                ))}
                                                <td className="py-2 pl-2 sticky right-0 bg-white dark:bg-gray-900 group-hover:bg-gray-50 dark:group-hover:bg-gray-800/30">
                                                    <button onClick={() => setSelectedRow(row)} className="text-gray-400 hover:text-blue-500 p-1" title="View full record">
                                                        <Eye size={14} />
                                                    </button>
                                                </td>
                                            </tr>
                                        ))}
                                    </tbody>
                                </table>
                            </div>
                        )}

                        {rowsData && rowsData.total > 0 && (
                            <div className="flex justify-between items-center mt-4 pt-3 border-t border-gray-200 dark:border-gray-700 text-xs flex-wrap gap-2">
                                <div className="flex items-center gap-2 text-gray-400">
                                    <span>{rowsData.total.toLocaleString()} records</span>
                                    <select
                                        value={pageSize}
                                        onChange={(e) => { setPageSize(Number(e.target.value)); setPage(1); }}
                                        className="text-xs py-1"
                                    >
                                        {[25, 50, 100, 200].map((n) => <option key={n} value={n}>{n}/page</option>)}
                                    </select>
                                </div>
                                <div className="flex items-center gap-3">
                                    <button onClick={() => setPage((p) => Math.max(1, p - 1))} disabled={page <= 1} className="btn btn-ghost !py-1 !px-2 disabled:opacity-30">Previous</button>
                                    <span className="text-gray-400">Page {page.toLocaleString()} of {totalPages.toLocaleString()}</span>
                                    <button onClick={() => setPage((p) => Math.min(totalPages, p + 1))} disabled={page >= totalPages} className="btn btn-ghost !py-1 !px-2 disabled:opacity-30">Next</button>
                                </div>
                            </div>
                        )}
                    </motion.div>
                </>
            )}

            <AnimatePresence>
                {selectedRow && (
                    <motion.div
                        initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }}
                        className="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4"
                        onClick={() => setSelectedRow(null)}
                    >
                        <motion.div
                            initial={{ opacity: 0, scale: 0.96 }} animate={{ opacity: 1, scale: 1 }} exit={{ opacity: 0, scale: 0.96 }}
                            className="glass-card p-6 max-w-2xl w-full max-h-[80vh] overflow-y-auto"
                            onClick={(e) => e.stopPropagation()}
                        >
                            <div className="flex items-center justify-between mb-4">
                                <h3 className="font-semibold text-gray-900 dark:text-white">Record Details</h3>
                                <button onClick={() => setSelectedRow(null)} className="text-gray-400 hover:text-gray-600 dark:hover:text-gray-200">
                                    <X size={18} />
                                </button>
                            </div>
                            <div className="space-y-3">
                                {columns.map((c) => {
                                    const value = selectedRow[c.name];
                                    return (
                                        <div key={c.name} className="border-b border-gray-100 dark:border-gray-800 pb-2">
                                            <div className="text-[11px] uppercase tracking-wide text-gray-400 mb-1">
                                                {c.name} <span className="lowercase text-gray-300">({c.doris_type})</span>
                                            </div>
                                            {c.is_array ? (
                                                (value as unknown[])?.length > 0 ? (
                                                    <div className="flex flex-wrap gap-1">
                                                        {(value as unknown[]).map((v, i) => (
                                                            <span key={i} className="px-2 py-0.5 rounded bg-gray-100 dark:bg-gray-800 text-xs">{String(v)}</span>
                                                        ))}
                                                    </div>
                                                ) : <span className="text-gray-400 dark:text-gray-500 text-sm">—</span>
                                            ) : (
                                                <span className="text-sm text-gray-900 dark:text-white break-words">
                                                    {value !== null && value !== undefined && value !== ""
                                                        ? String(value)
                                                        : <span className="text-gray-400 dark:text-gray-500">—</span>}
                                                </span>
                                            )}
                                        </div>
                                    );
                                })}
                            </div>
                        </motion.div>
                    </motion.div>
                )}
            </AnimatePresence>
        </div>
    );
}
