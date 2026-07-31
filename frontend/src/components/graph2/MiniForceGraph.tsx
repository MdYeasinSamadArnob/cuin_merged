"use client";

// Bounded-scale force graph (never more than a few hundred nodes -- the
// whole point of the v2 rewrite is to never physics-simulate the entire
// dataset at once, see ClusterBubbleMap for the top-level overview).
// Shared by ClusterDrillDown (one cluster's real members + match edges)
// and HopExplorer (an N-hop neighborhood) since both are "small node/
// edge list, force-laid-out, click a node" -- same rendering need,
// different data source and chrome around it.

import { useEffect, useRef, useState } from "react";
import * as d3 from "d3";

export interface MiniNode {
    id: string;
    label: string;
    segment?: string | null;
    isCenter?: boolean;
}

export interface MiniEdge {
    source: string;
    target: string;
    confidence_pct?: number;
    has_veto?: boolean;
    decision?: string;
    kind?: string;
}

function edgeColor(e: MiniEdge): string {
    if (e.kind === "CROSS_SEGMENT") return "#a855f7";
    if (e.has_veto) return "#ef4444";
    if (e.decision === "AUTO_LINK") return "#10b981";
    if (e.decision === "REVIEW") return "#f59e0b";
    if (e.decision === "REJECT") return "#9ca3af";
    return "#94a3b8";
}

export function MiniForceGraph({
    nodes, edges, height = 380, onNodeClick,
}: {
    nodes: MiniNode[];
    edges: MiniEdge[];
    height?: number;
    onNodeClick?: (id: string) => void;
}) {
    const containerRef = useRef<HTMLDivElement>(null);
    const [width, setWidth] = useState(600);
    const [layout, setLayout] = useState<Record<string, { x: number; y: number }>>({});

    useEffect(() => {
        const el = containerRef.current;
        if (!el) return;
        const ro = new ResizeObserver((entries) => setWidth(Math.max(entries[0].contentRect.width, 300)));
        ro.observe(el);
        return () => ro.disconnect();
    }, []);

    useEffect(() => {
        if (nodes.length === 0) {
            setLayout({});
            return;
        }
        const simNodes = nodes.map((n) => ({ ...n }));
        const simEdges = edges.map((e) => ({ ...e }));
        const sim = d3
            .forceSimulation(simNodes as any)
            .force("link", d3.forceLink(simEdges as any).id((d: any) => d.id).distance(70).strength(0.6))
            .force("charge", d3.forceManyBody().strength(-160))
            .force("center", d3.forceCenter(width / 2, height / 2))
            .force("collide", d3.forceCollide(26))
            .stop();
        // Same clamp as ClusterBubbleMap -- forceCenter doesn't bound
        // individual nodes, so an isolated/weakly-linked node under charge
        // alone can drift outside the SVG's own box and break click
        // hit-testing on whatever page content sits behind it.
        const pad = 18;
        for (let i = 0; i < 200; i++) {
            sim.tick();
            (simNodes as any[]).forEach((n) => {
                n.x = Math.max(pad, Math.min(width - pad, n.x));
                n.y = Math.max(pad, Math.min(height - pad, n.y));
            });
        }
        const pos: Record<string, { x: number; y: number }> = {};
        (simNodes as any[]).forEach((n) => {
            pos[n.id] = { x: n.x, y: n.y };
        });
        setLayout(pos);
    }, [nodes, edges, width, height]);

    return (
        <div ref={containerRef} style={{ height }} className="w-full relative">
            <svg width="100%" height="100%" viewBox={`0 0 ${width} ${height}`}>
                {edges.map((e, i) => {
                    const s = layout[e.source];
                    const t = layout[e.target];
                    if (!s || !t) return null;
                    return (
                        <line
                            key={i} x1={s.x} y1={s.y} x2={t.x} y2={t.y}
                            stroke={edgeColor(e)}
                            strokeWidth={e.confidence_pct ? 1 + e.confidence_pct / 50 : 1.5}
                            strokeOpacity={0.6}
                        />
                    );
                })}
                {nodes.map((n) => {
                    const pos = layout[n.id];
                    if (!pos) return null;
                    const isCompany = n.segment === "COMPANY";
                    return (
                        <g
                            key={n.id} transform={`translate(${pos.x},${pos.y})`}
                            className={onNodeClick ? "cursor-pointer" : ""}
                            onClick={() => onNodeClick?.(n.id)}
                        >
                            <circle
                                r={n.isCenter ? 14 : 10}
                                fill={isCompany ? "#f59e0b" : "#06b6d4"}
                                fillOpacity={0.85}
                                stroke={n.isCenter ? "#1d4ed8" : "white"}
                                strokeWidth={n.isCenter ? 2.5 : 1.5}
                            />
                            <text
                                y={n.isCenter ? 26 : 22} textAnchor="middle" fontSize={10}
                                className="fill-gray-700 dark:fill-gray-200"
                                style={{ pointerEvents: "none" }}
                            >
                                {n.label.length > 16 ? n.label.slice(0, 14) + "…" : n.label}
                            </text>
                        </g>
                    );
                })}
            </svg>
        </div>
    );
}
