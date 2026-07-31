"use client";

// The top-level overview: each CLUSTER is one bubble (never exploded to
// raw member dots) -- this is what actually fixes the old page's "dense
// blob of 20,000 identical dots" problem. Radius scales continuously
// with member_count AND color reflects a size tier -- redundant
// encoding, easier to read at a glance than either alone. Bounded to
// one page (~100-200 clusters) so d3-force stays fast.

import { useEffect, useRef, useState } from "react";
import * as d3 from "d3";
import { IdCard } from "lucide-react";

export interface ClusterItem {
    entity_id: string;
    member_count: number;
    record_type: string | null;
    size_tier: "small" | "medium" | "large";
    representative_names: string[];
    global_ref: string | null;
}

const TIER_COLOR: Record<string, { fill: string; stroke: string }> = {
    small: { fill: "#34d399", stroke: "#059669" },
    medium: { fill: "#60a5fa", stroke: "#2563eb" },
    large: { fill: "#fb923c", stroke: "#ea580c" },
};

function radiusFor(memberCount: number, maxSize: number): number {
    const minR = 8;
    const maxR = 32;
    const t = Math.min(1, Math.max(0, (memberCount - 2) / Math.max(maxSize - 2, 1)));
    return minR + t * (maxR - minR);
}

export function ClusterBubbleMap({
    items, maxClusterSize, onSelect, selectedId,
}: {
    items: ClusterItem[];
    maxClusterSize: number;
    onSelect: (entityId: string) => void;
    selectedId: string | null;
}) {
    const containerRef = useRef<HTMLDivElement>(null);
    const svgRef = useRef<SVGSVGElement>(null);
    const [dims, setDims] = useState({ width: 800, height: 520 });
    const [positions, setPositions] = useState<Record<string, { x: number; y: number }>>({});

    // Drag-to-reposition: no ongoing force simulation here (positions are
    // laid out once, then frozen into React state), so a dragged bubble
    // simply stays wherever it's dropped -- there's no physics to pin
    // against or release afterward, unlike the continuous-simulation
    // Classic mode graph. A small movement threshold distinguishes a drag
    // from a click so dragging never also fires onSelect.
    const dragRef = useRef<{ id: string; pointerId: number; startClientX: number; startClientY: number; startX: number; startY: number; moved: boolean } | null>(null);
    const DRAG_CLICK_THRESHOLD = 4;

    const handlePointerDown = (e: React.PointerEvent, id: string) => {
        const pos = positions[id];
        if (!pos) return;
        (e.currentTarget as Element).setPointerCapture(e.pointerId);
        dragRef.current = {
            id, pointerId: e.pointerId, startClientX: e.clientX, startClientY: e.clientY,
            startX: pos.x, startY: pos.y, moved: false,
        };
    };

    const handlePointerMove = (e: React.PointerEvent, id: string, r: number) => {
        const drag = dragRef.current;
        if (!drag || drag.id !== id || drag.pointerId !== e.pointerId) return;
        const dxClient = e.clientX - drag.startClientX;
        const dyClient = e.clientY - drag.startClientY;
        if (!drag.moved && Math.hypot(dxClient, dyClient) < DRAG_CLICK_THRESHOLD) return;
        drag.moved = true;
        const rect = svgRef.current?.getBoundingClientRect();
        const scaleX = rect ? dims.width / rect.width : 1;
        const scaleY = rect ? dims.height / rect.height : 1;
        const nextX = Math.max(r, Math.min(dims.width - r, drag.startX + dxClient * scaleX));
        const nextY = Math.max(r, Math.min(dims.height - r, drag.startY + dyClient * scaleY));
        setPositions((prev) => ({ ...prev, [id]: { x: nextX, y: nextY } }));
    };

    const handlePointerUp = (e: React.PointerEvent, id: string) => {
        const drag = dragRef.current;
        dragRef.current = null;
        if (!drag || drag.id !== id || drag.pointerId !== e.pointerId) return;
        if (!drag.moved) onSelect(id);
    };

    useEffect(() => {
        const el = containerRef.current;
        if (!el) return;
        const ro = new ResizeObserver((entries) => {
            const { width, height } = entries[0].contentRect;
            setDims({ width: Math.max(width, 300), height: Math.max(height, 300) });
        });
        ro.observe(el);
        return () => ro.disconnect();
    }, []);

    useEffect(() => {
        if (items.length === 0) {
            setPositions({});
            return;
        }
        const nodes = items.map((it) => ({ id: it.entity_id, r: radiusFor(it.member_count, maxClusterSize) }));
        const sim = d3
            .forceSimulation(nodes as any)
            .force("charge", d3.forceManyBody().strength(-20))
            .force("center", d3.forceCenter(dims.width / 2, dims.height / 2))
            .force("collide", d3.forceCollide<any>().radius((d: any) => d.r + 3).strength(1).iterations(3))
            .stop();
        // forceCenter only pulls the CENTROID of all nodes toward the middle --
        // it does NOT keep any individual node inside the viewBox. With ~120
        // nodes, repulsion alone pushes outliers well past the visible area
        // (confirmed live: nodes landing 300+px outside the SVG's own
        // bounding box, silently overlapping page content above/beside it
        // and breaking click hit-testing). Clamp every node back inside
        // [r, dim-r] after every tick so nothing can ever escape the canvas.
        for (let i = 0; i < 150; i++) {
            sim.tick();
            (nodes as any[]).forEach((n) => {
                n.x = Math.max(n.r, Math.min(dims.width - n.r, n.x));
                n.y = Math.max(n.r, Math.min(dims.height - n.r, n.y));
            });
        }
        const pos: Record<string, { x: number; y: number }> = {};
        (nodes as any[]).forEach((n) => {
            pos[n.id] = { x: n.x, y: n.y };
        });
        setPositions(pos);
    }, [items, dims.width, dims.height, maxClusterSize]);

    if (items.length === 0) {
        return <div ref={containerRef} className="w-full h-full flex items-center justify-center text-sm text-gray-400">No clusters match these filters.</div>;
    }

    // Render the selected bubble LAST so its highlight ring is never
    // clipped underneath a neighbor -- SVG has no z-index, paint order is
    // document order.
    const orderedItems = selectedId
        ? [...items.filter((it) => it.entity_id !== selectedId), ...items.filter((it) => it.entity_id === selectedId)]
        : items;

    return (
        <div ref={containerRef} className="w-full h-full">
            <svg ref={svgRef} data-testid="cluster-bubble-svg" width="100%" height="100%" viewBox={`0 0 ${dims.width} ${dims.height}`} className="select-none">
                {orderedItems.map((it) => {
                    const pos = positions[it.entity_id];
                    if (!pos) return null;
                    const r = radiusFor(it.member_count, maxClusterSize);
                    const color = TIER_COLOR[it.size_tier] || TIER_COLOR.medium;
                    const isSelected = selectedId === it.entity_id;
                    return (
                        <g
                            key={it.entity_id} transform={`translate(${pos.x},${pos.y})`}
                            className="cursor-grab active:cursor-grabbing"
                            onPointerDown={(e) => handlePointerDown(e, it.entity_id)}
                            onPointerMove={(e) => handlePointerMove(e, it.entity_id, r)}
                            onPointerUp={(e) => handlePointerUp(e, it.entity_id)}
                            style={{ touchAction: "none" }}
                        >
                            <title>{(it.representative_names || []).join(", ") || it.entity_id} -- {it.member_count} members</title>
                            {isSelected && (
                                <circle r={r + 7} fill="none" stroke="#1d4ed8" strokeWidth={2} opacity={0.55}>
                                    <animate attributeName="r" values={`${r + 4};${r + 13};${r + 4}`} dur="1.6s" repeatCount="indefinite" />
                                    <animate attributeName="opacity" values="0.6;0;0.6" dur="1.6s" repeatCount="indefinite" />
                                </circle>
                            )}
                            <circle
                                r={r} fill={color.fill} fillOpacity={0.78}
                                stroke={isSelected ? "#1d4ed8" : color.stroke} strokeWidth={isSelected ? 3 : 1.5}
                            />
                            {r > 15 && (
                                <text
                                    textAnchor="middle" dy="0.35em" fontSize={Math.min(13, r / 2.2)}
                                    fill="#0f172a" fontWeight={700} style={{ pointerEvents: "none" }}
                                >
                                    {it.member_count}
                                </text>
                            )}
                            {it.global_ref && (
                                <g transform={`translate(${r * 0.68},${-r * 0.68})`} style={{ pointerEvents: "none" }}>
                                    <circle r={6} fill="#10b981" stroke="white" strokeWidth={1} />
                                </g>
                            )}
                        </g>
                    );
                })}
            </svg>
        </div>
    );
}

export function TierLegend() {
    return (
        <div className="flex items-center gap-4 text-xs text-gray-500 dark:text-gray-400">
            {(["small", "medium", "large"] as const).map((tier) => (
                <div key={tier} className="flex items-center gap-1.5">
                    <span className="inline-block w-3 h-3 rounded-full" style={{ background: TIER_COLOR[tier].fill, border: `1.5px solid ${TIER_COLOR[tier].stroke}` }} />
                    <span className="capitalize">{tier}</span>
                </div>
            ))}
            <div className="flex items-center gap-1.5">
                <IdCard size={12} className="text-emerald-500" />
                <span>Has Global ID</span>
            </div>
        </div>
    );
}
