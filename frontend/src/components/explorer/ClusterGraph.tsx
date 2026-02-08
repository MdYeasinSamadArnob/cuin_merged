'use client';

import React, { useCallback, useEffect, useRef, useState } from 'react';
import * as d3 from 'd3';
import { X, ZoomIn, ZoomOut, Maximize, RotateCcw } from 'lucide-react';

interface ClusterGraphProps {
    data: any;
    loading: boolean;
    loadingMessage?: string;
}

interface GraphNode extends d3.SimulationNodeDatum {
    id: string;
    label: string;
    type: string;
    properties: any;
    x?: number;
    y?: number;
    fx?: number | null;
    fy?: number | null;
}

interface GraphEdge extends d3.SimulationLinkDatum<GraphNode> {
    source: string | GraphNode;
    target: string | GraphNode;
    type: string;
    weight: number;
    properties?: any;
}

export function ClusterGraph({ data, loading, loadingMessage = "Running Clustering Algorithm..." }: ClusterGraphProps) {
    const canvasRef = useRef<HTMLCanvasElement>(null);
    const containerRef = useRef<HTMLDivElement>(null);
    const [selectedNode, setSelectedNode] = useState<GraphNode | null>(null);
    const [hoveredNode, setHoveredNode] = useState<GraphNode | null>(null);

    // Simulation refs to persist across renders
    const simulationRef = useRef<d3.Simulation<GraphNode, GraphEdge> | null>(null);
    const nodesRef = useRef<GraphNode[]>([]);
    const edgesRef = useRef<GraphEdge[]>([]);
    const animationRef = useRef<number | null>(null);
    const userInteractedRef = useRef(false); // Track if user has manually zoomed/panned

    // Transform state using REF for animation loop (no stale closure issues)
    const transformRef = useRef({ x: 0, y: 0, k: 1 });
    const [, forceRender] = useState(0); // Forces re-render when needed

    // Drag state
    const [isDragging, setIsDragging] = useState(false);
    const lastMousePos = useRef({ x: 0, y: 0 });
    const draggedNodeRef = useRef<GraphNode | null>(null);

    // Theme colors
    const colors = {
        cluster: { fill: '#7c3aed', stroke: '#a78bfa', glow: '#8b5cf6' },
        record: { fill: '#06b6d4', stroke: '#67e8f9', glow: '#22d3ee' },
        golden: { fill: '#f59e0b', stroke: '#fcd34d', glow: '#fbbf24' },
        background: '#111827',
        text: '#e5e7eb',
        edge: {
            match: '#3b82f6',
            member: '#4b5563',
            review: '#f59e0b'
        }
    };

    // Helper: Get node at point
    const getNodeAtPoint = useCallback((x: number, y: number): GraphNode | null => {
        let closest: GraphNode | null = null;
        let minDist = Infinity;

        for (let i = nodesRef.current.length - 1; i >= 0; i--) {
            const node = nodesRef.current[i];
            const dist = Math.hypot((node.x || 0) - x, (node.y || 0) - y);
            const radius = node.type === 'cluster' ? 22 : 9;
            if (dist < radius + 5 && dist < minDist) {
                minDist = dist;
                closest = node;
            }
        }

        return closest;
    }, []);

    // Helper: Fit View
    const fitView = useCallback((force = false) => {
        // Skip if user has manually interacted (unless forced)
        if (!force && userInteractedRef.current) return;

        const canvas = canvasRef.current;
        const container = containerRef.current;
        if (!canvas || !container || nodesRef.current.length === 0) return;

        const padding = 80;
        const width = container.clientWidth;
        const height = container.clientHeight;

        // Calculate bounding box
        let minX = Infinity, minY = Infinity, maxX = -Infinity, maxY = -Infinity;
        nodesRef.current.forEach(node => {
            if ((node.x || 0) < minX) minX = node.x || 0;
            if ((node.x || 0) > maxX) maxX = node.x || 0;
            if ((node.y || 0) < minY) minY = node.y || 0;
            if ((node.y || 0) > maxY) maxY = node.y || 0;
        });

        if (minX === Infinity) return;

        // Add node radius to bounds
        minX -= 40; maxX += 40;
        minY -= 40; maxY += 40;

        const graphWidth = maxX - minX;
        const graphHeight = maxY - minY;
        if (graphWidth <= 0 || graphHeight <= 0) return;

        // Calculate scale to fit
        const scaleX = (width - padding * 2) / graphWidth;
        const scaleY = (height - padding * 2) / graphHeight;
        let scale = Math.min(scaleX, scaleY) * 0.85;
        scale = Math.min(Math.max(scale, 0.1), 2);

        // Calculate center translation
        const centerX = (minX + maxX) / 2;
        const centerY = (minY + maxY) / 2;

        transformRef.current = {
            x: width / 2 - centerX * scale,
            y: height / 2 - centerY * scale,
            k: scale
        };
        forceRender(n => n + 1);
    }, []);

    // Initialize Simulation
    useEffect(() => {
        if (!data || !data.nodes || loading) return;

        const oldNodesMap = new Map(nodesRef.current.map(n => [n.id, n]));

        // Use a spiral layout for initial positions
        const newNodes: GraphNode[] = data.nodes.map((n: any, i: number) => {
            const old = oldNodesMap.get(n.id);
            const angle = 0.1 * i;
            const radius = 25 * angle;
            return {
                ...n,
                x: old ? old.x : Math.cos(angle) * radius,
                y: old ? old.y : Math.sin(angle) * radius,
                vx: old ? old.vx : 0,
                vy: old ? old.vy : 0
            };
        });

        const newEdges: GraphEdge[] = data.edges.map((e: any) => ({
            ...e,
            source: e.source,
            target: e.target
        }));

        nodesRef.current = newNodes;
        edgesRef.current = newEdges;
        userInteractedRef.current = false; // Reset on new data

        // Stop existing simulation
        if (simulationRef.current) simulationRef.current.stop();

        // Create new simulation
        simulationRef.current = d3.forceSimulation<GraphNode, GraphEdge>(newNodes)
            .force("link", d3.forceLink<GraphNode, GraphEdge>(newEdges)
                .id(d => d.id)
                .distance(d => d.type === 'MEMBER_OF' ? 28 : 90)
                .strength(d => d.type === 'MEMBER_OF' ? 1 : 0.2)
            )
            .force("charge", d3.forceManyBody().strength((d: any) => d.type === 'cluster' ? -260 : -45))
            .force("collide", d3.forceCollide().radius((d: any) => (d.type === 'cluster' ? 28 : 10) + 4).iterations(2))
            .force("center", d3.forceCenter(0, 0).strength(0.1))
            .force("x", d3.forceX(0).strength(0.05))
            .force("y", d3.forceY(0).strength(0.05))
            .on("end", () => {
                fitView();
            });

        // Fit view after a delay
        const midFitTimer = window.setTimeout(() => fitView(), 1200);

        return () => {
            window.clearTimeout(midFitTimer);
            if (simulationRef.current) simulationRef.current.stop();
        };
    }, [data, loading, fitView]);

    // Animation loop for rendering
    useEffect(() => {
        const canvas = canvasRef.current;
        const container = containerRef.current;
        if (!canvas || !container) return;

        const render = () => {
            const ctx = canvas.getContext('2d');
            if (!ctx) return;

            const dpr = window.devicePixelRatio || 1;
            const rect = container.getBoundingClientRect();

            // Resize canvas if needed
            if (canvas.width !== rect.width * dpr || canvas.height !== rect.height * dpr) {
                canvas.width = rect.width * dpr;
                canvas.height = rect.height * dpr;
                ctx.scale(dpr, dpr);
            }

            // Clear canvas
            ctx.save();
            ctx.setTransform(1, 0, 0, 1, 0, 0);
            ctx.clearRect(0, 0, canvas.width, canvas.height);
            ctx.restore();

            // Apply background
            ctx.fillStyle = colors.background;
            ctx.fillRect(0, 0, rect.width, rect.height);

            // Apply Zoom/Pan Transform from REF
            const { x: tx, y: ty, k } = transformRef.current;
            ctx.save();
            ctx.translate(tx, ty);
            ctx.scale(k, k);

            // Draw Edges
            edgesRef.current.forEach(edge => {
                const source = edge.source as GraphNode;
                const target = edge.target as GraphNode;
                if (!source.x || !target.x) return;

                ctx.beginPath();
                ctx.moveTo(source.x, source.y!);
                ctx.lineTo(target.x, target.y!);

                ctx.lineWidth = edge.type === 'MATCHES' ? 2 / k : 1 / k;
                ctx.strokeStyle = edge.type === 'MATCHES' ? colors.edge.match :
                    edge.type === 'REVIEW' ? colors.edge.review : colors.edge.member;
                ctx.globalAlpha = 0.6;
                ctx.stroke();
            });
            ctx.globalAlpha = 1.0;

            // Draw Nodes
            nodesRef.current.forEach(node => {
                const isSelected = selectedNode?.id === node.id;
                const isHovered = hoveredNode?.id === node.id;
                const radius = node.type === 'cluster' ? 22 : 9;

                const nodeColor = node.type === 'cluster' ? colors.cluster :
                    node.type === 'golden_record' ? colors.golden : colors.record;

                // Glow effect
                if (isSelected || isHovered || node.type === 'cluster') {
                    ctx.shadowBlur = isSelected ? 30 : 15;
                    ctx.shadowColor = nodeColor.glow;
                } else {
                    ctx.shadowBlur = 0;
                }

                ctx.beginPath();
                ctx.arc(node.x || 0, node.y || 0, radius, 0, 2 * Math.PI);
                ctx.fillStyle = nodeColor.fill;
                ctx.fill();

                // Border
                ctx.lineWidth = (isSelected ? 3 : 1.5) / k;
                ctx.strokeStyle = isSelected ? '#fff' : nodeColor.stroke;
                ctx.stroke();

                // Label
                if (node.type === 'cluster' || isSelected || isHovered || k > 0.6) {
                    ctx.shadowBlur = 0;
                    ctx.fillStyle = '#fff';
                    ctx.font = `${node.type === 'cluster' ? 'bold 12px' : '10px'} Inter, sans-serif`;
                    ctx.textAlign = 'center';
                    ctx.textBaseline = 'middle';

                    if (node.type === 'cluster') {
                        ctx.fillText(node.properties?.size || 'C', node.x || 0, node.y || 0);
                        ctx.fillStyle = '#9ca3af';
                        ctx.font = '10px Inter, sans-serif';
                        ctx.fillText(node.label.substring(0, 15), node.x || 0, (node.y || 0) + radius + 12);
                    } else {
                        ctx.fillText(node.label.substring(0, 20), node.x || 0, (node.y || 0) + radius + 12);
                    }
                }
            });

            ctx.restore();
            animationRef.current = requestAnimationFrame(render);
        };

        render();

        return () => {
            if (animationRef.current) cancelAnimationFrame(animationRef.current);
        };
    }, [selectedNode, hoveredNode, colors]);

    // --- Mouse Handlers ---
    const handleWheel = (e: React.WheelEvent) => {
        e.preventDefault();
        userInteractedRef.current = true; // User has interacted
        const zoomIntensity = 0.001;
        const newK = Math.min(Math.max(transformRef.current.k - e.deltaY * zoomIntensity, 0.1), 5);

        // Zoom towards mouse position
        const rect = canvasRef.current?.getBoundingClientRect();
        if (rect) {
            const mouseX = e.clientX - rect.left;
            const mouseY = e.clientY - rect.top;
            const oldK = transformRef.current.k;
            const scaleChange = newK / oldK;

            transformRef.current = {
                x: mouseX - (mouseX - transformRef.current.x) * scaleChange,
                y: mouseY - (mouseY - transformRef.current.y) * scaleChange,
                k: newK
            };
        }
    };

    const handleMouseDown = (e: React.MouseEvent) => {
        const rect = canvasRef.current?.getBoundingClientRect();
        if (!rect) return;

        const { x: tx, y: ty, k } = transformRef.current;
        const mouseX = (e.clientX - rect.left - tx) / k;
        const mouseY = (e.clientY - rect.top - ty) / k;

        const node = getNodeAtPoint(mouseX, mouseY);

        if (node) {
            // Start dragging node
            draggedNodeRef.current = node;
            node.fx = node.x;
            node.fy = node.y;
            if (simulationRef.current) simulationRef.current.alphaTarget(0.3).restart();
        } else {
            userInteractedRef.current = true; // User is panning
        }

        setIsDragging(true);
        lastMousePos.current = { x: e.clientX, y: e.clientY };
    };

    const handleMouseMove = (e: React.MouseEvent) => {
        const rect = canvasRef.current?.getBoundingClientRect();
        if (!rect) return;

        const { x: tx, y: ty, k } = transformRef.current;

        if (isDragging) {
            const dx = e.clientX - lastMousePos.current.x;
            const dy = e.clientY - lastMousePos.current.y;

            if (draggedNodeRef.current) {
                // Dragging a node
                const node = draggedNodeRef.current;
                node.fx = (node.fx || 0) + dx / k;
                node.fy = (node.fy || 0) + dy / k;
            } else {
                // Panning the canvas
                transformRef.current.x += dx;
                transformRef.current.y += dy;
            }

            lastMousePos.current = { x: e.clientX, y: e.clientY };
            return;
        }

        // Hover detection
        const mouseX = (e.clientX - rect.left - tx) / k;
        const mouseY = (e.clientY - rect.top - ty) / k;

        const hit = getNodeAtPoint(mouseX, mouseY);
        if (hit?.id !== hoveredNode?.id) {
            setHoveredNode(hit);
        }

        if (canvasRef.current) {
            canvasRef.current.style.cursor = hit ? 'pointer' : 'grab';
        }
    };

    const handleMouseUp = () => {
        if (draggedNodeRef.current) {
            draggedNodeRef.current.fx = null;
            draggedNodeRef.current.fy = null;
            draggedNodeRef.current = null;
            if (simulationRef.current) simulationRef.current.alphaTarget(0);
        }
        setIsDragging(false);
    };

    const handleCanvasClick = (e: React.MouseEvent) => {
        if (isDragging && draggedNodeRef.current) return; // Was dragging a node

        const rect = canvasRef.current?.getBoundingClientRect();
        if (!rect) return;

        const { x: tx, y: ty, k } = transformRef.current;
        const mouseX = (e.clientX - rect.left - tx) / k;
        const mouseY = (e.clientY - rect.top - ty) / k;

        const node = getNodeAtPoint(mouseX, mouseY);
        setSelectedNode(node);
    };

    // Zoom button handlers
    const handleZoomIn = () => {
        const newK = Math.min(transformRef.current.k * 1.2, 5);
        const container = containerRef.current;
        if (container) {
            const cx = container.clientWidth / 2;
            const cy = container.clientHeight / 2;
            const oldK = transformRef.current.k;
            const scaleChange = newK / oldK;
            transformRef.current = {
                x: cx - (cx - transformRef.current.x) * scaleChange,
                y: cy - (cy - transformRef.current.y) * scaleChange,
                k: newK
            };
        }
        forceRender(n => n + 1);
    };

    const handleZoomOut = () => {
        const newK = Math.max(transformRef.current.k * 0.8, 0.1);
        const container = containerRef.current;
        if (container) {
            const cx = container.clientWidth / 2;
            const cy = container.clientHeight / 2;
            const oldK = transformRef.current.k;
            const scaleChange = newK / oldK;
            transformRef.current = {
                x: cx - (cx - transformRef.current.x) * scaleChange,
                y: cy - (cy - transformRef.current.y) * scaleChange,
                k: newK
            };
        }
        forceRender(n => n + 1);
    };

    if (loading) {
        return (
            <div className="h-full flex items-center justify-center bg-gray-900 rounded-xl border border-gray-700">
                <div className="flex flex-col items-center gap-4">
                    <div className="w-12 h-12 border-4 border-blue-500 border-t-transparent rounded-full animate-spin"></div>
                    <div className="text-blue-400 font-medium">{loadingMessage}</div>
                </div>
            </div>
        );
    }

    if (!data) {
        return (
            <div className="h-full flex items-center justify-center bg-gray-900 rounded-xl border border-gray-700">
                <div className="text-gray-500">Run preview to see clusters</div>
            </div>
        );
    }

    return (
        <div ref={containerRef} className="h-full w-full bg-gray-900 rounded-xl border border-gray-700 overflow-hidden relative">
            <canvas
                ref={canvasRef}
                className="block w-full h-full outline-none touch-none"
                onMouseDown={handleMouseDown}
                onMouseMove={handleMouseMove}
                onMouseUp={handleMouseUp}
                onMouseLeave={handleMouseUp}
                onClick={handleCanvasClick}
                onWheel={handleWheel}
            />

            {/* Controls Overlay */}
            <div className="absolute bottom-4 left-4 flex flex-col gap-2">
                <div className="bg-gray-800/90 p-1.5 rounded-lg border border-gray-700 shadow-xl backdrop-blur-sm flex flex-col gap-1">
                    <button
                        onClick={handleZoomIn}
                        className="p-1.5 text-gray-400 hover:text-white hover:bg-gray-700 rounded transition-colors"
                        title="Zoom In"
                    >
                        <ZoomIn size={18} />
                    </button>
                    <button
                        onClick={handleZoomOut}
                        className="p-1.5 text-gray-400 hover:text-white hover:bg-gray-700 rounded transition-colors"
                        title="Zoom Out"
                    >
                        <ZoomOut size={18} />
                    </button>
                    <button
                        onClick={() => fitView(true)}
                        className="p-1.5 text-gray-400 hover:text-white hover:bg-gray-700 rounded transition-colors"
                        title="Fit View"
                    >
                        <Maximize size={18} />
                    </button>
                    <button
                        onClick={() => {
                            if (!simulationRef.current) return;
                            simulationRef.current.alpha(1).restart();
                        }}
                        className="p-1.5 text-gray-400 hover:text-white hover:bg-gray-700 rounded transition-colors"
                        title="Restart Simulation"
                    >
                        <RotateCcw size={18} />
                    </button>
                </div>

                <div className="bg-gray-800/90 p-2 rounded-lg border border-gray-700 text-gray-300 shadow-xl backdrop-blur-sm">
                    <div className="flex flex-col gap-2">
                        <div className="flex items-center gap-2">
                            <div className="w-3 h-3 rounded-full bg-purple-600 border border-purple-400 shadow-[0_0_5px_#7c3aed]"></div>
                            <span className="text-xs">Cluster</span>
                        </div>
                        <div className="flex items-center gap-2">
                            <div className="w-3 h-3 rounded-full bg-cyan-500 border border-cyan-300 shadow-[0_0_5px_#06b6d4]"></div>
                            <span className="text-xs">Record</span>
                        </div>
                    </div>
                </div>
            </div>

            {/* Node Details Panel */}
            {selectedNode && (
                <div className="absolute top-4 right-4 w-80 bg-gray-800/95 backdrop-blur border border-gray-600 rounded-lg shadow-2xl overflow-hidden flex flex-col max-h-[90%] animate-in fade-in slide-in-from-right-10 duration-200">
                    <div className="flex justify-between items-center p-4 border-b border-gray-700 bg-gray-800">
                        <h3 className="text-lg font-bold text-blue-400 flex items-center gap-2">
                            <div className={`w-3 h-3 rounded-full ${selectedNode.type === 'cluster' ? 'bg-purple-500 shadow-[0_0_8px_#a855f7]' : 'bg-cyan-500 shadow-[0_0_8px_#06b6d4]'}`}></div>
                            {selectedNode.type === 'cluster' ? 'Cluster Profile' : 'Identity Profile'}
                        </h3>
                        <button onClick={() => setSelectedNode(null)} className="text-gray-400 hover:text-white transition-colors">
                            <X size={18} />
                        </button>
                    </div>

                    <div className="p-4 space-y-4 overflow-y-auto custom-scrollbar">
                        <div>
                            <label className="text-[10px] text-gray-500 uppercase tracking-wider font-semibold">System ID</label>
                            <div className="text-xs font-mono break-all text-gray-300 bg-gray-900/50 p-2 rounded mt-1 border border-gray-700">{selectedNode.id}</div>
                        </div>

                        <div>
                            <label className="text-[10px] text-gray-500 uppercase tracking-wider font-semibold">Primary Name</label>
                            <div className="text-sm font-medium text-white text-lg">{selectedNode.label}</div>
                        </div>

                        {selectedNode.type === 'record' && (
                            <div className="space-y-3 bg-gray-900/30 p-3 rounded-lg border border-gray-700/50">
                                <div className="grid grid-cols-2 gap-3">
                                    <div>
                                        <label className="text-[10px] text-gray-500 uppercase tracking-wider font-semibold">DOB</label>
                                        <div className="text-sm text-gray-300">{selectedNode.properties.dob || selectedNode.properties.dob_norm || 'N/A'}</div>
                                    </div>
                                    <div>
                                        <label className="text-[10px] text-gray-500 uppercase tracking-wider font-semibold">Phone</label>
                                        <div className="text-sm text-gray-300">{selectedNode.properties.phone || selectedNode.properties.phone_norm || 'N/A'}</div>
                                    </div>
                                </div>
                                <div>
                                    <label className="text-[10px] text-gray-500 uppercase tracking-wider font-semibold">Address</label>
                                    <div className="text-sm text-gray-300 break-words">
                                        {selectedNode.properties.address || selectedNode.properties.address_norm || 'N/A'}
                                    </div>
                                </div>
                                <div>
                                    <label className="text-[10px] text-gray-500 uppercase tracking-wider font-semibold">Email</label>
                                    <div className="text-sm text-gray-300 break-words">
                                        {selectedNode.properties.email || selectedNode.properties.email_norm || 'N/A'}
                                    </div>
                                </div>
                            </div>
                        )}

                        {selectedNode.type === 'cluster' && (
                            <div className="bg-purple-900/20 p-4 rounded-lg border border-purple-500/30">
                                <div className="flex justify-between items-center">
                                    <label className="text-xs text-purple-300 uppercase tracking-wider font-semibold">Cluster Size</label>
                                    <span className="text-2xl font-bold text-purple-400">{selectedNode.properties.size}</span>
                                </div>
                                <div className="mt-2 text-xs text-purple-300/70">
                                    Contains {selectedNode.properties.size} linked records
                                </div>
                            </div>
                        )}

                        {/* JSON Data for Debug - Hidden by default as requested */}
                        {/* <div className="pt-2 border-t border-gray-700 mt-2">
                            <details className="text-xs text-gray-500">
                                <summary className="cursor-pointer hover:text-gray-300">Raw Properties</summary>
                                <pre className="mt-2 bg-black/30 p-2 rounded overflow-x-auto text-[10px] text-gray-400">
                                    {JSON.stringify(selectedNode.properties, null, 2)}
                                </pre>
                            </details>
                        </div> */}
                    </div>
                </div>
            )}
        </div>
    );
}
