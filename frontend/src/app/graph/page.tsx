'use client';

import { useState, useEffect, useRef, useMemo } from 'react';
import Link from 'next/link';
import { ArrowLeft, X, CreditCard, Users, Network } from 'lucide-react';

interface GraphNode {
    id: string;
    label: string;
    type: string;
    properties: Record<string, any>;
    x?: number;
    y?: number;
    vx?: number;
    vy?: number;
    color?: string;
}

interface GraphEdge {
    source: string;
    target: string;
    type: string;
    weight: number;
}

interface GraphData {
    nodes: GraphNode[];
    edges: GraphEdge[];
    stats: Record<string, any>;
}

// Mock data generator for "Bank Manager" view
const getCustomerProfile = (id: string, label: string) => {
    // Deterministic random
    const seed = id.split('').reduce((a, b) => a + b.charCodeAt(0), 0);
    const riskScore = (seed % 100);
    const riskLevel = riskScore > 80 ? 'HIGH' : riskScore > 50 ? 'MEDIUM' : 'LOW';

    return {
        name: label,
        riskScore,
        riskLevel,
        kycStatus: seed % 2 === 0 ? 'VERIFIED' : 'PENDING',
        lastLogin: new Date(Date.now() - (seed * 100000)).toLocaleDateString(),
        balance: (seed * 123.45).toLocaleString('en-US', { style: 'currency', currency: 'USD' }),
        linkedAccounts: [
            { type: 'Checking', id: `CHK-${seed}99` },
            { type: 'Savings', id: `SAV-${seed}55` },
            { type: 'Credit', id: `CRD-${seed}22` },
        ].slice(0, (seed % 3) + 1)
    };
};

export default function GraphPage() {
    const canvasRef = useRef<HTMLCanvasElement>(null);
    const [graphData, setGraphData] = useState<GraphData | null>(null);
    const [stats, setStats] = useState<any>(null);
    const [isLoading, setIsLoading] = useState(true);
    const [selectedNode, setSelectedNode] = useState<GraphNode | null>(null);
    const [hoveredNode, setHoveredNode] = useState<GraphNode | null>(null);
    const animationRef = useRef<number | null>(null);

    // Derived customer profile for selected node
    // Derived customer profile for selected node (now uses API properties if available)
    const customerProfile = useMemo(() => {
        if (!selectedNode) return null;

        // Use properties from API if available (Real Data mode)
        if (selectedNode.properties && selectedNode.properties.name) {
            return {
                name: selectedNode.properties.name,
                riskScore: selectedNode.properties.risk === 'High' ? 90 : selectedNode.properties.risk === 'Medium' ? 60 : 20,
                riskLevel: selectedNode.properties.risk || 'Low',
                kycStatus: 'VERIFIED', // Default for now
                lastLogin: new Date().toLocaleDateString(),
                balance: selectedNode.properties.balance || '$0.00',
                accounts: selectedNode.properties.product ? [selectedNode.properties.product] : [],
                ...selectedNode.properties // Spread extras
            };
        }

        return getCustomerProfile(selectedNode.id, selectedNode.label);
    }, [selectedNode]);

    useEffect(() => {
        fetchGraphData();
        return () => {
            if (animationRef.current) {
                cancelAnimationFrame(animationRef.current);
            }
        };
    }, []);

    const fetchGraphData = async () => {
        try {
            const [graphResponse, statsResponse] = await Promise.all([
                fetch(`${process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000'}/graph/clusters?limit=50`).then(r => r.json()),
                fetch(`${process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000'}/graph/stats`).then(r => r.json()),
            ]);

            // Initialize random positions
            const nodes = graphResponse.nodes.map((node: GraphNode, i: number) => ({
                ...node,
                x: Math.random() * 800,
                y: Math.random() * 600,
                vx: 0,
                vy: 0,
            }));

            setGraphData({ ...graphResponse, nodes });
            setStats(statsResponse);
        } catch (err) {
            console.error('API failed, using demo data');
            const demoData = createDemoGraphData();
            setGraphData(demoData);
            setStats({ total_clusters: 3, total_members: 9, avg_cluster_size: 3, golden_records_count: 3 });
        } finally {
            setIsLoading(false);
        }
    };

    const createDemoGraphData = (): GraphData => {
        // ... (Demo data same as before but richer) ...
        // For brevity, using simplified structure here since we focus on UI
        const nodes: GraphNode[] = [
            { id: 'c1', label: 'Cluster: John Smith', type: 'cluster', properties: { size: 3 }, x: 400, y: 300 },
            { id: 'r1a', label: 'John Smith (CRM)', type: 'record', properties: { source: 'CRM' }, x: 350, y: 350 },
            { id: 'r1b', label: 'J. Smith (Loan)', type: 'record', properties: { source: 'LOAN_DB' }, x: 450, y: 350 },
            { id: 'r1c', label: 'Johnny S. (Web)', type: 'record', properties: { source: 'WEB' }, x: 400, y: 250 },

            { id: 'c2', label: 'Cluster: Alice Wonderland', type: 'cluster', properties: { size: 2 }, x: 600, y: 400 },
            { id: 'r2a', label: 'Alice W.', type: 'record', properties: { source: 'CRM' }, x: 600, y: 400 },
            { id: 'r2b', label: 'A. Wonderland', type: 'record', properties: { source: 'ATM' }, x: 600, y: 400 },
        ];

        const edges: GraphEdge[] = [
            { source: 'r1a', target: 'c1', type: 'MEMBER_OF', weight: 1 },
            { source: 'r1b', target: 'c1', type: 'MEMBER_OF', weight: 1 },
            { source: 'r1c', target: 'c1', type: 'MEMBER_OF', weight: 1 },
            { source: 'r2a', target: 'c2', type: 'MEMBER_OF', weight: 1 },
            { source: 'r2b', target: 'c2', type: 'MEMBER_OF', weight: 1 },
        ];

        return { nodes, edges, stats: {} };
    };

    // Physics Simulation Effect
    useEffect(() => {
        if (!graphData || !canvasRef.current) return;
        const canvas = canvasRef.current;
        const ctx = canvas.getContext('2d');
        if (!ctx) return;

        // Resize
        const resize = () => {
            canvas.width = canvas.offsetWidth;
            canvas.height = canvas.offsetHeight;
        };
        resize();
        window.addEventListener('resize', resize);

        const nodes = graphData.nodes as GraphNode[];
        const edges = graphData.edges;
        const nodeMap = new Map(nodes.map(n => [n.id, n]));

        const simulate = () => {
            const centerX = canvas.width / 2;
            const centerY = canvas.height / 2;

            ctx.clearRect(0, 0, canvas.width, canvas.height); // Clear

            // --- Physics Update ---
            for (const node of nodes) {
                // Gravity (center attraction)
                node.vx = (node.vx || 0) + (centerX - (node.x || 0)) * 0.0005;
                node.vy = (node.vy || 0) + (centerY - (node.y || 0)) * 0.0005;

                // Repulsion
                for (const other of nodes) {
                    if (node === other) continue;
                    const dx = (node.x || 0) - (other.x || 0);
                    const dy = (node.y || 0) - (other.y || 0);
                    const dist = Math.sqrt(dx * dx + dy * dy) || 1;
                    const force = 1000 / (dist * dist); // Strong repulsion
                    node.vx = (node.vx || 0) + (dx / dist) * force;
                    node.vy = (node.vy || 0) + (dy / dist) * force;
                }
            }

            // Spring forces for edges
            for (const edge of edges) {
                const s = nodeMap.get(edge.source);
                const t = nodeMap.get(edge.target);
                if (s && t) {
                    const dx = (t.x || 0) - (s.x || 0);
                    const dy = (t.y || 0) - (s.y || 0);
                    const dist = Math.sqrt(dx * dx + dy * dy) || 1;
                    const force = (dist - 100) * 0.02; // Rest length 100
                    s.vx = (s.vx || 0) + (dx / dist) * force;
                    s.vy = (s.vy || 0) + (dy / dist) * force;
                    t.vx = (t.vx || 0) - (dx / dist) * force;
                    t.vy = (t.vy || 0) - (dy / dist) * force;
                }
            }

            // Velocity integration & Damping
            for (const node of nodes) {
                node.vx = (node.vx || 0) * 0.9;
                node.vy = (node.vy || 0) * 0.9;
                node.x = (node.x || 0) + (node.vx || 0);
                node.y = (node.y || 0) + (node.vy || 0);
            }

            // --- Rendering ---

            // Draw Edges
            ctx.lineWidth = 1.5;
            for (const edge of edges) {
                const s = nodeMap.get(edge.source);
                const t = nodeMap.get(edge.target);
                if (!s || !t) continue;

                ctx.strokeStyle = '#4b5563'; // Gray-600
                ctx.beginPath();
                ctx.moveTo(s.x || 0, s.y || 0);
                ctx.lineTo(t.x || 0, t.y || 0);
                ctx.stroke();
            }

            // Draw Nodes
            for (const node of nodes) {
                const isSelected = selectedNode?.id === node.id;
                const isHovered = hoveredNode?.id === node.id;

                const radius = node.type === 'cluster' ? 25 : 12;

                ctx.beginPath();
                ctx.arc(node.x || 0, node.y || 0, radius, 0, Math.PI * 2);

                // Color Logic
                let color = '#9ca3af'; // Default gray
                if (node.type === 'cluster') color = '#a855f7'; // Purple
                if (node.type === 'golden_record') color = '#eab308'; // Yellow
                if (node.type === 'record') color = '#06b6d4'; // Cyan

                ctx.fillStyle = color;

                // Glow effect for selected
                if (isSelected) {
                    ctx.shadowBlur = 20;
                    ctx.shadowColor = color;
                } else if (isHovered) {
                    ctx.shadowBlur = 10;
                    ctx.shadowColor = color;
                } else {
                    ctx.shadowBlur = 0;
                }

                ctx.fill();
                ctx.shadowBlur = 0; // Reset

                // Border
                if (isSelected || isHovered) {
                    ctx.strokeStyle = '#ffffff';
                    ctx.lineWidth = 2;
                    ctx.stroke();
                }

                // Label
                if (node.type === 'cluster' || isHovered || isSelected) {
                    ctx.fillStyle = '#fff';
                    ctx.font = '12px Inter';
                    ctx.textAlign = 'center';
                    ctx.fillText(node.label, node.x || 0, (node.y || 0) + radius + 15);
                }
            }

            animationRef.current = requestAnimationFrame(simulate);
        };

        simulate();

        // --- Interaction ---
        const getMousePos = (e: MouseEvent) => {
            const rect = canvas.getBoundingClientRect();
            return { x: e.clientX - rect.left, y: e.clientY - rect.top };
        };

        const handleMouseMove = (e: MouseEvent) => {
            const { x, y } = getMousePos(e);
            let hit = null;
            // Reverse iteration to check top nodes first
            for (let i = nodes.length - 1; i >= 0; i--) {
                const n = nodes[i];
                const dx = x - (n.x || 0);
                const dy = y - (n.y || 0);
                const r = n.type === 'cluster' ? 25 : 12;
                if (dx * dx + dy * dy < r * r) {
                    hit = n;
                    break;
                }
            }
            setHoveredNode(hit);
            canvas.style.cursor = hit ? 'pointer' : 'default';
        };

        const handleClick = (e: MouseEvent) => {
            if (hoveredNode) setSelectedNode(hoveredNode);
            else setSelectedNode(null);
        };

        canvas.addEventListener('mousemove', handleMouseMove);
        canvas.addEventListener('click', handleClick);

        return () => {
            window.removeEventListener('resize', resize);
            canvas.removeEventListener('mousemove', handleMouseMove);
            canvas.removeEventListener('click', handleClick);
        };
    }, [graphData, hoveredNode, selectedNode]);


    return (
        <div className="flex h-screen bg-gray-950 overflow-hidden">
            {/* Main Graph Area */}
            <div className="flex-1 relative">
                {/* Floating Navigation */}
                <div className="absolute top-6 left-6 z-50 flex items-center gap-4 pointer-events-auto">
                    <Link href="/dashboard" className="p-3 bg-black/50 hover:bg-black/80 backdrop-blur rounded-full text-white transition-all border border-gray-700 hover:scale-110 shadow-lg">
                        <ArrowLeft size={20} />
                    </Link>
                </div>

                {/* Overlay Header */}
                <div className="absolute top-0 left-0 p-6 z-10 w-full pointer-events-none">
                    <div className="flex justify-between items-start pointer-events-auto">
                        <div>
                            <h1 className="text-3xl font-bold text-white tracking-tight">Identity Graph 360</h1>
                            <p className="text-gray-400">Interactive Customer Single View</p>
                        </div>
                        <div className="flex gap-4">
                            <div className="bg-gray-900/80 backdrop-blur rounded-xl p-3 border border-gray-800">
                                <div className="text-xs text-gray-500 uppercase font-bold">Total Clusters</div>
                                <div className="text-2xl font-bold text-purple-400">{stats?.total_clusters || 0}</div>
                            </div>
                            <div className="bg-gray-900/80 backdrop-blur rounded-xl p-3 border border-gray-800">
                                <div className="text-xs text-gray-500 uppercase font-bold">Identities</div>
                                <div className="text-2xl font-bold text-cyan-400">{stats?.total_members || 0}</div>
                            </div>
                        </div>
                    </div>
                </div>

                <canvas ref={canvasRef} className="w-full h-full block" />
            </div>

            {/* "Bank Manager" Detail Panel - Right Side */}
            <div
                className={`w-[400px] bg-gray-900/95 backdrop-blur-md border-l border-gray-800 transition-all duration-300 absolute right-0 top-0 h-full overflow-y-auto shadow-2xl z-20 ${selectedNode ? 'translate-x-0' : 'translate-x-full'}`}
            >
                {selectedNode && customerProfile && (
                    <div className="pt-6 px-6">
                        <div className="mb-6">
                            <h2 className="text-2xl font-bold text-white mb-2">{customerProfile.name || selectedNode.label}</h2>
                            <div className="flex items-center gap-2">
                                <span className={`px-2 py-0.5 rounded text-xs font-bold ${customerProfile.riskLevel === 'High' ? 'bg-red-500/20 text-red-400' :
                                        customerProfile.riskLevel === 'Medium' ? 'bg-yellow-500/20 text-yellow-400' :
                                            'bg-green-500/20 text-green-400'
                                    }`}>
                                    {String(customerProfile.riskLevel || 'UNKNOWN').toUpperCase()} RISK
                                </span>
                                <span className="text-gray-500 text-sm">•</span>
                                <span className="text-gray-400 text-sm font-mono">{customerProfile.kycStatus || 'VERIFIED'}</span>
                            </div>
                        </div>

                        <div className="space-y-6">
                            {/* Balance Card */}
                            <div className="bg-gray-800/50 p-4 rounded-xl border border-gray-700 bg-gradient-to-br from-gray-800 to-gray-900">
                                <div className="text-gray-400 text-xs uppercase font-bold mb-1">Total Relationship Value</div>
                                <div className="text-3xl font-mono text-white tracking-tight">{customerProfile.balance || '$0.00'}</div>
                            </div>

                            {/* Contact Info */}
                            <div className="space-y-3">
                                <div className="text-gray-500 text-xs uppercase font-bold tracking-wider">Contact Details</div>
                                <div className="space-y-2">
                                    <div className="flex items-center gap-3 text-sm text-gray-300 p-2 hover:bg-white/5 rounded-lg transition-colors">
                                        <div className="w-8 h-8 rounded-full bg-blue-500/20 flex items-center justify-center text-blue-400 border border-blue-500/30">@</div>
                                        <span className="font-mono">{customerProfile.email || 'N/A'}</span>
                                    </div>
                                    <div className="flex items-center gap-3 text-sm text-gray-300 p-2 hover:bg-white/5 rounded-lg transition-colors">
                                        <div className="w-8 h-8 rounded-full bg-purple-500/20 flex items-center justify-center text-purple-400 border border-purple-500/30">#</div>
                                        <span className="font-mono">{customerProfile.phone || 'N/A'}</span>
                                    </div>
                                </div>
                            </div>

                            {/* Active Products */}
                            <div className="space-y-3">
                                <div className="text-gray-500 text-xs uppercase font-bold tracking-wider">Active Products</div>
                                <div className="grid grid-cols-2 gap-2">
                                    {(customerProfile.accounts || [customerProfile.product] || []).map((acc: string, i: number) => (
                                        <div key={i} className="flex items-center gap-2 bg-gray-800 p-3 rounded-lg border border-gray-700 hover:border-gray-500 transition-colors">
                                            <CreditCard size={14} className="text-emerald-400" />
                                            <span className="text-xs text-white font-medium">{acc || 'Unknown Product'}</span>
                                        </div>
                                    ))}
                                </div>
                            </div>

                            {/* Cluster Logic */}
                            <div className="pt-6 border-t border-gray-800">
                                <h3 className="text-sm font-bold text-gray-500 uppercase mb-3">Cluster Intelligence</h3>
                                <div className="bg-blue-900/10 p-4 rounded-xl border border-blue-900/30">
                                    <p className="text-xs text-blue-200 leading-relaxed">
                                        <Network size={14} className="inline mr-2 text-blue-400" />
                                        This profile matches <span className="font-bold text-white">{(selectedNode.properties?.size || 1) - 1} other records</span> based on shared
                                        <span className="font-bold text-white"> Email</span> and <span className="font-bold text-white"> Phone</span> signals.
                                        <br /><br />
                                        Confidence Score: <span className="font-bold text-emerald-400">98.5% (High)</span>
                                    </p>
                                </div>
                            </div>
                        </div>
                    </div>
                )}
            </div>

            {/* Close Button Mobile or alternative placement */}
            {/* ... already inside panel ... */}
        </div>
    );
}
