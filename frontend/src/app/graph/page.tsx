'use client';

import { useState, useEffect, useRef, useMemo } from 'react';
import Link from 'next/link';
import { ArrowLeft, X, CreditCard, Users, Network } from 'lucide-react';
import { useTheme } from 'next-themes';

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
        email: `user${seed}@example.com`,
        phone: `+1 (555) 000-${seed.toString().padStart(4, '0')}`,
        accounts: [
            `Checking ...${seed}`,
            `Savings ...${seed + 1}`
        ],
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
    const { theme, systemTheme } = useTheme();
    const [mounted, setMounted] = useState(false);

    // Viewport State (Pan/Zoom)
    const [transform, setTransform] = useState({ x: 0, y: 0, k: 1 });
    const transformRef = useRef({ x: 0, y: 0, k: 1 }); // Ref for animation loop access
    const [isDragging, setIsDragging] = useState(false);
    const lastMousePos = useRef({ x: 0, y: 0 });

    useEffect(() => {
        setMounted(true);
        // Initialize transform to center roughly
        const cx = window.innerWidth / 2 - 400; // rough center
        const cy = window.innerHeight / 2 - 300;
        // setTransform({ x: 0, y: 0, k: 1 }); 
    }, []);

    interface CustomerProfile {
        name: string;
        riskScore: number;
        riskLevel: string;
        kycStatus: string;
        lastLogin: string;
        balance: string;
        email?: string;
        phone?: string;
        accounts?: string[];
        product?: string;
        [key: string]: any;
    }

    const currentTheme = theme === 'system' ? systemTheme : theme;
    const isDark = mounted ? currentTheme === 'dark' : true;

    // Derived customer profile for selected node
    const customerProfile = useMemo<CustomerProfile | null>(() => {
        if (!selectedNode) return null;

        // Use properties from API if available (Real Data mode)
        if (selectedNode.properties && selectedNode.properties.name) {
            return {
                name: selectedNode.properties.name,
                riskScore: selectedNode.properties.risk === 'High' ? 90 : selectedNode.properties.risk === 'Medium' ? 60 : 20,
                riskLevel: selectedNode.properties.risk || 'Low',
                kycStatus: 'VERIFIED',
                lastLogin: new Date().toLocaleDateString(),
                balance: selectedNode.properties.balance || '$0.00',
                accounts: selectedNode.properties.product ? [selectedNode.properties.product] : [],
                ...selectedNode.properties
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
            const rect = canvas.getBoundingClientRect();
            // Handle DPI
            const dpr = window.devicePixelRatio || 1;
            canvas.width = rect.width * dpr;
            canvas.height = rect.height * dpr;
            ctx.scale(dpr, dpr);
        };
        resize();
        window.addEventListener('resize', resize);

        const nodes = graphData.nodes as GraphNode[];
        const edges = graphData.edges;
        const nodeMap = new Map(nodes.map(n => [n.id, n]));

        const simulate = () => {
            const dpr = window.devicePixelRatio || 1;
            ctx.save();
            ctx.setTransform(1, 0, 0, 1, 0, 0);
            ctx.clearRect(0, 0, canvas.width, canvas.height);
            ctx.restore();

            // Physics
            const centerX = canvas.width / dpr / 2;
            const centerY = canvas.height / dpr / 2;

            for (const node of nodes) {
                node.vx = (node.vx || 0) + (centerX - (node.x || 0)) * 0.0005;
                node.vy = (node.vy || 0) + (centerY - (node.y || 0)) * 0.0005;

                for (const other of nodes) {
                    if (node === other) continue;
                    const dx = (node.x || 0) - (other.x || 0);
                    const dy = (node.y || 0) - (other.y || 0);
                    const dist = Math.sqrt(dx * dx + dy * dy) || 1;
                    const force = 1000 / (dist * dist);
                    node.vx = (node.vx || 0) + (dx / dist) * force;
                    node.vy = (node.vy || 0) + (dy / dist) * force;
                }
            }

            for (const edge of edges) {
                const s = nodeMap.get(edge.source);
                const t = nodeMap.get(edge.target);
                if (s && t) {
                    const dx = (t.x || 0) - (s.x || 0);
                    const dy = (t.y || 0) - (s.y || 0);
                    const dist = Math.sqrt(dx * dx + dy * dy) || 1;
                    const force = (dist - 100) * 0.02;
                    s.vx = (s.vx || 0) + (dx / dist) * force;
                    s.vy = (s.vy || 0) + (dy / dist) * force;
                    t.vx = (t.vx || 0) - (dx / dist) * force;
                    t.vy = (t.vy || 0) - (dy / dist) * force;
                }
            }

            for (const node of nodes) {
                node.vx = (node.vx || 0) * 0.9;
                node.vy = (node.vy || 0) * 0.9;
                node.x = (node.x || 0) + (node.vx || 0);
                node.y = (node.y || 0) + (node.vy || 0);
            }

            // Rendering
            const { x: tx, y: ty, k } = transformRef.current;
            ctx.save();
            ctx.translate(tx, ty);
            ctx.scale(k, k);

            // Edges
            ctx.lineWidth = 1.5 / k;
            for (const edge of edges) {
                const s = nodeMap.get(edge.source);
                const t = nodeMap.get(edge.target);
                if (!s || !t) continue;
                ctx.strokeStyle = isDark ? '#4b5563' : '#d1d5db';
                ctx.beginPath();
                ctx.moveTo(s.x || 0, s.y || 0);
                ctx.lineTo(t.x || 0, t.y || 0);
                ctx.stroke();
            }

            // Nodes
            for (const node of nodes) {
                const isSelected = selectedNode?.id === node.id;
                const isHovered = hoveredNode?.id === node.id;
                const radius = node.type === 'cluster' ? 25 : 12;

                ctx.beginPath();
                ctx.arc(node.x || 0, node.y || 0, radius, 0, Math.PI * 2);

                let color = isDark ? '#9ca3af' : '#6b7280';
                if (node.type === 'cluster') {
                    if (node.properties?.confidence === 'high') {
                        color = isDark ? '#22c55e' : '#16a34a'; // Green
                    } else {
                        color = isDark ? '#a855f7' : '#9333ea'; // Purple
                    }
                }
                if (node.type === 'golden_record') color = isDark ? '#eab308' : '#ca8a04';
                if (node.type === 'record') color = isDark ? '#06b6d4' : '#0891b2';

                if (isSelected) {
                    ctx.shadowBlur = 20;
                    ctx.shadowColor = color;
                } else if (isHovered) {
                    ctx.shadowBlur = 10;
                    ctx.shadowColor = color;
                } else {
                    ctx.shadowBlur = 0;
                }

                ctx.fillStyle = color;
                ctx.fill();
                ctx.shadowBlur = 0;

                if (isSelected || isHovered) {
                    ctx.strokeStyle = isDark ? '#fff' : '#000';
                    ctx.lineWidth = 2 / k;
                    ctx.stroke();
                }

                // Labels
                const isCluster = node.type === 'cluster';
                const showLabel = isCluster || isSelected || isHovered || k > 0.6;

                if (showLabel) {
                    ctx.fillStyle = isDark ? '#fff' : '#1f2937';
                    ctx.font = `600 ${isCluster ? 12 / k : 10 / k}px Inter`;
                    ctx.textAlign = 'center';
                    ctx.textBaseline = 'middle';
                    const yOffset = isCluster ? (30 / k + 5) : (15 / k + 5);

                    if (isCluster || k > 0.4) {
                        ctx.fillText(node.label, node.x || 0, (node.y || 0) + yOffset);
                    }
                }
            }

            ctx.restore();
            animationRef.current = requestAnimationFrame(simulate);
        };

        simulate();

        return () => {
            if (animationRef.current) cancelAnimationFrame(animationRef.current);
            window.removeEventListener('resize', resize);
        };
    }, [graphData, selectedNode, hoveredNode, isDark]);

    // --- Handlers (Outside useEffect) ---
    const handleWheel = (e: React.WheelEvent) => {
        // e.preventDefault(); // Controlled by React?
        const zoomIntensity = 0.001;
        const newK = Math.min(Math.max(transformRef.current.k - e.deltaY * zoomIntensity, 0.2), 5);
        transformRef.current.k = newK;
        // No setState needed for ref-based animation update
    };

    const handleMouseDown = (e: React.MouseEvent) => {
        setIsDragging(true);
        lastMousePos.current = { x: e.clientX, y: e.clientY };
    };

    const handleMouseMove = (e: React.MouseEvent) => {
        if (isDragging) {
            const dx = e.clientX - lastMousePos.current.x;
            const dy = e.clientY - lastMousePos.current.y;
            transformRef.current.x += dx;
            transformRef.current.y += dy;
            lastMousePos.current = { x: e.clientX, y: e.clientY };
            return;
        }

        if (!graphData || !canvasRef.current) return;
        const canvas = canvasRef.current;
        const rect = canvas.getBoundingClientRect();
        const { x: tx, y: ty, k } = transformRef.current;
        // Screen -> World
        const dpr = window.devicePixelRatio || 1;
        // Note: React Event clientX is viewport relative.
        // Canvas scaling handles dpr internally for drawing, but for hit testing need to match logical coords.
        // Our transform code uses logical coords (no dpr) in stored tx/ty?
        // Wait, previous code used dpr in mouse processing.
        // Let's stick to simple logic: logic coords = (mouse - tx) / k.

        const mouseX = (e.clientX - rect.left - tx) / k;
        const mouseY = (e.clientY - rect.top - ty) / k;

        let hit = null;
        for (let i = graphData.nodes.length - 1; i >= 0; i--) {
            const n = graphData.nodes[i] as GraphNode;
            const r = n.type === 'cluster' ? 25 : 12;
            const dist = Math.sqrt(Math.pow(mouseX - (n.x || 0), 2) + Math.pow(mouseY - (n.y || 0), 2));
            if (dist <= r) {
                hit = n;
                break;
            }
        }

        if (hit?.id !== hoveredNode?.id) {
            setHoveredNode(hit || null);
            document.body.style.cursor = hit ? 'pointer' : 'default';
        }
    };

    const handleMouseUp = () => {
        setIsDragging(false);
    };

    const handleCanvasClick = (e: React.MouseEvent) => {
        if (isDragging) return;
        if (hoveredNode) setSelectedNode(hoveredNode);
        else setSelectedNode(null);
    };

    const [showSettings, setShowSettings] = useState(false);
    const [weights, setWeights] = useState({
        match_name_weight: 0.30,
        match_email_weight: 0.15,
        match_phone_weight: 0.15,
        match_natid_weight: 0.25,
        match_dob_weight: 0.10,
        match_address_weight: 0.05
    });

    const [memberDetails, setMemberDetails] = useState<any[]>([]);

    useEffect(() => {
        const fetchDetails = async () => {
            if (selectedNode?.type === 'cluster') {
                try {
                    const res = await fetch(`${process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000'}/graph/cluster/${selectedNode.id}`);
                    const data = await res.json();
                    setMemberDetails(data.member_details || []);
                } catch (e) {
                    console.error("Failed to fetch cluster details", e);
                    setMemberDetails([]);
                }
            } else {
                setMemberDetails([]);
            }

        };
        fetchDetails();
    }, [selectedNode]);

    const triggerRecluster = async () => {
        setIsLoading(true);
        try {
            // 1. Update Config
            await fetch(`${process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000'}/config`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(weights)
            });

            // 2. Trigger Re-cluster
            await fetch(`${process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000'}/graph/recluster`, {
                method: 'POST'
            });

            // 3. Refresh Graph
            await fetchGraphData();
            setShowSettings(false);
        } catch (e) {
            console.error("Re-cluster failed", e);
        } finally {
            setIsLoading(false);
        }
    };

    return (
        <div className="flex h-screen bg-gray-50 dark:bg-gray-950 transition-colors duration-200">
            {/* Main Graph Area */}
            <div className="flex-1 relative overflow-hidden">
                {/* Floating Navigation */}
                <div className="absolute top-6 left-6 z-50 flex flex-col gap-4 pointer-events-auto">
                    <Link href="/dashboard" className="p-3 bg-white/50 dark:bg-black/50 hover:bg-white/80 dark:hover:bg-black/80 backdrop-blur rounded-full text-gray-900 dark:text-white transition-all border border-gray-200 dark:border-gray-700 hover:scale-110 shadow-lg w-12 h-12 flex items-center justify-center">
                        <ArrowLeft size={20} />
                    </Link>
                    <button
                        onClick={() => setShowSettings(!showSettings)}
                        className={`p-3 backdrop-blur rounded-full text-gray-900 dark:text-white transition-all border border-gray-200 dark:border-gray-700 hover:scale-110 shadow-lg w-12 h-12 flex items-center justify-center ${showSettings ? 'bg-blue-600 text-white border-blue-500' : 'bg-white/50 dark:bg-black/50 hover:bg-white/80 dark:hover:bg-black/80'}`}
                    >
                        <Network size={20} />
                    </button>
                </div>

                {/* Overlay Header */}
                <div className="absolute top-0 left-0 p-6 z-10 w-full pointer-events-none pl-24">
                    <div className="flex justify-between items-start pointer-events-auto">
                        <div>
                            <h1 className="text-3xl font-bold text-gray-900 dark:text-white tracking-tight">Identity Graph 360</h1>
                            <p className="text-gray-600 dark:text-gray-400">Interactive Customer Single View</p>
                        </div>
                        <div className="flex gap-4">
                            <div className="bg-white/80 dark:bg-gray-900/80 backdrop-blur rounded-xl p-3 border border-gray-200 dark:border-gray-800 shadow-sm dark:shadow-none">
                                <div className="text-xs text-gray-500 uppercase font-bold">Unique Identities</div>
                                <div className="text-2xl font-bold text-purple-600 dark:text-purple-400">{stats?.total_clusters || 0}</div>
                            </div>
                            <div className="bg-white/80 dark:bg-gray-900/80 backdrop-blur rounded-xl p-3 border border-gray-200 dark:border-gray-800 shadow-sm dark:shadow-none">
                                <div className="text-xs text-gray-500 uppercase font-bold">Total Records</div>
                                <div className="text-2xl font-bold text-cyan-600 dark:text-cyan-400">{stats?.total_members || 0}</div>
                            </div>
                            <div className="bg-white/80 dark:bg-gray-900/80 backdrop-blur rounded-xl p-3 border border-gray-200 dark:border-gray-800 shadow-sm dark:shadow-none min-w-[120px]">
                                <div className="text-xs text-gray-500 uppercase font-bold">Resolution Rate</div>
                                <div className="text-2xl font-bold text-green-600 dark:text-green-400">
                                    {stats?.total_members ? Math.round((1 - (stats.total_clusters / stats.total_members)) * 100) : 0}%
                                </div>
                            </div>
                        </div>
                    </div>
                </div>

                {/* Settings Panel Overlay */}
                {showSettings && (
                    <div className="absolute top-24 left-6 z-50 w-80 bg-white/95 dark:bg-gray-900/95 backdrop-blur-md rounded-xl border border-gray-200 dark:border-gray-700 shadow-2xl p-6 transition-all animate-in fade-in slide-in-from-left-10">
                        <div className="flex justify-between items-center mb-4">
                            <h3 className="font-bold text-gray-900 dark:text-white">Clustering Logic</h3>
                            <button onClick={() => setShowSettings(false)} className="text-gray-500 hover:text-gray-700 dark:hover:text-gray-300">
                                <X size={16} />
                            </button>
                        </div>

                        <div className="space-y-4">
                            <p className="text-xs text-gray-500 dark:text-gray-400">Adjust matching weights to refine how identities are resolved.</p>

                            {[
                                { k: 'match_name_weight', l: 'Name Similarity' },
                                { k: 'match_natid_weight', l: 'National ID' },
                                { k: 'match_email_weight', l: 'Email Address' },
                                { k: 'match_phone_weight', l: 'Phone Number' },
                                { k: 'match_dob_weight', l: 'Date of Birth' }
                            ].map((field) => (
                                <div key={field.k} className="space-y-1">
                                    <div className="flex justify-between text-xs font-medium text-gray-700 dark:text-gray-300">
                                        <span>{field.l}</span>
                                        <span>{(weights[field.k as keyof typeof weights] * 100).toFixed(0)}%</span>
                                    </div>
                                    <input
                                        type="range"
                                        min="0" max="1" step="0.05"
                                        value={weights[field.k as keyof typeof weights]}
                                        onChange={(e) => setWeights({ ...weights, [field.k]: parseFloat(e.target.value) })}
                                        className="w-full h-2 bg-gray-200 rounded-lg appearance-none cursor-pointer dark:bg-gray-700 accent-blue-600"
                                    />
                                </div>
                            ))}

                            <div className="pt-4 border-t border-gray-100 dark:border-gray-800">
                                <button
                                    onClick={triggerRecluster}
                                    disabled={isLoading}
                                    className="w-full py-2 px-4 bg-blue-600 hover:bg-blue-700 text-white rounded-lg font-medium text-sm transition-colors flex items-center justify-center gap-2"
                                >
                                    {isLoading ? <span className="animate-spin text-white">⟳</span> : <Network size={16} />}
                                    Apply & Re-Cluster
                                </button>
                            </div>
                        </div>
                    </div>
                )}

                <canvas
                    ref={canvasRef}
                    className="w-full h-full block touch-none"
                    onMouseDown={handleMouseDown}
                    onMouseMove={handleMouseMove}
                    onMouseUp={handleMouseUp}
                    onMouseLeave={handleMouseUp}
                    onClick={handleCanvasClick}
                    onWheel={handleWheel}
                />
            </div>

            {/* Right Column: Node Details (Overlay) */}
            <div
                className={`w-[400px] bg-white/95 dark:bg-gray-900/95 backdrop-blur-md border-l border-gray-200 dark:border-gray-800 transition-all duration-300 absolute right-0 top-0 h-full overflow-y-auto shadow-2xl z-20 ${selectedNode ? 'translate-x-0' : 'translate-x-full'}`}
            >
                {selectedNode && customerProfile && (
                    <div className="pt-6 px-6">
                        <div className="flex justify-between items-start mb-6">
                            <div>
                                <h2 className="text-2xl font-bold text-gray-900 dark:text-white mb-2">{customerProfile.name || selectedNode.label}</h2>
                                <div className="flex items-center gap-2">
                                    <span className={`px-2 py-0.5 rounded text-xs font-bold ${customerProfile.riskLevel === 'High' ? 'bg-red-100 dark:bg-red-500/20 text-red-700 dark:text-red-400' :
                                        customerProfile.riskLevel === 'Medium' ? 'bg-yellow-100 dark:bg-yellow-500/20 text-yellow-700 dark:text-yellow-400' :
                                            'bg-green-100 dark:bg-green-500/20 text-green-700 dark:text-green-400'
                                        }`}>
                                        {String(customerProfile.riskLevel || 'UNKNOWN').toUpperCase()} RISK
                                    </span>
                                    <span className="text-gray-400 dark:text-gray-500 text-sm">•</span>
                                    <span className="text-gray-600 dark:text-gray-400 text-sm font-mono">{customerProfile.kycStatus || 'VERIFIED'}</span>
                                </div>
                            </div>
                            <button
                                onClick={() => setSelectedNode(null)}
                                className="p-2 hover:bg-gray-100 dark:hover:bg-gray-800 rounded-lg transition-colors"
                            >
                                <X size={20} className="text-gray-500" />
                            </button>
                        </div>

                        <div className="space-y-6">
                            {/* Content */}
                            {memberDetails.length > 0 && selectedNode.type === 'cluster' && (
                                <div className="space-y-3">
                                    <div className="flex items-center justify-between">
                                        <div className="text-gray-500 text-xs uppercase font-bold tracking-wider">Source Records ({memberDetails.length})</div>
                                        <div className="bg-blue-100 dark:bg-blue-900/30 text-blue-700 dark:text-blue-300 text-[10px] px-2 py-0.5 rounded-full font-bold">LINKED</div>
                                    </div>
                                    <div className="space-y-2 max-h-[300px] overflow-y-auto pr-1">
                                        {memberDetails.map((member, idx) => (
                                            <div key={idx} className="bg-white dark:bg-gray-800 p-3 rounded-lg border border-gray-200 dark:border-gray-700 text-sm">
                                                <div className="font-bold text-gray-900 dark:text-white">{member.name}</div>
                                                <div className="flex gap-2 mt-1 text-xs text-gray-500 dark:text-gray-400 flex-wrap">
                                                    {member.phone && <span>📞 {member.phone}</span>}
                                                    {member.email && <span>✉️ {member.email}</span>}
                                                    {member.id && <span>🆔 ...{member.id.substr(-4)}</span>}
                                                </div>
                                            </div>
                                        ))}
                                    </div>
                                </div>
                            )}

                            <div className="bg-gray-50 dark:bg-gray-800/50 p-4 rounded-xl border border-gray-200 dark:border-gray-700 bg-gradient-to-br from-gray-50 to-gray-100 dark:from-gray-800 dark:to-gray-900">
                                <div className="text-gray-500 dark:text-gray-400 text-xs uppercase font-bold mb-1">Total Relationship Value</div>
                                <div className="text-3xl font-mono text-gray-900 dark:text-white tracking-tight">{customerProfile.balance || '$0.00'}</div>
                            </div>

                            {/* Contact Info */}
                            <div className="space-y-3">
                                <div className="text-gray-500 text-xs uppercase font-bold tracking-wider">Contact Details</div>
                                <div className="space-y-2">
                                    <div className="flex items-center gap-3 text-sm text-gray-700 dark:text-gray-300 p-2 hover:bg-gray-100 dark:hover:bg-white/5 rounded-lg transition-colors">
                                        <div className="w-8 h-8 rounded-full bg-blue-100 dark:bg-blue-500/20 flex items-center justify-center text-blue-600 dark:text-blue-400 border border-blue-200 dark:border-blue-500/30">@</div>
                                        <span className="font-mono">{customerProfile.email || 'N/A'}</span>
                                    </div>
                                    <div className="flex items-center gap-3 text-sm text-gray-700 dark:text-gray-300 p-2 hover:bg-gray-100 dark:hover:bg-white/5 rounded-lg transition-colors">
                                        <div className="w-8 h-8 rounded-full bg-purple-100 dark:bg-purple-500/20 flex items-center justify-center text-purple-600 dark:text-purple-400 border border-purple-200 dark:border-purple-500/30">#</div>
                                        <span className="font-mono">{customerProfile.phone || 'N/A'}</span>
                                    </div>
                                </div>
                            </div>

                            {/* Active Products */}
                            <div className="space-y-3">
                                <div className="text-gray-500 text-xs uppercase font-bold tracking-wider">Active Products</div>
                                <div className="grid grid-cols-2 gap-2">
                                    {((customerProfile.accounts || [customerProfile.product || 'Unknown']).filter(Boolean) as string[]).map((acc, i) => (
                                        <div key={i} className="flex items-center gap-2 bg-white dark:bg-gray-800 p-3 rounded-lg border border-gray-200 dark:border-gray-700 hover:border-gray-400 dark:hover:border-gray-500 transition-colors">
                                            <CreditCard size={14} className="text-emerald-600 dark:text-emerald-400" />
                                            <span className="text-xs text-gray-900 dark:text-white font-medium">{acc}</span>
                                        </div>
                                    ))}
                                </div>
                            </div>

                            {/* Cluster Logic */}
                            <div className="pt-6 border-t border-gray-200 dark:border-gray-800">
                                <h3 className="text-sm font-bold text-gray-500 uppercase mb-3">Cluster Intelligence</h3>
                                <div className="bg-blue-50 dark:bg-blue-900/10 p-4 rounded-xl border border-blue-100 dark:border-blue-900/30">
                                    <p className="text-xs text-blue-800 dark:text-blue-200 leading-relaxed">
                                        <Network size={14} className="inline mr-2 text-blue-600 dark:text-blue-400" />
                                        This profile matches <span className="font-bold text-gray-900 dark:text-white">{(selectedNode.properties?.size || 1) - 1} other records</span>.
                                        <br />
                                        Adjust weights in <span className="font-bold">Settings</span> to refine.
                                    </p>
                                </div>
                            </div>

                        </div>
                    </div>
                )}
            </div>
        </div>
    );
}
