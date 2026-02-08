"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { motion } from "framer-motion";
import {
    LayoutDashboard,
    GitBranch,
    Search,
    ClipboardCheck,
    Shield,
    Network,
    Settings,
    ChevronLeft,
    ChevronRight,
    Upload,
} from "lucide-react";
import { useState } from "react";
import { clsx } from "clsx";

const navigation = [
    { name: "Dashboard", href: "/dashboard", icon: LayoutDashboard },
    { name: "Upload", href: "/upload", icon: Upload },
    { name: "Pipeline", href: "/pipeline", icon: GitBranch },
    { name: "Explorer", href: "/explorer", icon: Search },
    { name: "Review", href: "/review", icon: ClipboardCheck },
    { name: "Compliance", href: "/compliance", icon: Shield },
    { name: "Graph", href: "/graph", icon: Network },
];

const secondaryNavigation = [
    { name: "Settings", href: "/settings", icon: Settings },
];

export default function Sidebar() {
    const pathname = usePathname();
    const [collapsed, setCollapsed] = useState(false);

    return (
        <motion.aside
            initial={false}
            animate={{ width: collapsed ? 80 : 256 }}
            className="h-screen bg-gray-900 border-r border-gray-800 flex flex-col"
        >
            {/* Logo */}
            <div className="h-16 flex items-center justify-between px-4 border-b border-gray-800">
                <Link href="/" className="flex items-center gap-3">
                    <div className="w-10 h-10 rounded-xl bg-gradient-to-br from-blue-500 to-cyan-400 flex items-center justify-center">
                        <span className="text-xl font-bold text-white">C</span>
                    </div>
                    {!collapsed && (
                        <motion.div
                            initial={{ opacity: 0 }}
                            animate={{ opacity: 1 }}
                            exit={{ opacity: 0 }}
                        >
                            <span className="text-lg font-semibold gradient-text">
                                CUIN v2
                            </span>
                            <p className="text-xs text-gray-500">Control Plane</p>
                        </motion.div>
                    )}
                </Link>
                <button
                    onClick={() => setCollapsed(!collapsed)}
                    className="p-1.5 rounded-lg hover:bg-gray-800 text-gray-400 hover:text-gray-200 transition-colors"
                >
                    {collapsed ? <ChevronRight size={18} /> : <ChevronLeft size={18} />}
                </button>
            </div>

            {/* Main Navigation */}
            <nav className="flex-1 px-3 py-4 space-y-1 overflow-y-auto">
                {navigation.map((item) => {
                    const isActive = pathname === item.href || pathname?.startsWith(item.href + "/");
                    return (
                        <Link
                            key={item.name}
                            href={item.href}
                            className={clsx(
                                "flex items-center gap-3 px-3 py-2.5 rounded-lg transition-all duration-200",
                                isActive
                                    ? "bg-blue-600/20 text-blue-400 border border-blue-500/30"
                                    : "text-gray-400 hover:text-gray-200 hover:bg-gray-800"
                            )}
                        >
                            <item.icon size={20} className={isActive ? "text-blue-400" : ""} />
                            {!collapsed && (
                                <motion.span
                                    initial={{ opacity: 0 }}
                                    animate={{ opacity: 1 }}
                                    className="text-sm font-medium"
                                >
                                    {item.name}
                                </motion.span>
                            )}
                            {isActive && !collapsed && (
                                <motion.div
                                    layoutId="activeIndicator"
                                    className="ml-auto w-1.5 h-1.5 rounded-full bg-blue-400"
                                />
                            )}
                        </Link>
                    );
                })}
            </nav>

            {/* Secondary Navigation */}
            <div className="px-3 py-4 border-t border-gray-800">
                {secondaryNavigation.map((item) => {
                    const isActive = pathname === item.href;
                    return (
                        <Link
                            key={item.name}
                            href={item.href}
                            className={clsx(
                                "flex items-center gap-3 px-3 py-2.5 rounded-lg transition-all duration-200",
                                isActive
                                    ? "bg-gray-800 text-gray-200"
                                    : "text-gray-500 hover:text-gray-300 hover:bg-gray-800"
                            )}
                        >
                            <item.icon size={20} />
                            {!collapsed && (
                                <span className="text-sm font-medium">{item.name}</span>
                            )}
                        </Link>
                    );
                })}
            </div>

            {/* Status Indicator */}
            <div className="px-3 py-4 border-t border-gray-800">
                <div className={clsx(
                    "flex items-center gap-3 px-3 py-2",
                    collapsed && "justify-center"
                )}>
                    <div className="relative">
                        <div className="w-2.5 h-2.5 rounded-full bg-emerald-500" />
                        <div className="absolute inset-0 w-2.5 h-2.5 rounded-full bg-emerald-500 pulse-ring" />
                    </div>
                    {!collapsed && (
                        <div>
                            <p className="text-xs font-medium text-gray-300">System Online</p>
                            <p className="text-xs text-gray-500">All services healthy</p>
                        </div>
                    )}
                </div>
            </div>
        </motion.aside>
    );
}
