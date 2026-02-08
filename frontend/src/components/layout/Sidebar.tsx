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
    Sun,
    Moon,
} from "lucide-react";
import { useState, useEffect } from "react";
import { clsx } from "clsx";
import { useTheme } from "next-themes";

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
    const { theme, setTheme } = useTheme();
    const [mounted, setMounted] = useState(false);

    useEffect(() => {
        setMounted(true);
    }, []);

    return (
        <motion.aside
            initial={false}
            animate={{ width: collapsed ? 80 : 256 }}
            className="h-screen bg-[var(--bg-secondary)] border-r border-[var(--border-color)] flex flex-col transition-colors duration-200"
        >
            {/* Logo */}
            <div className="h-16 flex items-center justify-between px-4 border-b border-[var(--border-color)]">
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
                            <p className="text-xs text-[var(--text-secondary)]">Control Plane</p>
                        </motion.div>
                    )}
                </Link>
                <button
                    onClick={() => setCollapsed(!collapsed)}
                    className="p-1.5 rounded-lg hover:bg-gray-200 dark:hover:bg-gray-800 text-[var(--text-secondary)] hover:text-[var(--text-primary)] transition-colors"
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
                                    ? "bg-blue-50 dark:bg-blue-900/20 text-blue-600 dark:text-blue-400 border border-blue-200 dark:border-blue-500/30"
                                    : "text-[var(--text-secondary)] hover:text-[var(--text-primary)] hover:bg-gray-200 dark:hover:bg-gray-800"
                            )}
                        >
                            <item.icon size={20} className={isActive ? "text-blue-600 dark:text-blue-400" : ""} />
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
                                    className="ml-auto w-1.5 h-1.5 rounded-full bg-blue-600 dark:bg-blue-400"
                                />
                            )}
                        </Link>
                    );
                })}
            </nav>

            {/* Secondary Navigation */}
            <div className="px-3 py-4 border-t border-[var(--border-color)]">
                {secondaryNavigation.map((item) => {
                    const isActive = pathname === item.href;
                    return (
                        <Link
                            key={item.name}
                            href={item.href}
                            className={clsx(
                                "flex items-center gap-3 px-3 py-2.5 rounded-lg transition-all duration-200",
                                isActive
                                    ? "bg-gray-200 dark:bg-gray-800 text-[var(--text-primary)]"
                                    : "text-[var(--text-secondary)] hover:text-[var(--text-primary)] hover:bg-gray-200 dark:hover:bg-gray-800"
                            )}
                        >
                            <item.icon size={20} />
                            {!collapsed && (
                                <span className="text-sm font-medium">{item.name}</span>
                            )}
                        </Link>
                    );
                })}

                {/* Theme Toggle */}
                {mounted && (
                    <button
                        onClick={() => setTheme(theme === 'dark' ? 'light' : 'dark')}
                        className={clsx(
                            "w-full flex items-center gap-3 px-3 py-2.5 rounded-lg transition-all duration-200 mt-2",
                            "text-[var(--text-secondary)] hover:text-[var(--text-primary)] hover:bg-gray-200 dark:hover:bg-gray-800"
                        )}
                    >
                        {theme === 'dark' ? <Sun size={20} /> : <Moon size={20} />}
                        {!collapsed && (
                            <span className="text-sm font-medium">
                                {theme === 'dark' ? 'Light Mode' : 'Dark Mode'}
                            </span>
                        )}
                    </button>
                )}
            </div>

            {/* Status Indicator */}
            <div className="px-3 py-4 border-t border-[var(--border-color)]">
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
                            <p className="text-xs font-medium text-[var(--text-primary)]">System Online</p>
                            <p className="text-xs text-[var(--text-secondary)]">All services healthy</p>
                        </div>
                    )}
                </div>
            </div>
        </motion.aside>
    );
}
