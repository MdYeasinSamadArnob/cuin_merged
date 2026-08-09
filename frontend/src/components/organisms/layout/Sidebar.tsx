"use client";

import Link from "next/link";
import { usePathname, useRouter } from "next/navigation";
import { motion } from "framer-motion";
import {
    LayoutDashboard,
    ClipboardCheck,
    Network,
    Settings,
    ChevronLeft,
    ChevronRight,
    Sun,
    Moon,
    Database,
    Webhook,
    Table2,
    Shield,
    LogOut,
} from "lucide-react";
import { useState } from "react";
import { clsx } from "clsx";
import { useAppStore } from "@/stores/useAppStore";
import { useAuthStore } from "@/stores/useAuthStore";
import { userCanSeeMenu } from "@/lib/menuPermissions";

// "Upload" (ad-hoc Excel/CSV) is intentionally not in the nav -- it's a
// legacy small-batch entry point, separate from the real Datasource
// pipeline everything else here drives. The page and its backend route
// still work if visited directly at /upload; only the nav link is
// hidden, so nothing about that mechanism is broken. "Rules" no longer
// gets its own entry either -- it's merged into "Settings" (a single
// tabbed page now), see /settings/page.tsx.
//
// "Pipeline" and "Explorer" are hidden here too, per business
// requirement -- same treatment as Upload, NOT a deletion. Both pages
// and their backend routes are fully intact: /pipeline is still linked
// from the Dashboard's "Recent Runs -> View All" and from the run
// detail page's back-link (it's the only run-list view that exists --
// there's no separate /runs list page), and /explorer still works if
// visited directly. Only the nav entries are removed.
const navigation = [
    { name: "Dashboard", href: "/dashboard", icon: LayoutDashboard, menuKey: "dashboard" },
    { name: "Source Data", href: "/data-viewer", icon: Table2, menuKey: "source_data" },
    { name: "Ingestion Pipeline", href: "/datasource", icon: Database, menuKey: "ingestion_pipeline" },
    { name: "Workbench", href: "/review", icon: ClipboardCheck, menuKey: "workbench" },
    { name: "Graph", href: "/graph", icon: Network, menuKey: "graph" },
    { name: "API", href: "/api-docs", icon: Webhook, menuKey: "api_docs" },
];

const secondaryNavigation = [
    { name: "Settings", href: "/settings", icon: Settings, menuKey: "settings" },
];

export default function Sidebar() {
    const pathname = usePathname();
    const router = useRouter();
    const [collapsed, setCollapsed] = useState(false);
    const { theme, toggleTheme } = useAppStore();
    const user = useAuthStore((s) => s.user);
    const clearSession = useAuthStore((s) => s.clearSession);

    const visibleNav = navigation.filter((item) => userCanSeeMenu(user, item.menuKey));
    const visibleSecondary = secondaryNavigation.filter((item) => userCanSeeMenu(user, item.menuKey));

    const handleLogout = () => {
        clearSession();
        router.replace("/login");
    };

    return (
        <motion.aside
            initial={false}
            animate={{ width: collapsed ? 80 : 256 }}
            className="h-screen bg-white dark:bg-gray-900 border-r border-gray-200 dark:border-gray-800 flex flex-col transition-colors duration-300 shadow-xl dark:shadow-none z-50"
        >
            {/* Logo */}
            <div className="h-16 flex items-center justify-between px-4 border-b border-gray-200 dark:border-gray-800">
                <Link href="/" className="flex items-center gap-3">
                    <img src="/logo/era_logo.png" alt="Era Infotech" className="h-10 w-auto object-contain shrink-0" />
                    {!collapsed && (
                        <motion.div
                            initial={{ opacity: 0 }}
                            animate={{ opacity: 1 }}
                            exit={{ opacity: 0 }}
                        >
                            <span className="text-lg font-semibold bg-gradient-to-r from-blue-600 to-cyan-600 dark:from-blue-400 dark:to-cyan-400 bg-clip-text text-transparent">
                                CIF
                            </span>
                            <p className="text-xs text-gray-500 dark:text-gray-400">Dedupe Engine</p>
                        </motion.div>
                    )}
                </Link>
                <button
                    onClick={() => setCollapsed(!collapsed)}
                    className="p-1.5 rounded-lg hover:bg-gray-100 dark:hover:bg-gray-800 text-gray-500 dark:text-gray-400 hover:text-gray-900 dark:hover:text-gray-200 transition-colors"
                >
                    {collapsed ? <ChevronRight size={18} /> : <ChevronLeft size={18} />}
                </button>
            </div>

            {/* Main Navigation */}
            <nav className="flex-1 px-3 py-4 space-y-1 overflow-y-auto">
                {visibleNav.map((item) => {
                    const isActive = pathname === item.href || pathname?.startsWith(item.href + "/");
                    return (
                        <Link
                            key={item.name}
                            href={item.href}
                            className={clsx(
                                "flex items-center gap-3 px-3 py-2.5 rounded-lg transition-all duration-200",
                                isActive
                                    ? "bg-blue-50 dark:bg-blue-600/20 text-blue-600 dark:text-blue-400 border border-blue-200 dark:border-blue-500/30"
                                    : "text-gray-600 dark:text-gray-400 hover:text-gray-900 dark:hover:text-gray-200 hover:bg-gray-100 dark:hover:bg-gray-800"
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

            {/* Bottom Section */}
            <div className="p-3 border-t border-gray-200 dark:border-gray-800 space-y-1">
                <button
                    onClick={toggleTheme}
                    className="w-full flex items-center gap-3 px-3 py-2.5 rounded-lg text-gray-600 dark:text-gray-400 hover:text-gray-900 dark:hover:text-gray-200 hover:bg-gray-100 dark:hover:bg-gray-800 transition-all duration-200"
                >
                    {theme === 'dark' ? <Sun size={20} /> : <Moon size={20} />}
                    {!collapsed && (
                        <motion.span
                            initial={{ opacity: 0 }}
                            animate={{ opacity: 1 }}
                            className="text-sm font-medium"
                        >
                            {theme === 'dark' ? 'Light Mode' : 'Dark Mode'}
                        </motion.span>
                    )}
                </button>

                {visibleSecondary.map((item) => (
                    <Link
                        key={item.name}
                        href={item.href}
                        className="flex items-center gap-3 px-3 py-2.5 rounded-lg text-gray-600 dark:text-gray-400 hover:text-gray-900 dark:hover:text-gray-200 hover:bg-gray-100 dark:hover:bg-gray-800 transition-all duration-200"
                    >
                        <item.icon size={20} />
                        {!collapsed && (
                            <motion.span
                                initial={{ opacity: 0 }}
                                animate={{ opacity: 1 }}
                                className="text-sm font-medium"
                            >
                                {item.name}
                            </motion.span>
                        )}
                    </Link>
                ))}

                {/* Role Management -- hard-gated on is_superuser directly,
                    NOT the general menu-permission system (userCanSeeMenu),
                    since this must be visible ONLY to the superuser, with
                    no override possible -- matches api/routes_roles.py's
                    require_superuser gate on the backend, which similarly
                    ignores menu overrides for this one router. */}
                {user?.is_superuser && (
                    <Link
                        href="/roles"
                        className="flex items-center gap-3 px-3 py-2.5 rounded-lg text-gray-600 dark:text-gray-400 hover:text-gray-900 dark:hover:text-gray-200 hover:bg-gray-100 dark:hover:bg-gray-800 transition-all duration-200"
                    >
                        <Shield size={20} />
                        {!collapsed && (
                            <motion.span initial={{ opacity: 0 }} animate={{ opacity: 1 }} className="text-sm font-medium">
                                Role Management
                            </motion.span>
                        )}
                    </Link>
                )}

                {user && (
                    <div className="pt-2 mt-1 border-t border-gray-200 dark:border-gray-800">
                        {!collapsed && (
                            <div className="px-3 py-1.5">
                                <p className="text-xs font-medium text-gray-700 dark:text-gray-300 truncate">{user.display_name}</p>
                                <p className="text-[11px] text-gray-400 truncate">{user.role_name}</p>
                            </div>
                        )}
                        <button
                            onClick={handleLogout}
                            className="w-full flex items-center gap-3 px-3 py-2.5 rounded-lg text-gray-600 dark:text-gray-400 hover:text-red-600 dark:hover:text-red-400 hover:bg-red-50 dark:hover:bg-red-900/20 transition-all duration-200"
                        >
                            <LogOut size={20} />
                            {!collapsed && (
                                <motion.span initial={{ opacity: 0 }} animate={{ opacity: 1 }} className="text-sm font-medium">
                                    Log out
                                </motion.span>
                            )}
                        </button>
                    </div>
                )}
            </div>
        </motion.aside>
    );
}
