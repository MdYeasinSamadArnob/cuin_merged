"use client";

/**
 * Role Management -- superuser-only (both here client-side, via
 * Sidebar.tsx's is_superuser gate, and server-side via
 * api/routes_roles.py's require_superuser dependency, which is the
 * real security boundary; a non-superuser hitting this page directly
 * would see every call below fail with 403).
 */

import { useState } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { motion } from "framer-motion";
import { Shield, Plus, Trash2, Users as UsersIcon, KeyRound } from "lucide-react";
import { api } from "@/lib/api";

// Excludes "role_management" on purpose -- that menu is hard-gated to
// is_superuser directly (see Sidebar.tsx), not the general menu
// permission system, so offering it as a togglable checkbox here would
// look like it does something when it never does anything (the backend
// ignores it entirely for this one router).
const ASSIGNABLE_MENU_KEYS = [
    "dashboard", "source_data", "ingestion_pipeline", "workbench", "graph", "api_docs", "settings",
];

const MENU_LABELS: Record<string, string> = {
    dashboard: "Dashboard",
    source_data: "Source Data",
    ingestion_pipeline: "Ingestion Pipeline",
    workbench: "Workbench",
    graph: "Graph",
    api_docs: "API",
    settings: "Settings",
};

interface Role {
    id: string;
    name: string;
    is_superuser: boolean;
    menu_keys: string[];
}

interface UserRow {
    id: string;
    email: string;
    display_name: string;
    role_id: string;
    role_name: string;
    is_active: boolean;
    last_login_at: string | null;
    menu_overrides: Record<string, boolean>;
}

function MenuCheckboxes({ selected, onChange, disabled }: { selected: string[]; onChange: (keys: string[]) => void; disabled?: boolean }) {
    return (
        <div className="flex flex-wrap gap-x-4 gap-y-1.5">
            {ASSIGNABLE_MENU_KEYS.map((key) => (
                <label key={key} className={`flex items-center gap-1.5 text-xs ${disabled ? "opacity-50" : "cursor-pointer"}`}>
                    <input
                        type="checkbox"
                        disabled={disabled}
                        checked={selected.includes(key)}
                        onChange={(e) => {
                            onChange(e.target.checked ? [...selected, key] : selected.filter((k) => k !== key));
                        }}
                    />
                    {MENU_LABELS[key]}
                </label>
            ))}
        </div>
    );
}

export default function RoleManagementPage() {
    const queryClient = useQueryClient();

    const { data: roles = [], isLoading: rolesLoading } = useQuery<Role[]>({
        queryKey: ["roles-list"],
        queryFn: () => api.listRoles(),
    });
    const { data: users = [], isLoading: usersLoading } = useQuery<UserRow[]>({
        queryKey: ["roles-users"],
        queryFn: () => api.listUsers(),
    });

    const invalidate = () => {
        queryClient.invalidateQueries({ queryKey: ["roles-list"] });
        queryClient.invalidateQueries({ queryKey: ["roles-users"] });
    };

    const [newRoleName, setNewRoleName] = useState("");
    const [newRoleMenus, setNewRoleMenus] = useState<string[]>([]);
    const [error, setError] = useState<string | null>(null);

    const createRole = useMutation({
        mutationFn: () => api.createRole(newRoleName.trim(), newRoleMenus),
        onSuccess: () => { setNewRoleName(""); setNewRoleMenus([]); invalidate(); },
        onError: (e: any) => setError(e.message || "Failed to create role."),
    });

    const updateRoleMenus = useMutation({
        mutationFn: ({ roleId, menuKeys }: { roleId: string; menuKeys: string[] }) => api.updateRole(roleId, { menu_keys: menuKeys }),
        onSuccess: invalidate,
        onError: (e: any) => setError(e.message || "Failed to update role."),
    });

    const deleteRole = useMutation({
        mutationFn: (roleId: string) => api.deleteRole(roleId),
        onSuccess: invalidate,
        onError: (e: any) => setError(e.message || "Failed to delete role."),
    });

    const [newUser, setNewUser] = useState({ email: "", password: "", display_name: "", role_id: "" });

    const createUser = useMutation({
        mutationFn: () => api.createUser(newUser),
        onSuccess: () => { setNewUser({ email: "", password: "", display_name: "", role_id: "" }); invalidate(); },
        onError: (e: any) => setError(e.message || "Failed to create user."),
    });

    const updateUser = useMutation({
        mutationFn: ({ userId, patch }: { userId: string; patch: any }) => api.updateUser(userId, patch),
        onSuccess: invalidate,
        onError: (e: any) => setError(e.message || "Failed to update user."),
    });

    const deleteUser = useMutation({
        mutationFn: (userId: string) => api.deleteUser(userId),
        onSuccess: invalidate,
        onError: (e: any) => setError(e.message || "Failed to delete user."),
    });

    const [expandedOverridesFor, setExpandedOverridesFor] = useState<string | null>(null);
    const setOverride = useMutation({
        mutationFn: ({ userId, menuKey, granted }: { userId: string; menuKey: string; granted: boolean | null }) =>
            api.setUserMenuOverride(userId, menuKey, granted),
        onSuccess: invalidate,
        onError: (e: any) => setError(e.message || "Failed to update override."),
    });

    return (
        <div className="space-y-8">
            <div className="flex items-center gap-3">
                <div className="p-2.5 rounded-xl bg-blue-100 dark:bg-blue-900/30 text-blue-600 dark:text-blue-400">
                    <Shield size={22} />
                </div>
                <div>
                    <h1 className="text-2xl font-bold text-gray-900 dark:text-white">Role Management</h1>
                    <p className="text-sm text-gray-500 dark:text-gray-400">Superuser-only. Create roles, assign menu access, manage users.</p>
                </div>
            </div>

            {error && (
                <div className="text-sm p-3 rounded-lg bg-red-50 dark:bg-red-900/20 text-red-700 dark:text-red-300 flex items-center justify-between">
                    {error}
                    <button onClick={() => setError(null)} className="text-xs underline">dismiss</button>
                </div>
            )}

            {/* Roles */}
            <motion.div initial={{ opacity: 0, y: 10 }} animate={{ opacity: 1, y: 0 }} className="glass-card p-6">
                <h2 className="font-semibold text-gray-900 dark:text-white mb-4 flex items-center gap-2">
                    <Shield size={16} /> Roles
                </h2>

                <div className="space-y-3 mb-6">
                    {rolesLoading && <p className="text-xs text-gray-400">Loading roles...</p>}
                    {roles.map((role) => (
                        <div key={role.id} className="p-4 rounded-lg bg-gray-50 dark:bg-gray-900/40 border border-gray-200 dark:border-gray-800">
                            <div className="flex items-center justify-between mb-2">
                                <div className="flex items-center gap-2">
                                    <span className="font-medium text-sm text-gray-900 dark:text-white">{role.name}</span>
                                    {role.is_superuser && <span className="badge badge-info">Superuser</span>}
                                </div>
                                {!role.is_superuser && (
                                    <button
                                        onClick={() => { if (confirm(`Delete role "${role.name}"?`)) deleteRole.mutate(role.id); }}
                                        className="text-gray-400 hover:text-red-500 transition-colors"
                                        title="Delete role"
                                    >
                                        <Trash2 size={14} />
                                    </button>
                                )}
                            </div>
                            {role.is_superuser ? (
                                <p className="text-xs text-gray-400">Full access to every menu -- can't be limited.</p>
                            ) : (
                                <MenuCheckboxes
                                    selected={role.menu_keys}
                                    onChange={(keys) => updateRoleMenus.mutate({ roleId: role.id, menuKeys: keys })}
                                />
                            )}
                        </div>
                    ))}
                </div>

                <div className="pt-4 border-t border-gray-200 dark:border-gray-800">
                    <p className="text-xs font-medium text-gray-500 dark:text-gray-400 mb-2">New role</p>
                    <div className="flex flex-col gap-2">
                        <input
                            value={newRoleName}
                            onChange={(e) => setNewRoleName(e.target.value)}
                            placeholder="Role name (e.g. Auditor)"
                            className="text-sm max-w-xs"
                        />
                        <MenuCheckboxes selected={newRoleMenus} onChange={setNewRoleMenus} />
                        <button
                            onClick={() => createRole.mutate()}
                            disabled={!newRoleName.trim() || createRole.isPending}
                            className="btn btn-primary !py-1.5 !px-3 text-xs w-fit flex items-center gap-1.5"
                        >
                            <Plus size={14} /> Create role
                        </button>
                    </div>
                </div>
            </motion.div>

            {/* Users */}
            <motion.div initial={{ opacity: 0, y: 10 }} animate={{ opacity: 1, y: 0 }} transition={{ delay: 0.05 }} className="glass-card p-6">
                <h2 className="font-semibold text-gray-900 dark:text-white mb-4 flex items-center gap-2">
                    <UsersIcon size={16} /> Users
                </h2>

                <div className="space-y-2 mb-6">
                    {usersLoading && <p className="text-xs text-gray-400">Loading users...</p>}
                    {users.map((u) => (
                        <div key={u.id} className="rounded-lg bg-gray-50 dark:bg-gray-900/40 border border-gray-200 dark:border-gray-800 overflow-hidden">
                            <div className="flex items-center justify-between p-3">
                                <div className="min-w-0">
                                    <div className="flex items-center gap-2">
                                        <span className="text-sm font-medium text-gray-900 dark:text-white truncate">{u.display_name}</span>
                                        {!u.is_active && <span className="badge badge-neutral">Deactivated</span>}
                                    </div>
                                    <p className="text-xs text-gray-400 truncate">{u.email}</p>
                                </div>
                                <div className="flex items-center gap-2 shrink-0">
                                    <select
                                        value={u.role_id}
                                        onChange={(e) => updateUser.mutate({ userId: u.id, patch: { role_id: e.target.value } })}
                                        className="text-xs !w-auto"
                                    >
                                        {roles.map((r) => (
                                            <option key={r.id} value={r.id}>{r.name}</option>
                                        ))}
                                    </select>
                                    <button
                                        onClick={() => setExpandedOverridesFor(expandedOverridesFor === u.id ? null : u.id)}
                                        className="text-gray-400 hover:text-blue-500 transition-colors"
                                        title="Per-user menu overrides"
                                    >
                                        <KeyRound size={14} />
                                    </button>
                                    <button
                                        onClick={() => updateUser.mutate({ userId: u.id, patch: { is_active: !u.is_active } })}
                                        className="text-xs text-gray-500 hover:text-gray-900 dark:hover:text-white underline"
                                    >
                                        {u.is_active ? "Deactivate" : "Reactivate"}
                                    </button>
                                    <button
                                        onClick={() => { if (confirm(`Delete user ${u.email}?`)) deleteUser.mutate(u.id); }}
                                        className="text-gray-400 hover:text-red-500 transition-colors"
                                        title="Delete user"
                                    >
                                        <Trash2 size={14} />
                                    </button>
                                </div>
                            </div>
                            {expandedOverridesFor === u.id && (
                                <div className="px-3 pb-3 pt-1 border-t border-gray-200 dark:border-gray-800">
                                    <p className="text-[11px] text-gray-400 mb-2">
                                        Per-user override -- wins over the role&apos;s default for that one menu, either direction.
                                    </p>
                                    <div className="flex flex-wrap gap-3">
                                        {ASSIGNABLE_MENU_KEYS.map((key) => {
                                            const overrideState = u.menu_overrides?.[key];
                                            return (
                                                <div key={key} className="flex items-center gap-1 text-xs">
                                                    <span className="text-gray-500 dark:text-gray-400">{MENU_LABELS[key]}</span>
                                                    <select
                                                        value={overrideState === undefined ? "default" : overrideState ? "grant" : "revoke"}
                                                        onChange={(e) => {
                                                            const v = e.target.value;
                                                            setOverride.mutate({
                                                                userId: u.id, menuKey: key,
                                                                granted: v === "default" ? null : v === "grant",
                                                            });
                                                        }}
                                                        className="text-xs !w-auto"
                                                    >
                                                        <option value="default">role default</option>
                                                        <option value="grant">force allow</option>
                                                        <option value="revoke">force deny</option>
                                                    </select>
                                                </div>
                                            );
                                        })}
                                    </div>
                                </div>
                            )}
                        </div>
                    ))}
                </div>

                <div className="pt-4 border-t border-gray-200 dark:border-gray-800">
                    <p className="text-xs font-medium text-gray-500 dark:text-gray-400 mb-2">New user</p>
                    <div className="flex flex-wrap gap-2 items-center">
                        <input
                            value={newUser.display_name}
                            onChange={(e) => setNewUser((s) => ({ ...s, display_name: e.target.value }))}
                            placeholder="Full name"
                            className="text-sm !w-40"
                        />
                        <input
                            value={newUser.email}
                            onChange={(e) => setNewUser((s) => ({ ...s, email: e.target.value }))}
                            placeholder="Email"
                            type="email"
                            className="text-sm !w-48"
                        />
                        <input
                            value={newUser.password}
                            onChange={(e) => setNewUser((s) => ({ ...s, password: e.target.value }))}
                            placeholder="Temporary password"
                            type="password"
                            className="text-sm !w-40"
                        />
                        <select
                            value={newUser.role_id}
                            onChange={(e) => setNewUser((s) => ({ ...s, role_id: e.target.value }))}
                            className="text-sm !w-auto"
                        >
                            <option value="">Select role...</option>
                            {roles.map((r) => (
                                <option key={r.id} value={r.id}>{r.name}</option>
                            ))}
                        </select>
                        <button
                            onClick={() => createUser.mutate()}
                            disabled={!newUser.email || !newUser.password || !newUser.display_name || !newUser.role_id || createUser.isPending}
                            className="btn btn-primary !py-1.5 !px-3 text-xs flex items-center gap-1.5"
                        >
                            <Plus size={14} /> Create user
                        </button>
                    </div>
                </div>
            </motion.div>
        </div>
    );
}
