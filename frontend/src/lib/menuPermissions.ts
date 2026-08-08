/**
 * CUIN v2 - Menu permission helper
 *
 * The single place that decides "can this user see this menu" on the
 * frontend -- used by Sidebar.tsx to filter nav items and by any
 * page-level guard. This is a UX convenience only; the real security
 * boundary is the backend's require_menu(...) dependency (see
 * backend/api/deps_auth.py) on each router.
 */

import type { AuthUser } from "@/stores/useAuthStore";

export function userCanSeeMenu(user: AuthUser | null, menuKey: string): boolean {
    if (!user) return false;
    return user.is_superuser || user.menus.includes(menuKey);
}
