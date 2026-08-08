/**
 * CUIN v2 - Auth Store
 *
 * Session state for the internal admin app's login/RBAC system. Same
 * zustand + persist pattern as useAppStore.ts, but its OWN localStorage
 * key ('cuin-auth-storage') -- must stay distinct from useAppStore's
 * 'cuin-storage', which app/layout.tsx already reads directly by that
 * exact key in a pre-hydration <script> for the theme flash-fix.
 */

import { create } from "zustand";
import { persist } from "zustand/middleware";

export interface AuthUser {
    id: string;
    email: string;
    display_name: string;
    role_name: string;
    is_superuser: boolean;
    menus: string[];
}

interface AuthState {
    token: string | null;
    user: AuthUser | null;
    hasHydrated: boolean;
    setSession: (token: string, user: AuthUser) => void;
    updateUser: (user: AuthUser) => void;
    clearSession: () => void;
    setHasHydrated: (value: boolean) => void;
}

export const useAuthStore = create<AuthState>()(
    persist(
        (set) => ({
            token: null,
            user: null,
            hasHydrated: false,
            setSession: (token, user) => set({ token, user }),
            updateUser: (user) => set({ user }),
            clearSession: () => set({ token: null, user: null }),
            setHasHydrated: (value) => set({ hasHydrated: value }),
        }),
        {
            name: "cuin-auth-storage",
            // persist's rehydration from localStorage is async and runs
            // AFTER first render -- AuthGuard's redirect check must wait
            // for this flag, or it sees token=null on a hard refresh and
            // bounces an already-logged-in user back to /login.
            onRehydrateStorage: () => (state) => {
                state?.setHasHydrated(true);
            },
        }
    )
);

// A plain getter for non-component modules (lib/api.ts's request()
// choke point) that can't use the useAuthStore() hook -- zustand
// stores expose their current state outside React via getState().
export function getAuthToken(): string | null {
    return useAuthStore.getState().token;
}
