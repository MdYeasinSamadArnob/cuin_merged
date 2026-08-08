"use client";

/**
 * Wraps every page except /login in the Sidebar + auth guard. Chosen
 * over physically moving all ~15 existing page directories into a
 * Next.js route group (e.g. app/(app)/...) -- that would achieve the
 * same visual result but risks breaking something in a large mechanical
 * file move (dynamic routes like runs/[id], relative imports, etc.) for
 * zero functional benefit over a pathname check here.
 *
 * This is a UX convenience, not the real security boundary -- the
 * backend's Depends(get_current_user)/require_menu(...) on every
 * router (see api/main.py) is what actually protects the data; this
 * just avoids flashing a Sidebar full of pages a logged-out visitor
 * can't use, and bounces them to /login instead.
 */

import { useEffect } from "react";
import { usePathname, useRouter } from "next/navigation";
import Sidebar from "@/components/organisms/layout/Sidebar";
import { useAuthStore } from "@/stores/useAuthStore";

export function AppShell({ children }: { children: React.ReactNode }) {
    const pathname = usePathname();

    if (pathname === "/login") {
        // No Sidebar, no auth check -- this IS how you get a session.
        return <>{children}</>;
    }

    return (
        <AuthGuard>
            <div className="flex h-screen overflow-hidden">
                <Sidebar />
                <main className="flex-1 overflow-y-auto">
                    <div className="container mx-auto px-6 py-8">{children}</div>
                </main>
            </div>
        </AuthGuard>
    );
}

function AuthGuard({ children }: { children: React.ReactNode }) {
    const router = useRouter();
    const token = useAuthStore((s) => s.token);
    const hasHydrated = useAuthStore((s) => s.hasHydrated);

    useEffect(() => {
        // Don't judge "logged out" until persist has actually finished
        // reading localStorage -- token is null for an instant on every
        // hard refresh regardless of session validity.
        if (hasHydrated && !token) {
            router.replace("/login");
        }
    }, [hasHydrated, token, router]);

    // Briefly blank while rehydrating (or while the redirect above takes
    // effect), rather than flashing a page full of data that's either
    // stale or about to 401 out from under it.
    if (!hasHydrated || !token) return null;
    return <>{children}</>;
}
