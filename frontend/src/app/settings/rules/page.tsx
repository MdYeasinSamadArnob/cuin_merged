"use client";

// /settings/rules merged into /settings (now a single, tabbed settings
// page -- see /settings/page.tsx's header comment for why). This route
// stays as a redirect rather than a 404 so any existing bookmarks or
// links keep working.

import { useEffect } from "react";
import { useRouter } from "next/navigation";

export default function RulesSettingsRedirect() {
    const router = useRouter();
    useEffect(() => {
        router.replace("/settings");
    }, [router]);
    return <div className="p-8 text-gray-500 dark:text-gray-400 text-sm">Redirecting to Settings...</div>;
}
