"use client";

import { useState } from "react";
import { useRouter } from "next/navigation";
import { LogIn } from "lucide-react";
import { api } from "@/lib/api";
import { useAuthStore } from "@/stores/useAuthStore";

export default function LoginPage() {
    const router = useRouter();
    const setSession = useAuthStore((s) => s.setSession);

    const [email, setEmail] = useState("");
    const [password, setPassword] = useState("");
    const [error, setError] = useState<string | null>(null);
    const [submitting, setSubmitting] = useState(false);

    const handleSubmit = async (e: React.FormEvent) => {
        e.preventDefault();
        setError(null);
        setSubmitting(true);
        try {
            const result = await api.login(email.trim(), password);
            setSession(result.access_token, result.user);
            router.replace("/dashboard");
        } catch {
            setError("Incorrect email or password.");
        } finally {
            setSubmitting(false);
        }
    };

    return (
        <div className="min-h-screen flex items-center justify-center bg-gray-50 dark:bg-gray-950 px-4">
            <div className="w-full max-w-sm">
                <div className="flex flex-col items-center mb-8">
                    <img src="/logo/era_logo.png" alt="Era Infotech" className="h-14 w-auto object-contain mb-3" />
                    <span className="text-xl font-semibold bg-gradient-to-r from-blue-600 to-cyan-600 dark:from-blue-400 dark:to-cyan-400 bg-clip-text text-transparent">
                        CIF
                    </span>
                    <p className="text-xs text-gray-500 dark:text-gray-400">Dedupe Engine</p>
                    <p className="text-sm text-gray-500 dark:text-gray-400 mt-2">Sign in to continue</p>
                </div>

                <form onSubmit={handleSubmit} className="glass-card p-6 space-y-4">
                    <div>
                        <label className="block text-xs font-medium text-gray-500 dark:text-gray-400 mb-1.5">Email</label>
                        <input
                            type="email"
                            value={email}
                            onChange={(e) => setEmail(e.target.value)}
                            required
                            autoFocus
                            autoComplete="username"
                            className="w-full"
                            placeholder="you@bank.com"
                        />
                    </div>
                    <div>
                        <label className="block text-xs font-medium text-gray-500 dark:text-gray-400 mb-1.5">Password</label>
                        <input
                            type="password"
                            value={password}
                            onChange={(e) => setPassword(e.target.value)}
                            required
                            autoComplete="current-password"
                            className="w-full"
                            placeholder="••••••••"
                        />
                    </div>

                    {error && (
                        <div className="text-sm p-3 rounded-lg bg-red-50 dark:bg-red-900/20 text-red-700 dark:text-red-300">
                            {error}
                        </div>
                    )}

                    <button
                        type="submit"
                        disabled={submitting || !email || !password}
                        className="btn btn-primary w-full flex items-center justify-center gap-2"
                    >
                        <LogIn size={16} />
                        {submitting ? "Signing in..." : "Sign In"}
                    </button>
                </form>
            </div>
        </div>
    );
}
