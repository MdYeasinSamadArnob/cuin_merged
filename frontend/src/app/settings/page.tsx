"use client";

import { useEffect, useState } from "react";
import { motion } from "framer-motion";
import { Settings as SettingsIcon, Database, Shield, Bell, Palette, Save, RefreshCw } from "lucide-react";
import { api } from "@/lib/api";

export default function SettingsPage() {
    const [loading, setLoading] = useState(true);
    const [saving, setSaving] = useState(false);
    const [config, setConfig] = useState<any>({
        blocking: {},
        scoring: {}
    });

    useEffect(() => {
        loadConfig();
    }, []);

    const loadConfig = async () => {
        try {
            setLoading(true);
            const data = await api.getConfig();
            setConfig(data);
        } catch (error) {
            console.error("Failed to load config", error);
        } finally {
            setLoading(false);
        }
    };

    const handleSave = async () => {
        try {
            setSaving(true);
            const updatePayload = {
                // Blocking
                blocking_max_block_size: Number(config.blocking.max_block_size),
                blocking_suppress_pct: Number(config.blocking.suppress_frequency_pct),
                blocking_lsh_threshold: Number(config.blocking.lsh_threshold),

                // Scoring
                match_name_weight: Number(config.scoring.name_weight),
                match_phone_weight: Number(config.scoring.phone_weight),
                match_email_weight: Number(config.scoring.email_weight),
                match_dob_weight: Number(config.scoring.dob_weight),
                match_natid_weight: Number(config.scoring.natid_weight),
                match_address_weight: Number(config.scoring.address_weight),
            };

            await api.updateConfig(updatePayload);
            // Reload to confirm application
            await loadConfig();
        } catch (error) {
            console.error("Failed to save config", error);
        } finally {
            setSaving(false);
        }
    };

    const updateNested = (category: 'blocking' | 'scoring', field: string, value: any) => {
        setConfig((prev: any) => ({
            ...prev,
            [category]: {
                ...prev[category],
                [field]: value
            }
        }));
    };

    if (loading && !config.blocking.max_block_size) {
        return <div className="p-8 text-gray-900 dark:text-white">Loading settings...</div>;
    }

    return (
        <div className="space-y-6">
            {/* Header */}
            <div className="flex justify-between items-center">
                <div>
                    <h1 className="text-2xl font-bold text-gray-900 dark:text-white">Settings</h1>
                    <p className="text-gray-600 dark:text-gray-400 mt-1">
                        Configure CUIN v2 Pipeline Parameters
                    </p>
                </div>
                <button
                    onClick={handleSave}
                    disabled={saving}
                    className="btn btn-primary gap-2"
                >
                    {saving ? <RefreshCw className="animate-spin" size={18} /> : <Save size={18} />}
                    Save Changes
                </button>
            </div>

            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                {/* Blocking Configuration */}
                <motion.div
                    initial={{ opacity: 0, y: 20 }}
                    animate={{ opacity: 1, y: 0 }}
                    className="glass-card p-6"
                >
                    <div className="flex items-center gap-3 mb-6">
                        <div className="p-2 rounded-lg bg-blue-100 dark:bg-blue-500/20">
                            <Database size={20} className="text-blue-600 dark:text-blue-400" />
                        </div>
                        <h2 className="font-semibold text-gray-900 dark:text-white">Blocking Strategy</h2>
                    </div>

                    <div className="space-y-6">
                        <div>
                            <div className="flex justify-between mb-2">
                                <label className="text-sm text-gray-600 dark:text-gray-300">Max Block Size</label>
                                <span className="text-xs text-blue-600 dark:text-blue-400">{config.blocking.max_block_size} records</span>
                            </div>
                            <input
                                type="number"
                                value={config.blocking.max_block_size || 200}
                                onChange={(e) => updateNested('blocking', 'max_block_size', e.target.value)}
                                className="input input-sm w-full bg-white dark:bg-gray-900 border-gray-200 dark:border-gray-700 text-gray-900 dark:text-white"
                            />
                            <p className="text-xs text-gray-500 dark:text-gray-400 mt-1">
                                Blocks larger than this are skipped or require secondary blocking.
                            </p>
                        </div>

                        <div>
                            <div className="flex justify-between mb-2">
                                <label className="text-sm text-gray-600 dark:text-gray-300">Suppression Frequency</label>
                                <span className="text-xs text-blue-600 dark:text-blue-400">{config.blocking.suppress_frequency_pct}%</span>
                            </div>
                            <input
                                type="range"
                                min="0.1" max="100" step="0.1"
                                value={config.blocking.suppress_frequency_pct || 5.0}
                                onChange={(e) => updateNested('blocking', 'suppress_frequency_pct', e.target.value)}
                                className="range range-xs range-primary"
                            />
                            <p className="text-xs text-gray-500 dark:text-gray-400 mt-1">
                                Keys appearing in more than X% of records are dropped.
                            </p>
                        </div>

                        <div>
                            <div className="flex justify-between mb-2">
                                <label className="text-sm text-gray-600 dark:text-gray-300">LSH Threshold (MinHash)</label>
                                <span className="text-xs text-blue-600 dark:text-blue-400">{config.blocking.lsh_threshold}</span>
                            </div>
                            <input
                                type="range"
                                min="0.1" max="0.9" step="0.05"
                                value={config.blocking.lsh_threshold || 0.5}
                                onChange={(e) => updateNested('blocking', 'lsh_threshold', e.target.value)}
                                className="range range-xs range-secondary"
                            />
                            <p className="text-xs text-gray-500 dark:text-gray-400 mt-1">
                                Lower threshold captures more fuzzy duplicates but increases load.
                            </p>
                        </div>
                    </div>
                </motion.div>

                {/* Scoring Configuration */}
                <motion.div
                    initial={{ opacity: 0, y: 20 }}
                    animate={{ opacity: 1, y: 0 }}
                    transition={{ delay: 0.1 }}
                    className="glass-card p-6"
                >
                    <div className="flex items-center gap-3 mb-6">
                        <div className="p-2 rounded-lg bg-emerald-100 dark:bg-emerald-500/20">
                            <Shield size={20} className="text-emerald-600 dark:text-emerald-400" />
                        </div>
                        <h2 className="font-semibold text-gray-900 dark:text-white">Matching Weights</h2>
                    </div>

                    <div className="space-y-4">
                        {[
                            { id: 'name_weight', label: 'Name Match', color: 'range-success' },
                            { id: 'phone_weight', label: 'Phone Match', color: 'range-info' },
                            { id: 'email_weight', label: 'Email Match', color: 'range-info' },
                            { id: 'natid_weight', label: 'National ID', color: 'range-warning' },
                            { id: 'dob_weight', label: 'Date of Birth', color: 'range-warning' },
                            { id: 'address_weight', label: 'Address Match', color: 'range-error' },
                        ].map((field) => (
                            <div key={field.id}>
                                <div className="flex justify-between mb-1">
                                    <label className="text-xs text-gray-600 dark:text-gray-300 uppercase font-semibold">{field.label}</label>
                                    <span className="text-xs font-mono text-gray-900 dark:text-white">{config.scoring[field.id]}</span>
                                </div>
                                <input
                                    type="range"
                                    min="0" max="2.0" step="0.05"
                                    value={config.scoring[field.id] || 0}
                                    onChange={(e) => updateNested('scoring', field.id, e.target.value)}
                                    className={`range range-xs ${field.color}`}
                                />
                            </div>
                        ))}
                    </div>
                    <div className="mt-4 p-3 bg-gray-100 dark:bg-gray-900/50 rounded text-xs text-gray-500 dark:text-gray-400">
                        Weights determine impact on final match score (0.0 - 1.0).
                        Higher weights mean that field is more important.
                    </div>
                </motion.div>
            </div>
        </div>
    );
}
