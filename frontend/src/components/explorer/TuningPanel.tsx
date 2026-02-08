import React, { useState } from 'react';
import { Play, Save, RotateCcw, ChevronLeft, Sliders } from 'lucide-react';

interface TuningPanelProps {
    onPreview: (config: any) => void;
    onSave?: (config: any) => void;
    loading: boolean;
    isOpen: boolean;
    setIsOpen: (isOpen: boolean) => void;
}

export function TuningPanel({ onPreview, onSave, loading, isOpen, setIsOpen }: TuningPanelProps) {
    // Default weights based on current backend defaults
    const [weights, setWeights] = useState({
        name: 1.0,
        dob: 1.0,
        email: 1.0,
        phone: 1.0,
        address: 0.5,
        city: 0.5
    });

    const [thresholds, setThresholds] = useState({
        autoLink: 0.95,
        review: 0.75
    });

    const [message, setMessage] = useState<string | null>(null);

    const handleChange = (field: string, value: number) => {
        setWeights(prev => ({ ...prev, [field]: value }));
    };

    const handleThresholdChange = (field: string, value: number) => {
        setThresholds(prev => ({ ...prev, [field]: value }));
    };

    const getCurrentConfig = () => ({
        auto_link_threshold: thresholds.autoLink,
        review_threshold: thresholds.review,
        name_weight: weights.name,
        dob_weight: weights.dob,
        email_weight: weights.email,
        phone_weight: weights.phone,
        address_weight: weights.address,
        natid_weight: 0.15
    });

    const handleApply = () => {
        onPreview(getCurrentConfig());
    };

    const handleSave = () => {
        if (onSave) {
            onSave(getCurrentConfig());
            setMessage('Configuration saved!');
            setTimeout(() => setMessage(null), 3000);
        }
    };

    if (!isOpen) {
        return (
            <button
                onClick={() => setIsOpen(true)}
                className="p-2.5 bg-gray-800/90 hover:bg-gray-700 text-white rounded-lg border border-gray-600 shadow-xl backdrop-blur transition-all hover:scale-105"
                title="Open Tuning Configuration"
            >
                <Sliders size={20} />
            </button>
        );
    }

    return (
        <div className="w-80 max-h-[660px] bg-gray-800/95 backdrop-blur border border-gray-700 rounded-xl p-4 flex flex-col gap-6 overflow-y-auto shadow-2xl">
            <button
                onClick={() => setIsOpen(false)}
                className="absolute top-2 right-2 p-1.5 text-gray-400 hover:text-white hover:bg-gray-700 rounded-lg transition-colors"
                title="Close Panel"
            >
                <ChevronLeft size={16} />
            </button>

            <div className="mt-2">
                <h3 className="text-white font-medium mb-1">Tuning & Weights</h3>
                <p className="text-xs text-gray-400">Adjust matching importance</p>
            </div>

            {/* Thresholds */}
            <div className="space-y-4">
                <h4 className="text-xs font-semibold text-gray-500 uppercase tracking-wider">Decision Thresholds</h4>
                
                <div className="space-y-2">
                    <div className="flex justify-between text-sm">
                        <span className="text-gray-300">Auto-Link</span>
                        <span className="text-emerald-400 font-mono">{thresholds.autoLink.toFixed(2)}</span>
                    </div>
                    <input
                        type="range"
                        min="0.5"
                        max="1.0"
                        step="0.01"
                        value={thresholds.autoLink}
                        onChange={(e) => handleThresholdChange('autoLink', parseFloat(e.target.value))}
                        className="w-full accent-emerald-500"
                    />
                </div>

                <div className="space-y-2">
                    <div className="flex justify-between text-sm">
                        <span className="text-gray-300">Review</span>
                        <span className="text-yellow-400 font-mono">{thresholds.review.toFixed(2)}</span>
                    </div>
                    <input
                        type="range"
                        min="0.0"
                        max="0.9"
                        step="0.01"
                        value={thresholds.review}
                        onChange={(e) => handleThresholdChange('review', parseFloat(e.target.value))}
                        className="w-full accent-yellow-500"
                    />
                </div>
            </div>

            <hr className="border-gray-700" />

            {/* Weights */}
            <div className="space-y-4">
                <h4 className="text-xs font-semibold text-gray-500 uppercase tracking-wider">Field Weights</h4>
                
                {Object.entries(weights).map(([field, weight]) => (
                    <div key={field} className="space-y-1">
                        <div className="flex justify-between text-sm">
                            <span className="text-gray-300 capitalize">{field}</span>
                            <span className="text-blue-400 font-mono">{weight.toFixed(1)}</span>
                        </div>
                        <input
                            type="range"
                            min="0"
                            max="2"
                            step="0.1"
                            value={weight}
                            onChange={(e) => handleChange(field, parseFloat(e.target.value))}
                            className="w-full accent-blue-500"
                        />
                    </div>
                ))}
            </div>

            <div className="mt-auto pt-4 border-t border-gray-700 space-y-3">
                <button
                    onClick={handleApply}
                    disabled={loading}
                    className="w-full flex items-center justify-center gap-2 px-4 py-2 bg-blue-600 hover:bg-blue-500 text-white rounded-lg transition-colors disabled:opacity-50"
                >
                    {loading ? (
                        <span className="animate-spin">⌛</span>
                    ) : (
                        <Play size={16} />
                    )}
                    Run Preview
                </button>
                
                {message && (
                    <div className="text-xs text-green-400 text-center animate-pulse">{message}</div>
                )}

                <div className="grid grid-cols-2 gap-2">
                    <button 
                        onClick={handleSave}
                        className="flex items-center justify-center gap-2 px-3 py-2 bg-gray-700 hover:bg-gray-600 text-gray-300 rounded-lg text-sm"
                    >
                        <Save size={14} />
                        Save
                    </button>
                    <button 
                        onClick={() => {
                            setWeights({ name: 1.0, dob: 1.0, email: 1.0, phone: 1.0, address: 0.5, city: 0.5 });
                            setThresholds({ autoLink: 0.95, review: 0.75 });
                        }}
                        className="flex items-center justify-center gap-2 px-3 py-2 bg-gray-700 hover:bg-gray-600 text-gray-300 rounded-lg text-sm"
                    >
                        <RotateCcw size={14} />
                        Reset
                    </button>
                </div>
            </div>
        </div>
    );
}
