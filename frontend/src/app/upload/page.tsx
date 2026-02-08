"use client";

import { useState, useEffect } from "react";

export default function UploadPage() {
    const [file, setFile] = useState<File | null>(null);
    const [uploading, setUploading] = useState(false);
    const [result, setResult] = useState<any>(null);
    const [error, setError] = useState<string | null>(null);

    const handleFileChange = (e: React.ChangeEvent<HTMLInputElement>) => {
        if (e.target.files) {
            setFile(e.target.files[0]);
        }
    };

    const handleUpload = async () => {
        if (!file) return;

        setUploading(true);
        setError(null);
        setResult(null);

        const formData = new FormData();
        formData.append("file", file);
        formData.append("run_mode", "AUTO");

        try {
            const response = await fetch(`${process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000'}/upload/file`, {
                method: "POST",
                body: formData,
            });

            if (!response.ok) {
                const data = await response.json();
                throw new Error(data.detail || "Upload failed");
            }

            const data = await response.json();
            setResult(data);
        } catch (err: any) {
            setError(err.message);
        } finally {
            setUploading(false);
        }
    };

    return (
        <div className="min-h-screen bg-gray-50 dark:bg-gray-950 py-12 px-4 sm:px-6 lg:px-8 transition-colors duration-300">
            <div className="max-w-md mx-auto bg-white dark:bg-gray-900 p-8 rounded-lg shadow-md">
                <h1 className="text-2xl font-bold mb-6 text-gray-900 dark:text-white">Upload Data</h1>

                <div className="mb-6">
                    <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
                        Select Data File (Excel or CSV)
                    </label>
                    <input
                        type="file"
                        accept=".xlsx, .xls, .csv"
                        onChange={handleFileChange}
                        className="block w-full text-sm text-gray-700 dark:text-gray-400
              file:mr-4 file:py-2 file:px-4
              file:rounded-full file:border-0
              file:text-sm file:font-semibold
              file:bg-blue-50 dark:file:bg-blue-900/30 file:text-blue-700 dark:file:text-blue-400
              hover:file:bg-blue-100 dark:hover:file:bg-blue-800/50"
                    />
                    <p className="mt-2 text-xs text-gray-500 dark:text-gray-400">
                        Supported formats: .xlsx, .xls, .csv
                    </p>
                </div>

                {error && (
                    <div className="mb-4 p-4 text-sm text-red-700 dark:text-red-400 bg-red-100 dark:bg-red-900/30 rounded-lg" role="alert">
                        {error}
                    </div>
                )}

                <button
                    onClick={handleUpload}
                    disabled={!file || uploading}
                    className={`w-full flex justify-center py-2 px-4 border border-transparent rounded-md shadow-sm text-sm font-medium text-white 
            ${!file || uploading ? 'bg-gray-400 dark:bg-gray-600 cursor-not-allowed' : 'bg-blue-600 hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500'}`}
                >
                    {uploading ? "Processing..." : "Upload & Analyze"}
                </button>

                {result && (
                    <div className="mt-8 text-center animate-pulse">
                        <h2 className="text-lg font-bold text-green-600 dark:text-green-400 mb-2">Upload Successful!</h2>
                        <p className="text-gray-600 dark:text-gray-400 mb-4">Initializing Agent Swarm...</p>
                        <div className="w-full bg-gray-200 dark:bg-gray-700 rounded-full h-2.5 mb-4">
                            <div className="bg-blue-600 h-2.5 rounded-full animate-progress" style={{ width: '100%' }}></div>
                        </div>
                        <a
                            href={`/runs/${result.run_id}`}
                            className="inline-flex items-center justify-center px-6 py-3 border border-transparent text-base font-medium rounded-md text-white bg-blue-600 hover:bg-blue-700 transition-all transform hover:scale-105"
                        >
                            Open Mission Control &rarr;
                        </a>

                        {/* Auto-redirect script effect */}
                        <RedirectEffect to={`/runs/${result.run_id}`} />
                    </div>
                )}
            </div>
        </div>
    );
}

function RedirectEffect({ to }: { to: string }) {
    useEffect(() => {
        const timer = setTimeout(() => {
            window.location.href = to;
        }, 1500);
        return () => clearTimeout(timer);
    }, [to]);
    return null;
}

