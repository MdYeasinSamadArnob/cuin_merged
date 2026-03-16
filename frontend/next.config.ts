import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  // Allow all origins for Server Actions and general access in development
  experimental: {
    serverActions: {
      allowedOrigins: ["*"],
    },
  },
  // Allow cross-origin requests in development
  allowedDevOrigins: ["*"],
  async headers() {
    return [
      {
        source: "/:path*",
        headers: [
          { key: "Access-Control-Allow-Origin", value: "*" },
          { key: "Access-Control-Allow-Methods", value: "GET,OPTIONS,PATCH,DELETE,POST,PUT" },
          { key: "Access-Control-Allow-Headers", value: "X-CSRF-Token, X-Requested-With, Accept, Accept-Version, Content-Length, Content-MD5, Content-Type, Date, X-Api-Version" },
        ]
      }
    ]
  },
  webpack: (config, { dev }) => {
    if (dev) {
      config.watchOptions = {
        ...config.watchOptions,
        poll: 1000,
        aggregateTimeout: 300,
        ignored: ['**/node_modules/**', '**/.git/**', '**/backend/**']
      };
    }
    return config;
  }
};

export default nextConfig;
