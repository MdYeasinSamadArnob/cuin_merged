# ============================================
# CUIN v2 Frontend - Dockerfile
# ============================================
# Multi-stage build for Next.js production
# ============================================

# Stage 1: Dependencies
FROM node:20-alpine AS deps
RUN apk add --no-cache libc6-compat
WORKDIR /app

# Copy package files
COPY package*.json ./
# Full install, not --only=production: `next build` needs devDependencies
# (typescript, tailwindcss, @types/*) to compile at all. This doesn't
# bloat the final image -- the runner stage below only copies
# .next/standalone (which Next.js already bundles required runtime deps
# into) and .next/static, never node_modules from this stage directly.
RUN npm ci

# Stage 2: Builder
FROM node:20-alpine AS builder
WORKDIR /app

COPY --from=deps /app/node_modules ./node_modules
COPY . .

# Next.js inlines NEXT_PUBLIC_* variables into the client bundle at BUILD
# TIME, not at container start -- there is no way to change them later
# without rebuilding. They must be set to whatever address the BROWSER
# will use to reach the backend (e.g. the host's LAN IP), never an
# internal Docker service name like "http://backend:8000", which only
# resolves inside the compose network and is unreachable from a browser.
ARG NEXT_PUBLIC_API_URL=http://localhost:8000
ARG NEXT_PUBLIC_WS_URL=ws://localhost:8000/ws
ENV NEXT_PUBLIC_API_URL=${NEXT_PUBLIC_API_URL}
ENV NEXT_PUBLIC_WS_URL=${NEXT_PUBLIC_WS_URL}

# Build the application
ENV NEXT_TELEMETRY_DISABLED=1
RUN npm run build

# Stage 3: Runner
FROM node:20-alpine AS runner
WORKDIR /app

ENV NODE_ENV=production
ENV NEXT_TELEMETRY_DISABLED=1

# Create non-root user for security
RUN addgroup --system --gid 1001 nodejs
RUN adduser --system --uid 1001 nextjs

# Copy built assets
COPY --from=builder /app/public ./public
COPY --from=builder --chown=nextjs:nodejs /app/.next/standalone ./
COPY --from=builder --chown=nextjs:nodejs /app/.next/static ./.next/static

USER nextjs

EXPOSE 3000

ENV PORT=3000
ENV HOSTNAME="0.0.0.0"

CMD ["node", "server.js"]
