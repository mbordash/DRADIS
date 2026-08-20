# ── Stage 1: install dependencies ─────────────────────────────────────────────
FROM node:20-alpine AS deps
RUN apk add --no-cache libc6-compat
WORKDIR /app
COPY package.json package-lock.json* ./
RUN npm ci

# ── Stage 2: build the Next.js app ────────────────────────────────────────────
FROM node:20-alpine AS builder
RUN apk add --no-cache libc6-compat
WORKDIR /app

COPY --from=deps /app/node_modules ./node_modules
COPY . .
# public/ is optional in Next.js but the final stage copies it unconditionally,
# and a COPY whose source does not exist is a hard build failure. It vanishes
# from a `git archive` build context (the AMI path) because git cannot track an
# empty directory and the .gitkeep marker is swallowed by a global ignore rule.
# Creating it here makes the build independent of how the context was produced.
RUN mkdir -p /app/public

# Build with standalone output (self-contained server.js)
# NEXT_PUBLIC_API_URL intentionally NOT set — the bundle uses relative '/api/*'
# paths so the Next.js rewrite proxy handles forwarding to DRADIS at runtime.
#
# NEXT_PUBLIC_DEMO_MODE is a BUILD-TIME arg: NEXT_PUBLIC_* vars are inlined into
# the browser bundle by `next build`, so the read-only demo banner / control
# gating must be baked here, not injected at runtime. Defaults to false so the
# normal (deploy-live) build is unchanged; deploy-demo.sh passes
# `--build-arg NEXT_PUBLIC_DEMO_MODE=true`.
ARG NEXT_PUBLIC_DEMO_MODE=false
ENV NEXT_PUBLIC_DEMO_MODE=$NEXT_PUBLIC_DEMO_MODE
ENV NEXT_TELEMETRY_DISABLED=1
RUN npm run build

# ── Stage 3: minimal production runner ────────────────────────────────────────
FROM node:20-alpine AS runner
RUN apk add --no-cache libc6-compat
WORKDIR /app

ENV NODE_ENV=production
ENV NEXT_TELEMETRY_DISABLED=1
ENV PORT=3002
ENV HOSTNAME=0.0.0.0

# Non-root user for security
RUN addgroup --system --gid 1001 nodejs \
 && adduser  --system --uid 1001 nextjs

# outputFileTracingRoot is NOT set in next.config.ts so standalone output is flat:
#   .next/standalone/server.js        ← entry point
#   .next/standalone/node_modules/    ← runtime deps (traced)
# Static assets and public/ must be copied alongside server.js.
COPY --from=builder --chown=nextjs:nodejs /app/.next/standalone            ./
COPY --from=builder --chown=nextjs:nodejs /app/.next/static                ./.next/static
# public/ may be empty but must exist for Next.js standalone runtime
RUN mkdir -p ./public
COPY --from=builder --chown=nextjs:nodejs /app/public/                     ./public/

USER nextjs
EXPOSE 3002

# DRADIS_API_URL is injected at runtime:
#   docker run -e DRADIS_API_URL=http://dradis-btc:9000 ...
CMD ["node", "server.js"]
