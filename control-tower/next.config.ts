import type { NextConfig } from 'next';

const nextConfig: NextConfig = {
  // Build output directory, overridable per instance.
  //
  // Two `next dev` servers on this same project directory would otherwise share
  // .next and overwrite each other's artifacts — the running one starts serving
  // files that no longer exist and answers 500 with MODULE_NOT_FOUND. Soaking
  // two venues side by side needs a Control Tower each, so each gets its own
  // build directory. Unset (the default, Docker, CI) keeps plain `.next`.
  distDir: process.env.NEXT_DIST_DIR || '.next',

  // Standalone output: produces a self-contained server.js for Docker.
  // Note: outputFileTracingRoot is intentionally omitted — setting it to '../'
  // works locally but resolves to '/' inside Docker (/app/../ = /), causing
  // Next.js to nest the output under .next/standalone/app/ instead of
  // .next/standalone/ directly, breaking the CMD path.
  output: 'standalone',

  // API proxying is handled by the catch-all route handler at
  // src/app/api/[...path]/route.ts — which runs at REQUEST time and can
  // read DRADIS_API_URL as a true runtime env var.
  //
  // We do NOT use next.config.ts rewrites() here because rewrites() is
  // evaluated at BUILD time: DRADIS_API_URL is unset during `npm run build`,
  // so the destination bakes in as localhost:9000 and fails in Docker.
};

export default nextConfig;
