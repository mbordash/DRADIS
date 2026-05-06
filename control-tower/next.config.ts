import type { NextConfig } from 'next';
import path from 'path';

const nextConfig: NextConfig = {
  // Standalone output: produces a self-contained server.js for Docker
  output: 'standalone',

  // Silence monorepo lockfile warning — control-tower is a subdirectory of the DRADIS repo
  outputFileTracingRoot: path.join(__dirname, '../'),

  // Proxy /api/* to the DRADIS Rust binary.
  //   Dev:    uses NEXT_PUBLIC_API_URL from .env.local  (e.g. http://localhost:9000)
  //   Docker: uses DRADIS_API_URL injected at runtime   (e.g. http://dradis-btc:9000)
  //
  // The browser always calls same-origin /api/* so no CORS issues and no URL
  // baked into the client bundle — DRADIS_API_URL is a server-only variable.
  async rewrites() {
    const apiBase =
      process.env.DRADIS_API_URL ??
      process.env.NEXT_PUBLIC_API_URL ??
      'http://localhost:9000';
    return [
      {
        source:      '/api/:path*',
        destination: `${apiBase}/api/:path*`,
      },
    ];
  },
};

export default nextConfig;
