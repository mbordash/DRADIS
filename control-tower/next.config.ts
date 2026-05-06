import type { NextConfig } from 'next';

const nextConfig: NextConfig = {
  // Standalone output: produces a self-contained server.js for Docker.
  // Note: outputFileTracingRoot is intentionally omitted — setting it to '../'
  // works locally but resolves to '/' inside Docker (/app/../ = /), causing
  // Next.js to nest the output under .next/standalone/app/ instead of
  // .next/standalone/ directly, breaking the CMD path.
  output: 'standalone',


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
