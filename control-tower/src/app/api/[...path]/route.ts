/**
 * Runtime API proxy — forwards all /api/* requests to the DRADIS engine.
 *
 * This replaces the next.config.ts rewrite approach. Rewrites are evaluated
 * at BUILD time, so DRADIS_API_URL is unset during `npm run build` and the
 * destination bakes in as localhost:9000 — which fails inside Docker when the
 * engine is on a different container (dradis-btc:9000).
 *
 * Route handlers run at REQUEST time on the Next.js server, so DRADIS_API_URL
 * is always the live runtime value injected by Docker / .env.local.
 *
 * Local dev:   DRADIS_API_URL=http://localhost:9000   (set in start-local.sh)
 * Docker:      DRADIS_API_URL=http://dradis-btc:9000  (set in deploy-multi.sh)
 */
import { NextRequest, NextResponse } from 'next/server';

const API_BASE = process.env.DRADIS_API_URL ?? 'http://localhost:9000';

async function proxy(req: NextRequest, path: string[]): Promise<NextResponse> {
  const url = new URL(req.url);
  const target = `${API_BASE}/api/${path.join('/')}${url.search}`;

  try {
    const upstream = await fetch(target, {
      method:  req.method,
      headers: { 'Content-Type': 'application/json' },
      body:    req.method !== 'GET' && req.method !== 'HEAD'
                 ? await req.text()
                 : undefined,
      // Don't cache — always live data
      cache: 'no-store',
    });

    const text = await upstream.text();
    return new NextResponse(text, {
      status:  upstream.status,
      headers: { 'Content-Type': 'application/json' },
    });
  } catch (err) {
    console.error(`[proxy] failed to reach ${target}:`, err);
    return NextResponse.json({ error: 'DRADIS engine unreachable' }, { status: 503 });
  }
}

export async function GET(
  req: NextRequest,
  { params }: { params: Promise<{ path: string[] }> },
) {
  const { path } = await params;
  return proxy(req, path);
}

export async function PATCH(
  req: NextRequest,
  { params }: { params: Promise<{ path: string[] }> },
) {
  const { path } = await params;
  return proxy(req, path);
}

export async function POST(
  req: NextRequest,
  { params }: { params: Promise<{ path: string[] }> },
) {
  const { path } = await params;
  return proxy(req, path);
}

