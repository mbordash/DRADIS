// SPDX-License-Identifier: AGPL-3.0-only
//
// DRADIS Control Tower — operator dashboard for the DRADIS trading engine.
// Copyright (C) 2026 Michael Bordash
//
// This file is part of DRADIS. DRADIS is free software: you can redistribute it
// and/or modify it under the terms of the GNU Affero General Public License,
// version 3, as published by the Free Software Foundation.
//
// DRADIS is distributed in the hope that it will be useful, but WITHOUT ANY
// WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
// A PARTICULAR PURPOSE. See the GNU Affero General Public License for details.
//
// You should have received a copy of the GNU Affero General Public License along
// with this program. If not, see <https://www.gnu.org/licenses/>.

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
 *
 * API key: if DRADIS_API_KEY is set (server-side env var, never sent to the
 * browser), it is forwarded as X-API-Key on every proxied request so external
 * tools like OpenClaw can be gated behind the same key without exposing it in
 * the client-side JS bundle.
 *
 * Authorization: the admin session token (Setup UI login) arrives from the
 * browser as X-Admin-Token — it can't use Authorization directly because CT
 * Basic Auth owns that header browser-side. We translate it to
 * Authorization: Bearer for the engine's /api/setup/* admin gate.
 */
import { NextRequest, NextResponse } from 'next/server';

const API_BASE = process.env.DRADIS_API_URL ?? 'http://127.0.0.1:9000';
// Server-side only — NOT NEXT_PUBLIC_ so it never appears in the browser bundle.
const API_KEY  = process.env.DRADIS_API_KEY ?? '';

async function proxy(req: NextRequest, path: string[]): Promise<NextResponse> {
  const url = new URL(req.url);
  const target = `${API_BASE}/api/${path.join('/')}${url.search}`;

  const headers: Record<string, string> = { 'Content-Type': 'application/json' };
  if (API_KEY) headers['X-API-Key'] = API_KEY;
  const adminToken = req.headers.get('x-admin-token');
  if (adminToken) headers['Authorization'] = `Bearer ${adminToken}`;

  try {
    const upstream = await fetch(target, {
      method:  req.method,
      headers,
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

export async function PUT(
  req: NextRequest,
  { params }: { params: Promise<{ path: string[] }> },
) {
  const { path } = await params;
  return proxy(req, path);
}

export async function DELETE(
  req: NextRequest,
  { params }: { params: Promise<{ path: string[] }> },
) {
  const { path } = await params;
  return proxy(req, path);
}

