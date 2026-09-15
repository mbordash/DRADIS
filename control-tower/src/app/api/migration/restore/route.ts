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
 * Instance backup upload for restore (E64), streamed.
 *
 * The catch-all proxy buffers a request body with `req.text()`, which would
 * corrupt a binary archive. This route streams the upload straight to the
 * engine, which verifies it and stages it for the next restart. The engine's
 * answer is small JSON and is returned as-is, including a 409 that asks the
 * operator to confirm overwriting an instance that already has trades.
 *
 * `/api/migration/restore/apply` and `/discard` are ordinary JSON calls and
 * still go through the catch-all.
 */
import type { NextRequest } from 'next/server';
import { ENGINE_API_BASE, engineHeaders } from '@/lib/engineUpstream';

export const runtime = 'nodejs';
export const dynamic = 'force-dynamic';

export async function POST(req: NextRequest) {
  const search = new URL(req.url).search;
  try {
    const init: RequestInit & { duplex: 'half' } = {
      method: 'POST',
      headers: { ...engineHeaders(req), 'Content-Type': 'application/gzip' },
      body: req.body,
      // Node's fetch streams a request body only in half-duplex mode.
      duplex: 'half',
      cache: 'no-store',
    };
    const upstream = await fetch(`${ENGINE_API_BASE}/api/migration/restore${search}`, init);
    const text = await upstream.text();
    return new Response(text, { status: upstream.status, headers: { 'Content-Type': 'application/json' } });
  } catch (err) {
    console.error('[migration] restore upload could not reach the engine:', err);
    return Response.json({ error: 'DRADIS engine unreachable' }, { status: 503 });
  }
}
