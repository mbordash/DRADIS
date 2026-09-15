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
 * Instance backup download (E64), streamed.
 *
 * The catch-all `/api/[...path]` proxy reads the engine's response as text and
 * labels it JSON, which would corrupt a binary archive and hold hundreds of
 * megabytes in memory. This route hands the engine's body through as a stream,
 * with its download headers.
 */
import type { NextRequest } from 'next/server';
import { ENGINE_API_BASE, engineHeaders } from '@/lib/engineUpstream';

export const runtime = 'nodejs';
export const dynamic = 'force-dynamic';

export async function GET(req: NextRequest) {
  try {
    const upstream = await fetch(`${ENGINE_API_BASE}/api/migration/archive`, {
      headers: engineHeaders(req),
      cache: 'no-store',
    });
    const headers = new Headers();
    for (const name of ['content-type', 'content-length', 'content-disposition']) {
      const value = upstream.headers.get(name);
      if (value) headers.set(name, value);
    }
    return new Response(upstream.body, { status: upstream.status, headers });
  } catch (err) {
    console.error('[migration] archive download could not reach the engine:', err);
    return Response.json({ error: 'DRADIS engine unreachable' }, { status: 503 });
  }
}
