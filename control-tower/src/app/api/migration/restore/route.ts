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
import http from 'node:http';
import https from 'node:https';
import { Readable } from 'node:stream';
import type { ReadableStream as NodeWebReadableStream } from 'node:stream/web';
import { ENGINE_API_BASE, engineHeaders } from '@/lib/engineUpstream';
import { basicAuthFailure } from '@/lib/basicAuth';

export const runtime = 'nodejs';
export const dynamic = 'force-dynamic';

/**
 * Stream `body` to the engine and collect its (small JSON) answer.
 *
 * Node's built-in `http`, not `fetch`. Node's `fetch` waits at most 300
 * seconds for response headers, and the engine only answers once the whole
 * archive has arrived, so any upload slower than five minutes failed as
 * "engine unreachable" even with the server's own limit raised (see
 * `ct-server.js`). A plain request has no such deadline: the socket has no
 * timeout by default, and the engine is on the Docker network beside us.
 */
function streamToEngine(
  url: URL,
  headers: Record<string, string>,
  body: ReadableStream<Uint8Array> | null,
): Promise<{ status: number; text: string }> {
  return new Promise((resolve, reject) => {
    const transport = url.protocol === 'https:' ? https : http;
    const upstream = transport.request(url, { method: 'POST', headers }, (res) => {
      const chunks: Buffer[] = [];
      res.on('data', (chunk: Buffer) => chunks.push(chunk));
      res.on('end', () => resolve({ status: res.statusCode ?? 502, text: Buffer.concat(chunks).toString('utf8') }));
      res.on('error', reject);
    });
    upstream.on('error', reject);
    if (!body) {
      upstream.end();
      return;
    }
    // A browser that gives up mid-upload errors this stream; tear the engine
    // request down with it so the engine discards the partial archive.
    Readable.fromWeb(body as unknown as NodeWebReadableStream<Uint8Array>)
      .on('error', (err) => upstream.destroy(err))
      .pipe(upstream);
  });
}

export async function POST(req: NextRequest) {
  // This route is excluded from the Basic Auth middleware, because Next.js
  // truncates the cloned request body it hands to middleware at 10 MB. The same
  // check runs here instead, before a single byte is forwarded.
  const unauthorized = basicAuthFailure(req);
  if (unauthorized) return unauthorized;

  const search = new URL(req.url).search;
  try {
    const upstream = await streamToEngine(
      new URL(`${ENGINE_API_BASE}/api/migration/restore${search}`),
      { ...engineHeaders(req), 'Content-Type': 'application/gzip' },
      req.body,
    );
    return new Response(upstream.text, { status: upstream.status, headers: { 'Content-Type': 'application/json' } });
  } catch (err) {
    console.error('[migration] restore upload could not reach the engine:', err);
    return Response.json({ error: 'DRADIS engine unreachable' }, { status: 503 });
  }
}
