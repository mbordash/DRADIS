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
 * HTTP Basic Auth for Control Tower, shared by `middleware.ts` and by the routes
 * that have to opt out of middleware.
 *
 * The restore upload cannot go through middleware: Next.js clones the request
 * body for any middleware-matched route and truncates that clone at
 * `middlewareClientMaxBodySize` (10 MB by default), silently, which cut a 71 MB
 * backup down to 10 MB mid-migration. The clone also ignores stream
 * backpressure, so simply raising the limit would buffer the whole archive in
 * memory twice. That route is excluded from the matcher instead and calls
 * `basicAuthFailure` itself, so excluding it costs no authentication.
 */
import { NextRequest, NextResponse } from 'next/server';

/**
 * Returns a 401 response when the request does not carry valid credentials, or
 * `null` when the request may proceed.
 *
 * Auth is skipped entirely when CT_USERNAME / CT_PASSWORD are not set, so local
 * dev (start-local.sh) works with zero config.
 */
export function basicAuthFailure(req: NextRequest): NextResponse | null {
  const expectedUser = process.env.CT_USERNAME;
  const expectedPass = process.env.CT_PASSWORD;

  // No credentials configured → open access (local dev / intentional)
  if (!expectedUser || !expectedPass) {
    return null;
  }

  const authHeader = req.headers.get('authorization') ?? '';
  if (authHeader.startsWith('Basic ')) {
    const encoded = authHeader.slice(6);
    const decoded = Buffer.from(encoded, 'base64').toString('utf-8');
    const colon   = decoded.indexOf(':');
    if (colon !== -1) {
      const user = decoded.slice(0, colon);
      const pass = decoded.slice(colon + 1);
      if (user === expectedUser && pass === expectedPass) {
        return null;
      }
    }
  }

  // Prompt the browser for credentials
  return new NextResponse('Unauthorized', {
    status: 401,
    headers: {
      'WWW-Authenticate': 'Basic realm="DRADIS Control Tower", charset="UTF-8"',
    },
  });
}
