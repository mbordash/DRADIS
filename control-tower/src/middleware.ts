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
 * HTTP Basic Auth middleware for Control Tower.
 *
 * Triggered on every request except static assets.
 * Auth is SKIPPED entirely when CT_USERNAME / CT_PASSWORD are not set in the
 * environment — so local dev (start-local.sh) works with zero config.
 *
 * Production usage — set in your server's .env or Docker run command:
 *   CT_USERNAME=admin
 *   CT_PASSWORD=your-strong-password
 */
import { NextRequest, NextResponse } from 'next/server';

export function middleware(req: NextRequest) {
  const expectedUser = process.env.CT_USERNAME;
  const expectedPass = process.env.CT_PASSWORD;

  // No credentials configured → open access (local dev / intentional)
  if (!expectedUser || !expectedPass) {
    return NextResponse.next();
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
        return NextResponse.next();
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

export const config = {
  // Apply to every route except Next.js internals and static files
  matcher: ['/((?!_next/static|_next/image|favicon.ico).*)'],
};

