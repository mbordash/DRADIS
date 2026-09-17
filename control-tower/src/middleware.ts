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
import { basicAuthFailure } from '@/lib/basicAuth';

export function middleware(req: NextRequest) {
  return basicAuthFailure(req) ?? NextResponse.next();
}

export const config = {
  // Apply to every route except Next.js internals and the public icons (so the login
  // prompt's tab shows them before credentials are sent).
  //
  // `api/migration/restore` is excluded for a different reason: Next.js clones the
  // request body for every middleware-matched route and silently truncates that
  // clone at `middlewareClientMaxBodySize` (10 MB), which cut a 71 MB backup upload
  // down to 10 MB. The route checks Basic Auth itself, so it stays protected.
  // The `$` matters: without it the exclusion is a prefix and would also drop
  // `restore/apply` and `restore/discard`, which restart the engine, out of auth.
  matcher: [
    '/((?!_next/static|_next/image|favicon.ico|icon.svg|apple-icon.png|api/migration/restore$).*)',
  ],
};

