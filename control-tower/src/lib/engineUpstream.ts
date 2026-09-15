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
 * Server-side only: where the engine is and how to authenticate to it.
 *
 * The same rules as the catch-all proxy in `app/api/[...path]/route.ts`: the
 * engine URL and API key are runtime env vars that never reach the browser, and
 * the admin session token arrives as X-Admin-Token and is forwarded as a Bearer
 * token for the engine's admin gate.
 */
import type { NextRequest } from 'next/server';

export const ENGINE_API_BASE = process.env.DRADIS_API_URL ?? 'http://127.0.0.1:9000';

export function engineHeaders(req: NextRequest): Record<string, string> {
  const headers: Record<string, string> = {};
  const apiKey = process.env.DRADIS_API_KEY ?? '';
  if (apiKey) headers['X-API-Key'] = apiKey;
  const adminToken = req.headers.get('x-admin-token');
  if (adminToken) headers['Authorization'] = `Bearer ${adminToken}`;
  return headers;
}
