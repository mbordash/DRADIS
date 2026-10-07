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
 * Entry point for the Control Tower's standalone server.
 *
 * Next.js creates the HTTP server itself and exposes only `keepAliveTimeout`,
 * so Node's own defaults govern everything else, including `requestTimeout`:
 * 300 seconds since Node 18, after which Node answers "408 Request Timeout"
 * and drops the connection. A backup restore streams hundreds of megabytes
 * through this server. On 2026-10-06 the first migration to a Marketplace
 * instance uploaded a 247 MB backup over a home uplink well under 1 MB/s and
 * the operator got "upload failed: HTTP 408"; the backup was the only copy of
 * the old instance's history.
 *
 * This raises `requestTimeout` on every server Next creates, then hands over
 * to the generated `server.js`. Raising it for the whole server, rather than
 * the restore route alone, is safe because the Control Tower is never reached
 * directly: the AMI exposes it only on the Docker network, and nginx in front
 * applies its own per-read client timeouts, which is where protection against
 * a client dribbling bytes belongs.
 */
'use strict';

const http = require('node:http');

/**
 * Two hours: the largest restore nginx accepts (4 GB) at about 0.6 MB/s. Finite
 * on purpose, so a request that genuinely stalls is still eventually closed.
 */
const REQUEST_TIMEOUT_MS = 2 * 60 * 60 * 1000;

/**
 * Wrap `http.createServer` so every server it makes carries `timeoutMs` as its
 * `requestTimeout`. Accepts both call shapes Node does: `(listener)` and
 * `(options, listener)`.
 */
function withRequestTimeout(createServer, timeoutMs) {
  return function createServerWithRequestTimeout(options, listener) {
    if (typeof options === 'function') {
      listener = options;
      options = {};
    }
    return createServer.call(this, { ...(options || {}), requestTimeout: timeoutMs }, listener);
  };
}

module.exports = { withRequestTimeout, REQUEST_TIMEOUT_MS };

if (require.main === module) {
  http.createServer = withRequestTimeout(http.createServer, REQUEST_TIMEOUT_MS);
  require('./server.js');
}
