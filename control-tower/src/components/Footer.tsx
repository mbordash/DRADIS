'use client';

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

import useSWR from 'swr';
import { getLatency } from '@/lib/api';
import { getSetupStatus } from '@/lib/setupApi';
import { DEMO_MODE } from '@/lib/demo';

/**
 * Venue latency pill — rolling engine→venue round-trip measured server-side
 * (Polymarket CLOB on intl builds, US venue API on US builds).
 *
 * Deploy-distance troubleshooting aid: green means the server is close enough
 * to the venue, red means fills will lag and the instance should be moved to
 * a region nearer the venue.
 */
function LatencyMeter() {
  const { data } = useSWR('latency', getLatency, {
    refreshInterval: 15_000,
    revalidateOnFocus: false,
  });

  // Hide entirely until the engine has completed at least one probe.
  if (!data || !data.probed) return null;

  const ms = data.p50_ms ?? data.last_ms;
  const unreachable = !data.ok && ms === null;

  const color = unreachable || !data.ok
    ? 'text-red-400 border-red-500/30 bg-red-500/10'
    : ms !== null && ms < 150
      ? 'text-emerald-400 border-emerald-500/30 bg-emerald-500/10'
      : ms !== null && ms < 400
        ? 'text-amber-300 border-amber-500/30 bg-amber-500/10'
        : 'text-red-400 border-red-500/30 bg-red-500/10';

  const label = unreachable
    ? `${data.venue} UNREACHABLE`
    : `${data.venue} ${ms}ms${data.ok ? '' : ' ⚠'}`;

  return (
    <span
      className={`inline-flex items-center gap-1 px-2 py-0.5 rounded border text-[10px] font-mono ${color}`}
      title={`Round-trip from your DRADIS server to the trading venue (median of last ${data.samples} probes). High latency? Deploy your instance in a region closer to the venue.`}
    >
      <span>📶</span>
      <span>{label}</span>
    </span>
  );
}

/**
 * Running engine version, e.g. "v1.0.4".
 *
 * The instance itself carries no version label an operator can see: AWS
 * Marketplace launches the image into the buyer's own account, so a seller
 * cannot set the EC2 Name tag, and a one-click launch leaves it blank. That
 * matters most during an upgrade, where the documented procedure has the old
 * and new instances running at once and the operator has to stop the right one.
 * Naming the version here means two open tabs identify themselves regardless of
 * what the EC2 console calls them.
 *
 * Shares the `setupStatus` SWR key with the dashboard, so this costs no extra
 * request — SWR dedupes by key and the value is already cached. Skipped on the
 * public demo, which does not serve the setup endpoint.
 */
function EngineVersion() {
  const { data } = useSWR(!DEMO_MODE ? 'setupStatus' : null, getSetupStatus, {
    refreshInterval: 60_000,
    revalidateOnFocus: false,
  });

  if (!data?.app_version) return null;

  return (
    <span
      className="text-gray-600"
      title="DRADIS engine version running on this instance"
    >
      v{data.app_version}
    </span>
  );
}

/** Shared page footer: branding line + venue latency meter + engine version. */
export default function Footer() {
  return (
    <footer className="text-center text-xs text-gray-700 pb-4 font-mono space-y-2">
      <div>
        <LatencyMeter />
      </div>
      <div>
        DRADIS Control Tower  Polymarket CLOB Orchestrator{' '}
        <span className="text-gray-600">So say we all.</span>{' '}
        <EngineVersion />
      </div>
    </footer>
  );
}
