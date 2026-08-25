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

import { Fragment, useEffect, useMemo, useState } from 'react';
import useSWR from 'swr';
import {
  LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer,
  ReferenceLine, Brush,
} from 'recharts';
import { getTelemetryHistory, getTelemetryAssets } from '@/lib/api';
import type { TelemetrySample } from '@/lib/types';
import type { VenueId } from '@/lib/setupApi';

const POLL_MS = 2000;            // server samples at 2s — match it while live
const SAMPLES_PER_MIN = 30;      // 60s / 2s

const ASSET_EMOJI: Record<string, string> = { btc: '₿', eth: 'Ξ', sol: '◎' };

const WINDOWS: { mins: number; label: string }[] = [
  { mins: 5,  label: '5m' },
  { mins: 15, label: '15m' },
  { mins: 30, label: '30m' },
  { mins: 60, label: '1h' },
];

// ── Chart-ready row derived from a server TelemetrySample ─────────────────────
interface Row {
  t: number;
  time: string;
  oracle: number;
  v5: number;
  v1: number;
  accel: number;
  d60: number;
  d10: number;
  funding: number;   // percent
  oi: number;        // open interest (base contracts)
  oiDelta: number;   // percent change vs previous poll
  cvd: number;       // taker buy/sell ratio (1.0 = balanced)
  pulse: number;     // institutional pulse (signed z-score)
  coherence: number; // 0..1 agreement
  ibitBps: number;   // per-ETF premium (bps)
  fbtcBps: number;
  arkbBps: number;
  tideOpen: boolean; // US cash session live
  // Horizon Raptor
  tradfiVel: number;   // SPY+QQQ 5s velocity
  macroCoh: number;    // BTC/QQQ correlation
  vix: number;         // UVXY price
  vixVel: number;      // UVXY 5s velocity
  horizonOpen: boolean; // US cash session
}

function fmtClock(ms: number): string {
  return new Date(ms).toLocaleTimeString('en-US', {
    hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false,
  });
}

// Sports telemetry now spans days (de-duplicated ~2h polls), so a seconds-level
// clock is ambiguous. Label these points with month/day + HH:MM instead.
function fmtDayClock(ms: number): string {
  return new Date(ms).toLocaleString('en-US', {
    month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit', hour12: false,
  });
}

// Render an ISO-8601 kickoff time as a compact local "Sat 8:10 PM" label.
function fmtKickoff(iso: string): string {
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return iso;
  return d.toLocaleString('en-US', {
    weekday: 'short', hour: 'numeric', minute: '2-digit', hour12: true,
  });
}

// rust_decimal::Decimal serializes to JSON as a *string* ("64000.5"), so every
// numeric signal arrives here as a string despite the TelemetrySample type. Coerce
// at the boundary — otherwise chart/stat formatters call .toFixed() on a string and
// crash the whole page.
const num = (v: unknown): number => {
  const n = typeof v === 'number' ? v : parseFloat(v as string);
  return Number.isFinite(n) ? n : 0;
};

function toRow(s: TelemetrySample): Row {
  return {
    t: Number(s.t),
    time: fmtClock(Number(s.t)),
    oracle: num(s.oracle_price),
    v5: num(s.velocity_5s),
    v1: num(s.velocity_1s),
    accel: num(s.acceleration),
    d60: num(s.drift_60m),
    d10: num(s.drift_10m),
    funding: num(s.funding_rate) * 100,
    oi: num(s.open_interest),
    oiDelta: num(s.oi_delta_pct) * 100,
    cvd: num(s.cvd_ratio),
    pulse: num(s.institutional_pulse),
    coherence: num(s.tide_coherence),
    ibitBps: num(s.ibit_premium_bps),
    fbtcBps: num(s.fbtc_premium_bps),
    arkbBps: num(s.arkb_premium_bps),
    tideOpen: !!s.tide_market_open,
    // Horizon Raptor
    tradfiVel: num(s.tradfi_velocity),
    macroCoh: num(s.macro_coherence),
    vix: num(s.vix_proxy),
    vixVel: num(s.vix_velocity),
    horizonOpen: !!s.horizon_market_open,
  };
}

// The Sports Raptor is venue-neutral and polls on its own (~2h) cadence; its telemetry
// is de-duplicated server-side, so it gets its own sparse, multi-day chart row type
// independent of the crypto asset samples.
interface SportsRow {
  t: number;
  time: string;
  consensus: number;   // vig-free consensus implied prob (0..1)
  drift: number;       // Δ consensus vs previous poll (signed)
  dispersion: number;  // spread of per-book implied probs (0..1)
  numBooks: number;    // bookmakers in the sample
}

function toSportsRow(s: TelemetrySample): SportsRow {
  return {
    t: Number(s.t),
    time: fmtDayClock(Number(s.t)),
    consensus: num(s.sports_consensus_prob),
    drift: num(s.sports_line_drift),
    dispersion: num(s.sports_book_dispersion),
    numBooks: num(s.sports_num_books),
  };
}

// The Tennis Raptor is the other slow, venue-neutral poller (900s default), so
// it gets the same sparse multi-day row treatment as the Sports feed.
interface TennisRow {
  t: number;
  time: string;
  gamesP1: number;    // games won in the CURRENT set
  gamesP2: number;
  setsP1: number;     // sets won in the match
  setsP2: number;
  feedAge: number;    // seconds since the score last moved (-1 → 0, unknown)
}

function toTennisRow(s: TelemetrySample): TennisRow {
  const age = num(s.tennis_feed_age_secs);
  return {
    t: Number(s.t),
    time: fmtDayClock(Number(s.t)),
    gamesP1: num(s.tennis_games_p1),
    gamesP2: num(s.tennis_games_p2),
    setsP1: num(s.tennis_sets_p1),
    setsP2: num(s.tennis_sets_p2),
    // -1 means "no timestamp on the score", not "zero seconds old". Plot it as
    // 0 so the unknown case cannot masquerade as a fresher-than-real feed.
    feedAge: age < 0 ? 0 : age,
  };
}

// Player series colors. Amber/sky rather than the emerald/sky used elsewhere:
// the two sides of a match are the one place on this page where two series must
// stay tellable apart under color-vision deficiency, and emerald↔sky separates
// by only ΔE 3.0 under tritanopia, versus 27.5 for this pair.
const P1_COLOR = '#f59e0b';
const P2_COLOR = '#38bdf8';

// Mirrors config::TENNIS_SCORE_STALENESS_SECS. Drawn as a reference line so the
// chart shows *why* the raptor flips to disconnected, rather than the pill just
// going dark with no visible cause.
const TENNIS_STALENESS_SECS = 600;

// ── Signal-graph card ─────────────────────────────────────────────────────────

interface SeriesDef<R> { key: keyof R; label: string; color: string }

function SignalChart<R extends { time: string }>({
  title, subtitle, data, series, fmtY, zeroLine = false, refY, refLabel,
  lineType = 'monotone',
}: {
  title: string;
  subtitle: string;
  data: R[];
  series: SeriesDef<R>[];
  fmtY: (v: number) => string;
  zeroLine?: boolean;
  /** Optional horizontal baseline (e.g. 1.0 for a balanced CVD ratio). */
  refY?: number;
  refLabel?: string;
  /**
   * Interpolation between samples. Continuous measures curve ('monotone');
   * counts that only ever change by whole steps — games, sets — must use
   * 'stepAfter', or the curve draws values the score never held (3.5 games)
   * and implies the change happened gradually between polls.
   */
  lineType?: 'monotone' | 'stepAfter';
}) {
  const latest = data[data.length - 1];
  return (
    <div className="card p-4">
      <div className="flex items-start justify-between mb-2">
        <div>
          <p className="label-muted text-[10px]">{title}</p>
          <p className="text-[10px] text-gray-600 font-mono">{subtitle}</p>
        </div>
        <div className="flex items-center gap-3 text-[10px] font-mono">
          {series.map(s => (
            <div key={String(s.key)} className="flex items-center gap-1.5">
              <span className="w-3 h-0.5 inline-block" style={{ background: s.color }} />
              <span className="text-gray-500">{s.label}</span>
              {latest && <span className="text-gray-300">{fmtY(latest[s.key] as number)}</span>}
            </div>
          ))}
        </div>
      </div>
      <div style={{ height: 200 }}>
        {data.length < 2 ? (
          <div className="h-full flex items-center justify-center text-gray-600 text-xs">
            Collecting samples…
          </div>
        ) : (
          <ResponsiveContainer width="100%" height="100%">
            <LineChart data={data} syncId="telemetry" margin={{ top: 6, right: 12, bottom: 0, left: 0 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#1e1e32" vertical={false} />
              <XAxis
                dataKey="time"
                tick={{ fill: '#6b7280', fontSize: 10, fontFamily: 'monospace' }}
                tickLine={false}
                axisLine={{ stroke: '#1e1e32' }}
                interval="preserveStartEnd"
                minTickGap={40}
              />
              <YAxis
                tick={{ fill: '#6b7280', fontSize: 10, fontFamily: 'monospace' }}
                tickLine={false}
                axisLine={false}
                tickFormatter={fmtY}
                width={60}
                domain={['auto', 'auto']}
              />
              <Tooltip
                contentStyle={{
                  background: '#0d0d1a', border: '1px solid #1e1e32',
                  borderRadius: 8, fontSize: 11, fontFamily: 'monospace',
                }}
                labelStyle={{ color: '#9ca3af' }}
                formatter={(v, name) => [fmtY(Number(v)), String(name)]}
              />
              {zeroLine && <ReferenceLine y={0} stroke="#374151" strokeDasharray="4 4" />}
              {typeof refY === 'number' && (
                <ReferenceLine
                  y={refY}
                  stroke="#4b5563"
                  strokeDasharray="4 4"
                  label={refLabel ? { value: refLabel, position: 'insideTopLeft', fill: '#6b7280', fontSize: 9 } : undefined}
                />
              )}
              {series.map(s => (
                <Line
                  key={String(s.key)}
                  type={lineType}
                  dataKey={s.key as string}
                  name={s.label}
                  stroke={s.color}
                  strokeWidth={1.8}
                  dot={false}
                  isAnimationActive={false}
                />
              ))}
            </LineChart>
          </ResponsiveContainer>
        )}
      </div>
    </div>
  );
}

// ── Overview scrubber (only shown when paused) ────────────────────────────────

function Scrubber({
  data, range, onChange,
}: {
  data: Row[];
  range: { startIndex: number; endIndex: number };
  onChange: (r: { startIndex: number; endIndex: number }) => void;
}) {
  return (
    <div className="card p-3">
      <p className="label-muted text-[10px] mb-1">Scrub window — drag the handles to inspect a past interval</p>
      <div style={{ height: 70 }}>
        <ResponsiveContainer width="100%" height="100%">
          <LineChart data={data} margin={{ top: 4, right: 12, bottom: 0, left: 0 }}>
            <YAxis hide domain={['auto', 'auto']} />
            <Line type="monotone" dataKey="oracle" stroke="#10b981" strokeWidth={1.2} dot={false} isAnimationActive={false} />
            <Brush
              dataKey="time"
              height={22}
              travellerWidth={8}
              stroke="#6366f1"
              fill="#13131f"
              startIndex={range.startIndex}
              endIndex={range.endIndex}
              // eslint-disable-next-line @typescript-eslint/no-explicit-any
              onChange={(r: any) => {
                if (typeof r?.startIndex === 'number' && typeof r?.endIndex === 'number') {
                  onChange({ startIndex: r.startIndex, endIndex: r.endIndex });
                }
              }}
              tickFormatter={() => ''}
            />
          </LineChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}

// ── Small UI bits ─────────────────────────────────────────────────────────────

function AssetSelector({
  assets, selected, onChange,
}: { assets: string[]; selected: string; onChange: (a: string) => void }) {
  if (assets.length <= 1) return null;
  return (
    <div className="flex items-center gap-1">
      {assets.map(a => {
        const active = a === selected;
        return (
          <button
            key={a}
            onClick={() => onChange(a)}
            className={[
              'flex items-center gap-1.5 text-xs font-mono px-3 py-1.5 rounded-lg border transition-colors',
              active
                ? 'bg-indigo-500/20 border-indigo-500/40 text-indigo-300'
                : 'bg-[#13131f] border-[#1e1e32] text-gray-500 hover:border-gray-600 hover:text-gray-300',
            ].join(' ')}
          >
            <span>{ASSET_EMOJI[a] ?? '◈'}</span>
            <span>{a.toUpperCase()}</span>
          </button>
        );
      })}
    </div>
  );
}

function ConnPill({ label, live }: { label: string; live: boolean }) {
  return (
    <div className="flex items-center gap-1.5 text-[10px] font-mono">
      <span className={`h-2 w-2 rounded-full ${live ? 'bg-green-400 animate-pulse' : 'bg-red-500'}`} />
      <span className={live ? 'text-green-400' : 'text-red-400'}>{label}</span>
    </div>
  );
}

function StatCard({ label, value, valueClass = '' }: { label: string; value: string; valueClass?: string }) {
  return (
    <div className="card px-4 py-3 flex flex-col gap-1">
      <span className="label-muted">{label}</span>
      <span className={`stat-value ${valueClass}`}>{value}</span>
    </div>
  );
}

function fmtSigned(n: number): string {
  const sign = n > 0 ? '+' : '';
  return `${sign}${n.toFixed(2)}`;
}

// Compact large magnitudes (open interest) → "12.3K", "1.2M".
function fmtCompact(n: number): string {
  const abs = Math.abs(n);
  if (abs >= 1e9) return `${(n / 1e9).toFixed(2)}B`;
  if (abs >= 1e6) return `${(n / 1e6).toFixed(2)}M`;
  if (abs >= 1e3) return `${(n / 1e3).toFixed(2)}K`;
  return n.toFixed(2);
}

// ── Tide Raptor — Institutional Pulse card (BTC-only) ─────────────────────────

function fmtBps(n: number): string {
  return `${n >= 0 ? '+' : ''}${n.toFixed(1)} bps`;
}

function TideCard({ data, latest }: { data: Row[]; latest: Row }) {
  const open = latest.tideOpen;
  const pulse = latest.pulse;
  const coherence = latest.coherence;

  // Greyed/idle styling when the US cash session is closed: premiums are stale
  // and the pulse is intentionally held at 0.
  const dim = open ? '' : 'opacity-50';
  const pulseClass = !open
    ? 'text-gray-500'
    : pulse > 0 ? 'text-green-400' : pulse < 0 ? 'text-red-400' : 'text-gray-400';

  // Coherence drives conviction: high agreement = trust the pulse.
  const cohClass = !open
    ? 'text-gray-500'
    : coherence >= 0.66 ? 'text-green-400' : coherence >= 0.34 ? 'text-amber-400' : 'text-gray-400';

  const etf = (label: string, bps: number) => (
    <div className="card px-3 py-2 flex flex-col gap-0.5">
      <span className="label-muted text-[10px]">{label}</span>
      <span className={`font-mono text-sm ${!open ? 'text-gray-500' : bps > 0 ? 'text-green-400' : bps < 0 ? 'text-red-400' : 'text-gray-400'}`}>
        {open ? fmtBps(bps) : '—'}
      </span>
    </div>
  );

  return (
    <div className="card p-4 border border-indigo-500/20">
      <div className="flex items-start justify-between mb-3">
        <div>
          <p className="label-muted text-[10px]">🌊 Institutional Pulse · Tide Raptor</p>
          <p className="text-[10px] text-gray-600 font-mono">
            Spot-BTC-ETF premium vs synthetic iNAV — IBIT / FBTC / ARKB · live: Convergence · GBoost · Basis
          </p>
        </div>
        <div className="flex items-center gap-1.5 text-[10px] font-mono">
          <span className={`h-2 w-2 rounded-full ${open ? 'bg-green-400 animate-pulse' : 'bg-gray-600'}`} />
          <span className={open ? 'text-green-400' : 'text-gray-500'}>
            {open ? 'US SESSION OPEN' : 'MARKET CLOSED'}
          </span>
        </div>
      </div>

      <div className={`grid grid-cols-2 sm:grid-cols-5 gap-3 ${dim}`}>
        <div className="card px-3 py-2 flex flex-col gap-0.5">
          <span className="label-muted text-[10px]">Pulse (Iₚ)</span>
          <span className={`font-mono text-lg ${pulseClass}`}>
            {open ? `${pulse >= 0 ? '+' : ''}${pulse.toFixed(2)}σ` : '—'}
          </span>
        </div>
        <div className="card px-3 py-2 flex flex-col gap-0.5">
          <span className="label-muted text-[10px]">Coherence (C)</span>
          <span className={`font-mono text-lg ${cohClass}`}>
            {open ? coherence.toFixed(2) : '—'}
          </span>
        </div>
        {etf('IBIT', latest.ibitBps)}
        {etf('FBTC', latest.fbtcBps)}
        {etf('ARKB', latest.arkbBps)}
      </div>

      <div className="mt-3" style={{ height: 160 }}>
        {data.length < 2 ? (
          <div className="h-full flex items-center justify-center text-gray-600 text-xs">
            {open ? 'Collecting samples…' : 'Pulse resumes at the US cash open (09:30 ET)'}
          </div>
        ) : (
          <ResponsiveContainer width="100%" height="100%">
            <LineChart data={data} syncId="telemetry" margin={{ top: 6, right: 12, bottom: 0, left: 0 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#1e1e32" vertical={false} />
              <XAxis
                dataKey="time"
                tick={{ fill: '#6b7280', fontSize: 10, fontFamily: 'monospace' }}
                tickLine={false}
                axisLine={{ stroke: '#1e1e32' }}
                interval="preserveStartEnd"
                minTickGap={40}
              />
              <YAxis
                tick={{ fill: '#6b7280', fontSize: 10, fontFamily: 'monospace' }}
                tickLine={false}
                axisLine={false}
                tickFormatter={(v: number) => v.toFixed(1)}
                width={44}
                domain={['auto', 'auto']}
              />
              <Tooltip
                contentStyle={{ background: '#0d0d1a', border: '1px solid #1e1e32', borderRadius: 8, fontSize: 11, fontFamily: 'monospace' }}
                labelStyle={{ color: '#9ca3af' }}
              />
              <ReferenceLine y={0} stroke="#374151" strokeDasharray="4 4" />
              <Line type="monotone" dataKey="pulse" name="pulse σ" stroke="#818cf8" strokeWidth={1.8} dot={false} isAnimationActive={false} />
              <Line type="monotone" dataKey="coherence" name="coherence" stroke="#22d3ee" strokeWidth={1.2} dot={false} isAnimationActive={false} />
            </LineChart>
          </ResponsiveContainer>
        )}
      </div>
    </div>
  );
}

// ── Horizon Raptor — TradFi Velocity / VIX Proxy card (BTC-only) ──────────────

function HorizonCard({ data, latest }: { data: Row[]; latest: Row }) {
  const open = latest.horizonOpen;
  const tradfiVel = latest.tradfiVel;
  const macroCoh = latest.macroCoh;
  const vix = latest.vix;
  const vixVel = latest.vixVel;

  const dim = open ? '' : 'opacity-50';
  const velClass = !open
    ? 'text-gray-500'
    : tradfiVel > 0 ? 'text-green-400' : tradfiVel < 0 ? 'text-red-400' : 'text-gray-400';

  // Macro coherence: high positive = BTC tracking tech, low = decoupled
  const cohClass = !open
    ? 'text-gray-500'
    : macroCoh >= 0.5 ? 'text-green-400' : macroCoh >= 0 ? 'text-amber-400' : 'text-red-400';

  // VIX velocity: spikes indicate panic
  const vixVelClass = !open
    ? 'text-gray-500'
    : vixVel > 0.5 ? 'text-red-400' : vixVel < -0.5 ? 'text-green-400' : 'text-gray-400';

  return (
    <div className="card p-4 border border-amber-500/20">
      <div className="flex items-start justify-between mb-3">
        <div>
          <p className="label-muted text-[10px]">🌅 TradFi Velocity · Horizon Raptor</p>
          <p className="text-[10px] text-gray-600 font-mono">
            SPY + QQQ momentum · BTC/QQQ correlation · UVXY VIX proxy · live: Maker · TrendReversal gates
          </p>
        </div>
        <div className="flex items-center gap-1.5 text-[10px] font-mono">
          <span className={`h-2 w-2 rounded-full ${open ? 'bg-green-400 animate-pulse' : 'bg-gray-600'}`} />
          <span className={open ? 'text-green-400' : 'text-gray-500'}>
            {open ? 'US SESSION OPEN' : 'MARKET CLOSED'}
          </span>
        </div>
      </div>

      <div className={`grid grid-cols-2 sm:grid-cols-4 gap-3 ${dim}`}>
        <div className="card px-3 py-2 flex flex-col gap-0.5">
          <span className="label-muted text-[10px]">TradFi Vel</span>
          <span className={`font-mono text-lg ${velClass}`}>
            {open ? `${tradfiVel >= 0 ? '+' : ''}${tradfiVel.toFixed(3)}` : '—'}
          </span>
        </div>
        <div className="card px-3 py-2 flex flex-col gap-0.5">
          <span className="label-muted text-[10px]">Macro Cₘ</span>
          <span className={`font-mono text-lg ${cohClass}`}>
            {open ? macroCoh.toFixed(2) : '—'}
          </span>
        </div>
        <div className="card px-3 py-2 flex flex-col gap-0.5">
          <span className="label-muted text-[10px]">VIX (UVXY)</span>
          <span className="font-mono text-lg text-amber-400">
            {open && vix > 0 ? `$${vix.toFixed(2)}` : '—'}
          </span>
        </div>
        <div className="card px-3 py-2 flex flex-col gap-0.5">
          <span className="label-muted text-[10px]">VIX Vel</span>
          <span className={`font-mono text-lg ${vixVelClass}`}>
            {open ? `${vixVel >= 0 ? '+' : ''}${vixVel.toFixed(3)}` : '—'}
          </span>
        </div>
      </div>

      <div className="mt-3" style={{ height: 160 }}>
        {data.length < 2 ? (
          <div className="h-full flex items-center justify-center text-gray-600 text-xs">
            {open ? 'Collecting samples…' : 'TradFi velocity resumes at the US cash open (09:30 ET)'}
          </div>
        ) : (
          <ResponsiveContainer width="100%" height="100%">
            <LineChart data={data} syncId="telemetry" margin={{ top: 6, right: 12, bottom: 0, left: 0 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#1e1e32" vertical={false} />
              <XAxis
                dataKey="time"
                tick={{ fill: '#6b7280', fontSize: 10, fontFamily: 'monospace' }}
                tickLine={false}
                axisLine={{ stroke: '#1e1e32' }}
                interval="preserveStartEnd"
                minTickGap={40}
              />
              <YAxis
                tick={{ fill: '#6b7280', fontSize: 10, fontFamily: 'monospace' }}
                tickLine={false}
                axisLine={false}
                tickFormatter={(v: number) => v.toFixed(2)}
                width={44}
                domain={['auto', 'auto']}
              />
              <Tooltip
                contentStyle={{ background: '#0d0d1a', border: '1px solid #1e1e32', borderRadius: 8, fontSize: 11, fontFamily: 'monospace' }}
                labelStyle={{ color: '#9ca3af' }}
              />
              <ReferenceLine y={0} stroke="#374151" strokeDasharray="4 4" />
              <Line type="monotone" dataKey="tradfiVel" name="TradFi vel" stroke="#f59e0b" strokeWidth={1.8} dot={false} isAnimationActive={false} />
              <Line type="monotone" dataKey="macroCoh" name="macro Cₘ" stroke="#22d3ee" strokeWidth={1.2} dot={false} isAnimationActive={false} />
            </LineChart>
          </ResponsiveContainer>
        )}
      </div>
    </div>
  );
}

// ── Main telemetry page ───────────────────────────────────────────────────────

// ── Asset-class sub-navigation ────────────────────────────────────────────────

type TelemetryClass = 'crypto' | 'sports' | 'politics';

const TELEMETRY_CLASSES: { id: TelemetryClass; label: string; icon: string; ready: boolean }[] = [
  { id: 'crypto',   label: 'Crypto',   icon: '₿',  ready: true },
  { id: 'sports',   label: 'Sports',   icon: '🏟️', ready: true },
  { id: 'politics', label: 'Politics', icon: '🗳️', ready: false },
];

function ClassNav({
  active, onChange, classes,
}: {
  active: TelemetryClass;
  onChange: (c: TelemetryClass) => void;
  classes: typeof TELEMETRY_CLASSES;
}) {
  return (
    <div className="flex items-center gap-1.5">
      {classes.map(c => {
        const isActive = c.id === active;
        return (
          <button
            key={c.id}
            disabled={!c.ready}
            onClick={() => c.ready && onChange(c.id)}
            title={c.ready ? undefined : 'Coming soon'}
            className={[
              'flex items-center gap-1.5 text-xs font-mono px-3 py-1.5 rounded-lg border transition-colors',
              isActive
                ? 'bg-indigo-500/20 border-indigo-500/40 text-indigo-300'
                : c.ready
                  ? 'bg-[#13131f] border-[#1e1e32] text-gray-500 hover:border-gray-600 hover:text-gray-300'
                  : 'bg-[#13131f] border-[#1e1e32] text-gray-700 cursor-not-allowed opacity-60',
            ].join(' ')}
          >
            <span>{c.icon}</span>
            <span>{c.label}</span>
            {!c.ready && <span className="text-[9px] text-gray-600">soon</span>}
          </button>
        );
      })}
    </div>
  );
}

export default function TelemetryPage({ availableAssets, venue }: { availableAssets: string[]; venue?: VenueId }) {
  // US builds run a crypto wing (Polymarket US lists crypto markets), so the
  // Crypto tab stays visible — but Sports remains the default landing tab.
  //
  // Deliberately `=== 'us'` and not "any non-intl venue": Kalshi's default
  // series (KALSHI_SERIES) are crypto contracts, so a Kalshi instance should
  // land on Crypto like an intl one does.
  const isUs = venue === 'us';
  const classes = TELEMETRY_CLASSES;
  // Use raptor-specific asset list (crypto underlyings only) rather than the
  // full DB pool list which may include venue-only entries (e.g. "kalshi").
  const { data: telemetryAssets } = useSWR('telemetry-assets', getTelemetryAssets, { refreshInterval: 30_000 });
  const assets = telemetryAssets?.length ? telemetryAssets : (availableAssets.length ? availableAssets : ['btc']);
  const [selectedAsset, setSelectedAsset] = useState<string>('');
  const asset = selectedAsset || assets[0];

  const [windowMins, setWindowMins] = useState(15);
  const [live, setLive] = useState(true);
  const [range, setRange] = useState<{ startIndex: number; endIndex: number } | null>(null);
  const [assetClass, setAssetClass] = useState<TelemetryClass>(isUs ? 'sports' : 'crypto');
  // setupStatus loads async — if the venue resolves to US after mount, move the
  // initial crypto default over to the US landing tab (Sports) once.
  const [usDefaultApplied, setUsDefaultApplied] = useState(false);
  useEffect(() => {
    if (isUs && !usDefaultApplied) {
      setUsDefaultApplied(true);
      if (assetClass === 'crypto') setAssetClass('sports');
    }
  }, [isUs, usDefaultApplied, assetClass]);

  const limit = windowMins * SAMPLES_PER_MIN;

  const { data: samples, error } = useSWR(
    ['telemetry-history', asset, limit],
    () => getTelemetryHistory(asset, limit),
    { refreshInterval: live ? POLL_MS : 0, revalidateOnFocus: false, keepPreviousData: true },
  );

  // The Sports Raptor is venue-neutral and publishes under a fixed "sports" key,
  // independent of the selected crypto asset. It polls every ~2h and its telemetry is
  // de-duplicated server-side (one point per change/heartbeat), so a modest fixed
  // request spans many days of readable movement regardless of the crypto window.
  const { data: sportsSamples } = useSWR(
    ['telemetry-history', 'sports', 288],
    () => getTelemetryHistory('sports', 288),
    { refreshInterval: live ? POLL_MS : 0, revalidateOnFocus: false, keepPreviousData: true },
  );
  const sportsLast = sportsSamples && sportsSamples.length > 0
    ? sportsSamples[sportsSamples.length - 1]
    : undefined;
  const sportsRows = useMemo<SportsRow[]>(
    () => (sportsSamples ?? []).map(toSportsRow),
    [sportsSamples],
  );

  // The Tennis Raptor publishes under its own fixed "tennis" health key, on the
  // same slow cadence as Sports, so it is fetched as a sibling series here.
  const { data: tennisSamples } = useSWR(
    ['telemetry-history', 'tennis', 288],
    () => getTelemetryHistory('tennis', 288),
    { refreshInterval: live ? POLL_MS : 0, revalidateOnFocus: false, keepPreviousData: true },
  );
  const tennisLast = tennisSamples && tennisSamples.length > 0
    ? tennisSamples[tennisSamples.length - 1]
    : undefined;
  const tennisRows = useMemo<TennisRow[]>(
    () => (tennisSamples ?? []).map(toTennisRow),
    [tennisSamples],
  );
  // The feed labels the tracked match "A vs B"; split it so the chart legends
  // carry the actual players rather than an anonymous "p1"/"p2".
  const [tennisP1, tennisP2] = useMemo(() => {
    const parts = (tennisLast?.tennis_match ?? '').split(' vs ');
    return [parts[0]?.trim() || 'player 1', parts[1]?.trim() || 'player 2'];
  }, [tennisLast?.tennis_match]);

  const rows = useMemo<Row[]>(() => (samples ?? []).map(toRow), [samples]);

  // When pausing, seed the scrub range to the full loaded window; clear on resume.
  useEffect(() => {
    if (!live && rows.length > 1 && range === null) {
      setRange({ startIndex: 0, endIndex: rows.length - 1 });
    }
    if (live && range !== null) setRange(null);
  }, [live, rows.length, range]);

  // Detail charts show the scrubbed slice when paused, else the full window.
  const viewRows = useMemo<Row[]>(() => {
    if (!live && range) {
      const end = Math.min(range.endIndex, rows.length - 1);
      const start = Math.max(0, Math.min(range.startIndex, end));
      return rows.slice(start, end + 1);
    }
    return rows;
  }, [rows, live, range]);

  const latest = rows[rows.length - 1];
  const lastSample = samples && samples.length > 0 ? samples[samples.length - 1] : undefined;
  const spanSecs = rows.length >= 2 ? Math.round((rows[rows.length - 1].t - rows[0].t) / 1000) : 0;

  return (
    <div className="space-y-5">
      {/* Asset-class sub-navigation */}
      <div className="flex items-center gap-3 flex-wrap">
        <p className="label-muted text-xs">📡 Telemetry</p>
        <ClassNav active={assetClass} onChange={setAssetClass} classes={classes} />
      </div>

      {assetClass === 'crypto' && (
      <div className="space-y-5">
      {/* Header / intro + controls */}
      <div className="card px-5 py-4 border border-indigo-500/20 bg-[#0d0d1a]">
        <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3">
          <div>
            <p className="label-muted text-xs">📡 Raptor Signal Telemetry</p>
            <p className="text-sm text-gray-400 mt-0.5">
              Live signal collectors -- watch the data streams to understand what your vipers see —
              <span className="text-gray-500"> from spot micro-structure up to perp macro pressure.</span>
            </p>
          </div>
          <AssetSelector assets={assets} selected={asset} onChange={setSelectedAsset} />
        </div>

        <div className="flex flex-wrap items-center gap-3 mt-3 pt-3 border-t border-[#1e1e32]">
          <ConnPill label="Price Raptor" live={!!lastSample?.price_connected} />
          <ConnPill label="Funding Raptor" live={!!lastSample?.funding_connected} />
          <ConnPill label="Derivatives Raptor" live={!!lastSample?.deriv_connected} />
          {asset === 'btc' && (
            <ConnPill label="Tide Raptor" live={!!lastSample?.tide_connected} />
          )}
          {asset === 'btc' && (
            <ConnPill label="Horizon Raptor" live={!!lastSample?.horizon_connected} />
          )}

          {/* Window selector */}
          <div className="flex items-center gap-1 ml-2">
            {WINDOWS.map(w => {
              const active = w.mins === windowMins;
              return (
                <button
                  key={w.mins}
                  onClick={() => setWindowMins(w.mins)}
                  className={[
                    'text-[11px] font-mono px-2.5 py-1 rounded border transition-colors',
                    active
                      ? 'bg-indigo-500/20 border-indigo-500/40 text-indigo-300'
                      : 'bg-[#13131f] border-[#1e1e32] text-gray-500 hover:border-gray-600 hover:text-gray-300',
                  ].join(' ')}
                >
                  {w.label}
                </button>
              );
            })}
          </div>

          {/* Live / Pause toggle */}
          <button
            onClick={() => setLive(v => !v)}
            className={[
              'flex items-center gap-1.5 text-[11px] font-mono px-3 py-1 rounded-lg border transition-colors',
              live
                ? 'bg-green-500/10 border-green-500/30 text-green-300 hover:bg-green-500/20'
                : 'bg-amber-500/10 border-amber-500/30 text-amber-300 hover:bg-amber-500/20',
            ].join(' ')}
          >
            <span className={`h-2 w-2 rounded-full ${live ? 'bg-green-400 animate-pulse' : 'bg-amber-400'}`} />
            <span>{live ? 'LIVE' : 'PAUSED'}</span>
          </button>

          <span className="text-[10px] text-gray-600 font-mono ml-auto">
            {rows.length} samples · {spanSecs}s loaded · {POLL_MS / 1000}s cadence
          </span>
        </div>
      </div>

      {error && (
        <div className="card px-4 py-3 border border-red-500/30 bg-red-500/5 text-red-300 text-xs font-mono">
          Failed to reach /api/telemetry/history — is the engine running?
        </div>
      )}

      {/* Current-value stat strip */}
      {latest && (
        <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-6 gap-3">
          <StatCard label="Oracle Price" value={`$${latest.oracle.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`} />
          <StatCard label="Velocity (5s)" value={fmtSigned(latest.v5)} valueClass={latest.v5 >= 0 ? 'text-green-400' : 'text-red-400'} />
          <StatCard label="Drift (10m)" value={fmtSigned(latest.d10)} valueClass={latest.d10 >= 0 ? 'text-green-400' : 'text-red-400'} />
          <StatCard label="Funding Rate" value={`${latest.funding >= 0 ? '+' : ''}${latest.funding.toFixed(4)}%`} valueClass={latest.funding >= 0 ? 'text-green-400' : 'text-red-400'} />
          <StatCard label="Open Interest Δ" value={`${latest.oiDelta >= 0 ? '+' : ''}${latest.oiDelta.toFixed(3)}%`} valueClass={latest.oiDelta >= 0 ? 'text-green-400' : 'text-red-400'} />
          <StatCard
            label="Taker CVD"
            value={latest.cvd > 0 ? latest.cvd.toFixed(3) : '—'}
            valueClass={latest.cvd === 0 ? 'text-gray-500' : latest.cvd >= 1 ? 'text-green-400' : 'text-red-400'}
          />
        </div>
      )}

      {/* Scrubber — only when paused */}
      {!live && range && rows.length > 1 && (
        <Scrubber data={rows} range={range} onChange={setRange} />
      )}

      {/* Institutional Pulse — Tide Raptor (BTC-only, consumed by Convergence/GBoost/Basis) */}
      {asset === 'btc' && latest && (
        <TideCard data={viewRows} latest={latest} />
      )}

      {/* TradFi Velocity — Horizon Raptor (BTC-only, consumed by Maker/TrendReversal gates) */}
      {asset === 'btc' && latest && (
        <HorizonCard data={viewRows} latest={latest} />
      )}

      {/* Signal charts */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        <SignalChart
          title="Oracle Price"
          subtitle="Binance Spot WS — current mark"
          data={viewRows}
          series={[{ key: 'oracle', label: 'price', color: '#10b981' }]}
          fmtY={v => `$${Math.round(v).toLocaleString('en-US')}`}
        />
        <SignalChart
          title="Velocity & Acceleration"
          subtitle="Δprice over 5s / 1s windows + accel"
          data={viewRows}
          zeroLine
          series={[
            { key: 'v5', label: '5s', color: '#6366f1' },
            { key: 'v1', label: '1s', color: '#22d3ee' },
            { key: 'accel', label: 'accel', color: '#f59e0b' },
          ]}
          fmtY={v => fmtSigned(v)}
        />
        <SignalChart
          title="Drift"
          subtitle="Δprice over 60m / 10m — medium-term trend"
          data={viewRows}
          zeroLine
          series={[
            { key: 'd60', label: '60m', color: '#a855f7' },
            { key: 'd10', label: '10m', color: '#ec4899' },
          ]}
          fmtY={v => fmtSigned(v)}
        />
        <SignalChart
          title="Funding Rate"
          subtitle="Binance perpetual — smart-money lean"
          data={viewRows}
          zeroLine
          series={[{ key: 'funding', label: 'rate', color: '#14b8a6' }]}
          fmtY={v => `${v.toFixed(4)}%`}
        />
        <SignalChart
          title="Open Interest Δ"
          subtitle="Binance perp OI change — 10m regime pressure"
          data={viewRows}
          zeroLine
          series={[{ key: 'oiDelta', label: 'ΔOI', color: '#f97316' }]}
          fmtY={v => `${v >= 0 ? '+' : ''}${v.toFixed(3)}%`}
        />
        <SignalChart
          title="Taker CVD Ratio"
          subtitle="Perp buy÷sell aggression — >1 buyers lifting, <1 sellers hitting"
          data={viewRows}
          refY={1}
          refLabel="balanced"
          series={[{ key: 'cvd', label: 'ratio', color: '#eab308' }]}
          fmtY={v => v.toFixed(3)}
        />
      </div>

      {/* Footer note */}
      <p className="text-[10px] font-mono text-gray-600">
        History is served from the engine ring buffer (<span className="text-gray-500">/api/telemetry/history</span>),
        so it survives page reloads. Pick a window, then <span className="text-gray-500">Pause</span> to scrub a past
        interval. Positive velocity/drift = price rising; funding &gt; 0 = longs paying shorts (bullish lean).
        The macro Derivatives Raptor adds perp context: rising <span className="text-gray-500">Open Interest Δ</span>
        {' '}with price = fresh positioning, while <span className="text-gray-500">Taker CVD</span> &gt; 1 marks buy-side
        aggression — your vipers fuse these slow macro reads with the fast spot micro signals.
      </p>
      </div>
      )}

      {assetClass === 'sports' && (
      <div className="space-y-5">
        <div className="card px-5 py-4 border border-emerald-500/20 bg-[#0d0d1a]">
          <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-2 mb-3">
            <div>
              <p className="label-muted text-xs">🏟️ Sports Raptor — Cross-Book Consensus</p>
              <p className="text-[11px] text-gray-500 mt-0.5">
                Venue-neutral observe-only feed (The Odds API). Vig-free consensus of the nearest
                priced event — shared by every deployed squadron across every venue.
              </p>
            </div>
            <div className="flex items-center gap-3 text-[10px] font-mono shrink-0">
              <ConnPill label="Sports Raptor" live={!!sportsLast?.sports_connected} />
              <span className="text-gray-500">
                books <span className="text-gray-300">{num(sportsLast?.sports_num_books).toFixed(0)}</span>
              </span>
            </div>
          </div>

          {/* Which event / outcome / books the numbers describe */}
          {sportsLast?.sports_connected && sportsLast?.sports_event ? (
            <div className="mb-4 rounded-lg border border-[#1e1e32] bg-[#0a0a14] px-4 py-3">
              <div className="flex flex-wrap items-baseline gap-x-2 gap-y-1">
                {sportsLast.sports_sport && (
                  <span className="text-[10px] font-mono px-1.5 py-0.5 rounded bg-emerald-500/15 text-emerald-300 border border-emerald-500/25">
                    {sportsLast.sports_sport}
                  </span>
                )}
                <span className="text-sm text-gray-200 font-medium">{sportsLast.sports_event}</span>
                {sportsLast.sports_commence && (
                  <span className="text-[11px] font-mono text-gray-500">
                    · {fmtKickoff(sportsLast.sports_commence)}
                  </span>
                )}
              </div>
              <p className="text-[11px] text-gray-500 mt-1.5">
                Consensus is the vig-free implied probability that{' '}
                <span className="text-emerald-300 font-mono">
                  {sportsLast.sports_reference || 'the reference outcome'}
                </span>{' '}
                wins — currently{' '}
                <span className="text-gray-200 font-mono">
                  {(num(sportsLast.sports_consensus_prob) * 100).toFixed(1)}%
                </span>.
              </p>
              {sportsLast.sports_books && (
                <p className="text-[10px] font-mono text-gray-600 mt-1">
                  <span className="text-gray-500">{num(sportsLast.sports_num_books).toFixed(0)} books:</span>{' '}
                  {sportsLast.sports_books}
                </p>
              )}
            </div>
          ) : (
            <div className="mb-4 rounded-lg border border-[#1e1e32] bg-[#0a0a14] px-4 py-3 text-[11px] font-mono text-gray-600">
              No priced event yet — the raptor tracks the nearest upcoming game with live odds
              (polls every ~2h to stay inside the free-tier budget).
            </div>
          )}

          <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
            <SignalChart<SportsRow>
              title="Consensus Probability"
              subtitle="Vig-free implied prob of reference outcome (0–1)"
              data={sportsRows}
              series={[{ key: 'consensus', label: 'consensus', color: '#34d399' }]}
              fmtY={v => v.toFixed(3)}
            />
            <SignalChart<SportsRow>
              title="Line Drift & Book Dispersion"
              subtitle="Δconsensus vs prior poll (signed) + cross-book spread"
              data={sportsRows}
              zeroLine
              series={[
                { key: 'drift', label: 'drift', color: '#f59e0b' },
                { key: 'dispersion', label: 'dispersion', color: '#38bdf8' },
              ]}
              fmtY={v => fmtSigned(v)}
            />
          </div>
        </div>

        <p className="text-[10px] font-mono text-gray-600">
          The Sports Raptor observes only — no Viper trades on it yet. It polls The Odds API on a
          slow (~2h) cadence to stay inside the free-tier budget (~500 requests/month), so the trend
          fills in gradually. <span className="text-gray-500">Consensus</span> is the vig-free cross-book
          implied probability of the reference outcome; <span className="text-gray-500">drift</span> is its
          move since the prior poll; <span className="text-gray-500">dispersion</span> is how much the
          books disagree — a proxy for soft, potentially mispriced lines.
        </p>

        {/* ── Tennis Raptor — live event state ────────────────────────────── */}
        <div className="card px-5 py-4 border border-sky-500/20 bg-[#0d0d1a]">
          <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-2 mb-3">
            <div>
              <p className="label-muted text-xs">🎾 Tennis Raptor — Live Event State</p>
              <p className="text-[11px] text-gray-500 mt-0.5">
                Venue-neutral observe-only feed (Live Tennis API). One tracked live match —
                sets, games, serving side and a derived break-point flag.
              </p>
            </div>
            <div className="flex items-center gap-3 text-[10px] font-mono shrink-0">
              <ConnPill label="Tennis Raptor" live={!!tennisLast?.tennis_connected} />
              <span className="text-gray-500">
                live <span className="text-gray-300">{num(tennisLast?.tennis_num_live).toFixed(0)}</span>
              </span>
            </div>
          </div>

          {/* Live scoreboard for the tracked match */}
          {tennisLast?.tennis_connected && tennisLast?.tennis_match ? (
            <div className="mb-4 rounded-lg border border-[#1e1e32] bg-[#0a0a14] px-4 py-3">
              <div className="flex flex-wrap items-baseline gap-x-2 gap-y-1">
                {tennisLast.tennis_tour && (
                  <span className="text-[10px] font-mono px-1.5 py-0.5 rounded bg-sky-500/15 text-sky-300 border border-sky-500/25 uppercase">
                    {tennisLast.tennis_tour}
                  </span>
                )}
                <span className="text-sm text-gray-200 font-medium">{tennisLast.tennis_match}</span>
                {tennisLast.tennis_tournament && (
                  <span className="text-[11px] font-mono text-gray-500">
                    · {tennisLast.tennis_tournament}
                  </span>
                )}
                {tennisLast.tennis_is_tiebreak && (
                  <span className="text-[10px] font-mono px-1.5 py-0.5 rounded bg-violet-500/15 text-violet-300 border border-violet-500/25">
                    TIEBREAK
                  </span>
                )}
                {tennisLast.tennis_break_point && (
                  <span className="text-[10px] font-mono px-1.5 py-0.5 rounded bg-rose-500/15 text-rose-300 border border-rose-500/25">
                    BREAK POINT
                  </span>
                )}
              </div>

              {/* Score grid — one row per player, color-keyed to the charts.
                  The serving side carries a ● marker, so "who is serving" is
                  never conveyed by color alone. */}
              <div className="mt-2.5 grid grid-cols-[1fr_auto_auto_auto] gap-x-4 gap-y-1 text-[11px] font-mono max-w-md">
                <span className="text-gray-600" />
                <span className="text-gray-600 text-right">sets</span>
                <span className="text-gray-600 text-right">games</span>
                <span className="text-gray-600 text-right">pts</span>

                {([
                  [tennisP1, P1_COLOR, 1, num(tennisLast.tennis_sets_p1), num(tennisLast.tennis_games_p1), 0],
                  [tennisP2, P2_COLOR, 2, num(tennisLast.tennis_sets_p2), num(tennisLast.tennis_games_p2), 1],
                ] as const).map(([name, color, side, sets, games, ptIdx]) => (
                  <Fragment key={side}>
                    <span className="flex items-center gap-1.5 text-gray-300 truncate">
                      <span className="w-2 h-2 rounded-sm shrink-0" style={{ background: color }} />
                      {name}
                      {num(tennisLast.tennis_server) === side && (
                        <span className="text-emerald-400" title="serving">●</span>
                      )}
                    </span>
                    <span className="text-gray-200 text-right">{sets.toFixed(0)}</span>
                    <span className="text-gray-200 text-right">{games.toFixed(0)}</span>
                    <span className="text-gray-200 text-right">
                      {(tennisLast.tennis_points ?? '').split('–')[ptIdx] || '–'}
                    </span>
                  </Fragment>
                ))}
              </div>
            </div>
          ) : (
            <div className="mb-4 rounded-lg border border-[#1e1e32] bg-[#0a0a14] px-4 py-3 text-[11px] font-mono text-gray-600">
              {num(tennisLast?.tennis_num_live) > 0
                ? 'Live matches on court, but no score has been published yet.'
                : 'Nothing on court — tennis has quiet hours daily, which is a healthy state, not a fault. Set LIVETENNIS_API_KEY in Setup if the pill stays offline.'}
            </div>
          )}

          <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
            <SignalChart<TennisRow>
              title="Games — Current Set"
              subtitle="Games won in the set in progress"
              data={tennisRows}
              lineType="stepAfter"
              series={[
                { key: 'gamesP1', label: tennisP1, color: P1_COLOR },
                { key: 'gamesP2', label: tennisP2, color: P2_COLOR },
              ]}
              fmtY={v => v.toFixed(0)}
            />
            <SignalChart<TennisRow>
              title="Sets Won"
              subtitle="Match score in sets"
              data={tennisRows}
              lineType="stepAfter"
              series={[
                { key: 'setsP1', label: tennisP1, color: P1_COLOR },
                { key: 'setsP2', label: tennisP2, color: P2_COLOR },
              ]}
              fmtY={v => v.toFixed(0)}
            />
            <div className="lg:col-span-2">
              <SignalChart<TennisRow>
                title="Feed Age"
                subtitle="Seconds since the tracked score last moved"
                data={tennisRows}
                refY={TENNIS_STALENESS_SECS}
                refLabel={`stale > ${TENNIS_STALENESS_SECS}s`}
                series={[{ key: 'feedAge', label: 'feed age', color: '#9ca3af' }]}
                fmtY={v => `${v.toFixed(0)}s`}
              />
            </div>
          </div>
        </div>

        <p className="text-[10px] font-mono text-gray-600">
          The Tennis Raptor observes only — no Viper trades on it yet, and the tracked match is
          chosen for signal liveness (sticky on the previous match, else the freshest score), not
          because it maps to a listed market. It polls every ~15 min to stay inside the free tier
          (100 requests/day), so <span className="text-gray-500">games</span> and{' '}
          <span className="text-gray-500">sets</span> step rather than curve — the score only ever
          moves in whole units, and between polls it genuinely has no value.{' '}
          <span className="text-gray-500">Feed age</span> above the dashed line means the score has
          gone stale, and the raptor then reports disconnected so a consumer widens or pulls rather
          than holding on a frozen number.
        </p>
      </div>
      )}
    </div>
  );
}

