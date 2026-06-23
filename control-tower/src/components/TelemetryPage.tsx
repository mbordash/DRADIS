'use client';

import { useEffect, useMemo, useState } from 'react';
import useSWR from 'swr';
import {
  LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer,
  ReferenceLine, Brush,
} from 'recharts';
import { getTelemetryHistory } from '@/lib/api';
import type { TelemetrySample } from '@/lib/types';

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
}

function fmtClock(ms: number): string {
  return new Date(ms).toLocaleTimeString('en-US', {
    hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false,
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
  };
}

// ── Signal-graph card ─────────────────────────────────────────────────────────

interface SeriesDef { key: keyof Row; label: string; color: string }

function SignalChart({
  title, subtitle, data, series, fmtY, zeroLine = false, refY, refLabel,
}: {
  title: string;
  subtitle: string;
  data: Row[];
  series: SeriesDef[];
  fmtY: (v: number) => string;
  zeroLine?: boolean;
  /** Optional horizontal baseline (e.g. 1.0 for a balanced CVD ratio). */
  refY?: number;
  refLabel?: string;
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
                formatter={(v: number, name: string) => [fmtY(v), name]}
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
                  type="monotone"
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

// ── Main telemetry page ───────────────────────────────────────────────────────

export default function TelemetryPage({ availableAssets }: { availableAssets: string[] }) {
  const assets = availableAssets.length ? availableAssets : ['btc'];
  const [selectedAsset, setSelectedAsset] = useState<string>('');
  const asset = selectedAsset || assets[0];

  const [windowMins, setWindowMins] = useState(15);
  const [live, setLive] = useState(true);
  const [range, setRange] = useState<{ startIndex: number; endIndex: number } | null>(null);

  const limit = windowMins * SAMPLES_PER_MIN;

  const { data: samples, error } = useSWR(
    ['telemetry-history', asset, limit],
    () => getTelemetryHistory(asset, limit),
    { refreshInterval: live ? POLL_MS : 0, revalidateOnFocus: false, keepPreviousData: true },
  );

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
      {/* Header / intro + controls */}
      <div className="card px-5 py-4 border border-indigo-500/20 bg-[#0d0d1a]">
        <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3">
          <div>
            <p className="label-muted text-xs">📡 Raptor Signal Telemetry</p>
            <p className="text-sm text-gray-400 mt-0.5">
              Live signal collectors feeding the squadrons. Watch the raw price, velocity,
              drift, funding and derivatives streams to understand what your vipers see —
              <span className="text-gray-500"> from spot micro-structure up to perp macro pressure.</span>
            </p>
          </div>
          <AssetSelector assets={assets} selected={asset} onChange={setSelectedAsset} />
        </div>

        <div className="flex flex-wrap items-center gap-3 mt-3 pt-3 border-t border-[#1e1e32]">
          <ConnPill label="Price Raptor" live={!!lastSample?.price_connected} />
          <ConnPill label="Funding Raptor" live={!!lastSample?.funding_connected} />
          <ConnPill label="Derivatives Raptor" live={!!lastSample?.deriv_connected} />

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

      <p className="text-[10px] font-mono text-gray-600">
        History is served from the engine ring buffer (<span className="text-gray-500">/api/telemetry/history</span>),
        so it survives page reloads. Pick a window, then <span className="text-gray-500">Pause</span> to scrub a past
        interval. Positive velocity/drift = price rising; funding &gt; 0 = longs paying shorts (bullish lean).
        The macro Derivatives Raptor adds perp context: rising <span className="text-gray-500">Open Interest Δ</span>
        {' '}with price = fresh positioning, while <span className="text-gray-500">Taker CVD</span> &gt; 1 marks buy-side
        aggression — your vipers fuse these slow macro reads with the fast spot micro signals.
      </p>
    </div>
  );
}

