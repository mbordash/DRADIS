'use client';

import {
  AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip,
  ResponsiveContainer, ReferenceLine,
} from 'recharts';
import type { PnlSnapshotRow } from '@/lib/types';

interface Props {
  data: PnlSnapshotRow[];
  startingBalance?: number;
  /** When true, compute balance as startingBalance + session_pnl (ghost mode — on-chain balance is flat). */
  ghostMode?: boolean;
}

function fmt(iso: string) {
  return new Date(iso).toLocaleTimeString('en-US', {
    hour: '2-digit', minute: '2-digit', hour12: false,
  });
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function CustomTooltip({ active, payload, label }: any) {
  if (!active || !payload?.length) return null;
  const bal = payload[0]?.value as number;
  const pnl = payload[1]?.value as number;
  return (
    <div className="card px-3 py-2 text-xs font-mono space-y-1 shadow-xl">
      <div className="text-gray-400">{label}</div>
      <div className="text-white">Balance <span className="text-indigo-300">${bal?.toFixed(2)}</span></div>
      {pnl !== undefined && (
        <div className={pnl >= 0 ? 'text-green-400' : 'text-red-400'}>
          Session P&amp;L <span>{pnl >= 0 ? '+' : ''}{pnl?.toFixed(2)}</span>
        </div>
      )}
    </div>
  );
}

export default function PnlChart({ data, startingBalance, ghostMode }: Props) {
  // API returns newest-first — reverse for chronological chart display
  const base = startingBalance ?? 0;
  const chartData = [...data].reverse().map(row => ({
    time:    fmt(row.ts),
    // In ghost mode the on-chain collateral never changes, so derive virtual balance from session P&L.
    balance: ghostMode
      ? base + parseFloat(row.session_pnl)
      : parseFloat(row.collateral),
    pnl:     parseFloat(row.session_pnl),
  }));

  if (chartData.length === 0) {
    return (
      <div className="card p-6 flex items-center justify-center h-48 text-gray-600 text-sm">
        No balance data yet — snapshots are recorded every 60 s.
      </div>
    );
  }

  const minBal = Math.min(...chartData.map(d => d.balance));
  const maxBal = Math.max(...chartData.map(d => d.balance));
  const pad    = (maxBal - minBal) * 0.15 || 5;
  const domain = [Math.floor(minBal - pad), Math.ceil(maxBal + pad)];

  return (
    <div className="card p-4">
      <p className="label-muted mb-3">Portfolio Balance</p>
      <ResponsiveContainer width="100%" height={220}>
        <AreaChart data={chartData} margin={{ top: 4, right: 12, bottom: 0, left: 0 }}>
          <defs>
            <linearGradient id="balGrad" x1="0" y1="0" x2="0" y2="1">
              <stop offset="5%"  stopColor="#6366f1" stopOpacity={0.25} />
              <stop offset="95%" stopColor="#6366f1" stopOpacity={0}    />
            </linearGradient>
          </defs>
          <CartesianGrid strokeDasharray="3 3" stroke="#1e1e32" vertical={false} />
          <XAxis
            dataKey="time"
            tick={{ fill: '#6b7280', fontSize: 11, fontFamily: 'monospace' }}
            tickLine={false}
            axisLine={{ stroke: '#1e1e32' }}
            interval="preserveStartEnd"
          />
          <YAxis
            domain={domain}
            tick={{ fill: '#6b7280', fontSize: 11, fontFamily: 'monospace' }}
            tickLine={false}
            axisLine={false}
            tickFormatter={v => `$${v}`}
            width={60}
          />
          <Tooltip content={<CustomTooltip />} />
          {startingBalance !== undefined && (
            <ReferenceLine y={startingBalance} stroke="#374151" strokeDasharray="4 4" />
          )}
          <Area
            type="monotone"
            dataKey="balance"
            stroke="#6366f1"
            strokeWidth={2}
            fill="url(#balGrad)"
            dot={false}
            activeDot={{ r: 4, fill: '#6366f1', stroke: '#0a0a12', strokeWidth: 2 }}
          />
        </AreaChart>
      </ResponsiveContainer>
    </div>
  );
}

