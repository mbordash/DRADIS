'use client';

import {
  AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip,
  ResponsiveContainer, ReferenceLine,
} from 'recharts';
import type { PnlSnapshotRow, PortfolioValue } from '@/lib/types';

interface Props {
  data: PnlSnapshotRow[];
  startingBalance?: number;
  /** When true, compute balance as startingBalance + session_pnl (ghost mode — on-chain balance is flat). */
  ghostMode?: boolean;
  /** Current real-time portfolio value (used for the most recent data point). */
  currentPortfolio?: PortfolioValue;
}

function fmt(iso: string) {
  return new Date(iso).toLocaleTimeString('en-US', {
    hour: '2-digit', minute: '2-digit', hour12: false,
  });
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function CustomTooltip({ active, payload, label }: any) {
  if (!active || !payload?.length) return null;

  const cashData = payload.find((p: any) => p.dataKey === 'cash');
  const totalData = payload.find((p: any) => p.dataKey === 'totalValue');

  const cash = cashData?.value as number;
  const total = totalData?.value as number;
  const inPositions = total - cash;

  return (
    <div className="card px-3 py-2 text-xs font-mono space-y-1 shadow-xl">
      <div className="text-gray-400">{label}</div>
      <div className="text-white">
        Total Value <span className="text-emerald-300 font-semibold">${total?.toFixed(2)}</span>
      </div>
      <div className="text-indigo-300">
        Cash <span className="text-white">${cash?.toFixed(2)}</span>
      </div>
      {inPositions > 0 && (
        <div className="text-gray-500">
          In Positions <span className="text-white">${inPositions?.toFixed(2)}</span>
        </div>
      )}
    </div>
  );
}

export default function PnlChart({ data, startingBalance, ghostMode, currentPortfolio }: Props) {
  // API returns newest-first — reverse for chronological chart display
  const base = startingBalance ?? 0;
  const reversedData = [...data].reverse();

  const chartData = reversedData.map((row, index) => {
    const sessionPnl = parseFloat(row.session_pnl);
    const collateral = parseFloat(row.collateral);
    const isLatestPoint = index === reversedData.length - 1;

    // Cash: actual collateral (liquid funds)
    const cash = ghostMode ? base + sessionPnl : collateral;

    // Total portfolio value calculation:
    // For the latest point, use real-time data from /api/portfolio if available
    // For historical points, approximate as: starting balance + session P&L + deployed capital
    let totalValue: number;

    if (isLatestPoint && currentPortfolio) {
      // Use accurate real-time data for the most recent point
      totalValue = parseFloat(currentPortfolio.total_value);
    } else {
      // Historical approximation: start + realized gains
      // Note: This doesn't include unrealized P&L since it's not tracked historically
      // The gap between cash and totalValue represents deployed capital in positions
      totalValue = base + sessionPnl;
    }

    return {
      time: fmt(row.ts),
      cash,
      totalValue,
      pnl: sessionPnl,
    };
  });

  if (chartData.length === 0) {
    return (
      <div className="card p-6 flex items-center justify-center h-48 text-gray-600 text-sm">
        No balance data yet — snapshots are recorded every 60 s.
      </div>
    );
  }

  // Calculate domain to fit both lines
  const allValues = chartData.flatMap(d => [d.cash, d.totalValue]);
  const minVal = Math.min(...allValues);
  const maxVal = Math.max(...allValues);
  const pad = (maxVal - minVal) * 0.15 || 5;
  const domain = [Math.floor(minVal - pad), Math.ceil(maxVal + pad)];

  return (
    <div className="card p-4">
      <div className="flex items-center justify-between mb-3">
        <p className="label-muted">Portfolio Overview</p>
        <div className="flex items-center gap-3 text-[10px] font-mono">
          <div className="flex items-center gap-1.5">
            <div className="w-3 h-0.5 bg-emerald-400" />
            <span className="text-gray-500">Total Value</span>
          </div>
          <div className="flex items-center gap-1.5">
            <div className="w-3 h-0.5 bg-indigo-400" />
            <span className="text-gray-500">Cash</span>
          </div>
        </div>
      </div>
      <ResponsiveContainer width="100%" height={240}>
        <AreaChart data={chartData} margin={{ top: 4, right: 12, bottom: 0, left: 0 }}>
          <defs>
            <linearGradient id="totalGrad" x1="0" y1="0" x2="0" y2="1">
              <stop offset="5%"  stopColor="#10b981" stopOpacity={0.2} />
              <stop offset="95%" stopColor="#10b981" stopOpacity={0} />
            </linearGradient>
            <linearGradient id="cashGrad" x1="0" y1="0" x2="0" y2="1">
              <stop offset="5%"  stopColor="#6366f1" stopOpacity={0.15} />
              <stop offset="95%" stopColor="#6366f1" stopOpacity={0} />
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
            <ReferenceLine
              y={startingBalance}
              stroke="#374151"
              strokeDasharray="4 4"
              label={{ value: 'Session Start', position: 'insideTopRight', fill: '#6b7280', fontSize: 10 }}
            />
          )}
          {/* Total Value - render first so it's behind */}
          <Area
            type="monotone"
            dataKey="totalValue"
            stroke="#10b981"
            strokeWidth={2.5}
            fill="url(#totalGrad)"
            dot={false}
            activeDot={{ r: 4, fill: '#10b981', stroke: '#0a0a12', strokeWidth: 2 }}
          />
          {/* Cash - render second so it's in front */}
          <Area
            type="monotone"
            dataKey="cash"
            stroke="#6366f1"
            strokeWidth={2}
            fill="url(#cashGrad)"
            dot={false}
            activeDot={{ r: 4, fill: '#6366f1', stroke: '#0a0a12', strokeWidth: 2 }}
          />
        </AreaChart>
      </ResponsiveContainer>
      <div className="mt-2 text-[10px] font-mono text-gray-600">
        <span className="text-gray-500">Cash</span> = liquid funds · <span className="text-gray-500">Total Value</span> = cash + positions (current point uses live data; historical points approximate)
      </div>
    </div>
  );
}

