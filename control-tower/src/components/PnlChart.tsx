'use client';

import {
  AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip,
  ResponsiveContainer, ReferenceLine, Scatter,
} from 'recharts';
import type { PnlSnapshotRow, PortfolioValue, TradeRow, OpenPositionRow } from '@/lib/types';

interface Props {
  data: PnlSnapshotRow[];
  startingBalance?: number;
  /** When true, compute balance as startingBalance + session_pnl (ghost mode — on-chain balance is flat). */
  ghostMode?: boolean;
  /** Current real-time portfolio value (used for the most recent data point). */
  currentPortfolio?: PortfolioValue;
  /** Completed trade events (exits) to overlay on the chart as markers. */
  trades?: TradeRow[];
  /** Open positions (entries) to overlay on the chart as markers. */
  openPositions?: OpenPositionRow[];
}

function fmt(iso: string) {
  return new Date(iso).toLocaleTimeString('en-US', {
    hour: '2-digit', minute: '2-digit', hour12: false,
  });
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function CustomTooltip({ active, payload, label }: any) {
  if (!active || !payload?.length) return null;

  // Check if hovering over a trade exit marker
  const tradeData = payload.find((p: any) => p.dataKey === 'tradeValue');
  if (tradeData?.payload?.trade) {
    const trade = tradeData.payload.trade as TradeRow;
    const pnl = parseFloat(trade.pnl);
    const pnlColor = pnl > 0 ? 'text-emerald-400' : pnl < 0 ? 'text-red-400' : 'text-gray-400';
    const symbol = trade.market.split(' ')[0]; // Extract symbol from market name

    return (
      <div className="card px-3 py-2 text-xs font-mono space-y-1.5 shadow-xl border-2 border-emerald-500/30">
        <div className="text-emerald-300 font-semibold flex items-center gap-1.5">
          <span>✅</span>
          <span>Trade Close</span>
        </div>
        <div className="text-gray-400 text-[10px] border-t border-gray-700 pt-1">{label}</div>
        <div className="space-y-0.5 pt-1">
          <div className="flex justify-between gap-3">
            <span className="text-gray-500">Strategy</span>
            <span className="text-white">{trade.strategy}</span>
          </div>
          <div className="flex justify-between gap-3">
            <span className="text-gray-500">Symbol</span>
            <span className="text-white">{symbol}</span>
          </div>
          <div className="flex justify-between gap-3">
            <span className="text-gray-500">Side</span>
            <span className="text-cyan-300">{trade.side}</span>
          </div>
          <div className="flex justify-between gap-3">
            <span className="text-gray-500">Shares</span>
            <span className="text-white">{parseFloat(trade.shares).toFixed(2)}</span>
          </div>
          <div className="flex justify-between gap-3 pt-1 border-t border-gray-700">
            <span className="text-gray-500">P&L</span>
            <span className={`font-semibold ${pnlColor}`}>
              {pnl >= 0 ? '+' : ''}${pnl.toFixed(2)}
            </span>
          </div>
          <div className="flex justify-between gap-3">
            <span className="text-gray-500">Reason</span>
            <span className="text-gray-400 text-[10px]">{trade.reason}</span>
          </div>
        </div>
      </div>
    );
  }

  // Check if hovering over a position entry marker
  const positionData = payload.find((p: any) => p.dataKey === 'positionValue');
  if (positionData?.payload?.position) {
    const position = positionData.payload.position as OpenPositionRow;
    const entryPrice = parseFloat(position.entry_price);
    const shares = parseFloat(position.shares);
    const symbol = position.market.split(' ')[0]; // Extract symbol from market name

    return (
      <div className="card px-3 py-2 text-xs font-mono space-y-1.5 shadow-xl border-2 border-indigo-500/30">
        <div className="text-indigo-300 font-semibold flex items-center gap-1.5">
          <span></span>
          <span>Position Entry</span>
        </div>
        <div className="text-gray-400 text-[10px] border-t border-gray-700 pt-1">{label}</div>
        <div className="space-y-0.5 pt-1">
          <div className="flex justify-between gap-3">
            <span className="text-gray-500">Strategy</span>
            <span className="text-white">{position.strategy}</span>
          </div>
          <div className="flex justify-between gap-3">
            <span className="text-gray-500">Symbol</span>
            <span className="text-white">{symbol}</span>
          </div>
          <div className="flex justify-between gap-3">
            <span className="text-gray-500">Side</span>
            <span className="text-cyan-300">{position.side}</span>
          </div>
          <div className="flex justify-between gap-3">
            <span className="text-gray-500">Entry Price</span>
            <span className="text-white">{entryPrice.toFixed(4)}</span>
          </div>
          <div className="flex justify-between gap-3">
            <span className="text-gray-500">Shares</span>
            <span className="text-white">{shares.toFixed(2)}</span>
          </div>
          <div className="flex justify-between gap-3 pt-1 border-t border-gray-700">
            <span className="text-gray-500">Status</span>
            <span className="text-yellow-400">Open</span>
          </div>
          {position.ghost_mode && (
            <div className="flex justify-between gap-3">
              <span className="text-gray-500">Mode</span>
              <span className="text-amber-400 text-[10px]"> ghost</span>
            </div>
          )}
        </div>
      </div>
    );
  }

  // Default tooltip for portfolio value
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

export default function PnlChart({ data, startingBalance, ghostMode, currentPortfolio, trades, openPositions }: Props) {
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
    // For historical points, use the stored total_value if available (Phase 3f-7),
    // otherwise fall back to approximation: starting balance + session P&L
    let totalValue: number;

    if (isLatestPoint && currentPortfolio) {
      // Use accurate real-time data for the most recent point
      totalValue = parseFloat(currentPortfolio.total_value);
    } else if (row.total_value) {
      // Use historical total value stored in snapshot (cash + positions at that time)
      totalValue = parseFloat(row.total_value);
    } else {
      // Fallback for old snapshots that don't have total_value recorded yet
      totalValue = base + sessionPnl;
    }

    return {
      time: fmt(row.ts),
      cash,
      totalValue,
      pnl: sessionPnl,
    };
  });

  // Calculate domain to determine marker offsets
  const allValues = chartData.flatMap(d => [d.cash, d.totalValue]);
  const minVal = Math.min(...allValues);
  const maxVal = Math.max(...allValues);
  const yRange = maxVal - minVal;

  // Process trades (exits) into scatter plot data
  // Filter trades to only those within the P&L chart's time range
  const oldestSnapshotTime = reversedData[0] ? new Date(reversedData[0].ts).getTime() : 0;
  const newestSnapshotTime = reversedData[reversedData.length - 1]
    ? new Date(reversedData[reversedData.length - 1].ts).getTime()
    : Date.now();

  const tradesInRange = (trades ?? []).filter(trade => {
    const tradeTime = new Date(trade.ts).getTime();
    return tradeTime >= oldestSnapshotTime && tradeTime <= newestSnapshotTime;
  });

  const tradeEvents = tradesInRange.map(trade => {
    const tradeTime = new Date(trade.ts);
    const pnl = parseFloat(trade.pnl);
    
    // Find the closest snapshot time to position the marker
    const closestSnapshot = reversedData.reduce((closest, snap) => {
      const snapTime = new Date(snap.ts);
      const closestTime = new Date(closest.ts);
      return Math.abs(snapTime.getTime() - tradeTime.getTime()) < 
             Math.abs(closestTime.getTime() - tradeTime.getTime()) ? snap : closest;
    }, reversedData[0]);

    const snapIndex = reversedData.findIndex(s => s.ts === closestSnapshot?.ts);
    const chartPoint = chartData[snapIndex];

    return {
      time: chartPoint?.time || fmt(trade.ts),
      // Position markers clearly above the total value line
      tradeValue: chartPoint ? chartPoint.totalValue + yRange * 0.08 : base,
      pnl,
      trade, // Store full trade for tooltip
      color: pnl > 0 ? '#10b981' : pnl < 0 ? '#ef4444' : '#6b7280',
    };
  });

  // Process open positions (entries) into scatter plot data
  const positionsInRange = (openPositions ?? []).filter(position => {
    const positionTime = new Date(position.ts).getTime();
    return positionTime >= oldestSnapshotTime && positionTime <= newestSnapshotTime;
  });

  const positionEvents = positionsInRange.map(position => {
    const positionTime = new Date(position.ts);

    // Find the closest snapshot time to position the marker
    const closestSnapshot = reversedData.reduce((closest, snap) => {
      const snapTime = new Date(snap.ts);
      const closestTime = new Date(closest.ts);
      return Math.abs(snapTime.getTime() - positionTime.getTime()) <
             Math.abs(closestTime.getTime() - positionTime.getTime()) ? snap : closest;
    }, reversedData[0]);

    const snapIndex = reversedData.findIndex(s => s.ts === closestSnapshot?.ts);
    const chartPoint = chartData[snapIndex];

    return {
      time: chartPoint?.time || fmt(position.ts),
      // Position markers clearly above the total value line (but below trade markers)
      positionValue: chartPoint ? chartPoint.totalValue + yRange * 0.04 : base,
      position, // Store full position for tooltip
      color: '#6366f1', // Indigo for position entries
    };
  });

  if (chartData.length === 0) {
    return (
      <div className="card p-6 flex items-center justify-center h-48 text-gray-600 text-sm">
        No balance data yet — snapshots are recorded every 60 s.
      </div>
    );
  }

  // Calculate Y-axis domain with padding to fit both lines and markers
  const pad = yRange * 0.15 || 5;
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
          {positionEvents.length > 0 && (
            <div className="flex items-center gap-1.5">
              <div className="w-4 h-4 rounded-full bg-indigo-400 flex items-center justify-center text-white text-[8px] font-bold">B</div>
              <span className="text-gray-500">Entries ({positionEvents.length})</span>
            </div>
          )}
          {tradeEvents.length > 0 && (
            <div className="flex items-center gap-1.5">
              <div className="w-4 h-4 rounded-full bg-emerald-400 flex items-center justify-center text-white text-[8px] font-bold">S</div>
              <span className="text-gray-500">Exits ({tradeEvents.length})</span>
            </div>
          )}
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
            minTickGap={20}
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
          {/* Position entry markers (B for buy) - render before trade exits */}
          {positionEvents.length > 0 && (
            <Scatter
              data={positionEvents}
              dataKey="positionValue"
              fill="#6366f1"
              shape={(props: any) => {
                const { cx, cy } = props;
                return (
                  <g>
                    <circle
                      cx={cx}
                      cy={cy}
                      r={8}
                      fill="#6366f1"
                      stroke="#0a0a12"
                      strokeWidth={1.5}
                      opacity={0.95}
                    />
                    <text
                      x={cx}
                      y={cy}
                      textAnchor="middle"
                      dominantBaseline="central"
                      fill="#ffffff"
                      fontSize="10"
                      fontWeight="600"
                      fontFamily="monospace"
                    >
                      B
                    </text>
                  </g>
                );
              }}
            />
          )}
          {/* Trade exit markers (S for sell with green/red colors) */}
          {tradeEvents.length > 0 && (
            <Scatter
              data={tradeEvents}
              dataKey="tradeValue"
              fill="#fbbf24"
              shape={(props: any) => {
                const { cx, cy, payload } = props;
                const pnl = payload.pnl;
                const color = pnl > 0 ? '#10b981' : pnl < 0 ? '#ef4444' : '#6b7280';
                return (
                  <g>
                    <circle
                      cx={cx}
                      cy={cy}
                      r={8}
                      fill={color}
                      stroke="#0a0a12"
                      strokeWidth={1.5}
                      opacity={0.95}
                    />
                    <text
                      x={cx}
                      y={cy}
                      textAnchor="middle"
                      dominantBaseline="central"
                      fill="#ffffff"
                      fontSize="10"
                      fontWeight="600"
                      fontFamily="monospace"
                    >
                      S
                    </text>
                  </g>
                );
              }}
            />
          )}
        </AreaChart>
      </ResponsiveContainer>
      <div className="mt-2 text-[10px] font-mono text-gray-600">
        <span className="text-gray-500">Cash</span> = liquid funds · <span className="text-gray-500">Total Value</span> = cash + positions (current point uses live data; historical points approximate)
        {(positionEvents.length > 0 || tradeEvents.length > 0) && (
          <>
            {' · '}
            <span className="text-gray-500">Markers</span>:
            {positionEvents.length > 0 && (
              <span>
                {' '}<span className="inline-block w-4 h-4 rounded-full bg-indigo-400 text-white text-[8px] font-semibold leading-4 text-center align-middle">B</span> buy
              </span>
            )}
            {positionEvents.length > 0 && tradeEvents.length > 0 && ' / '}
            {tradeEvents.length > 0 && (
              <span>
                <span className="inline-block w-4 h-4 rounded-full bg-emerald-400 text-white text-[8px] font-semibold leading-4 text-center align-middle">S</span> sell (profit) / <span className="inline-block w-4 h-4 rounded-full bg-red-400 text-white text-[8px] font-semibold leading-4 text-center align-middle">S</span> sell (loss)
              </span>
            )}
            {(tradesInRange.length < (trades?.length ?? 0) || positionsInRange.length < (openPositions?.length ?? 0)) && (
              <span className="text-gray-700">
                {' '}(showing {positionsInRange.length + tradesInRange.length} of {(trades?.length ?? 0) + (openPositions?.length ?? 0)} events in range)
              </span>
            )}
          </>
        )}
      </div>
    </div>
  );
}

