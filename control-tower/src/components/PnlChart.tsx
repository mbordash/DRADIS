'use client';

import { useState, useCallback, useRef } from 'react';
import {
  ComposedChart, Area, Line, XAxis, YAxis, CartesianGrid, Tooltip,
  ResponsiveContainer, ReferenceLine,
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
function MarkerTooltip({ active, payload, label }: any) {
  if (!active || !payload?.length) return null;

  // Only show tooltip when hovering over a B/S marker point
  const tradeEntry = payload.find((p: any) => p.dataKey === 'tradeDot' && p.payload?._tradeMarker);
  const posEntry   = payload.find((p: any) => p.dataKey === 'positionDot' && p.payload?._positionMarker);

  if (tradeEntry) {
    const { trade, pnl } = tradeEntry.payload._tradeMarker as { trade: TradeRow; pnl: number };
    const pnlColor = pnl > 0 ? 'text-emerald-400' : pnl < 0 ? 'text-red-400' : 'text-gray-400';
    const symbol = trade.market.split(' ')[0];
    return (
      <div className="card px-3 py-2 text-xs font-mono space-y-1.5 shadow-xl border-2 border-emerald-500/30">
        <div className="text-emerald-300 font-semibold flex items-center gap-1.5">
          <span>✅</span><span>Trade Close</span>
        </div>
        <div className="text-gray-400 text-[10px] border-t border-gray-700 pt-1">{label}</div>
        <div className="space-y-0.5 pt-1">
          <div className="flex justify-between gap-3"><span className="text-gray-500">Strategy</span><span className="text-white">{trade.strategy}</span></div>
          <div className="flex justify-between gap-3"><span className="text-gray-500">Symbol</span><span className="text-white">{symbol}</span></div>
          <div className="flex justify-between gap-3"><span className="text-gray-500">Side</span><span className="text-cyan-300">{trade.side}</span></div>
          <div className="flex justify-between gap-3"><span className="text-gray-500">Shares</span><span className="text-white">{parseFloat(trade.shares).toFixed(2)}</span></div>
          <div className="flex justify-between gap-3 pt-1 border-t border-gray-700">
            <span className="text-gray-500">P&L</span>
            <span className={`font-semibold ${pnlColor}`}>{pnl >= 0 ? '+' : ''}${pnl.toFixed(2)}</span>
          </div>
          <div className="flex justify-between gap-3"><span className="text-gray-500">Reason</span><span className="text-gray-400 text-[10px]">{trade.reason}</span></div>
        </div>
      </div>
    );
  }

  if (posEntry) {
    const { position } = posEntry.payload._positionMarker as { position: OpenPositionRow };
    const entryPrice = parseFloat(position.entry_price);
    const shares = parseFloat(position.shares);
    const symbol = position.market.split(' ')[0];
    return (
      <div className="card px-3 py-2 text-xs font-mono space-y-1.5 shadow-xl border-2 border-indigo-500/30">
        <div className="text-indigo-300 font-semibold flex items-center gap-1.5">
          <span>🎯</span><span>Position Entry</span>
        </div>
        <div className="text-gray-400 text-[10px] border-t border-gray-700 pt-1">{label}</div>
        <div className="space-y-0.5 pt-1">
          <div className="flex justify-between gap-3"><span className="text-gray-500">Strategy</span><span className="text-white">{position.strategy}</span></div>
          <div className="flex justify-between gap-3"><span className="text-gray-500">Symbol</span><span className="text-white">{symbol}</span></div>
          <div className="flex justify-between gap-3"><span className="text-gray-500">Side</span><span className="text-cyan-300">{position.side}</span></div>
          <div className="flex justify-between gap-3"><span className="text-gray-500">Entry Price</span><span className="text-white">{entryPrice.toFixed(4)}</span></div>
          <div className="flex justify-between gap-3"><span className="text-gray-500">Shares</span><span className="text-white">{shares.toFixed(2)}</span></div>
          <div className="flex justify-between gap-3 pt-1 border-t border-gray-700">
            <span className="text-gray-500">Status</span><span className="text-yellow-400">Open</span>
          </div>
          {position.ghost_mode && (
            <div className="flex justify-between gap-3"><span className="text-gray-500">Mode</span><span className="text-amber-400 text-[10px]">👻 ghost</span></div>
          )}
        </div>
      </div>
    );
  }

  // No marker at this point — render nothing (value is shown in the top display)
  return null;
}

export default function PnlChart({ data, startingBalance, ghostMode, currentPortfolio, trades, openPositions }: Props) {
  // API returns newest-first — reverse for chronological chart display
  const base = startingBalance ?? 0;
  const reversedData = [...data].reverse();

  // Birdeye-style: track hovered point to show value in header area
  const [hoveredPoint, setHoveredPoint] = useState<{ time: string; totalValue: number; cash: number } | null>(null);
  // Custom marker tooltip — bypasses Recharts hit-area limitations
  type MarkerTipState = {
    kind: 'trade';
    data: { trade: TradeRow; pnl: number };
    x: number; y: number;
  } | {
    kind: 'position';
    data: { position: OpenPositionRow };
    x: number; y: number;
  } | null;
  const [markerTip, setMarkerTip] = useState<MarkerTipState>(null);
  const chartContainerRef = useRef<HTMLDivElement>(null);

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const handleMouseMove = useCallback((state: any) => {
    if (state?.activePayload?.length) {
      const p = state.activePayload[0]?.payload;
      if (p?.totalValue !== undefined) setHoveredPoint({ time: p.time, totalValue: p.totalValue, cash: p.cash });
    }
  }, []);
  const handleMouseLeave = useCallback(() => setHoveredPoint(null), []);

  const chartData = reversedData.map((row) => {
    const sessionPnl = parseFloat(row.session_pnl);
    const collateral = parseFloat(row.collateral);

    // Cash: actual collateral (liquid funds)
    const cash = ghostMode ? base + sessionPnl : collateral;

    // Total value uses the same asset-scoped snapshot source for every point.
    // Mixing an all-assets realtime value into a single-asset series creates
    // artificial right-edge spikes/vertical lines.
    let totalValue: number;

    if (row.total_value) {
      // Use historical total value stored in snapshot (cash + positions at that time)
      totalValue = parseFloat(row.total_value);
    } else {
      // Fallback for old snapshots that don't have total_value recorded yet
      totalValue = base + sessionPnl;
    }

    return {
      time: fmt(row.ts),
      ts: row.ts, // Keep raw timestamp for marker matching
      cash,
      totalValue,
      pnl: sessionPnl,
    };
  });

  // Calculate basic domain first
  const allValues = chartData.flatMap(d => [d.cash, d.totalValue]);
  const minVal = Math.min(...allValues);
  const maxVal = Math.max(...allValues);
  const yRange = Math.max(maxVal - minVal, 10); // Ensure minimum range of 10 for marker visibility

  // Filter trades and positions to only those within the chart's time range
  const oldestSnapshotTime = reversedData[0] ? new Date(reversedData[0].ts).getTime() : 0;
  const newestSnapshotTime = reversedData[reversedData.length - 1]
    ? new Date(reversedData[reversedData.length - 1].ts).getTime()
    : Date.now();

  const tradesInRange = (trades ?? []).filter(trade => {
    const tradeTime = new Date(trade.ts).getTime();
    return tradeTime >= oldestSnapshotTime && tradeTime <= newestSnapshotTime;
  });

  const positionsInRange = (openPositions ?? []).filter(position => {
    const positionTime = new Date(position.ts).getTime();
    return positionTime >= oldestSnapshotTime && positionTime <= newestSnapshotTime;
  });

  // Build marker lookup maps keyed by chart point's time string
  const tradeMarkerMap = new Map<string, { pnl: number; trade: TradeRow }>();
  const positionMarkerMap = new Map<string, { position: OpenPositionRow }>();

  tradesInRange.forEach(trade => {
    const tradeTime = new Date(trade.ts).getTime();
    const closestPoint = chartData.reduce((closest, point) => {
      const pointTime = new Date(point.ts).getTime();
      const closestTime = new Date(closest.ts).getTime();
      return Math.abs(pointTime - tradeTime) < Math.abs(closestTime - tradeTime) ? point : closest;
    }, chartData[0]);
    if (closestPoint) {
      tradeMarkerMap.set(closestPoint.time, { pnl: parseFloat(trade.pnl), trade });
    }
  });

  positionsInRange.forEach(position => {
    const positionTime = new Date(position.ts).getTime();
    const closestPoint = chartData.reduce((closest, point) => {
      const pointTime = new Date(point.ts).getTime();
      const closestTime = new Date(closest.ts).getTime();
      return Math.abs(pointTime - positionTime) < Math.abs(closestTime - positionTime) ? point : closest;
    }, chartData[0]);
    if (closestPoint) {
      positionMarkerMap.set(closestPoint.time, { position });
    }
  });

  // Merge marker flags into chartData so Line components can render custom dots
  // on the categorical XAxis (Scatter doesn't support categorical axes reliably)
  const chartDataWithMarkers = chartData.map(point => ({
    ...point,
    tradeDot:    tradeMarkerMap.has(point.time)    ? point.totalValue + yRange * 0.15 : undefined,
    positionDot: positionMarkerMap.has(point.time) ? point.totalValue + yRange * 0.08 : undefined,
    _tradeMarker:    tradeMarkerMap.get(point.time),
    _positionMarker: positionMarkerMap.get(point.time),
  }));

  // For legend display (count)
  const tradeEvents = tradesInRange;
  const positionEvents = positionsInRange;

  if (chartData.length === 0) {
    return (
      <div className="card p-6 flex items-center justify-center h-48 text-gray-600 text-sm">
        No balance data yet — snapshots are recorded every 60 s.
      </div>
    );
  }

  // Calculate Y-axis domain with padding to fit both lines and markers
  // Add extra padding at top for markers positioned at totalValue + yRange * 0.15
  const topPad = yRange * 0.25; // Increased to ensure markers are visible
  const bottomPad = yRange * 0.05;
  const domain = [
    Math.floor(minVal - bottomPad),
    Math.ceil(maxVal + topPad)
  ];

  return (
    <div className="card p-4">
      {/* Birdeye-style header: static legend + live hovered value display */}
      <div className="flex items-start justify-between mb-3">
        <div>
          <p className="label-muted text-[10px]">Portfolio Overview</p>
          {/* Live value display — updates as cursor moves over chart */}
          <div className="mt-0.5 font-mono">
            {hoveredPoint ? (
              <div className="flex items-baseline gap-3">
                <span className="text-lg font-semibold text-emerald-300">${hoveredPoint.totalValue.toFixed(2)}</span>
                <span className="text-xs text-gray-500">total</span>
                <span className="text-sm text-indigo-300">${hoveredPoint.cash.toFixed(2)}</span>
                <span className="text-xs text-gray-500">cash</span>
                {hoveredPoint.totalValue - hoveredPoint.cash > 0 && (
                  <>
                    <span className="text-sm text-gray-400">${(hoveredPoint.totalValue - hoveredPoint.cash).toFixed(2)}</span>
                    <span className="text-xs text-gray-500">in positions</span>
                  </>
                )}
                <span className="text-[10px] text-gray-600">{hoveredPoint.time}</span>
              </div>
            ) : (
              (() => {
                const latest = chartDataWithMarkers[chartDataWithMarkers.length - 1];
                const inPos = latest ? latest.totalValue - latest.cash : 0;
                return latest ? (
                  <div className="flex items-baseline gap-3">
                    <span className="text-lg font-semibold text-emerald-300">${latest.totalValue.toFixed(2)}</span>
                    <span className="text-xs text-gray-500">total</span>
                    <span className="text-sm text-indigo-300">${latest.cash.toFixed(2)}</span>
                    <span className="text-xs text-gray-500">cash</span>
                    {inPos > 0 && (
                      <>
                        <span className="text-sm text-gray-400">${inPos.toFixed(2)}</span>
                        <span className="text-xs text-gray-500">in positions</span>
                      </>
                    )}
                  </div>
                ) : null;
              })()
            )}
          </div>
        </div>
        <div className="flex items-center gap-3 text-[10px] font-mono mt-1">
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
      
      {/* Stacked chart containers: Portfolio chart + Marker overlay */}
      <div ref={chartContainerRef} className="relative" style={{ height: 320 }}>
        {/* Custom marker tooltip overlay — positioned absolutely over chart */}
        {markerTip && (
          <div
            className="absolute z-50 pointer-events-none"
            style={{
              left: Math.min(markerTip.x + 14, (chartContainerRef.current?.clientWidth ?? 600) - 220),
              top: Math.max(markerTip.y - 10, 0),
            }}
          >
            {markerTip.kind === 'trade' ? (() => {
              const { trade, pnl } = markerTip.data;
              const pnlColor = pnl > 0 ? 'text-emerald-400' : pnl < 0 ? 'text-red-400' : 'text-gray-400';
              return (
                <div className="card px-3 py-2 text-xs font-mono space-y-1.5 shadow-xl border-2 border-emerald-500/30 w-48">
                  <div className="text-emerald-300 font-semibold flex items-center gap-1.5"><span>✅</span><span>Trade Close</span></div>
                  <div className="space-y-0.5 pt-1">
                    <div className="flex justify-between gap-3"><span className="text-gray-500">Strategy</span><span className="text-white truncate">{trade.strategy}</span></div>
                    <div className="flex justify-between gap-3"><span className="text-gray-500">Market</span><span className="text-white text-[10px] truncate max-w-[110px]">{trade.market.split(' ').slice(0,3).join(' ')}</span></div>
                    <div className="flex justify-between gap-3"><span className="text-gray-500">Side</span><span className="text-cyan-300">{trade.side}</span></div>
                    <div className="flex justify-between gap-3"><span className="text-gray-500">Shares</span><span className="text-white">{parseFloat(trade.shares).toFixed(2)}</span></div>
                    <div className="flex justify-between gap-3 pt-1 border-t border-gray-700">
                      <span className="text-gray-500">P&L</span>
                      <span className={`font-semibold ${pnlColor}`}>{pnl >= 0 ? '+' : ''}${pnl.toFixed(2)}</span>
                    </div>
                    <div className="flex justify-between gap-3"><span className="text-gray-500">Reason</span><span className="text-gray-400 text-[10px] truncate max-w-[110px]">{trade.reason}</span></div>
                  </div>
                </div>
              );
            })() : (() => {
              const { position } = markerTip.data;
              return (
                <div className="card px-3 py-2 text-xs font-mono space-y-1.5 shadow-xl border-2 border-indigo-500/30 w-48">
                  <div className="text-indigo-300 font-semibold flex items-center gap-1.5"><span>🎯</span><span>Position Entry</span></div>
                  <div className="space-y-0.5 pt-1">
                    <div className="flex justify-between gap-3"><span className="text-gray-500">Strategy</span><span className="text-white truncate">{position.strategy}</span></div>
                    <div className="flex justify-between gap-3"><span className="text-gray-500">Market</span><span className="text-white text-[10px] truncate max-w-[110px]">{position.market.split(' ').slice(0,3).join(' ')}</span></div>
                    <div className="flex justify-between gap-3"><span className="text-gray-500">Side</span><span className="text-cyan-300">{position.side}</span></div>
                    <div className="flex justify-between gap-3"><span className="text-gray-500">Entry Price</span><span className="text-white">{parseFloat(position.entry_price).toFixed(4)}</span></div>
                    <div className="flex justify-between gap-3"><span className="text-gray-500">Shares</span><span className="text-white">{parseFloat(position.shares).toFixed(2)}</span></div>
                    <div className="flex justify-between gap-3 pt-1 border-t border-gray-700"><span className="text-gray-500">Status</span><span className="text-yellow-400">Open</span></div>
                    {position.ghost_mode && <div className="flex justify-between gap-3"><span className="text-gray-500">Mode</span><span className="text-amber-400 text-[10px]">👻 ghost</span></div>}
                  </div>
                </div>
              );
            })()}
          </div>
        )}

        <ResponsiveContainer width="100%" height={320}>
          <ComposedChart
            data={chartDataWithMarkers}
            margin={{ top: 20, right: 12, bottom: 0, left: 0 }}
            onMouseMove={handleMouseMove}
            onMouseLeave={handleMouseLeave}
          >
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
            {/* No Recharts Tooltip — value shown in header, markers use custom SVG overlay */}
            <Tooltip content={() => null} />
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

            {/* Position entry markers — large transparent hit circle captures hover via SVG events */}
            <Line
              dataKey="positionDot"
              stroke="none"
              strokeWidth={0}
              isAnimationActive={false}
              dot={(props: any) => {
                if (props.payload.positionDot === undefined) return <g />;
                const { cx, cy, payload } = props;
                const isActive = markerTip?.kind === 'position' &&
                  markerTip.data.position === payload._positionMarker?.position;
                return (
                  <g
                    key={`pos-${cx}-${cy}`}
                    style={{ cursor: 'pointer' }}
                    onMouseEnter={(e) => {
                      const rect = chartContainerRef.current?.getBoundingClientRect();
                      if (rect) setMarkerTip({ kind: 'position', data: payload._positionMarker, x: e.clientX - rect.left, y: e.clientY - rect.top });
                    }}
                    onMouseLeave={() => setMarkerTip(null)}
                  >
                    {/* Large transparent hit area */}
                    <circle cx={cx} cy={cy} r={18} fill="transparent" />
                    {isActive && <circle cx={cx} cy={cy} r={14} fill="#6366f1" fillOpacity={0.2} stroke="#6366f1" strokeWidth={1.5} strokeDasharray="3 2" />}
                    <circle cx={cx} cy={cy} r={8} fill="#6366f1" stroke="#0a0a12" strokeWidth={1.5} opacity={0.95} />
                    <text x={cx} y={cy} textAnchor="middle" dominantBaseline="central" fill="#ffffff" fontSize="10" fontWeight="600" fontFamily="monospace" pointerEvents="none">B</text>
                  </g>
                );
              }}
              activeDot={false}
            />

            {/* Trade exit markers — large transparent hit circle captures hover via SVG events */}
            <Line
              dataKey="tradeDot"
              stroke="none"
              strokeWidth={0}
              isAnimationActive={false}
              dot={(props: any) => {
                if (props.payload.tradeDot === undefined) return <g />;
                const { cx, cy, payload } = props;
                const pnl = payload._tradeMarker?.pnl ?? 0;
                const color = pnl > 0 ? '#10b981' : pnl < 0 ? '#ef4444' : '#6b7280';
                const isActive = markerTip?.kind === 'trade' &&
                  markerTip.data.trade === payload._tradeMarker?.trade;
                return (
                  <g
                    key={`trade-${cx}-${cy}`}
                    style={{ cursor: 'pointer' }}
                    onMouseEnter={(e) => {
                      const rect = chartContainerRef.current?.getBoundingClientRect();
                      if (rect) setMarkerTip({ kind: 'trade', data: payload._tradeMarker, x: e.clientX - rect.left, y: e.clientY - rect.top });
                    }}
                    onMouseLeave={() => setMarkerTip(null)}
                  >
                    {/* Large transparent hit area */}
                    <circle cx={cx} cy={cy} r={18} fill="transparent" />
                    {isActive && <circle cx={cx} cy={cy} r={14} fill={color} fillOpacity={0.2} stroke={color} strokeWidth={1.5} strokeDasharray="3 2" />}
                    <circle cx={cx} cy={cy} r={8} fill={color} stroke="#0a0a12" strokeWidth={1.5} opacity={0.95} />
                    <text x={cx} y={cy} textAnchor="middle" dominantBaseline="central" fill="#ffffff" fontSize="10" fontWeight="600" fontFamily="monospace" pointerEvents="none">S</text>
                  </g>
                );
              }}
              activeDot={false}
            />
          </ComposedChart>
        </ResponsiveContainer>
      </div>
      
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

