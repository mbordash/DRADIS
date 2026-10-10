'use client';

import {
  LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, ReferenceLine,
} from 'recharts';
import type { PricePoint } from '@/lib/types';

interface Row { time: string; yes?: number; no?: number }

function clock(iso: string): string {
  const d = new Date(iso);
  return Number.isNaN(d.getTime()) ? '' : d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', hour12: false });
}

function merge(yes: PricePoint[] | null, no: PricePoint[] | null): Row[] {
  const byAt = new Map<string, Row>();
  const add = (pts: PricePoint[] | null, key: 'yes' | 'no') => {
    for (const p of pts ?? []) {
      const v = Number(p.price);
      if (!Number.isFinite(v)) continue;
      const row = byAt.get(p.at) ?? { time: p.at };
      row[key] = v;
      byAt.set(p.at, row);
    }
  };
  add(yes, 'yes');
  add(no, 'no');
  return Array.from(byAt.values())
    .sort((a, b) => a.time.localeCompare(b.time))
    .map(r => ({ ...r, time: clock(r.time) }));
}

const fmtY = (v: number) => v.toFixed(2);

export default function MarketHistoryChart({ yes, no }: { yes: PricePoint[] | null; no: PricePoint[] | null }) {
  const data = merge(yes, no);
  const series = [
    { key: 'yes' as const, label: 'YES', color: '#6366f1' },
    { key: 'no' as const, label: 'NO', color: '#10b981' },
  ];
  return (
    <div style={{ height: 200 }}>
      {data.length < 2 ? (
        <div className="h-full flex items-center justify-center text-gray-600 text-xs">
          Collecting samples…
        </div>
      ) : (
        <ResponsiveContainer width="100%" height="100%">
          <LineChart data={data} margin={{ top: 6, right: 12, bottom: 0, left: 0 }}>
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
            <ReferenceLine y={0.5} stroke="#4b5563" strokeDasharray="4 4" />
            {series.map(s => (
              <Line
                key={s.key}
                type="monotone"
                dataKey={s.key}
                name={s.label}
                stroke={s.color}
                strokeWidth={1.8}
                dot={false}
                isAnimationActive={false}
                connectNulls
              />
            ))}
          </LineChart>
        </ResponsiveContainer>
      )}
    </div>
  );
}
