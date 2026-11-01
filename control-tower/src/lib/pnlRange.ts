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
 * Portfolio chart range control: which window of P&L history to show.
 *
 * Pure helpers, no React. The server serves any window up to a year
 * (`/api/pnl/history?hours=`), downsampled to the requested point count, and
 * reports the oldest snapshot it holds (`/api/pnl/span`). The control greys
 * out every fixed window the instance has not been running long enough to
 * fill, and "All" asks for exactly as much history as exists.
 */

export type PnlRange = '24h' | '3d' | '7d' | '30d' | 'all';

export const PNL_RANGES: { id: PnlRange; label: string; hours: number | null }[] = [
  { id: '24h', label: '24h', hours: 24 },
  { id: '3d',  label: '3d',  hours: 24 * 3 },
  { id: '7d',  label: '7d',  hours: 24 * 7 },
  { id: '30d', label: '30d', hours: 24 * 30 },
  { id: 'all', label: 'All', hours: null },
];

/** Mirrors `db::PNL_HISTORY_MAX_HOURS` on the engine: one year. */
export const PNL_HISTORY_MAX_HOURS = 24 * 366;

/** Points the chart asks for, whatever the window; the server spreads them. */
export const PNL_HISTORY_POINTS = 1440;

export const PNL_RANGE_STORE_KEY = 'dradis.pnlRange';

export function isPnlRange(v: unknown): v is PnlRange {
  return PNL_RANGES.some(r => r.id === v);
}

/** Hours of history on the instance, from the oldest snapshot; null when unknown. */
export function availableHours(firstTs: string | null | undefined, now = Date.now()): number | null {
  if (!firstTs) return null;
  const t = new Date(firstTs).getTime();
  if (!Number.isFinite(t)) return null;
  return Math.max(0, (now - t) / 3_600_000);
}

/**
 * A fixed window is offered once the instance holds at least 90 % of it: a
 * chart of six days and twenty hours is still the week, while one of two
 * days under a "7d" label would be the 3d chart with the wrong name. "All"
 * is offered once there is more than a day, since below that it is "24h".
 * With the span unknown every option stays enabled: the server simply
 * returns what it has, and the chart says where the history begins.
 */
export function rangeEnabled(range: PnlRange, avail: number | null): boolean {
  if (avail === null) return true;
  const def = PNL_RANGES.find(r => r.id === range);
  if (!def) return false;
  if (def.hours === null) return avail > 24;
  return range === '24h' || avail >= def.hours * 0.9;
}

/** The `hours` to request for a range; "All" covers the whole span, capped at a year. */
export function hoursForRange(range: PnlRange, avail: number | null): number {
  const def = PNL_RANGES.find(r => r.id === range);
  if (!def) return 24;
  if (def.hours !== null) return def.hours;
  if (avail === null) return PNL_HISTORY_MAX_HOURS;
  return Math.min(PNL_HISTORY_MAX_HOURS, Math.max(24, Math.ceil(avail) + 1));
}

/**
 * Trades to load per asset for the chart's exit markers. `/api/trades` caps
 * at 500; sixty is plenty for a day but a month of GBoost paper trades runs
 * past it, so longer windows take the cap. The chart says when the list hit it.
 */
export function tradeLimitForHours(hours: number): number {
  return hours <= 24 ? 60 : 500;
}

/** Axis and tooltip time label: clock only inside a day, date and clock beyond. */
export function formatPointTime(iso: string, hours: number): string {
  const d = new Date(iso);
  if (hours <= 24) {
    return d.toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit', hour12: false });
  }
  const date = d.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
  const time = d.toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit', hour12: false });
  return `${date} ${time}`;
}

/** "Oct 17" style label for where the history begins. */
export function formatHistoryStart(iso: string): string {
  return new Date(iso).toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
}

/**
 * True when the plotted history begins materially after the window start: a
 * "30d" chart on an instance twelve days old is a twelve-day chart. Five
 * percent of the window absorbs the sampling stride and the span's refresh lag.
 */
export function historyBeginsAfterWindow(oldestTs: string | null | undefined, hours: number, now = Date.now()): boolean {
  if (!oldestTs) return false;
  const oldest = new Date(oldestTs).getTime();
  if (!Number.isFinite(oldest)) return false;
  const windowMs = hours * 3_600_000;
  return oldest - (now - windowMs) > windowMs * 0.05;
}

/**
 * How the banner names the delta's horizon: "over the last 7d" when the
 * window is full, "since Oct 17" when the history is shorter than the window
 * or on All. Same rule as the chart's "History begins" caption, so the two
 * never disagree about what the left edge is.
 */
export function windowLabel(range: PnlRange, hours: number, oldestTs: string | null | undefined, now = Date.now()): string {
  const def = PNL_RANGES.find(r => r.id === range);
  if (oldestTs && (range === 'all' || historyBeginsAfterWindow(oldestTs, hours, now))) {
    return `since ${formatHistoryStart(oldestTs)}`;
  }
  return `over the last ${def?.label ?? `${hours}h`}`;
}
