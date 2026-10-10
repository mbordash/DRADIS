// Pure, total formatting helpers for the Markets page. Unknown renders as a dash.

const DASH = '–';

function num(s: string | null | undefined): number | null {
  if (s === null || s === undefined || s === '') return null;
  const n = Number(s);
  return Number.isFinite(n) ? n : null;
}

/** Decimal string to at most 4 decimals, trailing zeros trimmed. */
export function fmtPx(s: string | null): string {
  const n = num(s);
  if (n === null) return DASH;
  return String(Number(n.toFixed(4)));
}

/** Dollar decimal string as signed cents, e.g. "+4.2¢". */
export function fmtCents(s: string | null): string {
  const n = num(s);
  if (n === null) return DASH;
  const c = n * 100;
  const sign = c > 0 ? '+' : c < 0 ? '-' : '';
  return `${sign}${Math.abs(c).toFixed(1)}¢`;
}

export function fmtMoney(n: number | null): string {
  if (n === null || n === undefined || !Number.isFinite(n)) return DASH;
  return `$${Math.round(n).toLocaleString('en-US')}`;
}

export function fmtCountdown(secs: number | null): string {
  if (secs === null || secs === undefined || !Number.isFinite(secs)) return DASH;
  if (secs <= 0) return 'closed';
  const s = Math.floor(secs);
  const d = Math.floor(s / 86400);
  const h = Math.floor((s % 86400) / 3600);
  const m = Math.floor((s % 3600) / 60);
  if (d > 0) return `${d}d ${h}h`;
  if (h > 0) return `${h}h ${m}m`;
  if (m > 0) return `${m}m`;
  return `${s}s`;
}

/** Two significant digits in exponent form, e.g. "1.2e-4". */
export function fmtSci(x: number): string {
  if (typeof x !== 'number' || !Number.isFinite(x)) return DASH;
  return x.toExponential(1);
}

/** Fraction (0..1) as a percent with one decimal. */
export function pct(x: number): string {
  if (typeof x !== 'number' || !Number.isFinite(x)) return DASH;
  return `${(x * 100).toFixed(1)}%`;
}

/** Local HH:MM:SS from an ISO timestamp. */
export function fmtClock(iso: string | null | undefined): string {
  if (!iso) return DASH;
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return DASH;
  return d.toLocaleTimeString([], { hour12: false });
}

/** Local date and time from an ISO timestamp. */
export function fmtLocal(iso: string | null | undefined): string {
  if (!iso) return DASH;
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return DASH;
  return d.toLocaleString();
}

/** Seconds until an ISO timestamp, or null. */
export function secsUntil(iso: string | null | undefined, now: number = Date.now()): number | null {
  if (!iso) return null;
  const t = new Date(iso).getTime();
  if (Number.isNaN(t)) return null;
  return Math.max(0, Math.floor((t - now) / 1000));
}
