/**
 * GET /api/portfolio
 *
 * Compute true portfolio value = DRADIS collateral balance + mark-to-market
 * value of all open positions, using live midpoint prices from the Polymarket
 * public CLOB API (no auth required).
 *
 * This mirrors exactly what Polymarket's own portfolio page shows, so the two
 * UIs stay in sync.
 *
 * This route runs server-side (Next.js Route Handler), so it can call both the
 * internal DRADIS API (unreachable from the browser in Docker) AND the external
 * Polymarket CLOB API — no CORS issues.
 */
import { NextResponse } from 'next/server';

const DRADIS_API = process.env.DRADIS_API_URL ?? 'http://localhost:9000';
// Server-side only — NOT NEXT_PUBLIC_ so it never appears in the browser bundle.
const DRADIS_API_KEY = process.env.DRADIS_API_KEY ?? '';
const POLY_CLOB  = 'https://clob.polymarket.com';

/** Build headers for internal DRADIS engine calls, injecting the API key when set. */
function dradisHeaders(): Record<string, string> {
  const h: Record<string, string> = {};
  if (DRADIS_API_KEY) h['X-API-Key'] = DRADIS_API_KEY;
  return h;
}

/** Shape of a single enriched position (superset of OpenPositionRow). */
interface EnrichedPosition {
  ts:            string;
  strategy:      string;
  token_id:      string;
  market:        string;
  side:          string;
  entry_price:   string;
  shares:        string;
  ghost_mode:    boolean;
  current_price: string;  // live mid from Polymarket, or entry_price if unavailable
  value:         string;  // shares × current_price
  unrealized_pnl: string; // shares × (current_price − entry_price)
}

export interface PortfolioValueResponse {
  collateral:      string; // cash on deposit (pUSD)
  positions_value: string; // Σ(shares × current_mid)
  total_value:     string; // collateral + positions_value
  unrealized_pnl:  string; // Σ(shares × (current_mid − entry_price))
  position_count:  number;
  positions:       EnrichedPosition[];
  prices_live:     boolean; // false if Polymarket CLOB was unreachable
}

export async function GET(): Promise<NextResponse> {
  try {
    // ── 1. Fetch DRADIS data concurrently ──────────────────────────────────
    const [posRes, pnlRes] = await Promise.all([
      fetch(`${DRADIS_API}/api/positions`,            { cache: 'no-store', headers: dradisHeaders() }),
      fetch(`${DRADIS_API}/api/pnl/history?limit=1`,  { cache: 'no-store', headers: dradisHeaders() }),
    ]);

    if (!posRes.ok || !pnlRes.ok) {
      return NextResponse.json({ error: 'DRADIS engine unreachable' }, { status: 502 });
    }

    const positions: Array<{
      ts: string; strategy: string; token_id: string; market: string;
      side: string; entry_price: string; shares: string; ghost_mode: boolean;
    }> = await posRes.json();

    const pnlHistory: Array<{ ts: string; session_pnl: string; collateral: string }> =
      await pnlRes.json();

    const collateral = pnlHistory.length > 0 ? parseFloat(pnlHistory[0].collateral) : 0;

    // ── 2. Fetch live midpoint prices from Polymarket public CLOB ──────────
    const uniqueTokens = [...new Set(positions.map(p => p.token_id))];
    const priceMap: Record<string, number> = {};
    let pricesLive = true;

    if (uniqueTokens.length > 0) {
      const results = await Promise.allSettled(
        uniqueTokens.map(async tokenId => {
          const res = await fetch(`${POLY_CLOB}/midpoint?token_id=${tokenId}`, {
            cache: 'no-store',
          });
          if (!res.ok) throw new Error(`midpoint ${res.status}`);
          const data = await res.json() as { mid?: string };
          const mid = parseFloat(data.mid ?? '0');
          if (mid > 0) priceMap[tokenId] = mid;
        }),
      );
      // Mark prices as not fully live if any token price fetch failed
      if (results.some(r => r.status === 'rejected')) {
        pricesLive = false;
      }
    }

    // ── 3. Compute totals ──────────────────────────────────────────────────
    let positionsValue = 0;
    let unrealizedPnl  = 0;

    const enriched: EnrichedPosition[] = positions.map(p => {
      const shares      = parseFloat(p.shares);
      const entryPrice  = parseFloat(p.entry_price);
      const currentPrice = priceMap[p.token_id] ?? entryPrice; // fallback to entry

      const value  = shares * currentPrice;
      const pnl    = shares * (currentPrice - entryPrice);

      positionsValue += value;
      unrealizedPnl  += pnl;

      return {
        ...p,
        current_price:   currentPrice.toFixed(4),
        value:           value.toFixed(2),
        unrealized_pnl:  pnl.toFixed(2),
      };
    });

    const totalValue = collateral + positionsValue;

    const payload: PortfolioValueResponse = {
      collateral:      collateral.toFixed(2),
      positions_value: positionsValue.toFixed(2),
      total_value:     totalValue.toFixed(2),
      unrealized_pnl:  unrealizedPnl.toFixed(2),
      position_count:  positions.length,
      positions:       enriched,
      prices_live:     pricesLive,
    };

    return NextResponse.json(payload);

  } catch (err) {
    console.error('[portfolio route]', err);
    return NextResponse.json({ error: 'internal error' }, { status: 500 });
  }
}
