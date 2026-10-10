'use client';

import { useState } from 'react';
import dynamic from 'next/dynamic';
import useSWR from 'swr';
import { getMarketDetail, getMarketBook, getMarketPrints, getMarketHistory } from '@/lib/api';
import { DEMO_MODE } from '@/lib/demo';
import { fmtAgo, fmtDur, isTroubled } from '@/components/ViperCard';
import type {
  MarketDetail, MarketType, SideQuote, LegBook, BookLevel, ModelReadingView, SportsSideLine,
} from '@/lib/types';
import { fmtPx, fmtCents, fmtCountdown, fmtSci, pct, fmtClock, fmtLocal } from './format';

const MarketHistoryChart = dynamic(() => import('./MarketHistoryChart'), { ssr: false });

const CARD = 'card p-4 border border-[#1e1e32] bg-[#0d0d1a]';
const H3 = 'text-xs font-mono uppercase tracking-wide text-gray-400';
const CAPTION = 'text-[10px] text-gray-600 font-mono';
const ERR = 'card px-4 py-3 border border-red-500/30 bg-red-500/5 text-red-300 text-xs font-mono';

export interface MarketDetailPanelProps {
  id: string;
  marketType: MarketType;
  onOpenSquadron: (squadronId: string) => void;
  onTakeHelm: (m: { condition_id: string; question: string; market_class: MarketType; end_date: string; criteria?: string }) => void;
}

function num(s: string | null | undefined): number | null {
  if (s === null || s === undefined || s === '') return null;
  const n = Number(s);
  return Number.isFinite(n) ? n : null;
}

function StateBadge({ d }: { d: MarketDetail }) {
  if (d.state === 'live') {
    return <span className="px-2 py-0.5 rounded text-[10px] font-mono bg-green-500/10 text-green-300">LIVE</span>;
  }
  if (d.resolution) {
    return <span className="px-2 py-0.5 rounded text-[10px] font-mono bg-indigo-500/10 text-indigo-300">RESOLVED {d.resolution.toUpperCase()}</span>;
  }
  return <span className="px-2 py-0.5 rounded text-[10px] font-mono bg-gray-500/10 text-gray-400">CLOSED</span>;
}

function Facts({ d }: { d: MarketDetail }) {
  const [open, setOpen] = useState(false);
  const long = d.criteria.length > 300;
  const shown = long && !open ? `${d.criteria.slice(0, 300)}…` : d.criteria;
  return (
    <div className={`${CARD} space-y-2`}>
      <div className="flex items-start justify-between gap-3">
        <h3 className={H3}>Market</h3>
        <StateBadge d={d} />
      </div>
      <p className="text-sm font-mono text-gray-200">{d.question}</p>
      {d.criteria && (
        <div>
          <p className="text-[11px] font-mono text-gray-500 whitespace-pre-wrap">{shown}</p>
          {long && (
            <button onClick={() => setOpen(o => !o)} className="text-[10px] font-mono text-teal-300 hover:text-teal-200">
              {open ? 'Show less' : 'Show more'}
            </button>
          )}
        </div>
      )}
      {d.leg_labels && (
        <p className="text-[11px] font-mono text-gray-400">
          YES: <span className="text-gray-200">{d.leg_labels[0]}</span>
          {' · '}NO: <span className="text-gray-200">{d.leg_labels[1]}</span>
        </p>
      )}
      <p className="text-[11px] font-mono text-gray-400">
        Closes {fmtLocal(d.close_time)}
        {d.state === 'live' && (
          <span className="text-gray-500">
            {d.past_listed_close ? ' (past its listed close, still trading)' : ` (${fmtCountdown(d.secs_to_close)} left)`}
          </span>
        )}
      </p>
    </div>
  );
}

function Cell({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex justify-between text-[11px] font-mono">
      <span className="text-gray-500">{label}</span>
      <span className="text-gray-200">{value}</span>
    </div>
  );
}

function SideBox({ title, q, tone }: { title: string; q: SideQuote; tone: string }) {
  return (
    <div className="rounded border border-[#1e1e32] p-2 space-y-1">
      <p className={`text-[10px] font-mono uppercase ${tone}`}>{title}</p>
      <Cell label="bid" value={fmtPx(q.bid)} />
      <Cell label="ask" value={fmtPx(q.ask)} />
      <Cell label="mid" value={fmtPx(q.mid)} />
      <Cell label="spread" value={fmtPx(q.spread)} />
    </div>
  );
}

function Quotes({ d }: { d: MarketDetail }) {
  return (
    <div className={`${CARD} space-y-2`}>
      <h3 className={H3}>Venue picture</h3>
      {d.quotes ? (
        <>
          <div className="grid grid-cols-2 gap-3">
            <SideBox title="YES" q={d.quotes.yes} tone="text-indigo-300" />
            <SideBox title="NO" q={d.quotes.no} tone="text-emerald-300" />
          </div>
          <Cell label="ask sum" value={fmtPx(d.quotes.ask_sum)} />
        </>
      ) : (
        <p className="text-xs font-mono text-amber-300">{d.quotes_unavailable ?? '–'}</p>
      )}
      <p className={CAPTION}>Top of book from the venue; refreshed every 5 s.</p>
    </div>
  );
}

function Ladder({ title, leg }: { title: string; leg: LegBook | null }) {
  const rows = (lv: BookLevel[], tone: string) => lv.slice(0, 6).map((l, i) => (
    <div key={i} className={`flex justify-between text-[11px] font-mono ${tone}`}>
      <span>{fmtPx(l.price)}</span>
      <span className="text-gray-500">{fmtPx(l.size)}</span>
    </div>
  ));
  return (
    <div className="rounded border border-[#1e1e32] p-2 space-y-1">
      <p className="text-[10px] font-mono uppercase text-gray-400">{title}</p>
      {leg ? (
        <>
          {rows([...leg.asks].slice(0, 6).reverse(), 'text-red-300')}
          <div className="border-t border-[#1e1e32]" />
          {rows(leg.bids, 'text-green-300')}
        </>
      ) : (
        <p className="text-[11px] font-mono text-gray-600">No depth published for this leg</p>
      )}
    </div>
  );
}

function Pending({ text }: { text: string }) {
  return <p className="text-[11px] font-mono text-gray-600">{text}</p>;
}

function Ours({ d }: { d: MarketDetail }) {
  const { open_positions: pos, trades, entries } = d.ours;
  const empty = pos.length === 0 && trades.length === 0 && entries.length === 0;
  return (
    <div className={`${CARD} space-y-3`}>
      <h3 className={H3}>DRADIS on this market</h3>
      {empty ? (
        <p className="text-xs font-mono text-gray-500">DRADIS has not traded this market.</p>
      ) : (
        <>
          {pos.length > 0 && (
            <div className="space-y-1">
              <p className="text-[10px] font-mono uppercase text-gray-500">Open positions</p>
              {pos.map((p, i) => (
                <div key={i} className="flex flex-wrap gap-x-3 text-[11px] font-mono text-gray-300">
                  <span className="text-teal-300">{p.strategy}</span>
                  <span>{p.side}</span>
                  <span>{fmtPx(p.shares)} sh</span>
                  <span>entry {fmtPx(p.entry_price)}</span>
                  <span>now {fmtPx(p.current_price ?? null)}</span>
                </div>
              ))}
            </div>
          )}
          {trades.length > 0 && (
            <div className="space-y-1">
              <p className="text-[10px] font-mono uppercase text-gray-500">Closed trades</p>
              {trades.map((t, i) => {
                const p = num(t.pnl);
                return (
                  <div key={i} className="flex flex-wrap gap-x-3 text-[11px] font-mono text-gray-300">
                    <span className="text-gray-500">{fmtLocal(t.ts)}</span>
                    <span className="text-teal-300">{t.strategy}</span>
                    <span>{t.side}</span>
                    <span>{fmtPx(t.entry_price)} → {fmtPx(t.exit_price)}</span>
                    <span className={p === null ? 'text-gray-500' : p >= 0 ? 'text-green-400' : 'text-red-400'}>
                      {p === null ? '–' : `${p >= 0 ? '+' : '-'}$${Math.abs(p).toFixed(2)}`}
                    </span>
                    <span className="text-gray-500 truncate max-w-[16rem]" title={t.reason}>{t.reason}</span>
                  </div>
                );
              })}
            </div>
          )}
          {entries.length > 0 && (
            <div className="space-y-1">
              <p className="text-[10px] font-mono uppercase text-gray-500">Entries</p>
              {entries.map((e, i) => (
                <div key={i} className="flex flex-wrap gap-x-3 text-[11px] font-mono text-gray-300">
                  <span className="text-gray-500">{fmtLocal(e.ts)}</span>
                  <span className="text-teal-300">{e.strategy}</span>
                  <span>{e.side}</span>
                  <span>{fmtPx(e.entry_price)}</span>
                  <span>{fmtPx(e.shares)} sh</span>
                </div>
              ))}
            </div>
          )}
        </>
      )}
      <p className={CAPTION}>Trades match by market name; a market the venue renamed loses its older trades here.</p>
    </div>
  );
}

function ModelCard({ m }: { m: ModelReadingView }) {
  const req = num(m.required_edge);
  const tone = (e: string | null) => {
    const v = num(e);
    return v !== null && req !== null && v >= req ? 'text-green-400' : 'text-gray-400';
  };
  return (
    <div className="rounded border border-[#1e1e32] p-3 space-y-1">
      <Cell label="fair YES" value={m.fair_yes.toFixed(3)} />
      <Cell label="fair NO" value={m.fair_no.toFixed(3)} />
      <div className="flex justify-between text-[11px] font-mono">
        <span className="text-gray-500">edge YES (need {fmtCents(m.required_edge)})</span>
        <span className={tone(m.edge_yes)}>{fmtCents(m.edge_yes)}</span>
      </div>
      <div className="flex justify-between text-[11px] font-mono">
        <span className="text-gray-500">edge NO (need {fmtCents(m.required_edge)})</span>
        <span className={tone(m.edge_no)}>{fmtCents(m.edge_no)}</span>
      </div>
      <Cell label="σ used / realized" value={`${fmtSci(m.sigma_used)} / ${fmtSci(m.sigma_realized)}`} />
      <Cell label="event multiplier" value={String(m.event_mult)} />
      <Cell label="spot / strike" value={`${m.spot} / ${m.strike}`} />
      <Cell label="time left" value={fmtDur(m.secs_left)} />
      <Cell label="samples" value={String(m.samples)} />
    </div>
  );
}

function SportsSide({ s, title }: { s: SportsSideLine | null; title: string }) {
  return (
    <div className="rounded border border-[#1e1e32] p-2 space-y-1">
      <p className="text-[10px] font-mono uppercase text-gray-400">{title}{s ? `: ${s.outcome_label}` : ''}</p>
      {s ? (
        <>
          <Cell label="consensus" value={pct(s.consensus)} />
          <Cell label="books" value={String(s.num_books)} />
          <Cell label="dispersion" value={s.dispersion === null ? '–' : pct(s.dispersion)} />
          <Cell label="drift" value={s.drift === null ? '–' : pct(s.drift)} />
          <Cell label="odds age" value={fmtDur(s.odds_age_secs)} />
        </>
      ) : (
        <p className="text-[11px] font-mono text-gray-600">–</p>
      )}
    </div>
  );
}

function Engine({ d, onOpenSquadron }: { d: MarketDetail; onOpenSquadron: (id: string) => void }) {
  const e = d.engine;
  return (
    <div className="space-y-4">
      <div className={`${CARD} space-y-2`}>
        <h3 className={H3}>Engine view</h3>
        {e.squadron ? (
          <>
            <button
              onClick={() => onOpenSquadron(e.squadron!.id)}
              className="inline-flex items-center gap-2 rounded bg-teal-500/10 px-2 py-1 text-[11px] font-mono text-teal-300 hover:bg-teal-500/20"
            >
              {e.squadron.id} · {e.squadron.asset}
              <span className="text-teal-500">{e.squadron.state}</span>
            </button>
            <p className="text-[10px] font-mono uppercase text-gray-500">Squadron verdicts (per squadron, not per market)</p>
            <div className="space-y-1">
              {e.verdicts.map(v => {
                const bad = isTroubled(v);
                return (
                  <div key={`${v.asset}-${v.strategy}`} className="flex items-baseline gap-3 text-[11px] font-mono">
                    <span className="text-gray-200 w-28 shrink-0">{v.strategy}</span>
                    <span className={bad ? 'text-red-400' : 'text-gray-300'}>{v.last_outcome}</span>
                    <span className="text-gray-500 truncate flex-1 min-w-0" title={v.last_reason ?? ''}>{v.last_reason ?? '–'}</span>
                    <span className={bad ? 'text-red-400' : 'text-gray-500'}>{fmtAgo(v.last_eval_secs_ago)}</span>
                  </div>
                );
              })}
            </div>
          </>
        ) : (
          <p className="text-xs font-mono text-gray-500">No squadron is flying this market.</p>
        )}
      </div>

      {(e.model || e.model_unavailable) && (
        <div className={`${CARD} space-y-2`}>
          <h3 className={H3}>Model reading (FairValue, global knobs)</h3>
          {e.model ? <ModelCard m={e.model} /> : <p className="text-xs font-mono text-amber-300">{e.model_unavailable}</p>}
        </div>
      )}

      {(e.sports || e.sports_unavailable) && (
        <div className={`${CARD} space-y-2`}>
          <h3 className={H3}>Bookmaker line</h3>
          {e.sports ? (
            <div className="grid grid-cols-2 gap-3">
              <SportsSide s={e.sports.yes} title="YES" />
              <SportsSide s={e.sports.no} title="NO" />
            </div>
          ) : (
            <p className="text-xs font-mono text-amber-300">{e.sports_unavailable}</p>
          )}
        </div>
      )}
    </div>
  );
}

export default function MarketDetailPanel({ id, marketType, onOpenSquadron, onTakeHelm }: MarketDetailPanelProps) {
  const [hours, setHours] = useState(2);
  const detail = useSWR(['market', id], () => getMarketDetail(id), { refreshInterval: 5_000, keepPreviousData: false });
  const live = detail.data?.state === 'live';
  const book = useSWR(live ? ['market-book', id] : null, () => getMarketBook(id), { refreshInterval: 5_000 });
  const prints = useSWR(live ? ['market-prints', id] : null, () => getMarketPrints(id, 20), { refreshInterval: 15_000 });
  const hist = useSWR(live ? ['market-history', id, hours] : null, () => getMarketHistory(id, hours), { refreshInterval: 30_000, keepPreviousData: true });

  if (detail.error && !detail.data) {
    return <div className={ERR}>{detail.error instanceof Error ? detail.error.message : 'Failed to load market'}</div>;
  }
  const d = detail.data;
  if (!d) return <p className="text-xs font-mono text-gray-500">Loading market…</p>;

  const b = book.data;
  const p = prints.data;
  const h = hist.data;

  return (
    <div className="space-y-4">
      <Facts d={d} />

      {d.state === 'closed' ? (
        <div className={CARD}>
          <p className="text-xs font-mono text-gray-300">
            {d.resolution ? `Resolved ${d.resolution.toUpperCase()}` : 'Closed, awaiting resolution'}
          </p>
        </div>
      ) : (
        <>
          <Quotes d={d} />

          <div className={`${CARD} space-y-2`}>
            <h3 className={H3}>Depth</h3>
            {!b ? <Pending text={book.error ? 'Depth unavailable.' : 'Loading depth…'} />
              : !b.published ? <Pending text="Not published by this venue." />
              : (
                <div className="grid grid-cols-2 gap-3">
                  <Ladder title="YES" leg={b.data.yes} />
                  <Ladder title="NO" leg={b.data.no} />
                </div>
              )}
          </div>

          <div className={`${CARD} space-y-2`}>
            <h3 className={H3}>Prints</h3>
            {!p ? <Pending text={prints.error ? 'Prints unavailable.' : 'Loading prints…'} />
              : !p.published ? <Pending text="Not published by this venue." />
              : p.data.prints.length === 0 ? <Pending text="No prints yet." />
              : (
                <div className="space-y-0.5">
                  {p.data.prints.slice(0, 20).map((r, i) => (
                    <div key={i} className="flex gap-3 text-[11px] font-mono">
                      <span className="text-gray-500">{fmtClock(r.at)}</span>
                      <span className={r.leg_is_yes ? 'text-indigo-300' : 'text-emerald-300'}>{r.leg_is_yes ? 'YES' : 'NO'}</span>
                      <span className={r.taker_side === 'Buy' ? 'text-green-400' : 'text-red-400'}>{r.taker_side}</span>
                      <span className="text-gray-200">{fmtPx(r.price)}</span>
                      <span className="text-gray-500">{fmtPx(r.size)}</span>
                    </div>
                  ))}
                </div>
              )}
          </div>

          <div className={`${CARD} space-y-2`}>
            <div className="flex items-center justify-between">
              <h3 className={H3}>History</h3>
              <div className="flex gap-1">
                {[2, 6, 24].map(n => (
                  <button
                    key={n}
                    onClick={() => setHours(n)}
                    className={`rounded border px-2 py-0.5 text-[10px] font-mono ${
                      hours === n ? 'border-teal-500/50 bg-teal-500/10 text-teal-200' : 'border-[#1e1e32] text-gray-500 hover:text-gray-300'
                    }`}
                  >
                    {n}h
                  </button>
                ))}
              </div>
            </div>
            {!h ? <Pending text={hist.error ? 'History unavailable.' : 'Loading history…'} />
              : !h.published ? <Pending text="Not published by this venue." />
              : <MarketHistoryChart yes={h.data.yes} no={h.data.no} />}
          </div>
        </>
      )}

      <Ours d={d} />
      <Engine d={d} onOpenSquadron={onOpenSquadron} />

      {!DEMO_MODE && d.state === 'live' && (
        <button
          onClick={() => onTakeHelm({
            condition_id: d.market_id,
            question: d.question,
            market_class: marketType,
            end_date: d.close_time ?? '',
            criteria: d.criteria || undefined,
          })}
          className="w-full rounded border border-teal-500/40 bg-teal-500/10 px-3 py-2 text-xs font-mono text-teal-200 hover:bg-teal-500/20"
        >
          Take the Helm on this market
        </button>
      )}
    </div>
  );
}
