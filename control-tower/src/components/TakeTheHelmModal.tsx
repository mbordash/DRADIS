'use client';

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

// "Take the Helm": the operator enters a position the vipers' gates would never
// produce, states a falsifiable reason for it, and the engine holds the exit.
//
// A sibling of Deploy Squadron, not a mode inside it: both create a squadron,
// and the market browser is the shared part. The flow is four steps —
//
//   1. pick a market and deploy a Helm squadron onto it (class `helm`, one
//      viper, no Raptors, whatever the venue files the market under);
//   2. the conviction form: thesis, probability, horizon, "what would make me
//      wrong", and the exit posture as explicit fields — never parsed from the
//      prose;
//   3. the deterministic validator blocks with every failing rule named; the
//      fee verdict and the LLM critique are shown; the critique advises and
//      cannot block or delay past its timeout;
//   4. the operator confirms having read it and acknowledges. The engine
//      enters on the next tick and enforces the posture from then on.

import { useCallback, useEffect, useRef, useState } from 'react';
import type { AvailableMarket, HelmIntentDetail, MarketType } from '@/lib/types';
import {
  acknowledgeHelmIntent, createHelmIntent, deploySquadron, getAvailableMarkets,
  getDeployments, getHelmIntent, HelmApiError,
} from '@/lib/api';
import { MarketBrowser } from './DeploySquadronModal';

type Step = 'market' | 'deploying' | 'form' | 'critique' | 'done';

const BROWSABLE: { type: MarketType; icon: string; label: string }[] = [
  { type: 'crypto',   icon: '🪙', label: 'Crypto' },
  { type: 'sports',   icon: '🏈', label: 'Sports' },
  { type: 'politics', icon: '🗳️', label: 'Politics' },
];

/** How long to wait for the queued deploy to become a live squadron. A deploy
 *  is queued and claimed within seconds; a minute without the row reporting
 *  its squadron means the modal has lost the thread, not that the squadron
 *  failed — it may well be patrolling. The message says where to look. */
const DEPLOY_WAIT_MS = 60_000;
/** How long the form waits on the critique before offering to proceed
 *  without it. The server's own timeout records `unavailable`; this only
 *  decides when the button appears. */
const CRITIQUE_WAIT_MS = 45_000;

interface Props {
  isOpen: boolean;
  onClose: () => void;
  /** Called once an intent is acknowledged, with the squadron it lives on. */
  onDone?: (squadronId: string) => void;
}

interface FormState {
  side: 'YES' | 'NO';
  thesis: string;
  confidence: string;
  horizon: 'expiry' | 'sooner';
  falsification: string;
  entry_kind: 'taker' | 'resting';
  entry_limit_price: string;
  size_usdc: string;
  stop_price: string;
  take_profit_price: string;
  time_limit_at: string;
  hold_to_settlement: boolean;
  catastrophic_floor_pct: string;
}

const EMPTY_FORM: FormState = {
  side: 'YES', thesis: '', confidence: '0.65', horizon: 'expiry', falsification: '',
  entry_kind: 'taker', entry_limit_price: '', size_usdc: '4', stop_price: '',
  take_profit_price: '', time_limit_at: '', hold_to_settlement: false, catastrophic_floor_pct: '',
};

const inputCls = 'w-full bg-[#0a0a14] border border-[#1e1e32] rounded px-2 py-1.5 text-xs font-mono text-gray-200 focus:outline-none focus:border-teal-500/50';
const labelCls = 'block text-[10px] font-mono uppercase tracking-wide text-gray-500 mb-1';

function orNull(s: string): string | null {
  const t = s.trim();
  return t === '' ? null : t;
}

/** A `datetime-local` value to RFC 3339 in UTC, or null when blank. */
function toRfc3339(local: string): string | null {
  if (!local.trim()) return null;
  const d = new Date(local);
  return Number.isNaN(d.getTime()) ? null : d.toISOString();
}

export default function TakeTheHelmModal({ isOpen, onClose, onDone }: Props) {
  const [step, setStep] = useState<Step>('market');

  // Step 1: market and squadron.
  const [browseType, setBrowseType] = useState<MarketType>('crypto');
  const [markets, setMarkets] = useState<AvailableMarket[]>([]);
  const [loadingMarkets, setLoadingMarkets] = useState(false);
  const [selectedMarket, setSelectedMarket] = useState<string | null>(null);
  const [name, setName] = useState('');
  const [squadronId, setSquadronId] = useState<string | null>(null);

  // Step 2: the form.
  const [form, setForm] = useState<FormState>(EMPTY_FORM);
  const [violations, setViolations] = useState<string[]>([]);

  // Step 3: critique and acknowledgement.
  const [detail, setDetail] = useState<HelmIntentDetail | null>(null);
  const [critiqueWaitedOut, setCritiqueWaitedOut] = useState(false);
  const [readCritique, setReadCritique] = useState(false);

  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const timers = useRef<number[]>([]);

  const clearTimers = () => {
    for (const t of timers.current) window.clearTimeout(t);
    timers.current = [];
  };

  // Reset on open so a second use starts clean.
  useEffect(() => {
    if (!isOpen) return;
    setStep('market'); setSelectedMarket(null); setName(''); setSquadronId(null);
    setForm(EMPTY_FORM); setViolations([]); setDetail(null); setCritiqueWaitedOut(false);
    setReadCritique(false); setBusy(false); setError(null);
    return clearTimers;
  }, [isOpen]);

  // Browse markets for the chosen type.
  useEffect(() => {
    if (!isOpen || step !== 'market') return;
    let cancelled = false;
    setLoadingMarkets(true);
    setMarkets([]);
    setSelectedMarket(null);
    getAvailableMarkets(browseType, { expiryWindow: browseType === 'crypto' ? '4h' : undefined })
      .then((r) => { if (!cancelled) setMarkets(r.markets); })
      .catch((e) => { if (!cancelled) setError(e instanceof Error ? e.message : String(e)); })
      .finally(() => { if (!cancelled) setLoadingMarkets(false); });
    return () => { cancelled = true; };
  }, [isOpen, step, browseType]);

  const selected = markets.find((m) => m.condition_id === selectedMarket) ?? null;

  // ── Step 1 → 2: deploy and wait for the squadron id ───────────────────────
  const deploy = useCallback(async () => {
    if (!selectedMarket) return;
    setBusy(true); setError(null);
    try {
      const res = await deploySquadron({
        mode: 'manual', market_type: 'helm', market_id: selectedMarket,
        raptors: [], vipers: ['helm'], name: name.trim() || undefined,
      });
      if (!res.success || !res.squadron_id) {
        setError(res.error || 'Deployment refused');
        setBusy(false);
        return;
      }
      const deploymentId = res.squadron_id;
      setStep('deploying');
      const startedAt = Date.now();
      const poll = async () => {
        try {
          const rows = await getDeployments();
          const row = rows.find((d) => d.id === deploymentId);
          if (row?.status === 'failed') {
            setError(row.error || 'Deployment failed');
            setStep('market'); setBusy(false);
            return;
          }
          if (row?.status === 'active' && row.squadron_id) {
            setSquadronId(row.squadron_id);
            setStep('form'); setBusy(false);
            return;
          }
        } catch {
          // A failed poll is retried; the engine's queue is the truth.
        }
        if (Date.now() - startedAt > DEPLOY_WAIT_MS) {
          setError(
            `No squadron id reported for deployment ${deploymentId} within 60 s. The squadron may already be up: ` +
            'look for a helm-open-… entry in the CAG list and give it an intent from its page. If the deployment ' +
            'shows failed in Deployments, the reason is on that row.',
          );
          setStep('market'); setBusy(false);
          return;
        }
        timers.current.push(window.setTimeout(poll, 2000));
      };
      timers.current.push(window.setTimeout(poll, 1500));
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
      setBusy(false);
    }
  }, [selectedMarket, name]);

  // ── Step 2 → 3: submit the conviction ─────────────────────────────────────
  const submit = useCallback(async () => {
    if (!squadronId) return;
    setBusy(true); setError(null); setViolations([]);
    const confidence = Number.parseFloat(form.confidence);
    try {
      const intent = await createHelmIntent({
        squadron_id: squadronId,
        side: form.side,
        thesis: form.thesis,
        confidence: Number.isFinite(confidence) ? confidence : -1,
        horizon: form.horizon,
        falsification: form.falsification,
        entry_kind: form.entry_kind,
        entry_limit_price: form.entry_kind === 'resting' ? orNull(form.entry_limit_price) : null,
        size_usdc: form.size_usdc.trim(),
        stop_price: orNull(form.stop_price),
        take_profit_price: orNull(form.take_profit_price),
        time_limit_at: toRfc3339(form.time_limit_at),
        hold_to_settlement: form.hold_to_settlement,
        catastrophic_floor_pct: orNull(form.catastrophic_floor_pct),
      });
      setStep('critique');
      setCritiqueWaitedOut(false);
      // Poll the intent until the critique answers (text or unavailable).
      const startedAt = Date.now();
      const poll = async () => {
        try {
          const d = await getHelmIntent(intent.id);
          setDetail(d);
          if (d.intent.critique != null) { setBusy(false); return; }
        } catch {
          // Retry on the next tick.
        }
        if (Date.now() - startedAt > CRITIQUE_WAIT_MS) {
          setCritiqueWaitedOut(true); setBusy(false);
          return;
        }
        timers.current.push(window.setTimeout(poll, 1500));
      };
      poll();
    } catch (e) {
      if (e instanceof HelmApiError) {
        setViolations(e.violations.length > 0 ? e.violations : [e.message]);
      } else {
        setError(e instanceof Error ? e.message : String(e));
      }
      setBusy(false);
    }
  }, [squadronId, form]);

  // ── Step 3 → 4: acknowledge ───────────────────────────────────────────────
  const acknowledge = useCallback(async () => {
    if (!detail) return;
    setBusy(true); setError(null); setViolations([]);
    try {
      const d = await acknowledgeHelmIntent(detail.intent.id);
      setDetail(d);
      setStep('done');
      onDone?.(d.intent.squadron_id);
    } catch (e) {
      if (e instanceof HelmApiError) {
        if (e.status === 409 && e.retryAfterSecs) {
          setError(`${e.message} — retrying in ${e.retryAfterSecs}s`);
          timers.current.push(window.setTimeout(acknowledge, e.retryAfterSecs * 1000));
          return;
        }
        setViolations(e.violations.length > 0 ? e.violations : [e.message]);
      } else {
        setError(e instanceof Error ? e.message : String(e));
      }
    } finally {
      setBusy(false);
    }
  }, [detail, onDone]);

  if (!isOpen) return null;

  const set = <K extends keyof FormState>(k: K, v: FormState[K]) => setForm((f) => ({ ...f, [k]: v }));
  const critique = detail?.intent.critique ?? null;
  const critiqueUnavailable = critique?.startsWith('unavailable:') ?? false;
  const canAcknowledge = !busy && readCritique && (critique != null || critiqueWaitedOut);

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 backdrop-blur-sm">
      <div className="bg-[#0d0d1a] rounded-xl border border-[#1e1e32] shadow-2xl w-full max-w-2xl mx-4 overflow-hidden max-h-[92vh] flex flex-col">
        {/* Header */}
        <div className="flex items-center justify-between px-5 py-4 border-b border-[#1e1e32]">
          <div className="flex items-center gap-2">
            <span className="text-lg">🧭</span>
            <h2 className="text-sm font-mono font-semibold text-gray-200">Take the Helm</h2>
            <span className="text-[10px] font-mono text-gray-500 ml-2">
              {step === 'market' && '1 · market'}
              {step === 'deploying' && '1 · deploying'}
              {step === 'form' && '2 · conviction'}
              {step === 'critique' && '3 · critique and acknowledgement'}
              {step === 'done' && '4 · entered'}
            </span>
          </div>
          <button onClick={onClose} className="text-gray-500 hover:text-gray-300 transition-colors p-1" aria-label="Close">
            <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
            </svg>
          </button>
        </div>

        <div className="px-5 py-4 space-y-4 overflow-y-auto">
          {error && (
            <div className="rounded border border-red-500/30 bg-red-500/10 px-3 py-2 text-xs font-mono text-red-300">{error}</div>
          )}

          {/* ── Step 1: market ─────────────────────────────────────────────── */}
          {(step === 'market' || step === 'deploying') && (
            <>
              <p className="text-xs font-mono text-gray-400">
                Pick the market. The squadron you deploy carries one viper, Helm, and no Raptors — whatever the
                venue files the market under, it resolves to class <span className="text-teal-300">helm</span>.
                The posture you state next is the whole exit plan.
              </p>
              <div className="flex gap-2">
                {BROWSABLE.map((b) => (
                  <button
                    key={b.type}
                    onClick={() => setBrowseType(b.type)}
                    disabled={step === 'deploying'}
                    className={`flex-1 rounded border px-3 py-2 text-xs font-mono transition-colors ${
                      browseType === b.type
                        ? 'border-teal-500/50 bg-teal-500/10 text-teal-200'
                        : 'border-[#1e1e32] text-gray-400 hover:bg-white/[0.02]'
                    }`}
                  >
                    {b.icon} {b.label}
                  </button>
                ))}
              </div>
              <MarketBrowser markets={markets} selected={selectedMarket} onSelect={setSelectedMarket} loading={loadingMarkets} />
              {selected && (
                <div className="rounded border border-[#1e1e32] bg-[#0a0a14] px-3 py-2 text-[11px] font-mono text-gray-400 space-y-1">
                  <p>
                    DRADIS files this market as <span className="text-gray-200">{selected.market_class}</span>; a
                    {' '}{selected.market_class} squadron would carry that class&apos;s Raptors and vipers.
                  </p>
                  <p>
                    Deployed as Helm it resolves to class <span className="text-teal-300">helm</span> — one viper,
                    no Raptors. Closes {new Date(selected.end_date).toLocaleString()}.
                  </p>
                </div>
              )}
              <div>
                <label className={labelCls}>Squadron name (optional)</label>
                <input className={inputCls} value={name} onChange={(e) => setName(e.target.value)} placeholder="e.g. btc-3pm-fade" disabled={step === 'deploying'} />
              </div>
              <button
                onClick={deploy}
                disabled={!selectedMarket || busy || step === 'deploying'}
                className="w-full rounded border border-teal-500/40 bg-teal-500/10 px-3 py-2 text-xs font-mono text-teal-200 hover:bg-teal-500/20 disabled:opacity-40 disabled:cursor-not-allowed"
              >
                {step === 'deploying' ? 'Deploying the Helm squadron…' : 'Deploy Helm squadron'}
              </button>
            </>
          )}

          {/* ── Step 2: conviction form ────────────────────────────────────── */}
          {step === 'form' && squadronId && (
            <>
              <p className="text-xs font-mono text-gray-400">
                Squadron <span className="text-teal-300">{squadronId}</span> is patrolling. State the conviction. The
                exit posture below is what the engine enforces; the prose is kept beside it and is never parsed.
              </p>
              <div className="grid grid-cols-2 gap-3">
                <div>
                  <label className={labelCls}>Side</label>
                  <select className={inputCls} value={form.side} onChange={(e) => set('side', e.target.value as 'YES' | 'NO')}>
                    <option value="YES">YES</option>
                    <option value="NO">NO</option>
                  </select>
                </div>
                <div>
                  <label className={labelCls}>Confidence (probability, strictly inside 0–1)</label>
                  <input className={inputCls} type="number" min="0.01" max="0.99" step="0.01" value={form.confidence} onChange={(e) => set('confidence', e.target.value)} />
                </div>
              </div>
              <div>
                <label className={labelCls}>Thesis — what you believe and why</label>
                <textarea className={`${inputCls} h-20`} value={form.thesis} onChange={(e) => set('thesis', e.target.value)} />
              </div>
              <div>
                <label className={labelCls}>What would make me wrong</label>
                <textarea className={`${inputCls} h-16`} value={form.falsification} onChange={(e) => set('falsification', e.target.value)} placeholder="The observable condition that would show the thesis wrong" />
              </div>
              <div className="grid grid-cols-3 gap-3">
                <div>
                  <label className={labelCls}>Horizon</label>
                  <select className={inputCls} value={form.horizon} onChange={(e) => set('horizon', e.target.value as 'expiry' | 'sooner')}>
                    <option value="expiry">Ride to expiry</option>
                    <option value="sooner">Expect to exit sooner</option>
                  </select>
                </div>
                <div>
                  <label className={labelCls}>Entry</label>
                  <select className={inputCls} value={form.entry_kind} onChange={(e) => set('entry_kind', e.target.value as 'taker' | 'resting')}>
                    <option value="taker">Taker (buy the ask now)</option>
                    <option value="resting">Resting bid (post-only)</option>
                  </select>
                </div>
                <div>
                  <label className={labelCls}>Size (USDC)</label>
                  <input className={inputCls} value={form.size_usdc} onChange={(e) => set('size_usdc', e.target.value)} />
                </div>
              </div>
              {form.entry_kind === 'resting' && (
                <div>
                  <label className={labelCls}>Resting bid price (below the ask)</label>
                  <input className={inputCls} value={form.entry_limit_price} onChange={(e) => set('entry_limit_price', e.target.value)} placeholder="0.40" />
                </div>
              )}
              <div className="rounded border border-[#1e1e32] bg-[#0a0a14] p-3 space-y-3">
                <p className="text-[10px] font-mono uppercase tracking-wide text-teal-400">Exit posture — the engine enforces exactly this</p>
                <div className="grid grid-cols-3 gap-3">
                  <div>
                    <label className={labelCls}>Stop price</label>
                    <input className={inputCls} value={form.stop_price} onChange={(e) => set('stop_price', e.target.value)} placeholder="below the bid" disabled={form.hold_to_settlement} />
                  </div>
                  <div>
                    <label className={labelCls}>Take-profit price</label>
                    <input className={inputCls} value={form.take_profit_price} onChange={(e) => set('take_profit_price', e.target.value)} placeholder="above the ask" />
                  </div>
                  <div>
                    <label className={labelCls}>Catastrophic floor (fraction of entry)</label>
                    <input className={inputCls} value={form.catastrophic_floor_pct} onChange={(e) => set('catastrophic_floor_pct', e.target.value)} placeholder="0.50" />
                  </div>
                </div>
                <div className="grid grid-cols-2 gap-3 items-end">
                  <div>
                    <label className={labelCls}>Time limit (before the market&apos;s close)</label>
                    <input className={inputCls} type="datetime-local" value={form.time_limit_at} onChange={(e) => set('time_limit_at', e.target.value)} />
                  </div>
                  <label className="flex items-center gap-2 text-xs font-mono text-gray-300 pb-1.5">
                    <input type="checkbox" checked={form.hold_to_settlement} onChange={(e) => { set('hold_to_settlement', e.target.checked); if (e.target.checked) set('stop_price', ''); }} />
                    Hold to settlement (no price stop; the floor is the insurance)
                  </label>
                </div>
                <p className="text-[10px] font-mono text-gray-500">
                  A posture must name an exit: a stop, a take-profit, a time limit, or hold to settlement. The validator
                  checks each against the live book and names every rule it fails.
                </p>
              </div>
              {violations.length > 0 && (
                <div className="rounded border border-amber-500/30 bg-amber-500/10 px-3 py-2">
                  <p className="text-[10px] font-mono uppercase tracking-wide text-amber-300 mb-1">Refused — fix each of these</p>
                  <ul className="list-disc pl-4 space-y-0.5">
                    {violations.map((v, i) => <li key={i} className="text-xs font-mono text-amber-200">{v}</li>)}
                  </ul>
                </div>
              )}
              <button
                onClick={submit}
                disabled={busy}
                className="w-full rounded border border-teal-500/40 bg-teal-500/10 px-3 py-2 text-xs font-mono text-teal-200 hover:bg-teal-500/20 disabled:opacity-40 disabled:cursor-not-allowed"
              >
                {busy ? 'Submitting…' : 'Submit conviction'}
              </button>
            </>
          )}

          {/* ── Step 3: critique and acknowledgement ───────────────────────── */}
          {step === 'critique' && detail && (
            <>
              <div className="rounded border border-[#1e1e32] bg-[#0a0a14] px-3 py-2 text-xs font-mono text-gray-300 space-y-1">
                <p>Intent <span className="text-teal-300">#{detail.intent.id}</span> on {detail.intent.market_name} — {detail.intent.side}, confidence {(detail.intent.first.confidence * 100).toFixed(0)}%, status {detail.intent.status}{detail.intent.ghost ? ' (ghost)' : ''}.</p>
                <p className="text-gray-400">Stored as first written. Any edit after reading the critique is a new version; the first never changes.</p>
              </div>
              <div className="rounded border border-amber-500/20 bg-amber-500/5 px-3 py-2">
                <p className="text-[10px] font-mono uppercase tracking-wide text-amber-300 mb-1">Fee verdict</p>
                <p className="text-xs font-mono text-amber-100">{detail.intent.fee_verdict ?? '—'}</p>
              </div>
              <div className="rounded border border-indigo-500/20 bg-indigo-500/5 px-3 py-2">
                <p className="text-[10px] font-mono uppercase tracking-wide text-indigo-300 mb-1">
                  Critique {detail.intent.critique_model && !critiqueUnavailable ? `· ${detail.intent.critique_model}` : ''}
                </p>
                {critique == null && !critiqueWaitedOut && (
                  <p className="text-xs font-mono text-gray-400 animate-pulse">Reading your reasoning… (it sees the thesis, the probability, the horizon, the falsification, the market name and the current price — nothing else)</p>
                )}
                {critique == null && critiqueWaitedOut && (
                  <p className="text-xs font-mono text-gray-400">No critique arrived in time. Acknowledging records it as unavailable; the trade proceeds either way.</p>
                )}
                {critique != null && (
                  <p className={`text-xs font-mono whitespace-pre-wrap ${critiqueUnavailable ? 'text-gray-400' : 'text-indigo-100'}`}>{critique}</p>
                )}
              </div>
              {violations.length > 0 && (
                <div className="rounded border border-amber-500/30 bg-amber-500/10 px-3 py-2">
                  <p className="text-[10px] font-mono uppercase tracking-wide text-amber-300 mb-1">Not acknowledged</p>
                  <ul className="list-disc pl-4 space-y-0.5">
                    {violations.map((v, i) => <li key={i} className="text-xs font-mono text-amber-200">{v}</li>)}
                  </ul>
                  <p className="text-[10px] font-mono text-gray-500 mt-1">Revise the intent from the squadron page, then acknowledge there.</p>
                </div>
              )}
              <label className="flex items-center gap-2 text-xs font-mono text-gray-300">
                <input type="checkbox" checked={readCritique} onChange={(e) => setReadCritique(e.target.checked)} />
                I have read the critique and the fee verdict. The engine will enter on the next tick and enforce the posture above.
              </label>
              <button
                onClick={acknowledge}
                disabled={!canAcknowledge}
                className="w-full rounded border border-teal-500/40 bg-teal-500/10 px-3 py-2 text-xs font-mono text-teal-200 hover:bg-teal-500/20 disabled:opacity-40 disabled:cursor-not-allowed"
              >
                {busy ? 'Acknowledging…' : 'Acknowledge and enter'}
              </button>
            </>
          )}

          {/* ── Step 4: done ───────────────────────────────────────────────── */}
          {step === 'done' && detail && (
            <div className="rounded border border-teal-500/30 bg-teal-500/10 px-3 py-3 text-xs font-mono text-teal-100 space-y-1">
              <p>Intent #{detail.intent.id} acknowledged on {detail.intent.squadron_id}.</p>
              <p className="text-gray-300">The engine enters on its next tick, subject to its own risk gates (kill switch, live-orders gate, exposure cap, collateral, drawdown). Follow it on the squadron page.</p>
              <button onClick={onClose} className="mt-2 rounded border border-[#1e1e32] px-3 py-1.5 text-xs font-mono text-gray-300 hover:bg-white/[0.03]">Close</button>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
