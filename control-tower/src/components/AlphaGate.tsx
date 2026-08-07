'use client';

/**
 * AlphaGate — blocking first-run acknowledgment overlay.
 *
 * Shown until the operator records the one-time alpha risk + jurisdiction
 * acknowledgment (POST /api/setup/acknowledge, persisted with a timestamp in
 * the instance DB). Rendered above everything, including the Setup view — no
 * interaction with the instance is possible until accepted.
 *
 * The jurisdiction text is venue-aware: the International build carries the
 * hard US-person warning; the US build a lighter eligibility note.
 */

import { useState } from 'react';
import { acknowledgeAlpha } from '@/lib/setupApi';

const REPO_URL = 'https://github.com/mbordash/DRADIS';

export default function AlphaGate({
  venue,
  appVersion,
  onAcknowledged,
}: {
  venue: 'intl' | 'us';
  appVersion?: string;
  onAcknowledged: () => void;
}) {
  const [riskOk, setRiskOk] = useState(false);
  const [jurisdictionOk, setJurisdictionOk] = useState(false);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const accept = async () => {
    setBusy(true);
    setError(null);
    try {
      await acknowledgeAlpha();
      onAcknowledged();
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to record acknowledgment — is the engine reachable?');
      setBusy(false);
    }
  };

  const checkboxCls =
    'mt-0.5 h-4 w-4 shrink-0 rounded border-gray-600 bg-[#0e0e18] accent-amber-500 cursor-pointer';

  return (
    <div className="fixed inset-0 z-[100] overflow-y-auto bg-black/80 backdrop-blur-sm">
      <div className="min-h-full flex items-center justify-center p-4">
        <div className="w-full max-w-2xl bg-[#13131f] border border-amber-500/40 rounded-2xl p-6 sm:p-8 space-y-5 shadow-2xl">
          <div className="flex items-center gap-3">
            <span className="text-[10px] font-mono font-bold tracking-widest bg-amber-500 text-black rounded px-2 py-1">
              ALPHA
            </span>
            <h2 className="text-lg font-mono text-gray-100">
              DRADIS {venue === 'intl' ? 'International' : 'US'}
              {appVersion ? ` v${appVersion}` : ''} — read before proceeding
            </h2>
          </div>

          {/* ── Risk ─────────────────────────────────────────────────────── */}
          <div className="bg-red-950/40 border border-red-500/30 rounded-xl p-4 space-y-2">
            <h3 className="text-sm font-mono text-red-300">⚠️ Alpha software · real-money risk</h3>
            <ul className="text-xs text-red-200/90 space-y-1 list-disc pl-4">
              <li>DRADIS is <strong>experimental alpha software</strong>. It places live orders with real funds and can <strong>lose some or all of the capital</strong> you give it access to.</li>
              <li>Bugs, venue outages, market conditions, or misconfiguration can produce unexpected trades and losses. Start in GHOST mode and with money you can afford to lose entirely.</li>
              <li>Provided under the GPLv3 <strong>without warranty of any kind</strong>. Nothing here is financial advice.</li>
            </ul>
          </div>

          {/* ── Jurisdiction ─────────────────────────────────────────────── */}
          <div className="bg-amber-950/40 border border-amber-500/30 rounded-xl p-4 space-y-2">
            <h3 className="text-sm font-mono text-amber-300">🌍 Jurisdiction — {venue === 'intl' ? 'International build' : 'US build'}</h3>
            {venue === 'intl' ? (
              <p className="text-xs text-amber-200/90">
                This build trades on Polymarket&apos;s <strong>international CLOB</strong>, which is{' '}
                <strong>not available to US persons</strong>. If you are a US person, do not use this
                build — deploy the separate <strong>DRADIS US</strong> build instead. By continuing you
                confirm you are legally permitted to trade on this venue in your jurisdiction and that
                you bear <strong>sole legal responsibility</strong> for that determination; the DRADIS
                project accepts none.
              </p>
            ) : (
              <p className="text-xs text-amber-200/90">
                This build trades on <strong>US-regulated venues</strong>. You remain responsible for
                confirming that you are eligible to trade on these venues under the laws that apply to
                you (state, residency, and account eligibility rules included).
              </p>
            )}
          </div>

          {/* ── Alpha support policy ─────────────────────────────────────── */}
          <div className="bg-[#0e0e18] border border-[#1e1e32] rounded-xl p-4 space-y-2">
            <h3 className="text-sm font-mono text-gray-300">🧪 During the alpha, support is not included</h3>
            <ul className="text-xs text-gray-400 space-y-1 list-disc pl-4">
              <li>For setup help, ask an AI assistant (ChatGPT, Gemini, Claude) — paste in the README and your question; they are very good at this.</li>
              <li>Report bugs via{' '}
                <a href={`${REPO_URL}/issues`} target="_blank" rel="noreferrer" className="text-sky-400 hover:underline">GitHub Issues</a>{' '}
                and request enhancements via{' '}
                <a href={`${REPO_URL}/discussions`} target="_blank" rel="noreferrer" className="text-sky-400 hover:underline">GitHub Discussions</a>.
              </li>
            </ul>
          </div>

          {/* ── Acknowledgment ───────────────────────────────────────────── */}
          <div className="space-y-2.5">
            <label className="flex items-start gap-2.5 text-xs text-gray-300 cursor-pointer">
              <input type="checkbox" className={checkboxCls} checked={riskOk} onChange={e => setRiskOk(e.target.checked)} />
              <span>I understand this is alpha software that trades real money and can lose it, is provided without warranty or support, and I accept these risks.</span>
            </label>
            <label className="flex items-start gap-2.5 text-xs text-gray-300 cursor-pointer">
              <input type="checkbox" className={checkboxCls} checked={jurisdictionOk} onChange={e => setJurisdictionOk(e.target.checked)} />
              <span>
                {venue === 'intl'
                  ? 'I confirm I am legally permitted to trade on the international venue this build connects to, and I bear sole legal responsibility for that determination.'
                  : 'I confirm I am eligible to trade on the US venues this build connects to under the laws that apply to me.'}
              </span>
            </label>
          </div>

          {error && (
            <div className="text-xs font-mono text-red-300 bg-red-950/50 rounded-lg px-3 py-2">{error}</div>
          )}

          <button
            onClick={accept}
            disabled={!riskOk || !jurisdictionOk || busy}
            className="w-full py-2.5 rounded-lg font-mono text-sm bg-amber-500 text-black font-semibold disabled:opacity-30 disabled:cursor-not-allowed hover:bg-amber-400 transition-colors"
          >
            {busy ? 'Recording…' : 'I acknowledge — continue to DRADIS'}
          </button>
          <p className="text-[10px] text-gray-600 font-mono text-center">
            Your acknowledgment is recorded with a timestamp on this instance.
          </p>
        </div>
      </div>
    </div>
  );
}
