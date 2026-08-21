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
import type { VenueId, Edition } from '@/lib/setupApi';

/** Display name per venue — used in the header, the jurisdiction heading, and
 *  the confirmation checkbox, so a Kalshi build never labels itself "US". */
const VENUE_NAME: Record<VenueId, string> = {
  intl:   'International',
  us:     'US',
  kalshi: 'Kalshi',
};

const REPO_URL = 'https://github.com/mbordash/DRADIS';

export default function AlphaGate({
  venue,
  appVersion,
  edition,
  onAcknowledged,
}: {
  venue: VenueId;
  appVersion?: string;
  edition?: Edition;
  onAcknowledged: () => void;
}) {
  const [riskOk, setRiskOk] = useState(false);
  const [jurisdictionOk, setJurisdictionOk] = useState(false);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // The engine serves its API before the SQLite pool finishes opening, so on a
  // freshly launched instance this call can land in that gap and come back
  // "DB not ready". That is a startup race, not a failure, and showing it as an
  // error makes a customer's very first interaction with the product look broken.
  // Retry quietly for a few seconds before surfacing anything.
  const accept = async () => {
    setBusy(true);
    setError(null);
    const DELAYS_MS = [400, 800, 1500, 2500, 4000];
    for (let attempt = 0; ; attempt++) {
      try {
        await acknowledgeAlpha();
        onAcknowledged();
        return;
      } catch (e) {
        const msg = e instanceof Error ? e.message : '';
        const stillStarting = /not ready|503|unavailable/i.test(msg);
        if (stillStarting && attempt < DELAYS_MS.length) {
          setError('Engine is still starting — retrying…');
          await new Promise(r => setTimeout(r, DELAYS_MS[attempt]));
          continue;
        }
        setError(
          stillStarting
            ? 'The engine is reachable but its database has not opened. If this persists past a minute it is not a slow start — check the engine log for "SQLite init failed".'
            : msg || 'Failed to record acknowledgment — is the engine reachable?',
        );
        setBusy(false);
        return;
      }
    }
  };

  const checkboxCls =
    'mt-0.5 h-4 w-4 shrink-0 rounded border-gray-600 bg-[#0e0e18] accent-amber-500 cursor-pointer';

  return (
    <div className="fixed inset-0 z-[100] overflow-y-auto bg-black/80 backdrop-blur-sm">
      <div className="min-h-full flex items-center justify-center p-4">
        <div className="w-full max-w-2xl bg-[#13131f] border border-amber-500/40 rounded-2xl p-6 sm:p-8 space-y-5 shadow-2xl">
          <div className="flex items-center gap-3">
            <h2 className="text-lg font-mono text-gray-100">
              DRADIS {VENUE_NAME[venue]}
              {appVersion ? ` v${appVersion}` : ''} — read before proceeding
            </h2>
          </div>

          {/* ── Risk ─────────────────────────────────────────────────────── */}
          <div className="bg-red-950/40 border border-red-500/30 rounded-xl p-4 space-y-2">
            <h3 className="text-sm font-mono text-red-300">⚠️ Real-money risk</h3>
            <ul className="text-xs text-red-200/90 space-y-1 list-disc pl-4">
              <li>DRADIS<strong>trading automation software provided AS IS</strong>. It places live orders with real funds and can <strong>lose some or all of the capital</strong> you give it access to.</li>
              <li>Automated trading involves substantial risk of loss: software bugs, security issues, network latency, API rate limits, venue outages, slippage, or misconfiguration can produce unintended trades and <strong>total loss of deployed capital</strong>. Start in GHOST mode and with money you can afford to lose entirely.</li>
              <li>Provided under the GPLv3 <strong>without warranty of any kind</strong>. Nothing here is financial advice.</li>
            </ul>
          </div>

          {/* ── Legal status ─────────────────────────────────────────────── */}
          <div className="bg-[#0e0e18] border border-[#1e1e32] rounded-xl p-4 space-y-2">
            <h3 className="text-sm font-mono text-gray-300">📜 Software, not a financial service</h3>
            <ul className="text-xs text-gray-400 space-y-1 list-disc pl-4">
              <li><strong className="text-gray-300">Non-custodial:</strong> DRADIS is self-hosted software. Your private keys, API keys, and funds stay exclusively on this instance under your control — the developers never store, access, or take custody of them.</li>
              <li><strong className="text-gray-300">Not a broker or adviser:</strong> this is a self-hosted automation tool. The developers are not acting as a broker, dealer, investment adviser, or money transmitter, and no strategy signal or AI recommendation produced by the engine constitutes financial or investment advice.</li>
              <li><strong className="text-gray-300">Limitation of liability:</strong> to the maximum extent permitted by law, the developers and contributors are not liable for any direct, indirect, incidental, special, consequential, or punitive damages — including loss of funds, profits, or data — arising from use of, or inability to use, this software.</li>
            </ul>
          </div>

          {/* ── Jurisdiction ─────────────────────────────────────────────── */}
          <div className="bg-amber-950/40 border border-amber-500/30 rounded-xl p-4 space-y-2">
            <h3 className="text-sm font-mono text-amber-300">🌍 Jurisdiction — {VENUE_NAME[venue]} venue</h3>
            {venue === 'intl' ? (
              <p className="text-xs text-amber-200/90">
                This build trades on Polymarket&apos;s <strong>international CLOB</strong>, which is{' '}
                <strong>not available to US persons</strong>. If you are a US person, do not use this
                venue — switch this instance to <strong>Polymarket US</strong> or <strong>Kalshi</strong>{' '}
                in Setup instead. By continuing you
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

          {/* ── Support policy ───────────────────────────────────────────── */}
          {/* A paid customer must be told how to get help, and by whom. The
              community wording below is honest for a free self-hosted build and
              would be unacceptable — quite possibly rejected — on a Marketplace
              product someone paid for. */}
          {edition === 'marketplace' ? (
            <div className="bg-[#0e0e18] border border-[#1e1e32] rounded-xl p-4 space-y-2">
              <h3 className="text-sm font-mono text-gray-300">🛟 Support</h3>
              <ul className="text-xs text-gray-400 space-y-1 list-disc pl-4">
                <li>Get help at{' '}
                  <a href="https://dradis.live/support" target="_blank" rel="noreferrer" className="text-sky-400 hover:underline">dradis.live/support</a>{' '}
                  — the form collects what is needed to diagnose a deployment.
                </li>
                <li>Or email{' '}
                  <a href="mailto:starbuck@dradis.live" className="text-sky-400 hover:underline">starbuck@dradis.live</a>.
                </li>
                <li><strong className="text-gray-300">Support will never ask for your wallet private key, seed phrase or API secrets.</strong> Anyone who does is not us.</li>
              </ul>
            </div>
          ) : (
            <div className="bg-[#0e0e18] border border-[#1e1e32] rounded-xl p-4 space-y-2">
              <h3 className="text-sm font-mono text-gray-300">🧪 Community-supported software</h3>
              <ul className="text-xs text-gray-400 space-y-1 list-disc pl-4">
                <li>Individual support is not included. For setup help, ask an AI assistant (ChatGPT, Gemini, Claude) — paste in the README and your question; they are very good at this.</li>
                <li>Report bugs via{' '}
                  <a href={`${REPO_URL}/issues`} target="_blank" rel="noreferrer" className="text-sky-400 hover:underline">GitHub Issues</a>{' '}
                  and request enhancements via{' '}
                  <a href={`${REPO_URL}/discussions`} target="_blank" rel="noreferrer" className="text-sky-400 hover:underline">GitHub Discussions</a>.
                </li>
              </ul>
            </div>
          )}

          {/* ── Acknowledgment ───────────────────────────────────────────── */}
          <div className="space-y-2.5">
            <label className="flex items-start gap-2.5 text-xs text-gray-300 cursor-pointer">
              <input type="checkbox" className={checkboxCls} checked={riskOk} onChange={e => setRiskOk(e.target.checked)} />
              <span>I understand this software trades real money and can lose it, is provided AS IS without warranty or individual support, and I accept these risks and the non-custodial, no-advice, and limitation-of-liability terms above.</span>
            </label>
            <label className="flex items-start gap-2.5 text-xs text-gray-300 cursor-pointer">
              <input type="checkbox" className={checkboxCls} checked={jurisdictionOk} onChange={e => setJurisdictionOk(e.target.checked)} />
              <span>
                {venue === 'intl'
                  ? 'I confirm I am legally permitted to trade on the international venue this build connects to, and I bear sole legal responsibility for that determination.'
                  : `I confirm I am eligible to trade on ${VENUE_NAME[venue]}, a US-regulated venue, under the laws that apply to me.`}
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
