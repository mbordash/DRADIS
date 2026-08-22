'use client';

/**
 * First-run venue choice, shown before anything else on a multi-venue image.
 *
 * This exists because of an ordering bug rather than for polish. The risk gate
 * that follows records a jurisdiction acknowledgment stamped with the venue the
 * engine is running, and that record is write-once — "the first acknowledgment
 * is the record of legal significance". While first boot seeded a default of
 * Polymarket International, a US buyer was shown a gate telling them the
 * International CLOB is not available to US persons, had to accept it to get
 * any further, and thereby filed a permanent acknowledgment against a venue
 * they may not legally trade.
 *
 * So the choice comes first, and the acknowledgment is made against the venue
 * the operator actually picked.
 *
 * Only rendered when the image carries more than one venue and none has been
 * chosen. A single-venue build has nothing to ask.
 */

import { useState } from 'react';
import type { VenueId } from '@/lib/setupApi';
import { putVenue, restartEngine, getSetupStatus } from '@/lib/setupApi';

const VENUES: {
  id: VenueId;
  name: string;
  custody: string;
  blurb: string;
  eligibility: string;
  usOk: boolean;
}[] = [
  {
    id: 'us',
    name: 'Polymarket US',
    custody: 'Custodial',
    blurb: 'CFTC-regulated US exchange. Funds stay in your Polymarket US account and DRADIS authenticates with an API key.',
    eligibility: 'Open to eligible US persons.',
    usOk: true,
  },
  {
    id: 'kalshi',
    name: 'Kalshi',
    custody: 'Custodial',
    blurb: 'CFTC-regulated US exchange. Requests are signed locally with an RSA key you generate in your Kalshi account.',
    eligibility: 'Open to eligible US persons.',
    usOk: true,
  },
  {
    id: 'intl',
    name: 'Polymarket International',
    custody: 'Self-custody',
    blurb: 'The international CLOB. Your funds stay in a wallet you control and DRADIS signs orders with its key.',
    eligibility: 'NOT available to US persons.',
    usOk: false,
  },
];

export default function VenueGate({
  available,
  onChosen,
}: {
  available: VenueId[];
  onChosen: () => void;
}) {
  const [pending, setPending] = useState<VenueId | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Only offer what this image can actually run.
  const options = VENUES.filter(v => available.includes(v.id));
  const chosen = options.find(v => v.id === pending);

  const [note, setNote] = useState<string | null>(null);

  const confirm = async () => {
    if (!pending) return;
    setBusy(true);
    setError(null);
    try {
      // Only restart when the engine is not already running this venue. A fresh
      // instance runs the intl fallback, so choosing Polymarket International
      // needs no restart at all — bouncing it anyway meant a pointless minute of
      // downtime on the very first thing a customer does.
      const res = await putVenue(pending);
      if (res.restart_required) {
        setNote('Restarting the engine — this takes 30-60 seconds.');
        await restartEngine();
      }
      // Wait for the engine to be RUNNING the chosen venue — not merely for the
      // choice to be recorded.
      //
      // `venue_selected` flips true the instant PUT writes the file, which is
      // before the restart has even begun. Polling on it dismissed this gate
      // while the old binary was still serving, so the jurisdiction gate behind
      // it rendered with the previous venue: choose Kalshi, get "DRADIS
      // International — read before proceeding". Acknowledging there would have
      // filed the write-once record against intl, which is the precise failure
      // this whole gate exists to prevent.
      //
      // `st.venue` comes from build_venue() — the binary actually running — so it
      // only reports the new venue once the swap is complete.
      const deadline = Date.now() + 120_000;
      for (;;) {
        try {
          const st = await getSetupStatus();
          if (st.venue === pending) { onChosen(); return; }
        } catch { /* engine still down mid-restart — keep waiting */ }
        if (Date.now() > deadline) {
          setError(`The engine did not come back as ${VENUES.find(v => v.id === pending)?.name ?? pending} within two minutes. It may still be starting — reload the page in a moment.`);
          setBusy(false);
          return;
        }
        setNote('Waiting for the engine to come back…');
        await new Promise(r => setTimeout(r, 3000));
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Could not set the venue — is the engine reachable?');
      setBusy(false);
    }
  };

  return (
    <div className="fixed inset-0 z-[110] overflow-y-auto bg-black/80 backdrop-blur-sm">
      <div className="min-h-full flex items-center justify-center p-4">
        <div className="w-full max-w-2xl bg-[#13131f] border border-indigo-500/40 rounded-2xl p-6 sm:p-8 space-y-5 shadow-2xl">
          <div>
            <h2 className="text-lg font-mono text-gray-100">Choose your trading venue</h2>
            <p className="text-xs text-gray-500 mt-1.5 leading-relaxed">
              This image can trade any of these. Pick the one you hold an account with — the
              engine restarts into it, and everything after this is specific to your choice.
              You can change it later in Setup.
            </p>
          </div>

          <div className="space-y-2">
            {options.map(v => {
              const on = pending === v.id;
              return (
                <button
                  key={v.id}
                  onClick={() => setPending(v.id)}
                  disabled={busy}
                  className={[
                    'w-full text-left rounded-xl border p-4 transition-colors disabled:opacity-50',
                    on
                      ? 'bg-indigo-500/10 border-indigo-500/50'
                      : 'bg-[#0e0e18] border-[#1e1e32] hover:border-gray-600',
                  ].join(' ')}
                >
                  <div className="flex items-baseline justify-between gap-3 flex-wrap">
                    <span className="text-sm font-mono text-gray-100">{v.name}</span>
                    <span className="text-[10px] font-mono uppercase tracking-wide text-gray-500">
                      {v.custody}
                    </span>
                  </div>
                  <p className="text-xs text-gray-500 mt-1.5 leading-relaxed">{v.blurb}</p>
                  <p
                    className={`text-[11px] font-mono mt-2 ${
                      v.usOk ? 'text-emerald-400/80' : 'text-amber-300/90'
                    }`}
                  >
                    {v.usOk ? '✓ ' : '⚠️ '}
                    {v.eligibility}
                  </p>
                </button>
              );
            })}
          </div>

          <p className="text-[11px] text-gray-600 leading-relaxed">
            Eligibility is yours to determine. DRADIS does not verify your jurisdiction, and
            you are solely responsible for confirming you may trade on the venue you select.
          </p>

          {note && !error && (
            <div className="bg-indigo-500/10 border border-indigo-500/30 rounded-xl px-4 py-3 text-xs font-mono text-indigo-300">
              {note}
            </div>
          )}

          {error && (
            <div className="bg-rose-500/10 border border-rose-500/30 rounded-xl px-4 py-3 text-xs font-mono text-rose-300">
              {error}
            </div>
          )}

          <div className="flex items-center justify-between gap-3 border-t border-[#1e1e32] pt-4">
            <div className="flex items-center gap-3">
              <span className="text-[11px] font-mono text-gray-600">
                {chosen ? `Selected: ${chosen.name}` : 'Select a venue to continue'}
              </span>
              {/* Until Continue is pressed nothing has been written, so undoing a
                  misclick should not mean reloading the page. */}
              {chosen && !busy && (
                <button
                  onClick={() => { setPending(null); setNote(null); setError(null); }}
                  className="text-[11px] font-mono text-gray-500 hover:text-gray-300 underline underline-offset-2"
                >
                  Change
                </button>
              )}
            </div>
            <button
              onClick={confirm}
              disabled={!pending || busy}
              className="text-xs font-mono px-4 py-2 rounded-lg border bg-indigo-500/20 border-indigo-500/40 text-indigo-300 hover:bg-indigo-500/30 disabled:opacity-40 disabled:cursor-not-allowed transition-colors"
            >
              {busy ? 'Applying…' : 'Continue'}
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}
