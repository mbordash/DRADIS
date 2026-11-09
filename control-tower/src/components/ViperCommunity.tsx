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
 * [E96] The Community card on Build a Viper: browse the signed community
 * registry on dradis.live and open a version in the import review.
 *
 * Nothing is fetched until the operator asks: the engine reaches its
 * compiled-in registry only on "Browse" and "Review", and checks every
 * signature itself. Importing goes through the same review and route as a file.
 */

import { useState } from 'react';
import { fetchRegistry, fetchRegistryBundle } from '@/lib/api';
import type { RegistryListing, RegistryViper, ViperBundle } from '@/lib/types';

const labelCls = 'block text-[10px] font-mono uppercase tracking-wide text-gray-500 mb-1';
const btn = 'px-3 py-1.5 rounded border text-xs font-mono transition-colors disabled:opacity-40 disabled:cursor-not-allowed';
const btnPlain = `${btn} border-[#1e1e32] text-gray-300 hover:bg-white/[0.03]`;
const btnSmall = 'px-2 py-0.5 rounded border text-[10px] font-mono transition-colors disabled:opacity-40 border-teal-500/40 text-teal-200 hover:bg-teal-500/10';

/** `1.10.0` sorts after `1.9.0`. */
function newer(a: string, b: string): boolean {
  const pa = a.split('.').map(Number), pb = b.split('.').map(Number);
  for (let i = 0; i < 3; i++) if ((pa[i] ?? 0) !== (pb[i] ?? 0)) return (pa[i] ?? 0) > (pb[i] ?? 0);
  return false;
}

export function CommunityCard({ disabled, onReview }: {
  disabled?: boolean;
  /** Open a fetched, verified bundle in the import review. `localName` is set
   *  for an update, to the installed copy's name. */
  onReview: (bundle: ViperBundle, label: string, localName?: string) => void;
}) {
  const [listing, setListing] = useState<RegistryListing | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState<string | null>(null);

  const browse = async () => {
    setLoading('index'); setError(null);
    try { setListing(await fetchRegistry()); }
    catch (e) { setError(e instanceof Error ? e.message : String(e)); }
    finally { setLoading(null); }
  };

  const review = async (v: RegistryViper, version: string, localName?: string) => {
    const key = `${v.entry.publisher}/${v.entry.name}`;
    setLoading(key); setError(null);
    try {
      const r = await fetchRegistryBundle(v.entry.publisher, v.entry.name, version);
      onReview(r.bundle, `dradis.live · ${key} ${version}`, localName);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(null);
    }
  };

  return (
    <div className="card p-3 border border-[#1e1e32] space-y-2">
      <p className={labelCls}>Community</p>
      <p className="text-[10px] font-mono text-gray-500">
        Vipers published on <span className="text-gray-400">dradis.live</span>, signed by the registry. You review each one before
        anything is saved, and it arrives off and on paper. Community content: not investment advice.
      </p>
      {!listing && (
        <button className={btnPlain} disabled={disabled || loading !== null} onClick={browse}>
          {loading === 'index' ? 'Loading…' : 'Browse community vipers'}
        </button>
      )}
      {error && <div className="rounded border border-red-500/30 bg-red-500/10 px-2 py-1.5 text-[10px] font-mono text-red-300 whitespace-pre-wrap">{error}</div>}
      {listing && (
        <div className="space-y-1.5">
          {listing.vipers.length === 0 && <p className="text-[10px] font-mono text-gray-500">Nothing published yet.</p>}
          {listing.vipers.map((v) => {
            const key = `${v.entry.publisher}/${v.entry.name}`;
            // Each local copy of this entry gets its own status and update.
            const copies = v.installed.map((inst) => ({
              inst,
              update: v.available && inst.version && newer(v.available, inst.version) ? v.available : null,
              yanked: v.entry.versions.find((x) => x.version === inst.version)?.yanked,
            }));
            const inst = copies[0]?.inst;
            return (
              <div key={key} className="rounded border border-[#1e1e32] px-2 py-1.5 space-y-1">
                <div className="flex items-start justify-between gap-2">
                  <div className="min-w-0">
                    <div className="text-xs font-mono text-gray-200 truncate">{v.entry.display_name}</div>
                    <div className="text-[10px] font-mono text-gray-500 truncate">
                      {key}{v.available ? ` · ${v.available}` : ''} · {v.entry.classes.join(', ')} · {v.entry.venues.length ? v.entry.venues.join(', ') : 'all venues'}
                    </div>
                  </div>
                  {v.available && !inst && (
                    <button className={btnSmall} disabled={disabled || loading !== null || !v.runs_here}
                            title={v.runs_here ? undefined : `Not declared for ${listing.venue}`}
                            onClick={() => review(v, v.available!)}>
                      {loading === key ? '…' : 'Review'}
                    </button>
                  )}
                </div>
                {v.entry.description && <div className="text-[10px] font-mono text-gray-500">{v.entry.description}</div>}
                {copies.map(({ inst: c, update, yanked }) => (
                  <div key={c.name} className="space-y-0.5">
                    <div className="flex items-center justify-between gap-2 text-[10px] font-mono text-indigo-300">
                      <span>
                        installed as {c.name}{c.version ? ` (${c.version})` : ''}{c.provenance === 'edited' ? ', edited here' : ''}
                        {!update && v.available && c.version === v.available ? ' · up to date' : ''}
                      </span>
                      {update && (
                        <button className={btnSmall} disabled={disabled || loading !== null} onClick={() => review(v, update, c.name)}>
                          {loading === key ? '…' : `Update to ${update}`}
                        </button>
                      )}
                    </div>
                    {update && c.provenance === 'edited' && (
                      <div className="text-[10px] font-mono text-amber-300">Updating replaces your edits to {c.name}. Export it first to keep them.</div>
                    )}
                    {yanked && <div className="text-[10px] font-mono text-red-300">The registry withdrew this version: {yanked}</div>}
                  </div>
                ))}
                {!v.available && (
                  <div className="text-[10px] font-mono text-amber-300">
                    {v.entry.versions.every((x) => x.yanked) ? 'Withdrawn by the registry.' : `Needs a newer DRADIS (script API level above ${listing.api_version}).`}
                  </div>
                )}
                {v.available && !v.runs_here && !inst && (
                  <div className="text-[10px] font-mono text-gray-600">Not declared for {listing.venue}.</div>
                )}
              </div>
            );
          })}
          <button className="text-[10px] font-mono text-gray-500 hover:text-gray-300" disabled={loading !== null} onClick={browse}>Refresh</button>
        </div>
      )}
    </div>
  );
}
