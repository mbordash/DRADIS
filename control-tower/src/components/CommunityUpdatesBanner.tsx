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
 * [E97] Dashboard notices for imported community vipers.
 *
 * A withdrawn version is red and stays until the operator updates or removes
 * that viper: it is the registry saying the version should not be run, and
 * nothing in DRADIS acts on it by itself. A plain new version is quiet and can
 * be dismissed; the dismissal is per version and per browser, so the next
 * version shows again.
 */

import { useEffect, useState } from 'react';
import type { CommunityNotice } from '@/lib/types';

const DISMISSED_KEY = 'dradis.community-updates.dismissed';

function noticeKey(n: CommunityNotice): string {
  return `${n.name}@${n.available ?? ''}`;
}

function readDismissed(): string[] {
  try {
    const raw = window.localStorage.getItem(DISMISSED_KEY);
    const v = raw ? JSON.parse(raw) : [];
    return Array.isArray(v) ? v.filter((x) => typeof x === 'string') : [];
  } catch {
    return [];
  }
}

function writeDismissed(keys: string[]) {
  try { window.localStorage.setItem(DISMISSED_KEY, JSON.stringify(keys)); } catch { /* private window */ }
}

export function CommunityUpdatesBanner({ notices, onOpen }: {
  notices: CommunityNotice[] | undefined;
  /** Go to Build a Viper, where the Community card shows the diff. */
  onOpen: () => void;
}) {
  const [dismissed, setDismissed] = useState<string[]>([]);
  useEffect(() => { setDismissed(readDismissed()); }, []);

  if (!notices?.length) return null;
  const withdrawn = notices.filter((n) => n.yanked);
  const updates = notices.filter((n) => !n.yanked && n.available && !dismissed.includes(noticeKey(n)));
  if (!withdrawn.length && !updates.length) return null;

  const dismiss = () => {
    // Keep only keys still in play, so the list does not grow forever.
    const live = new Set(notices.map(noticeKey));
    const next = Array.from(new Set([...dismissed.filter((k) => live.has(k)), ...updates.map(noticeKey)]));
    writeDismissed(next);
    setDismissed(next);
  };

  return (
    <div className="space-y-2">
      {withdrawn.length > 0 && (
        <div className="card px-4 py-3 border border-red-500/40 bg-red-500/10">
          <div className="flex items-start gap-3">
            <span className="text-lg leading-none">⛔</span>
            <div className="min-w-0 space-y-1">
              <p className="text-sm font-mono text-red-300">
                The registry withdrew {withdrawn.length === 1 ? 'a viper version' : 'viper versions'} you imported
              </p>
              {withdrawn.map((n) => (
                <p key={n.name} className="text-[11px] font-mono text-red-200/90 break-words">
                  {n.name} ({n.publisher}/{n.registry_name} {n.installed}): {n.yanked}
                  {n.available ? ` · ${n.available} is available` : ''}
                </p>
              ))}
              <p className="text-[11px] text-gray-400 leading-relaxed">
                It keeps running as configured; DRADIS does not switch it off. Review it, then update it,
                switch it off on its squadrons, or delete it.
              </p>
              <button onClick={onOpen} className="text-[11px] font-mono text-red-200 hover:text-white underline">
                Open Build a Viper →
              </button>
            </div>
          </div>
        </div>
      )}
      {updates.length > 0 && (
        <div className="card px-4 py-2.5 border border-teal-500/30 bg-teal-500/5">
          <div className="flex items-center gap-3">
            <span className="text-base leading-none">📦</span>
            <p className="min-w-0 flex-1 text-xs font-mono text-teal-200 truncate">
              Community update{updates.length > 1 ? 's' : ''}:{' '}
              {updates.map((n) => `${n.name} ${n.installed} → ${n.available}${n.edited ? ' (edited here)' : ''}`).join(', ')}
            </p>
            <button onClick={onOpen} className="text-[11px] font-mono text-teal-200 hover:text-white underline shrink-0">Review</button>
            <button onClick={dismiss} aria-label="Dismiss" className="text-[11px] font-mono text-gray-500 hover:text-gray-300 shrink-0">✕</button>
          </div>
        </div>
      )}
    </div>
  );
}
