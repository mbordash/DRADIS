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

import { useEffect, useState } from 'react';

/**
 * Transient confirmations ("Saved", "No problems found") shown where the
 * operator can see them, wherever they have scrolled to. A banner at the top
 * of a long page was off-screen by the time the operator pressed Save at the
 * bottom of it.
 *
 * Toasts confirm; they do not replace inline errors. A list of problems the
 * operator has to read while fixing them stays on the page, and the toast
 * only says it is there.
 *
 * A module-level store rather than a context provider, so any component can
 * call `toast.success(...)` without wiring, and one `<Toaster />` in the root
 * layout renders them.
 */

export type ToastTone = 'success' | 'error' | 'info';

export interface ToastItem {
  id: number;
  tone: ToastTone;
  message: string;
  /** Optional action, e.g. "Show" to scroll a problem list into view. */
  action?: { label: string; run: () => void };
}

type Listener = (items: ToastItem[]) => void;

let items: ToastItem[] = [];
let nextId = 1;
const listeners = new Set<Listener>();
const MAX_VISIBLE = 4;

function emit() {
  for (const l of listeners) l(items);
}

export function dismissToast(id: number) {
  items = items.filter((t) => t.id !== id);
  emit();
}

function push(tone: ToastTone, message: string, opts?: { action?: ToastItem['action']; durationMs?: number }) {
  const id = nextId++;
  items = [...items, { id, tone, message, action: opts?.action }].slice(-MAX_VISIBLE);
  emit();
  // Errors stay a little longer: they point at something the operator must act on.
  const ms = opts?.durationMs ?? (tone === 'error' ? 7000 : 4000);
  if (typeof window !== 'undefined') window.setTimeout(() => dismissToast(id), ms);
  return id;
}

export const toast = {
  success: (message: string, opts?: { action?: ToastItem['action']; durationMs?: number }) => push('success', message, opts),
  error: (message: string, opts?: { action?: ToastItem['action']; durationMs?: number }) => push('error', message, opts),
  info: (message: string, opts?: { action?: ToastItem['action']; durationMs?: number }) => push('info', message, opts),
};

const toneCls: Record<ToastTone, string> = {
  success: 'border-teal-500/40 bg-[#0d1a1a] text-teal-200',
  error: 'border-red-500/40 bg-[#1f0f12] text-red-200',
  info: 'border-indigo-500/40 bg-[#12122a] text-indigo-200',
};

const toneIcon: Record<ToastTone, string> = { success: '✓', error: '!', info: 'i' };

/** Mount once, in the root layout. */
export default function Toaster() {
  const [shown, setShown] = useState<ToastItem[]>(items);
  useEffect(() => {
    const l: Listener = (next) => setShown(next);
    listeners.add(l);
    return () => { listeners.delete(l); };
  }, []);

  if (!shown.length) return null;
  return (
    <div
      className="fixed bottom-4 right-4 left-4 sm:left-auto z-[60] flex flex-col items-stretch sm:items-end gap-2 pointer-events-none"
      aria-live="polite"
      role="status"
    >
      {shown.map((t) => (
        <div
          key={t.id}
          className={`pointer-events-auto w-full sm:w-auto sm:max-w-sm rounded-lg border px-3 py-2 shadow-2xl text-xs font-mono flex items-start gap-2 ${toneCls[t.tone]}`}
        >
          <span aria-hidden className="font-bold">{toneIcon[t.tone]}</span>
          <span className="flex-1 whitespace-pre-wrap break-words">{t.message}</span>
          {t.action && (
            <button
              className="underline underline-offset-2 hover:opacity-80"
              onClick={() => { t.action?.run(); dismissToast(t.id); }}
            >
              {t.action.label}
            </button>
          )}
          <button aria-label="Dismiss" className="opacity-60 hover:opacity-100" onClick={() => dismissToast(t.id)}>×</button>
        </div>
      ))}
    </div>
  );
}
