'use client';

import { useCallback, useEffect, useRef, useState, type ReactNode } from 'react';

/**
 * In-app replacement for `window.confirm`.
 *
 * The native dialog is unstyled, cannot render structured content (the risk
 * profile apply needs to list the squadrons it is about to overwrite), and
 * blocks the JS thread — which stalls SWR polling behind it. This keeps the
 * Control Tower's own look and lets a confirmation show real data.
 *
 * Styling follows the AdvancedConfigModal idiom so all overlays match.
 */

export interface ConfirmOptions {
  title: string;
  /** Structured body — plain text, or JSX for lists/emphasis. */
  body?: ReactNode;
  confirmLabel?: string;
  cancelLabel?: string;
  /** `danger` styles the confirm button as destructive. */
  tone?: 'default' | 'danger';
}

/**
 * Promise-based confirm, so call sites keep the `if (!(await confirm(...))) return;`
 * shape that `window.confirm` had.
 *
 * ```tsx
 * const [confirm, confirmDialog] = useConfirm();
 * ...
 * if (!(await confirm({ title: 'Restart?' }))) return;
 * return <>{confirmDialog}{...}</>;
 * ```
 */
export function useConfirm(): [(opts: ConfirmOptions) => Promise<boolean>, ReactNode] {
  const [opts, setOpts] = useState<ConfirmOptions | null>(null);
  // Held across renders so resolve() survives the re-render that opens the dialog.
  const resolver = useRef<((ok: boolean) => void) | null>(null);

  const confirm = useCallback((o: ConfirmOptions) => {
    // A second call while one is pending would strand the first promise forever
    // and leak the caller's `await`. Resolve it as cancelled first.
    resolver.current?.(false);
    setOpts(o);
    return new Promise<boolean>(resolve => { resolver.current = resolve; });
  }, []);

  const settle = useCallback((ok: boolean) => {
    resolver.current?.(ok);
    resolver.current = null;
    setOpts(null);
  }, []);

  const dialog = opts
    ? <ConfirmDialog opts={opts} onResolve={settle} />
    : null;

  return [confirm, dialog];
}

function ConfirmDialog({
  opts, onResolve,
}: {
  opts: ConfirmOptions;
  onResolve: (ok: boolean) => void;
}) {
  const confirmBtn = useRef<HTMLButtonElement>(null);

  // Escape cancels — matches both the native dialog and AdvancedConfigModal.
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => { if (e.key === 'Escape') onResolve(false); };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [onResolve]);

  // Focus the confirm action so Enter works without reaching for the mouse.
  useEffect(() => { confirmBtn.current?.focus(); }, []);

  const danger = opts.tone === 'danger';

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 backdrop-blur-sm p-4"
      onClick={() => onResolve(false)}
      role="dialog"
      aria-modal="true"
      aria-label={opts.title}
    >
      <div
        className="card w-full max-w-md flex flex-col shadow-2xl"
        onClick={e => e.stopPropagation()}
      >
        <div className="px-5 py-3 border-b border-[#1e1e32]">
          <h2 className="text-sm font-semibold text-white">{opts.title}</h2>
        </div>

        {opts.body != null && (
          <div className="px-5 py-4 text-xs text-gray-400 space-y-2 max-h-[60vh] overflow-y-auto">
            {opts.body}
          </div>
        )}

        <div className="px-5 py-3 border-t border-[#1e1e32] flex justify-end gap-2">
          <button
            onClick={() => onResolve(false)}
            className="text-xs font-mono px-3 py-1.5 rounded-lg border bg-[#13131f] border-[#1e1e32] text-gray-400 hover:border-gray-600 hover:text-gray-200 transition-colors"
          >
            {opts.cancelLabel ?? 'Cancel'}
          </button>
          <button
            ref={confirmBtn}
            onClick={() => onResolve(true)}
            className={[
              'text-xs font-mono px-3 py-1.5 rounded-lg border transition-colors',
              danger
                ? 'bg-rose-500/10 border-rose-500/30 text-rose-300 hover:bg-rose-500/20'
                : 'bg-indigo-500/20 border-indigo-500/40 text-indigo-300 hover:bg-indigo-500/30',
            ].join(' ')}
          >
            {opts.confirmLabel ?? 'Confirm'}
          </button>
        </div>
      </div>
    </div>
  );
}
