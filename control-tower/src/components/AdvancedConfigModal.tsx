'use client';

import { useState, useCallback, useEffect } from 'react';
import useSWR from 'swr';
import type { DynamicConfig, ConfigFieldSchema } from '@/lib/types';
import { getConfigSchema } from '@/lib/api';

// ── Advanced config modal ─────────────────────────────────────────────────────
//
// Renders the "rest" of a viper's editable config — every field flagged
// `advanced: true` in the Rust schema registry (GET /api/config/schema) that does
// NOT appear on the ViperCard's Basic panel. Inputs are clamped to the schema's
// min/max and saved per-field through the same generic PATCH path as the Basic
// panel, so non-power users can tune ad-hoc without a footgun.
//
// Values are edited in STORED units (e.g. a 12% stop-loss shows as 0.12) — the
// per-field description clarifies the encoding. Schema is fetched via SWR with a
// shared key, so multiple open cards/modals dedupe to one request.

interface RowProps {
  field:    ConfigFieldSchema;
  config:   DynamicConfig;
  onPatch:  (patch: Partial<DynamicConfig>) => Promise<void>;
  disabled: boolean;
}

/** Clamp a number to the schema's [min, max] when provided. */
function clamp(n: number, min: number | null, max: number | null): number {
  if (min != null && n < min) return min;
  if (max != null && n > max) return max;
  return n;
}

function AdvancedRow({ field, config, onPatch, disabled }: RowProps) {
  const stored = String((config as unknown as Record<string, unknown>)[field.key] ?? '');
  const [draft,  setDraft]  = useState(stored);
  const [saving, setSaving] = useState(false);
  const [error,  setError]  = useState<string | null>(null);

  // Re-sync local draft when the upstream config changes (e.g. after a save).
  useEffect(() => { setDraft(stored); }, [stored]);

  const commit = useCallback(async () => {
    if (field.type === 'bool') return; // handled by the toggle path
    const n = parseFloat(draft);
    if (isNaN(n)) { setError('not a number'); setDraft(stored); return; }
    const clamped = clamp(n, field.min, field.max);
    const next = String(clamped);
    setError(clamped !== n ? `clamped to ${clamped}` : null);
    if (next === stored) { setDraft(next); return; }
    setSaving(true);
    try {
      await onPatch({ [field.key]: next } as unknown as Partial<DynamicConfig>);
    } finally {
      setSaving(false);
    }
  }, [draft, field, stored, onPatch]);

  const toggleBool = useCallback(async () => {
    const next = stored === 'true' ? 'false' : 'true';
    setSaving(true);
    try {
      await onPatch({ [field.key]: next === 'true' } as unknown as Partial<DynamicConfig>);
    } finally {
      setSaving(false);
    }
  }, [field.key, stored, onPatch]);

  const bounds = [
    field.min != null ? `min ${field.min}` : null,
    field.max != null ? `max ${field.max}` : null,
  ].filter(Boolean).join(' · ');

  return (
    <div className="flex items-start justify-between gap-3 py-2 border-b border-[#1e1e32] last:border-0">
      <div className="min-w-0 flex-1">
        <div className="flex items-center gap-2">
          <span className="text-xs font-medium text-gray-300 truncate">{field.label}</span>
          <span className="text-[10px] font-mono text-gray-600 uppercase">{field.type}</span>
        </div>
        <p className="text-[11px] text-gray-500 leading-snug mt-0.5">{field.description}</p>
        {(bounds || error) && (
          <p className="text-[10px] font-mono mt-0.5">
            {bounds && <span className="text-gray-600">{bounds}</span>}
            {error && <span className="text-amber-400 ml-2">{error}</span>}
          </p>
        )}
      </div>
      <div className="flex items-center gap-2 shrink-0 pt-0.5">
        {field.type === 'bool' ? (
          <button
            onClick={toggleBool}
            disabled={disabled || saving}
            className={[
              'relative inline-flex h-5 w-9 items-center rounded-full transition-colors',
              stored === 'true' ? 'bg-green-500' : 'bg-gray-700',
              disabled || saving ? 'opacity-50 cursor-not-allowed' : 'cursor-pointer',
            ].join(' ')}
          >
            <span className={[
              'inline-block h-3.5 w-3.5 rounded-full bg-white shadow transition-transform',
              stored === 'true' ? 'translate-x-[18px]' : 'translate-x-[3px]',
            ].join(' ')} />
          </button>
        ) : (
          <>
            <input
              type="number"
              className="input-field w-24"
              value={draft}
              disabled={disabled || saving}
              min={field.min ?? undefined}
              max={field.max ?? undefined}
              step={field.step ?? undefined}
              onChange={e => setDraft(e.target.value)}
              onBlur={commit}
              onKeyDown={e => {
                if (e.key === 'Enter') (e.target as HTMLInputElement).blur();
                if (e.key === 'Escape') { setDraft(stored); setError(null); }
              }}
            />
            {field.unit && <span className="text-[10px] text-gray-600 w-9">{field.unit}</span>}
          </>
        )}
      </div>
    </div>
  );
}

interface Props {
  /** Viper display name — must match the schema `group` (e.g. "Arbitrage"). */
  viperName: string;
  config:    DynamicConfig;
  onPatch:   (patch: Partial<DynamicConfig>) => Promise<void>;
  onClose:   () => void;
  enabled:   boolean;
}

export default function AdvancedConfigModal({ viperName, config, onPatch, onClose, enabled }: Props) {
  const { data: schema = [], isLoading, error } = useSWR('config-schema', getConfigSchema, {
    revalidateOnFocus: false,
  });

  // Close on Escape.
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => { if (e.key === 'Escape') onClose(); };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [onClose]);

  const fields = schema.filter(f => f.group === viperName && f.advanced);

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 backdrop-blur-sm p-4"
      onClick={onClose}
    >
      <div
        className="card w-full max-w-lg max-h-[80vh] flex flex-col shadow-2xl"
        onClick={e => e.stopPropagation()}
      >
        {/* Header */}
        <div className="flex items-center justify-between px-5 py-3 border-b border-[#1e1e32]">
          <div className="min-w-0">
            <h2 className="text-sm font-semibold text-white">{viperName} — Advanced</h2>
            <p className="text-[11px] text-gray-500">Live-edited; clamped to safe ranges. Saves immediately.</p>
          </div>
          <button
            onClick={onClose}
            className="text-gray-500 hover:text-white transition-colors text-lg leading-none px-2"
            aria-label="Close"
          >
            ×
          </button>
        </div>

        {/* Body */}
        <div className="px-5 py-2 overflow-y-auto">
          {!enabled && (
            <div className="my-2 text-[11px] text-amber-400 bg-amber-500/10 border border-amber-500/20 rounded px-2 py-1">
              This viper is disabled — changes are saved but take effect only when enabled.
            </div>
          )}
          {isLoading && <p className="py-6 text-center text-xs text-gray-500">Loading schema…</p>}
          {error && <p className="py-6 text-center text-xs text-red-400">Failed to load config schema.</p>}
          {!isLoading && !error && fields.length === 0 && (
            <p className="py-6 text-center text-xs text-gray-500">No advanced settings for this viper.</p>
          )}
          {fields.map(f => (
            <AdvancedRow key={f.key} field={f} config={config} onPatch={onPatch} disabled={false} />
          ))}
        </div>

        {/* Footer */}
        <div className="px-5 py-3 border-t border-[#1e1e32] flex justify-end">
          <button
            onClick={onClose}
            className="text-xs px-3 py-1.5 rounded bg-[#1a1a2e] text-gray-300 hover:bg-[#22223a] transition-colors"
          >
            Done
          </button>
        </div>
      </div>
    </div>
  );
}


