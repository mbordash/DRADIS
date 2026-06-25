'use client';

import { useState, useCallback } from 'react';
import useSWR from 'swr';
import type { DynamicConfig, ViperDef, ConfigFieldSchema, FieldType } from '@/lib/types';
import { toDisplay, fromDisplay, fieldUnit } from '@/lib/types';
import { getConfigSchema } from '@/lib/api';
import { DEMO_MODE } from '@/lib/demo';
import AdvancedConfigModal from '@/components/AdvancedConfigModal';

// ── Accent color helpers ──────────────────────────────────────────────────────

const ACCENT: Record<string, { ring: string; badge: string; dot: string }> = {
  indigo:  { ring: 'ring-indigo-500/30',  badge: 'bg-indigo-500/10 text-indigo-300',  dot: 'bg-indigo-500'  },
  blue:    { ring: 'ring-blue-500/30',    badge: 'bg-blue-500/10 text-blue-300',      dot: 'bg-blue-500'    },
  emerald: { ring: 'ring-emerald-500/30', badge: 'bg-emerald-500/10 text-emerald-300',dot: 'bg-emerald-500' },
  orange:  { ring: 'ring-orange-500/30',  badge: 'bg-orange-500/10 text-orange-300',  dot: 'bg-orange-500'  },
  purple:  { ring: 'ring-purple-500/30',  badge: 'bg-purple-500/10 text-purple-300',  dot: 'bg-purple-500'  },
  cyan:    { ring: 'ring-cyan-500/30',    badge: 'bg-cyan-500/10 text-cyan-300',      dot: 'bg-cyan-500'    },
};

// ── Toggle switch ─────────────────────────────────────────────────────────────

function Toggle({ enabled, onToggle, loading }: { enabled: boolean; onToggle: () => void; loading?: boolean }) {
  return (
    <button
      onClick={onToggle}
      disabled={loading}
      title={enabled ? 'Click to disable' : 'Click to enable'}
      className={[
        'relative inline-flex h-5 w-9 shrink-0 items-center rounded-full transition-colors duration-200',
        'focus:outline-none focus:ring-2 focus:ring-offset-1 focus:ring-offset-[#13131f]',
        enabled ? 'bg-green-500 focus:ring-green-500' : 'bg-gray-700 focus:ring-gray-500',
        loading ? 'opacity-50 cursor-not-allowed' : 'cursor-pointer',
      ].join(' ')}
    >
      <span className={[
        'inline-block h-3.5 w-3.5 rounded-full bg-white shadow transition-transform duration-200',
        enabled ? 'translate-x-[18px]' : 'translate-x-[3px]',
      ].join(' ')} />
    </button>
  );
}

// ── Editable param row ────────────────────────────────────────────────────────

interface ParamRowProps {
  field:    ConfigFieldSchema;
  config:   DynamicConfig;
  onPatch:  (patch: Partial<DynamicConfig>) => Promise<void>;
  disabled: boolean;
}

function ParamRow({ field, config, onPatch, disabled }: ParamRowProps) {
  const type     = field.type as FieldType;
  const cfgKey   = field.key as keyof DynamicConfig;
  const rawValue = config[cfgKey];
  const initial  = toDisplay(type, rawValue as string);
  const [draft,    setDraft]    = useState(initial);
  const [editMode, setEditMode] = useState(false);
  const [saving,   setSaving]   = useState(false);

  // Reset draft when config prop changes (e.g. after a remote patch)
  const display = editMode ? draft : toDisplay(type, rawValue as string);

  const commit = useCallback(async () => {
    setEditMode(false);
    const stored = fromDisplay(type, draft);
    const prev   = fromDisplay(type, toDisplay(type, rawValue as string));
    if (stored === prev) return;
    setSaving(true);
    try {
      await onPatch({ [field.key]: stored } as Partial<DynamicConfig>);
    } finally {
      setSaving(false);
    }
  }, [draft, field.key, type, rawValue, onPatch]);

  return (
    <div className="flex items-center justify-between py-1 border-b border-[#1e1e32] last:border-0">
      <span className="text-xs text-gray-500 truncate mr-2">{field.label}</span>
      <div className="flex items-center gap-1">
        {editMode ? (
          <input
            className="input-field w-20"
            value={display}
            autoFocus
            disabled={disabled || saving}
            onChange={e => setDraft(e.target.value)}
            onBlur={commit}
            onKeyDown={e => { if (e.key === 'Enter') commit(); if (e.key === 'Escape') { setEditMode(false); setDraft(initial); } }}
          />
        ) : (
          <button
            onClick={() => { if (!disabled) { setDraft(toDisplay(type, rawValue as string)); setEditMode(true); } }}
            disabled={disabled || saving}
            className={[
              'text-xs font-mono tabular-nums px-2 py-1 rounded',
              'hover:bg-[#1a1a2e] transition-colors text-right w-20',
              disabled ? 'text-gray-600 cursor-default' : 'text-gray-200 cursor-text',
              saving ? 'opacity-50' : '',
            ].join(' ')}
          >
            {saving ? '…' : toDisplay(type, rawValue as string)}
          </button>
        )}
        {fieldUnit(type) && (
          <span className="text-xs text-gray-600 w-8">{fieldUnit(type)}</span>
        )}
      </div>
    </div>
  );
}

// ── ViperCard ─────────────────────────────────────────────────────────────────

interface Props {
  viper:   ViperDef;
  config:  DynamicConfig;
  onPatch: (patch: Partial<DynamicConfig>) => Promise<void>;
  /** Active market name returned by /api/status */
  market?: string;
}

export default function ViperCard({ viper, config, onPatch, market }: Props) {
  const [toggling, setToggling] = useState(false);
  const [showAdvanced, setShowAdvanced] = useState(false);
  const enabled  = config[viper.enableKey] as boolean;
  const accent   = ACCENT[viper.accentColor] ?? ACCENT.indigo;

  // Basic params are derived from the Rust schema registry (single source of
  // truth) — `advanced:false`, non-bool fields for this viper group. Shared SWR
  // key dedupes with the Advanced modal's fetch.
  const { data: schema = [], isLoading: schemaLoading } = useSWR('config-schema', getConfigSchema, {
    revalidateOnFocus: false,
  });
  const basicFields = schema.filter(
    f => f.group === viper.name && !f.advanced && f.type !== 'bool',
  );

  const handleToggle = async () => {
    if (DEMO_MODE) return;
    setToggling(true);
    try {
      await onPatch({ [viper.enableKey]: !enabled } as Partial<DynamicConfig>);
    } finally {
      setToggling(false);
    }
  };

  return (
    <div className={[
      'card p-4 flex flex-col gap-3 transition-all duration-200',
      enabled ? `ring-1 ${accent.ring}` : 'opacity-60',
    ].join(' ')}>
      {/* Header */}
      <div className="flex items-start justify-between gap-2">
        <div className="flex items-center gap-2 min-w-0">
          <span className={`inline-block h-2 w-2 rounded-full shrink-0 ${enabled ? accent.dot : 'bg-gray-700'}`} />
          <span className="text-sm font-semibold text-white truncate">{viper.name}</span>
        </div>
        <Toggle enabled={enabled} onToggle={handleToggle} loading={toggling || DEMO_MODE} />
      </div>

      {/* Description */}
      <p className="text-xs text-gray-500 leading-snug">{viper.description}</p>

      {/* Active market */}
      {market && market.length > 0 && (
        <div
          className="flex items-center gap-1.5 text-xs text-gray-400 bg-[#0f0f1a] border border-[#1e1e32] rounded px-2 py-1 truncate"
          title={market}
        >
          <span className="shrink-0 text-gray-600">📍</span>
          <span className="truncate font-mono">{market}</span>
        </div>
      )}

      {/* Status badge */}
      <span className={`self-start text-xs px-2 py-0.5 rounded-full font-mono ${
        enabled ? accent.badge : 'bg-gray-800 text-gray-600'
      }`}>
        {enabled ? 'ACTIVE' : 'DISABLED'}
      </span>

      {/* Params */}
      <div className="flex flex-col">
        {schemaLoading && basicFields.length === 0 ? (
          <p className="text-[11px] text-gray-600 py-1">Loading parameters…</p>
        ) : (
          basicFields.map(f => (
            <ParamRow
              key={f.key}
              field={f}
              config={config}
              onPatch={onPatch}
              disabled={!enabled || DEMO_MODE}
            />
          ))
        )}
      </div>

      {/* Advanced settings */}
      <button
        onClick={() => setShowAdvanced(true)}
        className="self-start text-xs text-gray-500 hover:text-gray-300 transition-colors mt-1"
      >
        Advanced ▸
      </button>

      {showAdvanced && (
        <AdvancedConfigModal
          viperName={viper.name}
          config={config}
          onPatch={onPatch}
          onClose={() => setShowAdvanced(false)}
          enabled={enabled}
        />
      )}
    </div>
  );
}

