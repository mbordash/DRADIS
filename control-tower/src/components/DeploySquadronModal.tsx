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

import { useState, useCallback, useEffect } from 'react';
import useSWR from 'swr';
import type { MarketType, DeploymentRegionInfo, AvailableMarket, RaptorKind, ViperKindInfo } from '@/lib/types';
import { getDeploymentRegion, getAvailableMarkets, getRaptorsForClass, getVipersForClass, deploySquadron } from '@/lib/api';

// ── Market Type Icons & Labels ────────────────────────────────────────────────

const MARKET_TYPE_CONFIG: Record<MarketType, { icon: string; label: string; color: string }> = {
  sports:   { icon: '🏈', label: 'Sports',   color: 'emerald' },
  politics: { icon: '🗳️', label: 'Politics', color: 'blue' },
  crypto:   { icon: '🪙', label: 'Crypto',   color: 'orange' },
};

// ── Market Type Selector ──────────────────────────────────────────────────────

interface MarketTypeSelectorProps {
  available: MarketType[];
  selected: MarketType | null;
  onSelect: (type: MarketType) => void;
}

function MarketTypeSelector({ available, selected, onSelect }: MarketTypeSelectorProps) {
  return (
    <div className="flex gap-2">
      {available.map(type => {
        const cfg = MARKET_TYPE_CONFIG[type];
        const isSelected = selected === type;
        return (
          <button
            key={type}
            onClick={() => onSelect(type)}
            className={`
              flex items-center gap-2 px-4 py-2.5 rounded-lg border transition-all
              ${isSelected 
                ? `bg-${cfg.color}-500/20 border-${cfg.color}-500/50 text-${cfg.color}-300`
                : 'bg-[#12121f] border-[#1e1e32] text-gray-400 hover:border-gray-600 hover:text-gray-300'
              }
            `}
          >
            <span className="text-lg">{cfg.icon}</span>
            <span className="text-sm font-mono font-medium">{cfg.label}</span>
          </button>
        );
      })}
    </div>
  );
}

// ── Quick Deploy Preview ──────────────────────────────────────────────────────

interface QuickPreviewProps {
  marketType: MarketType;
  raptors: RaptorKind[];
  vipers: ViperKindInfo[];
  loading: boolean;
}

function QuickDeployPreview({ marketType, raptors, vipers, loading }: QuickPreviewProps) {
  const cfg = MARKET_TYPE_CONFIG[marketType];
  const implementedRaptors = raptors.filter(r => r.implemented);
  
  if (loading) {
    return (
      <div className="bg-[#0a0a14] rounded-lg border border-[#1e1e32] p-4">
        <div className="flex items-center gap-2 text-gray-500">
          <span className="animate-pulse">📋</span>
          <span className="text-xs font-mono">Loading configuration...</span>
        </div>
      </div>
    );
  }

  return (
    <div className="bg-[#0a0a14] rounded-lg border border-[#1e1e32] p-4 space-y-3">
      <div className="flex items-center gap-2 text-gray-300">
        <span className="text-sm font-mono font-semibold">📋 Auto-Selection Preview</span>
      </div>
      
      <div className="grid gap-2 text-xs font-mono">
        <div className="flex items-center gap-2">
          <span className="text-gray-600 w-16">Market:</span>
          <span className={`text-${cfg.color}-300`}>
            DRADIS will select optimal {cfg.label.toLowerCase()} market
          </span>
        </div>
        
        <div className="flex items-center gap-2">
          <span className="text-gray-600 w-16">Raptors:</span>
          <div className="flex gap-1.5 flex-wrap">
            {implementedRaptors.length > 0 ? (
              implementedRaptors.map(r => (
                <span 
                  key={r.id}
                  className="bg-violet-500/10 text-violet-300 border border-violet-500/20 rounded px-1.5 py-0.5"
                >
                  {r.id}
                </span>
              ))
            ) : (
              <span className="text-gray-500 italic">None implemented yet</span>
            )}
          </div>
        </div>
        
        <div className="flex items-center gap-2">
          <span className="text-gray-600 w-16">Vipers:</span>
          <div className="flex gap-1.5 flex-wrap">
            {vipers.map(v => (
              <span 
                key={v.id}
                className="bg-cyan-500/10 text-cyan-300 border border-cyan-500/20 rounded px-1.5 py-0.5"
              >
                {v.display}
              </span>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}

// ── Manual Mode: Market Browser ───────────────────────────────────────────────

interface MarketBrowserProps {
  markets: AvailableMarket[];
  selected: string | null;
  onSelect: (conditionId: string) => void;
  loading: boolean;
}

function MarketBrowser({ markets, selected, onSelect, loading }: MarketBrowserProps) {
  if (loading) {
    return (
      <div className="bg-[#0a0a14] rounded-lg border border-[#1e1e32] p-4">
        <div className="flex items-center gap-2 text-gray-500">
          <span className="animate-pulse">🔍</span>
          <span className="text-xs font-mono">Fetching available markets...</span>
        </div>
      </div>
    );
  }

  if (markets.length === 0) {
    return (
      <div className="bg-[#0a0a14] rounded-lg border border-[#1e1e32] p-4">
        <div className="flex items-center gap-2 text-gray-500">
          <span>🛬</span>
          <span className="text-xs font-mono">No markets available for this type</span>
        </div>
      </div>
    );
  }

  return (
    <div className="bg-[#0a0a14] rounded-lg border border-[#1e1e32] overflow-hidden max-h-60 overflow-y-auto">
      <div className="px-3 py-2 border-b border-[#1e1e32] bg-[#0d0d1a]">
        <span className="text-xs font-mono text-gray-500">
          {markets.length} market{markets.length === 1 ? '' : 's'} available
        </span>
      </div>
      {markets.map(market => {
        const isSelected = selected === market.condition_id;
        const expiresAt = new Date(market.end_date);
        const hoursUntil = Math.max(0, (expiresAt.getTime() - Date.now()) / (1000 * 60 * 60));
        
        return (
          <button
            key={market.condition_id}
            onClick={() => onSelect(market.condition_id)}
            className={`
              w-full flex items-center gap-3 px-3 py-2.5 border-b border-[#1e1e32] last:border-0
              transition-colors text-left
              ${isSelected 
                ? 'bg-green-500/10'
                : 'hover:bg-white/[0.02]'
              }
            `}
          >
            <span className={`
              h-3 w-3 rounded-full border-2 flex-shrink-0
              ${isSelected 
                ? 'bg-green-400 border-green-400'
                : 'border-gray-600'
              }
            `} />
            <div className="min-w-0 flex-1">
              <p className="text-xs font-mono text-gray-200 truncate" title={market.question}>
                {market.question}
              </p>
            </div>
            <div className="flex items-center gap-3 flex-shrink-0">
              <span className="text-[10px] font-mono text-gray-500">
                {hoursUntil < 1 
                  ? `${Math.round(hoursUntil * 60)}m`
                  : hoursUntil < 24 
                    ? `${Math.round(hoursUntil)}h`
                    : `${Math.round(hoursUntil / 24)}d`
                }
              </span>
              <span className="text-[10px] font-mono text-green-400">
                ${market.liquidity.toLocaleString()}
              </span>
            </div>
          </button>
        );
      })}
    </div>
  );
}

// ── Manual Mode: Raptor/Viper Checkboxes ──────────────────────────────────────

interface ConfigChecklistProps {
  title: string;
  items: { id: string; display: string; implemented?: boolean }[];
  selected: Set<string>;
  onToggle: (id: string) => void;
}

function ConfigChecklist({ title, items, selected, onToggle }: ConfigChecklistProps) {
  return (
    <div className="space-y-2">
      <span className="text-xs font-mono text-gray-400">{title}</span>
      <div className="bg-[#0a0a14] rounded-lg border border-[#1e1e32] p-2 space-y-1">
        {items.map(item => {
          const isDisabled = item.implemented === false;
          const isChecked = selected.has(item.id);
          
          return (
            <label
              key={item.id}
              className={`
                flex items-center gap-2 px-2 py-1.5 rounded cursor-pointer
                ${isDisabled ? 'opacity-40 cursor-not-allowed' : 'hover:bg-white/[0.02]'}
              `}
            >
              <input
                type="checkbox"
                checked={isChecked}
                disabled={isDisabled}
                onChange={() => !isDisabled && onToggle(item.id)}
                className="h-3.5 w-3.5 rounded border-gray-600 bg-[#12121f] text-green-500 focus:ring-green-500/30"
              />
              <span className={`text-xs font-mono ${isChecked ? 'text-gray-200' : 'text-gray-500'}`}>
                {item.display}
              </span>
              {isDisabled && (
                <span className="text-[9px] font-mono text-amber-500/70 ml-auto">roadmap</span>
              )}
            </label>
          );
        })}
      </div>
    </div>
  );
}

// ── Manual Mode: Per-Viper Capital Budgets ────────────────────────────────────

interface ViperBudgetsProps {
  vipers: { id: string; display: string }[];
  budgets: Record<string, string>;
  onChange: (id: string, value: string) => void;
}

/** USDC max-exposure input per selected viper. Blank = keep squadron default. */
function ViperBudgets({ vipers, budgets, onChange }: ViperBudgetsProps) {
  if (vipers.length === 0) return null;
  return (
    <div className="space-y-2">
      <span className="text-xs font-mono text-gray-400">Viper Capital Budgets (USDC max exposure — blank keeps defaults)</span>
      <div className="bg-[#0a0a14] rounded-lg border border-[#1e1e32] p-2 grid grid-cols-2 gap-2">
        {vipers.map(v => (
          <label key={v.id} className="flex items-center gap-2 px-2 py-1">
            <span className="text-xs font-mono text-gray-300 flex-1">{v.display}</span>
            <div className="flex items-center gap-1">
              <span className="text-[10px] font-mono text-gray-500">$</span>
              <input
                type="number"
                min="0"
                step="1"
                placeholder="default"
                value={budgets[v.id] ?? ''}
                onChange={e => onChange(v.id, e.target.value)}
                className="w-20 bg-[#12121f] border border-[#1e1e32] rounded px-2 py-1 text-xs font-mono text-gray-200 text-right focus:outline-none focus:border-green-500/50"
              />
            </div>
          </label>
        ))}
      </div>
    </div>
  );
}

// ── Main Modal ────────────────────────────────────────────────────────────────

interface DeploySquadronModalProps {
  isOpen: boolean;
  onClose: () => void;
  /** Fired on a successful queue-up, with the deployment id and its class. */
  onDeployed?: (deploymentId: string, marketType: string) => void;
}

export default function DeploySquadronModal({ isOpen, onClose, onDeployed }: DeploySquadronModalProps) {
  // Mode: 'quick' or 'manual'
  const [mode, setMode] = useState<'quick' | 'manual'>('quick');
  
  // Selection state
  const [selectedType, setSelectedType] = useState<MarketType | null>(null);
  const [selectedMarket, setSelectedMarket] = useState<string | null>(null);
  const [selectedRaptors, setSelectedRaptors] = useState<Set<string>>(new Set());
  const [selectedVipers, setSelectedVipers] = useState<Set<string>>(new Set());
  const [viperBudgets, setViperBudgets] = useState<Record<string, string>>({});
  
  // Operator-chosen name. Optional: without one the squadron is named after its
  // class, which is fine until a second squadron of that class exists — then the
  // name is what tells them apart on screen and what gives each its own config
  // and budgets.
  const [name, setName] = useState('');

  // Deployment state
  const [deploying, setDeploying] = useState(false);
  const [confirmed, setConfirmed] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  // Fetch deployment region
  const { data: regionInfo, isLoading: regionLoading } = useSWR<DeploymentRegionInfo>(
    isOpen ? 'deployment-region' : null,
    getDeploymentRegion,
    { revalidateOnFocus: false }
  );

  // Fetch raptors/vipers for selected market type
  const { data: raptors = [], isLoading: raptorsLoading } = useSWR<RaptorKind[]>(
    isOpen && selectedType ? `raptors-${selectedType}` : null,
    () => getRaptorsForClass(selectedType!),
    { revalidateOnFocus: false }
  );

  const { data: vipers = [], isLoading: vipersLoading } = useSWR<ViperKindInfo[]>(
    isOpen && selectedType ? `vipers-${selectedType}` : null,
    () => getVipersForClass(selectedType!),
    { revalidateOnFocus: false }
  );

  // Fetch available markets for manual mode
  const { data: marketsResponse, isLoading: marketsLoading } = useSWR(
    isOpen && mode === 'manual' && selectedType ? `markets-${selectedType}` : null,
    () => getAvailableMarkets(selectedType!),
    { revalidateOnFocus: false, revalidateOnMount: true, dedupingInterval: 0 }
  );

  // Auto-select all implemented raptors and all vipers when type changes
  useEffect(() => {
    if (selectedType && raptors.length > 0) {
      setSelectedRaptors(new Set(raptors.filter(r => r.implemented).map(r => r.id)));
    }
    if (selectedType && vipers.length > 0) {
      setSelectedVipers(new Set(vipers.map(v => v.id)));
    }
  }, [selectedType, raptors, vipers]);

  // Clear selected market when type changes
  useEffect(() => {
    setSelectedMarket(null);
  }, [selectedType]);

  // Reset state when modal closes
  useEffect(() => {
    if (!isOpen) {
      setMode('quick');
      setSelectedType(null);
      setSelectedMarket(null);
      setSelectedRaptors(new Set());
      setSelectedVipers(new Set());
      setViperBudgets({});
      setError(null);
    }
  }, [isOpen]);

  // Toggle handlers
  const toggleRaptor = useCallback((id: string) => {
    setSelectedRaptors(prev => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  }, []);

  const toggleViper = useCallback((id: string) => {
    setSelectedVipers(prev => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  }, []);

  const setViperBudget = useCallback((id: string, value: string) => {
    setViperBudgets(prev => ({ ...prev, [id]: value }));
  }, []);

  // Deploy handler
  const handleDeploy = useCallback(async () => {
    if (!selectedType) return;
    
    setDeploying(true);
    setError(null);
    setConfirmed(null);
    
    // Collect valid budgets for selected vipers only (blank = keep default)
    const budgets: Record<string, number> = {};
    if (mode === 'manual') {
      for (const id of selectedVipers) {
        const parsed = parseFloat(viperBudgets[id] ?? '');
        if (Number.isFinite(parsed) && parsed >= 0) budgets[id] = parsed;
      }
    }
    
    try {
      const response = await deploySquadron({
        mode,
        market_type: selectedType,
        auto_config: mode === 'quick',
        market_id: mode === 'manual' ? selectedMarket ?? undefined : undefined,
        raptors: mode === 'manual' ? Array.from(selectedRaptors) : undefined,
        vipers: mode === 'manual' ? Array.from(selectedVipers) : undefined,
        viper_budgets: Object.keys(budgets).length > 0 ? budgets : undefined,
        name: name.trim() || undefined,
      });
      
      if (response.success && response.squadron_id) {
        // Confirm before closing. A deploy is QUEUED, not executed — the engine
        // picks it up on its own poll, so the squadron appears several seconds
        // later. Closing instantly left the operator watching an unchanged list
        // with nothing to say their click had landed, which reads as a dead
        // button even when the deployment succeeded.
        setConfirmed(`Queued. ${selectedType} squadron starting — it appears in the CAG registry shortly.`);
        onDeployed?.(response.squadron_id, selectedType);
        setTimeout(onClose, 1800);
      } else {
        setError(response.error || 'Deployment failed');
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Unknown error');
    } finally {
      setDeploying(false);
    }
    // Every value the callback READS must be listed, or useCallback memoises a
    // closure over the render in which it was created and keeps reading that
    // render's values forever. `name` and `viperBudgets` were missing: a typed
    // squadron name and per-viper capital budgets were captured as their initial
    // empty state and silently dropped from the request — the deploy succeeded,
    // so nothing surfaced the loss.
  }, [mode, selectedType, selectedMarket, selectedRaptors, selectedVipers,
      name, viperBudgets, onClose, onDeployed]);

  // Can deploy?
  // Why Deploy is unavailable, or null when it is available.
  //
  // A disabled button with no explanation is indistinguishable from a broken
  // one — the operator clicks it, nothing happens, and there is nothing on
  // screen or in the log to say why. Naming the missing piece costs a line.
  const blockedReason: string | null =
    !selectedType                                   ? 'Pick a market type first.'
    : mode === 'quick'                              ? null
    : !selectedMarket                               ? 'Pick a market from the list.'
    : selectedVipers.size === 0                     ? 'Select at least one viper.'
    : null;
  const canDeploy = blockedReason === null;

  if (!isOpen) return null;

  const availableTypes = regionInfo?.available_types ?? [];

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 backdrop-blur-sm">
      <div className="bg-[#0d0d1a] rounded-xl border border-[#1e1e32] shadow-2xl w-full max-w-lg mx-4 overflow-hidden">
        {/* Header */}
        <div className="flex items-center justify-between px-5 py-4 border-b border-[#1e1e32]">
          <div className="flex items-center gap-2">
            <span className="text-lg">🚀</span>
            <h2 className="text-sm font-mono font-semibold text-gray-200">Deploy Squadron</h2>
          </div>
          <button
            onClick={onClose}
            className="text-gray-500 hover:text-gray-300 transition-colors p-1"
          >
            <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
            </svg>
          </button>
        </div>

        {/* Mode Tabs */}
        <div className="flex border-b border-[#1e1e32]">
          <button
            onClick={() => setMode('quick')}
            className={`flex-1 px-4 py-2.5 text-xs font-mono transition-colors ${
              mode === 'quick'
                ? 'text-green-300 bg-green-500/10 border-b-2 border-green-500'
                : 'text-gray-500 hover:text-gray-400'
            }`}
          >
            🎯 Quick Deploy
          </button>
          <button
            onClick={() => setMode('manual')}
            className={`flex-1 px-4 py-2.5 text-xs font-mono transition-colors ${
              mode === 'manual'
                ? 'text-cyan-300 bg-cyan-500/10 border-b-2 border-cyan-500'
                : 'text-gray-500 hover:text-gray-400'
            }`}
          >
            ⚙️ Full Control
          </button>
        </div>

        {/* Content */}
        <div className="p-5 space-y-4 max-h-[60vh] overflow-y-auto">
          {/* Region info */}
          {regionInfo && (
            <div className="flex items-center gap-2 text-[10px] font-mono text-gray-600">
              <span>🌍</span>
              <span>
                {regionInfo.region.toUpperCase()} deployment — {availableTypes.join(', ')} markets
              </span>
            </div>
          )}

          {/* Market Type Selection */}
          <div className="space-y-2">
            <label className="text-xs font-mono text-gray-400">Market Type</label>
            {regionLoading ? (
              <div className="text-xs font-mono text-gray-500 animate-pulse">Loading regions...</div>
            ) : (
              <MarketTypeSelector
                available={availableTypes}
                selected={selectedType}
                onSelect={setSelectedType}
              />
            )}
          </div>

          {/* Mode-specific content */}
          {selectedType && (
            <>
              {mode === 'quick' ? (
                <QuickDeployPreview
                  marketType={selectedType}
                  raptors={raptors}
                  vipers={vipers}
                  loading={raptorsLoading || vipersLoading}
                />
              ) : (
                <>
                  {/* Market Browser */}
                  <div className="space-y-2">
                    <label className="text-xs font-mono text-gray-400">Select Market</label>
                    <MarketBrowser
                      markets={marketsResponse?.markets ?? []}
                      selected={selectedMarket}
                      onSelect={setSelectedMarket}
                      loading={marketsLoading}
                    />
                  </div>

                  {/* Raptor/Viper Config */}
                  <div className="grid grid-cols-2 gap-4">
                    <ConfigChecklist
                      title="Raptors (Signal Sources)"
                      items={raptors}
                      selected={selectedRaptors}
                      onToggle={toggleRaptor}
                    />
                    <ConfigChecklist
                      title="Vipers (Strategies)"
                      items={vipers.map(v => ({ id: v.id, display: v.display }))}
                      selected={selectedVipers}
                      onToggle={toggleViper}
                    />
                  </div>

                  {/* Per-Viper Capital Budgets */}
                  <ViperBudgets
                    vipers={vipers.filter(v => selectedVipers.has(v.id)).map(v => ({ id: v.id, display: v.display }))}
                    budgets={viperBudgets}
                    onChange={setViperBudget}
                  />
                </>
              )}
            </>
          )}

          {/* Error */}
          {error && (
            <div className="bg-red-500/10 border border-red-500/30 rounded-lg px-3 py-2">
              <span className="text-xs font-mono text-red-400">{error}</span>
            </div>
          )}

          {/* Name — optional, but the only thing that tells two squadrons of one
              class apart once a second exists. Inside the scroll container: when it
              sat below it the modal grew past the viewport and pushed the Deploy
              button off-screen, which reads as a dead button. */}
          <div>
            <label className="block text-[11px] font-mono text-gray-400 mb-1.5">
              Squadron name <span className="text-gray-600">(optional)</span>
            </label>
            <input
              type="text"
              value={name}
              onChange={e => setName(e.target.value)}
              placeholder="e.g. 15m Scalper"
              maxLength={48}
              className="w-full bg-[#0a0a14] border border-[#1e1e32] rounded px-3 py-2
                         text-xs font-mono text-gray-200 placeholder:text-gray-700
                         focus:outline-none focus:border-green-500/40"
            />
            <p className="text-[10px] text-gray-600 mt-1.5 leading-relaxed">
              Names a second squadron of this class so it gets its own strategy
              settings, budgets and positions. Leave blank for the default name.
            </p>
          </div>
        </div>

        {/* Footer */}
        <div className="flex items-center justify-end gap-3 px-5 py-4 border-t border-[#1e1e32] bg-[#0a0a14]">
          {confirmed ? (
            <span className="mr-auto text-[10px] font-mono text-green-400">✓ {confirmed}</span>
          ) : blockedReason ? (
            <span className="mr-auto text-[10px] font-mono text-amber-400/80">{blockedReason}</span>
          ) : null}
          <button
            onClick={onClose}
            className="px-4 py-2 text-xs font-mono text-gray-400 hover:text-gray-300 transition-colors"
          >
            Cancel
          </button>
          <button
            onClick={handleDeploy}
            disabled={!canDeploy || deploying}
            className={`
              flex items-center gap-2 px-4 py-2 rounded-lg text-xs font-mono font-medium transition-all
              ${canDeploy && !deploying
                ? 'bg-green-500/20 text-green-300 border border-green-500/30 hover:bg-green-500/30'
                : 'bg-gray-800 text-gray-600 border border-gray-700 cursor-not-allowed'
              }
            `}
          >
            {deploying ? (
              <>
                <span className="animate-spin">⏳</span>
                Deploying...
              </>
            ) : (
              <>
                🚀 Deploy Squadron
              </>
            )}
          </button>
        </div>
      </div>
    </div>
  );
}
