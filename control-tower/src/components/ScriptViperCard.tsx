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
 * [E86] A scripted viper on a squadron's page: what it is doing here, its mode,
 * and a switch for this squadron only. The script itself, its Live switch and
 * its parameters are edited on the Build a Viper page.
 */

import { useState } from 'react';
import type { DynamicConfig } from '@/lib/types';
import type { ViperStatusRow } from '@/lib/api';
import { fmtAgo, isTroubled } from '@/components/ViperCard';

export default function ScriptViperCard({ row, config, onPatch }: {
  row: ViperStatusRow;
  config: DynamicConfig;
  onPatch: (patch: Partial<DynamicConfig>) => Promise<void>;
}) {
  const name = row.strategy.replace(/^Script:/, '');
  const cfg = config.custom_vipers?.[name] ?? {};
  const enabled = cfg.enabled ?? true;
  const paperHere = (cfg.paper_here ?? false) || !!config.squadron_held;
  // [E102a] Live on the instance, but paper on a squadron that says so.
  const live = !!cfg.live && !paperHere;
  const mode = !enabled ? 'off' : live ? (config.ghost_mode ? 'live · ghost' : 'live') : (cfg.live ? 'paper here' : 'paper');
  const modeCls = mode === 'live' ? 'text-red-300 border-red-500/40'
    : mode === 'live · ghost' ? 'text-amber-300 border-amber-500/40'
    : mode === 'paper' || mode === 'paper here' ? 'text-teal-300 border-teal-500/40' : 'text-gray-500 border-gray-600/40';
  const bad = isTroubled(row);
  // [E102b] This squadron's share of the script's cap; blank = no share,
  // only the script's cap applies.
  const savedHere = cfg.max_exposure_here_usdc == null ? '' : String(cfg.max_exposure_here_usdc);
  const [capHere, setCapHere] = useState(savedHere);
  const capDirty = capHere.trim() !== savedHere;
  const capValid = capHere.trim() === '' || (Number.isFinite(Number(capHere)) && Number(capHere) > 0);

  return (
    <div className={`card p-4 border ${bad ? 'border-red-500/40' : 'border-indigo-500/20'} space-y-2`}>
      <div className="flex items-center justify-between gap-2">
        <div>
          <div className="text-sm font-mono text-gray-100">🛠️ {name}</div>
          <div className="text-[10px] font-mono text-gray-500">your script · eval {fmtAgo(row.last_eval_secs_ago)}</div>
        </div>
        <span className={`text-[9px] font-mono uppercase border rounded px-1 ${modeCls}`}>{mode}</span>
      </div>
      {row.detail && <div className="text-[11px] font-mono text-gray-300">{row.detail}</div>}
      {row.last_reason && row.last_reason !== row.last_buy_refused && (
        <div className={`text-[11px] font-mono ${bad || row.last_reason.startsWith('script error') ? 'text-red-300' : 'text-gray-400'}`}>
          {row.last_reason}
        </div>
      )}
      {row.last_buy_refused && (
        <div className="text-[11px] font-mono text-amber-300" title="A buy the script decided on, refused by an engine check after the decision">
          buy refused {fmtAgo(row.last_buy_refused_secs_ago ?? null)}: {row.last_buy_refused}
        </div>
      )}
      <div className="flex items-center gap-2 text-[11px] font-mono text-gray-400"
           title="This squadron's share of the script's cap: at most this much open here. The script's own cap still applies across every squadron. Blank: no share.">
        <span>Cap here $</span>
        <input
          className="w-16 bg-[#0d0d1a] border border-[#1e1e32] rounded px-1 py-0.5 text-gray-200"
          value={capHere}
          placeholder="—"
          onChange={(e) => setCapHere(e.target.value)}
        />
        {capDirty && (
          <button
            disabled={!capValid}
            className="px-2 py-0.5 rounded border border-indigo-500/50 text-indigo-200 disabled:opacity-40"
            onClick={() => onPatch({ custom_vipers: { [name]: { max_exposure_here_usdc: capHere.trim() === '' ? null : capHere.trim() } } })}
          >
            Save
          </button>
        )}
      </div>
      <div className="flex items-center justify-between pt-1">
        <button
          className={`px-2 py-1 rounded border text-[11px] font-mono ${enabled ? 'border-teal-500/50 bg-teal-500/10 text-teal-200' : 'border-[#1e1e32] text-gray-400'}`}
          onClick={() => onPatch({ custom_vipers: { [name]: { enabled: !enabled } } })}
          title="This squadron only. Off stops new entries; what it holds keeps its exits."
        >
          {enabled ? 'On here' : 'Off here'}
        </button>
        {/* [E102a] Hidden while the squadron is held: every script is paper there. */}
        {cfg.live && !config.squadron_held && (
          <button
            className={`px-2 py-1 rounded border text-[11px] font-mono ${paperHere ? 'border-teal-500/50 bg-teal-500/10 text-teal-200' : 'border-red-500/40 text-red-200'}`}
            onClick={() => onPatch({ custom_vipers: { [name]: { paper_here: !paperHere } } })}
            title="This squadron only. Paper here keeps the script in its paper ledger on this squadron while it trades live elsewhere."
          >
            {paperHere ? 'Paper here' : 'Live here'}
          </button>
        )}
        <a href="#vipers" className="text-[11px] font-mono text-indigo-300 hover:text-indigo-200">Edit in Build a Viper →</a>
      </div>
    </div>
  );
}
