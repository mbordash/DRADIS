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
  const mode = !enabled ? 'off' : cfg.live ? (config.ghost_mode ? 'live · ghost' : 'live') : 'paper';
  const modeCls = mode === 'live' ? 'text-red-300 border-red-500/40'
    : mode === 'live · ghost' ? 'text-amber-300 border-amber-500/40'
    : mode === 'paper' ? 'text-teal-300 border-teal-500/40' : 'text-gray-500 border-gray-600/40';
  const bad = isTroubled(row);

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
      <div className="flex items-center justify-between pt-1">
        <button
          className={`px-2 py-1 rounded border text-[11px] font-mono ${enabled ? 'border-teal-500/50 bg-teal-500/10 text-teal-200' : 'border-[#1e1e32] text-gray-400'}`}
          onClick={() => onPatch({ custom_vipers: { [name]: { enabled: !enabled } } })}
          title="This squadron only. Off stops new entries; what it holds keeps its exits."
        >
          {enabled ? 'On here' : 'Off here'}
        </button>
        <a href="#vipers" className="text-[11px] font-mono text-indigo-300 hover:text-indigo-200">Edit in Build a Viper →</a>
      </div>
    </div>
  );
}
