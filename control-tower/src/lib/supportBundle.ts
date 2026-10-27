// SPDX-License-Identifier: AGPL-3.0-only
//
// DRADIS — autonomous trading engine for crypto prediction markets.
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
 * Support bundle — one text file an operator can attach to a support request.
 *
 * The first support request (2026-10-17) asked why an instance was not trading.
 * Answering it needed the engine version and venue, which Vipers were holding
 * and on which gate, the squadrons on patrol, the live risk knobs and the recent
 * log. Every one of those is already on screen somewhere in the Control Tower,
 * so the operator was about to be asked for a shell command. Operators on the
 * Marketplace AMI do not have a shell, and should not need one: this gathers the
 * same facts from the same endpoints the views read and saves them as a file.
 *
 * Built in the browser with a Blob and an anchor click, so it works on a plain
 * http origin where `navigator.clipboard` (the Copy button) is unavailable.
 *
 * What it contains, and why it is safe to send: the setup status (version,
 * venue, edition, which integrations are configured, never their values), the
 * session status (raptor feed health, dark market feeds), the squadrons, the
 * Viper status registry with its refusal ledger, the DynamicConfig (risk knobs;
 * it carries no credentials by construction) as the instance-wide row and as
 * each squadron's own row, and the engine's log ring. Nothing
 * reads a credential, a wallet, or a key. Each section degrades to a one-line
 * note when its endpoint is unavailable, so a half-configured or unauthenticated
 * instance still produces a useful file.
 */

import { getConfig, getLogs, getSquadronConfig, getSquadrons, getStatus, getVipersStatus, type ViperStatusRow } from './api';
import type { DynamicConfig, SquadronSummary } from './types';
import { getSetupStatus } from './setupApi';

/** Log lines to include: the engine's whole ring buffer. */
const LOG_TAIL = 2000;

type Section = { title: string; body: string };

function rule(title: string): string {
  return `\n=== ${title} ${'='.repeat(Math.max(0, 70 - title.length))}\n`;
}

async function section<T>(title: string, load: () => Promise<T>, render: (v: T) => string): Promise<Section> {
  try {
    return { title, body: render(await load()) };
  } catch (e) {
    return { title, body: `unavailable: ${e instanceof Error ? e.message : String(e)}` };
  }
}

function fmtAgo(secs: number | null | undefined): string {
  if (secs == null) return 'n/a';
  if (secs < 60) return `${secs}s ago`;
  if (secs < 3600) return `${Math.floor(secs / 60)}m ago`;
  return `${Math.floor(secs / 3600)}h ${Math.floor((secs % 3600) / 60)}m ago`;
}

function renderVipers(rows: ViperStatusRow[]): string {
  if (rows.length === 0) return 'no viper status rows (no squadron has ticked yet)';
  return rows.map(r => {
    const head = `${r.asset.padEnd(8)} ${r.strategy.padEnd(22)} ${r.last_outcome.padEnd(9)} eval ${fmtAgo(r.last_eval_secs_ago)}`;
    const reason = r.last_reason ? `\n    holding: ${r.last_reason} (${fmtAgo(r.last_reason_secs_ago)})` : '';
    const signal = r.last_signal_at ? `\n    last signal: ${r.last_signal_at} (${fmtAgo(r.last_signal_secs_ago)})` : '';
    const detail = r.detail ? `\n    detail: ${r.detail}` : '';
    const refusals = r.refusals.length
      ? '\n    refused: ' + r.refusals.map(t => `${t.count}x ${t.reason}`).join('; ')
        + '\n    latest:  ' + r.refusals.map(t => t.last_detail).filter(Boolean).join(' | ')
      : '';
    return head + reason + signal + detail + refusals;
  }).join('\n');
}

/** Every Viper status row the engine has, across the assets the squadrons name. */
async function allViperRows(): Promise<ViperStatusRow[]> {
  const seen = new Map<string, ViperStatusRow>();
  const add = (rows: ViperStatusRow[]) => rows.forEach(r => seen.set(`${r.asset}/${r.strategy}`, r));
  add(await getVipersStatus());
  let assets: string[] = [];
  try {
    assets = Array.from(new Set((await getSquadrons()).map(s => s.asset).filter((a): a is string => !!a)));
  } catch { /* the squadron list is its own section; the default rows still stand */ }
  for (const a of assets) {
    if (![...seen.values()].some(r => r.asset.toLowerCase() === a.toLowerCase())) {
      try { add(await getVipersStatus(a)); } catch { /* reported by the default call's absence */ }
    }
  }
  return [...seen.values()].sort((x, y) => x.asset.localeCompare(y.asset) || x.strategy.localeCompare(y.strategy));
}

/** A config key that switches a Viper or a mode on or off. */
function isSwitch(key: string): boolean {
  return /^enable_/.test(key) || /_enabled$/.test(key) || key === 'ghost_mode';
}

type SquadronConfigRow = { squadron: SquadronSummary; config: DynamicConfig | null; error?: string };

/**
 * Every squadron's own config row beside the instance-wide one.
 *
 * The third support request (2026-10-27) carried a bundle whose Viper status
 * said eight Vipers were "disabled in config" while its config section showed
 * every `enable_*` flag true. Both were right: the status came from the
 * squadron's row, the config section printed only the instance-wide row, and a
 * squadron's row is what its loop reads. Printing the squadron rows ends that
 * contradiction: every on/off switch with its effective value, then whatever
 * else the squadron overrides, so a reader sees at once which Vipers can trade.
 */
async function squadronConfigs(): Promise<{ global: DynamicConfig; rows: SquadronConfigRow[] }> {
  const [global, squadrons] = await Promise.all([getConfig(), getSquadrons()]);
  const rows = await Promise.all(squadrons.map(async squadron => {
    try {
      return { squadron, config: await getSquadronConfig(squadron.id) };
    } catch (e) {
      return { squadron, config: null, error: e instanceof Error ? e.message : String(e) };
    }
  }));
  return { global, rows };
}

function renderSquadronConfigs({ global, rows }: { global: DynamicConfig; rows: SquadronConfigRow[] }): string {
  if (rows.length === 0) return 'no squadrons deployed';
  const g = global as unknown as Record<string, unknown>;
  return rows.map(({ squadron, config, error }) => {
    const head = `${squadron.id} (${squadron.asset ?? '-'}, ${squadron.state})`;
    if (!config) return `${head}
  unavailable: ${error ?? 'no config row'}`;
    const c = config as unknown as Record<string, unknown>;
    const keys = Object.keys(c).sort();
    const switches = keys.filter(isSwitch).map(k => `  ${k}: ${JSON.stringify(c[k])}`);
    const overrides = keys
      .filter(k => !isSwitch(k) && JSON.stringify(c[k]) !== JSON.stringify(g[k]))
      .map(k => `  ${k}: ${JSON.stringify(c[k])} (instance-wide: ${JSON.stringify(g[k])})`);
    return [
      head,
      '  switches (what this squadron\'s loop reads):',
      ...(switches.length ? switches : ['  (none)']),
      '  other differences from the instance-wide row:',
      ...(overrides.length ? overrides : ['  none']),
    ].join('\n');
  }).join('\n');
}

export interface SupportBundle {
  filename: string;
  text: string;
}

/** Gather the bundle. Never throws: a failing source becomes a note in its section. */
export async function buildSupportBundle(now = new Date()): Promise<SupportBundle> {
  const setup = await section('Instance', getSetupStatus, s => [
    `app_version:      ${s.app_version}`,
    `venue:            ${s.venue}${s.venues_available?.length ? ` (image has: ${s.venues_available.join(', ')})` : ''}`,
    `edition:          ${s.edition ?? 'community'}`,
    `venue_configured: ${s.venue_configured}`,
    `llm_provider:     ${s.llm_provider || 'none'} (enabled: ${s.llm_enabled ?? 'n/a'}, ready: ${s.llm_provider_ready ?? 'n/a'})`,
  ].join('\n'));

  const [status, squadrons, vipers, config, squadronConfig, logs] = await Promise.all([
    section('Session', getStatus, s => {
      const raptors = s.raptors
        ? Object.entries(s.raptors).map(([asset, h]) => `  ${asset}: ${JSON.stringify(h)}`).join('\n')
        : '  (no raptor health reported)';
      const dark = s.dark_market_feeds?.length
        ? s.dark_market_feeds.map(d => `  ${d.market}: dark for ${d.dark_for_secs}s`).join('\n')
        : '  none';
      return [
        `session_started_at: ${s.session_started_at ?? 'n/a'}`,
        `llm_advisor_enabled: ${s.llm_advisor_enabled ?? 'n/a'}`,
        'raptors:', raptors,
        'dark market feeds:', dark,
      ].join('\n');
    }),
    section('Squadrons', getSquadrons, rows => rows.length === 0
      ? 'none deployed'
      : rows.map(s => `${s.id.padEnd(16)} ${(s.asset ?? '-').padEnd(8)} ${s.state.padEnd(11)} ${s.market_class ?? '-'} | ${s.market_name ?? '-'}${s.maker_market_name ? ` | maker: ${s.maker_market_name}` : ''}`).join('\n')),
    section('Viper status (holding reasons and refusal ledger)', allViperRows, renderVipers),
    section('Dynamic config, instance-wide row (risk knobs, no credentials)', getConfig, c => JSON.stringify(c, null, 2)),
    section('Squadron config (each squadron\'s own row: every switch, then its other differences)', squadronConfigs, renderSquadronConfigs),
    section(`Engine log (last ${LOG_TAIL} lines of the ring buffer)`, () => getLogs(LOG_TAIL), l => l.lines.join('\n')),
  ]);

  const header = [
    'DRADIS support bundle',
    `generated_at: ${now.toISOString()}`,
    `control_tower: ${typeof navigator !== 'undefined' ? navigator.userAgent : 'n/a'}`,
    'contents: instance, session, squadrons, viper status, instance-wide config, each squadron\'s config, engine log. No credentials.',
    'send to: the support form at https://dradis.live or a reply to your support thread.',
  ].join('\n');

  const sections = [setup, status, squadrons, vipers, config, squadronConfig, logs];
  const text = header + sections.map(s => rule(s.title) + s.body + '\n').join('');

  const version = /app_version:\s+(\S+)/.exec(setup.body)?.[1] ?? 'unknown';
  const venue = /venue:\s+(\S+)/.exec(setup.body)?.[1] ?? 'unknown';
  const stamp = now.toISOString().replace(/[-:]/g, '').slice(0, 13).replace('T', '-');
  return { filename: `dradis-support-v${version}-${venue}-${stamp}.txt`, text };
}

/** Save the bundle through the browser's download path (works on http origins). */
export function saveTextFile(filename: string, text: string): void {
  const blob = new Blob([text], { type: 'text/plain;charset=utf-8' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  a.remove();
  setTimeout(() => URL.revokeObjectURL(url), 1_000);
}
