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

// Read-only demo mode flag.
//
// When `NEXT_PUBLIC_DEMO_MODE=true` at build time, the Control Tower renders a
// persistent "read-only demo" banner and disables every mutating control
// (viper toggles, config edits, RTB / manual-exit). This is the UI half of the
// demo; the backend independently rejects all writes when `DRADIS_READ_ONLY=true`,
// so the demo is safe even if a control is missed or the API is hit directly.
//
// NEXT_PUBLIC_* vars are inlined at build time, so this works in both server and
// client components.
export const DEMO_MODE = process.env.NEXT_PUBLIC_DEMO_MODE === 'true';

/** Public repo URL surfaced in the demo banner. */
export const REPO_URL = 'https://github.com/mbordash/DRADIS';
